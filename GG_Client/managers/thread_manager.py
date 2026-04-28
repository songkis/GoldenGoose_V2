import threading
import time
import queue
import gc
import pythoncom
from collections import deque
from concurrent.futures import ThreadPoolExecutor

from PyQt5.QtCore import QObject, QThread, QTimer, pyqtSlot, QMutex, QMutexLocker

from util.Utils32 import safe_invoke

logger = None  # 초기값


def set_logger(external_logger):
    global logger
    logger = external_logger


class LimitedThreadPoolExecutor(ThreadPoolExecutor):
    def __init__(self, max_workers=64, queue_size=2000, retry_interval=0.5):
        super().__init__(max_workers=max_workers)
        self.limited_queue = queue.Queue(maxsize=queue_size)
        self.retry_queue = deque(maxlen=500)
        self.retry_lock = threading.Lock()
        self.retry_interval = retry_interval
        self.retry_worker_thread = threading.Thread(
            target=self._retry_worker, daemon=True
        )
        self.retry_worker_thread.start()
        self._com_state = threading.local()  # 각 스레드마다 COM 초기화 여부 저장

    def _com_initialized_wrapper(self, fn):
        def wrapped(*args, **kwargs):
            if not hasattr(self._com_state, "initialized"):
                try:
                    pythoncom.CoInitialize()
                    self._com_state.initialized = True
                except pythoncom.com_error as e:
                    logger.warning(f"[COM] 초기화 실패: {e}")
            return fn(*args, **kwargs)

        return wrapped

    def submit(self, fn, *args, **kwargs):
        try:
            self.limited_queue.put(True, timeout=0.5)
        except queue.Full:
            logger.warning(
                "🚨 작업 제출 실패: Executor 큐가 가득 참. fallback으로 이동."
            )
            self._fallback(fn, args, kwargs)
            return None

        wrapped_fn = self._com_initialized_wrapper(fn)
        try:
            future = super().submit(wrapped_fn, *args, **kwargs)
            future.add_done_callback(self._release_queue_slot)
            return future
        except Exception as e:
            logger.exception(f"🚨 submit() 중 예외 발생: {e}")
            self.limited_queue.get()
            self._fallback(fn, args, kwargs)
            return None

    def _fallback(self, fn, args, kwargs):
        with self.retry_lock:
            if len(self.retry_queue) < self.retry_queue.maxlen:
                self.retry_queue.append((fn, args, kwargs))
                logger.info(f"[{self.__class__.__name__}] ✅ fallback 큐에 작업 저장됨")
            else:
                logger.warning("⚠ fallback 큐까지 포화 상태. 작업 완전 무시됨.")

    def _retry_worker(self):
        while True:
            time.sleep(self.retry_interval)
            with self.retry_lock:
                for _ in range(len(self.retry_queue)):
                    fn, args, kwargs = self.retry_queue.popleft()
                    if self.limited_queue.full():
                        self.retry_queue.appendleft((fn, args, kwargs))
                        break
                    wrapped_fn = self._com_initialized_wrapper(fn)
                    try:
                        future = super().submit(wrapped_fn, *args, **kwargs)
                        future.add_done_callback(self._release_queue_slot)
                    except Exception as e:
                        logger.exception(f"[retry_worker] 재제출 실패: {e}")
                        self.retry_queue.append((fn, args, kwargs))
                        break

    def _release_queue_slot(self, future):
        try:
            self.limited_queue.get()
        except Exception as e:
            logger.exception("슬롯 반환 중 예외:", exc_info=e)

    def utilization(self):
        """현재 작업 큐 사용률 반환 (0~1 사이 float)"""
        return self.limited_queue.qsize() / self.limited_queue.maxsize


# GooseRunner 클래스 - 비즈니스 로직 스레드 처리용
class GooseRunner(QObject):
    def __init__(self, goose, mainwindow):
        super().__init__()
        self.goose = goose
        self.mainwindow = mainwindow
        self._com_initialized = False  # COM 초기화 상태 플래그

    def ensure_com_initialized(self):
        """해당 스레드에서 COM 초기화가 필요한 경우에만 초기화."""
        import pythoncom

        if not self._com_initialized:
            try:
                pythoncom.CoInitialize()
                self._com_initialized = True
                logger.info(
                    f"[{self.__class__.__name__}] [GooseRunner] COM 초기화 완료 - Goose: {self.goose.Name}"
                )
            except pythoncom.com_error as e:
                logger.warning(
                    f"[GooseRunner] COM 초기화 실패 - Goose: {self.goose.Name}, error: {e}"
                )

    @pyqtSlot()
    def run(self):
        try:
            logger.info(
                f"[{self.__class__.__name__}] [GooseRunner.run] 실행됨 - Goose: {self.goose.Name}"
            )
            #  safe_invoke-direct(동기 호출)가 메인 스레드를 장시간 독점하는 문제 방지.
            # create_activeX_objects → Lay() → do_register → AdviseRealData(XING API 블로킹)
            # 순서로 메인 스레드가 수초간 점유되어 GuardianGoose 시작 타이머가 실행 기회를 못 얻음.
            # QTimer.singleShot(delay, func)으로 다음 이벤트 루프 사이클로 미루면
            # 현재 사이클에서 다른 이벤트(타이머 등)가 먼저 처리되어 UI 응답성 유지.
            goose = self.goose
            mainwindow = self.mainwindow

            def _do_create():
                safe_invoke(mainwindow, "create_activeX_objects", goose)

            QTimer.singleShot(0, _do_create)
            logger.info(
                f"[GooseRunner.run] create_activeX_objects QTimer.singleShot(0) 예약됨 - Goose: {goose.Name}"
            )
        except Exception as e:
            logger.exception(
                f"[GooseRunner.run] 예외 발생 - Goose: {self.goose.Name}, error: {e}"
            )
            self.mainwindow.ToTelegram(f"[GooseRunner.run] 오류:\n{str(e)}")

    # ❌ parent=self	QObject는 부모 있으면 다른 스레드로 이동 못 함
    # ✅ self.mainwindow = mainwindow	참조만 넘기면 안전하게 접근 가능
    # ✅ goose.Run(..., parent=self.mainwindow)	필요한 곳에서만 명시적으로 넘기기


class GooseThreadManager:
    def __init__(self, mainwindow):
        self.mainwindow = mainwindow
        self.threads = {}  # uuid -> QThread
        self.runners = {}  # uuid -> GooseRunner
        self.mutex = QMutex()

    def start(self, goose):
        uuid = goose.UUID
        with QMutexLocker(self.mutex):
            thread = QThread()
            runner = GooseRunner(goose, self.mainwindow)
            runner.moveToThread(thread)

            def on_started():
                logger.info(
                    f"[{self.__class__.__name__}] Thread started - Goose: {goose.Name}"
                )
                runner.run()

            thread.started.connect(on_started)
            thread.finished.connect(thread.deleteLater)
            thread.finished.connect(runner.deleteLater)

            self.threads[uuid] = thread
            self.runners[uuid] = runner

            thread.start()
            logger.info(
                f"[{self.__class__.__name__}] Thread started and runner moved - Goose: {goose.Name}"
            )

    def stop(self, uuid):
        if uuid in self.threads:
            thread = self.threads[uuid]
            thread.quit()
            thread.wait()
            del self.threads[uuid]
            del self.runners[uuid]
            logger.info(f"[{self.__class__.__name__}] Thread stopped - UUID: {uuid}")
            gc.collect()

    def stop_all(self):
        for uuid in list(self.threads.keys()):
            self.stop(uuid)
