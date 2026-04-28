import queue
import time
import asyncio
import logging
import threading

logger = logging.getLogger(__name__)


class AsyncExecutor:
    _instance = None
    _loop = None
    _loop_thread = None
    _loop_lock = threading.Lock()

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            with cls._loop_lock:
                if cls._instance is None:
                    cls._instance = AsyncExecutor()
        return cls._instance

    @classmethod
    def ensure_loop(cls):
        """
        BaseGoose와 공유하는 전역 asyncio 이벤트 루프를 생성합니다.
        """
        with cls._loop_lock:
            if cls._loop is not None:
                return cls._loop

            loop = asyncio.new_event_loop()

            def run_loop():
                asyncio.set_event_loop(loop)
                try:
                    loop.run_forever()
                except Exception as e:
                    logger.exception(f"[AsyncExecutor] asyncio 루프 예외: {e}")

            t = threading.Thread(
                target=run_loop,
                name="GlobalAsyncLoop",
                daemon=True,
            )
            t.start()

            cls._loop = loop
            cls._loop_thread = t
            logger.info("[AsyncExecutor] 전역 asyncio 이벤트 루프 생성 및 시작")
            return cls._loop

    def __init__(self):
        self.loop = self.ensure_loop()

    def run_coroutine(self, coro):
        """threadsafe하게 코루틴을 스케줄링합니다"""
        return asyncio.run_coroutine_threadsafe(coro, self.loop)

    def run_in_executor(self, func, *args):
        """이벤부 루프의 default executor에서 함수를 실행합니다"""
        return self.loop.run_in_executor(None, func, *args)


class DBAsyncWriter:
    """
    [Phase 4] 메인 스레드 블로킹 방지를 위한 비동기 DB 쓰기 워커
    - 큐(Queue)를 통해 DB 쓰기 작업을 수집하고 별도 데몬 스레드에서 일괄 처리합니다.
    """

    def __init__(self, execute_func):
        self.queue = queue.Queue()
        self.execute_func = execute_func  # IntradayTrading.execute_batch_updates
        self.worker_thread = threading.Thread(
            target=self._worker, name="DBAsyncWriter", daemon=True
        )
        self.worker_thread.start()
        logger.info("[DBAsyncWriter] 백그라운드 DB 쓰기 워커가 가동되었습니다.")

    def push_batch(self, **batch_data):
        """데이터베이스 업데이트 작업을 큐에 던집니다 (O(1))"""
        self.queue.put(batch_data)

    def stop(self):
        """워커 스레드를 안전하게 종료합니다."""
        self.queue.put(None)
        if self.worker_thread.is_alive():
            self.worker_thread.join(timeout=3.0)
        logger.info("[DBAsyncWriter] 백그라운드 워커가 성공적으로 종료되었습니다.")

    def _worker(self):
        # DB 연결은 워커 스레드 내에서 별도로 관리
        from util.CommUtils import get_db_connection

        while True:
            try:
                # 큐에서 작업 추출 (대기)
                batch_data = self.queue.get()
                if batch_data is None:
                    break

                with get_db_connection() as conn:
                    cursor = conn.cursor()
                    try:
                        self.execute_func(cursor, **batch_data)
                        conn.commit()
                    except Exception as e:
                        conn.rollback()
                        logger.error(f"[DBAsyncWriter] DB Batch Execution Error: {e}")
                    finally:
                        cursor.close()
                self.queue.task_done()
            except Exception as e:
                logger.error(f"[DBAsyncWriter] Internal Worker Error: {e}")
                time.sleep(1)  # 과도한 루프 방지

