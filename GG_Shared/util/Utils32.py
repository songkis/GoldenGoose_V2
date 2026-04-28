# -*- coding: utf-8 -*-
import threading

from PyQt5.QtCore import (
    Q_ARG,
    QCoreApplication,
    QMetaObject,
    QObject,
    Qt,
    QThread,
    pyqtSlot,
)


class Invoker(QObject):
    @pyqtSlot(object)
    def invoke(self, func):
        # [Optimization] ZMQ 폭주 시 I/O 병목을 유발하는 불필요한 info 로깅 제거
        try:
            func()
        except AttributeError as e:
            if logger:
                logger.error(f"[Invoker] AttributeError invoking function: {e}")
        except RuntimeError as e:
            # 🛡️  C++ UI 위젯이 이미 소멸(deleted)된 상태에서 접근 시 안전하게 무시
            if "has been deleted" in str(e):
                pass
            else:
                if logger:
                    logger.error(f"[Invoker] RuntimeError invoking function: {e}")
        except Exception as e:
            if logger:
                logger.error(f"[Invoker] Error invoking function: {e}")


logger = None  # 초기값


def set_utils32_logger(external_logger):
    global logger
    logger = external_logger


_invoker = None
_depth = threading.local()


def get_invoker():
    global _invoker
    with _zmq_global_lock:  # Reuse global lock for thread-safety during init
        if _invoker is None:
            from PyQt5.QtWidgets import QApplication

            app = QApplication.instance()
            if not app:
                return None

            # [Critical Fix] Ensure Invoker is always on the Main (GUI) Thread
            _invoker = Invoker()
            main_thread = app.thread()
            if _invoker.thread() != main_thread:
                _invoker.moveToThread(main_thread)
                if logger:
                    logger.info(f"🛡️ [Invoker] Created and MOVED to Main Thread ({main_thread}) from {threading.current_thread().name}")
            else:
                if logger:
                    logger.info(f"🛡️ [Invoker] Created on Main Thread ({main_thread})")
                    
        return _invoker


_zmq_global_lock = threading.RLock()  # Defined for get_invoker safety


def safe_invoke(obj, method_name=None, *args):
    """중첩 호출 방지 + 메인스레드에서 invokeMethod 또는 QTimer.safe 실행"""
    if not hasattr(_depth, "value"):
        _depth.value = 0

    # ✅ 함수형만 들어온 경우 (obj/method_name 없이)
    if obj is None and method_name is None and len(args) == 1 and callable(args[0]):
        if _depth.value >= 10:
            if logger:
                logger.error("[safe_invoke-depth] 함수형 invokeMethod depth overflow")
            return

        invoker = get_invoker()
        if not invoker:
            # Fallback: QApplication이 없으면 직접 실행 (기동 초기 단계)
            try:
                args[0]()
            except RuntimeError as e:
                if "has been deleted" in str(e):
                    pass  # C++ 객체가 소멸된 경우 무시
                elif logger:
                    logger.error(f"[safe_invoke-fallback] 실행 에러: {e}")
            except Exception as e:
                if logger:
                    logger.error(f"[safe_invoke-fallback] 알 수 없는 에러: {e}")
            return

        _depth.value += 1
        try:
            QMetaObject.invokeMethod(
                invoker, "invoke", Qt.QueuedConnection, Q_ARG("PyQt_PyObject", args[0])
            )
            if logger:
                logger.debug(
                    f"[safe_invoke-func] 처리 요청됨 (invokeMethod), _depth.value : {_depth.value}"
                )
        except Exception as e:
            if logger:
                logger.exception(f"[safe_invoke-func] invokeMethod 호출 실패: {e}")
        finally:
            _depth.value -= 1
        return

    if obj is None or method_name is None:
        if logger:
            logger.warning("[safe_invoke] obj 또는 method_name이 None입니다")
        return

    #  C++ 위젯 객체가 삭제되었는지(is null) 사전 확인
    try:
        # 파이썬 객체는 있지만 내부 C++ 객체가 날아간 경우 테스트
        if hasattr(obj, "metaObject") and obj.metaObject() is None:
            return
    except RuntimeError:
        return  # underlying C/C++ object has been deleted

    # 메인스레드면 직접 호출
    if QThread.currentThread() == QCoreApplication.instance().thread():
        try:
            func = getattr(obj, method_name)
            func(*args)
        except RuntimeError as e:
            if "has been deleted" not in str(e) and logger:
                logger.exception(
                    f"[safe_invoke-direct] RuntimeError in {method_name}: {e}"
                )
        except AttributeError as e:
            if logger:
                logger.debug(
                    f"[safe_invoke-direct] {method_name} 속성 없음(무시됨): {e}"
                )
        except Exception as e:
            if logger:
                logger.exception(f"[safe_invoke-direct] Error in {method_name}: {e}")
    else:
        if _depth.value >= 10:
            if logger:
                logger.error(
                    f"[safe_invoke-depth] invokeMethod depth overflow in {method_name}"
                )
            return

        _depth.value += 1
        try:
            qargs = []
            for arg in args:
                if isinstance(arg, str):
                    qargs.append(Q_ARG("QString", arg))
                elif isinstance(arg, int):
                    qargs.append(Q_ARG("int", arg))
                elif isinstance(arg, float):
                    qargs.append(Q_ARG("double", arg))
                elif isinstance(arg, bool):
                    qargs.append(Q_ARG("bool", arg))
                else:
                    # [Resilient Fallback] Type error instead of crash, use PyObject if unknown
                    qargs.append(Q_ARG("PyQt_PyObject", arg))

            # [Fix] Target the invoker for lambda-like method calls if obj/method_name are used but need cross-thread
            QMetaObject.invokeMethod(obj, method_name, Qt.QueuedConnection, *qargs)
        except RuntimeError as e:
            if "has been deleted" not in str(e) and logger:
                logger.exception(
                    f"[safe_invoke-queued] RuntimeError in {method_name}: {e}"
                )
        except Exception as e:
            if logger:
                logger.exception(f"[safe_invoke-queued] Error in {method_name}: {e}")
        finally:
            _depth.value -= 1
