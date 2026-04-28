import heapq
import inspect
import logging
import sys
import time
import pythoncom


sys.path.append("..")

import threading

from collections import defaultdict, deque
import datetime as dt
from threading import Event, Thread

import pandas as pd
import pandas.io.sql as pdsql
from PyQt5.QtCore import pyqtSlot, QTimer
from util.CommUtils import (
    get_db_connection,
    get_db_stats,
    isBeforeOpenTime,
    isOverCloseTime,
    updateSearchStock,
    check_buy_signal,
    get_linenumber,
    get_funcname,
    to_numeric_safe,
    AccountGuard,
)

from config.ai_settings import DOWN_MKT_BUY_YN, ORDER_INTERVAL, SYS_ID, 거래환경
from config.telegram_setting import ToTelegram
from dto.CPortStock import CPortStock
from SQL.sql import (
    GET_REAL_PRICE_INFO_STOCK,
    PORTFOLIO_DELETE,
    PORTFOLIO_DELETE_BY_KEY,
    REPLACE_포트폴리오,
    SYNC_포트폴리오,
    UPDATE_매수후고가,
)

# from strategy.core.TradingComm import *
from util.Utils32 import safe_invoke

# import datetime as dt  #  명확한 모듈 네임스페이스 확보
# from datetime import timedelta
from xing.XAQuaries import t1101
from xing.XAReals import K3_, S3_
# from util.async_executor import AsyncExecutor

UTILIZATION_LIMIT = 0.95  # 90% 이상이면 등록 제한
logger = None  # 초기값


def set_logger(external_logger):
    global logger
    logger = external_logger


class OrderCoordinator:
    _lock = threading.Lock()
    #  Per-Ticker Throttle (중복 체결 억제)
    _ticket_last_order_time = {}
    _pending_order_amt = 0

    @classmethod
    def can_order(cls, ticker, interval=10):
        """종목별 간격 제한 확인 (중복 체결 억제)"""
        current_time = time.time()
        with cls._lock:
            last_time = cls._ticket_last_order_time.get(ticker, 0)
            if current_time - last_time < interval:
                return False
            cls._ticket_last_order_time[ticker] = current_time
        return True

    @classmethod
    def register_order_sent(cls, amount):
        """주문 즉시 가상 잔고 차감"""
        cls._pending_order_amt += amount

    @classmethod
    def get_pending_amount(cls):
        """가상(미체결) 주문 금액 조회 (API 연동)"""
        with cls._lock:
            return cls._pending_order_amt

    @classmethod
    def reconcile(cls, amount):
        """[Phase 9] API 실시간 미체결 데이터와 내부 상태 동기화 (Source of Truth)"""
        with cls._lock:
            cls._pending_order_amt = amount

    @classmethod
    def sync_with_account(cls, buy_amt):
        """
        체결이 완료되어 매입금액에 반영되면 가상 잔고를 차감.
        """
        cls._pending_order_amt -= buy_amt


# 글로벌 코디네이터 인스턴스 (모든 Goose가 공유)
coordinator = OrderCoordinator()


class SyncAccInfoCoordinator:
    _lock = threading.Lock()
    _last_sync_time = 0

    @classmethod
    def can_sync(cls, interval=1):
        """1초 간격 제한 확인"""
        current_time = time.time()
        if current_time - cls._last_sync_time < interval:
            return False
        return True

    @classmethod
    def register_sync_sent(cls):
        """sync 즉시 시간 기록"""
        cls._last_sync_time = time.time()


# 글로벌 코디네이터 인스턴스 (모든 Goose가 공유)
syncAccCoordinator = SyncAccInfoCoordinator()


class BaseGoose:
    # [PHASE 7] _tsg_cache removed - moved to 64-bit Brain

    # 🔹 전역 asyncio 이벤트 루프 (AsyncExecutor로 대체됨)
    # _async_loop = None
    # _async_loop_thread = None
    # _async_loop_lock = threading.Lock()

    # @classmethod
    # def _ensure_async_loop(cls):
    #     return AsyncExecutor.ensure_loop()

    @classmethod
    def instance(self):
        pass

    def __init__(self, Name, UUID):
        self.매도 = 1
        self.매수 = 2
        self.지정가 = "00"
        self.시장가 = "03"
        self.조건없음 = "0"
        self.조건IOC = "1"
        self.조건FOK = "2"

        self.신용거래코드 = "000"

        self.Name = Name
        self.UUID = UUID
        self.DATABASE = None
        # 매도조건_점진적적용 = True
        self.매도조건_점진적적용 = True
        self.매도조건_적용지연시간 = 30  # 분 단위 (30분 후부터 적용)

        # 🔹 실시간 객체 관리용 딕셔너리 초기화 (AttributeError 방지)
        self.advs_realdata_objs = {}
        self.advs_orderbook_objs = {}
        self.zmq_pub = None  # Pre-initialize to prevent AttributeError

        # 🔹 비동기 신호 처리용 워커 초기화
        self.signal_worker = None

        # [Global Coordinator Access]
        self.coordinator = coordinator
        self.syncAccCoordinator = syncAccCoordinator

        # [Concurrency & State] Initialize primitives here to prevent AttributeError before Run
        self.recv_realdata_queue = deque(maxlen=10000)
        self.recv_realdata_queue_event = Event()
        self.recv_realdata_queue_lock = threading.Lock()
        self.recv_realdata_recpt_time = defaultdict(float)
        self.advs_realdata_mng_queue = []
        self.advs_realdata_mng_queue_event = Event()
        self.advs_realdata_mng_queue_lock = threading.Lock()
        self.advs_realdata_mng_set = {"register": set(), "unregister": set()}
        self.recv_lock = threading.Lock()

        추정순자산 = int(
            self.parent.dialog["계좌정보조회"].dfAccSumInfo["추정순자산"].values[0]
        )
        self.parent.account_guard = AccountGuard(추정순자산)

        # [ZMQ] Reactor Init (Shared Publisher & Topic Setup)
        self._ensure_zmq_pub()

    def _publish_order_result(self, ticker, result_type, detail, **kwargs):
        """
        [Sync] Publish Order Result to 64-bit Brain
        result_type: 'SUCCESS' (Accept/Fill) or 'FAIL' (Reject/Error)
        """
        _zmq_pub = getattr(self, "zmq_pub", None)
        if not _zmq_pub:
            return

        try:
            payload = {
                "ticker": ticker,
                "result": result_type,
                "detail": detail,
                "timestamp": dt.datetime.now().strftime("%H:%M:%S.%f"),
            }
            # Merge extra data (price, qty, etc.)
            if kwargs:
                payload.update(kwargs)

            # TOPIC_ORDER_RESULT = b"ORD_RES"
            _zmq_pub.publish_data(self.TOPIC_ORDER_RESULT, payload)
            logger.info(
                f"📡 [ZMQ] Published ORDER_RESULT: {ticker} {result_type} {detail} {kwargs}"
            )
        except Exception as e:
            logger.error(f"ZMQ Publish Error: {e}")

    # [PHASE 7] ZMQ Router - 64-bit Brain Command Receiver

    @pyqtSlot(dict)
    def receive_external_signal(self, payload: dict):
        """
        [Step 3.3] ZMQReceiverThread로부터 수신된 시그널 처리 슬롯
        - 64-bit 서버에서 발송된 SignalPayload 구조체를 32-bit 내부 규격으로 변환하여 하달
        """
        try:
            ticker = payload.get("ticker")
            if not ticker:
                return

            # SignalPayload(64bit) -> Internal Command(32bit) 매핑
            # 32-bit 엔진은 'command' 키를 대문자로 인식함
            command = str(payload.get("order_type", "BUY")).upper()
            
            # [Indirection] 기존 _on_zmq_command 로직을 그대로 재활용하여 무결성 유지
            # payload를 직접 전달하되, 기존 logic이 기대하는 'command' 필드를 보정하여 전달
            internal_data = payload.copy()
            internal_data["command"] = command
            
            logger.info(f"[{self.Name}] 📥 External Signal Received: {ticker} {command} ({payload.get('reason', 'N/A')})")
            
            # 메인 스레드 집행 보장 (safe_invoke는 이미 이 슬롯이 호출된 시점에 메인 스레드일 가능성이 높으나 방어적으로 사용 가능)
            # 하지만 QThread 시그널-슬롯 연결이 메인 스레드로 마샬링하므로 직접 호출해도 무방
            self._on_zmq_command(internal_data)
            
        except Exception as e:
            if logger:
                logger.error(f"[{self.Name}] receive_external_signal error: {e}")

    def _on_zmq_command(self, data):
        """Dispatcher for ZMQ commands (called from Main Logic via safe_invoke).
        NOTE: This method is ALREADY running on the Qt main thread via safe_invoke
        from main_window_logic._on_zmq_command_received. Do NOT re-queue via safe_invoke.
        """
        command = data.get("command")
        ticker = data.get("ticker", "?")
        logger.info(
            f"[{self.__class__.__name__}] 📥 Received ZMQ CMD: {command} for {ticker}"
        )
        logger.debug(f"[{self.__class__.__name__}] 📦 Full Payload: {data}")
        if command in ["BUY", "SELL"]:
            try:
                if hasattr(self, "smart_executor") and self.smart_executor is not None:
                    logger.info(
                        f"[{self.__class__.__name__}] ▶️ Routing {ticker} to SmartExecutionManager"
                    )
                    self.smart_executor.execute_smart_order(data)
                else:
                    logger.info(
                        f"[{self.__class__.__name__}] ▶️ Routing {ticker} to direct handler (no SmartExecutor)"
                    )
                    if command == "BUY":
                        self._handle_zmq_buy(data)
                    else:
                        self._handle_zmq_sell(data)
            except Exception as e:
                logger.error(
                    f"❌ [{self.__class__.__name__}] _on_zmq_command execution error for {ticker}: {e}",
                    exc_info=True,
                )
        else:
            logger.warning(
                f"[{self.__class__.__name__}] Unknown ZMQ command: {command}"
            )

    def _ensure_lock_controller(self):
        """직렬화(Pickle) 과정에서 유실되는 lock_controller 동적 복구 (Crash 방어)"""
        if getattr(self, "lock_controller", None) is None:
            from gooses.lock_controllers import LockController

            self.lock_controller = LockController(default_timeout_seconds=120)
            logger.warning(
                f"[{self.Name}] ⚠️ lock_controller dynamically restored to prevent Crash."
            )

    def _handle_zmq_buy(self, data):
        """[Reflex] Buy with instant order-flow check"""
        self._ensure_lock_controller()
        self.process_buy_orders(data)

    def _handle_zmq_sell(self, data):
        """[Reflex] Sell immediately (survival first)"""
        self._ensure_lock_controller()
        self.process_sell_orders(data)

    def __getstate__(self):  # pickle 직렬화 못하는 객체 제거 ( Hardening)
        state = self.__dict__.copy()

        # 1. Aggressive unpickleable detector (COM, PyQt, ZMQ, Threading)
        def _is_unpickleable(v):
            if v is None:
                return False

            # Case 1: Known problematic type strings
            v_type_str = str(type(v))
            unpickleable_types = [
                "PyIDispatch",
                "CDispatch",
                "CoClass",
                "PyIUnknown",
                "COMObject",
                "Dispatch",
                "PyQt5",
                "zmq",
                "threading",
                "_Trio",
            ]
            if any(x in v_type_str for x in unpickleable_types):
                return True

            # Case 2: Heuristic detection for COM/ActiveX (most reliable)
            if hasattr(v, "_oleobj_") or hasattr(v, "_dispatch_"):
                return True

            # Case 3: Method/Function/Lambda references (cannot be pickled normally inside goose)
            if callable(v) and not hasattr(v, "__name__"):  # likely a lambda or partial
                return True

            return False

        # 2. Deep Recursive Cleaner (Dict, List, Tuple, Set)
        def _deep_clean(obj):
            if isinstance(obj, dict):
                return {
                    k: _deep_clean(v) for k, v in obj.items() if not _is_unpickleable(v)
                }
            elif isinstance(obj, list):
                return [_deep_clean(v) for v in obj if not _is_unpickleable(v)]
            elif isinstance(obj, tuple):
                return tuple(_deep_clean(v) for v in obj if not _is_unpickleable(v))
            elif isinstance(obj, set):
                return {_deep_clean(v) for v in obj if not _is_unpickleable(v)}
            elif _is_unpickleable(obj):
                return None
            return obj

        # 3. Comprehensive Blacklist (Top-level attributes)
        # This prevents even looking at these heavy-weight or broken-by-design objects
        blacklist = [
            "parent",
            "timer_realdata",
            "clock",
            "local_guard_levels",
            "ticks_cache",
            "XQ_t1857",
            "QA_CSPAT00600",
            "XR_S3_",
            "XR_K3_",
            "XR_SC1",
            "XQ_t0424",
            "XR_SC0",
            "XR_SC2",
            "XR_SC3",
            "XR_SC4",
            "XQ_t1101",
            "XQ_t8430",
            "XQ_t8436",
            "advs_realdata_objs",
            "advs_orderbook_objs",
            "query_objs",
            "data_fetchers",
            "recv_realdata_queue",
            "advs_realdata_mng_queue",
            "batSellQ",
            "recv_realdata_queue_event",
            "recv_realdata_queue_lock",
            "batSellQ_lock",
            "advs_realdata_mng_queue_event",
            "advs_realdata_mng_queue_lock",
            "recv_lock",
            "goose_creation_mutex",
            "recv_realdata_worker_thread",
            "advs_realdata_worker_thread",
            "signal_worker",
            "_async_loop_thread",
            "lock_controller",
            "split_order_controller",
            "red_card_controller",
            "portfolioManager",
            "tr_handlers",
            "smart_executor",
            "zmq_pub",
            "zmq_push",
            "timers",
            "coordinator",
            "syncAccCoordinator",
            "make_json_safe",
        ]

        for key in blacklist:
            if key in state:
                state[key] = None

        # 4. Apply Final Deep Sanitize
        clean_state = {}
        for k, v in state.items():
            if _is_unpickleable(v):
                continue
            try:
                clean_state[k] = _deep_clean(v)
            except Exception:
                clean_state[k] = None  # Bulletproof fail-safe

        return clean_state

    def __setstate__(self, state):
        self.__dict__.update(state)
        #  Restore all transients via objectInit
        self.objectInit()

    def set_parent(self, parent):
        self.parent = parent

    def set_database(self, database):
        self.DATABASE = database

    def set_secret(self, 계좌번호="계좌번호", 비밀번호="비밀번호"):
        self.계좌번호 = 계좌번호.trim()
        self.비밀번호 = 비밀번호.trim()

    def modal(self, parent):
        pass

    def getstatus(self):
        result = []
        return [
            self.__class__.__name__,
            self.Name,
            self.UUID,
            self.running,
            len(self.portfolio),
        ]

    def 초기조건(self):
        pass

    def Lay(self, flag=True, parent=None):
        pass

    def check_goose_worker_status(self):
        # logger.info(f"[{self.__class__.__name__}] recv_realdata_worker_thread Alive: {self.recv_realdata_worker_thread.is_alive()}")
        logger.info(
            f"[{self.__class__.__name__}] recv_realdata_queue len: {len(self.recv_realdata_queue)}"
        )
        # logger.info(f"[{self.__class__.__name__}] advs_realdata_worker_thread Alive: {self.advs_realdata_worker_thread.is_alive()}")
        logger.info(
            f"[{self.__class__.__name__}] advs_realdata_mng_queue len : {len(self.advs_realdata_mng_queue)}, {self.advs_realdata_mng_queue}"
        )
        logger.info(
            f"[{self.__class__.__name__}] self.portfolio len : {len(self.portfolio)} : advs_realdata_objs  len : {len(self.advs_realdata_objs)}, {self.advs_realdata_objs}"
        )
        if self.__class__.__name__ == "GuardianGoose":
            logger.info(
                f"[{self.parent.__class__.__name__}].gooseTradeExecutor.utilization(): {self.parent.gooseTradeExecutor.limited_queue.qsize(), self.parent.gooseTradeExecutor.limited_queue.maxsize} : {self.parent.gooseTradeExecutor.utilization()}"
            )
            logger.info(
                f"[{self.__class__.__name__}] get_db_stats() : {get_db_stats()}"
            )

    def parse_time_range(self, time_str):
        """'09:00:00-15:20:00' 같은 문자열을 튜플 리스트로 변환하는 함수"""
        time_ranges = []
        for time_range in time_str.split(","):
            start_end = time_range.strip().split("-")
            if len(start_end) == 2:
                time_ranges.append((start_end[0], start_end[1]))
        return time_ranges

    def queryInit(self):
        logger.info(f"[{self.__class__.__name__}] ==------->>> queryInit 호출~~~!!!")
        if hasattr(self, "XQ_t1857") and self.XQ_t1857 is not None:
            self.XQ_t1857.RemoveService()
            self.XQ_t1857 = None
            logger.info(
                f"[{self.__class__.__name__}] ==------->>> self.XQ_t1857 = None"
            )
        if hasattr(self, "QA_CSPAT00600") and self.QA_CSPAT00600 is not None:
            self.QA_CSPAT00600 = None
            logger.info(
                f"[{self.__class__.__name__}] ==------->>> self.QA_CSPAT00600 = None"
            )

        if hasattr(self, "clock") and self.clock is not None:
            try:
                self.clock.stop()
            except Exception:
                pass
            finally:
                self.clock = None

        try:
            if hasattr(self, "XR_S3_") and self.XR_S3_ != None:
                self.XR_S3_.UnadviseRealData()
                logger.info(
                    f"[{self.__class__.__name__}] ==------->>> self.XR_S3_.UnadviseRealData()"
                )
        except Exception:
            pass
        finally:
            # self.XR_S3_ = None
            pass

        try:
            if hasattr(self, "XR_K3_") and self.XR_K3_ != None:
                self.XR_K3_.UnadviseRealData()
                logger.info(
                    f"[{self.__class__.__name__}] ==------->>> self.XR_K3_.UnadviseRealData()"
                )
        except Exception:
            pass
        finally:
            # self.XR_K3_ = None
            pass

        # if self.XR_SC0 != None:
        #     self.XR_SC0.UnadviseRealData()

        try:
            if hasattr(self, "XR_SC1") and self.XR_SC1 != None:
                self.XR_SC1.UnadviseRealData()
                logger.info(
                    f"[{self.__class__.__name__}] ==------->>> self.XR_SC1.UnadviseRealData()"
                )
        except Exception:
            pass
        finally:
            # self.XR_SC1 = None
            pass

        # if self.XR_SC2 != None:
        #     self.XR_SC2.UnadviseRealData()
        # if self.XR_SC3 != None:
        #     self.XR_SC3.UnadviseRealData()
        # if self.XR_SC4 != None:
        #     self.XR_SC4.UnadviseRealData()

        # self.parent = None
        # self.XR_SC0 = None # 접수
        # self.XR_SC2 = None # 정정
        # self.XR_SC3 = None # 취소
        # self.XR_SC4 = None # 거부
        self.XQ_t0424 = None  # 계좌정보

    def objectInit(self):
        logger.info(f"[{self.__class__.__name__}] objectInit 호출~~~!!!")
        self._start_time = (
            time.time()
        )  # Track initialization time for noise suppression

        if (
            getattr(self, "recv_realdata_worker_thread", None)
            and self.recv_realdata_worker_thread.is_alive()
        ):
            # Non-blocking check or join if necessary, but usually we just want to ensure state is clean
            pass

        if (
            getattr(self, "advs_realdata_worker_thread", None)
            and self.advs_realdata_worker_thread.is_alive()
        ):
            pass

        self.batSellQ = None
        self.lock_controller = None
        self.split_order_controller = None
        self.kospi_codes = []
        self.kosdaq_codes = []

        #  Concurrency & State Primitives
        # 기존 스레드나 COM 객체가 고아(Orphan)가 되어 앱이 튕기는 현상(Crash) 방지.
        # 이미 할당된 객체는 덮어쓰지 않고 재사용하도록 보호막(hasattr) 적용.

        if not hasattr(self, "recv_realdata_queue") or self.recv_realdata_queue is None:
            self.recv_realdata_queue = deque(maxlen=10000)
        if (
            not hasattr(self, "recv_realdata_queue_event")
            or self.recv_realdata_queue_event is None
        ):
            self.recv_realdata_queue_event = Event()
        if (
            not hasattr(self, "recv_realdata_queue_lock")
            or self.recv_realdata_queue_lock is None
        ):
            self.recv_realdata_queue_lock = threading.Lock()
        if (
            not hasattr(self, "recv_realdata_recpt_time")
            or self.recv_realdata_recpt_time is None
        ):
            self.recv_realdata_recpt_time = defaultdict(float)

        # [CRITICAL] 이 변수를 무조건 {}로 초기화하면 메모리 릭 및 C++ Access Violation 강제종료 발생!
        if not hasattr(self, "advs_realdata_objs") or self.advs_realdata_objs is None:
            self.advs_realdata_objs = {}

        if (
            not hasattr(self, "advs_realdata_mng_queue")
            or self.advs_realdata_mng_queue is None
        ):
            self.advs_realdata_mng_queue = []
        if (
            not hasattr(self, "advs_realdata_mng_queue_event")
            or self.advs_realdata_mng_queue_event is None
        ):
            self.advs_realdata_mng_queue_event = Event()
        if (
            not hasattr(self, "advs_realdata_mng_queue_lock")
            or self.advs_realdata_mng_queue_lock is None
        ):
            self.advs_realdata_mng_queue_lock = threading.Lock()
        if (
            not hasattr(self, "advs_realdata_mng_set")
            or self.advs_realdata_mng_set is None
        ):
            self.advs_realdata_mng_set = {"register": set(), "unregister": set()}

        if not hasattr(self, "tr_handlers") or self.tr_handlers is None:
            self.tr_handlers = {}
        if not hasattr(self, "recv_lock") or self.recv_lock is None:
            self.recv_lock = threading.Lock()

        # [Phase 3] Smart Execution Manager For 32-bit Iceberg (v2.0 Sync)
        self._ensure_zmq_pub()
        self._ensure_smart_executor()

        if isBeforeOpenTime() or isOverCloseTime():
            self.금일매도종목 = []

        #  State Caching for Autonomous Execution
        if not hasattr(self, "local_guard_levels") or self.local_guard_levels is None:
            self.local_guard_levels = {}  # 64-bit brain targets
        if not hasattr(self, "ticks_cache") or self.ticks_cache is None:
            self.ticks_cache = {}  # Latest tick prices for Price Escape monitoring

    def _ensure_smart_executor(self):
        """Ensures SmartExecutionManager is initialized and hooked."""
        if not hasattr(self, "smart_executor") or self.smart_executor is None:
            try:
                from managers.smart_execution_manager import SmartExecutionManager

                self.smart_executor = SmartExecutionManager(self)

                # Register Orderbook Handlers for OBI pacing
                if not hasattr(self, "tr_handlers") or self.tr_handlers is None:
                    self.tr_handlers = {}
                self.tr_handlers["H1_"] = self.smart_executor.update_tick
                self.tr_handlers["HA_"] = self.smart_executor.update_tick
                logger.info(
                    f"[{self.__class__.__name__}] SmartExecutionManager Init & TR Handlers OK"
                )
            except Exception as e:
                logger.error(
                    f"[{self.__class__.__name__}] Failed to init SmartExecutionManager: {e}"
                )

    def _ensure_zmq_pub(self):
        """Phoenix-Socket 기반 싱글톤 퍼블리셔 복구 및 토픽 설정"""
        try:
            # 1. 속성 존재 여부 및 유효성 체크
            if hasattr(self, "zmq_pub") and self.zmq_pub is not None:
                # [Optimization] 소켓이 살아있는지 publish_data의 None-reinit 로직을 신뢰함
                return

            from util.zmq_manager import (
                get_shared_publisher,
                TOPIC_TICK,
                TOPIC_ORDER_RESULT,
                make_json_safe,
            )

            # 2. Shared Publisher Instance (ZMQPublisher 클래스 인스턴스)
            # [Phoenix-Socket] 내부에서 RLock 및 Auto-Reinit을 처리함
            self.zmq_pub = get_shared_publisher()

            if self.zmq_pub is None:
                logger.error(
                    f"[{self.__class__.__name__}] Failed to get shared ZMQ publisher."
                )

            self.TOPIC_TICK = TOPIC_TICK
            self.TOPIC_ORDER_RESULT = TOPIC_ORDER_RESULT
            self.make_json_safe = make_json_safe

            logger.info(
                f"[{self.__class__.__name__}] 📡 Phoenix-ZMQ Shared Publisher Restored"
            )
        except Exception as e:
            logger.error(f"[{self.__class__.__name__}] ZMQ Restoring Error: {e}")
            self.zmq_pub = None

    def query_buy(
        self,
        종목코드,
        매수수량,
        현재가,
        order_price_type=None,
        on_done=None,
        order_log_data=None,
    ):
        #  Pickle 복원 후 또는 초기화 지연 시 COM 객체 분실 방어
        if not hasattr(self, "QA_CSPAT00600") or self.QA_CSPAT00600 is None:
            try:
                from xing.XAQuaries import CSPAT00600

                self.QA_CSPAT00600 = CSPAT00600(parent=self)
                logger.info(
                    f"[{self.__class__.__name__}] On-demand initialized QA_CSPAT00600 for Buy"
                )
            except Exception as e:
                logger.error(f"Failed to auto-init QA_CSPAT00600 for Buy: {e}")

        _종목코드 = 종목코드
        _매수수량 = int(float(매수수량))
        _현재가 = 현재가
        _order_log_data = order_log_data

        #  Auto-Metadata Generation (If missing)
        if _order_log_data is None:
            _order_log_data = {
                "market_code": "S3_" if _종목코드 in self.kospi_codes else "K3_",
                "market_info": None,
                "종목코드": _종목코드,
                "종목명": "",
                "거래유형": "매수(Auto)",
                "매수가": _현재가,
                "매도가": _현재가,
                "매도수량": _매수수량,
                "매수일": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "평가손익": 0,
            }

        # [Atomic Execution] Record INTENT before async shot
        try:
            self.saveOrderHist(**_order_log_data)
            logger.info(f"✅ [Order-Hist-Saved] {_종목코드} (BUY) 시작 기록 완료")
        except Exception as e:
            logger.error(f"⚠️ [Order-Hist-Error] {_종목코드} (BUY) 기록 실패: {e}")

        @pyqtSlot()
        def do_buy():
            try:
                import time

                #  Retry mechanism for Communication Failures (-16)
                max_retries = 3
                retry_count = 0
                nRet = -16

                while retry_count < max_retries:
                    if not hasattr(BaseGoose, "_last_api_call_time"):
                        BaseGoose._last_api_call_time = 0.0

                    #  Shared API Throttling across Buy/Sell (XingAPI 250ms TPS Guard)
                    elapsed = time.time() - BaseGoose._last_api_call_time
                    wait_time = 0.25 + (retry_count * 0.2)  # 재시도 시 점진적 대기 추가
                    if elapsed < wait_time:
                        time.sleep(wait_time - elapsed)

                    BaseGoose._last_api_call_time = time.time()

                    # [Explicit Market Guard] If Market Price (03), price must be empty string
                    _매수방법 = (
                        order_price_type
                        if order_price_type is not None
                        else self.매수방법
                    )
                    try:
                        _주문가 = (
                            str(int(float(_현재가))) if str(_매수방법) != "03" else ""
                        )
                        _주문수량_str = str(int(float(_매수수량)))
                    except (ValueError, TypeError):
                        _주문가 = "" if str(_매수방법) == "03" else "0"
                        _주문수량_str = "0"
                        logger.error(
                            f"❌ [API Error-Buy] Invalid numeric format: {_매수수량}, {_현재가}"
                        )

                    nRet = self.QA_CSPAT00600.Query(
                        계좌번호=self.계좌번호,
                        입력비밀번호=self.비밀번호,
                        종목번호=_종목코드,
                        주문수량=_주문수량_str,
                        주문가=_주문가,
                        매매구분=self.매수,
                        호가유형코드=_매수방법,
                        신용거래코드=self.신용거래코드,
                        주문조건구분=self.조건없음,
                    )

                    if nRet is not None and nRet >= 0:
                        logger.info(
                            f"🚀 [Order Sent] {_종목코드} | 수량: {_매수수량} | 금액: {_매수수량 * _현재가:,.0f} (Handle: {nRet}, Retries: {retry_count})"
                        )
                        break
                    else:
                        retry_count += 1
                        logger.warning(
                            f"⚠️ [API Retry-Buy] {_종목코드} Failed with nRet: {nRet}. Retrying... ({retry_count}/{max_retries})"
                        )
                        if retry_count >= max_retries:
                            break

                if nRet is not None and nRet < 0:
                    logger.critical(
                        f"💀 [API Final-Fail-Buy] {_종목코드} failed after {retry_count} retries with code {nRet}"
                    )
                    # 64-bit 브레인에 실패 보고 (비정상 종료 상황)
                    if hasattr(self, "_publish_order_result"):
                        self._publish_order_result(
                            _종목코드,
                            "FAIL",
                            f"API Error {nRet} after retries",
                            side="buy",
                        )

            except Exception as e:
                logger.error(
                    f"❌ [BaseGoose.query_buy.do_buy] Fatal Error: {e}", exc_info=True
                )
            finally:
                if on_done:
                    on_done(nRet)

        safe_invoke(None, None, do_buy)

    def query_sell(
        self,
        종목코드,
        매도수량,
        현재가,
        order_type="00",
        on_done=None,
        order_log_data=None,
    ):
        #  Pickle 복원 후 또는 초기화 지연 시 COM 객체 분실 방어
        if not hasattr(self, "QA_CSPAT00600") or self.QA_CSPAT00600 is None:
            try:
                from xing.XAQuaries import CSPAT00600

                self.QA_CSPAT00600 = CSPAT00600(parent=self)
                logger.info(
                    f"[{self.__class__.__name__}] On-demand initialized QA_CSPAT00600 for Sell"
                )
            except Exception as e:
                logger.error(f"Failed to auto-init QA_CSPAT00600 for Sell: {e}")

        _종목코드 = 종목코드
        _매도수량 = int(float(매도수량))
        _현재가 = 현재가  #  do_sell 슬롯 내 NameError 방지
        _order_log_data = order_log_data

        #  Auto-Metadata Generation (If missing)
        if _order_log_data is None:
            _order_log_data = {
                "market_code": "S3_" if _종목코드 in self.kospi_codes else "K3_",
                "market_info": None,
                "종목코드": _종목코드,
                "종목명": "",
                "거래유형": f"매도({order_type}/Auto)",
                "매수가": _현재가,
                "매도가": _현재가,
                "매도수량": _매도수량,
                "매수일": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "평가손익": 0,
            }

        # [Atomic Execution] Record INTENT before async shot
        try:
            self.saveOrderHist(**_order_log_data)
            logger.info(f"✅ [Order-Hist-Saved] {_종목코드} (SELL) 시작 기록 완료")
        except Exception as e:
            logger.error(f"⚠️ [Order-Hist-Error] {_종목코드} (SELL) 기록 실패: {e}")

        # 🛡️  시장가(03) 강제 매핑 및 가격 0원 처리
        if order_type == "03" or str(self.매도방법) == "03":
            _호가구분 = "03"  # 시장가 코드
            _주문가 = ""  # 시장가 주문 시 가격은 공란이어야 미체결 보장
        else:
            _호가구분 = "00"  # 지정가
            try:
                _주문가 = str(int(float(현재가)))
            except (ValueError, TypeError):
                _주문가 = "0"

        @pyqtSlot()
        def do_sell():
            try:
                import time

                #  Retry mechanism for Communication Failures (-16)
                max_retries = 3
                retry_count = 0
                nRet = -16

                while retry_count < max_retries:
                    if not hasattr(BaseGoose, "_last_api_call_time"):
                        BaseGoose._last_api_call_time = 0.0

                    #  Shared API Throttling across Buy/Sell (XingAPI 250ms TPS Guard)
                    elapsed = time.time() - BaseGoose._last_api_call_time
                    wait_time = 0.25 + (retry_count * 0.2)  # 재시도 시 점진적 대기 추가
                    if elapsed < wait_time:
                        time.sleep(wait_time - elapsed)

                    BaseGoose._last_api_call_time = time.time()

                    nRet = self.QA_CSPAT00600.Query(
                        계좌번호=self.계좌번호,
                        입력비밀번호=self.비밀번호,
                        종목번호=_종목코드,
                        주문수량=str(_매도수량),
                        주문가=_주문가,
                        매매구분=self.매도,
                        호가유형코드=_호가구분,
                        신용거래코드=self.신용거래코드,
                        주문조건구분=self.조건없음,
                    )

                    if nRet is not None and nRet >= 0:
                        logger.info(
                            f"📤 [Market Sell] {_종목코드} {_매도수량}주 Executed (Type:{_호가구분}, Handle:{nRet}, Retries: {retry_count})"
                        )
                        break
                    else:
                        retry_count += 1
                        logger.warning(
                            f"⚠️ [API Retry-Sell] {_종목코드} Failed with nRet: {nRet}. Retrying... ({retry_count}/{max_retries})"
                        )
                        if retry_count >= max_retries:
                            break

                if nRet is not None and nRet < 0:
                    logger.critical(
                        f"💀 [API Final-Fail-Sell] {_종목코드} failed after {retry_count} retries with code {nRet}"
                    )
                    # 64-bit 브레인에 실패 보고
                    if hasattr(self, "_publish_order_result"):
                        self._publish_order_result(
                            _종목코드,
                            "FAIL",
                            f"API Error {nRet} after retries",
                            side="sell",
                        )

            except Exception as e:
                logger.error(
                    f"❌ [BaseGoose.query_sell.do_sell] Fatal Error: {e}", exc_info=True
                )
            finally:
                if on_done:
                    on_done(nRet)

        safe_invoke(None, None, do_sell)

    # [PHASE 7] maybe_cleanup_tsg_cache removed

    # 🛡️  32bit Autonomous Hard-Stop (자율 생존 패닉 버튼)
    # ZMQ 통신이 끊기거나 64bit가 뻗어도, 치명적인 손실선 도달 시 32bit가 스스로 시장가 투매

    def check_local_guard(self, ticker: str, current_price: float):
        """
         32-bit Autonomous Local Guard
        64-bit의 ZMQ 응답 대기 없이 틱 데이터를 기반으로 자율 익절/손절을 집행합니다.
        """
        if not hasattr(self, "portfolio") or ticker not in self.portfolio:
            return

        position = self.portfolio[ticker]
        entry_price = float(getattr(position, "매수가", 0.0))
        qty = int(getattr(position, "수량", 0))

        if entry_price <= 0 or qty <= 0:
            return

        if not hasattr(self, "panic_sold_tickers"):
            self.panic_sold_tickers = set()

        if ticker in self.panic_sold_tickers:
            return

        # 1. 캐싱된 64-bit 동적 레벨 로드 (없으면 기본 방어선 -7%만 작동)
        guard_levels = getattr(self, "local_guard_levels", {}).get(ticker, {})
        dynamic_sl = guard_levels.get("stop_loss", 0.0)
        dynamic_tp = guard_levels.get("take_profit1", 0.0)

        # 2. 하드스탑(최후의 보루)과 브레인 손절선 중 더 높은(안전한) 가격을 방어선으로 채택
        hard_stop_price = entry_price * 0.93
        cut_off_price = (
            max(dynamic_sl, hard_stop_price) if dynamic_sl > 0 else hard_stop_price
        )

        # 3. 트리거 검사 (Tick 단위 정밀 타격)
        trigger_reason = ""
        if current_price <= cut_off_price:
            trigger_reason = f"손절/Trailing-Stop 붕괴 (컷오프: {cut_off_price:,.0f})"
        elif dynamic_tp > 0 and current_price >= dynamic_tp:
            trigger_reason = f"Take-Profit 목표가 도달 (목표가: {dynamic_tp:,.0f})"

        # 4. 자율 청산 집행 및 64-bit 통보
        if trigger_reason:
            self.panic_sold_tickers.add(ticker)
            logger.critical(
                f"🚨 [32bit Local Guard] {ticker} 자율 청산 발동! 사유: {trigger_reason}, 현재가: {current_price:,.0f}"
            )

            try:
                # 락 해제 및 증권사 다이렉트 매도 주문
                if hasattr(self.lock_controller, "unlock_sell"):
                    self.lock_controller.unlock_sell(ticker)

                self.query_sell(ticker, qty, current_price)

                # 64-bit 브레인으로 즉각 상태 동기화 통보 (상태 불일치 방지)
                if hasattr(self, "_publish_order_result"):
                    self._publish_order_result(
                        ticker,
                        "LOCAL_GUARD_SELL",
                        f"32bit Autonomous Guard Triggered at {current_price}",
                        qty=qty,
                        price=current_price,
                        side="sell",
                        type="FILL",
                    )
            except Exception as e:
                logger.error(f"❌ [32bit Local Guard] 투매 집행 중 치명적 에러: {e}")

    # 공통 실시간 데이터 처리 함수 (AIGoose/GuardianGoose에서 통합)
    # 모든 함수가 self.* 인스턴스 변수를 사용하므로 메모리 격리 보장

    def OnReceiveRealData(self, szTrCode, result):
        """실시간 데이터 수신 이벤트 핸들러"""
        # [DEBUG] Verify Event Firing
        # if szTrCode in ["K3_", "S3_"]:
        #     # Log unconditionally for verification (can be sampled later)
        #     logger.info(f"⚡ [BaseGoose] OnReceiveRealData Event Fired: {szTrCode}")

        with self.recv_realdata_queue_lock:
            self.recv_realdata_queue.append(
                {
                    "szTrCode": szTrCode,
                    "result": result,
                }
            )
            self.recv_realdata_queue_event.set()  # 워커 깨우기

    def recv_realdata_worker(self):
        """실시간 데이터 수신 워커 스레드"""
        try:
            logger.info(
                f"[{self.Name}] recv_realdata_worker 시작됨, 큐 크기: {len(self.recv_realdata_queue)}"
            )
            while self.running:
                self.recv_realdata_queue_event.wait(timeout=1.0)

                while True:
                    with self.recv_realdata_queue_lock:
                        if not self.recv_realdata_queue:
                            self.recv_realdata_queue_event.clear()
                            break
                        item = self.recv_realdata_queue.popleft()

                    try:
                        self.process_recv_realdata(item)
                    except Exception:
                        logger.exception(
                            f"[{self.Name}] 실시간 데이터 처리 중 오류 발생: {item}"
                        )
                    else:
                        logger.debug(
                            f"[{self.Name}] 처리 완료: {item} / 남은 큐: {len(self.recv_realdata_queue)}"
                        )
        except Exception as e:
            logger.exception(f"[{self.Name}] recv_realdata_worker 예외: {e}")
            ToTelegram(f"[{self.Name}] recv_realdata_worker 예외 발생: {e}")
        finally:
            logger.info(
                f"[{self.Name}] recv_realdata_worker 종료됨. 최종 큐 크기: {len(self.recv_realdata_queue)}"
            )

    def process_recv_realdata(self, item):
        """실시간 데이터 처리"""
        szTrCode = item["szTrCode"]
        result = item["result"]
        handler = self.tr_handlers.get(szTrCode)

        if handler:
            if szTrCode in ["K3_", "S3_", "H1_", "HA_"]:
                handler(szTrCode, result)
            else:
                handler(result)
        else:
            logger.debug(f"[{self.Name}] Unknown szTrCode: {szTrCode}")

    def start_advs_realdata_queue_handler(self, use_gui_timer=True):
        """실시간 데이터 큐 핸들러 시작"""
        if use_gui_timer:
            # [FIX] QTimer는 메인 스레드 이벤트 루프가 필요하므로 safe_invoke로 실행
            safe_invoke(None, None, self.setup_qtimer_for_advs_realdata)
        else:
            if (
                getattr(self, "advs_realdata_worker_thread", None)
                and self.advs_realdata_worker_thread.is_alive()
            ):
                logger.info(
                    f"[{self.Name}] advs_realdata_worker_thread 이미 실행 중, 시작 생략"
                )
            else:
                self.advs_realdata_worker_thread = Thread(
                    target=self.advs_realdata_worker, daemon=True
                )
                self.advs_realdata_worker_thread.start()

    def setup_qtimer_for_advs_realdata(self):
        """QTimer를 이용한 실시간 데이터 처리 설정 (메시지 펌핑 및 워커 스레드 시작)"""
        # 1. 큐 처리 워커 스레드 시작
        if not (
            getattr(self, "advs_realdata_worker_thread", None)
            and self.advs_realdata_worker_thread.is_alive()
        ):
            self.advs_realdata_worker_thread = Thread(
                target=self.advs_realdata_worker, daemon=True
            )
            self.advs_realdata_worker_thread.start()
            logger.info(f"[{self.Name}] advs_realdata_worker 스레드 시작됨")

        # 2. 메인 스레드 메시지 펌핑 타이머 설정
        if getattr(self, "timer_realdata", None) and self.timer_realdata.isActive():
            return  # 이미 타이머 실행 중이면 재설정 안 함

        self.timer_realdata = QTimer()
        self.timer_realdata.timeout.connect(self.pump_win32_messages)
        self.timer_realdata.start(100)  # 0.1초 간격으로 펌핑
        logger.info(f"[{self.Name}] Message Pump Timer 시작 (100ms)")

    def pump_win32_messages(self):
        """win32com 이벤트를 처리하기 위한 메시지 펌핑"""
        try:
            # [DEBUG] 5초마다 펌핑 로그 출력하여 동작 확인
            now = time.monotonic()
            if (
                not hasattr(self, "_last_pump_log")
                or now - getattr(self, "_last_pump_log", 0) > 5.0
            ):
                logger.debug(
                    f"[{self.Name}] ⚡ PumpWaitingMessages Executing... (Timer Active)"
                )
                self._last_pump_log = now

            pythoncom.PumpWaitingMessages()
        except Exception:
            pass

    def register_realdata(self, stock_cd, market_type):
        """실시간 종목 등록 요청 (메인스레드에서 실행 보장)."""
        # [PHASE 16.1 Stability Guard]
        if not self.running or self.advs_realdata_objs is None:
            logger.warning(
                f"[{self.Name}] [실시간등록스킵] {stock_cd} → Goose가 중단되었거나 상태가 유효하지 않음"
            )
            return

        if stock_cd in self.advs_realdata_objs:
            logger.debug(f"[{self.Name}] [중복등록무시] {stock_cd} → 이미 등록됨")
            return

        @pyqtSlot()
        def do_register():
            try:
                # 종목에 따라 K3_ 또는 S3_ 객체 생성
                # [DEBUG] Testing A prefix for S3_ (some APIs require this for real)
                # KOSDAQ uses J? KOSPI uses A? Or just A for all?
                # Usually XingAPI uses simple code. But let's try A.
                obj = (
                    K3_(parent=self, 식별자=self.UUID)
                    if market_type == 2
                    else S3_(parent=self, 식별자=self.UUID)
                )
                obj.connectRealSignal(self.OnReceiveRealData)

                # 실시간 데이터 등록
                obj.AdviseRealData(stock_cd)

                # [Phase 3] Orderbook subscription for OBI calculations
                from xing.XAReals import H1_, HA_

                orderbook_obj = (
                    HA_(parent=self, 식별자=self.UUID)
                    if market_type == 2
                    else H1_(parent=self, 식별자=self.UUID)
                )
                orderbook_obj.connectRealSignal(self.OnReceiveRealData)
                orderbook_obj.AdviseRealData(stock_cd)

                if getattr(self, "advs_orderbook_objs", None) is None:
                    self.advs_orderbook_objs = {}
                self.advs_orderbook_objs[stock_cd] = orderbook_obj
                logger.debug(
                    f"[{self.Name}] [Orderbook등록] {stock_cd} ({orderbook_obj.__class__.__name__})"
                )

                # [PHASE 16.1] Double check before assignment
                if self.advs_realdata_objs is not None:
                    self.advs_realdata_objs[stock_cd] = obj
                else:
                    logger.warning(
                        f"[{self.Name}] [실시간등록취소] {stock_cd} → advs_realdata_objs가 갑자기 None이 됨"
                    )
                    obj.UnadviseRealData()
                    return

                # 검증
                assert obj is not None, "실시간 객체가 등록되지 않았습니다."
                assert hasattr(obj.ActiveX_sink, "_real_callback"), (
                    "_real_callback 설정 실패"
                )

                logger.info(
                    f"[{self.Name}] [실시간등록완료(Main)] {stock_cd} → {obj.__class__.__name__}"
                )

            except Exception as e:
                logger.exception(f"[{self.Name}] [실시간등록오류] {stock_cd} → {e}")
                ToTelegram(f"[{self.Name}] [실시간등록오류] {stock_cd} → {e}")

                # 실패 시 객체 정리
                if "obj" in locals():
                    try:
                        obj.UnadviseRealData()
                    except Exception:
                        pass
                    del obj

        # 메인 스레드에서 실행
        safe_invoke(None, None, do_register)

    def unregister_all(self):
        # print(f"hasattr(self, 'advs_realdata_objs') : {hasattr(self, 'advs_realdata_objs')}")
        if not hasattr(self, "advs_realdata_objs") or self.advs_realdata_objs is None:
            return
        for stock_cd in list(self.advs_realdata_objs.keys()):
            self.unregister_realdata(stock_cd)

    def unregister_realdata(self, stock_cd):
        """실시간 데이터 수신 해제 요청 (메인스레드에서 안전하게 처리)."""

        if stock_cd not in self.advs_realdata_objs:
            logger.warning(
                f"[{self.Name}] [실시간해제] {stock_cd} → 등록되지 않아 무시됨"
            )
            return

        @pyqtSlot()
        def do_unregister():
            try:
                if self.advs_realdata_objs is None:
                    return

                # 1. Tick 객체 해제
                obj = self.advs_realdata_objs.pop(stock_cd, None)
                if obj:
                    try:
                        obj.UnadviseRealDataWithKey(stock_cd)
                        try:
                            obj.disconnect()
                        except Exception:
                            pass
                        if hasattr(obj, "clear"):
                            obj.clear()
                        logger.info(
                            f"[{self.__class__.__name__}] [do_unregister Tick 해제] {stock_cd} → {obj.__class__.__name__}"
                        )
                    except Exception as e:
                        logger.error(f"Tick Unadvise Error for {stock_cd}: {e}")
                else:
                    logger.warning(
                        f"[{self.Name}] [do_unregister 해제스킵] {stock_cd} → 등록되지 않음"
                    )

                # 2. [Phase 3] Orderbook 해제
                if getattr(self, "advs_orderbook_objs", None) is not None:
                    orderbook_obj = self.advs_orderbook_objs.pop(stock_cd, None)
                    if orderbook_obj:
                        try:
                            orderbook_obj.UnadviseRealDataWithKey(stock_cd)
                            logger.info(
                                f"[{self.__class__.__name__}] [Orderbook 해제완료] {stock_cd}"
                            )
                        except Exception as e:
                            logger.error(
                                f"Orderbook Unadvise Error for {stock_cd}: {e}"
                            )

                if "obj" in locals() and obj:
                    del obj

            except Exception as outer_e:
                logger.exception(
                    f"[{self.Name}] [unregister_realdata 예외] {stock_cd} → {outer_e}"
                )
                ToTelegram(
                    f"[{self.Name}] [unregister_realdata 예외] {stock_cd} → {outer_e}"
                )

        safe_invoke(None, None, do_unregister)

    def executeSearchRealData(self, lst):
        식별자, result = lst
        if 식별자 == self.XQ_t1857.식별자:
            try:
                code = result["종목코드"]
                flag = result["종목상태"]
                # 검색종목 저장
                self.saveSearchStock(code, 1 if self.Name == "AIGoose" else 2, 0)
                # self.executeAdviseRealData(code, flag)
                # self.executeUnadviseRealData(code, flag)#flag 'O'인경우만 작동

            except Exception as e:
                클래스이름 = self.__class__.__name__
                함수이름 = get_funcname()
                logger.error(
                    "%s-%s %s: %s" % (클래스이름, 함수이름, get_linenumber(), e)
                )
            finally:
                pass

    def executeAdviseRealData(self, code, flag):
        try:
            parent = getattr(self, "parent", None)
            if parent is None:
                logger.info(f"[{self.__class__.__name__}] 🚨 등록 중단: parent is None")
                return

            executor = getattr(parent, "gooseTradeExecutor", None)
            if executor is None:
                logger.info(
                    f"[{self.__class__.__name__}] 🚨 등록 중단: gooseTradeExecutor is None"
                )
                return

            executor_util = executor.utilization()
            if executor_util > UTILIZATION_LIMIT:
                logger.info(
                    f"[{self.__class__.__name__}] 🚨 등록 중단: Executor 사용률 {executor_util:.2%} 초과"
                )
                # self.saveSearchStock(code, 1 if self.Name == 'AIGoose' else 2, 0)
                return  # 너무 과부하 상태일 경우 등록 중단

            # 이미 등록된 경우 무시
            if (
                code in self.advs_realdata_objs
                or code in self.advs_realdata_mng_set["register"]
            ):
                self.saveSearchStock(
                    code, 1 if self.Name == "AIGoose" else 2, 2
                )  # 이미 매수된 건이 재 등록되어 들어온 경우 대비.
                logger.debug(f"[{self.Name}] {code} 등록 중복 요청 무시")
                return

            # [ Refined] Decouple Subscription from Portfolio Limit
            SUBSCRIPTION_LIMIT = 100  # XING API limit safe zone
            if len(self.advs_realdata_objs) >= SUBSCRIPTION_LIMIT:
                logger.warning(
                    f"[{self.Name}] 구독 제한({SUBSCRIPTION_LIMIT}) 초과로 {code} 등록 무시"
                )
                return

            # Remove previous strict locks that returned early on full portfolio
            # allow candidate monitoring up to SUBSCRIPTION_LIMIT

            # # 실제 등록된 종목 + 등록 대기 중인 종목 수 기준 제한
            # real_registered = len(self.advs_realdata_objs)
            # pending = len(self.advs_realdata_mng_set["register"])
            # if real_registered + pending >= self.포트폴리오수:
            #     if real_registered > self.포트폴리오수:
            #         self.executeUnadviseRealData(code, "O")
            #     else:
            #         logger.warning(
            #             f"[{self.Name}] 등록 제한 초과: {code} (등록 : {real_registered} + 대기 : {pending} ≥ 제한 : {self.포트폴리오수})"
            #         )
            #         # self.saveSearchStock(code, 1 if self.Name == 'AIGoose' else 2, 0)
            #         return

            #  NameError 방지를 위한 초기화 (기존 로직 복구)
            real_registered = len(self.advs_realdata_objs)
            pending = len(self.advs_realdata_mng_set["register"])

            if (
                code not in self.portfolio
                and code not in self.advs_realdata_objs
                and code not in self.advs_realdata_mng_set["register"]
                and len(self.portfolio) < self.포트폴리오수
            ):
                market_type = None
                if code in self.kospi_codes and flag in ["N", "R"]:
                    # if hasattr(self, "XR_S3_") and self.XR_S3_ != None:
                    market_type = 1
                elif code in self.kosdaq_codes and flag in ["N", "R"]:
                    # if hasattr(self, "XR_K3_") and self.XR_K3_ != None:
                    market_type = 2

                if market_type:
                    self.push_advs_realdata_register(code, market_type)

            else:
                self.executeUnadviseRealData(code, "O")
                logger.warning(
                    f"[{self.Name}] 등록 제한 초과 또는 조건 미달: {code} (등록 : {real_registered} + 대기 : {pending} ≥ 제한 : {self.포트폴리오수})"
                )
        except Exception as e:
            logger.exception(f"executeAdviseRealData 실패: {e}")
            ToTelegram(f"executeAdviseRealData 실패: {e}")

    def push_advs_realdata_register(self, code: str, market_type: int):
        """실시간 종목 등록 요청을 큐에 추가"""
        try:
            작업 = ("REGISTER", code, market_type)
            with self.advs_realdata_mng_queue_lock:
                if code not in self.advs_realdata_mng_set["register"]:
                    self.advs_realdata_mng_set["register"].add(code)
                    heapq.heappush(
                        self.advs_realdata_mng_queue, (time.monotonic(), 작업)
                    )
                    self.advs_realdata_mng_queue_event.set()
                    logger.info(
                        f"[{self.__class__.__name__}] advs_realdata_mng_queue 추가됨: {작업}"
                    )
        except Exception:
            logger.exception(f"[{self.Name}] 등록 큐 추가 실패: {code}")

    def executeUnadviseRealData(self, code, flag):
        try:
            self._ensure_lock_controller()
            # 안전한 속성 접근 (getattr 사용)
            advs_objs = getattr(self, "advs_realdata_objs", None)
            if advs_objs is None or code not in advs_objs:
                logger.debug(f"[{self.Name}] {code} 등록건 없음, 무시")
                return

            if flag != "O":
                return

            if code in self.portfolio:
                return
            if code in self.lock_controller.get_all("buy"):
                return
            if code in self.lock_controller.get_all("sell"):
                return
            if code in self.advs_realdata_mng_set["unregister"]:
                return

            self.push_advs_realdata_unregister(code)

        except Exception as e:
            logger.exception(f"executeUnadviseRealData 실패: {e}")
            ToTelegram(f"executeUnadviseRealData 실패: {e}")

    def push_advs_realdata_unregister(self, code: str):
        """실시간 종목 해제 요청을 큐에 추가"""
        try:
            작업 = ("UNREGISTER", code)
            with self.advs_realdata_mng_queue_lock:
                if code not in self.advs_realdata_mng_set["unregister"]:
                    self.advs_realdata_mng_set["unregister"].add(code)
                    heapq.heappush(
                        self.advs_realdata_mng_queue, (time.monotonic(), 작업)
                    )
                    self.advs_realdata_mng_queue_event.set()
                    logger.info(
                        f"[{self.__class__.__name__}] advs_realdata_mng_queue 추가됨: {작업}"
                    )
        except Exception:
            logger.exception(f"[{self.Name}] 해제 큐 추가 실패: {code}")

    def ensure_com_initialized(self):
        """해당 스레드에서 COM 초기화가 필요한 경우에만 초기화."""
        import pythoncom

        self._com_initialized = getattr(self, "_com_initialized", False)
        if not self._com_initialized:
            try:
                pythoncom.CoInitialize()
                self._com_initialized = True
            except pythoncom.com_error as e:
                logger.warning(f"COM 초기화 실패: {e}")

    def executeAdviseRealDataPortFolio(self):
        # 현재 가지고 있는 포트폴리오의 실시간데이타를 받는다.
        # AIGoose은 COMBINED_SCORE1인 종목만 가지고와 시작함. 어제의 포트폴리오의 신호는 보내지 못함.
        # 그래서, 포트폴리오용만 전체 종목 사용.
        # with get_db_connection() as conn:  # with 문을 사용하여 자동으로 conn.close()
        #    query = GET_STOCK_LIST#COMBINED_SCORE 1: 상승
        #    #query = 'select 종목코드,종목명,ETF구분,구분 from 종목코드 WHERE COMBINED_SCORE = 1'# 상승
        #    종목코드테이블 = pdsql.read_sql_query(query, con=conn)
        # kospi_codes = 종목코드테이블.query("구분==1")['종목코드'].values.tolist()
        # kosdaq_codes = 종목코드테이블.query("구분==2")['종목코드'].values.tolist()
        try:
            for code in self.portfolio.keys():
                logger.info(
                    f"[{self.__class__.__name__}] executeAdviseRealDataPortFolio : code : {code}, self.advs_realdata_mng_set['register'] : {self.advs_realdata_mng_set['register']}"
                )
                logger.info(
                    f"[{self.__class__.__name__}] executeAdviseRealDataPortFolio : self.advs_realdata_objs : {self.advs_realdata_objs}"
                )
                if (
                    code in self.advs_realdata_objs
                    or code in self.advs_realdata_mng_set["register"]
                ):
                    logger.debug(f"[{self.Name}] {code} 등록 중복 요청 무시")
                    continue

                # if code not in self.advs_realdata_mng_set['register']:
                market_type = None
                if code in self.kospi_codes:
                    # self.XR_S3_ is not None and \
                    market_type = 1
                elif code in self.kosdaq_codes:
                    # self.XR_K3_ is not None and \
                    market_type = 2
                if market_type is not None:
                    with self.advs_realdata_mng_queue_lock:
                        if code not in self.advs_realdata_mng_set["register"]:
                            self.advs_realdata_mng_set["register"].add(code)
                            작업 = ("REGISTER", code, market_type)
                            heapq.heappush(
                                self.advs_realdata_mng_queue, (time.monotonic(), 작업)
                            )
                            self.advs_realdata_mng_queue_event.set()  # 워커 깨우기
                            logger.info(
                                f"[{self.__class__.__name__}] executeAdviseRealDataPortFolio market_type : {market_type} 등록 code : {code}"
                            )

        except ValueError as e:
            logger.warning(f"값 오류로 인한 등록 건너뜀: {e}")
        except Exception as e:
            logger.exception(f"executeAdviseRealDataPortFolio 실패: {e}")
            ToTelegram(f"executeAdviseRealDataPortFolio 실패: {e}")
            # raise

    def executeUnadviseRealDatas(self):
        try:
            with (
                get_db_connection() as conn
            ):  # with 문을 사용하여 자동으로 conn.close()
                query = GET_REAL_PRICE_INFO_STOCK % (self.Name, SYS_ID)
                self.가격정보요청대상 = pdsql.read_sql_query(query, con=conn)
            self.가격정보요청대상리스트 = self.가격정보요청대상[
                "종목코드"
            ].values.tolist()

            for advs_code in self.advs_realdata_objs:
                if (len(self.가격정보요청대상리스트) == 0) or (
                    advs_code not in self.가격정보요청대상리스트
                ):
                    # self.executeUnadviseRealData(advs_code, "O")
                    # 큐에 등록하지 않고 바로 해제
                    self.unregister_realdata(advs_code)
                    logger.info(
                        f"[{self.__class__.__name__}] executeUnadviseRealDatas {self.Name} advs_code : {advs_code}"
                    )
                    # updateSearchStock(종목코드, -4)

        except Exception as e:
            logger.error(
                "%s-%s %s: %s"
                % (self.__class__.__name__, get_funcname(), get_linenumber(), e)
            )

    # 예: 매 10분마다 오래된 항목 제거
    def cleanup_old_entries(self, threshold=60.0):
        with self.recv_lock:
            now = time.monotonic()
            keys_to_delete = [
                k
                for k, v in self.recv_realdata_recpt_time.items()
                if now - v > threshold
            ]
            for k in keys_to_delete:
                del self.recv_realdata_recpt_time[k]
        # gc.collect() 제거 - 불필요한 오버헤드, Python GC가 자동으로 처리

    # [PHASE 7] SignalWorker removed - ZMQ Reactor replaces it

    def advs_realdata_worker(self):
        try:
            # [FIX] Initialize COM for this thread
            pythoncom.CoInitialize()

            logger.info(
                f"[{self.Name}] advs_realdata_worker 시작됨, 현재 큐 크기: {len(self.advs_realdata_mng_queue)}"
            )
            next_run_time = time.monotonic()

            while self.running:
                # Use short timeout to allow frequent processing
                if not self.advs_realdata_mng_queue_event.wait(timeout=1.0):
                    continue

                with self.advs_realdata_mng_queue_lock:
                    if not self.advs_realdata_mng_queue:
                        self.advs_realdata_mng_queue_event.clear()
                        continue
                    _, 작업 = heapq.heappop(self.advs_realdata_mng_queue)
                    # logger.info(
                    #     f"[{self.Name}] advs_realdata_worker 작업 : {작업}, isinstance(작업, tuple): {isinstance(작업, tuple)}, {len(작업)} "
                    # )

                    작업유형 = 작업[0]
                    stock_cd = 작업[1]

                    if 작업유형 == "UNREGISTER":
                        self.unregister_realdata(stock_cd)
                        self.advs_realdata_mng_set["unregister"].discard(stock_cd)

                    elif 작업유형 == "REGISTER" and len(작업) >= 3:
                        market_type = 작업[2]
                        retry_after = 작업[3] if len(작업) == 4 else 0

                        if retry_after and time.monotonic() < retry_after:
                            # 아직 재시도 시간이 안 됨
                            heapq.heappush(
                                self.advs_realdata_mng_queue, (retry_after, 작업)
                            )
                            continue

                        executor_util = self.parent.gooseTradeExecutor.utilization()
                        if executor_util > UTILIZATION_LIMIT:
                            next_retry = time.monotonic() + 1.5
                            heapq.heappush(
                                self.advs_realdata_mng_queue,
                                (
                                    next_retry,
                                    ("REGISTER", stock_cd, market_type, next_retry),
                                ),
                            )
                            logger.warning(
                                f"🚨 advs_realdata 등록 재시도 예약: {stock_cd} / 사용률 {executor_util:.2%}"
                            )
                        else:
                            self.register_realdata(stock_cd, market_type)
                            self.advs_realdata_mng_set["register"].discard(stock_cd)
                            self.saveSearchStock(stock_cd, 1, 2)
                    else:
                        logger.warning(f"[{self.Name}] 무시된 작업: {작업}")
                        continue

                    logger.info(
                        f"[{self.Name}] advs_realdata 처리됨: {작업}, 남은 큐: {len(self.advs_realdata_mng_queue)}"
                    )
                # 정확한 주기 유지
                next_run_time += 0.5
                sleep_duration = max(0, next_run_time - time.monotonic())
                if sleep_duration > 0:
                    time.sleep(sleep_duration)
                else:
                    logger.warning(f"[{self.Name}] 루프 지연 발생: {sleep_duration}")
                    next_run_time = time.monotonic()
            logger.info(
                f"[{self.Name}] advs_realdata_worker 종료됨. 남은 큐 크기: {len(self.advs_realdata_mng_queue)}"
            )
        except Exception as e:
            logger.exception(f"[{self.Name}] 작업 처리 실패: {작업} / 예외: {e}")
            ToTelegram(f"[{self.Name}] 작업 처리 실패: {작업} / 예외: {e}")
        finally:
            pythoncom.CoUninitialize()
            logger.info(
                f"[{self.Name}] advs_realdata_worker 종료됨. 남은 큐 크기: {len(self.advs_realdata_mng_queue)}"
            )

    def executeSC1(self, result):
        try:
            self._ensure_lock_controller()
            _체결시각 = result["체결시각"]
            _종목코드 = str(result["종목코드"].strip().replace("A", ""))
            _종목명 = result["종목명"]
            _매매구분 = str(result["매매구분"]).strip()
            _주문번호 = result["주문번호"]
            _체결번호 = result["체결번호"]
            _주문수량 = int(str(result["주문수량"]).strip())
            _주문가격 = int(str(result["주문가격"]).strip())
            _체결수량 = int(str(result["체결수량"]).strip())
            _체결가격 = int(str(result["체결가격"]).strip())
            _주문평균체결가격 = int(str(result["주문평균체결가격"]).strip())
            _체결번호 = str(result["체결번호"]).strip()
            _side = "sell" if _매매구분 == "1" else "buy"

            # 내가 주문한 것이 체결된 경우 처리
            if _주문번호 in self.주문번호리스트:
                # 기존 등록 로직 제거됨 (아래로 이동)
                logger.info(
                    f"[{self.__class__.__name__}] executeSC1: {type(self), type(result), result}"
                )
                if _매매구분 == "1":  # 매도
                    P = self.portfolio.get(_종목코드, None)
                    #  선제적으로 분리해둔 pending_sell 가상 객체 복구 및 체결 정산
                    if P is None and hasattr(self, "_pending_sell_stocks_data"):
                        P = self._pending_sell_stocks_data.get(_종목코드, None)

                    if P:
                        P.수량 = max(P.수량 - _체결수량, 0)
                        if P.수량 == 0:
                            if hasattr(self, "_pending_sell_stocks_data"):
                                self._pending_sell_stocks_data.pop(_종목코드, None)
                            logger.info(
                                "%s %s 종목매도 data : %s %s %s %s",
                                self.Name,
                                P.종목명,
                                P.매수일,
                                P.종목코드,
                                P.매수가,
                                P.수량,
                            )
                            self.portfolio.pop(_종목코드, None)
                            self.lock_controller.unlock_sell(_종목코드)
                            self.포트폴리오종목삭제(_종목코드)
                            self.executeUnadviseRealData(_종목코드, "O")
                            updateSearchStock(_종목코드, 1)  # 매도상태
                        else:
                            self.포트폴리오종목갱신(_종목코드, P)
                            # TODO: 빠른거래시 화면의 응답속도도 영향을 주므로 일단은 커멘트처리
                            # self.parent.GooseView()
                            # ToTelegram(__class__.__name__ + "매도 : %s 체결수량:%s 체결가격:%s" % (종목명, 주문수량, 주문평균체결가격))
                elif _매매구분 == "2":  # 매수
                    P = self.portfolio.get(_종목코드, None)
                    if P is None:
                        P = CPortStock(
                            종목코드=_종목코드,
                            종목명=_종목명,
                            매수가=_주문평균체결가격,
                            수량=_체결수량,
                            매수일=dt.datetime.now(),
                        )
                        self.portfolio[_종목코드] = P
                        logger.info(
                            f"[PORTFOLIO-NEW] ✅ {_종목코드} ({_종목명}) 등록: 가격={_주문평균체결가격:,}, 수량={_체결수량}"
                        )
                    else:
                        P.수량 += _체결수량
                        logger.info(
                            f"[PORTFOLIO-ADD] ➕ {_종목코드} ({_종목명}) 수량증가: {P.수량 - _체결수량}→{P.수량}"
                        )

                    if P.수량 == _주문수량:
                        self.lock_controller.unlock_buy(_종목코드)
                        logger.info(
                            "%s %s 종목매수 data : %s %s %s %s",
                            self.Name,
                            P.종목명,
                            P.매수일,
                            P.종목코드,
                            P.매수가,
                            P.수량,
                        )
                        # self.parent.GooseView()
                        # ToTelegram(__class__.__name__ + "매수 : %s 체결수량:%s 체결가격:%s" % (종목명, 주문수량, 주문평균체결가격))

                    # [CRITICAL] Portfolio DB 등록 with detailed logging
                    try:
                        logger.info(f"[PORTFOLIO-DB] 📝 {_종목코드} DB 등록 시작...")
                        self.포트폴리오종목갱신(_종목코드, P)
                        logger.info(f"[PORTFOLIO-DB] ✅ {_종목코드} DB 등록 성공")
                    except Exception as db_err:
                        logger.error(
                            f"[PORTFOLIO-DB] ❌ {_종목코드} DB 등록 실패: {db_err}"
                        )
                        logger.exception(f"[PORTFOLIO-DB] 상세 에러:")

                    # 가상계좌의 주문금액 차감
                    #  체결가격 제곱승산 버그 소각 -> 정상적인 체결금액 산출. D2 예수금 무한 팽창 차단.
                    체결금액 = _체결가격 * _체결수량
                    coordinator.sync_with_account(체결금액)

                    # [Step 4.1] 64-bit 서버로 체결 피드백 즉시 전송 (Shadow State 동기화)
                    try:
                        from GG_Shared.util.zmq_manager import send_feedback
                        feedback_payload = {
                            "type": "FILL",
                            "ticker": _종목코드,
                            "filled_qty": _체결수량,
                            "avg_price": _주문평균체결가격 if _주문평균체결가격 > 0 else _체결가격,
                            "side": _side,
                            "cl_ord_id": _주문번호,
                            "timestamp": dt.datetime.now().strftime("%H:%M:%S.%f")
                        }
                        send_feedback(feedback_payload)
                        logger.info(f"📡 [ZMQ Feedback] Sent FILL event for {_종목코드} to 64-bit Brain.")
                    except Exception as fe:
                        logger.error(f"❌ [ZMQ Feedback] Failed to send FILL event: {fe}")

                    # [Internal Audit] 10초(또는 3초) 간격 제한 확인
                    if not syncAccCoordinator.can_sync(interval=1):
                        return

                    with syncAccCoordinator._lock:  # 원자적 실행 보장
                        #  백그라운드 스레드에서 COM 객체 직접 호출 금지 (Crash 방지)
                        # safe_invoke를 사용하여 메인 GUI 스레드로 안전하게 위임
                        safe_invoke(
                            None, None, self.parent.dialog["계좌정보조회"].inquiry
                        )
                        syncAccCoordinator.register_sync_sent()
                # 굳이 여기서.. 부하회피
                # if self.parent is not None:
                #    self.parent.GooseView()

                # 체결 기록 dict 구조로
                체결데이터 = {
                    "거래환경": self.parent.account_dict.get("거래환경", ""),
                    "GG_NM": self.Name,
                    "UUID": self.UUID,
                    "일자": dt.datetime.now().strftime("%Y-%m-%d"),
                    "체결시각": _체결시각,
                    "종목코드": _종목코드,
                    "종목명": _종목명,
                    "매매구분": _매매구분,
                    "주문번호": _주문번호,
                    "체결번호": _체결번호,
                    "주문수량": _주문수량,
                    "주문가격": _주문가격,
                    "체결수량": _체결수량,
                    "체결가격": _체결가격,
                    "주문평균체결가격": _주문평균체결가격,
                }
                self.체결기록(data=체결데이터)

                # [FIX] 체결 후 포트폴리오에 존재하는 경우에만 실시간 등록 시도
                # 전량 매도의 경우 위 로직에서 pop 되었으므로 여기 진입 안 함 -> 재등록 방지
                if _종목코드 in self.portfolio:
                    market_type = None
                    if _종목코드 in self.kospi_codes:
                        market_type = 1
                    elif _종목코드 in self.kosdaq_codes:
                        market_type = 2

                    if market_type:
                        # 이미 등록되어 있는지 확인 (LockController나 objs check는 register_realdata에서 함)
                        # 하지만 불필요한 큐 진입 방지를 위해 간단한 체크
                        if _종목코드 not in self.advs_realdata_objs:
                            with self.advs_realdata_mng_queue_lock:
                                # Double check inside lock
                                if (
                                    _종목코드
                                    not in self.advs_realdata_mng_set["register"]
                                ):
                                    self.advs_realdata_mng_set["register"].add(
                                        _종목코드
                                    )
                                    작업 = ("REGISTER", _종목코드, market_type)
                                    heapq.heappush(
                                        self.advs_realdata_mng_queue,
                                        (time.monotonic(), 작업),
                                    )
                                    self.advs_realdata_mng_queue_event.set()
                                    logger.info(
                                        f"[{self.__class__.__name__}] executeSC1(After Trade) advs_queue ADD: {작업}"
                                    )
                                else:
                                    logger.debug(
                                        f"[{self.__class__.__name__}] executeSC1: {_종목코드} 이미 등록된 상태 (LOCK CHECK) -> 스킵"
                                    )
                        else:
                            logger.debug(
                                f"[{self.__class__.__name__}] executeSC1: {_종목코드} 이미 등록/대기중 -> 스킵"
                            )

            # [Sync] Publish to 64-bit Brain
            self._publish_order_result(
                _종목코드,
                "SUCCESS",
                f"Filled {_체결수량} @ {_체결가격} ({_side})",
                qty=_체결수량,
                price=_체결가격,
                ord_no=_주문번호,
                fill_no=_체결번호,
                ord_qty=_주문수량,  # [Partial Fill Support] Total ordered quantity for this ord_no
                side=_side,
                type="FILL",
            )
        except Exception as e:
            logger.exception(f"[executeSC1 예외] {e}")

    def executeH1_HA_(self, szTrCode, result):
        """
        [Phase 3] 스마트 호가 잔량 수급 (OBI 캐시 연료)
        """
        if hasattr(self, "smart_executor"):
            self.smart_executor.update_tick(szTrCode, result)

    def executeK3_S3_(self, szTrCode, result):
        """
        실시간 체결/호가 신호를 비동기 큐에 쌓고,
        별도의 asyncio 워커에서 순차 처리한다.

        - 1초 제한(쓰로틀링)은 제거.
        - 실행 중 오류 시에도 전체 시스템이 멈추지 않도록 폴백 로직 포함.
        """
        # if not (self.매수거래중 or self.매도거래중):
        #     # logger.debug(f"💤 [{self.Name}] 매수/매도 거래시간이 아님 → 종목코드 {result['종목코드']} 무시")
        #     return

        stock_cd = result.get("종목코드")
        current_price = float(result.get("현재가"))  # 실제 키값에 맞게 사용
        if stock_cd is None:
            logger.warning(
                f"[{self.__class__.__name__}] executeK3_S3_: '종목코드' 없음 → 결과 무시: {result}"
            )
            return

        # [ZMQ] Tick Sensor -> 64-bit Brain
        self._ensure_zmq_pub()
        if self.zmq_pub:
            try:
                #  Update local tick cache for OFI Guard / Price Escape
                if not hasattr(self, "ticks_cache"):
                    self.ticks_cache = {}
                self.ticks_cache[stock_cd] = current_price

                safe_result = self.make_json_safe(result)
                # [DEBUG] Log tick publishing (Success case)
                # [DEBUG] Log tick publishing (Unconditional for verification)
                # logger.info(f"[ZMQ-PUB] {stock_cd} TICK sent to 5555")

                self.zmq_pub.publish_data(
                    self.TOPIC_TICK,
                    {
                        "ticker": stock_cd,
                        "data": safe_result,
                        "timestamp": dt.datetime.now().strftime("%H:%M:%S.%f"),
                    },
                )

                # 🛡️  32-bit Autonomous Local Guard (Tick-level Precision)

                self.check_local_guard(stock_cd, current_price)
            except Exception as e:
                logger.error(f"[{self.__class__.__name__}] ZMQ Tick PUB Error: {e}")

    def process_buy_orders(self, data):
        logger.info(
            f"Trace: process_buy_orders called with data keys={list(data.keys())}"
        )
        """매수 주문 처리 스레드"""
        self._ensure_lock_controller()  #  매수 진입점 Lock 증발 원천 차단

        ticker = data.get("ticker")

        #  UI 포트폴리오 한도 이중 잠금 (Double Lock)

        # 현재 보유 종목과 매수 락이 걸린 종목의 총합 계산
        port_keys = (
            set(self.portfolio.keys())
            if hasattr(self, "portfolio") and self.portfolio is not None
            else set()
        )
        locked_buys = (
            self.lock_controller.get_all("buy")
            if hasattr(self, "lock_controller")
            else set()
        )
        lst = port_keys.union(locked_buys)
        max_cap = getattr(self, "포트폴리오수", 5)  # UI 설정값

        # [Smart Rotation] Directive 3: Smart Rotation & Weed Pulling
        decision_state = data.get("decision_state", "")
        grade = data.get("grade", "B")
        is_platinum = (grade == "S") or (decision_state == "APPROVE_PLATINUM_PASS")

        # 신규 진입이면서 한도에 도달한 경우 처리
        if len(lst) >= max_cap and ticker not in port_keys:
            if is_platinum:
                logger.info(
                    f"🔥 [Smart Rotation] Leader Stock Detected ({ticker}, Grade:{grade}). Searching for weed to pull..."
                )

                # Identify "Weeds": Grade C/F or lowest profit rate (수익율)
                weed_ticker = None
                lowest_profit = 999.0

                # 1. First priority: Grade C/F stocks in existing portfolio
                for t, pos in self.portfolio.items():
                    p_grade = getattr(pos, "grade", "B")
                    if p_grade in ["C", "F"]:
                        weed_ticker = t
                        break

                # 2. Second priority: Lowest profit rate (수익율) stock
                if not weed_ticker:
                    for t, pos in self.portfolio.items():
                        # 수익율 attribute check from CPortStock
                        p_profit = getattr(pos, "수익율", 0.0)
                        if p_profit < lowest_profit:
                            lowest_profit = p_profit
                            weed_ticker = t

                if weed_ticker:
                    weed_pos = self.portfolio[weed_ticker]
                    w_qty = getattr(weed_pos, "수량", 0)
                    w_price = getattr(weed_pos, "현재가", 0)

                    logger.warning(
                        f"✄ [Smart Rotation] Pulling weed: {weed_ticker} (Profit:{getattr(weed_pos, '수익율', 0.0):.2f}%) to make room for {ticker}"
                    )

                    # 32-bit execution logic: Sell weed at Market Price (03) and evacuate slot
                    try:
                        self.query_sell(weed_ticker, w_qty, w_price, order_type="03")

                        # [State Sync] 가상 분리 및 포트폴리오 팝
                        removed_pos = self.portfolio.pop(weed_ticker, None)
                        if not hasattr(self, "_pending_sell_stocks_data"):
                            self._pending_sell_stocks_data = {}
                        if removed_pos:
                            self._pending_sell_stocks_data[weed_ticker] = removed_pos

                        logger.info(
                            f"✅ [Smart Rotation] {weed_ticker} slot evacuated. Proceeding with S-grade buy for {ticker}."
                        )
                        # Continue to self.buy_order(ticker, data) which is down below
                    except Exception as rot_e:
                        logger.error(
                            f"❌ [Smart Rotation] Weed pull failed for {weed_ticker}: {rot_e}"
                        )
                        return
                else:
                    logger.warning(
                        f"🚫 [Smart Rotation] No pullable stock found for {ticker}. Blocking buy."
                    )
                    return
            else:
                logger.warning(
                    f"🚫 [{self.__class__.__name__}] UI 포트폴리오 한도 초과 (현재:{len(lst)}/최대:{max_cap}). "
                    f"매수 프로세스 강제 종료: {ticker}"
                )
                return

        # [수정 후:  패치]

        if getattr(self, "parent", None) is None:
            #  ZMQ 통신 중 Parent 유실 시 동적 복구 (Self-Healing)
            from PyQt5.QtWidgets import QApplication

            for widget in QApplication.topLevelWidgets():
                if widget.__class__.__name__ == "MainWindow":
                    self.parent = widget
                    logger.info(
                        f"🔄 [{self.__class__.__name__}] Parent(MainWindow) 연결 동적 복구 성공."
                    )
                    break

            # 복구 실패 시에만 최후의 Fallback 가동
            if getattr(self, "parent", None) is None:
                logger.error(
                    f"❌ [{self.__class__.__name__}] Parent 복구 실패. Fallback 매수 강제 집행: {ticker}"
                )
                qty = data.get("qty", data.get("quantity", 0))
                price = data.get("price", 0)
                if hasattr(self, "query_buy") and qty > 0:
                    self._ensure_lock_controller()
                    self.lock_controller.lock_buy(ticker)
                    self.query_buy(ticker, qty, price)
                return

        is_today_sold = False
        # _is_portfolio_over = False
        매수거래중 = self.매수거래중
        주문가능금액부족 = self.parent.주문가능금액부족
        for goose in self.parent.gooses:
            if ticker in goose.금일매도종목:
                is_today_sold = True
                break
        # for goose in self.parent.gooses:
        #     if ticker in goose.portfolio.keys():
        #         _is_portfolio_over = True
        #         break

        if (
            매수거래중 and not 주문가능금액부족 and not is_today_sold
            # and not _is_portfolio_over
        ):
            # lst = set(self.portfolio.keys()).union(self.lock_controller.get_all("buy"))
            # 포트폴리오 수 설정 이내에 등록되어 있지 않으면 매수 프로세스 시작
            # logger.info(
            #     f"len(lst), self.포트폴리오수 ,self.portfolio.keys(), lock.get_all('buy'), lock.is_buy_locked : "
            #     f"{len(lst), self.포트폴리오수, self.portfolio.keys(), self.lock_controller.get_all('buy'), self.lock_controller.is_buy_locked(_종목코드)} "
            # )
            # if len(lst) >= self.포트폴리오수 and ticker not in self.portfolio.keys():
            #    return

            if not self.lock_controller.is_buy_locked(ticker):
                if data.get("deep_pullback_required", False):
                    try:
                        # [Safe-Gate] VWAP 기반 정밀 눌림목 체크
                        # [Phase 2.2] VWAP 미보유 시 당일 고가 대비 -4% 이상 하락했을 때만 진입 허용 (Proxy)
                        # 여기서는 일단 고유 로직으로 placeholder를 두고, 추후 TR 조회를 통해 VWAP 정규화 가능
                        high_price = float(
                            data.get("latest_row", {}).get("고가", price)
                        )
                        if price > high_price * 0.96:  # -4% 미만 하락 시
                            logger.warning(
                                f"🔒 [Deep-Pullback-Lock] {ticker} 과열 대장주 -> 충분한 눌림목(-4%) 대기 중 (현재가:{price} > Target:{high_price * 0.96:.0f})"
                            )
                            return

                        logger.info(
                            f"✅ [Deep-Pullback-Clear] {ticker} 과열 대장주 눌림목 조건 충족 -> 매수 진행"
                        )
                    except Exception as e:
                        logger.error(f"Error in Deep Pullback Lock: {e}")

                self.buy_order(ticker, data)

            # else:
            #     if not self.lock_controller.is_buy_locked(ticker):
            #         self.executeUnadviseRealData(ticker, "O")
            #         updateSearchStock(ticker, -2)  # 포트폴리오초과

    def buy_order(self, ticker, data):

        price = float(data.get("price", 0))
        qty = int(data.get("qty", data.get("quantity", 0)))
        market_type = data.get("market_type", 1)
        market_code = "S3_" if market_type == 1 else "K3_"
        accInfo = self.parent.dialog[
            "계좌정보조회"
        ]  # 다른 쓰레드에서 쓰고있을 때 lock방지용
        market_info = self.get_market_info(accInfo, market_code)

        # 1. 시장 상황 및 시그널 체크 (명확한 변수 할당)
        is_down_mkt_prevent = str(DOWN_MKT_BUY_YN).strip().upper() == "N"

        # 2. 하락장 매수 금지 로직 (논리 분리)
        if is_down_mkt_prevent:
            has_no_buy_signal = not check_buy_signal(data.get("latest_minute"))
            logger.info(
                f"[{ticker}] 매수검증 상세 - 금지모드(N): {is_down_mkt_prevent}, 시그널없음: {has_no_buy_signal}"
            )
            if has_no_buy_signal:
                try:
                    종목명 = self.종목코드테이블.query("종목코드=='%s'" % ticker)[
                        "종목명"
                    ].values[0]
                except Exception as e:
                    종목명 = ""

                self.saveOrderHist(
                    market_code=market_code,
                    market_info=market_info,
                    종목코드=ticker,
                    종목명=종목명,
                    거래유형="하락장미매수",
                    매수가=price,
                    매도가=price,
                    매도수량=0,
                    매수일=dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    평가손익=0,
                    result=data,
                )
                self.executeUnadviseRealData(ticker, "O")
                updateSearchStock(ticker, -5)  # 하락장 미매수
                return
            else:
                pass

        if not self.lock_controller.is_buy_locked(ticker):
            self.lock_controller.lock_buy(ticker)

            # [Internal Audit] 10초(또는 3초) 간격 제한 확인 (Per Ticker)
            if not coordinator.can_order(ticker, interval=ORDER_INTERVAL - 2):
                logger.warning(
                    f"🚫 [BaseGoose] can_order interval blocked order for {ticker}"
                )
                return

            with coordinator._lock:  # 원자적 실행 보장
                # 1. 증권사 서버에서 최신 데이터 가져오기 (매입금액, 예수금 등)
                #  UI 프리징 원인! 메인 스레드 동기화 쿼리 금지. 로컬 캐시를 신뢰함.
                # accInfo.inquiry()

                # 2. 가상 잔고 반영 (이미 계산된 _pending_order_amt 차감)
                original_d2 = int(accInfo.dfAccSumInfo.at[0, "추정D2예수금"])
                actual_d2 = original_d2 - coordinator._pending_order_amt

                # 3. DataFrame 업데이트 (이후 계산 로직들이 이 값을 참조하게 됨)
                accInfo.dfAccSumInfo.at[0, "추정D2예수금"] = actual_d2

                logger.info(
                    f"[Money-Sync] 실제D2:{original_d2:,.0f} | 가상차감:{coordinator._pending_order_amt:,.0f} | 적용D2:{actual_d2:,.0f}"
                )

                try:
                    order_amount = qty * price
                    #  종목명 캐싱 및 safe-fetch
                    try:
                        종목명 = self.종목코드테이블.query("종목코드=='%s'" % ticker)[
                            "종목명"
                        ].values[0]
                    except Exception:
                        종목명 = ""

                    # [Pardon-Audit] 주문 데이터 패키징 후 query_buy로 전달 (전달 후 기록 보존용)
                    order_log_data = {
                        "market_code": market_code,
                        "market_info": market_info,
                        "종목코드": ticker,
                        "종목명": 종목명,
                        "거래유형": "매수",
                        "매수가": price,
                        "매도가": price,  # 매수 시에는 매도가에 현재가를 임시 기록
                        "매도수량": qty,
                        "매수일": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "평가손익": 0,
                        "result": data,
                    }

                    target_price_type = data.get("order_price_type", None)
                    self.query_buy(
                        ticker,
                        qty,
                        price,
                        order_price_type=target_price_type,
                        order_log_data=order_log_data,
                    )
                    type_str = (
                        "LIMIT(00)"
                        if target_price_type == "00"
                        else f"MARKET({self.매수방법})"
                    )
                    logger.info(
                        f"[ZMQ-BUY] {ticker} | {qty}qty | {price}price | OrderType: {type_str}"
                    )
                    #  Pre-emptive Pending already added by Router (MainWindowLogic)
                    # coordinator.register_order_sent(order_amount)

                    logger.info(
                        f"🚀 [Order Sent] {ticker} | 금액: {order_amount:,.0f} | 누적펜딩: {coordinator._pending_order_amt:,.0f}"
                    )
                except Exception as e:
                    logger.error(f"❌ [Order-Issue-Error] {ticker} 매수 집행 실패: {e}")

                return 2
        else:
            logger.info(f"[{ticker}] 이미 Buy Lock 상태이므로 query_buy 스킵")

    @classmethod
    def get_instant_order_flow_status(cls, ticker):
        """
        [Order Flow Imbalance]
        Blocking XAQuery_t1101 with Message Pump (Timeout: 0.5s)
        """

        # Local Handler for synchronous callback
        class ResponseHandler:
            def __init__(self):
                self.received = False
                self.data = None

            def OnReceiveData(self, tr_code, result=None):
                if logger:
                    logger.info(f"[OrderFlow] {tr_code} Received Data: {result}")
                self.data = result
                self.received = True

            def OnReceiveMessage(self, systemError, messageCode, message):
                pass

        handler = ResponseHandler()
        try:
            # t1101 is already imported at the top of the file via 'from xing.XAQuaries import *'
            # The local import here shadowed the global and caused UnboundLocalError if the 'if' was skipped.

            # Parent is handler (local object), not cls
            query = t1101(parent=handler)
            query.Query(ticker)

            start_time = time.time()
            timeout = 0.5  # 0.5s Timeout

            while not handler.received:
                pythoncom.PumpWaitingMessages()
                if time.time() - start_time > timeout:
                    # logger is global, so it works. cls.logger might not if not set.
                    # BaseGoose.logger is set by set_logger?
                    # Global logger is safer here as per BaseGoose patterns.
                    if logger:
                        logger.warning(
                            f"[OrderFlow] Checking {ticker} Timeout ({timeout}s)"
                        )
                    return False
                time.sleep(0.01)  # Avoid CPU spin

            data = handler.data
            if not data:
                return False

            bid1 = int(data.get("bidrem1", 0) or 0)
            offer1 = int(data.get("offerrem1", 0) or 0)

            from config.ai_settings import PARAMS
            from GG_Server.strategy.rules.entry_strategy import check_order_flow_imbalance

            threshold = PARAMS.get("order_flow_threshold", 5.0)

            is_imbalance, ratio = check_order_flow_imbalance(bid1, offer1, threshold)

            if not is_imbalance:
                if logger:
                    logger.info(
                        f"[Filter-OrderFlow] {ticker} Ratio: {ratio:.1f} (Target: {threshold}) - Skip"
                    )
            else:
                if logger:
                    logger.info(
                        f"🚀 [TRIPLE-CONFIRMED] {ticker} Order Flow Imbalance Confirmed (Ratio: {ratio:.1f})"
                    )

            return is_imbalance

        except Exception as e:
            if logger:
                logger.error(f"[OrderFlow] {ticker} Error: {e}")
            return False

    def process_sell_orders(self, data):
        logger.info(
            f"Trace: process_sell_orders called with data keys={list(data.keys())}"
        )
        self._ensure_lock_controller()
        ticker = data.get("ticker")

        #  ZMQ Cross-Routing (Ghost Portfolio 무한 루프 방어)
        local_P = self.portfolio.get(ticker)
        if not local_P:
            # 내 포트폴리오에 없다면 형제 Goose(GuardianGoose 등)가 가지고 있는지 수소문하여 위임
            if hasattr(self, "parent") and hasattr(self.parent, "gooses"):
                for sibling in self.parent.gooses:
                    if sibling.Name != self.Name and ticker in sibling.portfolio:
                        logger.warning(
                            f"🔄 [Auto-Routing] {ticker} 보유자 불일치! {self.Name} -> {sibling.Name}(으)로 매도 명령 이관."
                        )
                        sibling.process_sell_orders(data)
                        return

            logger.debug(
                f"[{self.Name}] Ignoring SELL for {ticker} (Not in ANY portfolio. Already sold?)"
            )
            return
        """매도 주문 처리 — 32-bit 실행 레이어"""

        if getattr(self, "parent", None) is None:
            logger.error(
                f"❌ [{self.__class__.__name__}] ë³´ëª¨ ìì²´(parent) ì°ê²°ì´ ì ì¤ëììµëë¤. íì·¡ Fallbackì¼ë¡ ë§¤ëë¥¼ ê°ì  ì§íí©ëë¤: {ticker}"
            )
            qty = data.get("qty", 0)
            price = data.get("price", 0)
            if hasattr(self, "query_sell") and qty > 0:
                self._ensure_lock_controller()
                self.lock_controller.lock_sell(ticker)
                self.query_sell(ticker, qty, price, "LIMIT")
            return

        #  ZMQ payload에 없는 정보 대신 내부 메모리의 정확한 원본(CPortStock) 참조
        local_P = self.portfolio.get(ticker)
        if not local_P:
            logger.debug(f"[{self.Name}] Ignoring SELL for {ticker} (Not in portfolio)")
            return

        qty = int(data.get("qty", 0))
        price = float(data.get("price", 0))
        sell_type = data.get("reason", "hold")

        # 시장구분 안전 추론 (기본값 KOSPI 1)
        market_type = (
            1
            if ticker in self.kospi_codes
            else (2 if ticker in self.kosdaq_codes else 1)
        )
        szTrCode = "S3_" if market_type == 1 else "K3_"

        매도거래중 = self.매도거래중

        if 매도거래중 == True:
            # 하락중일때도 매도 종료하며 반등 기다림
            if not self.lock_controller.is_sell_locked(ticker):
                accInfo = self.parent.dialog[
                    "계좌정보조회"
                ]  # 다른 쓰레드에서 쓰고있을 때 lock방지용
                market_info = self.get_market_info(accInfo, szTrCode)

                # 트레일링 스탑
                _매도수량 = qty  # _P.get('수량', qty)
                매도유형 = self.매도유형
                if 매도유형 == "01":
                    # if self.checkTrailingStop(
                    #     ticker,
                    #     _P.get('매수가', 0),
                    #     price,
                    #     _P.get('매수후고가', 0),
                    #     _market_info["지수등락율"],
                    #     _sell_condition,
                    # ):
                    sell_type = "trailing_stop"
                # 수익/손절 스탑
                elif 매도유형 == "00":
                    self.sell_order(
                        szTrCode,
                        ticker,
                        local_P.매수가,
                        price,
                        local_P.매수후고가,
                        data,
                        market_info["지수등락율"],
                        qty,  #  강제 100% 매도 버그 소각 -> 브레인이 계산한 정교한 분할매도 수량(qty) 절대 존중
                        local_P.매수일,
                        accInfo,
                        market_info,
                        data,
                    )

    def sell_order(
        self,
        szTrCode,
        종목코드,
        매수가,
        현재가,
        매수후고가,
        result,
        지수등락율,
        수량,
        매수일,
        accInfo,
        market_info,
        data,
    ):
        sell_type = data.get("reason", "hold")
        매도수량 = int(data.get("qty", 수량))

        if sell_type not in ("hold",) and 매도수량 > 0:
            try:
                매수일 = dt.datetime.strptime(str(매수일), "%Y-%m-%d %H:%M:%S")
            except Exception:
                매수일 = dt.datetime.now()

            logger.info(f"sellOrder sell_info {종목코드}: {data}")
            logger.info(
                f"sellOrder is_sell_locked({종목코드}). : {self.lock_controller.is_sell_locked(종목코드)}"
            )

            self.lock_controller.lock_sell(종목코드)

            is_today_sold = False
            for goose in self.parent.gooses:
                if 종목코드 in goose.금일매도종목:
                    is_today_sold = True
                    break

            if not is_today_sold:
                self.금일매도종목.append(종목코드)

            #  ZMQ 통신 비동기 레이스 컨디션 방어.
            # 증권사 체결 대기 시간(300ms~1s) 동안 32bit Router가 슬롯이 꽉 찼다고 착각하지 않도록,
            # 매도 주문 발사 즉시 포트폴리오에서 가상으로 분리(Hide)하여 즉각적인 매수 슬롯 확보.
            P_obj = self.portfolio.get(종목코드, None)
            if P_obj and 매도수량 >= P_obj.수량:
                temp_p = self.portfolio.pop(종목코드, None)
                if temp_p:
                    if not hasattr(self, "_pending_sell_stocks_data"):
                        self._pending_sell_stocks_data = {}
                    self._pending_sell_stocks_data[종목코드] = temp_p
                    logger.info(
                        f"⚡ [Slot-Freed] {종목코드} ZMQ 매도 즉시 가상 슬롯 확보 완료 (Router Bypass)"
                    )

            종목명 = ""
            try:
                종목명 = self.종목코드테이블.query("종목코드=='%s'" % 종목코드)[
                    "종목명"
                ].values[0]
            except Exception as e:
                종목명 = ""

            평가손익 = (현재가 - 매수가) * 매도수량
            _acc_col = (
                "종목번호"
                if "종목번호" in accInfo.dfAccStockInfo.columns
                else "종목코드"
            )
            condition = accInfo.dfAccStockInfo[_acc_col] == 종목코드
            df = accInfo.dfAccStockInfo[condition]
            if not df.empty:
                평가손익 = to_numeric_safe(df.iloc[0]["평가손익"])

            # [Pardon-Audit] 주문 데이터 패키징 후 query_sell로 전달 (전달 후 기록 보존용)
            order_log_data = {
                "market_code": szTrCode,
                "market_info": market_info,
                "종목코드": 종목코드,
                "종목명": 종목명,
                "거래유형": sell_type,
                "매수가": 매수가,
                "매도가": 현재가,
                "매도수량": 매도수량,
                "매수일": str(매수일),
                "평가손익": 평가손익,
                "result": data,
            }

            self.query_sell(종목코드, 매도수량, 현재가, order_log_data=order_log_data)
            logger.info(f"[ZMQ-SELL] {종목코드} | {매도수량}qty | EXIT")
            logger.info(f"sellOrder query_sell : {종목코드, 매도수량, 현재가}")

    def 포트폴리오읽기(self):
        # print (f"self.parent.account_dict['SYS_ID'] : {self.parent.account_dict['SYS_ID']}")
        try:
            with (
                get_db_connection() as conn
            ):  # with 문을 사용하여 자동으로 conn.close()
                query = """SELECT GG_ID, GG_NM, 포트폴리오키, 매수일, 종목코드, 종목명, 매수가, 수량, 매수후고가, STATUS, SYS_ID
                            FROM 포트폴리오 
                            WHERE GG_ID = '%s' 
                            AND SYS_ID = (SELECT tac.CONF_VALUE 
                                        FROM TB_AI_CONF tac 
                                        WHERE tac.CONF_ID = '1')""" % (self.UUID)
                df = pdsql.read_sql_query(query, con=conn)
            self.portfolio = dict()

            # [Performance Optimization] iterrows() 제거 - 10x faster
            if not df.empty:
                # 날짜 변환을 벡터화
                df["매수일"] = pd.to_datetime(
                    df["매수일"].astype(str).str[:19], format="%Y-%m-%d %H:%M:%S"
                )

                # Dictionary로 변환 (iterrows 대신)
                for 종목코드, row_data in df.set_index("포트폴리오키").iterrows():
                    self.portfolio[종목코드] = CPortStock(
                        매수일=row_data["매수일"],
                        종목코드=row_data["종목코드"],
                        종목명=row_data["종목명"],
                        매수가=row_data["매수가"],
                        수량=row_data["수량"],
                        매수후고가=row_data["매수후고가"],
                        STATUS=row_data["STATUS"],
                    )
                    logger.debug(
                        "포트폴리오읽기 QUERY : %s,\n GG_ID %s, GG_NM %s, 포트폴리오키 %s, 매수일 %s, 종목코드 %s, 종목명 %s, 매수가 %s, 수량 %s, 매수후고가 %s, STATUS %s, SYS_ID %s, 거래환경 %s"
                        % (
                            query,
                            row_data["GG_ID"],
                            row_data["GG_NM"],
                            종목코드,
                            row_data["매수일"],
                            row_data["종목코드"],
                            row_data["종목명"],
                            row_data["매수가"],
                            row_data["수량"],
                            row_data["매수후고가"],
                            row_data["STATUS"],
                            row_data["SYS_ID"],
                            거래환경,
                        )
                    )
        except Exception as e:
            logger.error(
                "%s-%s %s: %s"
                % (self.__class__.__name__, get_funcname(), get_linenumber(), e)
            )

    def 포트폴리오쓰기(self):
        """포트폴리오를 DB에 저장 (배치 INSERT로 성능 개선)"""
        try:
            with get_db_connection() as conn:  # with 문으로 자동 연결 반환
                cursor = conn.cursor()
                cursor.execute(PORTFOLIO_DELETE % (self.UUID, SYS_ID))

                query = "INSERT INTO 포트폴리오(GG_ID, GG_NM, 포트폴리오키, 매수일, 종목코드, 종목명, 매수가, 수량, 매수후고가, STATUS, SYS_ID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

                # 배치 데이터 준비
                batch_data = [
                    (
                        self.UUID,
                        self.Name,
                        k,
                        str(v.매수일),  # Timestamp -> str 변환
                        v.종목코드,
                        v.종목명,
                        v.매수가,
                        v.수량,
                        v.매수후고가,
                        v.STATUS,
                        SYS_ID,
                    )
                    for k, v in self.portfolio.items()
                    if v.수량 > 0
                ]

                # 배치 INSERT 실행 (성능 개선)
                if batch_data:
                    cursor.executemany(query, batch_data)
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(
                            f"포트폴리오쓰기 배치 INSERT: {len(batch_data)}건, SYS_ID: {SYS_ID}"
                        )
                cursor.close()
        except Exception as e:
            logger.error(
                "%s-%s %s: %s"
                % (self.__class__.__name__, get_funcname(), get_linenumber(), e)
            )

    def 포트폴리오종목갱신(self, 포트폴리오키, P):
        try:
            with (
                get_db_connection() as conn
            ):  # with 문을 사용하여 자동으로 conn.close()
                cursor = conn.cursor()
                if P.수량 > 0:
                    P.매수일 = str(P.매수일)[:19]
                    # logger.debug( '포트폴리오종목갱신 data P.매수일: %s' % P.매수일)
                    query = REPLACE_포트폴리오
                    data = [
                        self.UUID,
                        self.Name,
                        포트폴리오키,
                        P.매수일,
                        P.종목코드,
                        P.종목명,
                        P.매수가,
                        P.수량,
                        P.매수후고가,
                        P.STATUS,
                        SYS_ID,
                    ]
                else:
                    query = PORTFOLIO_DELETE_BY_KEY
                    data = [self.UUID, SYS_ID, 포트폴리오키]

                cursor.execute(query, data)
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        f"포트폴리오종목갱신 cursor.rowcount: {cursor.rowcount}, SYS_ID: {SYS_ID}, 거래환경: {거래환경}"
                    )
                cursor.close()

        except Exception as e:
            클래스이름 = self.__class__.__name__
            함수이름 = get_funcname()
            logger.error("%s-%s %s: %s" % (클래스이름, 함수이름, get_linenumber(), e))

    def 매수후고가갱신(self, 포트폴리오키, P):
        try:
            with (
                get_db_connection() as conn
            ):  # with 문을 사용하여 자동으로 conn.close()
                cursor = conn.cursor()
                query = UPDATE_매수후고가
                data = [P.매수후고가, 포트폴리오키, SYS_ID, P.매수후고가, P.매수후고가]

                cursor.execute(query, data)
                logger.debug(
                    f"매수후고가갱신 cursor.rowcount: {cursor.rowcount}, SYS_ID: {SYS_ID}, 거래환경: {거래환경}"
                )
                cursor.close()

        except Exception as e:
            클래스이름 = self.__class__.__name__
            함수이름 = get_funcname()
            logger.error("%s-%s %s: %s" % (클래스이름, 함수이름, get_linenumber(), e))

    def 포트폴리오종목동기화(self, P):
        retUUID = ""
        GG_NM = ""
        try:
            with (
                get_db_connection() as conn
            ):  # with 문을 사용하여 자동으로 conn.close()
                cursor = conn.cursor()
                if P.수량 > 0:
                    # 계좌정보에서 포트폴리오 생성
                    if len(P.종목명) == 0 and len(P.매수일) == 0:
                        # 종목명구하기
                        selQuery = """SELECT GG_NM, UUID, 주문시각, 종목명 
                        FROM 거래주문내역 
                        WHERE 1=1
                        AND 거래환경 = ? 
                        AND GG_NM = ? 
                        AND 종목코드 = ?
                        AND   매매구분 = '매수' 
                        ORDER BY 주문시각 DESC 
                        LIMIT 1"""
                        params = (거래환경, GG_NM, P.종목코드)
                        # print("종목명, 매수일 없는 P 경우 selQuery", selQuery)
                        df = pdsql.read_sql_query(selQuery, con=conn, params=params)
                        # print(f"동기화 거래주문내역 : {df}")
                        # 주문내역이 있으면 사용
                        if not df.empty:
                            P.매수일 = str(
                                df.loc[0, "주문시각"]
                            )  # 포트폴리오 읽기에서 이형식으로 읽는다. 읽기형식 %F제거
                            # 매수일 2022-10-07 10:14:16.541638 Unexpected err=ValueError('unconverted data remains: .541638')
                            # strptime() argument 1 must be str
                            P.매수일 = dt.datetime.strptime(
                                P.매수일, "%Y-%m-%d %H:%M:%S"
                            )
                            P.종목명 = df.loc[0, "종목명"]
                            GG_NM = df.loc[0, "GG_NM"]
                            retUUID = df.loc[0, "UUID"]
                        # 거래주문내역 없으면 증권사 계좌정보 사용
                        else:
                            selQuery = (
                                "SELECT 종목명 FROM 종목코드 WHERE 종목코드 =  '%s'"
                                % P.종목코드
                            )
                            # print("거래주문내역 없으면 selQuery ", selQuery)
                            df = pdsql.read_sql_query(selQuery, con=conn)
                            P.매수일 = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            P.종목명 = df.loc[0, "종목명"]
                            GG_NM = GG_NM
                            retUUID = self.UUID

                    else:
                        GG_NM = GG_NM
                        retUUID = self.UUID
                        P.매수일 = str(P.매수일)[:19]

                        # logger.debug(  'P.종목코드 %s, P.종목명 %s' %(P.종목코드, P.종목명))
                    query = SYNC_포트폴리오
                    data = [
                        retUUID,
                        GG_NM,
                        P.종목코드,
                        P.매수일,
                        P.종목코드,
                        P.종목명,
                        P.매수가,
                        P.수량,
                        int(P.매수후고가),
                        P.STATUS,
                        SYS_ID,
                    ]

                else:
                    query = PORTFOLIO_DELETE_BY_KEY
                    data = [self.UUID, SYS_ID, P.종목코드]
                cursor.execute(query, data)
                logger.debug(
                    f"포트폴리오종목동기화 cursor.rowcount: {cursor.rowcount}, SYS_ID: {SYS_ID}, 거래환경: {거래환경}, data :{data}"
                )
                cursor.close()
        except Exception as e:
            클래스이름 = self.__class__.__name__
            함수이름 = get_funcname()
            logger.error("%s-%s %s: %s" % (클래스이름, 함수이름, get_linenumber(), e))

        # 실시간 가격정보 전송요청을 위해
        return retUUID

    def moveHalfOverAIToGuardian(self, GuardianGoose):
        logger.debug(
            f"moveHalfOverAIToGuardian GuardianGoose: {GuardianGoose}, SYS_ID: {SYS_ID}, 거래환경: {거래환경}"
        )
        try:
            # AIGoose의 현제 종목수 > 포트제한수/2
            # 초과 되는 포트는 GuardianGoose로 강제이송.
            # halfOverCnt = len(self.portfolio.items()) - (self.포트폴리오수//2)
            # GuardianGoose 기준으로 이동갯수 확정.
            halfOverCnt = int(
                GuardianGoose.포트폴리오수  # + (GuardianGoose.포트폴리오수 / 2)
            ) - len(GuardianGoose.portfolio.items())
            logger.info(f"[{self.__class__.__name__}] halfOverCnt : {halfOverCnt}")
            if halfOverCnt > 0 and isOverCloseTime():
                # items()가 반환하는 데이터 예시
                items = self.portfolio.items()
                sorted_items = sorted(
                    items, key=lambda item: item[1].매수일
                )  # item[1]은 value를 의미
                logger.info(
                    f"[{self.__class__.__name__}] sorted_items : {sorted_items}"
                )
                # n개 뽑아내기
                top_n_items = sorted_items[:halfOverCnt]
                logger.info(f"[{self.__class__.__name__}] top_n_items : {top_n_items}")
                with (
                    get_db_connection() as conn
                ):  # with 문을 사용하여 자동으로 conn.close()
                    cursor = conn.cursor()
                    for k, v in top_n_items:
                        try:
                            logger.info(
                                f"[{self.__class__.__name__}] params : {GuardianGoose.UUID}, {GuardianGoose.Name}, {v.종목코드}"
                            )
                            cursor.execute(
                                """UPDATE 포트폴리오 
                                        SET GG_ID="%s", GG_NM="%s" 
                                        WHERE 포트폴리오키 = "%s" AND SYS_ID = '%d'
                                            """
                                % (
                                    GuardianGoose.UUID,
                                    GuardianGoose.Name,
                                    v.종목코드,
                                    SYS_ID,
                                )
                            )
                        except Exception as e:
                            query = "insert or replace into 포트폴리오(GG_ID, GG_NM, 포트폴리오키, SYS_ID) values (?, ?, ?, ?)"
                            data = (
                                GuardianGoose.UUID,
                                GuardianGoose.Name,
                                v.종목코드,
                                SYS_ID,
                            )
                            cursor.execute(query, data)
                            self.포트폴리오종목삭제(v.종목코드)

                    logger.debug(
                        f"moveHalfOverAIToGuardian cursor.rowcount: {cursor.rowcount}, SYS_ID: {SYS_ID}, 거래환경: {거래환경}"
                    )
                    cursor.close()
        except Exception as e:
            클래스이름 = self.__class__.__name__
            함수이름 = get_funcname()
            logger.error("%s-%s %s: %s" % (클래스이름, 함수이름, get_linenumber(), e))

    def deleteGooseSaveHist(self):
        try:
            with (
                get_db_connection() as conn
            ):  # with 문을 사용하여 자동으로 conn.close()
                cursor = conn.cursor()
                cursor.execute(
                    """
                                DELETE FROM TB_GOOSE 
                                WHERE ID NOT IN(
                                    SELECT ID
                                    FROM TB_GOOSE 
                                    ORDER BY ID DESC
                                    LIMIT 3
                                )
                                """
                )
                logger.debug(
                    f"deleteGooseSaveHist cursor.rowcount: {cursor.rowcount}, SYS_ID: {SYS_ID}, 거래환경: {거래환경}"
                )
                cursor.close()
        except Exception as e:
            클래스이름 = self.__class__.__name__
            함수이름 = get_funcname()
            logger.error("%s-%s %s: %s" % (클래스이름, 함수이름, get_linenumber(), e))

    def 최근거래주문내역(self, 거래환경, 매매구분, 포트폴리오키):
        # print(f"GG_NM : [{self.Name}]")
        df = None
        try:
            with (
                get_db_connection() as conn
            ):  # with 문을 사용하여 자동으로 conn.close()
                selQuery = """SELECT GG_NM, UUID, 매수일, 주문시각, 종목명, 수량, 
                    (SELECT MAX(고가) AS 고가
					FROM 일별주가
					WHERE 종목코드 = A.종목코드
					AND 날짜 >= strftime('%Y%m%d', DATE(A.주문시각, '-1 day'))) AS 고가
                FROM 거래주문내역 A
                WHERE 1=1
                AND 거래환경 = ?
                AND 종목코드 =  ?
                AND 매매구분 = ? 
                ORDER BY 주문시각 DESC 
                LIMIT 1 """

                params = (거래환경, 포트폴리오키, 매매구분)
                # 실행 중이면 오늘 것만 한정하여
                # 최근 1개만 가져옴.
                # if self.running :
                #    selQuery = selQuery.replace("ORDER"," AND strftime('%%Y-%%m-%%d',주문시각) = DATE('now', '+9 hours') ORDER ")
                df = pdsql.read_sql_query(selQuery, con=conn, params=params)
            logger.debug(
                f"최근거래 주문내역 selQuery : {selQuery} df: {df}, SYS_ID: {SYS_ID}, 거래환경: {거래환경}"
            )
        except Exception as e:
            클래스이름 = self.__class__.__name__
            함수이름 = get_funcname()
            logger.error("%s-%s %s: %s" % (클래스이름, 함수이름, get_linenumber(), e))

        # 실시간 가격정보 전송요청을 위해
        return df

    def 최근거래주문고가(self, 포트폴리오키):
        # print(f"GG_NM : [{self.Name}]")
        df = None
        try:
            with (
                get_db_connection() as conn
            ):  # with 문을 사용하여 자동으로 conn.close()
                selQuery = """
                            SELECT 고가
                            FROM 일별주가
                            WHERE 종목코드 = ?
                            ORDER BY 날짜 DESC 
                            LIMIT 1
                            """

                params = (포트폴리오키,)  # 단일요소인경우 전달방법, 붙이기

                df = pdsql.read_sql_query(selQuery, con=conn, params=params)
            logger.debug(
                f"최근거래주문고가 selQuery : {selQuery} df: {df}, SYS_ID: {SYS_ID}"
            )
        except Exception as e:
            클래스이름 = self.__class__.__name__
            함수이름 = get_funcname()
            logger.error("%s-%s %s: %s" % (클래스이름, 함수이름, get_linenumber(), e))

        # 실시간 가격정보 전송요청을 위해
        return df

    # 미사용
    def 최근거래주문내역들(self, 거래환경, 매매구분):
        df = None
        try:
            with (
                get_db_connection() as conn
            ):  # with 문을 사용하여 자동으로 conn.close()
                selQuery = """SELECT 종목코드 --GG_NM, UUID, 매수일, 주문시각, 종목명, 수량 
                FROM 거래주문내역 
                WHERE 1=1
                AND 거래환경='%s' 
                AND GG_NM='%s' 
                AND 매매구분 = '%s' 
                ORDER BY 주문시각 DESC LIMIT 20 """ % (
                    거래환경,
                    self.Name,
                    매매구분,
                )

                # 실행 중이면 오늘 것만 한정하여
                if self.running:
                    selQuery = selQuery.replace(
                        "ORDER",
                        " AND strftime('%Y-%m-%d',주문시각) = DATE('now', '+9 hours') ORDER ",
                    )

                df = pdsql.read_sql_query(selQuery, con=conn)
            logger.debug(
                f"최근거래 주문내역들 selQuery : {selQuery} df: {df}, SYS_ID: {SYS_ID}, 거래환경: {거래환경}"
            )
        except Exception as e:
            클래스이름 = self.__class__.__name__
            함수이름 = get_funcname()
            logger.error("%s-%s %s: %s" % (클래스이름, 함수이름, get_linenumber(), e))

        # 실시간 가격정보 전송요청을 위해
        return df

    def Goose별주문이력조회(self, 거래환경, 매매구분, 포트폴리오키):
        df = None
        try:
            with (
                get_db_connection() as conn
            ):  # with 문을 사용하여 자동으로 conn.close()
                # Goose별 종목 주문이력 조회
                selQuery = """SELECT * FROM( 
                SELECT GG_NM, UUID, 주문시각, 종목명 , 매수가, 수량 
                FROM 거래주문내역 
                WHERE 1=1 
                AND 거래환경='%s' 
                AND GG_NM = 'AIGoose' 
                AND 종목코드 =  '%s' 
                AND 매매구분 = '%s' 
                ORDER BY 주문시각 DESC LIMIT 1 
                ) UNION  SELECT * FROM  ( 
                SELECT GG_NM, UUID, 주문시각, 종목명 , 매수가, 수량 
                FROM 거래주문내역 
                WHERE 1=1 
                AND 거래환경='%s' 
                AND GG_NM = 'GuardianGoose' 
                AND 종목코드 =  '%s' 
                AND 매매구분 = '%s' 
                ORDER BY 주문시각 DESC LIMIT 1 )""" % (
                    거래환경,
                    포트폴리오키,
                    매매구분,
                    거래환경,
                    포트폴리오키,
                    매매구분,
                )

                # 실행 중이면 오늘 것만 한정하여
                if self.running:
                    selQuery = selQuery.replace(
                        "ORDER",
                        " AND strftime('%Y-%m-%d',주문시각) = DATE('now', '+9 hours') ORDER ",
                    )

                df = pdsql.read_sql_query(selQuery, con=conn)
            logger.debug(
                f"Goose별주문이력조회 selQuery : {selQuery} df: {df}, SYS_ID: {SYS_ID}, 거래환경: {거래환경}"
            )
        except Exception as e:
            클래스이름 = self.__class__.__name__
            함수이름 = get_funcname()
            logger.error("%s-%s %s: %s" % (클래스이름, 함수이름, get_linenumber(), e))
        return df

    def 포트폴리오종목삭제(self, 포트폴리오키):
        try:
            with (
                get_db_connection() as conn
            ):  # with 문을 사용하여 자동으로 conn.close()
                cursor = conn.cursor()
                query = PORTFOLIO_DELETE_BY_KEY
                data = [self.UUID, SYS_ID, 포트폴리오키]
                # logger.info( ' data : %s' % data)
                cursor.execute(query, data)
                logger.debug(
                    f"포트폴리오종목삭제 cursor.rowcount: {cursor.rowcount}, SYS_ID: {SYS_ID}, 거래환경: {거래환경}"
                )
                cursor.close()
        except Exception as e:
            클래스이름 = self.__class__.__name__
            함수이름 = get_funcname()
            logger.error("%s-%s %s: %s" % (클래스이름, 함수이름, get_linenumber(), e))

    def 체결기록(self, data):
        try:
            with (
                get_db_connection() as conn
            ):  # with 문을 사용하여 자동으로 conn.close()
                query = """insert into 거래결과(거래환경, GG_NM, UUID, 일자, 체결시각, 종목코드, 종목명, 매매구분, 주문번호, 체결번호, 주문수량, 주문가격, 체결수량, 체결가격, 주문평균체결가격) 
                            values            (      ?,    ?,    ?,    ?,      ?,           ?,     ?,      ?,       ?,       ?,      ?,       ?,       ?,      ?,             ?)"""
                cursor = conn.cursor()
                cursor.execute(query, tuple(data.values()))
                logger.debug(
                    f"체결기록 cursor.rowcount: {cursor.rowcount}, SYS_ID: {SYS_ID}, 거래환경: {거래환경}, data:{data}"
                )
                cursor.close()
        except Exception as e:
            클래스이름 = self.__class__.__name__
            함수이름 = get_funcname()
            logger.error("%s-%s %s: %s" % (클래스이름, 함수이름, get_linenumber(), e))

    @classmethod
    def get_market_info(cls, accInfo, szTrCode):
        """
         Xing API 및 초기화 레이턴시 대응:
        시장 정보(지수) 데이터가 아직 수신되지 않은 상태(None)에서의 KeyError 원천 차단.
        """
        market_tp_dic = {}
        market_info = {
            "시장구분": 0,
            "전일지수": 0.0,
            "현재지수": 0.0,
            "시가지수": 0.0,
            "저가지수": 0.0,
            "고가지수": 0.0,
            "전일대비구분": "0",
            "지수등락율": 0.0,
            "거래량전일대비": 0.0,
            "거래대금전일대비": 0.0,
            "시가등락율": 0.0,
            "시가시간": "0",
            "고가등락율": 0.0,
            "고가시간": "0",
            "저가등락율": 0.0,
            "저가시간": "0",
            "첫번째등락율": 0.0,
            "두번째등락율": 0.0,
            "세번째등락율": 0.0,
            "네번째등락율": 0.0,
            "상승종목수": 0,
            "보합종목수": 0,
            "하락종목수": 0,
        }

        # [Safety] accInfo(View_계좌정보조회)가 None인 경우 대응
        if not accInfo:
            return market_info

        # [Safety] 시장구분 매핑 및 원본 사전(ActiveX 수신 데이터) 확보
        if szTrCode == "S3_" and getattr(accInfo, "kospiDic", None) is not None:
            market_tp_dic = accInfo.kospiDic
            market_info["시장구분"] = 1
        elif szTrCode == "K3_" and getattr(accInfo, "kosdaqDic", None) is not None:
            market_tp_dic = accInfo.kosdaqDic
            market_info["시장구분"] = 2

        # [Logic] 데이터가 있는 경우에만 덮어쓰기 (Safe Parsing)
        if market_tp_dic:
            try:

                def to_f(k):
                    return float(market_tp_dic.get(k, 0.0))

                def to_s(k):
                    return str(market_tp_dic.get(k, "0"))

                def to_i(k):
                    return int(market_tp_dic.get(k, 0))

                market_info.update(
                    {
                        "전일지수": to_f("전일지수"),
                        "현재지수": to_f("현재지수"),
                        "시가지수": to_f("시가지수"),
                        "저가지수": to_f("저가지수"),
                        "고가지수": to_f("고가지수"),
                        "전일대비구분": to_s("전일대비구분"),
                        "지수등락율": to_f("지수등락율"),
                        "거래량전일대비": to_f("거래량전일대비"),
                        "거래대금전일대비": to_f("거래대금전일대비"),
                        "시가등락율": to_f("시가등락율"),
                        "시가시간": to_s("시가시간"),
                        "고가등락율": to_f("고가등락율"),
                        "고가시간": to_s("고가시간"),
                        "저가등락율": to_f("저가등락율"),
                        "저가시간": to_s("저가시간"),
                        "첫번째등락율": to_f("첫번째등락율"),
                        "두번째등락율": to_f("두번째등락율"),
                        "세번째등락율": to_f("세번째등락율"),
                        "네번째등락율": to_f("네번째등락율"),
                        "상승종목수": to_i("상승종목수"),
                        "보합종목수": to_i("보합종목수"),
                        "하락종목수": to_i("하락종목수"),
                    }
                )
            except (ValueError, TypeError) as e:
                # 데이터 파싱 에러 시 기본값 유지하며 로그만 기록
                logger.error(f"Error parsing market info: {e}")

        # [Logic] 현재 시각 추가 (saveOrderHist 등에서 사용)
        market_info["현재시각"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        return market_info

    def 주문기록(self, data):
        """
        거래주문내역 테이블 삽입 레이어
        """
        try:
            # 데이터 길이 및 내용 검증 (37개 컬럼)
            if len(data) != 37:
                logger.error(
                    f"❌ [Order-Hist-Draft] 데이터 길이 불일치! Expected 37, Got {len(data)}"
                )
                return

            with get_db_connection() as conn:
                query = """INSERT INTO 거래주문내역(
                    거래환경, GG_NM, UUID, 주문시각, 종목코드, 종목명, 매매구분, 
                    매수가, 매도가, 수량, 매수일, 수익금액, 체결강도, 등락율, 
                    전일지수, 현재지수, 시가지수, 저가지수, 고가지수, 전일대비구분, 
                    지수등락율, 거래량전일대비, 거래대금전일대비, 시가시간, 시가등락율, 
                    고가시간, 고가등락율, 저가시간, 저가등락율, 첫번째등락율, 
                    두번째등락율, 세번째등락율, 네번째등락율, 상승종목수, 보합종목수, 
                    하락종목수, 시장구분
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

                cursor = conn.cursor()
                cursor.execute(query, data)
                conn.commit()

                logger.info(
                    f"✅ [Order-Hist-Saved] 종목: {data[4]}({data[5]}) | 유형: {data[6]} | 수량: {data[9]} | 시각: {data[3]}"
                )
                cursor.close()
        except Exception as e:
            logger.error(f"❌ [Order-Hist-System-Error] {get_linenumber()}: {e}")
            logger.error(f"Failed Data: {data}")

    def 뉴스등록(self, data):
        try:
            with (
                get_db_connection() as conn
            ):  # with 문을 사용하여 자동으로 conn.close()
                # logger.debug( 종목코드 %s, 제목 %s ' %( data[4], data[5]))
                query = "insert into 뉴스(날짜, 시간, 뉴스구분자, 키값, 종목코드, 제목, BODY길이) values (?, ?, ?, ?, ?, ?, ?)"
                cursor = conn.cursor()
                cursor.execute(query, data)
                cursor.close()
        except Exception as e:
            클래스이름 = self.__class__.__name__
            함수이름 = get_funcname()
            logger.error("%s-%s %s: %s" % (클래스이름, 함수이름, get_linenumber(), e))

    def 뉴스종목등록(self, data):
        regStockCodes = []

        try:
            종목코드들 = data[4]
            idx = 0
            with (
                get_db_connection() as conn
            ):  # with 문을 사용하여 자동으로 conn.close()
                cursor = conn.cursor()
                for i in range(0, int(len(data[4]) / 6)):
                    newStockCode = 종목코드들[0 + int(idx) : 6 + int(idx)]

                    if newStockCode != "000000":
                        query = "insert into 뉴스종목(날짜, 시간, 키값, 종목코드) values (?, ?, ?, ?)"
                        data = [data[0], data[1], data[2], newStockCode]
                        cursor.execute(query, data)
                        regStockCodes.append(newStockCode)
                    idx += 6
                cursor.close()
        except Exception as e:
            클래스이름 = self.__class__.__name__
            함수이름 = get_funcname()
            logger.error("%s-%s %s: %s" % (클래스이름, 함수이름, get_linenumber(), e))
        return regStockCodes

    def 뉴스검색결과(self, p종목코드):
        # 3일전 이후
        검색시작일자 = (dt.datetime.now() - dt.timedelta(days=3)).strftime("%Y%m%d")

        try:
            with (
                get_db_connection() as conn
            ):  # with 문을 사용하여 자동으로 conn.close()
                query = (
                    "SELECT 날짜, 시간, 키값, 종목코드 FROM 뉴스종목 WHERE 종목코드 LIKE '%s' AND 날짜 >= '%s' ORDER BY 키값 DESC "
                    % ("%" + p종목코드 + "%", 검색시작일자)
                )
                # print(query)
                df = pdsql.read_sql_query(query, con=conn)

            if df.empty:
                return False
            return True

        except Exception as e:
            클래스이름 = self.__class__.__name__
            함수이름 = get_funcname()
            logger.error("%s-%s %s: %s" % (클래스이름, 함수이름, get_linenumber(), e))

    def 뉴스삭제(self):
        # 5일전 이 삭제
        검색시작일자 = (dt.datetime.now() - dt.timedelta(days=3)).strftime("%Y%m%d")

        try:
            with (
                get_db_connection() as conn
            ):  # with 문을 사용하여 자동으로 conn.close()
                cursor = conn.cursor()

                query = "DELETE FROM 뉴스 WHERE 날짜 < ? "
                data = [검색시작일자]
                cursor.execute(query, data)
                logger.debug(f"뉴스 {cursor.rowcount}건 삭제 성공 !!!")

                query = "DELETE FROM 뉴스종목 WHERE 날짜 < ? "
                data = [검색시작일자]
                cursor.execute(query, data)
                cursor.close()
                logger.debug(f"뉴스종목 {cursor.rowcount}건 삭제 성공 !!!")

        except Exception as e:
            클래스이름 = self.__class__.__name__
            함수이름 = get_funcname()
            logger.error("%s-%s %s: %s" % (클래스이름, 함수이름, get_linenumber(), e))

    def 거래주문내역삭제(self):
        # 5일전 이 삭제
        검색시작일자 = (dt.datetime.now() - dt.timedelta(days=120)).strftime("%Y-%m-%d")

        try:
            with (
                get_db_connection() as conn
            ):  # with 문을 사용하여 자동으로 conn.close()
                logger.debug("검색시작일자 : %s" % 검색시작일자)
                cursor = conn.cursor()

                query = "DELETE FROM 거래결과 WHERE 일자 < ? "  # strftime('%Y-%m-%d', 'now',  '-120 days')
                data = [검색시작일자]
                cursor.execute(query, data)

                query = "DELETE FROM 거래주문내역 WHERE 주문시각 < ? "  # strftime('%Y-%m-%d', 'now',  '-120 days')
                data = [검색시작일자]
                cursor.execute(query, data)
                logger.debug("거래주문내역삭제 성공 !!!")

                cursor.close()
                logger.debug("분별작업정보삭제 성공 !!!")

        except Exception as e:
            클래스이름 = self.__class__.__name__
            함수이름 = get_funcname()
            logger.error("%s-%s %s: %s" % (클래스이름, 함수이름, get_linenumber(), e))

    def 분별작업정보삭제(self):
        # 5일전 이 삭제
        검색시작일자 = (dt.datetime.now() - dt.timedelta(days=30)).strftime("%Y-%m-%d")

        try:
            with (
                get_db_connection() as conn
            ):  # with 문을 사용하여 자동으로 conn.close()
                logger.debug("검색시작일자 : %s" % 검색시작일자)
                cursor = conn.cursor()

                query = "DELETE FROM 검색종목 WHERE 등록일시 < ? AND SYS_ID = ?"  # strftime('%Y-%m-%d', 'now',  '-120 days')
                data = [검색시작일자, SYS_ID]
                cursor.execute(query, data)

                query = "DELETE FROM 분별주가 WHERE 등록일자 < ? "  # strftime('%Y-%m-%d', 'now',  '-120 days')
                data = [검색시작일자]
                cursor.execute(query, data)

                query = "DELETE FROM TB_TRADE_SIGNAL WHERE 예측일자 < ? "  # strftime('%Y-%m-%d', 'now',  '-120 days')
                data = [검색시작일자]
                cursor.execute(query, data)

                cursor.close()
        except Exception as e:
            클래스이름 = self.__class__.__name__
            함수이름 = get_funcname()
            logger.error("%s-%s %s: %s" % (클래스이름, 함수이름, get_linenumber(), e))

    def 환율가격정보삭제(self):
        # 5백테스트: 과거 데이터를 사용하여 신호의 성과를 검증할 때, 충분한 데이터가 필요함! 일반적으로 최소 1개월 이상의 데이터가 필요하며, 가능하다면 3개월 이상의 데이터를 사용하는 것이 좋습니다.
        검색시작일자 = (dt.datetime.now() - dt.timedelta(days=90)).strftime("%Y-%m-%d")

        try:
            with (
                get_db_connection() as conn
            ):  # with 문을 사용하여 자동으로 conn.close()
                logger.debug("검색시작일자 : %s" % 검색시작일자)
                cursor = conn.cursor()

                query = "DELETE FROM TB_EXCHANGE WHERE reg_dttm < ? "  # strftime('%Y-%m-%d', 'now',  '-120 days')
                data = [검색시작일자]
                cursor.execute(query, data)
                cursor.close()
                logger.debug("환율가격정보삭제 성공 !!!")

        except Exception as e:
            클래스이름 = self.__class__.__name__
            함수이름 = get_funcname()
            logger.error("%s-%s %s: %s" % (클래스이름, 함수이름, get_linenumber(), e))

    def saveSearchStock(self, code, GEN_GG_TP, status):
        for goose in self.parent.gooses[:2]:  # 0번과 1번 Goose만 확인
            if code in goose.금일매도종목 or code in goose.portfolio:
                return

        # 현재시각 = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        현재일자 = dt.datetime.now().strftime("%Y-%m-%d")
        with get_db_connection() as conn:  # with 문을 사용하여 자동으로 conn.close()
            query = """insert or replace into 검색종목(종목코드, 종목명, GEN_GG_TP, 등록일시, 매매구분, SYS_ID)
              values (?, (SELECT 종목명
				FROM 종목코드
				WHERE 종목코드 = ?), ?, ?, ?, ?)"""
            data = [code, code, GEN_GG_TP, 현재일자, status, SYS_ID]
            cursor = conn.cursor()
            cursor.execute(query, data)
            logger.debug(
                f"saveSearchStock cursor.rowcount: {cursor.rowcount}, SYS_ID: {SYS_ID}, 거래환경: {거래환경}"
            )
            cursor.close()

    # 분별주가 검색대상 제외

    # 매수 조건 설정: 변동폭과 평균의 결합 평가
    # 현재가가 평균보다 크고 변동폭이 평균보다 낮을 때 매수 조건 성립
    def saveOrderHist(
        self,
        market_code,
        market_info,
        종목코드,
        종목명,
        거래유형,
        매수가,
        매도가,
        매도수량,
        매수일,
        평가손익,
        result=None,
    ):
        """
        주문 히스토리 데이터 재구성 레이어
        """
        try:
            # market_info 무결성 체크 및 복원
            parent = getattr(self, "parent", None)

            if not market_info:
                try:
                    accInfo = parent.dialog["계좌정보조회"] if parent else None
                    market_info = self.get_market_info(accInfo, market_code)
                except Exception:
                    market_info = self.get_market_info(None, market_code)

            # 🛡️ [: Data Pollution Protection] 부모가 없으면 "모의" 환경으로 강무
            env_mode = "모의"
            if parent and hasattr(parent, "account_dict"):
                env_mode = parent.account_dict.get("거래환경", "모의")

            data = [
                env_mode,
                self.Name,
                self.UUID,
                market_info.get(
                    "현재시각", dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ),
                종목코드,
                종목명,
                거래유형,
                int(to_numeric_safe(매수가) or 0),
                int(to_numeric_safe(매도가) or 0),
                int(to_numeric_safe(매도수량) or 0),
                str(매수일),
                int(to_numeric_safe(평가손익) or 0),
                result.get("체결강도", 0) if isinstance(result, dict) else 0,
                result.get("등락율", 0) if isinstance(result, dict) else 0,
                market_info.get("전일지수", 0.0),
                market_info.get("현재지수", 0.0),
                market_info.get("시가지수", 0.0),
                market_info.get("저가지수", 0.0),
                market_info.get("고가지수", 0.0),
                market_info.get("전일대비구분", "0"),
                market_info.get("지수등락율", 0.0),
                market_info.get("거래량전일대비", 0.0),
                market_info.get("거래대금전일대비", 0.0),
                market_info.get("시가시간", ""),
                market_info.get("시가등락율", 0.0),
                market_info.get("고가시간", ""),
                market_info.get("고가등락율", 0.0),
                market_info.get("저가시간", ""),
                market_info.get("저가등락율", 0.0),
                market_info.get("첫번째등락율", 0.0),
                market_info.get("두번째등락율", 0.0),
                market_info.get("세번째등락율", 0.0),
                market_info.get("네번째등락율", 0.0),
                market_info.get("상승종목수", 0),
                market_info.get("보합종목수", 0),
                market_info.get("하락종목수", 0),
                market_info.get("시장구분", 0),
            ]

            self.주문기록(data=data)

        except Exception as e:
            logger.error(f"❌ [Save-Order-Hist-Error] {get_linenumber()}: {e}")
            logger.error(f"Args: {종목코드}, {거래유형}, {매수가}, {매도수량}")


# accInfo = self.parent.dialog['계좌정보조회']#다른 쓰레드에서 쓰고있을 때 lock방지용
def goose_loader():
    return None
