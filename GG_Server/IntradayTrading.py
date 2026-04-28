import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "GG_Shared"))
)
import threading
import time as _tm
from pathlib import Path
from collections import deque, defaultdict
from datetime import datetime as _dt

import pandas as pd

# [ZMQ] Imports
from GG_Shared.util.zmq_manager import (
    ZMQSubscriber,
    ZMQPushPull,
    TOPIC_EVENT,
    TOPIC_TICK,
    TOPIC_ORDER_RESULT,
)
from config.comm_settings import ZMQ_STATUS_PORT
from config.ai_settings import (
    MIN_TERM,
    MIN_TERM_CD,
    MIN_TERM_GAIN_STK_CNT,
    BUY_TOP_CNT,
    SYS_ID,
    PARAMS,
)
from config.log_settings import setup_logger
from SQL.sql import (
    GET_BUYABLE_AMT,
    GET_PORTFOLIO_LIST,
    GET_PRED_TRGT_STK_LIST,
    GET_STOCK_LIST_300,
    GET_TODAY_LOSS_STOCKS,
    UPDATE_BUY_READY_STAT,
)
from GG_Shared.util.CommUtils import (
    set_commutils_logger,
    to_numeric_safe,
    AccountGuard,
    get_db_connection,
    parse_avail_cash,
)
from config.telegram_setting import ToTelegram

# [Modularized Managers]
from managers.RiskManager import PortfolioRiskManager, set_logger as set_risk_logger
from managers.EventLoopManager import EventLoopManager
from strategy.core.execution_pipeline import ExecutionPipeline
from util.trading_db_handler import (
    fetch_active_positions,
    execute_batch_updates,
    fetch_datas,
)
from strategy.indicators.market_analysis import (
    get_market_momentum_status,
    set_logger as set_market_logger,
)
from managers.adaptive_trade_manager import AdaptiveTradeManager
from managers.OrderManager import OrderManager
from util.async_executor import DBAsyncWriter

# from gooses.lock_controllers import StockRedCardController, PortfolioManager
from managers.StateManager import StateManager
from managers.intraday_state_manager import IntradayStateManager
from SQL.sql import SET_BUY_READY_STK_ITEM

# Global Logger Setup
logger = setup_logger(Path(__file__).stem)
set_commutils_logger(logger)
set_risk_logger(logger)
set_market_logger(logger)

# Robust sys.path Injection
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


class IntradayTrading(object):
    """
    [Orchestrator] IntradayTrading Engine
    Modularized architecture for high-performance and zero-defect trading.
    """

    def __init__(self, test_mode=False):
        self._stop_event = threading.Event()
        self.test_mode = test_mode
        self.sys_id = SYS_ID
        self.system_params = PARAMS
        self.logger = logger
        # State Management
        self.portfolio_list = []
        self.active_positions_map = {}
        self.realtime_tick_cache = {}
        self.trade_signal_cache = {}
        self.shadow_portfolio = {}
        self.active_smart_orders = {}
        self._pending_sell_stocks = {}
        self._pending_buy_stocks = {}
        self.cooldown_orders = {}
        self.tick_lock = threading.Lock()
        self.capital_lock = threading.Lock()
        self.order_lock = threading.Lock()
        self.cl_ord_lock = threading.Lock()
        self.pending_cl_orders = {}

        # Accounting
        self.account_info = None
        self.virtual_avail_cash = 0.0
        self.avail_cash = 0.0
        self.total_equity = 0.0
        self.buyable_amt = 0.0
        self.last_buy_order_time = 0.0

        # Locks & Events
        self.doit_lock = threading.Lock()
        self.analysis_event = threading.Event()
        self._is_doing_it = False
        self._pending_ticker = None
        self._pending_market_avg = None

        # [Root State] Unified State Management Instance
        self.state_manager = StateManager()

        # Managers Initialization
        self.risk_manager = PortfolioRiskManager()
        self.event_manager = EventLoopManager(self)
        self.execution_pipeline = ExecutionPipeline(self)
        self.adaptive_manager = AdaptiveTradeManager()
        self.order_service = OrderManager(self)
        self.shadow_manager = IntradayStateManager(self)
        self.db_writer = DBAsyncWriter(execute_batch_updates)
        self.account_guard = AccountGuard(start_equity=0)
        # self.red_card_controller = StockRedCardController()
        # self.portfolioManager = PortfolioManager(
        #    total_slots=PARAMS.get("max_positions", 5)
        # )

        # Communication
        try:
            self.zmq_sub = ZMQSubscriber(
                topics=[TOPIC_EVENT, TOPIC_TICK, TOPIC_ORDER_RESULT]
            )
            self.zmq_push = ZMQPushPull(mode="PUSH")
            # [Step 2.2] Moved to shadow_manager.listen_state_feedback()
            # self.zmq_pull = ZMQPushPull(mode="PULL", port=ZMQ_STATUS_PORT)
        except Exception as e:
            logger.error(f"[IntradayTrading] ZMQ Init Failed: {e}")

        # Cache for stock list
        self._stock_list_300_cache = None
        self._stock_list_300_date = None
        self.tick_log_count = 0
        self.cpower_history = defaultdict(lambda: deque(maxlen=3))

    def initialize(self):
        logger.info("🚀 [Orchestrator] Initializing IntradayTrading Engine...")
        self.target_stk_init()
        self.load_initial_state()
        self.risk_manager.preload_beta_cache()
        self.start_analysis_worker()
        self.event_manager.start_listener()
        self.shadow_manager.listen_state_feedback()
        self.schedule_shutdown()
        self.gen_trade_signal("ALL")

    def target_stk_init(self):
        try:
            with get_db_connection() as conn:
                conn.execute(SET_BUY_READY_STK_ITEM, (SYS_ID, SYS_ID, SYS_ID))
                conn.commit()
        except Exception as e:
            logger.error(
                f"❌ [Target Stk Init] Failed to initialize target stocks: {e}"
            )

    def load_initial_state(self):
        self.active_positions_map = fetch_active_positions(self.sys_id)
        # [Zero-Defect] DB에서 마지막 가용 자산 로드하여 Cold-Start 방어
        try:
            with get_db_connection() as conn:
                df_capital = pd.read_sql_query(GET_BUYABLE_AMT, conn)
                init_cash = 0
                if not df_capital.empty:
                    init_cash = to_numeric_safe(df_capital.iloc[0, 0])

                # [Hardening] DB 데이터가 없거나 0원인 경우 시스템 기본값(1억)으로 초기화하여 엔진 정지 방지
                if init_cash <= 0:
                    init_cash = 100000000.0
                    logger.info(
                        f"💰 [Cold-Start] Initializing with default capital: {init_cash:,.0f}원"
                    )
                else:
                    logger.info(
                        f"💰 [Cold-Start] Loaded initial capital from DB: {init_cash:,.0f}원"
                    )

                self.avail_cash = init_cash
                self.virtual_avail_cash = init_cash
                self.total_equity = init_cash
                self.buyable_amt = init_cash
        except Exception as e:
            logger.error(f"❌ [Cold-Start] Failed to load capital from DB: {e}")
            # Fatal fallback
            self.virtual_avail_cash = 100000000.0

        logger.info(f"✅ Loaded {len(self.active_positions_map)} active positions.")

    def start_analysis_worker(self):
        def worker():
            while not self._stop_event.is_set():
                if self.analysis_event.wait(timeout=1.0):
                    self.analysis_event.clear()
                    self.doIT(self._pending_ticker)

        threading.Thread(target=worker, name="AnalysisWorker", daemon=True).start()

    def gen_trade_signal(self, ticker=None):
        self._pending_ticker = ticker
        self.analysis_event.set()

    def doIT(self, ticker=None):
        if not self.doit_lock.acquire(blocking=False):
            return
        try:
            self._is_doing_it = True
            self._sync_account_info()

            # 1. DB Refresh & Portfolio Sync
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(UPDATE_BUY_READY_STAT, [self.sys_id, self.sys_id])
                self.dfPortfolio = pd.read_sql_query(
                    GET_PORTFOLIO_LIST % self.sys_id, conn
                )
                self.portfolio_list = (
                    self.dfPortfolio["종목코드"].astype(str).tolist()
                    if not self.dfPortfolio.empty
                    else []
                )

                # 2. Market Analysis
                if not self._stock_list_300_cache:
                    self._stock_list_300_cache = pd.read_sql_query(
                        GET_STOCK_LIST_300, conn
                    )["종목코드"].tolist()

                if ticker == "ALL":
                    self._pending_market_avg, _ = get_market_momentum_status(
                        self._stock_list_300_cache, MIN_TERM
                    )
                    # 3. Candidate Selection
                    query_params = (
                        MIN_TERM_CD,
                        self.sys_id,
                        MIN_TERM_CD,
                        self.sys_id,
                        len(
                            self._stock_list_300_cache
                        ),  # limit : 모든 매수대상 종목 조회
                    )
                    tickers = pd.read_sql_query(
                        GET_PRED_TRGT_STK_LIST, conn, params=query_params
                    )["종목코드"].tolist()
                    data_pool = fetch_datas(tickers)

                else:  # 개별 종목 진입일때
                    tickers = [ticker]
                    data_pool = fetch_datas(ticker)

                from strategy.core.TradingComm import select_candidates_v2

                candidates = select_candidates_v2(
                    data_pool=data_pool,
                    min_candidates=len(self._stock_list_300_cache)
                    if ticker == "ALL"
                    else 1,
                    params=PARAMS,
                    capital=self.buyable_amt,
                    port_list=self.portfolio_list,
                    market_avg_acc=self._pending_market_avg,
                    tp="intraday",
                )

                logger.info(
                    f"✅ ticker: {ticker}, self._stock_list_300_cache: {len(self._stock_list_300_cache)}, "
                    f"self._pending_market_avg: {self._pending_market_avg}, "
                    f"candidates: {len(candidates)}"
                )
                # 4. Execute Pipeline
                if candidates:
                    logger.info(
                        f"🚀 [Orchestrator] Passing {len(candidates)} candidates to ExecutionPipeline."
                    )
                    self.execution_pipeline.run_execution_pipeline(
                        candidates, capital=self.buyable_amt
                    )
        except Exception as e:
            logger.error(f"❌ [Orchestrator] doIT failed: {e}", exc_info=True)
        finally:
            self._is_doing_it = False
            self.doit_lock.release()

    def _on_pull_data(self, data):
        """32-bit 클라이언트에서 PUSH한 상태 데이터를 비동기로 수신 (PULL)"""
        try:
            # [Step 4.2] 체결 피드백(FILL)인 경우 Shadow State 매니저로 위임
            if data.get("type") == "FILL":
                self.shadow_manager.sync_virtual_state(data)
            else:
                # EventLoopManager의 로직을 재활용하여 일관성 유지 (기존 ACCOUNT_STATUS 등)
                self.event_manager._on_zmq_event(TOPIC_EVENT, data)
        except Exception as e:
            logger.error(f"❌ [ZMQ Pull] Error processing data: {e}")

    def _sync_account_info(self):
        """
        [Step 2.3] 서버의 기존 API 잔고 조회 우회 (Bypass)
        - 32-bit 클라이언트에 REQ_ACCOUNT_INFO를 보내지 않고, 섀도우 상태를 즉시 반환
        """
        # self.zmq_push.push_data({"command": "REQ_ACCOUNT_INFO"}) # API 조회 로직 주석 처리
        logger.debug("📡 [Bypass] Using Shadow State for account info.")
        return self.virtual_avail_cash, self.active_positions_map

    def schedule_shutdown(self):
        now = _dt.now()
        shutdown_time = now.replace(hour=15, minute=30, second=5, microsecond=0)
        if now < shutdown_time:
            threading.Timer(
                (shutdown_time - now).total_seconds(), self.shutdown
            ).start()

    def shutdown(self):
        ToTelegram(f"GoldenGoose {MIN_TERM}분봉 매매신호 생성작업 종료")
        self._stop_event.set()
        self.db_writer.stop()
        logger.info("🛑 [Orchestrator] Shutdown complete.")


if __name__ == "__main__":
    engine = IntradayTrading()
    engine.initialize()
    while not engine._stop_event.wait(timeout=1.0):
        pass
