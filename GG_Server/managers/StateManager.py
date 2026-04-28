import logging
import threading
from collections import deque, defaultdict
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)


class StateManager:
    """
    [StateManager] GoldenGoose 엔진의 통합 상태 관리자
    - 모든 인메모리 상태 변수와 동기화 락(Lock)을 중앙 집중화합니다.
    """

    def __init__(self):
        # [Locks] 동합 동기화 관리
        self.tick_lock = threading.Lock()
        self.order_lock = threading.Lock()
        self.cl_ord_lock = threading.Lock()
        self.capital_lock = threading.Lock()
        self.doit_lock = threading.Lock()

        # [Inventory State]
        self.active_positions_map: Dict[str, Dict] = {}
        self.portfolio_list: List[str] = []
        self.shadow_portfolio: Dict[str, Dict] = {}  # 체결 선반영 메모리

        # [Tick & Market State]
        self.realtime_tick_cache: Dict[str, Dict] = {}
        self.cpower_history = defaultdict(lambda: deque(maxlen=3))
        self.market_conditions: Dict[str, Any] = {}
        self.high_price_locks: Dict[str, int] = {}  # {ticker: minute_of_last_update}

        # [Order Tracking]
        self.pending_cl_orders: Dict[str, Dict] = {}
        self.active_smart_orders: Dict[str, float] = {}  # {ticker: timestamp}
        self._pending_sell_stocks: Dict[str, float] = {}
        self._pending_buy_stocks: Dict[str, float] = {}

        # [Account State]
        self.account_info: Dict[str, Any] = {}
        self.virtual_avail_cash: float = 0.0

        # [Volatile Engine State]
        self.recovering_leaders: List[str] = []
        self.market_regime: str = "NEUTRAL"

    def get_position(self, ticker: str) -> Optional[Dict]:
        with self.order_lock:
            return self.active_positions_map.get(ticker)

    def update_account(self, data: Dict[str, Any]):
        with self.capital_lock:
            self.account_info = data
            self.virtual_avail_cash = float(data.get("주문가능금액", 0.0))

    def mark_order_sent(self, ticker: str, cl_ord_id: str, side: str, qty: int):
        with self.cl_ord_lock:
            self.pending_cl_orders[cl_ord_id] = {
                "ticker": ticker,
                "side": side,
                "qty": qty,
                "time": threading._time() if hasattr(threading, "_time") else 0.0,
            }
        with self.order_lock:
            self.active_smart_orders[ticker] = (
                threading._time() if hasattr(threading, "_time") else 0.0
            )

    def clear_order_lock(self, ticker: str):
        with self.order_lock:
            self.active_smart_orders.pop(ticker, None)

    # --- [Volatile State Accessors] ---

    def get_recovering_leaders(self) -> List[str]:
        """주도주 귀환 리스트를 스레드 안전하게 복사하여 반환 (Iteration Error 방지)"""
        with self.doit_lock:
            return list(self.recovering_leaders)

    def set_recovering_leaders(self, leaders: List[str]):
        """주도주 귀환 리스트 업데이트"""
        with self.doit_lock:
            self.recovering_leaders = list(leaders)

    def get_market_regime(self) -> str:
        with self.doit_lock:
            return self.market_regime

    def set_market_regime(self, regime: str):
        with self.doit_lock:
            self.market_regime = regime
