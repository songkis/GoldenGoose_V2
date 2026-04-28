import time as _tm
import logging
from typing import Set, Dict, Any

logger = logging.getLogger(__name__)


class PositionManager:
    """
     Handle position health checks and reconciliation (Ghost Buster).
    Separates the "Account Truth" synchronization logic from the main trading engine.
    """

    def __init__(self, brain):
        self.brain = brain
        self._ghost_confirm_count = {}

    def set_logger(self, external_logger):
        global logger
        logger = external_logger

    def reconcile_positions(
        self, real_portfolio_tickers: Set[str], account_info: Dict[str, Any]
    ) -> bool:
        """
        [Atomic Recon] Synchronize Brain's internal state with Actual Broker Balance.
        Returns True if any internal state was modified (ghost detected/recovered).
        """
        ghost_detected = False
        brain_positions = set(list(self.brain.state.active_positions_map.keys()))

        # 1. Zombies: In Brain but NOT in Account (Virtual positions to purge)
        zombies = brain_positions - real_portfolio_tickers
        for ticker in zombies:
            pos_data = self.brain.active_positions_map.get(ticker)
            if not pos_data:
                continue
            if pos_data.get("상태") in ["open", "regist"]:
                # Grace Period Check (Exclude very recent orders)
                order_lease_time = 15.0
                last_order_time = getattr(
                    self.brain.state, "active_smart_orders", {}
                ).get(ticker, 0)
                if _tm.time() - last_order_time < order_lease_time:
                    continue

                # 5-Strike Rule for network lag defense
                self._ghost_confirm_count[ticker] = (
                    self._ghost_confirm_count.get(ticker, 0) + 1
                )
                if self._ghost_confirm_count[ticker] % 2 == 0:
                    logger.info(
                        f"👻 [Ghost Trace] {ticker} Strike {self._ghost_confirm_count[ticker]}/5..."
                    )

                if self._ghost_confirm_count[ticker] >= 5:
                    logger.warning(
                        f"👻 [Ghost Buster] Zombie Position Confirmed (5 Strikes): {ticker}. Atomic Purge Executed."
                    )
                    self.brain.state.active_positions_map.pop(ticker, None)
                    if (
                        hasattr(self.brain, "bought_stocks")
                        and ticker in self.brain.bought_stocks
                    ):
                        self.brain.bought_stocks.pop(ticker, None)
                    self._ghost_confirm_count.pop(ticker, None)
                    ghost_detected = True

        # 2. Stragglers: In Account but NOT in Brain (Missed positions to recover)
        stragglers = real_portfolio_tickers - brain_positions
        for ticker in stragglers:
            logger.warning(
                f"🚑 [Position Recovery] Straggler Position Found: {ticker}. Force-Syncing with Account Truth."
            )
            acc_positions = account_info.get("positions", [])
            acc_info = next((p for p in acc_positions if p.get("ticker") == ticker), {})

            self.brain.state.active_positions_map[ticker] = {
                "상태": "open",
                "진입가": float(acc_info.get("avg_buy_price", 0)),
                "수량": int(acc_info.get("quantity", 0)),
                "current_price": float(acc_info.get("current_price", 0)),
                "pnl_pct": float(acc_info.get("pnl_pct", 0)),
                "is_recovered": True,
            }
            ghost_detected = True

        return ghost_detected
