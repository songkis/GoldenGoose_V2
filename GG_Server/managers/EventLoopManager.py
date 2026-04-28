import logging
import time as _tm
import threading
from GG_Shared.util.zmq_manager import TOPIC_EVENT, TOPIC_TICK, TOPIC_ORDER_RESULT
from util.CommUtils import updateSearchStock, parse_avail_cash

# Import modularized components
from .zmq_receiver import ZMQReceiverThread
from .tick_processor import handle_tick_logic

logger = logging.getLogger(__name__)

class EventLoopManager:
    """
    [EventLoopManager] ZMQ 이벤트 수신 및 틱 데이터 라우팅 전담 매니저
    """
    def __init__(self, context):
        self.ctx = context
        self.running = False

    def start_listener(self):
        try:
            if not hasattr(self.ctx, "zmq_sub") or self.ctx.zmq_sub is None:
                logger.error("[EventLoopManager] ZMQ Subscriber not initialized in context!")
                return
            if getattr(self.ctx.zmq_sub, "running", False):
                logger.debug("[EventLoopManager] ZMQ Subscriber is already running. Skipping redundant start.")
                return
            logger.info("[EventLoopManager] Starting ZMQ Listener Thread delegation...")
            self.ctx.zmq_sub.start_listener(self._on_zmq_event)
            self.running = True
        except Exception as e:
            logger.error(f"[EventLoopManager] Failed to start ZMQ Listener: {e}")

    def _on_zmq_event(self, topic, data):
        try:
            if topic == TOPIC_EVENT:
                event_type = data.get("event")
                if event_type in ["COLLECTION_ONE_COMPLETE", "COLLECTION_ALL_COMPLETE", "ACCOUNT_STATUS"]:
                    if event_type in ("COLLECTION_ONE_COMPLETE", "COLLECTION_ALL_COMPLETE"):
                        logger.info(f"[ZMQ] Received {event_type} ({data.get('ticker')}) at {data.get('timestamp')}. Triggering Analysis...")
                        self.ctx.gen_trade_signal(data.get("ticker"))

                    self.ctx.account_info = data.get("account_info", data)
                    is_emergency = self.ctx.account_info.get("is_emergency", False)
                    real_positions = self.ctx.account_info.get("positions", [])
                    if isinstance(real_positions, list):
                        real_tickers = [p.get("종목코드") for p in real_positions if p.get("종목코드")]
                        if not getattr(self.ctx, "test_mode", False):
                            if set(getattr(self.ctx, "portfolio_list", [])) != set(real_tickers):
                                logger.info(f"🔄 [Portfolio Sync] 계좌 실시간 동기화: {list(set(getattr(self.ctx, 'portfolio_list', [])))} -> {list(set(real_tickers))}")
                                self.ctx.portfolio_list = real_tickers

                    avail_cash = parse_avail_cash(self.ctx.account_info)
                    if (_tm.time() - getattr(self.ctx, "last_buy_order_time", 0)) < 5.0:
                        logger.info(f"⏳ [ZMQ Guard] 주문 직후 계좌 동기화 유예 중... (Virtual: {self.ctx.virtual_avail_cash:,.0f}원)")
                    elif not getattr(self.ctx, "test_mode", False):
                        if avail_cash > 0:
                            with getattr(self.ctx, "capital_lock", threading.Lock()):
                                self.ctx.avail_cash = self.ctx.virtual_avail_cash = self.ctx.buyable_amt = avail_cash
                            logger.info(f"💰 [ZMQ] Account Sync: AvailCash = {avail_cash:,.0f}원 (Emergency: {is_emergency})")
                        else:
                            logger.warning(f"⚠️ [ZMQ] Account Sync Incomplete: 0원 포착. 기존 잔고 유지.")

                    if hasattr(self.ctx, "account_guard") and self.ctx.account_guard:
                        if not self.ctx.account_guard.start_day_equity:
                            self.ctx.account_guard.start_day_equity = self.ctx.account_info.get("추정순자산", 0)
                        self.ctx.account_guard.is_emergency_mode = is_emergency

            elif topic == TOPIC_ORDER_RESULT:
                ticker, result, detail = data.get("ticker"), data.get("result"), data.get("detail")
                logger.info(f"📨 [ZMQ] Received ORDER_RESULT: {ticker} {result} {detail}")
                side, qty, msg_type = data.get("side", "buy"), data.get("qty", 0), data.get("type", "")

                if msg_type == "ICEBERG_DONE":
                    with getattr(self.ctx, "order_lock", threading.Lock()):
                        logger.info(f"🧊 [ZMQ] Iceberg Done acked for {ticker}")
                        if hasattr(self.ctx, "active_smart_orders"):
                            if isinstance(self.ctx.active_smart_orders, set): self.ctx.active_smart_orders.discard(ticker)
                            else: self.ctx.active_smart_orders.pop(ticker, None)
                        if not hasattr(self.ctx, "cooldown_orders"): self.ctx.cooldown_orders = {}
                        self.ctx.cooldown_orders[ticker] = _tm.time()
                    return

                if msg_type == "execution" or result == "FAIL":
                    if result == "FAIL": status_text = detail or "API REJECTED"
                    else:
                        exec_data = data.get("data", {})
                        status_text = str(exec_data.get("주문체결명", exec_data.get("상태", "")))
                    
                    if any(x in status_text for x in ["거부", "오류", "거절"]) or result == "FAIL":
                        msg_type, result = "REJECTED", "FAIL"
                        with getattr(self.ctx, "order_lock", threading.Lock()):
                            logger.warning(f"❌ [ZMQ] Order REJECTED for {ticker}: {status_text}")
                            if hasattr(self.ctx, "active_smart_orders"):
                                if isinstance(self.ctx.active_smart_orders, set): self.ctx.active_smart_orders.discard(ticker)
                                else: self.ctx.active_smart_orders.pop(ticker, None)
                            if side == "buy":
                                unit_cash = getattr(self.ctx, "단위투자금", 1000000)
                                self.ctx.avail_cash += unit_cash
                                self.ctx.virtual_avail_cash += unit_cash
                    elif "취소" in status_text: msg_type, result = "CANCELLED", "FAIL"
                    elif "체결" in status_text: msg_type = "FILL"

                    if msg_type != "REJECTED":
                        raw_ticker = exec_data.get("종목코드", exec_data.get("expcode", ""))
                        ticker = raw_ticker.replace("A", "") if raw_ticker else ticker
                        data["ticker"] = ticker
                        try: data["qty"] = int(float(exec_data.get("체결수량", exec_data.get("체결량", exec_data.get("execqty", 0)))))
                        except: data["qty"] = 0
                        qty = data["qty"]

                if result == "SUCCESS":
                    price, qty, side = data.get("price", 0), data.get("qty", 0), data.get("side", "buy")
                    if msg_type == "FILL":
                        logger.info(f"✅ [Fill] {ticker} ({side}) 체결 완료: {qty}주 @ {price}원")
                        if side == "buy":
                            updateSearchStock(ticker, 2)
                            new_qty = self.ctx.shadow_portfolio.get(ticker, {}).get("qty", 0) + qty
                            self.ctx.shadow_portfolio[ticker] = {"qty": new_qty, "price": price, "time": _tm.time()}
                            if ticker not in getattr(self.ctx, "portfolio_list", []): self.ctx.portfolio_list.append(ticker)
                            if hasattr(self.ctx, "active_positions_map"):
                                if ticker not in self.ctx.active_positions_map: self.ctx.active_positions_map[ticker] = {"상태": "open", "종목코드": ticker}
                                self.ctx.active_positions_map[ticker].update({"포지션수량": new_qty, "진입가": price})
                            with getattr(self.ctx, "order_lock", threading.Lock()):
                                if hasattr(self.ctx, "active_smart_orders"): self.ctx.active_smart_orders.pop(ticker, None)
                        elif side == "sell":
                            updateSearchStock(ticker, -1)
                            existing_qty = self.ctx.shadow_portfolio.get(ticker, {}).get("qty", self.ctx.active_positions_map.get(ticker, {}).get("포지션수량", 0))
                            rem_qty = max(0, existing_qty - qty)
                            if rem_qty > 0: self.ctx.shadow_portfolio[ticker]["qty"] = rem_qty
                            else:
                                self.ctx.shadow_portfolio.pop(ticker, None)
                                if ticker in getattr(self.ctx, "portfolio_list", []): self.ctx.portfolio_list.remove(ticker)
                                with getattr(self.ctx, "order_lock", threading.Lock()):
                                    if hasattr(self.ctx, "active_smart_orders"): self.ctx.active_smart_orders.pop(ticker, None)
                            
                            buy_price = self.ctx.active_positions_map.get(ticker, {}).get("매수가", self.ctx.active_positions_map.get(ticker, {}).get("진입가", 0))
                            if buy_price > 0:
                                try:
                                    from strategy.core.TradingComm import global_decision_engine
                                    global_decision_engine.update_performance_feedback(is_win=(price > buy_price))
                                except: pass

                elif result == "FAIL":
                    logger.warning(f"❌ [Sync] Order FAILED for {ticker}: {detail}")
                    with getattr(self.ctx, "order_lock", threading.Lock()):
                        if hasattr(self.ctx, "active_smart_orders") and ticker in self.ctx.active_smart_orders: del self.ctx.active_smart_orders[ticker]
                    if side == "buy" and ticker in getattr(self.ctx, "trade_signal_cache", {}):
                        rollback_amt = self.ctx.trade_signal_cache[ticker].get("capital", 0)
                        if rollback_amt > 0:
                            with getattr(self.ctx, "capital_lock", threading.Lock()): self.ctx.virtual_avail_cash += rollback_amt
                    cl_ord_id = data.get("cl_ord_id")
                    if cl_ord_id:
                        with self.ctx.cl_ord_lock: self.ctx.pending_cl_orders.pop(cl_ord_id, None)

            elif topic == TOPIC_TICK:
                if getattr(self.ctx, "tick_log_count", 0) < 100: self.ctx.tick_log_count = getattr(self.ctx, "tick_log_count", 0) + 1
                handle_tick_logic(self, data)
        except Exception as e:
            logger.error(f"[EventLoopManager] ZMQ Event Handler Error: {e}")
