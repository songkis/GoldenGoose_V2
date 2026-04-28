import logging
import time as _tm
import threading
import uuid as _uuid
from collections import deque
from datetime import datetime as _dt
import numpy as np
from PyQt5.QtCore import QThread, pyqtSignal
from GG_Shared.util.zmq_manager import TOPIC_EVENT, TOPIC_TICK, TOPIC_ORDER_RESULT, ZMQPushPull
from util.CommUtils import to_numeric_safe, updateSearchStock, parse_avail_cash
from strategy.core.TradingComm import prepare_sell_order_params
from strategy.core.execution_engine import process_sell_orders
from config.ai_settings import SYS_ID, PARAMS
from config.comm_settings import ZMQ_PULL_PORT

logger = logging.getLogger(__name__)


class ZMQReceiverThread(QThread):
    """
    [ZMQReceiverThread] 64-bit 서버로부터 시그널을 수신하는 전용 스레드 (32-bit Client용)
    - QThread를 사용하여 UI Thread(MainThread)와의 시그널 연동 시 Thread-Safety 보장
    """
    signal_received = pyqtSignal(dict)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.running = True
        self.zmq_pull = None

    def run(self):
        try:
            # 64-bit 서버가 PUSH하고, 32-bit 클라이언트가 PULL함 (Bind mode)
            self.zmq_pull = ZMQPushPull(mode="PULL", port=ZMQ_PULL_PORT)
            logger.info(f"✅ [ZMQReceiverThread] Listening on PULL port {ZMQ_PULL_PORT}")
            
            def on_data(data):
                if data:
                    # QThread 시그널은 수신자가 속한 스레드로 자동 마샬링됨
                    self.signal_received.emit(data)
            
            # Non-blocking listener start (uses its own threading.Thread internally but we manage it via QThread lifecycle)
            self.zmq_pull.start_pull_listener(on_data)
            
            while self.running:
                self.msleep(500)  # CPU 점유율 방지 및 라이프사이클 유지
        except Exception as e:
            logger.error(f"❌ [ZMQReceiverThread] Critical Run Error: {e}")
        finally:
            self.stop()

    def stop(self):
        self.running = False
        if self.zmq_pull:
            try:
                self.zmq_pull.close()
                logger.info("[ZMQReceiverThread] ZMQ Socket closed.")
            except Exception:
                pass
        self.quit()
        # self.wait() # wait() inside stop() can cause deadlock if called from the thread itself


class EventLoopManager:
    """
    [EventLoopManager] ZMQ 이벤트 수신 및 틱 데이터 라우팅 전담 매니저
    """

    def __init__(self, context):
        self.ctx = context  # IntradayTrading instance
        self.running = False

    def start_listener(self):
        try:
            if not hasattr(self.ctx, "zmq_sub") or self.ctx.zmq_sub is None:
                logger.error(
                    "[EventLoopManager] ZMQ Subscriber not initialized in context!"
                )
                return
            if getattr(self.ctx.zmq_sub, "running", False):
                logger.debug(
                    "[EventLoopManager] ZMQ Subscriber is already running. Skipping redundant start."
                )
                return
            logger.info("[EventLoopManager] Starting ZMQ Listener Thread delegation...")
            self.ctx.zmq_sub.start_listener(self._on_zmq_event)
            self.running = True
        except Exception as e:
            logger.error(f"[EventLoopManager] Failed to start ZMQ Listener: {e}")

    def _on_zmq_event(self, topic, data):
        """
        Callback for ZMQ SUB events.
        """
        try:
            if topic == TOPIC_EVENT:
                event_type = data.get("event")
                if event_type in [
                    "COLLECTION_ONE_COMPLETE",
                    "COLLECTION_ALL_COMPLETE",
                    "ACCOUNT_STATUS",
                ]:
                    if event_type in (
                        "COLLECTION_ONE_COMPLETE",
                        "COLLECTION_ALL_COMPLETE",
                    ):
                        timestamp = data.get("timestamp", "Unknown")
                        ticker = data.get("ticker", "Unknown")
                        logger.info(
                            f"[ZMQ] Received COLLECTION_ONE_COMPLETE ({ticker}) at {timestamp}. Triggering Analysis..."
                        )
                        self.ctx.gen_trade_signal(ticker)

                    # [Zero-Defect] ACCOUNT_STATUS 또는 COLLECTION_ONE_COMPLETE에서 계좌 동기화 수행
                    self.ctx.account_info = data.get("account_info", data)

                    is_emergency = self.ctx.account_info.get("is_emergency", False)
                    real_positions = self.ctx.account_info.get("positions", [])
                    if isinstance(real_positions, list):
                        real_tickers = [
                            p.get("종목코드")
                            for p in real_positions
                            if p.get("종목코드")
                        ]
                        if not getattr(self.ctx, "test_mode", False):
                            old_port = set(getattr(self.ctx, "portfolio_list", []))
                            new_port = set(real_tickers)
                            if old_port != new_port:
                                logger.info(
                                    f"🔄 [Portfolio Sync] 계좌 실시간 동기화로 슬롯 상태 갱신: {list(old_port)} -> {list(new_port)}"
                                )
                                self.ctx.portfolio_list = real_tickers

                    avail_cash = parse_avail_cash(self.ctx.account_info)
                    last_buy_time = getattr(self.ctx, "last_buy_order_time", 0)
                    now_ts = _tm.time()
                    if (now_ts - last_buy_time) < 5.0:
                        logger.info(
                            f"⏳ [ZMQ Guard] 주문 직후 계좌 동기화 유예 중... (Virtual: {self.ctx.virtual_avail_cash:,.0f}원)"
                        )
                    else:
                        if not getattr(self.ctx, "test_mode", False):
                            if avail_cash <= 0:
                                # [Zero-Defect] 가용 현금이 0원일 경우, 데이터 누락으로 간주하고 기존 잔액 유지
                                logger.warning(
                                    f"⚠️ [ZMQ] Account Sync Incomplete: 0원 포착. 기존 잔고({self.ctx.avail_cash:,.0f}원) 유지. "
                                    f"포착된 키 목록: {list(self.ctx.account_info.keys())}"
                                )
                            else:
                                with getattr(
                                    self.ctx, "capital_lock", threading.Lock()
                                ):
                                    self.ctx.avail_cash = avail_cash
                                    self.ctx.virtual_avail_cash = avail_cash
                                    self.ctx.buyable_amt = avail_cash
                                logger.info(
                                    f"💰 [ZMQ] Account Sync: AvailCash = {avail_cash:,.0f}원 (Emergency: {is_emergency}) | Event: {event_type}"
                                )
                        else:
                            logger.info(
                                f"🧪 [ZMQ Test] Account Sync Skipped to preserve dummy capital. (Real AvailCash: {avail_cash:,.0f}원)"
                            )

                    if hasattr(self.ctx, "account_guard") and self.ctx.account_guard:
                        if (
                            self.ctx.account_guard.start_day_equity is None
                            or self.ctx.account_guard.start_day_equity == 0
                        ):
                            self.ctx.account_guard.start_day_equity = (
                                self.ctx.account_info.get("추정순자산", 0)
                            )
                        self.ctx.account_guard.is_emergency_mode = is_emergency

            elif topic == TOPIC_ORDER_RESULT:
                ticker = data.get("ticker")
                result = data.get("result")
                detail = data.get("detail")
                logger.info(
                    f"📨 [ZMQ] Received ORDER_RESULT: {ticker} {result} {detail}"
                )
                side = data.get("side", "buy")
                qty = data.get("qty", 0)
                msg_type = data.get("type", "")

                if msg_type == "ICEBERG_DONE":
                    with getattr(self.ctx, "order_lock", threading.Lock()):
                        logger.info(f"🧊 [ZMQ] Iceberg Done acked for {ticker}")
                        if hasattr(self.ctx, "active_smart_orders"):
                            if isinstance(self.ctx.active_smart_orders, set):
                                self.ctx.active_smart_orders.discard(ticker)
                            elif isinstance(self.ctx.active_smart_orders, dict):
                                self.ctx.active_smart_orders.pop(ticker, None)
                        if not hasattr(self.ctx, "cooldown_orders"):
                            self.ctx.cooldown_orders = {}
                        self.ctx.cooldown_orders[ticker] = _tm.time()
                    return

                if msg_type == "execution" or result == "FAIL":
                    if result == "FAIL":
                        status_text = detail if detail else "API REJECTED"
                    else:
                        exec_data = data.get("data", {})
                        status_text = str(
                            exec_data.get("주문체결명", exec_data.get("상태", ""))
                        )
                    if (
                        "거부" in status_text
                        or "오류" in status_text
                        or "거절" in status_text
                        or result == "FAIL"
                    ):
                        msg_type = "REJECTED"
                        result = "FAIL"
                        with getattr(self.ctx, "order_lock", threading.Lock()):
                            logger.warning(
                                f"❌ [ZMQ] Order REJECTED for {ticker}: {status_text}"
                            )
                            if hasattr(self.ctx, "active_smart_orders"):
                                if isinstance(self.ctx.active_smart_orders, set):
                                    self.ctx.active_smart_orders.discard(ticker)
                                else:
                                    self.ctx.active_smart_orders.pop(ticker, None)
                            if side == "buy":
                                unit_cash = getattr(self.ctx, "단위투자금", 1000000)
                                self.ctx.avail_cash += unit_cash
                                self.ctx.virtual_avail_cash += unit_cash
                                logger.info(
                                    f"🔄 [Rollback] {ticker} buy rejected. Restored {unit_cash} to AvailCash ({self.ctx.avail_cash})"
                                )
                    elif "취소" in status_text:
                        msg_type = "CANCELLED"
                        result = "FAIL"
                    elif "체결" in status_text:
                        msg_type = "FILL"

                    if msg_type != "REJECTED":
                        raw_ticker = exec_data.get(
                            "종목코드", exec_data.get("expcode", "")
                        )
                        ticker = raw_ticker.replace("A", "") if raw_ticker else ticker
                        data["ticker"] = ticker
                        qty_str = exec_data.get(
                            "체결수량",
                            exec_data.get("체결량", exec_data.get("execqty", 0)),
                        )
                        try:
                            data["qty"] = int(float(qty_str))
                        except (ValueError, TypeError):
                            data["qty"] = 0
                        qty = data["qty"]

                if result == "SUCCESS":
                    price = data.get("price", 0)
                    qty = data.get("qty", 0)
                    side = data.get("side", "buy")

                    if msg_type == "FILL":
                        logger.info(
                            f"✅ [Fill] {ticker} ({side}) 체결 완료: {qty}주 @ {price}원"
                        )
                        if side == "buy":
                            updateSearchStock(ticker, 2)
                            existing_buy = self.ctx.shadow_portfolio.get(
                                ticker, {"qty": 0}
                            )
                            new_buy_qty = existing_buy.get("qty", 0) + qty
                            self.ctx.shadow_portfolio[ticker] = {
                                "qty": new_buy_qty,
                                "price": price,
                                "time": _tm.time(),
                            }
                            if ticker not in getattr(self.ctx, "portfolio_list", []):
                                self.ctx.portfolio_list.append(ticker)

                            if hasattr(self.ctx, "active_positions_map"):
                                if ticker not in self.ctx.active_positions_map:
                                    self.ctx.active_positions_map[ticker] = {
                                        "상태": "open",
                                        "종목코드": ticker,
                                    }
                                self.ctx.active_positions_map[ticker]["포지션수량"] = (
                                    new_buy_qty
                                )
                                self.ctx.active_positions_map[ticker]["진입가"] = price

                            with getattr(self.ctx, "order_lock", threading.Lock()):
                                if hasattr(self.ctx, "active_smart_orders"):
                                    self.ctx.active_smart_orders.pop(ticker, None)
                        elif side == "sell":
                            updateSearchStock(ticker, -1)
                            if ticker not in self.ctx.shadow_portfolio:
                                db_qty = 0
                                if ticker in self.ctx.active_positions_map:
                                    db_qty = self.ctx.active_positions_map[ticker].get(
                                        "포지션수량", 0
                                    )
                                if db_qty > 0:
                                    self.ctx.shadow_portfolio[ticker] = {
                                        "qty": db_qty,
                                        "price": price,
                                        "time": _tm.time(),
                                    }
                            existing_sell = self.ctx.shadow_portfolio.get(
                                ticker, {"qty": qty}
                            )
                            rem_qty = max(0, existing_sell.get("qty", qty) - qty)
                            if rem_qty > 0:
                                logger.info(
                                    f"✨ [Partial Fill] {ticker} 매도 부분체결: {qty}주 (잔여: {rem_qty}주). 포지션 및 락 유지."
                                )
                                self.ctx.shadow_portfolio[ticker]["qty"] = rem_qty
                            else:
                                logger.info(
                                    f"🧹 [Position Cleared] {ticker} 전량 매도 완료"
                                )
                                self.ctx.shadow_portfolio.pop(ticker, None)
                                if (
                                    hasattr(self.ctx, "portfolio_list")
                                    and ticker in self.ctx.portfolio_list
                                ):
                                    self.ctx.portfolio_list.remove(ticker)
                                with getattr(self.ctx, "order_lock", threading.Lock()):
                                    if hasattr(self.ctx, "active_smart_orders"):
                                        self.ctx.active_smart_orders.pop(ticker, None)

                        buy_price = 0
                        if ticker in self.ctx.active_positions_map:
                            buy_price = self.ctx.active_positions_map[ticker].get(
                                "매수가", 0
                            ) or self.ctx.active_positions_map[ticker].get("진입가", 0)
                        if side == "sell" and buy_price > 0:
                            final_profit_pct = (price - buy_price) / buy_price
                            is_win = final_profit_pct > 0.0
                            try:
                                from strategy.core.TradingComm import (
                                    global_decision_engine,
                                )

                                global_decision_engine.update_performance_feedback(
                                    is_win=is_win
                                )
                                logger.info(
                                    f"🧠 [Feedback Loop] {ticker} Sell PnL: {final_profit_pct * 100:.2f}%"
                                )
                            except Exception as e:
                                logger.debug(f"[Feedback Loop] Error: {e}")

                elif result == "FAIL":
                    reason = detail
                    logger.warning(f"❌ [Sync] Order FAILED for {ticker}: {reason}")
                    with getattr(self.ctx, "order_lock", threading.Lock()):
                        if (
                            hasattr(self.ctx, "active_smart_orders")
                            and ticker in self.ctx.active_smart_orders
                        ):
                            del self.ctx.active_smart_orders[ticker]
                            logger.info(
                                f"🔓 [Unlock] {ticker} 주문 실패(FAIL). Zombie 락 해제."
                            )

                    if side == "buy" and ticker in getattr(
                        self.ctx, "trade_signal_cache", {}
                    ):
                        signal = self.ctx.trade_signal_cache[ticker]
                        rollback_amt = signal.get("capital", 0)
                        if rollback_amt > 0:
                            with getattr(self.ctx, "capital_lock", threading.Lock()):
                                self.ctx.virtual_avail_cash += rollback_amt
                                logger.info(
                                    f"💰 [Rollback] {ticker} 매수 실패로 잔고 즉시 복구: +{rollback_amt:,.0f}원 -> {self.ctx.virtual_avail_cash:,.0f}원"
                                )

                    cl_ord_id = data.get("cl_ord_id")
                    if cl_ord_id:
                        with self.ctx.cl_ord_lock:
                            self.ctx.pending_cl_orders.pop(cl_ord_id, None)

            elif topic == TOPIC_TICK:
                if self.ctx.tick_log_count < 100:
                    self.ctx.tick_log_count += 1
                self._handle_tick(data)
        except Exception as e:
            logger.error(f"[EventLoopManager] ZMQ Event Handler Error: {e}")

    def _handle_tick(self, tick_data):
        try:
            ticker = tick_data.get("ticker")
            result = tick_data.get("data", {})
            if not ticker or not result:
                return
            current_price = result.get("현재가", 0)
            if not current_price:
                return

            current_ts = _tm.time()
            with self.ctx.tick_lock:
                if ticker not in self.ctx.realtime_tick_cache:
                    self.ctx.realtime_tick_cache[ticker] = {
                        "ticks": [],
                        "ask_sum": 0,
                        "bid_sum": 0,
                        "cpower": 0.0,
                    }
                cache = self.ctx.realtime_tick_cache[ticker]
                cache["cpower"] = result.get("체결강도", cache["cpower"])

                bid_val = result.get(
                    "매수호가", result.get("매수호가1", result.get("매수1호가"))
                )
                if bid_val is not None:
                    if isinstance(bid_val, list) and len(bid_val) > 0:
                        cache["best_bid"] = abs(int(bid_val[0]))
                    else:
                        cache["best_bid"] = abs(int(bid_val))

                ask_val = result.get(
                    "매도호가", result.get("매도호가1", result.get("매도1호가"))
                )
                if ask_val is not None:
                    if isinstance(ask_val, list) and len(ask_val) > 0:
                        cache["best_ask"] = abs(int(ask_val[0]))
                    else:
                        cache["best_ask"] = abs(int(ask_val))

                cache["ask_sum"] = result.get(
                    "총매도호가잔량",
                    result.get(
                        "매도10호가총잔량",
                        result.get("매도누적체결량", cache["ask_sum"]),
                    ),
                )
                cache["bid_sum"] = result.get(
                    "총매수호가잔량",
                    result.get(
                        "매수10호가총잔량",
                        result.get("매수누적체결량", cache["bid_sum"]),
                    ),
                )

                cache["ticks"].append(
                    {"ts": current_ts, "vol": result.get("체결량", 0)}
                )
                cutoff_time = current_ts - 10.0
                cache["ticks"] = [t for t in cache["ticks"] if t["ts"] >= cutoff_time]

            tick_count = len(cache.get("ticks", []))
            cpower = float(cache.get("cpower", 0.0))
            ask_sum = float(cache.get("ask_sum", 0.0))
            bid_sum = float(cache.get("bid_sum", 0.0))
            is_fakeout = (bid_sum / (ask_sum + 1.0)) < 1.5

            if tick_count >= 8 or cpower >= 150:
                is_fakeout = False

            self.ctx.cpower_history[ticker].append(cpower)
            today_open = float(result.get("시가", current_price))
            is_cpower_decay = False
            if len(self.ctx.cpower_history[ticker]) == 3:
                h = self.ctx.cpower_history[ticker]
                if h[0] > h[1] > h[2]:
                    is_cpower_decay = True

            is_high_price_lock = (current_price > today_open * 1.08) and is_cpower_decay
            if is_high_price_lock:
                if not hasattr(self.ctx, "high_price_locks"):
                    self.ctx.high_price_locks = {}
                if ticker not in self.ctx.high_price_locks:
                    self.ctx.high_price_locks[ticker] = _tm.time()
                    logger.warning(
                        f"🔒 [Hard Lock] {ticker} 고점(+8%↑) 수급 하락 감지. 매수 신호 HOLD 강제 전환."
                    )

            if ticker not in self.ctx.active_positions_map:
                return

            position_item = self.ctx.active_positions_map[ticker]
            buy_price = position_item.get("매수가", position_item.get("진입가", 0))
            if buy_price > 0:
                current_profit_pct = (current_price - buy_price) / buy_price * 100.0
                old_max = position_item.get("max_profit_pct", 0.0)
                position_item["max_profit_pct"] = max(old_max, current_profit_pct)

            portfolio_dict = {
                "매수가": buy_price,
                "수량": position_item.get("수량", position_item.get("포지션수량", 0)),
                "매수후고가": position_item.get(
                    "매수후고가", position_item.get("최고가", 0)
                ),
                "매수일": position_item.get("매수일", "UNKNOWN"),
                "현재가": current_price,
                "체결강도": result.get("체결강도", 0),
                "체결량": result.get("체결량", 0),
                "누적거래량": result.get("누적거래량", 0),
                "매도호가": result.get("매도호가", 0),
                "매수호가": result.get("매수호가", 0),
                "시장구분": position_item.get("시장구분", 1),
                "is_fakeout": is_fakeout,
                "max_profit_pct": position_item.get("max_profit_pct", 0.0),
            }

            trade_signal = self.ctx.trade_signal_cache.get(ticker)
            if not trade_signal:
                trade_signal = {
                    "v3_regime": getattr(self.ctx, "global_regime", "NEUTRAL"),
                    "tick_acc": 100.0,
                    "intrinsic_grade": position_item.get("grade", "B"),
                }

            with getattr(self.ctx, "order_lock", threading.Lock()):
                if (
                    hasattr(self.ctx, "active_smart_orders")
                    and ticker in self.ctx.active_smart_orders
                ):
                    if _tm.time() - self.ctx.active_smart_orders.get(ticker, 0) > 15.0:
                        del self.ctx.active_smart_orders[ticker]
                    else:
                        return

            if ticker not in portfolio_dict or portfolio_dict.get("수량", 0) <= 0:
                return

            sell_info = process_sell_orders(
                self.ctx,
                ticker,
                portfolio_dict,
                current_price,
                trade_signal,
                params=PARAMS,
            )
            if sell_info.get("is_sell"):
                current_ts = _tm.time()
                last_time = self.ctx._pending_sell_stocks.get(ticker, 0)
                is_emergency_exit = sell_info.get("action") in [
                    "STOP_LOSS",
                    "EMERGENCY_EXIT",
                    "LIMIT_UP_CRASH",
                ]
                sell_guard_threshold = 2.0 if is_emergency_exit else 15.0

                if current_ts - last_time < sell_guard_threshold:
                    return

                sell_command = prepare_sell_order_params(
                    self.ctx, ticker, portfolio_dict["수량"], current_price, sell_info
                )
                if sell_command:
                    with getattr(self.ctx, "order_lock", threading.Lock()):
                        if ticker in self.ctx.active_smart_orders:
                            return
                        self.ctx.active_smart_orders[ticker] = _tm.time()

                    cl_ord_id = str(_uuid.uuid4())[:8]
                    sell_command["cl_ord_id"] = cl_ord_id
                    with self.ctx.cl_ord_lock:
                        self.ctx.pending_cl_orders[cl_ord_id] = {
                            "ticker": ticker,
                            "side": "sell",
                            "time": _tm.time(),
                            "qty": sell_command["qty"],
                        }
                    self.ctx.zmq_push.push_data(sell_command)
                    self.ctx._pending_sell_stocks[ticker] = current_ts
                    logger.info(
                        f"⚡ [ZMQ] SELL Triggered: {ticker} | reason={sell_info.get('reason')} | ClOrdID: {cl_ord_id}"
                    )

        except Exception as e:
            logger.error(f"[EventLoopManager] _handle_tick error: {e}")
