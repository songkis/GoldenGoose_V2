import logging
import time as _tm
import uuid as _uuid
import threading
from strategy.core.TradingComm import prepare_sell_order_params
from strategy.core.execution_engine import process_sell_orders
from config.ai_settings import PARAMS

logger = logging.getLogger(__name__)

def handle_tick_logic(manager, tick_data):
    ctx = manager.ctx
    try:
        ticker = tick_data.get("ticker")
        result = tick_data.get("data", {})
        if not ticker or not result: return
        current_price = result.get("현재가", 0)
        if not current_price: return

        current_ts = _tm.time()
        with ctx.tick_lock:
            if ticker not in ctx.realtime_tick_cache:
                ctx.realtime_tick_cache[ticker] = {"ticks": [], "ask_sum": 0, "bid_sum": 0, "cpower": 0.0}
            cache = ctx.realtime_tick_cache[ticker]
            cache["cpower"] = result.get("체결강도", cache["cpower"])
            
            for key, fields in [("best_bid", ["매수호가", "매수호가1", "매수1호가"]), ("best_ask", ["매도호가", "매도호가1", "매도1호가"])]:
                for f in fields:
                    val = result.get(f)
                    if val is not None:
                        cache[key] = abs(int(val[0])) if isinstance(val, list) and len(val) > 0 else abs(int(val))
                        break

            cache["ask_sum"] = result.get("총매도호가잔량", result.get("매도10호가총잔량", result.get("매도누적체결량", cache["ask_sum"])))
            cache["bid_sum"] = result.get("총매수호가잔량", result.get("매수10호가총잔량", result.get("매수누적체결량", cache["bid_sum"])))
            cache["ticks"].append({"ts": current_ts, "vol": result.get("체결량", 0)})
            cache["ticks"] = [t for t in cache["ticks"] if t["ts"] >= current_ts - 10.0]

        tick_count, cpower = len(cache.get("ticks", [])), float(cache.get("cpower", 0.0))
        ask_sum, bid_sum = float(cache.get("ask_sum", 0.0)), float(cache.get("bid_sum", 0.0))
        is_fakeout = (bid_sum / (ask_sum + 1.0)) < 1.5
        if tick_count >= 8 or cpower >= 150: is_fakeout = False

        ctx.cpower_history[ticker].append(cpower)
        today_open = float(result.get("시가", current_price))
        is_cpower_decay = len(ctx.cpower_history[ticker]) == 3 and ctx.cpower_history[ticker][0] > ctx.cpower_history[ticker][1] > ctx.cpower_history[ticker][2]

        if (current_price > today_open * 1.08) and is_cpower_decay:
            if not hasattr(ctx, "high_price_locks"): ctx.high_price_locks = {}
            if ticker not in ctx.high_price_locks:
                ctx.high_price_locks[ticker] = _tm.time()
                logger.warning(f"🔒 [Hard Lock] {ticker} 고점(+8%↑) 수급 하락 감지. 매수 신호 HOLD 강제 전환.")

        if ticker not in ctx.active_positions_map: return

        position_item = ctx.active_positions_map[ticker]
        buy_price = position_item.get("매수가", position_item.get("진입가", 0))
        if buy_price > 0:
            current_profit_pct = (current_price - buy_price) / buy_price * 100.0
            position_item["max_profit_pct"] = max(position_item.get("max_profit_pct", 0.0), current_profit_pct)

        portfolio_dict = {
            "매수가": buy_price, "수량": position_item.get("수량", position_item.get("포지션수량", 0)),
            "매수후고가": position_item.get("매수후고가", position_item.get("최고가", 0)),
            "매수일": position_item.get("매수일", "UNKNOWN"), "현재가": current_price,
            "체결강도": result.get("체결강도", 0), "체결량": result.get("체결량", 0),
            "누적거래량": result.get("누적거래량", 0), "매도호가": result.get("매도호가", 0),
            "매수호가": result.get("매수호가", 0), "시장구분": position_item.get("시장구분", 1),
            "is_fakeout": is_fakeout, "max_profit_pct": position_item.get("max_profit_pct", 0.0),
        }

        trade_signal = ctx.trade_signal_cache.get(ticker, {"v3_regime": getattr(ctx, "global_regime", "NEUTRAL"), "tick_acc": 100.0, "intrinsic_grade": position_item.get("grade", "B")})

        with getattr(ctx, "order_lock", threading.Lock()):
            if hasattr(ctx, "active_smart_orders") and ticker in ctx.active_smart_orders:
                if _tm.time() - ctx.active_smart_orders.get(ticker, 0) > 15.0: del ctx.active_smart_orders[ticker]
                else: return

        if portfolio_dict.get("수량", 0) <= 0: return

        sell_info = process_sell_orders(ctx, ticker, portfolio_dict, current_price, trade_signal, params=PARAMS)
        if sell_info.get("is_sell"):
            last_time = ctx._pending_sell_stocks.get(ticker, 0)
            is_emergency = sell_info.get("action") in ["STOP_LOSS", "EMERGENCY_EXIT", "LIMIT_UP_CRASH"]
            if _tm.time() - last_time < (2.0 if is_emergency else 15.0): return

            sell_command = prepare_sell_order_params(ctx, ticker, portfolio_dict["수량"], current_price, sell_info)
            if sell_command:
                with getattr(ctx, "order_lock", threading.Lock()):
                    if ticker in ctx.active_smart_orders: return
                    ctx.active_smart_orders[ticker] = _tm.time()

                cl_ord_id = str(_uuid.uuid4())[:8]
                sell_command["cl_ord_id"] = cl_ord_id
                with ctx.cl_ord_lock:
                    ctx.pending_cl_orders[cl_ord_id] = {"ticker": ticker, "side": "sell", "time": _tm.time(), "qty": sell_command["qty"]}
                ctx.zmq_push.push_data(sell_command)
                ctx._pending_sell_stocks[ticker] = _tm.time()
                logger.info(f"⚡ [ZMQ] SELL Triggered: {ticker} | reason={sell_info.get('reason')} | ClOrdID: {cl_ord_id}")
    except Exception as e:
        logger.error(f"[EventLoopManager] _handle_tick error: {e}")
