import logging
import datetime
import time as _tm
import pandas as pd

import uuid as _uuid

from util.CommUtils import (
    to_numeric_safe,
    is_buyable,
)
from util.Utils import align_quote_price
from util.zmq_manager import make_json_safe
from strategy.core.TradingComm import (
    prepare_sell_order_params,
    send_order_payload,
    get_bars_since_entry,
)
from strategy.core.TradeExitEngine import (
    optimize_exit_strategy,
    calculate_hybrid_stop_loss,
)
from config.ai_settings import SYS_ID
from core.schemas import PositionInfo, SignalPayload

logger = logging.getLogger(__name__)


def _safe_get(obj, key, default=None):
    """
    [Zero-Defect Helper] 데이터클래스와 딕셔너리 객체 모두에서 안전하게 값을 추출합니다.
    """
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


class ExecutionPipeline:
    """
    [ExecutionPipeline] 매매 집행 파이프라인 (IntradayTrading에서 분리)
    Zero-Defect Operational Parity with Monolithic Version.
    """

    def __init__(self, context):
        self.ctx = context  # IntradayTrading instance

    def run_execution_pipeline(self, candidates_pool, capital=0):
        """
        [V11.0: Zero-Defect Executive Tier]
        Extracted from IntradayTrading.process_analysis_results
        """
        try:
            if not candidates_pool:
                return

            batch_data_trace_signal = []
            batch_update_high = []
            batch_update_high_port = []
            batch_upsert_position_dict = {}
            batch_update_closed = []  # 🛠️ [Fix 1] 청산 DB 기록을 위한 대기열 초기화
            reject_stocks = []
            new_buys_count = 0
            slots_taken_in_loop = 0

            # [Phase 1: Concurrent Slot Management]
            virtual_running_slots = len(self.ctx.portfolio_list)
            freed_slots_in_loop = 0

            # [ZMQ Sync: Slot Estimation]
            if self.ctx.account_info:
                ai_slots = int(
                    to_numeric_safe(self.ctx.account_info.get("aigoose_slots", 0))
                )
                guard_slots = int(
                    to_numeric_safe(self.ctx.account_info.get("guardian_slots", 0))
                )
                total_eval_count = int(
                    to_numeric_safe(self.ctx.account_info.get("total_eval_count", 0))
                )
                virtual_running_slots = max(
                    virtual_running_slots, ai_slots + guard_slots, total_eval_count
                )

            # [Capital Calculation]
            # [Slot Guard: concentrated trading limit]
            total_slots = self.ctx.system_params.get("max_positions", 15)
            if self.ctx.account_info:
                total_slots = int(
                    to_numeric_safe(
                        self.ctx.account_info.get("total_slot_cnt", total_slots)
                    )
                )
            # Directive 3: Hard-cap at 15
            total_slots = min(total_slots, 15)

            # [Order Orchestration: Priority Sorting]
            def get_priority_key(x):
                f_dec = x.get("v3_indicators", {}).get("final_trading_decision", {})
                grade = f_dec.get("final_grade", "C")
                priority = {"S": 3, "A": 2, "B": 1}.get(grade, 0)
                return (priority, x.get("combined_score", 0))

            candidates_pool = sorted(
                candidates_pool, key=get_priority_key, reverse=True
            )

            # [Hardening: Global Asset Guard Initialization]
            # [Quant] 현재 보유 총 매입금액 + 미체결 매수 주문 합계를 계산하여 전역 자산 한도 관리
            port_value = 0.0
            if hasattr(self.ctx, "dfPortfolio") and not self.ctx.dfPortfolio.empty:
                # [Fix] '매입금액' 컬럼이 DB에 없으므로 매수가 * 수량으로 계산 (벡터화 연산 보장)
                df = self.ctx.dfPortfolio
                # pandas Series 연산을 위해 pd.to_numeric 명시적 사용
                buy_prices = pd.to_numeric(df["매수가"], errors="coerce").fillna(0)
                buy_qtys = pd.to_numeric(df["수량"], errors="coerce").fillna(0)
                port_value = (buy_prices * buy_qtys).sum()

            # 미체결 매수 주문 금액 가산
            pending_buy_value = 0.0
            with self.ctx.cl_ord_lock:
                for ord_info in self.ctx.pending_cl_orders.values():
                    if ord_info.get("side") == "buy":
                        pending_buy_value += to_numeric_safe(
                            ord_info.get("qty", 0)
                        ) * to_numeric_safe(ord_info.get("price", 0))

            total_invested_value = port_value + pending_buy_value
            asset_limit = self.ctx.total_equity * 0.98  # 2% 슬리피지/수수료 버퍼

            logger.info(
                f"🛡️ [Asset Guard] Current Invested: {total_invested_value:,.0f} | Limit: {asset_limit:,.0f} (Equity: {self.ctx.total_equity:,.0f})"
            )

            for idx, candidate in enumerate(candidates_pool):
                try:
                    stock_cd = candidate.get("ticker")
                    if not stock_cd:
                        continue

                    v3_indicators = candidate.get("v3_indicators", {})
                    minute_df = candidate.get("minute_df")
                    latest_daily = candidate.get("latest_row")

                    # [Zero-Defect] 루프 진입 추적 로그 (데이터 유실 여부 즉시 판별)
                    logger.info(
                        f"🔍 [Pipeline Trace] {stock_cd} Cycle Start (Idx:{idx}, Grade:{_safe_get(_safe_get(v3_indicators, 'final_trading_decision', {}), 'final_grade', 'G')})"
                    )

                    if minute_df is None or minute_df.empty or latest_daily is None:
                        logger.warning(
                            f"⚠️ [Pipeline Drop] {stock_cd}: Missing critical data (MinuteDF:{minute_df is None}, LatestRow:{latest_daily is None})"
                        )
                        continue

                    # [Price Sync] Real-time Tick Cache Calibration
                    current_price = 0
                    with self.ctx.tick_lock:
                        if stock_cd in self.ctx.realtime_tick_cache:
                            tick_cache = self.ctx.realtime_tick_cache[stock_cd]
                            current_price = tick_cache.get("price", 0)

                    if current_price <= 0:
                        current_price = minute_df["종가"].iloc[-1]
                        if current_price <= 0:
                            logger.error(
                                f"❌ [Pipeline Drop] {stock_cd}: Zero Price detected."
                            )
                            continue

                    combined_score = candidate.get("combined_score", 0)
                    params = self.ctx.system_params

                    # [TLVI: Variable Extraction]
                    final_decision = v3_indicators.get("final_trading_decision", {})
                    final_stock_eval = v3_indicators.get("final_stock_eval", {})
                    execution_trigger = v3_indicators.get("execution_trigger") or {}
                    hybrid_exit_levels = v3_indicators.get("hybrid_exit_levels", {})

                    # [Phase 0: Metric Extraction]
                    intra_acc = _safe_get(
                        execution_trigger,
                        "intra_acc",
                        _safe_get(execution_trigger, "tick_acc", 100.0),
                    )
                    can_buy = _safe_get(final_decision, "final_can_buy", False)
                    grade = _safe_get(final_decision, "final_grade", "G")
                    is_buy = _safe_get(final_stock_eval, "is_buy", False)
                    trigger_hit = _safe_get(execution_trigger, "trigger_hit", 0)
                    final_reason = _safe_get(final_decision, "final_reason", "")
                    is_recovering_leader = _safe_get(
                        final_stock_eval, "is_recovering_leader", False
                    )

                    # [Phase 1: Pipeline Strict Guard & Zombie Kill-Switch]
                    # 사면권(VIP) 발동 조건 추출 및 엣지 결여 시 자격 박탈
                    in_portfolio = stock_cd in self.ctx.portfolio_list
                    combined_score = candidate.get("combined_score", 0.0)

                    # [Desync Fix] v3_indicators root 및 decision 내부를 모두 뒤져서 사면권 확인
                    has_pardon_flag = _safe_get(
                        v3_indicators, "has_pardon", False
                    ) or _safe_get(final_decision, "has_pardon", False)
                    has_vip_pass = (
                        has_pardon_flag or is_recovering_leader or grade in ["S", "A"]
                    )

                    if combined_score <= 0.0 and not has_pardon_flag:
                        has_vip_pass = False

                    if not (
                        is_buyable(
                            can_buy,
                            is_buy,
                            trigger_hit,
                            grade,
                            is_recovering_leader=is_recovering_leader,
                            reason_str=final_reason,
                        )
                        or in_portfolio
                        or has_vip_pass
                    ):
                        if not in_portfolio and combined_score > 50:
                            logger.info(
                                f"🚫 [Pipeline Gate] {stock_cd} Rejected: Buyable Filter Failed (Pardon:{has_vip_pass})"
                            )
                        continue

                    # [Status check]
                    has_pardon = has_vip_pass

                    # [TLVI] 필수 지표 초기화 및 산출
                    atr_5m = candidate.get(
                        "atr_5m", latest_daily.get("ATR", current_price * 0.02)
                    )
                    avg_amt_5m = 0.0
                    if (
                        minute_df is not None
                        and not minute_df.empty
                        and len(minute_df) >= 5
                    ):
                        avg_amt_5m = (
                            minute_df["거래량"].iloc[-5:] * minute_df["종가"].iloc[-5:]
                        ).mean()

                    # [Quant & Kelly] Sizing (동적 비중배율 파워 트레인)
                    # [Zero-Defect] 사이징의 기준을 단순 현금(avail_cash)이 아닌 추정순자산(total_equity)으로 변경하여 Kelly의 수학적 의도 보존
                    total_asset_for_sizing = (
                        self.ctx.total_equity
                        if self.ctx.total_equity > 0
                        else (capital if capital > 0 else self.ctx.virtual_avail_cash)
                    )

                    # [Context Sync] 만약 ctx 내부 자본금이 0이라면 외부 주입된 capital로 동기화
                    if self.ctx.virtual_avail_cash <= 0 and total_asset_for_sizing > 0:
                        self.ctx.virtual_avail_cash = total_asset_for_sizing

                    per_slot_capital = total_asset_for_sizing / max(1, total_slots)

                    # [Phase 2: Brian Sizing Synchronization]
                    # 브레인 산출 비중(Kelly)을 최우선 적용. 5% 미만 할당 시 기각.
                    brain_ratio = float(
                        _safe_get(final_decision, "position_size_ratio", 0.0)
                    )
                    # if brain_ratio < 0.05:
                    #     # 브레인이 할당한 비중이 5% 미만이거나 0인 경우 매수 기각 (자본 기아 및 수수료 방어)
                    #     if not in_portfolio and combined_score > 50:
                    #         logger.info(
                    #             f"⚠️ [Pipeline Skip] {stock_cd} Rejected: Too Small Ratio ({brain_ratio:.4f})"
                    #         )
                    #     continue

                    current_capital = total_asset_for_sizing * brain_ratio

                    # [Security] 주문 가능 현금 범위를 초과할 수 없음
                    current_capital = min(
                        current_capital, per_slot_capital, self.ctx.virtual_avail_cash
                    )

                    # [Status check]
                    position = self.ctx.active_positions_map.get(stock_cd, {})
                    status = position.get("상태", "none")
                    매수가 = to_numeric_safe(position.get("진입가", 0))
                    수량 = int(to_numeric_safe(position.get("포지션수량", 0)))
                    highest_price = to_numeric_safe(
                        position.get("최고가", current_price)
                    )
                    initial_sl = to_numeric_safe(position.get("청산가", 0))
                    take_profit1 = to_numeric_safe(position.get("목표가1", 0))
                    take_profit2 = to_numeric_safe(position.get("목표가2", 0))
                    trade_strategy = position.get("매매기법", "breakout")

                    # [Directive 2] Single-Bullet Enforcement: No pyramiding logic allowed.

                    # [Guard] Time Guard
                    # now_time = datetime.datetime.now().time()
                    # if now_time >= datetime.time(14, 50) and status == "none":
                    #     logger.info(
                    #         f"🚫 [Pipeline Policy] {stock_cd} Postponed: After market hours buy block (14:50+). Current: {now_time}"
                    #     )
                    #     continue

                    # [Phase 2: Exit Logic (For existing positions)]
                    if in_portfolio:
                        # Update high price
                        if current_price > highest_price:
                            highest_price = current_price
                            batch_update_high.append(
                                (round(float(highest_price), 2), stock_cd, SYS_ID)
                            )

                        # Re-anchor SL if actual fill price is known
                        if 매수가 > 0 and initial_sl == 0:
                            exit_strategy = optimize_exit_strategy(
                                stock_cd,
                                매수가,
                                intra_acc,
                                hybrid_exit_levels,
                                grade,
                                params,
                            )
                            initial_sl = to_numeric_safe(
                                exit_strategy.get("final_stop_loss", 0.0)
                            )
                            take_profit1 = to_numeric_safe(
                                hybrid_exit_levels.get("take_profit1", 0.0)
                            )
                            take_profit2 = to_numeric_safe(
                                hybrid_exit_levels.get("take_profit2", 0.0)
                            )
                        # Re-anchor SL: DB 업데이트 로직 삭제 (메모리 관리로 I/O 오버헤드 삭감)

                        # Dynamic Exit Update
                        bars_count = get_bars_since_entry(
                            stock_cd, position.get("매수일", ""), minute_df
                        )
                        # [Zero-Defect] Initialize current_sl for Time-Decay logic
                        current_sl = initial_sl

                        # Time-Decay Exit
                        if 매수가 > 0:
                            profit_pct = (current_price - 매수가) / 매수가 * 100.0
                            if (
                                bars_count >= 20
                                and profit_pct < 0.5
                                and intra_acc < 80.0
                            ):
                                current_sl = max(current_sl, 매수가 * 0.998)

                        current_sl = calculate_hybrid_stop_loss(
                            current_price, 매수가, highest_price, atr_5m, current_sl
                        )

                        # Process Sell Orders (DTO Transformation)
                        portfolio_info = PositionInfo(
                            ticker=stock_cd,
                            entry_price=매수가,
                            qty=수량,
                            max_price_after_buy=highest_price,
                            current_price=current_price,
                            vwap=to_numeric_safe(latest_daily.get("VWAP", current_price)),
                            entry_time=position.get("매수일", ""),
                            tick_acc=intra_acc,
                            atr_5m=atr_5m,
                            max_profit_pct=to_numeric_safe(position.get("max_profit_pct", 0.0)),
                            stop_loss=current_sl,
                            take_profit1=take_profit1,
                            take_profit2=take_profit2,
                            bars_since_entry=bars_count
                        )
                        
                        # [State Migration] Access centralized recovering_leaders list safely
                        is_recovering = stock_cd in self.ctx.state_manager.get_recovering_leaders()
                        
                        # Pack context for downstream engines (Legacy Dict Support for internal mapping)
                        portfolio_dict = {
                            "매수가": 매수가,
                            "수량": 수량,
                            "매수후고가": highest_price,
                            "현재가": current_price,
                            "시장구분": 1 if latest_daily.get("시장구분") == 1 else 2,
                            "max_profit_pct": portfolio_info.max_profit_pct,
                            "is_recovering_leader": is_recovering,
                            "vwap": portfolio_info.vwap,
                            "buy_date": portfolio_info.entry_time,
                            "buy_price": portfolio_info.entry_price,
                        }
                        
                        # [TLVI Harness] Unpacking signals for legacy ExecutionEngine
                        temp_trade_signal = {
                            "stop_loss": current_sl,
                            "take_profit1": take_profit1,
                            "take_profit2": take_profit2,
                            "bars_since_entry": bars_count,
                            "tick_acc": intra_acc,
                            "atr_5m": atr_5m,
                        }

                        from strategy.core.execution_engine import process_sell_orders

                        sell_info = process_sell_orders(
                            self.ctx,
                            stock_cd,
                            portfolio_dict,
                            current_price,
                            temp_trade_signal,
                            params=params,
                        )

                        if _safe_get(sell_info, "final_can_sell", False):
                            # [Zero-Defect Guard] 중복 매도 방지 (최근 15초 이내 동일 종목 매도 발송 이력 확인)
                            last_sell_time = self.ctx._pending_sell_stocks.get(
                                stock_cd, 0
                            )
                            if _tm.time() - last_sell_time < 15.0:
                                continue

                            sell_command = prepare_sell_order_params(
                                self.ctx, stock_cd, 수량, current_price, sell_info
                            )
                            if sell_command:
                                cl_ord_id = str(_uuid.uuid4())[:8]
                                sell_command["cl_ord_id"] = cl_ord_id
                                with self.ctx.cl_ord_lock:
                                    self.ctx.pending_cl_orders[cl_ord_id] = {
                                        "ticker": stock_cd,
                                        "side": "sell",
                                        "time": _tm.time(),
                                        "qty": 수량,
                                    }

                                send_order_payload(
                                    self.ctx.zmq_push,
                                    stock_cd,
                                    "SELL",
                                    수량 * current_price,
                                    indicators={
                                        "cl_ord_id": cl_ord_id,
                                        "qty": 수량,
                                        "reason": _safe_get(
                                            sell_info, "reason", "EXIT"
                                        ),
                                        "buy_date": _safe_get(
                                            sell_info,
                                            "buy_date",
                                            portfolio_dict.get("buy_date"),
                                        ),
                                        "buy_price": _safe_get(
                                            sell_info,
                                            "buy_price",
                                            portfolio_dict.get("buy_price"),
                                        ),
                                    },
                                )
                                # 🛠️ [Fix 2] ZMQ 발송 즉시 DB 청산(Closed) 업데이트 대기열에 추가 (Traceability)
                                profit_amt = (current_price - 매수가) * 수량
                                today_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                batch_update_closed.append(
                                    (round(float(current_price), 2), round(float(profit_amt), 0), today_str, stock_cd, SYS_ID)
                                )

                                self.ctx._pending_sell_stocks[stock_cd] = _tm.time()
                                if _safe_get(sell_info, "sell_ratio", 0.0) == 1.0:
                                    freed_slots_in_loop += 1
                                continue

                    # [Quota Guard] 상위 3개 신규 종목만 매수 집행 제한
                    if not in_portfolio and new_buys_count >= 3:
                        logger.info(
                            f"⏳ [Pipeline Wait] {stock_cd} Postponed: New buys quota reached (3/cycle)"
                        )
                        continue

                    # [Directive 2: Zombie Cut & Capital Rotation Engine]
                    # S/A급 주도주 진입이 예상되나 자본/슬롯이 부족한 경우 '좀비 종목' 강제 퇴출
                    is_high_conviction = grade in ["S", "A"]
                    is_slot_full = (virtual_running_slots - freed_slots_in_loop + slots_taken_in_loop) >= total_slots
                    is_capital_short = (total_invested_value + current_capital) > asset_limit

                    if not in_portfolio and is_high_conviction and (is_slot_full or is_capital_short):
                        # [Atomic Sacrifice Search] Iteration Error 방지를 위한 동기화 락 강제 적용
                        with self.ctx.order_lock:
                            zombie_list = []
                            # Fix: 타 종목 minute_df 부재로 인한 t_bars=0 버그 해결.
                            # 포트폴리오 내 수익률과 체결강도가 가장 저조한 종목 1개를 찾아 교체(Sacrifice) 대상으로 선정
                            worst_target = None
                            worst_score = 9999.0
                            
                            for target_cd, pos_info in self.ctx.active_positions_map.items():
                                t_tick = to_numeric_safe(pos_info.get("tick_acc", 100.0))
                                t_buy_p = to_numeric_safe(pos_info.get("진입가", 0))
                                t_curr_p = to_numeric_safe(pos_info.get("현재가", 0))
                                t_profit = ((t_curr_p - t_buy_p) / t_buy_p * 100.0) if t_buy_p > 0 else 0.0
                                
                                # 교체 타겟 점수 (낮을수록 나쁨)
                                target_score = t_profit + (t_tick / 100.0)
                                if target_score < worst_score:
                                    worst_score = target_score
                                    worst_target = (target_cd, pos_info, t_tick, t_profit)
                                    
                            if worst_target:
                                w_cd, w_pos, w_tick, w_profit = worst_target
                                # 좀비 조건: 수익률 1.5% 미만 AND 수급 폭발(50.0) 미만인 경우에만 컷
                                if w_profit < 1.5 and w_tick < 50.0:
                                    t_bars = 99  # logging 호환성용 dummy
                                    t_tick = w_tick
                                    t_profit = w_profit
                                    zombie_list.append((w_cd, w_pos))
                             
                            # 최대 1개 좀비 소각 집행 (안전성)
                            for z_ticker, z_pos in zombie_list[:1]:
                                z_qty = int(to_numeric_safe(z_pos.get("포지션수량", 0)))
                                z_price = to_numeric_safe(z_pos.get("현재가", 0))
                                if z_qty > 0:
                                    logger.warning(f"💀 [Zombie-Cut] Sacrificing {z_ticker} for {stock_cd} (Bars:{t_bars}, Tick:{t_tick:.1f}, Profit:{t_profit:.2f}%)")
                                    
                                    # ZMQ 시장가 매도 ("03") 발송 (SignalPayload 적용)
                                    z_payload = SignalPayload(
                                        ticker=z_ticker,
                                        order_type="SELL",
                                        price=z_price,
                                        quantity=z_qty,
                                        cl_ord_id=f"ZMB_{str(_uuid.uuid4())[:4]}",
                                        reason=f"ZOMBIE_CUT_FOR_{stock_cd}"
                                    )
                                    self.ctx.zmq_push.push_data(z_payload.to_dict())
                                    
                                    # [Haircut 0.98] 보수적 자본 선제 회복 (2% 페널티)
                                    recovered_cash = z_price * z_qty * 0.98
                                    with self.ctx.capital_lock:
                                        self.ctx.virtual_avail_cash += recovered_cash
                                    
                                    total_invested_value -= (z_price * z_qty)
                                    freed_slots_in_loop += 1
                                    logger.info(f"💰 [Haircut Recovery] {z_ticker} 소각 후 {recovered_cash:,.0f}원 선행 회수 (Haircut 0.98 적용)")

                    # [Slot Guard] 최종 슬롯 체크
                    if (virtual_running_slots - freed_slots_in_loop + slots_taken_in_loop) >= total_slots:
                        logger.warning(f"❌ [Pipeline Block] {stock_cd} Rejected: Portfolio Full ({virtual_running_slots}/{total_slots})")
                        continue

                    # [Phase 4: Final Verification Gate]
                    # VIP 패스(S/A등급) 또는 사면권 보유 시 모든 기술적 필터 바이패스 집행
                    if (
                        is_buyable(
                            can_buy,
                            is_buy,
                            trigger_hit,
                            grade,
                            is_recovering_leader=is_recovering_leader,
                            reason_str=final_reason,
                        )
                        or in_portfolio
                        or has_vip_pass
                    ):
                        if current_capital > 0:
                            # [Zero-Defect Guard] 중복 매수 방지 로직 강화
                            # 1. 최근 30초 이내 동일 종목 매수 발송 이력 확인
                            # 2. 현재 Smart Order(Iceberg 등) 진행 중인 종목 차단
                            # 3. 이미 포트폴리오에 있는 경우 불필요한 추가 매수 차단 (피라미딩 제외)
                            last_buy_time = self.ctx._pending_buy_stocks.get(
                                stock_cd, 0
                            )
                            last_smart_order_time = self.ctx.active_smart_orders.get(
                                stock_cd, 0
                            )
                            now_ts = _tm.time()

                            # Cooldown을 30초로 상향 (ZMQ/Broker 지연 대응)
                            is_in_cooldown = now_ts - last_buy_time < 30.0
                            is_smart_order_active = (
                                stock_cd in self.ctx.active_smart_orders
                                and now_ts - last_smart_order_time < 60.0
                            )

                            if is_in_cooldown or is_smart_order_active:
                                logger.info(
                                    f"⏳ [Pipeline Cooldown] {stock_cd} skipped: Order active/cooldown. (Last:{now_ts - last_buy_time:.1f}s, Smart:{stock_cd in self.ctx.active_smart_orders})"
                                )
                                continue

                            # [Global Asset Guard] 전역 자산 한도 내 집행 가능 여부 최종 확인
                            if (total_invested_value + current_capital) > asset_limit:
                                logger.warning(
                                    f"🛡️ [Asset Guard] {stock_cd} BUY Blocked: Total Value would exceed limit ({total_invested_value + current_capital:,.0f} > {asset_limit:,.0f})"
                                )
                                continue

                            # [Position Hard-Cap] 현재 보유 금액 + 주문 예정 금액이 목표 자본을 초과하는지 검증
                            current_val = 수량 * current_price
                            if current_val >= (current_capital * 0.9):
                                logger.info(
                                    f"🚫 [Position Lock] {stock_cd} already reaches target size ({current_val:,.0f} >= {current_capital:,.0f})"
                                )
                                continue
                            # Prepare and send BUY order
                            cl_ord_id = str(_uuid.uuid4())[:8]
                            buy_qty = (
                                int(current_capital // current_price)
                                if current_price > 0
                                else 0
                            )
                            if buy_qty > 0:
                                buy_price = align_quote_price(current_price * 1.002)
                                buy_payload = SignalPayload(
                                    ticker=stock_cd,
                                    order_type="BUY",
                                    price=buy_price,
                                    quantity=buy_qty,
                                    cl_ord_id=cl_ord_id,
                                    reason=grade
                                )
                                with self.ctx.cl_ord_lock:
                                    self.ctx.pending_cl_orders[cl_ord_id] = {
                                        "ticker": stock_cd,
                                        "side": "buy",
                                        "time": _tm.time(),
                                        "qty": buy_qty,
                                        "price": buy_price,
                                    }

                                # Atomic capital deduction
                                with self.ctx.capital_lock:
                                    self.ctx.virtual_avail_cash -= buy_qty * buy_price

                                logger.info(
                                    f"🚀 [ZMQ Dispatch] {stock_cd} BUY Signal Sent | Qty:{buy_qty} | Grade:{grade} | ID:{cl_ord_id}"
                                )
                                self.ctx.zmq_push.push_data(buy_payload.to_dict())
                                self.ctx.active_smart_orders[stock_cd] = _tm.time()
                                self.ctx._pending_buy_stocks[stock_cd] = _tm.time()
                                new_buys_count += 1
                                slots_taken_in_loop += 1
                                total_invested_value += buy_qty * buy_price
                                logger.info(
                                    f"🔥 [Execution] BUY Triggered: {stock_cd} | Grade:{grade} | Qty:{buy_qty} | Target:{current_capital:,.0f}"
                                )

                                # Record Initial Position State
                                exit_strategy = optimize_exit_strategy(
                                    stock_cd,
                                    buy_price,
                                    intra_acc,
                                    hybrid_exit_levels,
                                    grade,
                                    params,
                                )
                                init_sl = to_numeric_safe(
                                    exit_strategy.get("final_stop_loss", 0.0)
                                )
                                batch_upsert_position_dict[stock_cd] = (
                                    stock_cd,
                                    datetime.datetime.now().strftime("%Y-%m-%d"),
                                    round(float(buy_price), 2),
                                    0,
                                    init_sl,
                                    to_numeric_safe(
                                        hybrid_exit_levels.get("take_profit1", 0.0)
                                    ),
                                    to_numeric_safe(
                                        hybrid_exit_levels.get("take_profit2", 0.0)
                                    ),
                                    round(float(buy_price), 2),
                                    "regist",
                                    trade_strategy,
                                    SYS_ID,
                                )
                            else:
                                # Else for if buy_qty > 0
                                logger.warning(
                                    f"⚠️ [Execution] {stock_cd} BUY skipped: Calculated Qty is 0 (Cap: {current_capital:,.0f}, Price: {current_price:,.0f})"
                                )
                        else:
                            # Else for if current_capital > 0
                            _sizing_diagnosis = f"(Asset:{total_asset_for_sizing:,.0f}, Ratio:{brain_ratio:.4f})"
                            logger.warning(
                                f"💰 [Execution Block] {stock_cd} Rejected: Zero Capital for Sizing {_sizing_diagnosis} (Avail:{self.ctx.virtual_avail_cash:,.0f})"
                            )
                    else:
                        # Else for if (is_buyable or ... or has_vip_pass)
                        if not in_portfolio and combined_score > 50:
                            logger.info(
                                f"🚫 [Pipeline Gate] {stock_cd} Blocked at final verification gate (Pardon:{has_vip_pass}, Score:{combined_score:.1f}, Grade:{grade})"
                            )

                except Exception as ex:
                    logger.error(
                        f"❌ [Alpha Pipeline Error] {candidate.get('ticker')}: {ex}",
                        exc_info=True,
                    )

            # 🛠️ [Fix 3] Phase 4: Batch Dispatch에 매도 업데이트(batch_update_closed) 강제 주입
            if batch_upsert_position_dict or batch_update_high or batch_update_closed:
                if self.ctx.db_writer:
                    self.ctx.db_writer.push_batch(
                        batch_update_high=batch_update_high,
                        batch_upsert_position=list(batch_upsert_position_dict.values()),
                        batch_update_closed=batch_update_closed
                    )

        except Exception as e:
            logger.error(f"ExecutionPipeline Error: {e}", exc_info=True)
