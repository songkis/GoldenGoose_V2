import logging
from enum import Enum
from dataclasses import dataclass
from typing import Dict, Any
from core.schemas import ExitDecisionDTO, PositionData, ExitLevelsData
from strategy.core.TradeDecisionEngine import TradePersona
from config.ai_settings import PARAMS

logger = logging.getLogger("GoldenGoose.ExitBrain")


class ExitActionType(Enum):
    HOLD = "HOLD"
    LIMIT_UP_CRASH = "LIMIT_UP_BREAK_EXIT"  # 상한가 붕괴 긴급탈출
    EMERGENCY_CUT = "EMERGENCY_EXIT"  # 시장 패닉/수급 붕괴
    STOP_LOSS = "STOP_LOSS"  # 손절가 도달
    TRAILING_STOP = "TRAILING_STOP"  # 고점 대비 하락 (수익 보존)
    TAKE_PROFIT_PARTIAL = "TAKE_PROFIT_PARTIAL"  # 1차 목표가 도달
    TAKE_PROFIT_FINAL = "TAKE_PROFIT_FINAL"  # 최종 목표가 도달


class TradeExitEngine:
    _partial_sold_cache = {}
    _last_signal_time = {}

    def __init__(self):
        self.hard_take_profit_pct = (
            float(PARAMS.get("hard_take_profit_pct", 2.5)) / 100.0
        )
        sl_val = float(PARAMS.get("hard_stop_loss_pct", -2.5))
        if sl_val > 0:
            sl_val = -sl_val
        self.hard_stop_loss_pct = sl_val / 100.0

        self.slippage_buffer = float(PARAMS.get("slippage_buffer", 0.5)) / 100.0

        # [Sync] WFO 연동 동적 파라미터 바인딩 (변수명 통합)
        self.timecut_bars = int(PARAMS.get("timecut_bars", 45))
        self.be_act_s = float(PARAMS.get("be_activation_s", 1.07))
        self.be_act_norm = float(PARAMS.get("be_activation_normal", 1.03))
        self.be_pro_s = float(PARAMS.get("be_protect_s", 1.03))
        self.be_pro_norm = float(PARAMS.get("be_protect_normal", 1.01))

        logger.debug(
            f"⚙️ [Exit Brain] Initialized with global PARAMS: TP {self.hard_take_profit_pct * 100:.1f}%, SL {self.hard_stop_loss_pct * 100:.1f}%, Slip {self.slippage_buffer * 100:.2f}%, TimeCut {self.timecut_bars} bars"
        )

    def update_hard_limits(
        self,
        target_profit_pct: float,
        stop_loss_pct: float,
        slippage_buffer: float = None,
    ):
        self.hard_take_profit_pct = target_profit_pct
        self.hard_stop_loss_pct = stop_loss_pct
        if slippage_buffer is not None:
            self.slippage_buffer = slippage_buffer
        logger.info(
            f"🔄 [Exit Brain] Hard Limits Updated: TP {target_profit_pct * 100:.1f}%, SL {stop_loss_pct * 100:.1f}%, Slip {self.slippage_buffer * 100:.2f}%"
        )

    def evaluate_sell_decision(
        self,
        stock_code: str,
        position: PositionData,
        levels: ExitLevelsData,
        persona: TradePersona,
        signal_msg: str,
        is_limit_up_entry: bool,
        **kwargs,  # [WFO Adapter]
    ) -> ExitDecisionDTO:
        # [WFO Adapter] Update local policy from optimizer
        if kwargs:
            for k, v in kwargs.items():
                if hasattr(PARAMS, k):
                    setattr(PARAMS, k, v)
        import time

        current_time = time.time()
        last_time = self._last_signal_time.get(stock_code, 0)

        is_emergency_request = (
            "EMERGENCY" in signal_msg
            or "STRONG SELL" in signal_msg
            or "LIMIT_UP_BREAK" in signal_msg
        )

        if not is_emergency_request and (current_time - last_time < 5.0):
            return self._build_exit_result(
                stock_code, False, ExitActionType.HOLD, 0.0, reason="COOLDOWN_ACTIVE"
            )

        # 본절 지킴이 (Breakeven Trailing)
        be_trigger = float(PARAMS.get("BREAKEVEN_TRIGGER", 1.5))
        be_cut = float(PARAMS.get("BREAKEVEN_CUT", 0.3))
        if (
            position.max_profit_pct >= be_trigger
            and position.profit_rate * 100 <= be_cut
        ):
            return self._build_exit_result(
                stock_code,
                True,
                ExitActionType.TRAILING_STOP,
                1.0,
                reason="BREAKEVEN_GUARD",
                order_type="00",
            )

        # [Step 1] 타임스탑 (Dead-Time Exit) - [Sync] WFO timecut_bars 속성 직접 참조
        time_stop_threshold = self.timecut_bars
        bars_since = position.bars_since_entry

        dynamic_be_threshold = position.entry_price + (position.atr_5m * 0.25)

        if (
            bars_since >= time_stop_threshold
            and position.current_price < dynamic_be_threshold
        ):
            return self._build_exit_result(
                stock_code,
                True,
                ExitActionType.EMERGENCY_CUT,
                1.0,
                reason=f"TIME_DECAY_BREAK_EVEN ({bars_since}s > {time_stop_threshold}s & Price Below E(x) Edge)",
                order_type="00",
            )

        current_hm = 0
        if position.current_time_str:
            try:
                time_part = (
                    position.current_time_str.split(" ")[1]
                    if " " in position.current_time_str
                    else position.current_time_str
                )
                hour, minute = (
                    int(time_part.split(":")[0]),
                    int(time_part.split(":")[1]),
                )
                current_hm = hour * 100 + minute
            except Exception:
                pass

        if current_hm >= 1430:
            eod_sl_price = max(levels.stop_loss, position.purchase_price * 0.995)
            if position.current_price < eod_sl_price:
                return self._build_exit_result(
                    stock_code,
                    True,
                    ExitActionType.STOP_LOSS,
                    1.0,
                    reason="EOD_TIGHTENING_SL",
                    order_type="00",
                )

        optimal_order_type = "00"

        k_adj = 1.5 if position.tick_acc > 80 else 3.0
        atr_margin = position.atr_5m * k_adj
        dynamic_sl_price = position.purchase_price - atr_margin

        if position.current_price <= dynamic_sl_price:
            if (
                position.current_price >= position.vwap or position.grade in ["S", "A"]
            ) and position.tick_acc >= 40.0:
                pass
            else:
                return self._build_exit_result(
                    stock_code,
                    True,
                    ExitActionType.EMERGENCY_CUT,
                    1.0,
                    reason=f"SLIPPAGE_DEFENSE (ATR {k_adj}x 붕괴: -{atr_margin / position.purchase_price * 100:.2f}%)",
                    order_type="00",
                )

        target_tp_ratio = getattr(self, "hard_take_profit_pct", 0.0)
        if (
            hasattr(levels, "hard_take_profit_ratio")
            and levels.hard_take_profit_ratio > 0.0
        ):
            target_tp_ratio = max(target_tp_ratio, levels.hard_take_profit_ratio)

        if target_tp_ratio > 0.0:
            is_bull_regime = position.market_regime in ["BULL", "SUPER_BULL"]
            is_leader_stock = position.grade in ["S", "A"]
            is_momentum_exploding = (position.tick_acc >= 15.0) and (
                position.profit_rate > self.slippage_buffer
            )

            if (is_bull_regime and is_leader_stock) or is_momentum_exploding:
                target_tp_ratio = 999.0
                levels.take_profit1 = position.purchase_price * 10.0
                levels.take_profit2 = position.purchase_price * 10.0
                if is_momentum_exploding:
                    logger.info(
                        f"🚀 [Profit Maximization] {stock_code} 수급 폭발(Acc:{position.tick_acc:.1f})! Hard-TP 해제 및 Trailing 전환."
                    )
            elif position.profit_rate >= target_tp_ratio:
                return self._build_exit_result(
                    stock_code,
                    True,
                    ExitActionType.TAKE_PROFIT_FINAL,
                    1.0,
                    reason=f"HARD_TP_HIT (+{position.profit_rate * 100:.1f}% >= 목표 +{target_tp_ratio * 100:.1f}%)",
                    order_type="00",
                )

        if hasattr(levels, "max_holding_days") and levels.max_holding_days > 0:
            days = getattr(position, "days_since_entry", 0)
            if days >= levels.max_holding_days:
                return self._build_exit_result(
                    stock_code,
                    True,
                    ExitActionType.EMERGENCY_CUT,
                    1.0,
                    reason=f"MAX_HOLDING_EXCEEDED (보유 {days}영업일 >= 한도 {levels.max_holding_days}일)",
                    order_type="00",
                )

        if is_limit_up_entry:
            if (
                "LIMIT_UP_BREAK_EXIT" in signal_msg
                or "LIMIT_UP_DRAIN_PREVENT" in signal_msg
            ):
                return self._build_exit_result(
                    stock_code,
                    True,
                    ExitActionType.LIMIT_UP_CRASH,
                    1.0,
                    order_type="00",
                )

            limit_up_trailing = position.max_price_after_buy * 0.96
            if position.current_price <= limit_up_trailing:
                return self._build_exit_result(
                    stock_code,
                    True,
                    ExitActionType.LIMIT_UP_CRASH,
                    1.0,
                    reason="LIMIT_UP_PHYSICAL_BREAK (고점대비 -4% 투매 감지)",
                    order_type="00",
                )

            if position.current_price >= levels.take_profit2:
                return self._build_exit_result(
                    stock_code,
                    True,
                    ExitActionType.TAKE_PROFIT_FINAL,
                    1.0,
                    order_type=optimal_order_type,
                )
            return self._build_exit_result(stock_code, False, ExitActionType.HOLD, 0.0)

        if current_hm > 0:
            try:
                if 1330 <= current_hm <= 1430:
                    if not is_limit_up_entry and position.profit_rate >= 1.0:
                        return self._build_exit_result(
                            stock_code,
                            True,
                            ExitActionType.TAKE_PROFIT_FINAL,
                            1.0,
                            reason=f"TIME_CUT_PROFIT (마의 구간 {current_hm} 진입)",
                            order_type=optimal_order_type,
                        )
                if current_hm >= 1515 and not is_limit_up_entry:
                    if position.grade not in ["S", "A"] or position.profit_rate < 0:
                        return self._build_exit_result(
                            stock_code,
                            True,
                            ExitActionType.EMERGENCY_CUT,
                            1.0,
                            reason=f"OVERNIGHT_AVOID (장 마감 {current_hm} 강제 청산)",
                            order_type="00",
                        )
            except Exception:
                pass

        if "STRONG SELL" in signal_msg or "EMERGENCY SELL" in signal_msg:
            return self._build_exit_result(
                stock_code, True, ExitActionType.EMERGENCY_CUT, 1.0, order_type="00"
            )

        applied_sl = levels.stop_loss

        is_tick_acc_dead = position.tick_acc < 30.0
        if is_tick_acc_dead and applied_sl > 0:
            margin_ratio = 1.01 if position.grade in ["S", "A"] else 1.015
            front_run_price = applied_sl * margin_ratio

            is_trend_broken = not (position.current_price >= position.vwap)
            is_v_bounce = "V-Bounce" in str(position.reason) or "Pardon" in str(
                position.reason
            )
            is_mature = position.bars_since_entry >= 60

            apply_whipsaw_guard = (not is_trend_broken) or (
                position.grade in ["S", "A"]
            )
            if is_v_bounce and not is_mature:
                apply_whipsaw_guard = True

            if position.current_price <= front_run_price and not apply_whipsaw_guard:
                return self._build_exit_result(
                    stock_code,
                    True,
                    ExitActionType.STOP_LOSS,
                    1.0,
                    reason=f"CHANDELIER_FRONT_RUN (TickAcc 급감 {position.tick_acc:.1f} & 추세 붕괴)",
                    order_type="00",
                )

        if levels.stop_loss > 0 and position.current_price <= levels.stop_loss:
            return self._build_exit_result(
                stock_code,
                True,
                ExitActionType.STOP_LOSS,
                1.0,
                reason="SYSTEM_STOP_LOSS (지정 손절가 이탈)",
                order_type=optimal_order_type,
                target_price=position.current_price,
            )

        if "ADAPTIVE EXIT" in signal_msg:
            return self._build_exit_result(
                stock_code, True, ExitActionType.EMERGENCY_CUT, 1.0, order_type="00"
            )

        if position.current_price >= levels.take_profit2:
            return self._build_exit_result(
                stock_code,
                True,
                ExitActionType.TAKE_PROFIT_FINAL,
                1.0,
                order_type=optimal_order_type,
            )

        if (
            "PARTIAL SELL" in signal_msg
            or position.current_price >= levels.take_profit1
        ):
            if position.grade in ["S", "A"] and position.tick_acc >= 50.0:
                return self._build_exit_result(
                    stock_code, False, ExitActionType.HOLD, 0.0
                )

            last_sold_time = self._partial_sold_cache.get(stock_code, 0)
            if (current_time - last_sold_time) < 43200:
                return self._build_exit_result(
                    stock_code, False, ExitActionType.HOLD, 0.0
                )

            if position.initial_quantity > 0 and position.quantity <= (
                position.initial_quantity * 0.6
            ):
                pass
            else:
                ratio = (
                    0.3
                    if persona == TradePersona.BULL_AGGRESSIVE
                    else (0.7 if persona == TradePersona.BEAR_DEFENSIVE else 0.5)
                )
                if position.current_price * position.quantity < 100000:
                    ratio = 1.0
                self._partial_sold_cache[stock_code] = current_time
                return self._build_exit_result(
                    stock_code,
                    True,
                    ExitActionType.TAKE_PROFIT_PARTIAL,
                    ratio,
                    order_type=optimal_order_type,
                )

        if position.tick_acc < 5.0 and position.profit_rate > 0.005:
            vol_activation_margin = 0.0
        else:
            vol_activation_margin = (
                position.atr_5m * 1.5
                if position.atr_5m > 0
                else position.purchase_price * 0.02
            )

        if position.current_price >= (position.purchase_price + vol_activation_margin):
            ts_multiplier = 2.0
            if position.profit_rate < 0.02:
                dynamic_ts_price = position.max_price_after_buy * 0.99
            else:
                profit_pct = position.profit_rate * 100.0
                is_ofi_strong = (position.tick_acc >= 20.0) and (
                    position.grade in ["S", "A"]
                )

                dynamic_slope = PARAMS.get("ofi_damping_slope", 0.05)
                slope = dynamic_slope if is_ofi_strong else 0.15
                ts_multiplier = max(1.0, 2.0 - ((profit_pct - 2.0) * slope))
                core_multiplier = ts_multiplier * 2.0
                dynamic_ts_trade = position.max_price_after_buy - (
                    position.atr_5m * ts_multiplier
                )
                dynamic_ts_core = position.max_price_after_buy - (
                    position.atr_5m * core_multiplier
                )

                if position.current_price <= dynamic_ts_trade:
                    ts_order_type = "00"

                    if position.current_price <= dynamic_ts_core:
                        reason_tag = (
                            "PARABOLIC_TS_CORE_EXIT (Core 추세 붕괴)"
                            if (
                                position.profit_rate >= 0.02
                                and (
                                    position.tick_acc >= 20.0
                                    and position.grade in ["S", "A"]
                                )
                            )
                            else "PARABOLIC_TS_CORE_EXIT"
                        )
                        return self._build_exit_result(
                            stock_code,
                            True,
                            ExitActionType.STOP_LOSS,
                            1.0,
                            reason=reason_tag,
                            order_type=ts_order_type,
                        )

            ts_pct = levels.trailing_stop_pct if levels.trailing_stop_pct > 0 else 2.5
            trailing_stop_price = position.max_price_after_buy * (1 - ts_pct / 100.0)
            if position.current_price <= trailing_stop_price:
                return self._build_exit_result(
                    stock_code,
                    True,
                    ExitActionType.TRAILING_STOP,
                    1.0,
                    reason=f"TRAILING_STOP ({ts_pct}%)",
                    order_type="00",
                )

        be_activation = (
            self.be_act_s
            if persona == TradePersona.BULL_AGGRESSIVE or position.grade in ["S", "A"]
            else self.be_act_norm
        )
        be_protect = (
            self.be_pro_s
            if persona == TradePersona.BULL_AGGRESSIVE or position.grade in ["S", "A"]
            else self.be_pro_norm
        )

        if position.max_price_after_buy >= position.purchase_price * be_activation:
            if position.current_price <= position.purchase_price * be_protect:
                return self._build_exit_result(
                    stock_code,
                    True,
                    ExitActionType.TRAILING_STOP,
                    1.0,
                    reason="BREAK_EVEN_PROTECT",
                    order_type="00",
                )

        return self._build_exit_result(stock_code, False, ExitActionType.HOLD, 0.0)

    def _build_exit_result(
        self,
        stock_code: str,
        is_sell: bool,
        action: ExitActionType,
        ratio: float,
        reason: str = None,
        order_type: str = "00",
        target_price: float = 0.0,
    ) -> ExitDecisionDTO:
        display_reason = reason or action.name
        if is_sell:
            import time

            self._last_signal_time[stock_code] = time.time()
            logger.info(
                f"📤 [Exit Brain] {stock_code} 매도 승인 | 사유: {display_reason} | 비율: {ratio * 100}% | 타입: {order_type}"
            )
        return ExitDecisionDTO(
            stock_code=stock_code,
            final_can_sell=is_sell,
            action_type=action.name,
            order_type=order_type,
            target_price=target_price,
            reason=display_reason,
            sell_ratio=ratio,
        )


import pandas as pd
import numpy as np
import datetime


def calculate_trailing_stop_pct(
    min_data: pd.DataFrame,
    base_trailing_stop_pct: float = 2.0,
    atr_period: int = 14,
    volume_ratio: float = 1.0,
    price_momentum: float = 0.0,
    market_condition: str = "NEUTRAL",
    intraday_volatility: float = 0.0,
    trade_time_minutes: int = 0,
) -> float:
    try:
        from strategy.indicators.technical_indicators import calculate_atr

        trailing_stop_pct = base_trailing_stop_pct

        if min_data is not None and len(min_data) > atr_period:
            try:
                current_atr = calculate_atr(min_data, window=atr_period)
                current_close = min_data["종가"].iloc[-1]

                if current_close > 0:
                    atr_pct = (current_atr / current_close) * 100
                    dynamic_stop = atr_pct * 2.0
                    trailing_stop_pct = (base_trailing_stop_pct * 0.4) + (
                        dynamic_stop * 0.6
                    )
            except Exception:
                pass

        if market_condition == "BULLISH":
            trailing_stop_pct *= 1.1
        elif market_condition == "BEARISH":
            trailing_stop_pct *= 0.85
        elif market_condition == "VOLATILE":
            trailing_stop_pct *= 1.2
        elif market_condition == "CRASH":
            trailing_stop_pct *= 1.5
            trailing_stop_pct = min(trailing_stop_pct, 5.0)

        if volume_ratio > 2.0:
            trailing_stop_pct *= 1.1
        elif volume_ratio < 0.5:
            trailing_stop_pct *= 0.9

        if price_momentum > 0.5:
            trailing_stop_pct *= 1.1
        elif price_momentum < -0.5:
            trailing_stop_pct *= 0.85

        if trade_time_minutes > 120:
            trailing_stop_pct *= 0.9
        elif trade_time_minutes < 30:
            trailing_stop_pct *= 1.1

        trailing_stop_pct = max(1.0, min(5.0, trailing_stop_pct))
        return round(trailing_stop_pct, 2)
    except Exception as e:
        logger.error(f"[TrailingStop Error] {e}")
        return round(base_trailing_stop_pct, 2)


def signal_based_exit(
    combined_now: float,
    combined_peak: float,
    atr_percent: float,
) -> Dict[str, Any]:
    try:
        result = {"should_exit": False, "reason": "", "confidence": 0.0}
        if combined_peak > 0 and combined_now < combined_peak * 0.5:
            result["should_exit"] = True
            result["reason"] = "점수 급락"
            result["confidence"] = 0.8
            return result
        if atr_percent > 3.0:
            result["should_exit"] = True
            result["reason"] = "고변동성"
            result["confidence"] = 0.6
            return result
        return result
    except Exception:
        return {"should_exit": False, "reason": "", "confidence": 0.0}


def calculate_dynamic_chandelier_stop(
    current_high: float,
    atr: float,
    beta: float,
    grade: str,
    market_regime: str = "NEUTRAL",
    current_time: datetime.time = None,
    profit_rate: float = 0.0,
    params: Dict[str, Any] = None,
) -> float:
    try:
        if params is None:
            params = {}
        if grade in ["S", "A"]:
            base_k = params.get(f"sl_factor_{grade.lower()}", 3.5)
        elif grade == "C":
            base_k = 2.5
        else:
            base_k = 3.0

        beta_factor = max(0.8, min(1.5, beta))
        final_k = base_k * beta_factor

        regime_multiplier = 1.0
        if market_regime == "BULL":
            regime_multiplier = 1.2
        elif market_regime == "CRASH":
            regime_multiplier = 0.6

        is_morning_zone = False
        if current_time:
            if datetime.time(9, 0, 0) <= current_time <= datetime.time(9, 20, 0):
                is_morning_zone = True

        if (is_morning_zone and profit_rate < 0.03) or grade == "S":
            if market_regime == "CRASH":
                regime_multiplier = 1.0
            final_k = max(2.5, final_k * regime_multiplier)
        else:
            final_k *= regime_multiplier

        if profit_rate >= 0.03:
            final_k = max(2.0, 1.5 * beta_factor)

        stop_price = current_high - (atr * final_k)
        return float(round(stop_price, 2))
    except Exception:
        return current_high * 0.95


def calculate_hybrid_stop_loss(
    current_price, entry_price, current_high, atr, stop_loss_v2, params=None
):
    if params is None:
        params = {}
    chandelier_stop = current_high - (atr * 3.0)
    hard_sl_pct = abs(params.get("hard_stop_loss_pct", -3.0)) / 100.0
    hard_sl = entry_price * (1.0 - hard_sl_pct) if entry_price > 0 else 0
    final_stop_loss = max(stop_loss_v2, chandelier_stop, hard_sl)
    return round(final_stop_loss, 2)


def calculate_hybrid_exit_levels(
    ticker_results: Dict[str, Any],
    daily_df: pd.DataFrame,
    minute_df: pd.DataFrame,
    grade: str,
    params: Dict[str, Any] = None,
) -> Dict[str, Any]:
    try:
        if params is None:
            params = {}
        current_price = minute_df["종가"].iloc[-1]
        daily_atr = daily_df.iloc[-1].get("ATR", current_price * 0.03)
        intra_std = (
            float(minute_df["종가"].tail(30).std()) if len(minute_df) > 1 else 0.0
        )
        if pd.isna(intra_std):
            intra_std = 0.0

        capped_intra_vol = min(intra_std * 2.0, daily_atr * 0.5)
        hybrid_vol = (daily_atr * 0.8) + (capped_intra_vol * 0.2)
        sl_factor = min(params.get(f"sl_factor_{grade.lower()}", 2.0), 3.0)
        raw_tp_factor = params.get(f"tp_factor_{grade.lower()}", 3.0)
        tp_factor = max(sl_factor * 1.5, min(raw_tp_factor, 5.0))

        is_high_vol_warning = bool(intra_std > (daily_atr / 10))
        if is_high_vol_warning:
            sl_factor = min(sl_factor, 1.5)

        stop_loss = current_price - (hybrid_vol * sl_factor)
        take_profit1 = current_price + (hybrid_vol * tp_factor)
        take_profit2 = take_profit1 + (hybrid_vol * sl_factor)

        return {
            "hybrid_vol": round(float(hybrid_vol), 2),
            "stop_loss": round(float(stop_loss), 2),
            "take_profit1": round(float(take_profit1), 2),
            "take_profit2": round(float(take_profit2), 2),
            "is_high_vol_warning": is_high_vol_warning,
        }
    except Exception:
        return {}


def optimize_exit_strategy(
    ticker,
    current_price,
    intra_acc,
    hybrid_exit_levels,
    grade,
    params: Dict[str, Any] = None,
):
    if params is None:
        params = {}
    try:
        stop_loss = hybrid_exit_levels.get("stop_loss", 0.0)
        acc_threshold = 67.0 if grade == "S" else 60.0
        adaptive_exit_signal = False
        reason = "유지"
        if intra_acc < acc_threshold:
            if current_price > (stop_loss * 1.02):
                adaptive_exit_signal = True
                reason = f"가속도 저하({intra_acc:.2f}): 선제적 수익 보전"

        base_risk = abs(params.get("hard_stop_loss_pct", -3.5)) / 100.0
        max_risk_map = {"S": base_risk, "A": base_risk + 0.01, "B": base_risk + 0.02}
        allowed_risk = max_risk_map.get(grade, base_risk + 0.015)
        tight_stop_loss = current_price * (1 - allowed_risk)
        final_stop_loss = max(stop_loss, tight_stop_loss)

        return {
            "final_stop_loss": round(final_stop_loss, 2),
            "adaptive_exit_signal": adaptive_exit_signal,
            "exit_reason": reason,
            "risk_exposure": round((1 - final_stop_loss / current_price) * 100, 2),
        }
    except Exception as e:
        logger.error(f"[{ticker}] optimize_exit_strategy Error: {e}")
        return {}


def optimize_trade_levels(
    paramDic, levels, grade, volatility_profile=None, params=None
):
    if params is None:
        params = {}
    try:
        entry_price = getattr(paramDic, "entry_price", 0)
        tp1 = getattr(levels, "take_profit1", 0)
        tp2 = getattr(levels, "take_profit2", 0)
        sl = getattr(levels, "stop_loss", 0)
        is_high_vol = getattr(levels, "is_high_vol_warning", False)

        max_intraday_cap_fallback = {"S": 0.12, "A": 0.08, "B": 0.05}.get(grade, 0.06)
        max_intraday_cap = params.get(
            f"max_intraday_cap_{grade.lower()}", max_intraday_cap_fallback
        )
        atr_5m = getattr(paramDic, "atr_5m", entry_price * 0.015)
        min_profit_margin = 0.018
        current_tp1_pct = (tp1 - entry_price) / entry_price if entry_price > 0 else 0

        if is_high_vol:
            min_guaranteed_profit_fallback = {"S": 0.12, "A": 0.10, "B": 0.08}.get(
                grade, 0.08
            )
            min_guaranteed_profit = params.get(
                f"min_guaranteed_profit_{grade.lower()}", min_guaranteed_profit_fallback
            )
            relaxed_tp1_pct = max(min_guaranteed_profit, current_tp1_pct * 0.7)
            optimized_tp1 = entry_price * (1 + relaxed_tp1_pct)
        elif current_tp1_pct > max_intraday_cap:
            optimized_tp1 = entry_price * (1 + max_intraday_cap)
        elif current_tp1_pct < min_profit_margin:
            optimized_tp1 = entry_price * (1 + min_profit_margin)
        else:
            optimized_tp1 = tp1

        optimized_tp2 = max(tp2, optimized_tp1 * 1.03)
        if optimized_tp2 < optimized_tp1 * 1.02:
            optimized_tp2 = optimized_tp1 * 1.03

        expected_profit = optimized_tp1 - entry_price
        base_max_loss = expected_profit / 1.5
        max_allowed_loss = base_max_loss + (entry_price * 0.005 if is_high_vol else 0.0)
        rr_based_sl = entry_price - max_allowed_loss
        hard_limit_sl_pct = abs(params.get("hard_stop_loss_pct", -5.0))
        hard_limit_sl = entry_price * (1 - hard_limit_sl_pct / 100.0)

        if sl < rr_based_sl * 0.99:
            tight_stop = max(rr_based_sl, entry_price - (entry_price * 0.015))
            optimized_sl = max(tight_stop, hard_limit_sl)
        else:
            optimized_sl = max(sl, rr_based_sl, hard_limit_sl)

        atr_multiplier = 2.5 if is_high_vol else 2.0
        dynamic_stop_dist = atr_5m * atr_multiplier
        if optimized_sl >= entry_price:
            optimized_sl = entry_price - dynamic_stop_dist

        levels.update(
            {
                "stop_loss": round(optimized_sl, 2),
                "take_profit1": round(optimized_tp1, 2),
                "take_profit2": round(optimized_tp2, 2),
                "original_tp1_pct": round(current_tp1_pct * 100, 2),
                "optimized_tp1_pct": round(
                    ((optimized_tp1 - entry_price) / entry_price) * 100, 2
                )
                if entry_price > 0
                else 0,
                "vol_adjusted_sl": is_high_vol,
            }
        )
        return levels
    except Exception as e:
        logger.error(f"Level Optimization Error: {e}")
        return levels
