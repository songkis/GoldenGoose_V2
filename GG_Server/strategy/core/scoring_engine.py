from typing import Dict, Any, Optional
import pandas as pd
import numpy as np
from core.schemas import (
    StockEvaluation,
    StockEvaluationResult,
    EntryDecisionDTO,
    ExecutionTriggerResult,
)
from util.Utils import sanitize_dict, safe_get

logger = None


def set_logger(external_logger):
    global logger
    logger = external_logger


def apply_momentum_premium(
    combined_score: float,
    tick_acc: float,
    supply_intra: float,
    daily_score: float,
    surge_rate: float = 0.0,
    vwap_dist: float = 0.0,
    bb_dist: float = 1.0,
    atr_threshold: float = 2.0,
    params: Any = None,
) -> tuple:
    """
    [Early-Bird Entry Optimization]
    1. 과열 캡(Cap) 적용: FOMO 방지
    2. Sweet Spot 프리미엄: 상승 초입 수급 가점
    """
    new_score = combined_score

    # 1. 🛡️ [FOMO-Guard] ATR 기반 동적 페널티
    penalty = calculate_fomo_penalty(surge_rate, atr_threshold)
    if penalty > 0:
        new_score -= penalty
        if logger:
            logger.warning(
                f"🚫 [FOMO-Guard] 과열 감지(Surge:{surge_rate:.1f}%). 페널티 -{penalty} 부여."
            )

    # 2. 🚀 [Sweet Spot Premium]
    is_sweet_spot = (
        (tick_acc >= 20.0)
        and (0.0 <= surge_rate <= 5.0)
        and (vwap_dist <= 2.0)
        and (bb_dist < 1.0)
    )

    if is_sweet_spot and supply_intra >= 80.0 and daily_score >= 10.0:
        premium = 30.0
        new_score = min(100.0, new_score + premium)
        if logger:
            logger.info(
                f"✨ [Sweet Spot Premium] 상승 초입 수급 포착! (+{premium}점) -> {new_score:.1f}"
            )

    # Thresholds (Source from Optimization Params)
    # Using float() for safety during numeric comparison
    s_t = float(params.get("s_threshold_normal", 85.0)) if params else 85.0
    a_t = float(params.get("a_threshold_normal", 55.0)) if params else 55.0
    b_t = float(params.get("b_threshold_floor", 45.0)) if params else 45.0

    if new_score >= s_t:
        new_grade = "S"
    elif new_score >= a_t:
        new_grade = "A"
    elif new_score >= b_t:
        new_grade = "B"
    else:
        new_grade = "C"

    return float(new_score), str(new_grade)


def compute_intraday_scores(
    minute_df: pd.DataFrame,
    context_from_daily: Optional[StockEvaluation] = None,
    params: Any = None,
    day_open: float = None,
    day_high: float = None,
    day_low: float = None,
    atr_pct: float = 2.0,
) -> Dict[str, Any]:
    """
    분봉 기반 타이밍/확증 스코어 산출 함수 (Dataclass Aligned)
    """
    if minute_df is None or minute_df.empty:
        return {
            "score": 0.0,
            "confidence": 0.0,
            "regime": "unknown",
            "signals": {},
            "vwap_distance": 0.0,
            "vol_surge_ratio": 1.0,
            "pullback_quality": 0.0,
        }

    # [TLVI Harness] Initialize context variables directly from Dataclass
    current_state = context_from_daily
    if current_state:
        market_regime = current_state.market_regime
        supply_s = float(current_state.supply_s)
    else:
        market_regime = "NEUTRAL"
        supply_s = 0.0

    def pick(col_candidates):
        for c in col_candidates:
            if c in minute_df.columns:
                return c
        return None

    try:
        price_col = pick(["체결가", "종가", "close", "Close"])
        high_col = pick(["고가", "high", "High"]) or price_col
        low_col = pick(["저가", "low", "Low"]) or price_col
        vol_col = pick(["거래량", "volume", "Volume"])
        buy_vol_col = pick(["매수체결량", "buy_volume", "buy_vol"])
        sell_vol_col = pick(["매도체결량", "sell_volume", "sell_vol"])

        if price_col is None:
            raise ValueError("Essential price columns missing")

        price = minute_df[price_col].astype(float)
        highv = minute_df[high_col].astype(float)
        lowv = minute_df[low_col].astype(float)
        vol = (
            minute_df[vol_col].astype(float)
            if vol_col
            else pd.Series(np.ones(len(minute_df)))
        )

        open_col = pick(["시가", "open", "Open"])
        if open_col and open_col in minute_df.columns:
            open_val = float(minute_df[open_col].iloc[-1])
        else:
            open_val = (
                float(price.iloc[-2]) if len(price) > 1 else float(price.iloc[-1])
            )

        body = abs(price.iloc[-1] - open_val)
        safe_body = max(body, price.iloc[-1] * 0.001)
        noise_ratio = (highv.iloc[-1] - lowv.iloc[-1]) / safe_body
        if ((highv.iloc[-1] - lowv.iloc[-1]) / (price.iloc[-1] + 1e-9)) < 0.015:
            noise_ratio = 1.0

        typical = (highv + lowv + price) / 3.0
        cum_vol = vol.cumsum().replace(0, np.nan)
        vwap = (typical * vol).cumsum() / cum_vol
        vwap_curr = vwap.iloc[-1] if not vwap.empty else price.iloc[-1]

        vol_5 = vol.rolling(5, min_periods=1).sum()
        vol_20_ewm = vol.ewm(span=20, adjust=False).mean()
        vol_surge_ratio = float((vol_5.iloc[-1] / (vol_20_ewm.iloc[-1] + 1e-9)))

        if day_open is None:
            day_open = (
                float(minute_df.iloc[0][open_col or price_col])
                if open_col or price_col in minute_df.columns
                else float(price.iloc[0])
            )

        is_yang_candle = bool(price.iloc[-1] >= day_open)

        ofi_val = 0.0
        if buy_vol_col and sell_vol_col:
            buy_v = minute_df[buy_vol_col].astype(float)
            sell_v = minute_df[sell_vol_col].astype(float)
            net_vol = buy_v - sell_v
            ofi_val = float(net_vol.diff().iloc[-1]) if len(net_vol) > 1 else 0.0

        lbk = int(params.breakout_lookback) if params else 60
        ref_high = (
            float(highv.iloc[-lbk:-1].max())
            if len(minute_df) > lbk
            else float(highv.iloc[:-1].max())
        )
        breakout = bool(price.iloc[-1] > ref_high)

        vwap_above = bool(price.iloc[-1] >= vwap_curr)
        vwap_distance = float((price.iloc[-1] - vwap_curr) / (vwap_curr + 1e-9) * 100.0)

        plb = int(params.pullback_lookback) if params else 40
        recent_high = float(highv.iloc[-plb:].max())
        recent_low = float(lowv.iloc[-plb:].min())
        swing_range = max(1e-9, recent_high - recent_low)
        pullback_ratio = (
            float((recent_high - price.iloc[-1]) / swing_range)
            if price.iloc[-1] < recent_high
            else 0.0
        )

        target_min = params.pullback_min if params else 0.3
        target_max = params.pullback_max if params else 0.65
        if pullback_ratio <= 0:
            pb_quality = 0.0
        elif pullback_ratio < target_min:
            pb_quality = pullback_ratio / target_min
        elif pullback_ratio > target_max:
            pb_quality = max(
                0.0, 1.0 - (pullback_ratio - target_max) / (1.0 - target_max)
            )
        else:
            pb_quality = 1.0

        base = 0.0
        base += 35.0 if vwap_above else 0.0
        base += 10.0 if breakout else 0.0

        net_multiplier = 1.0
        net_ratio_thresh = (
            params.net_ratio_distribution_limit
            if hasattr(params, "net_ratio_distribution_limit")
            else 0.4
        )
        if buy_vol_col and sell_vol_col:
            buy_v_last = float(minute_df[buy_vol_col].iloc[-1])
            sell_v_last = float(minute_df[sell_vol_col].iloc[-1])
            total_v_last = buy_v_last + sell_v_last
            if total_v_last > 100:
                net_ratio = buy_v_last / total_v_last
                if net_ratio < net_ratio_thresh:
                    net_multiplier = 0.5
                elif net_ratio > 0.6:
                    net_multiplier = 1.1

        base *= net_multiplier
        if vol_surge_ratio >= (params.vol_spike_high if params else 3.0):
            base += 25.0
        elif vol_surge_ratio >= (params.vol_spike_med if params else 1.8):
            base += 12.0

        # Direct attribute access for trigger data
        tick_acc_val = current_state.tick_acc if current_state else 0.0

        div_score = 0.0
        if (
            pb_quality >= 0.6
            and vwap_distance >= -1.5
            and tick_acc_val >= 15.0
            and is_yang_candle
        ):
            div_score = 40.0

        if vwap_distance < -2.0 and tick_acc_val < 10.0:
            base -= 100.0

        ma20_val = price.ewm(span=20, adjust=False).mean().iloc[-1]
        atr_val_scaling = ma20_val * (atr_pct / 100.0)
        bb_upper = ma20_val + (2.0 * atr_val_scaling)
        bb_dist_val = price.iloc[-1] / (bb_upper + 1e-9)

        base += div_score
        base += 20.0 * float(pb_quality)

        is_leader_or_bounce = (
            current_state.intrinsic_grade if current_state else "C"
        ) in ["S", "A"] or (current_state.is_true_bounce if current_state else False)
        noise_limit = 5.0 if is_leader_or_bounce else 2.5
        if noise_ratio > noise_limit:
            base = -500.0

        if vol_surge_ratio > 3.0 and vwap_distance > 5.0 and not is_yang_candle:
            base = -500.0

        if vwap_distance >= 3.0:
            vol_mitigation = (
                0.0
                if vol_surge_ratio >= 2.5
                else np.clip((2.5 - vol_surge_ratio) / 1.0, 0.0, 1.0)
            )
            penalty_factor = float(np.tanh((vwap_distance - 3.0) * 0.5))
            base -= 200.0 * penalty_factor * vol_mitigation

        if market_regime == "BULL" and supply_s >= 60.0:
            base += 30.0

        intraday_drop_pct = (
            (recent_high - price.iloc[-1]) / (recent_high + 1e-9)
        ) * 100.0
        is_true_bounce_val = bool(
            pb_quality >= 0.5
            and vol_surge_ratio >= 2.0
            and is_yang_candle
            and vwap_above
            and intraday_drop_pct < 6.0
        )

        rs_gap_val = current_state.rs_gap if current_state else 0.0
        rs_multiplier = 1.0
        if market_regime == "CRASH":
            if rs_gap_val < (params.rs_gap_crash_annihilate if params else 0.0):
                rs_multiplier = 0.1
            elif rs_gap_val > (params.rs_gap_crash_accelerate if params else 3.0):
                rs_multiplier = 1.2
        base *= rs_multiplier

        return {
            "score": round(base, 2) if base > -400 else 0.0,
            "confidence": round(base / 100.0, 2) if base > -400 else 0.0,
            "regime": "momentum",
            "signals": {
                "breakout": breakout,
                "vwap_above": vwap_above,
                "is_vwap_pullback": abs(vwap_distance) <= 1.5
                and pb_quality >= 0.7
                and vol_surge_ratio < 1.0,
                "is_true_bounce": is_true_bounce_val,
            },
            "vwap_distance": round(vwap_distance, 4),
            "vol_surge_ratio": round(vol_surge_ratio, 3),
            "pullback_quality": round(pb_quality, 3),
            "is_true_bounce": is_true_bounce_val,
            "bb_dist_upper": round(bb_dist_val, 4),
            "ofi_val": float(ofi_val),
            "vwap_dist": float(vwap_distance),
            "recent_low": float(recent_low),
            "noise_ratio": round(noise_ratio, 4),
            "avg_volume_5": round(float(vol_5.iloc[-1] / 5.0), 2),
            "support_levels": {
                "open": float(day_open),
                "vwap": float(vwap_curr),
                "ma20": float(ma20_val),
                "bb_upper": float(bb_upper),
            },
        }
    except Exception as e:
        if logger:
            logger.error(f"Error in compute_intraday_scores: {e}")
        return {
            "score": 0.0,
            "confidence": 0.0,
            "regime": "unknown",
            "signals": {},
            "vwap_distance": 0.0,
            "vol_surge_ratio": 1.0,
            "pullback_quality": 0.0,
        }


def calculate_fomo_penalty(surge_rate: float, atr_threshold: float = 2.0) -> float:
    if surge_rate <= atr_threshold:
        return 0.0
    penalty = 50.0 * np.clip(
        np.exp((surge_rate - atr_threshold) / (atr_threshold + 1e-9)) - 1, 0, 1
    )
    return float(penalty)


def apply_dynamic_scalar(atr_pct: float, tick_acc: float) -> float:
    try:
        if tick_acc >= 10.0:
            return 1.0
        norm_atr = np.clip((atr_pct - 1.0) / 4.0, 0.0, 1.0)
        norm_tick = np.clip((tick_acc - 1.0) / 2.0, 0.0, 1.0)
        blended = (norm_atr * 0.8) + (norm_tick * 0.2)
        scalar = 0.5 + (0.5 * blended)
        return float(round(scalar, 4))
    except Exception:
        return 1.0


def calculate_refined_supply_score_v6(minute_df: pd.DataFrame) -> float:
    try:
        if minute_df is None or minute_df.empty:
            return 0.0
        recent = minute_df.tail(10)
        vol_col = "체결량" if "체결량" in recent.columns else "volume"
        strength_col = "체결강도" if "체결강도" in recent.columns else "strength"
        net_buy_col = (
            "순매수체결량" if "순매수체결량" in recent.columns else "net_buy_vol"
        )
        net_cnt_col = (
            "순체결건수" if "순체결건수" in recent.columns else "net_trade_cnt"
        )

        vol_acc = recent[vol_col].iloc[-3:].mean() / (recent[vol_col].mean() + 1e-9)
        avg_strength = (
            recent[strength_col].mean() if strength_col in recent.columns else 100.0
        )
        strength_score = np.clip((avg_strength - 100) / 0.5, 0, 40)

        total_net_buy = (
            recent[net_buy_col].sum() if net_buy_col in recent.columns else 0.0
        )
        total_volume = recent[vol_col].sum()
        net_buy_ratio = total_net_buy / (total_volume + 1e-9)
        concentration_score = np.clip(net_buy_ratio * 150, 0, 30)

        avg_net_vol_per_cnt = (
            total_net_buy / (recent[net_cnt_col].sum() + 1)
            if net_cnt_col in recent.columns
            else 0.0
        )
        smart_money_score = (
            10 if avg_net_vol_per_cnt > recent[vol_col].median() * 0.5 else 0
        )

        final_supply_intra = (
            (vol_acc * 20) + strength_score + concentration_score + smart_money_score
        )
        return round(float(np.clip(final_supply_intra, 0, 100)), 2)
    except Exception as e:
        if logger:
            logger.error(f"Error in supply_score_v6: {e}")
        return 0.0


def calculate_pivot_from_high_penalty(
    minute_df: pd.DataFrame, current_index_change: float = 0.0
) -> float:
    try:
        if minute_df is None or len(minute_df) < 2:
            return 0.0
        day_high = (
            minute_df["고가"].max()
            if "고가" in minute_df.columns
            else minute_df["high"].max()
        )
        day_low = (
            minute_df["저가"].min()
            if "저가" in minute_df.columns
            else minute_df["low"].min()
        )
        curr_price = (
            minute_df.iloc[-1]["종가"]
            if "종가" in minute_df.columns
            else minute_df.iloc[-1]["close"]
        )
        open_p = (
            minute_df.iloc[0]["시가"]
            if "시가" in minute_df.columns
            else minute_df.iloc[0]["open"]
        )

        if (day_high - day_low) <= 0:
            return 0.0
        threshold, sensitivity = (
            (0.25, 60)
            if current_index_change <= -1.0
            else ((0.50, 40) if current_index_change >= 1.0 else (0.40, 50))
        )
        if open_p > 0 and (day_high - open_p) / open_p < 0.025:
            return 0.0
        pivot_ratio = (day_high - curr_price) / (day_high - day_low)
        if pivot_ratio > threshold:
            return round(min(35.0, (pivot_ratio - threshold) * sensitivity), 2)
        return 0.0
    except Exception:
        return 0.0


def final_stock_evaluation_v7_1(
    ticker: str,
    df: pd.DataFrame,
    market_type: str = "KOSDAQ",
    index_return_5d: float = 0.0,
    is_bull_market: bool = True,
    tp: str = "picking",
    supply_intra: float = 0.0,
    params: Any = None,
    market_regime: str = "NEUTRAL",
    market_score: float = 50.0,
    tick_acc: float = 0.0,
    supply_intra_val: float = 0.0,
    mode: str = "intraday",
    is_true_bounce: bool = False,
    day_open: float = None,
    day_high: float = None,
    day_low: float = None,
) -> StockEvaluationResult:
    if df is not None and not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)

    df_copy = df.copy()
    rolling_high = (
        df_copy["고가"].tail(20).max()
        if "고가" in df_copy.columns
        else df_copy["high"].tail(20).max()
    )
    rolling_low = (
        df_copy["저가"].tail(20).min()
        if "저가" in df_copy.columns
        else df_copy["low"].tail(20).min()
    )
    vcp_ratio = (rolling_high - rolling_low) / (rolling_low + 1e-9)

    latest = df_copy.iloc[-1]
    entry_price = latest["종가"] if "종가" in latest else latest["close"]
    prev_close_5d = (
        df_copy["종가"].iloc[-5] if len(df_copy) >= 5 else df_copy["종가"].iloc[0]
    )
    stock_ret_5d = ((entry_price / prev_close_5d) - 1) * 100
    rs_gap = stock_ret_5d - index_return_5d

    prev_close = df_copy["종가"].iloc[-2] if len(df_copy) >= 2 else entry_price
    surge_rate = ((entry_price / prev_close) - 1) * 100.0

    is_buy = True
    reject_reason = ""
    is_recovering_leader = False

    is_trend_ok = tick_acc >= 15.0 and supply_intra_val >= 60.0
    if not is_trend_ok:
        if mode == "picking" and rs_gap >= 5.0 and supply_intra >= 70.0:
            is_recovering_leader = True
        elif mode == "intraday":
            if (
                (tick_acc >= 20.0 and supply_intra_val >= 80.0)
                or (supply_intra_val >= 90.0 and tick_acc >= 25.0)
                or (is_true_bounce and tick_acc >= 15.0)
            ):
                is_recovering_leader = True
                if surge_rate >= 10.0:
                    is_recovering_leader = False

    if not is_trend_ok and not is_recovering_leader:
        is_buy = False
        reject_reason = "REJECT_GATE_1: 수급 추세 미달"

    if (
        is_buy
        and rs_gap < 0
        and market_regime not in ["CRASH", "BEAR"]
        and not is_recovering_leader
    ):
        is_buy = False
        reject_reason = f"REJECT_GATE_2: RS 미달 (RS Gap: {rs_gap:.1f}%)"

    vcp_thresh = params.vcp_contraction_threshold if params else 0.18
    if is_recovering_leader or is_true_bounce:
        vcp_thresh = max(0.45, vcp_thresh * 2.5)
    if is_buy and vcp_ratio > vcp_thresh:
        is_buy = False
        reject_reason = f"REJECT_GATE_3: VCP 팽창 ({vcp_ratio:.2f} > {vcp_thresh:.2f})"

    amt_series = (
        df_copy["거래대금"]
        if "거래대금" in df_copy.columns
        else df_copy["종가"] * df_copy["거래량"]
    )
    recent_amt = amt_series.tail(5).mean()
    if is_buy and tp == "picking" and recent_amt < 5_000_000_000:
        is_buy = False
        reject_reason = "REJECT_GATE_4: 유동성 미달"

    daily_score = 0.0
    intrinsic_grade = "C"
    if is_buy:
        vol_ma20 = df_copy["거래량"].rolling(20).mean().iloc[-1]
        vol_acc = float(latest["거래량"] / (vol_ma20 + 1e-9))
        if mode == "intraday" and supply_intra_val > 0:
            vol_acc = max(vol_acc, supply_intra_val / 20.0)
        vol_score = min(40.0, vol_acc * 10.0)
        rs_score = min(60.0, max(0.0, rs_gap * 3.0))
        daily_score = vol_score + rs_score

        s_cutoff = params.s_threshold_normal if params else 85.0
        if (supply_intra >= 90.0 and tick_acc >= 25.0) or is_recovering_leader:
            daily_score = max(daily_score, s_cutoff)

        s_t = params.s_threshold_normal if params else 80.0
        a_t = params.a_threshold_normal if params else 65.0
        b_t = params.b_threshold_floor if params else 50.0
        if daily_score >= s_t:
            intrinsic_grade = "S"
        elif daily_score >= a_t:
            intrinsic_grade = "A"
        elif daily_score >= b_t:
            intrinsic_grade = "B"
        else:
            intrinsic_grade = "C"
            is_buy = False
            reject_reason = "REJECT_FINAL: 점수 미달"
        energy_status = f"PASSED ({intrinsic_grade})"
    else:
        energy_status = reject_reason

    final_res = {
        "ticker": ticker,
        "intrinsic_grade": intrinsic_grade,
        "grade": intrinsic_grade,
        "daily_score": round(float(daily_score), 2),
        "score": round(float(daily_score), 2),
        "combined_score": round(float(daily_score), 2),
        "is_buy": is_buy,
        "is_buy_eligible": is_buy,
        "final_can_buy": is_buy,
        "energy_status": energy_status,
        "vcp_ratio": round(float(vcp_ratio), 2),
        "rs_gap": round(float(rs_gap), 2),
        "supply_s": supply_intra,
        "is_recovering_leader": is_recovering_leader,
        "is_true_bounce": is_true_bounce,
        "market_regime": market_regime,
        "surge_rate": round(float(surge_rate), 2),
        "current_price": float(entry_price),
        "reason": energy_status,
    }
    return StockEvaluation(**sanitize_dict(final_res))


def apply_segmented_pardon_gate(
    ticker: str,
    decision_result: EntryDecisionDTO,
    final_stock_eval: StockEvaluation,
    combined_score: float,
    trigger_info: ExecutionTriggerResult,
    params: Any = None,
    logger: Any = None,
) -> tuple:
    """
    [Pardon Engine] Dataclass Aligned (No-Getattr)
    """
    _intrinsic_grade = final_stock_eval.intrinsic_grade
    _score = float(combined_score)
    _supply = float(trigger_info.supply_intra)
    _acc = float(trigger_info.intra_acc)

    is_pardon_candidate = (
        _intrinsic_grade in ["S", "A"] and _supply >= 85.0 and _acc >= 25.0
    )

    if is_pardon_candidate and not decision_result.final_can_buy:
        if logger:
            logger.info(f"🛡️ [Pardon-Engine] {ticker} 주도주 사면권 발동")
        decision_result.final_can_buy = True
        decision_result.decision_state = "APPROVE_PARDON"
        decision_result.reason = (
            f"🛡️ [Pardon] 주도주 강력 수급 사면 (Original: {decision_result.reason})"
        )
        s_cutoff = params.s_threshold_normal if params else 85.0
        if _score < s_cutoff:
            _score = s_cutoff

    return decision_result, _score
