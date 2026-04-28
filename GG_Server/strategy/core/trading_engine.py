# -*- coding: utf-8 -*-
import datetime
import dataclasses
from datetime import timedelta
from typing import Any, Dict, Optional, Literal
import logging
import pandas as pd

from config.ai_settings import (
    MIN_TERM,
    TREND_MINUTE_WIN_CNT,
)
from core.schemas import (
    EntryDecisionDTO,
    ExecutionTriggerResult,
    MarketState,
    StockEvaluation,
    TradingDecisionParam,
    MarketContextData,
    StockEvaluationData,
)
from strategy.indicators.technical_indicators import (
    calculate_intraday_acceleration_v5_6,
)
from strategy.core.scoring_engine import (
    calculate_refined_supply_score_v6,
    final_stock_evaluation_v7_1,
    compute_intraday_scores,
    apply_dynamic_scalar,
    apply_momentum_premium,
)
from strategy.indicators.pattern_recognizer import (
    process_limit_up_from_df,
)
from strategy.core.trigger_engine import (
    check_execution_trigger_v10_2,
)
from util.Utils import (
    safe_get,
    safe_set,
    sanitize_dict,
    safe_update,
)
from .trading_factory import get_current_optimal_params
from .trading_decision_gate import get_failed_candidate_result, get_final_trading_decision

logger = logging.getLogger(__name__)

def combined_score_for_ticker_v3(
    ticker: str,
    daily_df: pd.DataFrame,
    params: Optional[Dict[str, Any]] = None,
    capital: float = 100000000.0,
    minute_df: Optional[pd.DataFrame] = None,
    intraday_params: Optional[Dict[str, Any]] = None,
    market_avg_acc: float = 0.0,
    tp: str = "intraday",
    today_market_data_df: Optional[pd.DataFrame] = None,
    market_regime_override: Optional[str] = None,
    port_list: list = None,
    global_decision_engine=None,
    master_strategy_evaluator=None
) -> Dict[str, Any]:
    """
    [V6.0: Supply-First Acceleration Model]
    메인 분석 엔진 함수 (원본 로직 100% 유지)
    """
    from util.CommUtils import isOverCloseTime
    from config.ai_settings import (
        PARAMS,
        S_THRESHOLD_NORMAL,
        A_THRESHOLD_NORMAL,
        B_THRESHOLD_FLOOR,
    )

    # 1. [Safety] 결과 변수 초기화
    supply_intra = 0.0
    intraday_score = 0
    final_combined_score = 0.0
    final_can_buy = False
    position_size_ratio = 1.0
    hard_reject_reason = None
    _is_pardon_reject = False
    is_single_order = False
    decision_result = EntryDecisionDTO(
        stock_code=ticker,
        final_can_buy=False,
        decision_state="INIT",
        combined_score=0.0,
        grade="C",
        reason="초기화",
    )
    decision = {}
    market_ctx = None
    stock_eval_ctx = None

    combined_score = 0.0
    is_recovering_leader = False
    atr_5m = 0

    v3_indicators = {
        "final_stock_eval": None,
        "execution_trigger": None,
        "final_trading_decision": None,
        "hybrid_exit_levels": None,
    }
    day_acc_score = 0.0
    current_price = 0

    latest = None
    latest_minute = None
    supply_day = 0.0
    fake_score = 0.0

    vtp = {}
    trigger_info = None
    market_conditions = {}

    try:
        if params is None:
            params = get_current_optimal_params()
            local_defaults = {
                "vcp_contraction_threshold": 0.15,
                "momentum_period": 14,
                "volume_surge_ratio": 2.0,
                "min_adx_for_trend": 20,
                "base_risk_percent": 1.5,
                "atr_stop_mult": 1.5,
            }
            for k, v in local_defaults.items():
                params.setdefault(k, v)

        if not isinstance(daily_df, pd.DataFrame):
            try:
                data = pd.DataFrame(daily_df)
                if data.empty and isinstance(daily_df, dict):
                    data = pd.DataFrame([daily_df])
            except Exception:
                data = pd.DataFrame([daily_df])
        else:
            data = daily_df

        if len(data) < 200:
            logger.warning(f"[{ticker}] Skip: Not enough data (len={len(data)})")
            return get_failed_candidate_result(ticker, "데이터 일수 부족 (len < 200)")

        if tp == "intraday" and (
            minute_df is None
            or len(minute_df) < int(round((TREND_MINUTE_WIN_CNT * 0.35), 2))
        ):
            logger.warning(
                f"[{ticker}] Skip: Not enough intraday data ({len(minute_df) if minute_df is not None else 0})"
            )
            return get_failed_candidate_result(
                ticker,
                f"분봉 데이터 부족 ({len(minute_df) if minute_df is not None else 0})",
            )

        if not isOverCloseTime():
            if tp == "intraday":
                last_idx_str = str(data.index[-1])
                last_col_str = (
                    str(data["날짜"].iloc[-1]) if "날짜" in data.columns else ""
                )
                today_str1 = datetime.datetime.now().strftime("%Y%m%d")
                today_str2 = datetime.datetime.now().strftime("%Y-%m-%d")

                if (
                    today_str1 in last_idx_str
                    or today_str2 in last_idx_str
                    or today_str1 in last_col_str
                    or today_str2 in last_col_str
                ):
                    data = data.iloc[:-1]
            else:
                if data.iloc[-1]["거래량"] == 0 or pd.isna(data.iloc[-1]["종가"]):
                    data = data.iloc[:-1]

        latest = data.iloc[-1].to_dict()
        current_price = latest.get("종가")
        if tp == "intraday" and (minute_df is not None and not minute_df.empty):
            latest_minute = minute_df.iloc[-1].to_dict()
            if latest_minute.get("종가"):
                current_price = latest_minute.get("종가")

        from strategy.indicators.market_analysis import analyze_market_conditions

        market_conditions = analyze_market_conditions(
            today_market_data_df
            if today_market_data_df is not None
            else latest.get("시장구분", 1)
        )
        if market_regime_override:
            market_conditions.market_regime = market_regime_override

        if (
            not hasattr(market_conditions, "current_index_change")
            or market_conditions.current_index_change is None
        ):
            market_conditions.current_index_change = 0.0

        market_conditions.system_params = params
        market_type = market_conditions.market_type
        buy_condition = market_conditions.buy_condition
        current_index_change = market_conditions.current_index_change
        market_data = market_conditions.market_data
        index_return_5d = market_conditions.index_return_5d
        market_regime = (
            market_regime_override
            if market_regime_override
            else market_conditions.regime
        )

        is_true_bounce = False
        if tp == "intraday" and (minute_df is not None and not minute_df.empty):
            try:
                current_price_v = float(minute_df["종가"].iloc[-1])
                day_open_v = float(minute_df["시가"].iloc[0])
                is_yang_candle_v = bool(current_price_v >= day_open_v)
                supply_intra_v = calculate_refined_supply_score_v6(minute_df)
                is_strong_bounce = supply_intra_v >= 75.0 and is_yang_candle_v
                is_true_bounce = is_strong_bounce
            except Exception:
                pass
        market_score = market_conditions.market_score
        is_bull_market = buy_condition
        supply_day = float(latest.get("체결강도", 100.0))

        tick_acc = 0.0
        supply_intra_val = 0.0

        if tp == "intraday" and (minute_df is not None and not minute_df.empty):
            limit_up_data = process_limit_up_from_df(minute_df)
            try:
                m_term = params.get("min_term", 3) if params else 3
                tick_acc = calculate_intraday_acceleration_v5_6(minute_df, m_term)
                supply_intra_val = calculate_refined_supply_score_v6(minute_df)
            except Exception:
                pass

        day_open = day_high = day_low = None
        atr_pct = 2.0

        if tp == "intraday" and (minute_df is not None and not minute_df.empty):
            try:
                _m_df = minute_df
                day_open = float(_m_df["시가"].iloc[0])
                day_high = float(_m_df["고가"].max())
                day_low = float(_m_df["저가"].min())
            except Exception:
                pass

        try:
            if data is not None and len(data) >= 14:
                atr_14 = (data["고가"] - data["저가"]).rolling(14).mean().iloc[-1]
                curr_price_base = current_price if current_price > 0 else 1.0
                atr_pct = (atr_14 / curr_price_base) * 100.0
        except Exception:
            atr_pct = 2.0

        trigger_info = ExecutionTriggerResult(msg="EOD Mode")
        final_stock_eval = final_stock_evaluation_v7_1(
            ticker,
            data,
            market_type=market_type,
            index_return_5d=index_return_5d,
            is_bull_market=is_bull_market,
            tp=tp,
            supply_intra=supply_day,
            market_regime=market_regime_override or market_regime,
            market_score=market_score,
            params=params,
            tick_acc=tick_acc,
            supply_intra_val=supply_intra_val,
            mode=tp,
            is_true_bounce=is_true_bounce,
            day_open=day_open,
            day_high=day_high,
            day_low=day_low,
        )

        safe_set(final_stock_eval, "market_regime", market_regime_override or market_regime)
        safe_set(final_stock_eval, "is_true_bounce", is_true_bounce)

        if is_true_bounce:
            safe_set(final_stock_eval, "is_true_bounce", True)
            safe_set(final_stock_eval, "is_buy", True)

        is_recovering_leader = final_stock_eval.is_recovering_leader
        if market_regime_override:
            final_stock_eval.market_regime = market_regime_override

        daily_score = final_stock_eval.daily_score

        v3_indicators = {}
        position_size_ratio = final_stock_eval.position_size_ratio
        day_acc_score = final_stock_eval.day_acc_score
        gap_result = None

        if tp == "intraday" and minute_df is not None:
            if not isinstance(minute_df, pd.DataFrame):
                try:
                    minute_df = pd.DataFrame(minute_df)
                    if minute_df.empty and isinstance(minute_df, dict):
                        minute_df = pd.DataFrame([minute_df])
                except Exception:
                    minute_df = pd.DataFrame([minute_df])

            if minute_df.empty:
                return get_failed_candidate_result(ticker, "분봉 데이터 비어있음")

            _min_bars_interval = max(1, int(MIN_TERM) if MIN_TERM else 3)
            min_bars_today = max(8, int(45 / _min_bars_interval))
            if hasattr(minute_df.index, "date"):
                today_bars = minute_df[
                    minute_df.index.date == minute_df.index[-1].date()
                ]
            else:
                today_bars = minute_df
            if len(minute_df) < min_bars_today:
                logger.info(
                    f"[{ticker}] Not enough bars ({len(minute_df)}/{min_bars_today}). Skipping Score."
                )
                return get_failed_candidate_result(
                    ticker, f"당일 봉 수 부족 ({len(minute_df)}/{min_bars_today})"
                )

            intraday_v2 = compute_intraday_scores(
                minute_df=minute_df,
                context_from_daily=final_stock_eval,
                params=params,
                day_open=day_open,
                day_high=day_high,
                day_low=day_low,
                atr_pct=atr_pct,
            )
            pb_q = intraday_v2.get("pullback_quality", 0.0)
            vol_s = intraday_v2.get("vol_surge_ratio", 1.0)
            is_true_bounce = is_true_bounce or intraday_v2.get("is_true_bounce", False)

            trigger_info = check_execution_trigger_v10_2(
                minute_df,
                final_stock_eval,
                market_avg_acc,
                is_true_bounce=is_true_bounce,
                is_recovering_leader=is_recovering_leader,
                intra_acc_val=tick_acc,
                supply_intra_val=supply_intra_val,
            )
            supply_intra = trigger_info.supply_intra
            fake_score = trigger_info.fake_score
            trigger_info.pullback_quality = pb_q
            trigger_info.vol_surge_ratio = vol_s
            trigger_info.is_true_bounce = is_true_bounce

            final_stock_eval.pullback_quality = pb_q
            final_stock_eval.vol_surge_ratio = vol_s
            final_stock_eval.is_true_bounce = is_true_bounce
            if hasattr(final_stock_eval, "breakout_info") and isinstance(
                final_stock_eval.breakout_info, dict
            ):
                final_stock_eval.breakout_info["bb_dist"] = intraday_v2.get(
                    "bb_dist_upper", 0.0
                )

            is_vwap_pullback = intraday_v2.get("signals", {}).get(
                "is_vwap_pullback", False
            )
            intraday_score = trigger_info.intraday_score
            is_dip_mode = "Healthy Pullback" in final_stock_eval.energy_status
            curr_price_base = current_price if current_price > 0 else 1.0
            m_term = params.get("min_term", 3) if params else 3
            tick_acc = calculate_intraday_acceleration_v5_6(minute_df, m_term)
            vol_scalar = apply_dynamic_scalar(atr_pct, tick_acc)

            if logger:
                logger.info(
                    f"[{ticker}] Vol_Scalar 적용: {vol_scalar:.4f} (ATR%:{atr_pct:.2f}, Tick_Acc:{tick_acc:.2f})"
                )

            daily_score *= vol_scalar
            intraday_score *= vol_scalar
            final_score = (daily_score * 0.5) + (intraday_score * 0.5)
            final_score *= market_conditions.market_energy

            rs_gap_val = final_stock_eval.rs_gap
            final_score, grade = apply_momentum_premium(
                final_score,
                tick_acc,
                supply_intra,
                daily_score,
                surge_rate=rs_gap_val,
                vwap_dist=intraday_v2.get("vwap_dist", 0.0),
                bb_dist=intraday_v2.get("bb_dist_upper", 1.0),
                atr_threshold=atr_pct,
                params=params,
            )

            if (
                -0.5 < intraday_v2.get("vwap_dist", 0.0) < 3.0
                and intraday_v2.get("ofi_val", 0) > 0
            ):
                final_score += 30.0
                is_single_order = True

            combined_score = round(float(final_score), 2)
            if params is None:
                params = {}
            _s_cut = float(params.get("s_threshold_normal", 85.0))
            _a_cut = float(params.get("a_threshold_normal", 55.0))
            _b_cut = float(params.get("b_threshold_floor", 45.0))

            if combined_score >= _s_cut:
                grade = "S"
            elif combined_score >= _a_cut:
                grade = "A"
            elif combined_score >= _b_cut:
                grade = "B"
            else:
                grade = "C"

            is_v_bounce_event = tick_acc >= 20.0 and supply_intra >= 60.0
            fuse_grade = grade
            grade = fuse_grade
            is_s_class = supply_intra >= 80

            try:
                atr_5m = atr_pct
                paramDic = TradingDecisionParam(ticker=ticker)
                paramDic.ticker = ticker
                paramDic.combined_score = combined_score
                paramDic.fuse_grade = fuse_grade
                paramDic.intrinsic_grade = fuse_grade
                paramDic.market_regime = market_regime
                paramDic.momentum_state = getattr(
                    market_conditions, "momentum_state", "NORMAL"
                )
                paramDic.pb_quality = float(
                    trigger_info.pullback_score or trigger_info.pb_quality or 0.0
                )
                paramDic.pullback_quality = paramDic.pb_quality
                paramDic.supply_intra = float(trigger_info.supply_intra)
                paramDic.vol_surge_ratio = float(trigger_info.vol_surge_ratio)
                paramDic.intraday_score = float(trigger_info.intraday_score)
                paramDic.intra_acc = float(
                    trigger_info.tick_acc or trigger_info.intra_acc or 0.0
                )
                paramDic.tick_acc = paramDic.intra_acc
                paramDic.is_true_bounce = is_true_bounce
                ai_prob_val = latest.get("AI_PROB")
                if ai_prob_val is None or pd.isna(ai_prob_val):
                    if "AI_PROB" in data.columns and not pd.isna(
                        data["AI_PROB"].iloc[-1]
                    ):
                        ai_prob_val = float(data["AI_PROB"].iloc[-1])
                    else:
                        ai_prob_val = 0.5

                limit_up_data = final_stock_eval.limit_up_data
                is_limit_up_trade = limit_up_data.get("is_limit_up_entry", False)
                trigger_hit_val = trigger_info.trigger_hit
                trigger_msg = trigger_info.msg

                market_ctx = MarketState(
                    regime=market_regime,
                    index_change=current_index_change,
                    is_panic=(market_regime == "CRASH"),
                    drop_rate=current_index_change,
                )

                _breakout_info = final_stock_eval.breakout_info
                if _breakout_info is None:
                    _breakout_info = {}

                is_breakout = _breakout_info.get("is_breakout", False)
                meets_basic = final_stock_eval.is_buy and trigger_hit_val
                custom_reason = (
                    trigger_msg if not trigger_hit_val else "Ready for AI Evaluation"
                )
                noise_ratio = (
                    float(intraday_v2.get("noise_ratio", 0.5))
                    if "intraday_v2" in locals()
                    else 0.5
                )
                avg_vol_5 = (
                    float(intraday_v2.get("avg_volume_5", 0.0))
                    if "intraday_v2" in locals()
                    else 0.0
                )

                curr_vol_actual = (
                    float(latest_minute.get("거래량", 0.0))
                    if (tp == "intraday" and latest_minute)
                    else float(latest.get("거래량", 0.0))
                )

                stock_eval_ctx = StockEvaluation(
                    ticker=ticker,
                    current_price=current_price,
                    is_breakout=is_breakout,
                    meets_basic_criteria=meets_basic,
                    grade=grade,
                    score=float(combined_score),
                    daily_score=final_stock_eval.daily_score,
                    is_limit_up_trade=is_limit_up_trade,
                    ai_surge_probability=float(ai_prob_val),
                    reason=custom_reason,
                    intrinsic_grade=grade,
                    market_regime=market_regime,
                    is_recovering_leader=is_recovering_leader,
                    is_true_bounce=is_true_bounce,
                    expected_win_rate=0.8,
                    noise_ratio=noise_ratio,
                    avg_volume_5=avg_vol_5,
                    current_volume=curr_vol_actual,
                    supply_intra=float(supply_intra),
                    tick_acc=float(tick_acc),
                    recent_low=0.0,
                    rs_gap=final_stock_eval.rs_gap,
                    bb_dist=float(_breakout_info.get("bb_dist", 0.0)),
                    surge_rate=float(_breakout_info.get("surge_rate", 0.0)),
                    support_levels={},
                    atr_val=float(atr_5m),
                )

                decision_result = global_decision_engine.evaluate_buy_decision(
                    stock_code=ticker,
                    market_ctx=market_ctx,
                    stock_eval=stock_eval_ctx,
                    account_balance=capital,
                )

                paramDic.ai_prob = ai_prob_val
                paramDic.is_blocked = False
                paramDic.fuse_reason = "PASSED"
                paramDic.intra_acc = getattr(trigger_info, "intra_acc", 0.0)
                paramDic.intraday_score = trigger_info.intraday_score
                paramDic.supply_intra = trigger_info.supply_intra
                paramDic.is_bull_market = is_bull_market
                paramDic.rs_gap = final_stock_eval.rs_gap
                paramDic.vcp_ratio = final_stock_eval.vcp_ratio
                paramDic.current_index_change = current_index_change
                paramDic.volume_dry = final_stock_eval.volume_dry
                paramDic.entry_price = current_price
                paramDic.atr_5m = atr_5m
                _breakout_info_pd = final_stock_eval.breakout_info
                paramDic.is_bottom_breakout = _breakout_info_pd.get(
                    "is_breakout", False
                )
                paramDic.breakout_strength = _breakout_info_pd.get(
                    "breakout_strength", 0.0
                )
                paramDic.volume_multiplier = final_stock_eval.volume_multiplier
                paramDic.volume_surge_ratio = paramDic.volume_multiplier
                paramDic.daily_score = final_stock_eval.daily_score
                paramDic.trend_score = final_stock_eval.trend_score
                paramDic.breakout_info = _breakout_info_pd
                paramDic.is_vwap_pullback = is_vwap_pullback

                vcp_pass = final_stock_eval.vcp_ratio <= params.get(
                    "vcp_contraction_threshold", 0.15
                )
                trend_template_pass = "TT:Pass" in final_stock_eval.energy_status
                track_a_swing = vcp_pass and trend_template_pass

                track_b_momentum = False
                if is_bull_market or market_regime in ["BULL", "NORMAL"]:
                    is_breakout = final_stock_eval.breakout_info.get(
                        "is_breakout", False
                    )
                    if trigger_info.intra_acc >= 60.0 and is_breakout:
                        track_b_momentum = True

                paramDic.track_a_swing = track_a_swing
                paramDic.track_b_momentum = track_b_momentum

                dec_reason = decision_result.reason
                dec_approved = decision_result.final_can_buy
                is_pardon_granted = dec_approved or "[Pardon]" in dec_reason
                if is_pardon_granted:
                    combined_score = max(float(combined_score), 85.0)
                    paramDic.combined_score = combined_score
                    _grade_locked = (
                        "S"
                        if combined_score >= 85.0
                        else ("A" if combined_score >= 55.0 else "B")
                    )
                    final_trading_decision = {
                        "final_grade": _grade_locked,
                        "intrinsic_grade": grade,
                        "intrinsic_can_buy": True,
                        "final_can_buy": True,
                        "final_reason": decision_result.reason,
                        "combined_score": float(combined_score),
                        "is_acc_sync": True,
                        "is_blocked_by_market": False,
                        "decision_state": decision_result.decision_state,
                        "position_size_ratio": decision_result.extra_info.get(
                            "position_size_ratio", 1.0
                        ),
                        "extra_info": decision_result.extra_info,
                    }
                else:
                    final_trading_decision = get_final_trading_decision(
                        ticker,
                        daily_df,
                        minute_df,
                        paramDic,
                        trigger_info,
                        tp,
                        params=params,
                        decision_result=decision_result,
                        master_strategy_evaluator=master_strategy_evaluator
                    )

                is_safe_entry, reject_msg = True, "Pass"
                grade = final_trading_decision.get("final_grade", "F")
                combined_score = float(
                    final_trading_decision.get("combined_score", 0.0)
                )
                if logger:
                    logger.info(
                        f"[{ticker}] intraday 📊 FINAL TRACE -> combined_score: {combined_score} | Grade: {grade}"
                    )

                paramDic.final_grade = grade
                from strategy.core.TradeExitEngine import (
                    calculate_hybrid_exit_levels,
                    optimize_trade_levels,
                )
                hybrid_exit_levels = calculate_hybrid_exit_levels(
                    paramDic, daily_df, minute_df, grade, params=params
                )

                v3_indicators.update(
                    {
                        "final_stock_eval": final_stock_eval,
                        "execution_trigger": trigger_info,
                        "final_trading_decision": final_trading_decision,
                        "hybrid_exit_levels": hybrid_exit_levels,
                        "is_vwap_pullback": is_vwap_pullback,
                    }
                )
                decision = v3_indicators.get("final_trading_decision", {})
                if market_regime_override and not isinstance(decision, dict):
                    decision.market_regime = market_regime_override

                optimize_trade_levels(
                    paramDic,
                    v3_indicators.get("hybrid_exit_levels", {}),
                    grade,
                    params=params,
                )
                optimizer_mode = bool(params and params.get("optimizer_mode"))
                combined_score_val = float(decision.get("combined_score", 0.0))
                final_grade = decision.get("final_grade", grade)

                ACTIVE_PARAMS = params if params else PARAMS
                min_score_global = float(ACTIVE_PARAMS.get("min_score", 2.0))
                s_thresh = float(
                    ACTIVE_PARAMS.get("s_threshold_normal", S_THRESHOLD_NORMAL)
                )
                a_thresh = float(
                    ACTIVE_PARAMS.get("a_threshold_normal", A_THRESHOLD_NORMAL)
                )
                b_floor = float(
                    ACTIVE_PARAMS.get("b_threshold_floor", B_THRESHOLD_FLOOR)
                )

                grade_min_required = {"S": s_thresh, "A": a_thresh, "B": b_floor}.get(
                    final_grade, min_score_global
                )
                effective_min_score = max(min_score_global, grade_min_required)

                regime_limits = {"CRASH": 20.0, "NEUTRAL": 30.0, "BULL": 40.0}
                pivot_reject_limit = regime_limits.get(market_regime, 30.0)

                fs_eval = v3_indicators.get("final_stock_eval") or {}
                intrinsic_grade = (
                    fs_eval.intrinsic_grade
                    if not isinstance(fs_eval, dict)
                    else fs_eval.get("intrinsic_grade", "C")
                )
                if intrinsic_grade == "S":
                    pivot_reject_limit += 10.0

                rs_overheat_limit = float(ACTIVE_PARAMS.get("rs_overheat_limit", 18.0))
                volume_surge_threshold_gate = float(
                    ACTIVE_PARAMS.get("volume_surge_threshold", 1.3)
                )

                pivot_penalty_val = 0.0
                rs_gap_val = 0.0
                vol_surge_val = 1.0
                if v3_indicators:
                    pivot_penalty_val = float(v3_indicators.get("pivot_penalty", 0.0))
                    try:
                        rs_gap_val = float(fs_eval.rs_gap)
                    except Exception:
                        pass
                    try:
                        vol_surge_val = float(fs_eval.volume_surge_ratio)
                    except Exception:
                        pass

                final_combined_score = (
                    decision_result.combined_score
                    if decision_result.combined_score > 0
                    else float(combined_score)
                )
                final_grade = decision_result.grade
                hard_reject_reason = decision_result.reason

                if not decision.get("final_can_buy", False) and not is_limit_up_trade:
                    rule_reason = decision.get("final_reason", "Rule Engine Reject")
                    hard_reject_reason = f"⛔ [Rule] {rule_reason}"

                if (
                    hard_reject_reason is None
                    and not trigger_hit_val
                    and not is_limit_up_trade
                ):
                    hard_reject_reason = f"⛔ [Trigger] {trigger_msg}"

                if (
                    hard_reject_reason is None
                    and combined_score_val < effective_min_score
                ):
                    hard_reject_reason = (
                        f"⚠️ 점수 미달 (score={combined_score_val:.2f}, "
                        f"req={effective_min_score:.2f}, grade={final_grade})"
                    )

                if (
                    hard_reject_reason is None
                    and pivot_penalty_val >= pivot_reject_limit
                ):
                    if final_grade in ["B", "C"] or combined_score_val < b_floor:
                        hard_reject_reason = f"🛑 Pivot Penalty High ({pivot_penalty_val:.2f} >= {pivot_reject_limit:.2f})"

                if (
                    hard_reject_reason is None
                    and rs_gap_val >= rs_overheat_limit
                    and vol_surge_val < volume_surge_threshold_gate
                    and final_grade in ["B", "C"]
                ):
                    hard_reject_reason = f"🛑 RS Overheat (RS={rs_gap_val:.1f}%, Vol={vol_surge_val:.2f}x) >= limit"

                vcp_ratio = final_stock_eval.vcp_ratio
                is_v_bounce_pass = False
                _extra_info = decision.get("extra_info", {})
                if _extra_info and isinstance(_extra_info, dict):
                    is_v_bounce_pass = _extra_info.get("is_v_bounce_pass", False)

                vcp_tick_limit = (
                    25.0 if (is_recovering_leader or is_true_bounce) else 60.0
                )

                if (
                    hard_reject_reason is None
                    and not is_v_bounce_pass
                    and vcp_ratio <= 0.15
                    and tick_acc < vcp_tick_limit
                ):
                    hard_reject_reason = f"🛑 가짜 돌파 (VCP Fakeout) 차단. VCP({vcp_ratio:.2f}) 만족하나 체결강도({tick_acc:.1f}) < {vcp_tick_limit} 미달"

                _rule_reason = str(decision_result.reason)
                _is_pardon_context = (
                    "[Pardon]" in _rule_reason
                    or "(BYPASS)" in _rule_reason
                    or (
                        hard_reject_reason is not None
                        and "[Pardon]" in str(hard_reject_reason)
                    )
                )

                if hard_reject_reason is not None and not _is_pardon_context:
                    stock_eval_ctx.meets_basic_criteria = False
                    stock_eval_ctx.reason = hard_reject_reason

                if _is_pardon_context:
                    stock_eval_ctx.meets_basic_criteria = True
                    stock_eval_ctx.is_true_bounce = True
                    if "[Pardon]" not in stock_eval_ctx.reason:
                        stock_eval_ctx.reason = f"🛡️ [Pardon-Context] {_rule_reason}"

                stock_eval_ctx.score = float(combined_score)
                stock_eval_ctx.grade = final_grade
                if final_grade in ["S", "A"]:
                    stock_eval_ctx.intrinsic_grade = final_grade

                is_fomo_reject = decision_result.decision_state == "REJECT_FOMO"
                if is_fomo_reject:
                    hard_reject_reason = decision_result.reason
                elif _is_pardon_context and not decision_result.final_can_buy:
                    if logger:
                        logger.info(
                            f"🛡️ [Pardon Protection] {ticker} 브레인 기술적 기각을 사면권([Pardon])으로 우회합니다."
                        )
                    decision_result.final_can_buy = True
                    decision_result.decision_state = "APPROVE_PARDON"
                    decision_result.reason = f"🛡️ [Pardon] {hard_reject_reason}"

                if not decision_result.final_can_buy:
                    if hard_reject_reason is None:
                        hard_reject_reason = decision_result.reason
                    if logger and not optimizer_mode:
                        logger.info(
                            f"🚫 [Entry Block] {ticker} Reject: {hard_reject_reason} | Grade: {final_grade}"
                        )

                    safe_update(
                        decision,
                        {
                            "final_can_buy": False,
                            "final_reason": hard_reject_reason,
                            "decision_state": decision_result.decision_state,
                        },
                    )

                    if (
                        isinstance(v3_indicators, dict)
                        and "final_trading_decision" in v3_indicators
                    ):
                        v3_indicators["final_trading_decision"]["final_can_buy"] = False
                        v3_indicators["final_trading_decision"]["final_reason"] = (
                            hard_reject_reason
                        )

                        if (
                            "execution_trigger" in v3_indicators
                            and v3_indicators["execution_trigger"] is not None
                        ):
                            safe_set(
                                v3_indicators["execution_trigger"], "trigger_hit", False
                            )
                            safe_set(
                                v3_indicators["execution_trigger"],
                                "approved_quantity",
                                0,
                            )

                    if (
                        not safe_get(decision_result, "final_can_buy", False)
                        and _is_pardon_reject
                    ):
                        if "Market Crash" in str(
                            safe_get(decision_result, "reason", "")
                        ):
                            decision_result.final_can_buy = True
                            decision_result.decision_state = "APPROVE_PARDON"
                            decision_result.reason = f"{hard_reject_reason} | {safe_get(decision_result, 'reason', 'N/A')}"

                final_can_buy = decision_result.final_can_buy
                final_reason = decision_result.reason
                decision_state_val = decision_result.decision_state
                trigger_hit_sync = final_can_buy
                approved_quantity = decision_result.approved_quantity

                if (
                    is_recovering_leader
                    and combined_score >= 85.0
                    and not final_can_buy
                ):
                    reason_raw = str(final_reason)
                    if any(
                        kw in reason_raw
                        for kw in ["VWAP", "VCP", "MA50", "트렌드", "역배열", "기술적"]
                    ):
                        final_can_buy = True
                        final_reason = (
                            f"🛡️ [Pardon] V자 반등 특례 (Bypass: {reason_raw[:15]}...)"
                        )
                        trigger_hit_sync = True
                        hard_reject_reason = final_reason

                        if (
                            isinstance(v3_indicators, dict)
                            and "execution_trigger" in v3_indicators
                        ):
                            safe_set(
                                v3_indicators["execution_trigger"], "trigger_hit", True
                            )
                            safe_set(
                                v3_indicators["execution_trigger"], "msg", final_reason
                            )
                            safe_set(
                                v3_indicators["execution_trigger"],
                                "approved_quantity",
                                9999999,
                            )

                if (
                    isinstance(v3_indicators, dict)
                    and "final_trading_decision" in v3_indicators
                ):
                    if not final_can_buy:
                        v3_indicators["final_trading_decision"]["final_can_buy"] = False
                        v3_indicators["final_trading_decision"]["final_reason"] = (
                            final_reason
                        )
                    else:
                        v3_indicators["final_trading_decision"]["final_can_buy"] = True
                        v3_indicators["final_trading_decision"]["execution_signal"] = (
                            "BUY"
                        )
                        v3_indicators["final_trading_decision"]["final_reason"] = (
                            final_reason
                        )

                if (
                    isinstance(v3_indicators, dict)
                    and "execution_trigger" in v3_indicators
                    and v3_indicators["execution_trigger"]
                ):
                    safe_set(
                        v3_indicators["execution_trigger"],
                        "trigger_hit",
                        trigger_hit_sync,
                    )
                    safe_set(v3_indicators["execution_trigger"], "msg", final_reason)
                    safe_set(
                        v3_indicators["execution_trigger"],
                        "intra_acc",
                        float(
                            safe_get(
                                trigger_info,
                                "intra_acc",
                                safe_get(trigger_info, "tick_acc", 0.0),
                            )
                        ),
                    )
                    safe_set(
                        v3_indicators["execution_trigger"],
                        "tick_acc",
                        float(
                            safe_get(
                                trigger_info,
                                "tick_acc",
                                safe_get(trigger_info, "intra_acc", 0.0),
                            )
                        ),
                    )
                    safe_set(
                        v3_indicators["execution_trigger"],
                        "approved_quantity",
                        decision_result.approved_quantity,
                    )

                execution_signal = (
                    "BUY" if final_can_buy and trigger_hit_sync else "HOLD"
                )
                _has_pardon_flag = (
                    "[Pardon]" in str(final_reason)
                    or "[Pardon]" in str(hard_reject_reason)
                    or "Hyper-Momentum Override" in str(final_reason)
                )

                safe_update(
                    decision,
                    {
                        "final_can_buy": final_can_buy,
                        "final_reason": final_reason,
                        "decision_state": decision_state_val,
                        "is_limit_up_mode": is_limit_up_trade,
                        "execution_signal": execution_signal,
                        "has_pardon": _has_pardon_flag,
                        "is_blocked_by_market": safe_get(
                            decision, "is_blocked_by_market", False
                        ),
                        "is_recovering_leader": is_recovering_leader,
                    },
                )

                if (
                    final_stock_eval
                    and "position_size_ratio" in decision_result.extra_info
                ):
                    final_stock_eval.position_size_ratio = decision_result.extra_info[
                        "position_size_ratio"
                    ]
                    v3_indicators["final_stock_eval"] = final_stock_eval

                if market_regime_override and isinstance(v3_indicators, dict):
                    v3_indicators["market_regime"] = market_regime_override
                    if isinstance(decision, dict):
                        decision["market_regime"] = market_regime_override

                v3_indicators = sanitize_dict(v3_indicators)

            except Exception as e:
                import traceback
                logger.error(
                    f"[{ticker}] Intraday Fusion Error: {e}\n{traceback.format_exc()}"
                )
                return get_failed_candidate_result(
                    ticker, f"Intraday Fusion Fatal: {e}"
                )
            is_s_class = False

    except Exception as e:
        logger.error(f"[{ticker}] combined_score_for_ticker Error: {e}")
        return get_failed_candidate_result(ticker, f"연산 에러: {e}")

    if v3_indicators is not None:
        v3_indicators["atr_5m"] = atr_5m

    final_can_buy_flag = final_can_buy

    if "decision" in locals():
        final_reason_str = str(getattr(decision, "final_reason", "")) or str(
            hard_reject_reason
        )
        has_vip_pass = (
            "[Pardon]" in final_reason_str
            or "Hyper-Momentum Override" in final_reason_str
        )
        decision["has_pardon"] = has_vip_pass
        if v3_indicators is not None:
            v3_indicators["has_pardon"] = has_vip_pass
            if has_vip_pass and "final_trading_decision" in v3_indicators:
                v3_indicators["final_trading_decision"]["has_pardon"] = True
                v3_indicators["final_trading_decision"]["final_can_buy"] = True
                decision["final_can_buy"] = True

    final_reason_str = str(hard_reject_reason)
    has_vip_pass_final = (
        "[Pardon]" in final_reason_str or "Hyper-Momentum Override" in final_reason_str
    )

    final_can_buy_flag = final_can_buy
    if has_vip_pass_final:
        final_can_buy_flag = True

    if isinstance(v3_indicators, dict):
        v3_indicators["has_pardon"] = has_vip_pass_final
        if "final_trading_decision" in v3_indicators:
            v3_indicators["final_trading_decision"]["has_pardon"] = has_vip_pass_final
            if has_vip_pass_final:
                v3_indicators["final_trading_decision"]["is_blocked_by_market"] = False
                v3_indicators["final_trading_decision"]["final_can_buy"] = True

    result = {
        "ticker": ticker,
        "entry_price": current_price,
        "combined_score": round(float(combined_score), 2),
        "can_buy": bool(final_can_buy_flag),
        "hard_reject_reason": hard_reject_reason if hard_reject_reason else "PASSED",
        "day_acc_score": day_acc_score,
        "supply_day": supply_day,
        "supply_intra": supply_intra,
        "intra_acc": float(trigger_info.intra_acc or trigger_info.tick_acc or 0.0),
        "pb_quality": float(
            trigger_info.pullback_quality or trigger_info.pb_quality or 0.0
        ),
        "vol_surge_ratio": float(trigger_info.vol_surge_ratio),
        "market_conditions": market_conditions,
        "gap_result": gap_result,
        "v3_indicators": v3_indicators,
        "atr_5m": atr_5m,
        "latest_row": latest,
        "minute_df": minute_df,
        "position_size_ratio": position_size_ratio,
        "is_single_order": is_single_order,
        "is_true_bounce": is_true_bounce,
        "is_recovering_leader": is_recovering_leader,
        "is_v_bounce_event": is_v_bounce_event if "is_v_bounce_event" in locals() else False,
    }

    def _recursive_asdict(obj):
        if obj is None:
            return None
        import dataclasses
        if dataclasses.is_dataclass(obj):
            return _recursive_asdict(dataclasses.asdict(obj))
        elif isinstance(obj, dict):
            return {k: _recursive_asdict(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [_recursive_asdict(v) for v in obj]
        return obj

    result["v3_indicators"] = _recursive_asdict(result.get("v3_indicators", {}))
    result["gap_result"] = _recursive_asdict(result.get("gap_result", {}))
    return result
