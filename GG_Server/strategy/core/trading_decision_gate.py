# -*- coding: utf-8 -*-
import logging
import pandas as pd
import inspect
from typing import Any, Optional
from dataclasses import asdict
from util.Utils import safe_get
from .trading_factory import create_strategy_evaluator

logger = logging.getLogger(__name__)

def get_final_trading_decision(
    ticker,
    daily_df,
    minute_df,
    ticker_results,
    exec_trigger,
    tp,
    params=None,
    decision_result=None,
    master_strategy_evaluator=None
):
    """
    [V5.0 Zero-Defect]
    의사결정 게이트웨이 로직
    """
    is_recovering_leader = safe_get(ticker_results, "is_recovering_leader", False)
    is_true_bounce = safe_get(ticker_results, "is_true_bounce", False)
    
    if ticker_results and not isinstance(ticker_results, dict):
        stock_data = asdict(ticker_results)
    else:
        stock_data = ticker_results.copy() if ticker_results else {}

    stock_data["ticker"] = ticker
    stock_data["is_true_bounce"] = is_true_bounce
    stock_data["daily_df"] = daily_df
    stock_data["minute_df"] = minute_df

    ai_prob = 0.5
    is_ai_missing = False

    try:
        if "ai_prob" in ticker_results:
            ai_prob = float(ticker_results["ai_prob"])
        elif "AI_PROB" in ticker_results:
            ai_prob = float(ticker_results["AI_PROB"])
        elif (
            daily_df is not None
            and "AI_PROB" in daily_df.columns
            and not pd.isna(daily_df["AI_PROB"].iloc[-1])
        ):
            ai_prob = float(daily_df["AI_PROB"].iloc[-1])
        elif (
            minute_df is not None
            and "AI_PROB" in minute_df.columns
            and not pd.isna(minute_df["AI_PROB"].iloc[-1])
        ):
            ai_prob = float(minute_df["AI_PROB"].iloc[-1])
    except Exception as e:
        if logger:
            logger.debug(f"[{ticker}] AI 데이터 매핑 실패: {e}")

    if ai_prob is None or ai_prob == 0.0:
        ai_prob = 0.5
        is_ai_missing = True
        if logger:
            logger.debug(f"[{ticker}] ⚠️ AI 데이터 없음 -> 중립(0.5) 처리")

    stock_data["ai_surge_probability"] = ai_prob
    stock_data["AI_PROB"] = ai_prob
    stock_data["is_ai_missing"] = is_ai_missing

    combined_score = float(safe_get(stock_data, "combined_score", 0.0))
    grade = safe_get(ticker_results, "fuse_grade")

    if not grade:
        if params is None:
            params = {}
        cutoff_s = float(params.get("s_threshold_normal", 85.0))
        cutoff_a = float(params.get("a_threshold_normal", 55.0))
        cutoff_b = float(params.get("b_threshold_floor", 45.0))

        if combined_score >= cutoff_s:
            grade = "S"
        elif combined_score >= cutoff_a:
            grade = "A"
        elif combined_score >= cutoff_b:
            grade = "B"
        else:
            grade = "C"

    stock_data["final_grade"] = grade
    stock_data["intrinsic_grade"] = grade

    if exec_trigger:
        stock_data["trigger_hit"] = safe_get(exec_trigger, "trigger_hit", False)
        stock_data["execution_trigger"] = exec_trigger

    market_data = {
        "is_bull_market": safe_get(ticker_results, "is_bull_market", False),
        "current_index_change": safe_get(ticker_results, "current_index_change", 0.0),
        "market_regime": safe_get(ticker_results, "market_regime", "NEUTRAL"),
        "momentum_state": safe_get(ticker_results, "momentum_state", "NORMAL"),
        "system_params": params if params else {},
    }

    evaluator = (
        create_strategy_evaluator(params) if params else master_strategy_evaluator
    )

    eval_result = evaluator.run_all(stock_data, market_data)
    score_modifier = float(safe_get(eval_result, "score_modifier", 0.0))

    if not safe_get(eval_result, "is_approved", True):
        score_modifier = 0.0

    final_combined_score = max(0.0, combined_score + score_modifier)

    if params is None:
        params = {}
    cutoff_s = float(params.get("s_threshold_normal", 85.0))
    cutoff_a = float(params.get("a_threshold_normal", 55.0))
    cutoff_b = float(params.get("b_threshold_floor", 45.0))

    if final_combined_score >= cutoff_s:
        re_evaluated_grade = "S"
    elif final_combined_score >= cutoff_a:
        re_evaluated_grade = "A"
    elif final_combined_score >= cutoff_b:
        re_evaluated_grade = "B"
    else:
        re_evaluated_grade = "C"

    is_blocked = safe_get(ticker_results, "is_blocked", False)
    final_reason_msg = eval_result["reason"]

    if is_blocked and eval_result["is_approved"]:
        final_reason_msg = safe_get(ticker_results, "fuse_reason", "Engine Blocked")
    elif final_combined_score < cutoff_b and eval_result["is_approved"]:
        final_reason_msg = f"점수 미달 ({final_combined_score:.1f} < {cutoff_b})"

    supply_intra_val = float(safe_get(ticker_results, "supply_intra", 0.0))
    tick_acc_val = float(
        safe_get(ticker_results, "tick_acc", safe_get(ticker_results, "intra_acc", 0.0))
    )
    is_v_bounce_event = tick_acc_val >= 20.0 and supply_intra_val >= 60.0

    brain_reason = str(decision_result.reason) if decision_result else ""
    is_hard_executed_by_brain = (
        "엣지 결여" in brain_reason or "자본 기아" in brain_reason
    )

    if params and (
        (is_true_bounce or is_v_bounce_event)
        and supply_intra_val >= params.get("a_threshold_normal", 55.0)
    ):
        if not is_hard_executed_by_brain:
            eval_result["is_approved"] = True
            is_blocked = False
            final_reason_msg = f"🛡️ [Pardon] True-Bounce/Momentum 강제 승격 사면. (Original: {final_reason_msg})"
            if final_combined_score < cutoff_a:
                final_combined_score = float(cutoff_a)
                re_evaluated_grade = "A"

    execution_result = re_evaluated_grade
    a_threshold = cutoff_a
    is_acc_sync = (
        safe_get(ticker_results, "day_acc_score", 0) >= a_threshold
        and safe_get(ticker_results, "intra_acc", 0) >= 25.0
    )

    result = {
        "final_grade": execution_result,
        "intrinsic_grade": re_evaluated_grade,
        "intrinsic_can_buy": eval_result["is_approved"],
        "final_can_buy": eval_result["is_approved"]
        and not is_blocked
        and (execution_result != "F"),
        "final_reason": final_reason_msg,
        "combined_score": float(final_combined_score),
        "is_acc_sync": is_acc_sync,
        "is_blocked_by_market": is_blocked
        or (
            not eval_result["is_approved"] and "Market" in eval_result.get("reason", "")
        ),
        "extra_info": eval_result.get("extra_info", {}),
    }

    if decision_result is not None:
        result["decision_state"] = decision_result.decision_state
        result["position_size_ratio"] = decision_result.extra_info.get(
            "position_size_ratio", 1.0
        )
        if "REJECT" in str(decision_result.decision_state):
            result["final_can_buy"] = False
            result["final_reason"] = (
                f"[{decision_result.decision_state}] {decision_result.reason}"
            )

    return result

def get_failed_candidate_result(ticker: str, reason: str = "Insufficient data") -> dict:
    """분석 실패/데이터 부족 시 반환하는 표준 Fallback 딕셔너리"""
    return {
        "ticker": ticker,
        "entry_price": 0.0,
        "combined_score": 0.0,
        "day_acc_score": 0.0,
        "supply_day": 0.0,
        "supply_intra": 0.0,
        "latest_row": None,
        "minute_df": None,
        "atr_5m": 0.0,
        "v3_indicators": {},
        "exit_levels": {},
        "can_buy": False,
        "is_valid": False,
        "fail_reason": reason,
        "market_conditions": {},
        "gap_result": {},
        "position_size_ratio": 1.0,
        "is_true_bounce": False,
        "is_recovering_leader": False,
        "is_v_bounce_event": False,
    }

def get_bars_since_entry(ticker, entry_time, minute_df):
    """[V10.0] 진입 이후 경과된 봉 개수 산출"""
    try:
        if minute_df is None or minute_df.empty:
            return 0
        if entry_time is None:
            return 0

        if isinstance(entry_time, str):
            parsed_time = pd.to_datetime(entry_time)
        else:
            parsed_time = entry_time

        if parsed_time.hour == 0 and parsed_time.minute == 0:
            return 0

        idx_after = minute_df.index[minute_df.index >= parsed_time]
        return len(idx_after)

    except Exception as e:
        if logger:
            logger.debug(f"get_bars_since_entry Error: {e}")
        return 0
