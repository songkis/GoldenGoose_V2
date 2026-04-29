# -*- coding: utf-8 -*-
import pandas as pd
import logging
from typing import Dict, Any, Tuple
from core.schemas import TradingDecisionParam, MarketState, StockEvaluation
from strategy.core.scoring_engine import apply_momentum_premium
from strategy.core.scoring_engine import apply_dynamic_scalar

logger = logging.getLogger(__name__)

def integrate_and_grade_scores(
    ticker: str,
    daily_score: float,
    intraday_score: float,
    tick_acc: float,
    supply_intra: float,
    atr_pct: float,
    rs_gap_val: float,
    market_conditions: Any,
    params: Dict[str, Any],
    intraday_v2: Dict[str, Any]
) -> Tuple[float, str]:
    """
    [Relocated] 점수 통합 및 등급 산출 (원본 로직 유지)
    """
    vol_scalar = apply_dynamic_scalar(atr_pct, tick_acc)
    
    daily_score *= vol_scalar
    intraday_score *= vol_scalar
    final_score = (daily_score * 0.5) + (intraday_score * 0.5)
    final_score *= market_conditions.market_energy

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

    combined_score = round(float(final_score), 2)
    
    _s_cut = float(params.get("s_threshold_normal", 85.0)) if params else 85.0
    _a_cut = float(params.get("a_threshold_normal", 55.0)) if params else 55.0
    _b_cut = float(params.get("b_threshold_floor", 45.0)) if params else 45.0

    if combined_score >= _s_cut:
        final_grade = "S"
    elif combined_score >= _a_cut:
        final_grade = "A"
    elif combined_score >= _b_cut:
        final_grade = "B"
    else:
        final_grade = "C"
        
    return combined_score, final_grade

def assemble_decision_params(
    ticker: str,
    combined_score: float,
    grade: str,
    market_regime: str,
    market_conditions: Any,
    trigger_info: Any,
    is_true_bounce: bool,
    latest: Dict[str, Any],
    data: pd.DataFrame,
    final_stock_eval: Any,
    current_price: float,
    tp: str,
    latest_minute: Dict[str, Any],
    intraday_v2: Dict[str, Any]
) -> Tuple[TradingDecisionParam, MarketState, StockEvaluation]:
    """
    [Relocated] 의사결정 파라미터 DTO 조립 (원본 로직 유지)
    """
    paramDic = TradingDecisionParam(ticker=ticker)
    paramDic.ticker = ticker
    paramDic.combined_score = combined_score
    paramDic.fuse_grade = grade
    paramDic.intrinsic_grade = grade
    paramDic.market_regime = market_regime
    paramDic.momentum_state = getattr(market_conditions, "momentum_state", "NORMAL")
    paramDic.pb_quality = float(trigger_info.pullback_score or trigger_info.pb_quality or 0.0)
    paramDic.pullback_quality = paramDic.pb_quality
    paramDic.supply_intra = float(trigger_info.supply_intra)
    paramDic.vol_surge_ratio = float(trigger_info.vol_surge_ratio)
    paramDic.intraday_score = float(trigger_info.intraday_score)
    paramDic.intra_acc = float(trigger_info.tick_acc or trigger_info.intra_acc or 0.0)
    paramDic.tick_acc = paramDic.intra_acc
    paramDic.is_true_bounce = is_true_bounce

    ai_prob_val = latest.get("AI_PROB")
    if ai_prob_val is None or pd.isna(ai_prob_val):
        if "AI_PROB" in data.columns and not pd.isna(data["AI_PROB"].iloc[-1]):
            ai_prob_val = float(data["AI_PROB"].iloc[-1])
        else:
            ai_prob_val = 0.5

    limit_up_data = final_stock_eval.limit_up_data
    is_limit_up_trade = limit_up_data.get("is_limit_up_entry", False)
    trigger_hit_val = trigger_info.trigger_hit
    trigger_msg = trigger_info.msg

    market_ctx = MarketState(
        regime=market_regime,
        index_change=getattr(market_conditions, "current_index_change", 0.0),
        is_panic=(market_regime == "CRASH"),
        drop_rate=getattr(market_conditions, "current_index_change", 0.0),
    )

    _breakout_info = getattr(final_stock_eval, "breakout_info", {})
    if _breakout_info is None: _breakout_info = {}
    is_breakout = _breakout_info.get("is_breakout", False)
    
    meets_basic = final_stock_eval.is_buy and trigger_hit_val
    custom_reason = trigger_msg if not trigger_hit_val else "Ready for AI Evaluation"
    
    curr_vol_actual = float(latest_minute.get("거래량", 0.0)) if (tp == "intraday" and latest_minute) else float(latest.get("거래량", 0.0))

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
        is_recovering_leader=getattr(final_stock_eval, "is_recovering_leader", False),
        is_true_bounce=is_true_bounce,
        expected_win_rate=0.8,
        noise_ratio=float(intraday_v2.get("noise_ratio", 0.5)),
        avg_volume_5=float(intraday_v2.get("avg_volume_5", 0.0)),
        current_volume=curr_vol_actual,
        supply_intra=float(trigger_info.supply_intra),
        tick_acc=paramDic.tick_acc,
        recent_low=0.0,
        rs_gap=final_stock_eval.rs_gap,
        bb_dist=float(_breakout_info.get("bb_dist", 0.0)),
        surge_rate=float(_breakout_info.get("surge_rate", 0.0)),
        support_levels={},
        atr_val=float(getattr(final_stock_eval, "atr_5m", getattr(final_stock_eval, "atr_pct", 2.0))),
    )
    
    return paramDic, market_ctx, stock_eval_ctx
