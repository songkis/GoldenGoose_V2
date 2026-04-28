import logging
from typing import Dict, Any, Optional


def reconstruct_final_result(
    ticker: str,
    decision_result: Dict[str, Any],
    final_stock_eval: Dict[str, Any],
    trigger_info: Dict[str, Any],
    final_score_dic: Dict[str, Any],
    params: Dict[str, Any],
    market_regime: str,
    current_index_change: float,
    current_price: float,
    atr_5m: float,
    intraday_score: float,
    is_bull_market: bool,
    is_vwap_pullback: bool,
    ai_prob_val: float,
    logger: Optional[logging.Logger] = None,
) -> Dict[str, Any]:
    """
    [Modularized] Reconstructs the massive paramDic and final result DTO.
    Extracted from TradingComm.py to improve token efficiency and maintainability.
    """
    paramDic = {}

    # 1. Basic Metrics & Scores
    paramDic["ticker"] = ticker
    paramDic["ai_prob"] = ai_prob_val
    paramDic["is_blocked"] = getattr(final_score_dic, "is_blocked", False)
    paramDic["fuse_reason"] = getattr(final_score_dic, "reason", "UNKNOWN")
    paramDic["intra_acc"] = getattr(trigger_info, "intra_acc", 0.0)
    paramDic["intraday_score"] = intraday_score
    paramDic["supply_intra"] = getattr(trigger_info, "supply_intra", 0.0)
    paramDic["is_bull_market"] = is_bull_market
    paramDic["rs_gap"] = final_stock_eval.rs_gap
    paramDic["vcp_ratio"] = final_stock_eval.vcp_ratio
    paramDic["current_index_change"] = current_index_change
    paramDic["volume_dry"] = final_stock_eval.volume_dry
    paramDic["entry_price"] = current_price
    paramDic["atr_5m"] = atr_5m

    # 2. Breakout & Volume Info
    breakout_info = final_stock_eval.breakout_info
    paramDic["is_bottom_breakout"] = breakout_info.get("is_breakout", False)
    paramDic["breakout_strength"] = breakout_info.get("breakout_strength", 0.0)
    paramDic["volume_multiplier"] = final_stock_eval.volume_multiplier
    paramDic["volume_surge_ratio"] = paramDic["volume_multiplier"]
    paramDic["daily_score"] = final_stock_eval.daily_score
    paramDic["trend_score"] = final_stock_eval.trend_score
    paramDic["breakout_info"] = breakout_info
    paramDic["is_vwap_pullback"] = is_vwap_pullback

    # 3. Adaptive Execution Tracks
    vcp_pass = final_stock_eval.vcp_ratio <= params.get(
        "vcp_contraction_threshold", 0.15
    )
    trend_template_pass = "TT:Pass" in final_stock_eval.energy_status
    paramDic["track_a_swing"] = vcp_pass and trend_template_pass

    track_b_momentum = False
    if is_bull_market or market_regime in ["BULL", "NORMAL"]:
        if getattr(trigger_info, "intra_acc", 0.0) >= 60.0 and breakout_info.get(
            "is_breakout", False
        ):
            track_b_momentum = True
    paramDic["track_b_momentum"] = track_b_momentum

    return paramDic
