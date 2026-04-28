import logging
from typing import Dict, Any, Optional

def manage_trade_exit_levels(
    ticker: str,
    paramDic: Dict[str, Any],
    daily_df: Any,
    minute_df: Any,
    grade: str,
    params: Dict[str, Any],
    logger: Optional[logging.Logger] = None
) -> Dict[str, Any]:
    """
    [Modularized] Calculates and optimizes hybrid exit levels.
    Extracted from TradingComm.py.
    """
    from strategy.core.TradingComm import calculate_hybrid_exit_levels, optimize_trade_levels
    
    # 1. Calculate Hybrid Exit Levels
    hybrid_exit_levels = calculate_hybrid_exit_levels(
        paramDic, daily_df, minute_df, grade, params=params
    )
    
    # 2. Optimize Trade Levels (Modify hybrid_exit_levels in-place)
    optimize_trade_levels(
        paramDic,
        hybrid_exit_levels,
        grade,
        params=params,
    )
    
    if logger:
        logger.info(f"[{ticker}] Exit Levels Optimized for Grade {grade}")
        
    return hybrid_exit_levels
