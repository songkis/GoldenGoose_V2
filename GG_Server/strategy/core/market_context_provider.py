# -*- coding: utf-8 -*-
import pandas as pd
import logging
from typing import Dict, Any, Optional
from core.schemas import StockEvaluation, MarketState

logger = logging.getLogger(__name__)

def extract_market_context(latest: Dict[str, Any], today_market_data_df: Optional[pd.DataFrame], 
                           params: Dict[str, Any], market_regime_override: Optional[str] = None):
    """
    [Relocated] 시장 국면 및 데이터 분석 (원본 로직 유지)
    """
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
    
    market_regime = (
        market_regime_override
        if market_regime_override
        else market_conditions.regime
    )
    
    return market_conditions, market_regime
