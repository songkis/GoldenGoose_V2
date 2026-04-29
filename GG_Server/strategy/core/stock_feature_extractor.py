# -*- coding: utf-8 -*-
import pandas as pd
import logging
from typing import Dict, Any, Optional, Tuple
from strategy.indicators.technical_indicators import calculate_intraday_acceleration_v5_6
from strategy.core.scoring_engine import calculate_refined_supply_score_v6
from strategy.indicators.pattern_recognizer import process_limit_up_from_df

logger = logging.getLogger(__name__)

def calculate_stock_features(ticker: str, data: pd.DataFrame, minute_df: Optional[pd.DataFrame], 
                             tp: str, params: Dict[str, Any], current_price: float) -> Dict[str, Any]:
    """
    [Relocated] 개별 종목 지표 계산 (원본 로직 유지)
    """
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
            
    tick_acc = 0.0
    supply_intra_val = 0.0
    limit_up_data = {}

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
        
    return {
        "is_true_bounce": is_true_bounce,
        "tick_acc": tick_acc,
        "supply_intra_val": supply_intra_val,
        "limit_up_data": limit_up_data,
        "day_open": day_open,
        "day_high": day_high,
        "day_low": day_low,
        "atr_pct": atr_pct
    }
