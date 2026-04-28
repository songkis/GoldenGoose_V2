import pandas as pd
import numpy as np
import datetime
from datetime import timedelta
from strategy.core.trading_models import MarketConditions

# Import modularized functions
from .market_status_io import fetch_trade_data, save_market_status_to_db
from .market_energy import get_market_energy_score_df_v6_2
from .market_momentum import get_market_momentum_status
from .market_formatter import format_combined_market_status

logger = None
market_cache = {}
MIN_TERM = 1
M_CACHE_EXPIRY = timedelta(minutes=MIN_TERM)

def set_logger(external_logger):
    global logger
    logger = external_logger

__all__ = [
    "analyze_market_conditions",
    "get_market_energy_score_df_v6_2",
    "save_market_status_to_db",
    "get_segmented_market_regime",
    "format_combined_market_status",
    "get_market_momentum_status",
]

market_regime_state = {
    "KOSPI": {"current_regime": "NEUTRAL", "pending_regime": "NEUTRAL", "confirmation_count": 0, "last_updated": None},
    "KOSDAQ": {"current_regime": "NEUTRAL", "pending_regime": "NEUTRAL", "confirmation_count": 0, "last_updated": None},
}

def get_segmented_market_regime():
    global market_regime_state
    return {k: v["current_regime"] for k, v in market_regime_state.items()}

def analyze_market_conditions(market_tp_cd_or_df):
    global market_regime_state
    now = datetime.datetime.now()

    if isinstance(market_tp_cd_or_df, (pd.DataFrame, pd.Series)):
        market_data, market_type, is_simulation = market_tp_cd_or_df, "KOSDAQ", True
    else:
        market_tp_cd = market_tp_cd_or_df
        market_type, is_simulation = ("KOSPI" if market_tp_cd == 1 else "KOSDAQ"), False

    if not is_simulation and market_type in market_cache:
        cached_data = market_cache[market_type]
        if now - cached_data.last_updated < M_CACHE_EXPIRY:
            cached_data.market_regime = market_regime_state[market_type]["current_regime"]
            return cached_data

    if not is_simulation: market_data = fetch_trade_data(market_tp_cd, logger)

    if market_data.empty:
        if logger: logger.error(f"[{market_type}] Market Data Fetch Failed -> Defaulting to CRASH Regime")
        return MarketConditions(market_type=market_type, buy_condition=False, sell_condition=True, current_index=0.0, current_index_change=0.0,
                                high_rate=0.0, low_rate=0.0, gap_rate=0.0, market_ma5_dist=0.0, index_return_5d=0, market_dip_rate=3.0,
                                rising_stocks=0, falling_stocks=0, adr_ratio=0.0, recent_index_changes=[], market_data=None, last_updated=now,
                                market_score=0, market_regime="CRASH")

    try:
        recent_data = market_data.iloc[-1]
        current_index = float(str(recent_data["현재지수"]).replace(",", ""))
        current_index_change = float(recent_data["지수등락율"])
        rising_stocks = int(recent_data["상승종목수"])
        falling_stocks = int(recent_data["하락종목수"])

        if (rising_stocks + falling_stocks == 0) and not is_simulation:
            return market_cache.get(market_type, MarketConditions(market_type=market_type, market_regime="NEUTRAL", market_energy=1.0, last_updated=now))

        money_diff = float(recent_data["거래대금전일대비"])
        high_rate, low_rate, gap_rate = float(recent_data["두번째등락율"]), float(recent_data["세번째등락율"]), float(recent_data["네번째등락율"])

        close_series = market_data["현재지수"].astype(str).str.replace(",", "").astype(float)
        if len(close_series) >= 20:
            ma5, ma20 = close_series.rolling(window=5).mean().iloc[-1], close_series.rolling(window=20).mean().iloc[-1]
            market_ma5_dist, market_ma20_dist = (current_index / ma5) - 1.0, (current_index / ma20) - 1.0
            index_slope = (current_index - close_series.iloc[-5]) / close_series.iloc[-5] * 100
        elif len(close_series) >= 5:
            ma5 = close_series.rolling(window=5).mean().iloc[-1]
            market_ma5_dist, market_ma20_dist, index_slope = (current_index / ma5) - 1.0, (current_index / ma5) - 1.0, 0.0
        else:
            market_ma5_dist, market_ma20_dist, index_slope = 0.0, 0.0, 0.0

        adr_ratio = (rising_stocks if rising_stocks > 0 else 0.1) / falling_stocks if falling_stocks > 0 else float(min(rising_stocks, 20.0))

        market_score = 50.0
        if market_ma20_dist > 0: market_score += 15
        if index_slope > 0.1: market_score += 10
        elif index_slope < -0.1: market_score -= 10
        if adr_ratio > 1.2 and current_index_change < 0: market_score += 10
        elif adr_ratio < 0.8 and current_index_change > 0: market_score -= 15

        is_strong_bounce = (current_index_change >= 0.8) or (current_index_change >= 0.5 and adr_ratio >= 1.0)
        raw_regime = "NEUTRAL"
        is_panic = (current_index_change <= -3.0) or (adr_ratio <= 0.35)
        if adr_ratio <= 0.5 or (current_index_change <= -2.0 and adr_ratio < 0.7) or is_panic: raw_regime = "CRASH"
        elif index_slope < -0.2 or (market_ma20_dist < 0 and adr_ratio < 0.85):
            raw_regime = "NEUTRAL" if is_strong_bounce else "BEAR"
        elif adr_ratio >= 1.1 and market_ma20_dist > 0: raw_regime = "BULL"

        is_opening_noise = not is_simulation and (now.time() < datetime.time(9, 0, 5) or rising_stocks + falling_stocks == 0 or current_index == 0)
        regime_state = market_regime_state[market_type]

        if is_opening_noise: final_regime = regime_state["current_regime"]
        elif is_simulation: final_regime = raw_regime
        else:
            fast_recovery = regime_state["current_regime"] in ["CRASH", "BEAR"] and current_index_change >= 0.5 and adr_ratio >= 1.0
            if fast_recovery:
                regime_state["current_regime"], regime_state["confirmation_count"], final_regime = "NEUTRAL", 0, "NEUTRAL"
            elif raw_regime != regime_state["current_regime"]:
                if raw_regime == "CRASH":
                    if current_index_change >= 0.5: regime_state["current_regime"] = "NEUTRAL"
                    else: regime_state["current_regime"], regime_state["confirmation_count"] = "CRASH", 0
                elif raw_regime == regime_state["pending_regime"]: regime_state["confirmation_count"] += 1
                else: regime_state["pending_regime"], regime_state["confirmation_count"] = raw_regime, 1
                
                is_recovering = raw_regime in ["BULL", "NEUTRAL"] and current_index_change >= 0.3
                if regime_state["confirmation_count"] >= 2 or is_recovering:
                    regime_state["current_regime"], regime_state["confirmation_count"] = raw_regime, 0
            else: regime_state["confirmation_count"] = 0
            final_regime = regime_state["current_regime"]

        if not is_simulation: save_market_status_to_db(market_type, final_regime, current_index_change, adr_ratio, logger)

        buy_condition, sell_condition = final_regime in ["BULL", "NEUTRAL"], final_regime in ["BEAR", "CRASH"]
        if current_index_change < -1.0: market_dip_rate = 3.0
        elif current_index_change < -0.5: market_dip_rate = 2.25
        elif current_index_change > 0.5: market_dip_rate = 1.2
        else: market_dip_rate = 1.5

        if current_index_change < 0:
            if (money_diff < 0) or (0 < money_diff < 0.9):
                market_dip_rate *= (1.5 if adr_ratio <= 0.6 else 1.2)
            elif money_diff > 0 and adr_ratio > 0.8: market_dip_rate *= 0.85
        if gap_rate > 1.5 and current_index_change < gap_rate: market_dip_rate *= 1.2

        result = {
            "market_type": market_type, "buy_condition": buy_condition, "sell_condition": sell_condition,
            "current_index": current_index, "current_index_change": current_index_change,
            "high_rate": high_rate, "low_rate": low_rate, "gap_rate": gap_rate, "market_ma5_dist": market_ma5_dist,
            "index_return_5d": 0, "market_dip_rate": market_dip_rate, "rising_stocks": rising_stocks,
            "falling_stocks": falling_stocks, "adr_ratio": adr_ratio, "recent_index_changes": close_series.tail(10).tolist(),
            "market_data": market_data, "last_updated": now, "market_score": market_score, "market_regime": final_regime,
            "momentum_state": "STRONG_BOUNCE" if is_strong_bounce else "NORMAL",
            "market_energy": get_market_energy_score_df_v6_2(market_data, logger),
        }
        market_cond = MarketConditions(**result)
        market_cache[market_type] = market_cond
        return market_cond
    except Exception as e:
        if logger: logger.error(f"[analyze_market_conditions Error] {e}")
        return MarketConditions(market_type=market_type, buy_condition=False, sell_condition=True, current_index=0.0, current_index_change=0.0, market_dip_rate=3.0, last_updated=now, market_score=0, market_regime="CRASH")
