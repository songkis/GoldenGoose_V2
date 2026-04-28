# -*- coding: utf-8 -*-


def process_ticker(
    ticker,
    df_daily,
    params,
    capital,
    market_avg_acc,
    tp,
    market_regime_override=None,
    port_list=None,
):
    """
    Worker function for multiprocessing.
    """
    try:
        from util.CommUtils import load_data
        from strategy.core.TradingComm import combined_score_for_ticker_v3

        # 1. Load Minute Data (Fresh connection per process)
        try:
            minute_df = load_data(ticker, "분별주가")
        except Exception:
            minute_df = None

        #  Segmented Regime extraction
        if isinstance(market_regime_override, dict):
            target_market = market_regime_override.get("target_market", "KOSPI")
            actual_regime = market_regime_override.get(target_market, "NEUTRAL")
        else:
            actual_regime = market_regime_override

        # 2. Score Ticker
        result = combined_score_for_ticker_v3(
            ticker,
            df_daily,
            params=params,
            capital=capital,
            minute_df=minute_df,
            market_avg_acc=market_avg_acc,
            tp=tp,
            market_regime_override=actual_regime,
            port_list=port_list,
        )

        return result

    except Exception as e:
        # Return error info instead of crashing
        return {"error": str(e), "ticker": ticker}
