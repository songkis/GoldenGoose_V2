import numpy as np
from concurrent.futures import as_completed

def get_market_momentum_status(data_pool, min_term, logger=None):
    """현재 분석 대상 종목들의 통합 가속도 평균을 병렬로 산출함"""
    from util.CommUtils import GLOBAL_THREAD_POOL
    from strategy.indicators.technical_indicators import calculate_intraday_acceleration_v5_6

    all_acc_scores = []

    def calculate_single_ticker(ticker):
        try:
            from util.CommUtils import load_data_incremental
            minute_df = load_data_incremental(ticker, "분별주가")
            if minute_df is None or minute_df.empty: return None
            return calculate_intraday_acceleration_v5_6(minute_df, min_term)
        except Exception as e:
            if logger: logger.debug(f"Momentum scan failed for {ticker}: {e}")
            return None

    futures = {GLOBAL_THREAD_POOL.submit(calculate_single_ticker, t): t for t in data_pool}
    for future in as_completed(futures):
        result = future.result()
        if result is not None: all_acc_scores.append(result)

    market_avg_acc = np.mean(all_acc_scores) if all_acc_scores else 0.0
    return float(round(market_avg_acc, 2)), all_acc_scores
