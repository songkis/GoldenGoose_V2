from typing import Dict, Any, List

# Import necessary modules for scoring
# Assuming these are available and side-effect free
from util.CommUtils import load_data
from config.log_settings import setup_logger

logger = None
minute_data_cache = {}


def init_worker():
    """
    Initialize worker process.
    Setup logger and clear cache.
    """
    global logger, minute_data_cache
    logger = setup_logger("ParallelWorker")
    minute_data_cache = {}


def process_chunk(
    chunk_data: List[tuple],
    params: Dict[str, Any],
    capital: int,
    market_avg_acc: float,
    tp: str,
) -> List[Dict[str, Any]]:
    """
    Process a chunk of tickers.
    chunk_data: List of (ticker, daily_df) tuples
    """
    global minute_data_cache
    from strategy.core.TradingComm import combined_score_for_ticker_v3
    results = []

    for ticker, daily_df in chunk_data:
        try:
            # Check cache for minute data
            if ticker in minute_data_cache:
                minute_df = minute_data_cache[ticker]
            else:
                # Load and cache
                try:
                    minute_df = load_data(ticker, "분별주가")
                    minute_data_cache[ticker] = minute_df
                except Exception as e:
                    # logger.debug(f"Worker: Failed to load minute data for {ticker}: {e}")
                    minute_df = None

            # Calculate score
            r = combined_score_for_ticker_v3(
                ticker,
                daily_df,
                params=params,
                capital=capital,
                minute_df=minute_df,
                market_avg_acc=market_avg_acc,
                tp=tp,
            )

            results.append({"ticker": ticker, **r})

        except Exception as e:
            if logger:
                logger.error(f"Worker: Critical Error processing {ticker}: {e}")
            pass

    return results


