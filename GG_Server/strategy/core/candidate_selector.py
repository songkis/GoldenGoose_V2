import os
import logging
import concurrent.futures
import pandas as pd
from typing import Any, Dict, Optional
from functools import partial

from util.Utils import set_global_regime
from strategy.indicators.market_analysis import analyze_market_conditions
from strategy.rules.candidate_selection import (
    rank_trading_candidates,
)
from strategy.parallel_processor import init_worker, process_chunk
from strategy.core.worker import process_ticker
from SQL.sql import UPDATE_SEARCH_STK_SORT_NO
from util.CommUtils import get_db_connection
from config.ai_settings import SYS_ID, BUY_TOP_CNT

logger = logging.getLogger(__name__)

# [Phase 6] Global ThreadPoolExecutor
GLOBAL_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
    max_workers=max(4, (os.cpu_count() or 4) * 2)
)


def set_logger(external_logger):
    global logger
    logger = external_logger


def candidate_stocks(data_pool, params, capital, market_avg_acc, tp, port_list=None):
    """Batch scoring logic moved from TradingComm."""
    from util.CommUtils import load_data
    from strategy.core.TradingComm import combined_score_for_ticker_v3

    results = []
    error_count = 0
    use_executor = tp == "intraday" and len(data_pool) > 10

    market_cond_kosdaq = analyze_market_conditions(2)
    market_cond_kospi = analyze_market_conditions(1)

    regime_kosdaq = (
        getattr(market_cond_kosdaq, "market_regime", "NEUTRAL")
        if hasattr(market_cond_kosdaq, "market_regime")
        else market_cond_kosdaq.get("market_regime", "NEUTRAL")
    )
    regime_kospi = (
        getattr(market_cond_kospi, "market_regime", "NEUTRAL")
        if hasattr(market_cond_kospi, "market_regime")
        else market_cond_kospi.get("market_regime", "NEUTRAL")
    )

    regime_rank = {"CRASH": 4, "BEAR": 3, "NEUTRAL": 2, "BULL": 1}
    r_kosdaq = regime_rank.get(regime_kosdaq.upper(), 2)
    r_kospi = regime_rank.get(regime_kospi.upper(), 2)

    global_regime = regime_kospi if r_kospi > r_kosdaq else regime_kosdaq
    set_global_regime(global_regime)

    if use_executor:
        try:
            pool_args = []
            for ticker, df in data_pool.items():
                m_type = 1
                if df is not None and not df.empty and "시장구분" in df.columns:
                    m_type = int(df["시장구분"].iloc[-1])
                else:
                    m_type = 1 if ticker.startswith("0") else 2

                ticker_is_kospi = m_type == 1
                regime_map = {
                    "KOSPI": regime_kospi,
                    "KOSDAQ": regime_kosdaq,
                    "target_market": "KOSPI" if ticker_is_kospi else "KOSDAQ",
                }
                pool_args.append(
                    (
                        ticker,
                        df,
                        params,
                        capital,
                        market_avg_acc,
                        tp,
                        regime_map,
                        port_list,
                    )
                )

            futures = [
                GLOBAL_EXECUTOR.submit(process_ticker, *args) for args in pool_args
            ]
            for future in concurrent.futures.as_completed(futures):
                res = future.result()
                if "error" in res:
                    error_count += 1
                elif "ticker" in res:
                    results.append(res)
            return results, error_count
        except Exception as e:
            logger.error(f"[Global ThreadPool] Failed: {e}")
            results = []
            error_count = 0

    for ticker, df in data_pool.items():
        try:
            minute_df = load_data(ticker, "분별주가")
        except Exception:
            minute_df = None

        m_type = 1
        if df is not None and not df.empty and "시장구분" in df.columns:
            m_type = int(df["시장구분"].iloc[-1])
        else:
            m_type = 1 if ticker.startswith("0") else 2

        ticker_is_kospi = m_type == 1
        actual_regime = regime_kospi if ticker_is_kospi else regime_kosdaq

        try:
            r = combined_score_for_ticker_v3(
                ticker,
                df,
                params=params,
                capital=capital,
                minute_df=minute_df,
                market_avg_acc=market_avg_acc,
                tp=tp,
                market_regime_override=actual_regime,
                port_list=port_list,
            )
            results.append({"ticker": ticker, **r})
        except Exception as e:
            error_count += 1
            logger.info(f"candiate_stocks Error: {e}")
    return results, error_count


def select_candidates_v2(
    data_pool: Dict[str, pd.DataFrame],
    min_candidates: int = 20,
    params: Optional[Dict[str, Any]] = None,
    capital: int = 20_000_000,
    port_list: list = None,
    market_avg_acc: float = 0.0,
    tp: str = "intraday",
):
    try:
        final_results = []
        try:
            capital = float(str(capital).replace(",", "")) if capital else 20000000.0
            threshold = float(params.get("buy_threshold", 60.0)) if params else 60.0
        except Exception:
            capital = 20000000.0
            threshold = 60.0

        results, e_count = candidate_stocks(
            data_pool, params, capital, market_avg_acc, tp, port_list=port_list
        )

        if not results:
            return []

        for r in results:
            v3_ind = r.get("v3_indicators", {})
            is_recovering = isinstance(v3_ind, dict) and v3_ind.get(
                "final_stock_eval", {}
            ).get("is_recovering_leader", False)
            is_elite = isinstance(v3_ind, dict) and v3_ind.get(
                "final_trading_decision", {}
            ).get("final_grade") in ["S", "A"]
            is_true_bounce = isinstance(v3_ind, dict) and v3_ind.get(
                "execution_trigger", {}
            ).get("is_true_bounce", False)

            if is_true_bounce:
                r["can_buy"] = True

            if not r.get("can_buy", False) and not is_recovering and not is_elite:
                r["combined_score"] = 0.0
            elif is_recovering or is_elite:
                r["can_buy"] = True
                if "v3_indicators" in r:
                    decision = r["v3_indicators"].get("final_trading_decision", {})
                    decision["final_can_buy"] = True
                    decision["has_pardon"] = True

            ticker = r.get("ticker")
            if port_list and ticker in port_list:
                final_results.append(r)

        if tp == "intraday" and min_candidates == 1:
            for c in results:
                if c.get("ticker") not in [fr.get("ticker") for fr in final_results]:
                    final_results.append(c)
            return final_results
        elif (
            tp == "intraday" and min_candidates >= BUY_TOP_CNT
        ):  # 정렬된 종목을 검색종목의 sort_no에 update
            results_sorted = rank_trading_candidates(
                results, port_list, mode="intraday"
            )
            idx = 0
            for r in results_sorted:
                with get_db_connection() as conn:
                    query = UPDATE_SEARCH_STK_SORT_NO
                    cursor = conn.cursor()
                    idx += 1
                    cursor.execute(query, (idx, r["ticker"], SYS_ID))
                    conn.commit()
            return []
    except Exception as e:
        logger.error(f"select_candidates_v2 failed: {e}")
        return []


def select_candidates_parallel(
    data_pool: Dict[str, pd.DataFrame],
    min_candidates: int = 20,
    params: Optional[Dict[str, Any]] = None,
    capital: int = 20_000_000,
    port_list: list = None,
    market_avg_acc: float = 0.0,
    tp: str = "picking",
    index_data: Dict[str, pd.DataFrame] = None,
):
    import math

    base_floor = params.get("b_threshold_floor", 45.0) if params else 45.0
    threshold = base_floor
    items = list(data_pool.items())
    total_items = len(items)
    num_threads = min((os.cpu_count() or 4) * 2, 16)
    chunk_size = math.ceil(total_items / num_threads)
    chunks = [items[i : i + chunk_size] for i in range(0, total_items, chunk_size)]

    init_worker()
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        func = partial(
            process_chunk,
            params=params,
            capital=capital,
            market_avg_acc=market_avg_acc,
            tp=tp,
        )
        results_list = list(executor.map(func, chunks))
        results = []
        for res in results_list:
            results.extend(res)

        final_results = []
        if port_list:
            for r in results:
                if r.get("ticker") in port_list:
                    final_results.append(r)

        if tp == "intraday":
            results_sorted = rank_trading_candidates(
                results, port_list, mode="intraday"
            )
        else:
            results_sorted = rank_trading_candidates(results, port_list, mode="picking")

        candidates = []
        for r in results_sorted:
            current_score = r.get("combined_score", 0.0)
            if current_score == 0.0:
                current_score = float(r.get("supply_day", 0.0)) + float(
                    r.get("supply_intra", 0.0)
                )
                r["combined_score"] = current_score
            # [Optimization] If we are in picking mode, we should return all candidates for further ranking/filtering.
            # Otherwise, enforce the threshold floor.
            if tp == "picking" or current_score >= threshold:
                candidates.append(r)

        return candidates[:min_candidates]


def prepare_intraday_data_pool(port_list=None):
    """Centralized data pool retrieval for intraday analysis.
    Ensures that both active portfolio stocks and new candidates are included.
    """
    from SQL.sql import GET_PRE_INTRADAY_TRGT_STK_LIST
    from util.CommUtils import get_db_connection, load_data
    from config.ai_settings import TIME_STEP
    import pandas as pd

    data_pool = {}
    try:
        with get_db_connection() as conn:
            # 1. Fetch Target Tickers (New Candidates)
            query = GET_PRE_INTRADAY_TRGT_STK_LIST
            df_target = pd.read_sql_query(query, con=conn)
            tickers = set(df_target["종목코드"].tolist())

            # 2. Add Portfolio Tickers (Active Positions)
            if port_list:
                tickers.update(port_list)

            # 3. Fetch Data for each ticker (Daily OHLC for context)
            for t in tickers:
                try:
                    df = load_data(t, "일별주가")
                    #  Portfolio stocks are forced into the pool regardless of length if they have data
                    is_in_port = port_list and t in port_list
                    min_len = (TIME_STEP or 30) if not is_in_port else 5

                    if df is not None and len(df) >= min_len:
                        data_pool[t] = df
                except Exception:
                    continue
    except Exception as e:
        logger.error(f"Failed to prepare intraday data pool: {e}")

    return data_pool
