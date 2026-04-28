# -*- coding: utf-8 -*-
"""
종목 선정 모듈 (Candidate Selection)

TradingComm.py에서 분리된 종목 선정 및 랭킹 함수들.
스코어 기반 종목 필터링, 랭킹, 후보 선정 등을 제공합니다.
"""

logger = None


def set_logger(external_logger):
    """외부 로거 설정"""
    global logger
    logger = external_logger


__all__ = [
    "rank_trading_candidates",
]


def rank_trading_candidates(
    results: list, port_list: list = None, mode: str = "intraday"
) -> list:
    """
    [Phase 12] Zero-Defect Unified Golden Power Rank
    - SSOT for both Intraday and Stock Picking pipelines.
    - Uses a linear power-law equation for O(1) ranking complexity.
    - golden_power_rank = (supply_intra + intra_acc) * (pb_quality / (surge_rate + 1.0))
    """
    if not results:
        return []
    safe_port = port_list if port_list is not None else []

    import numpy as np

    # 1. Picking Mode Pre-processing: RS Threshold Calculation
    rs_threshold_top5 = 999
    if mode == "picking":
        rs_results = []
        for x in results:
            rs_score = 0.0
            try:
                if x.get("v3_indicators") and x["v3_indicators"].get("final_stock_eval"):
                    rs_score = float(
                        x["v3_indicators"]["final_stock_eval"].get("rs_gap", 0.0)
                    )
                else:
                    rs_score = float(x.get("rs_gap", 0.0))
            except Exception:
                pass
            x["_rs_val"] = rs_score
            rs_results.append(rs_score)
        rs_threshold_top5 = np.percentile(rs_results, 95) if rs_results else 999

    def _get_valid_score(item):
        s = float(item.get("combined_score", 0.0) or 0.0)
        if s <= 0.0 and item.get("v3_indicators"):
            s = float(
                item["v3_indicators"]
                .get("final_stock_eval", {})
                .get("daily_score", 0.0)
            )
        if s <= 0.0:
            s = float(item.get("swing_score", 0.0) or 0.0)
        if s <= 0.0 and item.get("latest_row"):
            s = float(item["latest_row"].get("Swing_Score", 0.0) or 0.0)
        return float(s)

    def golden_sort_key(x):
        if not x or not isinstance(x, dict):
            return (1, 1, 0, 0, 0)

        v3 = x.get("v3_indicators") or {}
        decision = v3.get("final_trading_decision") or {}
        latest = x.get("latest_row") or {}

        # [Factor Extraction] Root 팩터 우선 참조
        intra_acc = float(x.get("intra_acc", decision.get("intra_acc", decision.get("tick_acc", 0.0))))
        supply_intra = float(x.get("supply_intra", decision.get("supply_intra", 0.0)))
        pb_quality = float(x.get("pb_quality", x.get("pullback_quality", decision.get("pb_quality", decision.get("pullback_quality", 0.0)))))

        # Price & Surge calculation
        cp = float(x.get("entry_price", latest.get("종가", 0)) or 0)
        op = float(latest.get("시가", cp) or cp)
        pc = float(latest.get("전일종가", op) or op)

        if pc <= 0:
            surge_rate = 0.0
        else:
            surge_rate = ((cp - pc) / pc) * 100

        is_yin_candle = cp < op
        is_valid = _get_valid_score(x) > 0.0

        # [Logic Change: Power-Law Linear Equation]
        # O(1) complexity ranking formula
        # Fix: Prevent ZeroDivisionError when surge_rate == -1.0 and avoid negative ranks for surge_rate < -1.0
        safe_surge = max(float(surge_rate), 0.0)
        golden_power_rank = (supply_intra + intra_acc) * (
            (1.0 + pb_quality) / (safe_surge + 1.0)
        )

        # [Bearish Penalty] 음봉 투매 판정 시 90% 소멸 페널티
        if is_yin_candle:
            golden_power_rank *= 0.1

        trading_val = float(latest.get("거래대금", 0) or latest.get("누적거래대금", 0))

        x["_sort_metrics"] = {
            "golden_rank": golden_power_rank,
            "surge_rate": surge_rate,
            "supply_intra": supply_intra,
            "intra_acc": intra_acc,
            "pb_quality": pb_quality,
            "is_valid": is_valid,
        }

        return (
            bool(x.get("ticker") not in safe_port),  # HOLD 우선
            bool(not is_valid),  # 0점 종목 유배
            float(-golden_power_rank),  # 메인 랭킹
            float(-(supply_intra + intra_acc)),  # 수급 합계 동점 처리
            float(-trading_val),  # 거래대금 동점 처리
        )

    try:
        results_sorted = sorted(results, key=golden_sort_key)
        import logging

        log = (
            logger
            if logger is not None
            else logging.getLogger("candidate_selection")
        )

        for x in results_sorted:
            if not x or not isinstance(x, dict):
                continue
            ticker = x.get("ticker", "000000")
            hold_tag = "[HOLD]" if ticker in safe_port else "[NEW]"
            metrics = x.get("_sort_metrics", {})
            log.info(
                f"[{mode.upper()} Sort] {hold_tag} {ticker} | "
                f"GoldenRank: {metrics.get('golden_rank', 0):.1f} | "
                f"Surge: {metrics.get('surge_rate', 0):.1f}% | "
                f"Supply: {metrics.get('supply_intra', 0):.1f} | "
                f"Acc: {x.get('intra_acc', 0.0):.1f} | "
                f"PB: {x.get('pb_quality', 0.0):.1f}"
            )
        return results_sorted
    except Exception as e:
        import logging
        logging.error(f"Critical error in rank_trading_candidates ({mode}): {e}")
        return results
