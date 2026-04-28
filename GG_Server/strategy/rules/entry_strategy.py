# -*- coding: utf-8 -*-
"""
매수 전략 모듈 (Entry Strategy)

TradingComm.py에서 분리된 매수 조건 및 전략 함수들.
일봉/분봉 매수 조건, 트렌드 조건, 실행 트리거 등을 제공합니다.
"""

from typing import Tuple

logger = None


def set_logger(external_logger):
    """외부 로거 설정"""
    global logger
    logger = external_logger



__all__ = [
    "check_order_flow_imbalance",
]


def check_order_flow_imbalance(
    bid_rem1: int, offer_rem1: int, threshold: float = 1.5
) -> Tuple[bool, float]:
    """
    [Order Flow Imbalance Strategy - Breakout Logic (Korean Market)]
    Checks for upward momentum based on 1st Order Book Remainder.
    In the Korean market, strong upside breakouts often occur when
    Sell Limit Orders (offer_rem) > Buy Limit Orders (bid_rem).

    Args:
        bid_rem1: Buy Limit Order Remainder at Level 1 (매수1호가잔량)
        offer_rem1: Sell Limit Order Remainder at Level 1 (매도1호가잔량)
        threshold: Ratio threshold (Default 1.5 - Offer is 1.5x Bid)

    Returns:
        (bool, float): (Is Imbalance, Ratio)
    """
    try:
        # 방어 로직 (0으로 나누기 방지 및 기본 매수세 꼬임 방지)
        if bid_rem1 <= 0:
            if offer_rem1 > 0:
                # 매도 물량만 있고 매수 물량이 0이면 시장가 매수(체결)로 인해 호가창에 공백이 생겼을 수 있음.
                # 이 경우 극단적인 비율(강한 돌파 가능성)로 취급하여 일단 통과 (최소 점수는 이미 확보된 상태)
                return True, 99.0
            return False, 0.0

        # 1. Imbalance Ratio Calculation (Offer / Bid)
        # 돌파 기준이므로, 위에 깔려있는 매도 물량(Offer)이 매수 물량(Bid)보다 많을수록 가산점
        imbalance_ratio = float(offer_rem1) / float(bid_rem1)

        # 2. Logic: Ratio >= Threshold
        if imbalance_ratio >= threshold:
            return True, imbalance_ratio

        return False, imbalance_ratio

    except Exception as e:
        if logger:
            logger.error(f"[check_order_flow_imbalance Error] {e}")
        return False, 0.0
