# -*- coding: utf-8 -*-
"""
동적 임계값 모듈 (Dynamic Thresholds)

TradingComm.py에서 분리된 동적 임계값 계산 함수들.
시장 상황에 따라 RSI, 거래량, EMA, 손절 등의 임계값을 동적으로 조정합니다.
"""

import numpy as np



logger = None


def set_logger(external_logger):
    """외부 로거 설정"""
    global logger
    logger = external_logger


__all__ = [
    "get_market_volatility",
]


def get_market_volatility(current_index_change, volume_rate):
    """시장 변동성(지수 등락률, 거래량 변화 등) 종합 스코어 계산"""
    return abs(current_index_change) * 0.7 + abs(volume_rate) * 0.3


# [Phase 2] 동적 임계치(Dynamic Thresholds) 예외 처리 및 안전장치 (Failsafe)
def clip_threshold(value: float, min_val: float, max_val: float) -> float:
    """임계값이 극단적인 범위를 벗어나지 않도록 클리핑 (Safety Boundary)"""
    return float(np.clip(value, min_val, max_val))




