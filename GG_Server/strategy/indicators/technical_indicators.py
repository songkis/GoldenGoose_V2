# -*- coding: utf-8 -*-
"""
기술 지표 계산 모듈 (Technical Indicators)

TradingComm.py에서 분리된 기술 지표 계산 함수들.
RSI, MACD, ATR, 볼린저밴드, 추세 점수 등 기본 기술적 분석 지표를 제공합니다.
"""

import numpy as np
import pandas as pd

logger = None


def set_logger(external_logger):
    """외부 로거 설정"""
    global logger
    logger = external_logger


__all__ = [
    "calculate_atr_5m",
    "calculate_intraday_acceleration_v5_6",
    "calculate_stock_beta",
]


def calculate_stock_beta(stock_returns, market_returns, window=20):
    """
    [Phase 1] Market Sensitivity Beta 산출
    시장 지수 수익률과 종목 수익률의 공분산을 통해 변동성 상관관계 도출
    """
    try:
        # 동기화를 위해 인덱스 기준 교집합 추출
        common_idx = stock_returns.index.intersection(market_returns.index)
        if len(common_idx) < window:
            return 1.0

        s = stock_returns.loc[common_idx].tail(window)
        m = market_returns.loc[common_idx].tail(window)

        if len(s) < window or len(m) < window:
            return 1.0

        covariance = np.cov(s, m)[0][1]
        variance = np.var(m)

        beta = covariance / (variance + 1e-9)
        # 극단적 값 방어 (0.5 ~ 3.0 사이로 클리핑)
        return float(np.clip(beta, 0.5, 3.0))
    except Exception:
        return 1.0



def calculate_atr_5m(minute_df, window=14):
    """
    [Internal Audit: ATR Calculator V1.0]
    - TR(True Range) 계산 후 지수이동평균(EMA) 또는 단순평균(SMA) 적용
    """
    # [V8.2 Fix] EWM Memory/Latency Optimization
    try:
        if minute_df is None or len(minute_df) < window:
            return 0.0

        # 윈도우의 3배수만큼만 잘라서 연산 (데이터 폭증 시 병목 차단)
        df_slice = minute_df.tail(window * 3)

        high = df_slice["고가"]
        low = df_slice["저가"]
        close = df_slice["종가"]
        prev_close = close.shift(1)

        tr = pd.concat(
            [high - low, (high - prev_close).abs(), (low - prev_close).abs()], axis=1
        ).max(axis=1)

        atr = tr.ewm(span=window, adjust=False).mean().iloc[-1]
        return float(atr)
    except Exception:
        return 0.0


def calculate_intraday_acceleration_v5_6(df, interval_min=3):
    """
    [V5.7] 분봉 가속도 - 연속적 가중치 함수 및 최소 거래대금 보정 적용
    """
    try:
        long_win = max(15, int(60 / interval_min))
        if len(df) < long_win:
            return 0.0

        ema_short = df["종가"].ewm(span=5, adjust=False).mean()
        ema_long = df["종가"].ewm(span=15, adjust=False).mean()
        disparity = (ema_short / ema_long) - 1.0
        velocity_delta = disparity.iloc[-1] - disparity.iloc[-2]

        # [Refinement] 연속적 가중치 함수: 선형 보간(Linear Interpolation) 적용
        curr_cnt = df["순체결건수"].iloc[-1]
        avg_cnt = df["순체결건수"].rolling(10).mean().iloc[-1]
        cnt_acc_ratio = np.clip(curr_cnt / (avg_cnt + 1e-9), 0.5, 2.5)
        execution_multiplier = np.interp(
            cnt_acc_ratio, [0.5, 1.5, 2.5], [0.8, 1.0, 1.3]
        )

        # [Refinement] 수급 농도 보정: 최소 거래대금 허들 (3분 합산 5천만원 기준)
        recent_amt = (df["거래량"] * df["종가"]).tail(3).mean()
        amt_filter = np.interp(recent_amt, [1e7, 5e7], [0.7, 1.0])

        vol_ratio = df["거래량"].tail(3).mean() / (df["거래량"].tail(15).mean() + 1e-9)

        if disparity.iloc[-1] > 0:
            base_score = 30 + (disparity.iloc[-1] * 1500)
            acc_bonus = (
                (velocity_delta * 4000)
                if velocity_delta > 0
                else (velocity_delta * 1500)
            )
            score = (base_score + acc_bonus) * execution_multiplier * amt_filter
            score *= np.interp(vol_ratio, [1.0, 2.0, 4.0], [1.0, 1.15, 1.3])
        else:
            score = (
                15.0 * execution_multiplier * amt_filter if velocity_delta > 0 else 0.0
            )

        return round(float(np.clip(score, 0, 100)), 2)
    except Exception as e:
        if logger:
            logger.error(f"Intraday Acc Engine Error: {e}")
        return 0.0
