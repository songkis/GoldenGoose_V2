import numpy as np
import pandas as pd
import ta
from ta.momentum import RSIIndicator
from ta.volatility import AverageTrueRange, BollingerBands
from ta.volume import OnBalanceVolumeIndicator

# Pandas FutureWarning silencing
try:
    pd.set_option("future.no_silent_downcasting", True)
except Exception:
    pass

logger = None  # 초기값


def set_logger(external_logger):
    global logger
    logger = external_logger


# ====== 시장 상태 판별 ======
def detect_market_regime(data, lookback=20):
    """시장 상태를 판별 (상승/하락/보합)"""
    if len(data) < lookback:
        return "neutral"

    # ADX로 트렌드 강도 측정
    adx = ta.trend.ADXIndicator(data["고가"], data["저가"], data["종가"], window=14)
    adx_value = adx.adx().iloc[-1] if len(adx.adx()) > 0 else 20

    # 가격 기울기
    price_slope = (
        (data["종가"].iloc[-1] - data["종가"].iloc[-lookback])
        / data["종가"].iloc[-lookback]
        * 100
    )

    # 이동평균선 배열
    sma20 = (
        data["종가"].rolling(20).mean().iloc[-1]
        if len(data) >= 20
        else data["종가"].mean()
    )

    # 시장 상태 판별
    if adx_value > 25:  # 강한 트렌드
        if price_slope > 2 and data["종가"].iloc[-1] > sma20:
            return "bullish"
        elif price_slope < -2 and data["종가"].iloc[-1] < sma20:
            return "bearish"

    return "neutral"  # 보합/횡보


# ====== 스윙 포인트 탐지 ======
def detect_swing_points(data, window=5):
    """스윙 하이/로우 포인트 탐지"""
    highs = data["고가"].rolling(window=window * 2 + 1, center=True).max()
    lows = data["저가"].rolling(window=window * 2 + 1, center=True).min()

    swing_high = data["고가"] == highs
    swing_low = data["저가"] == lows

    return swing_high, swing_low


# ====== 지지/저항 레벨 계산 ======
def calculate_support_resistance(data, lookback=50):
    """동적 지지/저항 레벨 계산"""
    if len(data) < lookback:
        return data["종가"].iloc[-1], data["종가"].iloc[-1]

    recent_data = data.tail(lookback)
    swing_high, swing_low = detect_swing_points(recent_data)

    # 최근 스윙 포인트 기반 지지/저항
    resistance_points = (
        recent_data.loc[swing_high, "고가"]
        if swing_high.any()
        else pd.Series([recent_data["고가"].max()])
    )
    support_points = (
        recent_data.loc[swing_low, "저가"]
        if swing_low.any()
        else pd.Series([recent_data["저가"].min()])
    )

    resistance = (
        resistance_points.mean()
        if len(resistance_points) > 0
        else recent_data["고가"].max()
    )
    support = (
        support_points.mean() if len(support_points) > 0 else recent_data["저가"].min()
    )

    return support, resistance


# ====== 멀티 타임프레임 분석 ======
def multi_timeframe_score(data, current_price):
    """여러 시간대 분석을 통한 종합 점수"""
    score = 0

    # 단기 (5-10)
    if len(data) >= 10:
        ema5 = data["종가"].ewm(span=5).mean().iloc[-1]
        ema10 = data["종가"].ewm(span=10).mean().iloc[-1]
        if current_price > ema5 > ema10:
            score += 2
        elif current_price < ema5 < ema10:
            score -= 2

    # 중기 (20-50)
    if len(data) >= 50:
        sma20 = data["종가"].rolling(20).mean().iloc[-1]
        sma50 = data["종가"].rolling(50).mean().iloc[-1]
        if current_price > sma20 > sma50:
            score += 3
        elif current_price < sma20 < sma50:
            score -= 3

    # 장기 (100-200)
    if len(data) >= 200:
        sma100 = data["종가"].rolling(100).mean().iloc[-1]
        sma200 = data["종가"].rolling(200).mean().iloc[-1]
        if current_price > sma100 > sma200:
            score += 1
        elif current_price < sma100 < sma200:
            score -= 1

    return score


def generate_daily_swt_technical_indicators(data):
    win_len = len(data)

    # 기본 이동평균
    for ma_period in [5, 10, 20, 50]:
        if win_len >= ma_period:
            data[f"SMA_{ma_period}"] = data["종가"].rolling(ma_period).mean()
            data[f"EMA_{ma_period}"] = data["종가"].ewm(span=ma_period).mean()

    # RSI (다중 기간)
    for rsi_period in [7, 14, 21]:
        if win_len >= rsi_period:
            data[f"RSI_{rsi_period}"] = RSIIndicator(
                data["종가"], window=rsi_period
            ).rsi()

    # MACD
    if win_len >= 26:
        macd = ta.trend.MACD(data["종가"])
        data["MACD"] = macd.macd()
        data["MACD_Signal"] = macd.macd_signal()
        data["MACD_Hist"] = macd.macd_diff()

    # Bollinger Bands
    if win_len >= 20:
        bb = BollingerBands(data["종가"], window=20, window_dev=2)
        data["BB_Upper"] = bb.bollinger_hband()
        data["BB_Lower"] = bb.bollinger_lband()
        data["BB_Width"] = bb.bollinger_wband()
        data["BB_%B"] = bb.bollinger_pband()

    # ATR (변동성)
    if win_len >= 14:
        atr = AverageTrueRange(data["고가"], data["저가"], data["종가"], window=14)
        data["ATR"] = atr.average_true_range()
        data["ATR_Percent"] = (data["ATR"] / data["종가"]) * 100

    # Volume 지표
    data["Volume_SMA"] = (
        data["거래량"].rolling(20).mean() if win_len >= 20 else data["거래량"].mean()
    )
    data["Volume_Ratio"] = data["거래량"] / data["Volume_SMA"]

    # [Task 1] Volume Dry-Up (VCP) 지표를 위한 거래량 데이터 추가
    data["Volume_3D_Avg"] = data["거래량"].rolling(3, min_periods=1).mean()
    data["Volume_20D_Avg"] = data["거래량"].rolling(20, min_periods=1).mean()
    data["Volume_20D_Std"] = data["거래량"].rolling(20, min_periods=1).std()

    # OBV
    obv = OnBalanceVolumeIndicator(data["종가"], data["거래량"])
    data["OBV"] = obv.on_balance_volume()
    data["OBV_SMA"] = data["OBV"].rolling(10).mean() if win_len >= 10 else data["OBV"]

    # 스윙 포인트
    if win_len >= 10:
        swing_high, swing_low = detect_swing_points(data)
        data["Swing_High"] = swing_high.astype(int)
        data["Swing_Low"] = swing_low.astype(int)

    # 지지/저항 레벨
    support, resistance = calculate_support_resistance(data)
    data["Support"] = support
    data["Resistance"] = resistance

    # 시장 상태
    market_regime = detect_market_regime(data)
    data["Market_Regime"] = market_regime

    # 스윙 트레이딩 점수 계산
    data["Swing_Score"] = calculate_swing_trading_score(data)

    # Price & Volume Rate
    data["Price_Rate"] = data["종가"].pct_change(3)
    data["Volume_Rate"] = data["거래량"].pct_change(3)

    # 이상치 처리
    data = data.replace([np.inf, -np.inf], np.nan)
    data = data.ffill().fillna(0).infer_objects(copy=False)

    return data

def calculate_swing_trading_score(data):
    """종합적인 스윙 트레이딩 점수 계산"""
    score = 0
    last_idx = -1

    # 1. 가격 위치 점수 (지지/저항 대비)
    if "Support" in data.columns and "Resistance" in data.columns:
        current_price = data["종가"].iloc[last_idx]
        support = data["Support"].iloc[last_idx]
        resistance = data["Resistance"].iloc[last_idx]

        price_range = resistance - support if resistance > support else 1
        price_position = (
            (current_price - support) / price_range if price_range > 0 else 0.5
        )

        # 지지선 근처에서 매수 신호
        if price_position < 0.3:
            score += 3
        # 저항선 근처에서 매도 신호
        elif price_position > 0.7:
            score -= 3

    # 2. RSI 다이버전스
    if "RSI_14" in data.columns and len(data) >= 14:
        rsi_current = data["RSI_14"].iloc[last_idx]

        # 과매도 구간
        if rsi_current < 30:
            score += 4
        # 과매수 구간
        elif rsi_current > 70:
            score -= 4
        # 중립 구간에서 방향성
        elif 40 < rsi_current < 60:
            rsi_prev = (
                data["RSI_14"].iloc[last_idx - 1] if len(data) > 1 else rsi_current
            )
            if rsi_current > rsi_prev:
                score += 1
            else:
                score -= 1

    # 3. MACD 신호
    if "MACD_Hist" in data.columns and len(data) >= 26:
        macd_hist = data["MACD_Hist"].iloc[last_idx]
        macd_hist_prev = data["MACD_Hist"].iloc[last_idx - 1] if len(data) > 1 else 0

        # MACD 히스토그램 방향 전환
        if macd_hist > 0 and macd_hist_prev <= 0:
            score += 3  # 골든 크로스
        elif macd_hist < 0 and macd_hist_prev >= 0:
            score -= 3  # 데드 크로스

    # 4. 볼륨 확인
    if "Volume_Ratio" in data.columns:
        vol_ratio = data["Volume_Ratio"].iloc[last_idx]
        price_change = (
            data["Price_Rate"].iloc[last_idx] if "Price_Rate" in data.columns else 0
        )

        # 가격 상승 + 거래량 증가
        if price_change > 0 and vol_ratio > 1.2:
            score += 2
        # 가격 하락 + 거래량 증가 (매도 압력)
        elif price_change < 0 and vol_ratio > 1.2:
            score -= 2

    # 5. 볼린저 밴드 위치
    if "BB_%B" in data.columns:
        bb_percent = data["BB_%B"].iloc[last_idx]

        if bb_percent < 0.2:  # 하단 밴드 근처
            score += 2
        elif bb_percent > 0.8:  # 상단 밴드 근처
            score -= 2

    # 6. 시장 상태별 가중치
    if "Market_Regime" in data.columns:
        regime = data["Market_Regime"].iloc[last_idx]

        if regime == "bullish":
            score = score * 1.2  # 상승장에서는 매수 신호 강화
        elif regime == "bearish":
            score = score * 0.8  # 하락장에서는 신호 약화

    # 7. 멀티 타임프레임 확인
    if len(data) >= 50:
        mtf_score = multi_timeframe_score(data, data["종가"].iloc[last_idx])
        score += mtf_score

    # 점수 정규화 (-10 ~ +10)
    return np.clip(score, -10, 10)

