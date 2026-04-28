import pandas as pd
import datetime
import logging

logger = logging.getLogger(__name__)


def process_limit_up_from_df(df):
    """
    [V5.4] 상한가 품질 분석 및 일반 종목 바이패스 로직 통합
    """
    if df is None or len(df) == 0:
        return {"can_buy": True, "reason": "데이터 없음 (Bypass)"}

    # 1. 당일 데이터 필터링
    today_str = datetime.datetime.now().strftime("%Y-%m-%d")
    today_df = df[df["등록일자"] == today_str].copy()

    if today_df.empty:
        # 데이터가 없는 장 초반 등에는 매수를 막지 않고 일반 로직에 맡김
        return {
            "can_buy": True,
            "is_limit_up_entry": False,
            "reason": "당일 데이터 없음 (Bypass)",
        }

    # 2. 상한가 기록 확인
    limit_rows = today_df[today_df["전일대비구분"] == 1]

    # [중요] 상한가 기록이 없다면 일반 종목임 -> can_buy를 True로 반환하여 일반 매수 로직 허용
    if limit_rows.empty:
        return {
            "can_buy": True,
            "is_limit_up_entry": False,
            "is_currently_at_limit": False,
            "reason": "상한가 미달 (일반 종목 Bypass)",
        }

    # 3. 상한가 정보 추출
    limit_price = limit_rows["종가"].iloc[0]
    first_lock_time = limit_rows["시간"].iloc[0]

    # [추가 부분]
    current_price = today_df["종가"].iloc[-1]
    # 현재가가 상한가와 같은지 여부 판단 (True/False)
    is_currently_at_limit = current_price >= limit_price

    # 4. 상한가 안착 이후의 데이터셋 및 잔량 계산
    post_lock_df = today_df[today_df.index >= limit_rows.index[0]]
    current_bid_remains = (
        post_lock_df["매수잔량"].iloc[-1] if "매수잔량" in post_lock_df else 0
    )
    peak_bid_remains = (
        post_lock_df["매수잔량"].max() if "매수잔량" in post_lock_df else 0
    )

    is_drain_risk, drain_ttz, drain_msg = check_limit_up_drain_risk(
        post_lock_df, current_bid_remains, peak_bid_remains
    )
    # if is_drain_risk:
    #    execute_sell_order(ticker, "상한가 붕괴 전 선제 매도")

    # [Vectorized Optimization] 상한가 안착 후 풀림 횟수 계산 (O(N) -> O(1) Vectorized)
    is_at_limit = today_df["종가"] >= limit_price
    has_been_locked = is_at_limit.cummax()  # 한 번이라도 도달한 적이 있는가
    is_broken = (today_df["종가"] < limit_price) & has_been_locked
    break_count = (is_broken & ~is_broken.shift(1, fill_value=False)).sum()

    # 6. 상따 승인 점수 계산
    # 시간 점수
    if first_lock_time <= "100000":
        time_score = 60
    elif first_lock_time <= "120000":
        time_score = 40
    else:
        time_score = 20

    # 잔량 강도
    total_vol = today_df["거래량"].sum()
    vol_score = min(40, (current_bid_remains / total_vol * 100)) if total_vol > 0 else 0
    stability_penalty = break_count * 15
    total_score = time_score + vol_score - stability_penalty

    # 7. 최종 결과 (상한가 종목 전용 필터링)
    # 상한가에 도달했던 종목이라면: (점수가 높거나) OR (현재 상한가에 딱 붙어있거나)
    # 하지만 상한가가 풀려서 내려오는 중(is_currently_at_limit=False)이라면 can_buy는 False가 됨
    can_buy_limit = (total_score >= 70) or is_currently_at_limit

    return {
        "can_buy": can_buy_limit,
        "limit_up_price": limit_price,
        "bid_remains": current_bid_remains,
        "peak_bid_remains": peak_bid_remains,
        "arrival_time": first_lock_time,
        "is_limit_up_entry": True,
        "is_drain_risk": is_drain_risk,
        "drain_ttz": drain_ttz,
        "drain_msg": drain_msg,
        "is_currently_at_limit": is_currently_at_limit,  # 현재 안착 여부
        "break_count": break_count,
        "limit_up_score": total_score,
        "reason": f"LimitUp-Mode(Score:{total_score:.1f}, Locked:{is_currently_at_limit})",
    }


def check_limit_up_drain_risk(post_lock_df, current_bid_remains, peak_bid_remains):
    """
    [V5.5] 상한가 잔량 소모 속도 분석을 통한 선제 탈출 로직 (Consolidated from TradingComm)
    """
    if len(post_lock_df) < 3:
        return False, 0.0, "데이터 부족"

    recent_remains = (
        post_lock_df["매수잔량"].tail(3).values
        if "매수잔량" in post_lock_df.columns
        else [0, 0, 0]
    )
    drain_per_min = recent_remains[0] - recent_remains[-1]
    drain_speed_per_sec = max(0, drain_per_min / 120)
    ttz = 9999
    if drain_speed_per_sec > 0:
        ttz = current_bid_remains / drain_speed_per_sec

    health_ratio = (
        (current_bid_remains / peak_bid_remains) * 100 if peak_bid_remains > 0 else 100
    )
    is_high_risk = False
    risk_msg = "안전"

    if health_ratio < 15.0:
        is_high_risk = True
        risk_msg = f"위험: 잔량 급감 ({health_ratio:.1f}%)"
    elif ttz < 30:
        is_high_risk = True
        risk_msg = f"위험: 소모 속도 과다 (TTZ: {ttz:.1f}s)"

    return is_high_risk, ttz, risk_msg


