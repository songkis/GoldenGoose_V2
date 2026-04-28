# -*- coding: utf-8 -*-
import datetime as dt
from datetime import timedelta, time as dt_time
from PyQt5.QtCore import QTime
from config.system_params import KRX_HOLIDAYS

def kst_now():
    return (dt.datetime.utcnow() + timedelta(hours=9)).strftime("%Y-%m-%d %H:%M:%S")

def get_lookback_start_time(days=2):
    """N거래일 전 09:00:00 타임스탬프를 정확히 계산 (주말/공휴일/데이터 결손일 제외)"""
    from .db_utils import get_db_connection
    import logging
    logger = logging.getLogger(__name__)

    current_time = dt.datetime.now()
    # 테스트 환경(휴장일)에서도 작동하도록 오늘이 휴장일이면 어제부터 역산 시작
    temp_date = current_time.date()
    # 만약 현재가 09:00 이전이면 오늘을 제외하고 어제부터 카운트 (데이터 연속성 확보)
    if current_time.hour < 9:
        temp_date -= timedelta(days=1)

    trading_days_found = 0
    max_lookback = 30  # 최대 30일까지 역산 (무한 루프 방지)
    lookback_count = 0

    while trading_days_found < days and lookback_count < max_lookback:
        is_trading_day = False
        # 1차 필터: 주말 및 공휴일 제외
        if (
            temp_date.weekday() < 5
            and temp_date.strftime("%Y-%m-%d") not in KRX_HOLIDAYS
        ):
            # 2차 필터: 실제 데이터 존재 여부 확인 (Zero-Defect)
            try:
                with get_db_connection() as conn:
                    cursor = conn.cursor()
                    # 해당 날짜의 데이터가 최소 1건이라도 있는지 확인 (시장 전체 대상)
                    target_date_str = temp_date.strftime("%Y-%m-%d")
                    cursor.execute(
                        "SELECT 1 FROM 분별주가 WHERE 등록일자 = ? LIMIT 1",
                        (target_date_str,),
                    )
                    if cursor.fetchone():
                        is_trading_day = True
            except Exception as e:
                logger.error(f"get_lookback_start_time 데이터 확인 중 오류: {e}")
                # DB 오류 시에는 기존 방식(휴장일 리스트)에 의존
                is_trading_day = True

        if is_trading_day:
            trading_days_found += 1

        if trading_days_found < days:
            temp_date -= timedelta(days=1)
            lookback_count += 1

    # 최종 산출된 n일 전 날짜의 09:00:00 반환
    return dt.datetime.combine(temp_date, dt_time(9, 0, 0))

def isOverCloseTime():
    result = False
    current_time = QTime.currentTime()
    end_time = QTime(15, 30, 1)  # 15:30 설정
    if current_time >= end_time:
        result = True
    return result

def isBeforeOpenTime():
    result = False
    current_time = QTime.currentTime()
    end_time = QTime(9, 1, 59)
    if current_time < end_time:
        result = True
    return result

def isOverBackupTime():
    result = False
    current_time = QTime.currentTime()
    end_time = QTime(18, 0)  # 15:30 설정
    if current_time >= end_time:
        result = True
    return result

def get_most_recent_trading_day_start(today=None, hour=12, minute=0):
    """KRX 휴장일을 고려하여 '직전 영업일 12:00' 타임스탬프 산출"""
    if today is None:
        today = dt.datetime.now()

    # 1. 일단 어제로 설정
    current = today - timedelta(days=1)

    while True:
        # 주말 제외
        if current.weekday() >= 5:
            current -= timedelta(days=1)
            continue

        # 2. 공휴일 체크
        date_str = current.strftime("%Y-%m-%d")
        if date_str in KRX_HOLIDAYS:
            current -= timedelta(days=1)
            continue

        # 3. 영업일 확정
        return current.replace(hour=hour, minute=minute, second=0, microsecond=0)

def get_trading_day_offset():
    now = dt.datetime.now()
    weekday = now.weekday()  # 월요일=0, 화요일=1, ..., 일요일=6

    # 오늘이 월요일이면 3일 전(금요일), 아니면 1일 전
    if weekday == 0:
        offset = "-3 days"
    # 일요일이면 2일 전(금요일), 토요일이면 1일 전(금요일) 등으로 확장 가능
    elif weekday == 6:
        offset = "-2 days"
    else:
        offset = "-1 days"

    return offset
