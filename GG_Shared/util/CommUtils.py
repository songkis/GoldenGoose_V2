# -*- coding: utf-8 -*-
import inspect
import os
import concurrent.futures
import math
import numpy as np
import pandas as pd
import threading
import logging

from PyQt5.QtCore import QTime
from config.system_params import KRX_HOLIDAYS
import datetime as dt
from datetime import timedelta, time as dt_time


# 1. 암호화
from cryptography.fernet import Fernet


import contextlib

# print "This is line 7, python says line ", get_linenumber()
import sqlite3
from queue import Empty, Full, Queue

import struct  # struct 라이브러리를 임포트해야 합니다.

from typing import Any, Optional, Tuple


GLOBAL_THREAD_POOL = concurrent.futures.ThreadPoolExecutor(
    max_workers=max(4, (os.cpu_count() or 4) * 2)
)


MINUTE_DF_CACHE = {}
MINUTE_DF_CACHE_LOCK = threading.Lock()

logger = logging.getLogger(__name__)  # 기본 로거 초기화


def deep_get(obj, key, default=None):
    """
    [Zero-Defect Utility] 중첩된 딕셔너리 또는 객체에서 안전하게 값을 추출합니다.
    """
    if obj is None:
        return default

    if isinstance(obj, dict):
        return obj.get(key, default)

    return getattr(obj, key, default)


def set_commutils_logger(external_logger):
    global logger
    logger = external_logger


# 비밀 키 생성 (한 번만 생성하면 됨)
def generate_key():
    return Fernet.generate_key()


# 비밀 키 저장 (반드시 안전하게 저장해야 함)
SECRET_KEY = b"mFt09RHW8pQ7s3tRDrvh7fdwXPCbyYvNbXUtXCWtZqk="  # generate_key()
# Fernet 객체 생성
cipher_suite = Fernet(SECRET_KEY)


def encrypt_data(data):
    """암호화 함수"""
    if data:
        return cipher_suite.encrypt(data.encode()).decode()
    return data


def decrypt_data(data):
    """복호화 함수"""
    if data:
        return cipher_suite.decrypt(data.encode()).decode()
    return data


def get_lookback_start_time(days=2):
    """N거래일 전 09:00:00 타임스탬프를 정확히 계산 (주말/공휴일/데이터 결손일 제외)"""
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


def is_encrypted(data):
    """암호화된 데이터인지 확인"""
    try:
        # Fernet 암호화된 데이터는 32 바이트 길이의 키와 16바이트의 IV와 같은 형태로 44글자 길이를 가짐
        # 일반적으로 'b'로 시작하고, Base64로 인코딩된 문자열로 끝납니다.
        # Base64로 인코딩된 암호화된 데이터는 44문자로 이루어져 있습니다.
        # 여기서 b'...'로 시작하는 문자열을 체크
        if isinstance(data, str) and data.startswith("gAAAAA"):
            return True
        return False
    except Exception:
        return False


#
# def enable_wal_mode():
#    from config.comm_settings import DATABASE
#
#    """DB에 WAL 모드를 활성화 (한 번만 실행)"""
#    with sqlite3.connect(DATABASE) as conn:
#        conn.execute("PRAGMA journal_mode=WAL;")  # WAL 모드 활성화
#        conn.create_function("kst_now", 0, kst_now)
#
#
## WAL 모드는 최초 1회만 실행
# enable_wal_mode()


def kst_now():
    return (dt.datetime.utcnow() + timedelta(hours=9)).strftime("%Y-%m-%d %H:%M:%S")


# 2. DB connection
class ConnectionPool:
    """SQLite 연결 풀 클래스 - 성능 개선을 위한 연결 재사용 (Thread-safe)"""

    def __init__(self, max_connections=10, database=None):
        from config.comm_settings import DATABASE

        self.database = database or DATABASE
        self.max_connections = max_connections
        self._pool = Queue(maxsize=max_connections)
        self._lock = threading.Lock()
        self._created = 0
        self._total_requests = 0
        self._pool_hits = 0

    def _create_new_connection(self):
        """실제 SQLite 연결 생성 및 동적 설정 적용"""
        try:
            # [Institutional-Grade] Concurrency Settings
            # busy_timeout: 30초 대기 (Database is locked 방지)
            # check_same_thread=False: 멀티스레드 환경 필수
            conn = sqlite3.connect(
                self.database,
                timeout=30.0,
                isolation_level=None,
                check_same_thread=False,
            )
            conn.row_factory = sqlite3.Row

            # WAL 모드 적용 (동시 읽기/쓰기 성능 극대화)
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA busy_timeout = 30000;")

            # 커스텀 함수 등록
            conn.create_function("kst_now", 0, kst_now)

            return conn
        except Exception as e:
            logger.error(f"연결 생성 실패: {e}")
            raise

    def get_connection(self):
        """연결 풀에서 연결 가져오기"""
        self._total_requests += 1

        # 1. 풀에 연결이 있으면 재사용
        try:
            conn = self._pool.get_nowait()
            self._pool_hits += 1
            # 연결 유효성 검사
            try:
                conn.execute("SELECT 1")
                return conn
            except Exception:
                try:
                    conn.close()
                except Exception:
                    pass
                with self._lock:
                    self._created -= 1
        except Empty:
            pass

        # 2. 아직 최대 연결 수에 도달하지 않았으면 새로 생성
        with self._lock:
            if self._created < self.max_connections:
                conn = self._create_new_connection()
                self._created += 1
                return conn

        # 3. 풀이 가득 찬 경우 대기 (최대 10초)
        try:
            conn = self._pool.get(timeout=10.0)
            try:
                conn.execute("SELECT 1")
                return conn
            except Exception:
                try:
                    conn.close()
                except Exception:
                    pass
                return (
                    self._create_new_connection()
                )  # 예외적으로 신규 생성해서 반환 (카운트 유지)
        except Empty:
            # 여전히 가득 찬 경우 임시 연결 생성 (풀 카운트 외부)
            logger.warning("연결 풀 고갈 - 임시 연결 생성")
            return self._create_new_connection()

    def return_connection(self, conn):
        """연결을 풀에 반환"""
        if not conn:
            return

        try:
            # 유효성 검사 후 풀에 삽입 시도
            conn.execute("SELECT 1")
            try:
                self._pool.put_nowait(conn)
            except Full:
                # 풀이 가득 찬 경우 (임시 연결이 유입되었을 가능성)
                conn.close()
        except Exception:
            # 불량 연결 폐기
            try:
                conn.close()
            except Exception:
                pass
            with self._lock:
                # 풀 내부의 정규 연결인 경우에만 카운트 감소 (추측)
                # 엄밀히 관리하려면 conn에 할당 시점 정보를 담아야 하나,
                # 여기서는 안전하게 _created가 0보다 클 때만 감소시킴
                if self._created > 0:
                    self._created -= 1

    def get_stats(self):
        """연결 풀 통계 반환"""
        hit_rate = (
            (self._pool_hits / self._total_requests * 100)
            if self._total_requests > 0
            else 0
        )
        return {
            "created": self._created,
            "pool_size": self._pool.qsize(),
            "total_requests": self._total_requests,
            "pool_hits": self._pool_hits,
            "hit_rate": f"{hit_rate:.2f}%",
        }


# 전역 연결 풀 인스턴스
_db_pool = None
_pool_lock = threading.Lock()


@contextlib.contextmanager
def get_db_connection():
    """DB 연결을 가져오는 스레드 안전 컨텍스트 매니저 (WAL/Timeout 지원)"""
    global _db_pool

    if _db_pool is None:
        with _pool_lock:
            if _db_pool is None:
                _db_pool = ConnectionPool(max_connections=10)

    conn = _db_pool.get_connection()
    try:
        # isolation_level=None (Autocommit) 상태이므로,
        # 명시적 트랜잭션이 필요한 경우 호출자가 BEGIN/COMMIT을 사용해야 합니다.
        yield conn
    except Exception as e:
        logger.error(f"DB 작업 중 오류 발생: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        _db_pool.return_connection(conn)


def get_db_stats():
    """DB 연결 풀 통계 반환"""
    if _db_pool:
        return _db_pool.get_stats()
    return {"status": "Not initialized"}


def close_db_connection(conn):
    """DB 연결을 풀에 반환 (수동 반환용)"""
    if _db_pool and conn:
        _db_pool.return_connection(conn)


def getAIConfVal(key):
    try:
        with get_db_connection() as conn:  # with 문을 사용하여 자동으로 conn.close()
            cursor = conn.cursor()
            cursor.execute(
                """SELECT CONF_ID, CONF_KEY, CONF_VALUE
                            FROM TB_AI_CONF tac WHERE 1=1 --CONF_ID IN ('11','12')
                            ORDER BY tac.CONF_ID """
            )
            rows = cursor.fetchall()
            if rows:
                # CONF_KEY를 Key, CONF_VALUE를 Value로 딕셔너리 생성
                ai_conf_dict = {row[1]: row[2] for row in rows}
            cursor.close()
        return ai_conf_dict[key]
    except Exception as e:
        pass


def set_window_context(window, logger=None):

    with get_db_connection() as conn:  # with 문을 사용하여 자동으로 conn.close()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """SELECT *
                            FROM TB_ACC_INFO tai 
                            WHERE tai.SYS_iD = (SELECT tac.CONF_VALUE 
                            FROM TB_AI_CONF tac 
                            WHERE tac.CONF_ID = '1')"""
            )
            row = cursor.fetchone()
            columns = [desc[0] for desc in cursor.description]  # 컬럼명 리스트 생성
            row_dict = dict(zip(columns, row))  # 튜플을 딕셔너리로 변환

            # 데이터 복호화 처리 (apply 대신 if문 사용)
            for column in ["ACC_NO", "USER_ID", "PWD", "CERT_PWD", "TRX_PWD"]:
                if (
                    column in row_dict and row_dict[column] is not None
                ):  # 값이 존재하는 경우만 처리
                    row_dict[column] = (
                        decrypt_data(row_dict[column])
                        if is_encrypted(row_dict[column])
                        else row_dict[column]
                    )

            if row:
                window.account_dict = {
                    "SYS_ID": row_dict["SYS_ID"],
                    "거래환경": row_dict["SYS_NM"],
                    "접속URL": row_dict["URL"],
                    "계좌명": row_dict["ACC_NM"],
                    "계좌번호": row_dict["ACC_NO"],
                    "사용자ID": row_dict["USER_ID"],
                    "사용자비밀번호": row_dict["PWD"],
                    "인증서비밀번호": row_dict["CERT_PWD"],
                    "거래비밀번호": row_dict["TRX_PWD"],
                }

            cursor.execute(
                """SELECT CONF_ID, CONF_KEY, CONF_VALUE
                            FROM TB_AI_CONF tac
                            ORDER BY tac.CONF_ID """
            )
            rows = cursor.fetchall()
            if rows:
                # CONF_KEY를 Key, CONF_VALUE를 Value로 딕셔너리 생성
                window.ai_conf_dict = {row[1]: row[2] for row in rows}
            cursor.close()
        except Exception as e:
            클래스이름 = window.__class__.__name__
            함수이름 = inspect.currentframe().f_code.co_name
            window.logger.error(
                "%s-%s %s: %s"
                % (클래스이름, 함수이름, inspect.currentframe().f_lineno, e)
            )


# 글로벌 설정 객체 (Zero-Defect Config Injection)
config = None


def setConfig(ext_config):
    """AI 파라미터 설정을 주입하고 DB에서 즉시 로드 (Headless 지원)"""
    global config
    config = ext_config

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # 1. AI 설정 로드
            cursor.execute("SELECT CONF_KEY, CONF_VALUE FROM TB_AI_CONF")
            rows = cursor.fetchall()
            if rows:
                config.ai_conf_dict = {row[0]: row[1] for row in rows}

            # 2. 시스템/계좌 기본 정보 로드 (SYS_ID 등)
            cursor.execute(
                """SELECT SYS_ID, SYS_NM FROM TB_ACC_INFO tai 
                   WHERE tai.SYS_iD = (SELECT tac.CONF_VALUE FROM TB_AI_CONF tac WHERE tac.CONF_ID = '1')"""
            )
            row = cursor.fetchone()
            if row:
                config.account_dict = {"SYS_ID": row[0], "거래환경": row[1]}
            cursor.close()
    except Exception as e:
        if logger:
            logger.error(f"[setConfig Lazy-Load Error] {e}")


def get_linenumber():
    try:
        frame = inspect.currentframe()
        if frame and frame.f_back:
            return frame.f_back.f_lineno
    except Exception:
        pass
    return 0


def get_funcname():
    try:
        frame = inspect.currentframe()
        if frame and frame.f_back:
            return frame.f_back.f_code.co_name
    except Exception:
        pass
    return "unknown"


def isOverCloseTime():
    result = False
    current_time = QTime.currentTime()
    end_time = QTime(15, 30, 1)  # 15:30 설정
    if current_time >= end_time:
        result = True
    # print (f"isOverCloseTime : {result}")
    return result


def isBeforeOpenTime():
    result = False
    current_time = QTime.currentTime()
    end_time = QTime(9, 1, 59)
    if current_time < end_time:
        result = True
    # print (f"isOverCloseTime : {result}")
    return result


def isOverBackupTime():
    result = False
    current_time = QTime.currentTime()
    end_time = QTime(18, 0)  # 15:30 설정
    if current_time >= end_time:
        result = True
    # print (f"isOverBackupTime : {result}")
    return result


def safe_float(val, default=0.0):
    try:
        if val is None or (isinstance(val, str) and val.strip() == ""):
            return default
        return float(val)
    except Exception:
        return default


def to_numeric_safe(value):
    """
    다양한 데이터 타입(숫자, 텍스트, numpy, bytes)을 안전하게 숫자로 변환하는 함수
    """
    # 1. 파이썬 기본 숫자(int, float) 또는 넘파이 숫자
    if isinstance(value, (int, float, np.integer, np.floating)):
        try:
            val = float(value)
            if math.isnan(val) or math.isinf(val):
                return 0.0
            return round(val, 2)
        except (ValueError, TypeError):
            return 0.0

    # 2. 이진 데이터(bytes)
    if isinstance(value, bytes):
        try:
            return float(value.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            try:
                if len(value) == 8:
                    return struct.unpack("<q", value)[0]
            except struct.error:
                return 0.0
        return 0.0

    # 3. 문자열(string)
    if isinstance(value, str):
        try:
            return float(value.replace(",", ""))  # "1,234" 같은 경우 처리
        except ValueError:
            return 0.0

    # 4. 처리 불가능 → 0 반환
    return 0.0


def get_first_tuple(
    df: pd.DataFrame,
    key_col: str,
    key_value: Any,
    target_cols: list[str],
) -> Optional[Tuple[Any, ...]]:
    try:
        subset = df.loc[df[key_col] == key_value, target_cols]
        if subset.empty:
            return None
        return tuple(subset.iloc[0])
    except Exception:
        return None


def transform_to_daily_volume(df: pd.DataFrame) -> pd.DataFrame:
    """
    [V4.6.1 Adaptive Integrity]
    누적거래량 컬럼 존재 시 일일 거래량으로 변환, 미존재 시 기존 거래량 유효성 검사
    """
    # 1. 컬럼 존재 여부 체크 (방어적 설계)
    if "누적거래량" in df.columns:
        # 기존 로직 수행
        df["거래량"] = df["누적거래량"].diff()

        # 첫 번째 행 처리
        if len(df) > 0:
            df.loc[df.index[0], "거래량"] = df.iloc[0]["누적거래량"]

        # [Critical] 음수/리셋 구간 처리 - 승률 방어의 핵심
        # 누적이 전일보다 적다면, 데이터 리셋으로 판단하여 당일 수치 채택
        reset_mask = df["거래량"] <= 0
        df.loc[reset_mask, "거래량"] = df.loc[reset_mask, "누적거래량"]

        # 로그 기록 (데이터 보정 발생 시)
        if reset_mask.any():
            logger.debug(
                f"[Data Integrity] {reset_mask.sum()}개 지점에서 거래량 리셋 보정 완료."
            )

    elif "거래량" in df.columns:
        # 누적거래량은 없지만 거래량 컬럼이 있는 경우, 기존 데이터 그대로 사용 (시스템 연속성)
        pass
    else:
        # 두 컬럼 모두 없을 경우 에러 대신 빈 값 처리하여 시스템 중단 방지
        df["거래량"] = 0.0
        logger.info("[Critical Warning] 거래량 관련 데이터가 전무합니다.")

    return df


def load_data(stock_cd, table_name):
    """DB에서 주가 데이터를 로드하며, 분봉의 경우 최근 영업일 12:00부터 가져오도록 강제함"""
    from SQL.sql import GET_DAY_PRICE_BY_STK_CD, GET_MIN_PRICE_BY_STK_CD

    #  지표 정합성(MA50 등)을 위해 2일치 09:00 데이터부터 로드
    start_time = get_lookback_start_time(days=2)
    start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")

    try:
        with get_db_connection() as conn:
            if table_name == "일별주가":
                query = GET_DAY_PRICE_BY_STK_CD
                params = (stock_cd,)
            elif table_name == "분별주가":
                query = GET_MIN_PRICE_BY_STK_CD
                # (종목코드, 시작시간) 순서로 파마리터 주입
                params = (stock_cd, start_time_str)
            else:
                return pd.DataFrame()

            data = pd.read_sql_query(query, params=params, con=conn)

            if data.empty:
                return data

            # 1. [Pre-processing] 테이블별 특화 전처리
            if table_name == "일별주가":
                # 누적거래량 -> 당일 거래량 변환 (반드시 정렬 전 수행하거나 내부에서 정렬 필요)
                data = transform_to_daily_volume(data)
            elif table_name == "분별주가":
                # [중요] 여기서 등록일자+시간을 '등록일시' Datetime으로 변환
                data = prepare_minute_df(data)

            logger.debug(f"[{stock_cd}] {len(data)} bars loaded for {table_name}")
            # 2. [Data Integrity] 중복 제거 및 정렬
            # 인덱스로 만들기 전에 중복 날짜를 제거하여 유일성 확보

            # 3. [Validation] 필수 컬럼 NaN 제거 (데이터 품질 보장)
            # 지표 계산에 에러를 유발하는 핵심 컬럼 위주로 필터링
            essential_cols = ["종가", "고가", "저가", "거래량"]
            # 컬럼 이름이 한글일 경우를 대비해 존재하는 컬럼만 subset 지정
            existing_cols = [c for c in essential_cols if c in data.columns]
            data = data.dropna(subset=existing_cols)

            #  지표 계산 시 Spike 방지를 위해 결측치 보간 (ffill -> bfill)
            data = data.ffill().bfill().fillna(0)

            return data

    except Exception as e:
        logger.error(f"SQLite load_data error [{stock_cd}]: {e}")
        return pd.DataFrame()


def load_data_incremental(stock_cd, table_name="분별주가"):
    """
    [Phase 7 Incremental Caching]
    기존 load_data를 래핑하여, 분별주가의 경우 마지막으로 읽어온 시간 이후의 신규 봉(1~3개)만 DB에서 조회하고
    기존 메모리에 유지된 DataFrame에 이어붙여 파싱 오버헤드를 제로화합니다.
    """
    if table_name != "분별주가":
        return load_data(stock_cd, table_name)

    from SQL.sql import GET_MIN_PRICE_SINCE_TIME

    with MINUTE_DF_CACHE_LOCK:
        cached_df = MINUTE_DF_CACHE.get(stock_cd)

    # 1. 캐시가 없거나, 비어있거나, 봉수가 부족(예: 15개 미만)하면 최초 전체 로드
    if cached_df is None or cached_df.empty or len(cached_df) < 15:
        df = load_data(stock_cd, table_name)
        if not df.empty:
            with MINUTE_DF_CACHE_LOCK:
                # 메모리 절약을 위해 최근 당일치기 데이터 이상 (약 1000봉) 유지하여 어제 12:00 데이터 보존
                MINUTE_DF_CACHE[stock_cd] = df.tail(1000).copy()
        return df

    # 2. 마지막 인덱스 시간 추출
    last_time_idx = cached_df.index[-1]
    # '등록일시' 인덱스는 pd.Timestamp 형태이므로 sqlite 조회용 "YYYY-MM-DD HH:MM:SS" 문자로 변환
    last_time_str = last_time_idx.strftime("%Y-%m-%d %H:%M:%S")

    # 3. DB에서 신규 증분 데이터만 쿼리 (Lock 밖에서 수행하여 다른 병렬 조회 블록 방지)
    try:
        with get_db_connection() as conn:
            new_data = pd.read_sql_query(
                GET_MIN_PRICE_SINCE_TIME, params=(stock_cd, last_time_str), con=conn
            )

        # 신규 데이터가 없으면 기존 복사본 반환
        if new_data.empty:
            return cached_df.copy()

        # 4. 신규 데이터 전처리 (prepare_minute_df 내부에서 '등록일시' 컬럼 생성됨)
        new_data = prepare_minute_df(new_data)

        # 중복 제거 및 인덱스 설정
        date_col = "등록일시"
        if date_col in new_data.columns:
            new_data = new_data.drop_duplicates(subset=[date_col])
            new_data = new_data.sort_values(by=date_col)
            new_data.set_index(date_col, inplace=True)

        # 필수 컬럼 NaN 제거
        essential_cols = ["종가", "고가", "저가", "거래량"]
        existing_cols = [c for c in essential_cols if c in new_data.columns]
        new_data = new_data.dropna(subset=existing_cols)

        if new_data.empty:
            return cached_df.copy()

        # 5. 기존 DataFrame과 신규 DataFrame 병합 (이어붙이기)
        with MINUTE_DF_CACHE_LOCK:
            # 병합 전 만약을 위해 다시 한 번 가져옵니다 (다른 스레드가 업데이트 했을 수 있음)
            current_cache = MINUTE_DF_CACHE.get(stock_cd, cached_df)
            combined_df = pd.concat([current_cache, new_data])

            # [Zero-Defect Integrity] 인덱스 중복 제거 및 엄격한 정렬 (새 데이터 덮어쓰기)
            # drop_duplicates와 sort_index를 통합하여 인덱스 무결성 보장
            combined_df = combined_df[~combined_df.index.duplicated(keep="last")]
            combined_df.sort_index(inplace=True)

            # 용량 관리 (최근 1000봉 유지)
            MINUTE_DF_CACHE[stock_cd] = combined_df.tail(1000)

            return MINUTE_DF_CACHE[stock_cd].copy()

    except Exception as e:
        logger.error(f"SQLite load_data_incremental error [{stock_cd}]: {e}")
        # 실패 시엔 fallback으로 기존 캐시 반환
        return cached_df.copy()


def get_actual_buying_power(account_info, open_buy_amt=0, reserve_ratio=0.95):
    """
    [Hardening] 자산(Net Asset) 기반 사이징 파워 산출 로직.
    """
    try:
        # 1. 명시적 키 이름 탐색
        net_asset = 0.0
        for k in ["추정순자산", "총자산", "d_sunjasan", "total_asset"]:
            val = to_numeric_safe(account_info.get(k, 0))
            if val >= 50_000_000:  # 5천만원 이상 자산 기준
                net_asset = val
                break

        # 2. 브루트포스 최대값 탐색 (인코딩 깨짐 대비)
        if net_asset == 0:
            vals = [to_numeric_safe(v) for v in account_info.values()]
            if vals:
                net_asset = max(vals)

        d2_cash = to_numeric_safe(account_info.get("추정D2예수금", 0))

        # [Strategy] 순자산에서 미체결 주문과 슬리피지 버퍼(reserve_ratio)를 적용하여 베팅 여력 산출
        available_cash = d2_cash
        buying_power = int(max(0, (net_asset - open_buy_amt) * reserve_ratio))

        # 5.  소액 주문 노이즈 필터링
        # 자산의 0.5% 미만이거나 10만원 미만인 경우 효율성을 위해 0 처리
        if buying_power < max(net_asset * 0.005, 100_000):
            buying_power = 0

        current = dt.datetime.now()
        if current.minute % 10 == 0 and current.second < 10:
            logger.info(
                f" [BuyingPower V9] Asset:{net_asset:,.0f} | D2:{d2_cash:,.0f} | "
                f"Avail_Cash:{available_cash:,.0f} | Result:{buying_power:,.0f}"
            )

        return float(max(0, buying_power))

    except Exception as e:
        logger.error(f"get_actual_buying_power error: {e}")
        return 0.0


def update_capital(capital):
    from SQL.sql import UPDATE_BUYABLE_AMT

    # 투자가능금액 DB저장
    with get_db_connection() as conn:  # with 문을 사용하여 자동으로 conn.close()
        cursor = conn.cursor()
        query = UPDATE_BUYABLE_AMT
        cursor.execute(query, (capital,))
        cursor.close()


def make_json_safe(obj):
    """
    NumPy 타입을 포함한 모든 비표준 객체를 JSON 직렬화 가능 타입으로 변환
    """
    # 1. 기본 타입 처리
    if obj is None:
        return None

    # 2. 딕셔너리/리스트 재귀 처리
    if isinstance(obj, dict):
        return {str(k): make_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [make_json_safe(i) for i in obj]

    # 3. NumPy 객체 판별 (타입 체크 에러 방지용 문자열 비교 방식 포함)
    cls_name = obj.__class__.__name__

    # NumPy 불리언 처리 (가장 빈번한 에러 원인)
    if "bool" in cls_name.lower():
        return bool(obj)

    # NumPy 숫자 처리
    if "float" in cls_name.lower():
        return float(obj)
    if "int" in cls_name.lower():
        return int(obj)

    # NumPy 배열 처리
    if isinstance(obj, np.ndarray):
        return make_json_safe(obj.tolist())

    # 4. 기타 처리
    return obj


def update_dynamic_exit_levels_v2(
    current_price, entry_price, stop_loss, combined_score
):
    """
    [V7.6 Aggressive Profit Protection]
    수익률 구간별로 손절선을 공격적으로 상향하여 수익 반납 방어
    """
    # Safe value check
    current_price = to_numeric_safe(current_price)
    entry_price = to_numeric_safe(entry_price)
    stop_loss = to_numeric_safe(stop_loss)

    if entry_price == 0:
        return stop_loss

    conf = combined_score / 100.0
    profit_rate = (current_price - entry_price) / entry_price
    new_stop_loss = stop_loss

    # 1단계: 수익 3% 이상 - 본절가 방어 (Breakeven)
    if profit_rate >= 0.03:
        # 본절가 -1% 수준으로 올려서 손실 거래 전환 방지
        new_stop_loss = max(new_stop_loss, entry_price * 0.99)

    # 2단계: 수익 5% 이상 - 수익금의 절반 이상 보전
    if profit_rate >= 0.05:
        # 기존 0.95 -> 0.96으로 상향 (신뢰도에 따라 최대 98%까지)
        base_trailing = 0.96 + (0.02 * conf)
        new_stop_loss = max(new_stop_loss, current_price * base_trailing)

    # 3단계: 수익 7% 이상 - 초강력 추격 (98% 고정)
    # 이때부터는 '날아가는 말'에서 떨어지지 않도록 타이트하게 붙음
    if profit_rate >= 0.07:
        # 고점 대비 2% 하락 시 즉시 전량 익절 구조
        ultra_trailing = 0.98 if conf > 0.8 else 0.975
        new_stop_loss = max(new_stop_loss, current_price * ultra_trailing)

    return round(new_stop_loss, 2)


def is_buyable(
    can_buy,
    is_buy,
    trigger_hit,
    grade,
    decision_state=None,
    is_recovering_leader=False,
    reason_str="",
):
    """
    [V10.5 Optimized] 최종 매수 실행 게이트웨이 (명령 복종 패턴)
    - [Zero-Defect: Executive Pardon Honor] 뇌의 사면권([Pardon]) 확인 시 무조건 즉시 승인
    - 뇌가 기각(can_buy=False)한 종목은 어떤 경우에도 진입 불가 (단일 진실 공급원 준수)
    """
    clean_grade = str(grade).strip().upper()
    actual_can_buy = str(can_buy).upper() in ["TRUE", "1", "YES"] or can_buy is True

    # 🛡️ [Zero-Defect: Executive Pardon Honor] 브레인 사면권 절대 복종 (VIP 프리패스)
    # 뇌가 명시적으로 사면([Pardon])을 결정했다면, 하위 지표(trigger_hit 등) 미달을 무시하고 즉시 문을 엽니다.
    if (
        "[Pardon]" in str(reason_str)
        or "Hyper-Momentum Override" in str(reason_str)
        or is_recovering_leader
    ):
        if logger:
            logger.info(
                f"🛡️ [Gateway Bypass] {clean_grade}등급 주도주/사면권 확인. 최종 승인."
            )
        return True

    # 1. 뇌(Brain)의 최종 결정 플래그 확인 (단일 진실 공급원)
    if not actual_can_buy:
        return False

    # 0. 전략 엔진 최종 승인 바이패스 (APPROVE 계열 상태 대응)
    if decision_state and str(decision_state).startswith("APPROVE"):
        return True

    # 2. 불필요한 등급 제외 (B등급 이상만 진입 허용)
    if clean_grade not in ["S", "A"]:
        if not is_recovering_leader:
            return False

    # 3. 실제 실행 트리거 확인 (Adaptive Execution)
    actual_is_buy = str(is_buy).upper() in ["TRUE", "1", "YES"] or is_buy is True
    actual_trigger_hit = (
        str(trigger_hit).upper() in ["TRUE", "1", "YES"] or trigger_hit is True
    )

    return actual_trigger_hit or actual_is_buy


def parse_avail_cash(data: dict) -> float:
    """
    [Zero-Defect] 증권사별 다양한 주문가능금액 키워드를 탐색하여 가용 자산을 추출합니다.
    """
    if not data or not isinstance(data, dict):
        return 0.0

    priority_keys = [
        "총평가금액",
        "D+2추정예수금",
        "추정D2예수금",
        "D+2예수금",
        "현금주문가능금액",
        "주문가능금액",
        "D+2금액",
        "추정순자산",
        "d2avamt",
        "dnams",
        "d2_wash_avail",
        "d2_avail",
        "예수금",
        "현금잔고",
        "d2_available",
        "buyable_amt",
        "avail_cash",
    ]

    for key in priority_keys:
        val = data.get(key)
        if val is not None:
            numeric_val = to_numeric_safe(val)
            if float(numeric_val) > 0:
                return float(numeric_val)

    return 0.0


def prepare_minute_df(db_rows):
    """
    DB에서 가져온 행 데이터를 분석용 DataFrame으로 변환
    """
    df = pd.DataFrame(db_rows)

    if df.empty:
        return df

    # [Zero-Defect Fix] 'RangeIndex' object has no attribute 'date' 오류 방지를 위해 인덱스 및 타입 강제
    if not df.empty and "등록일시" in df.columns:
        df["등록일시"] = pd.to_datetime(df["등록일시"], errors="coerce")
        df.set_index("등록일시", inplace=True)
    else:
        # 데이터가 비어있어도 인덱스 타입을 DatetimeIndex로 초기화하여 런타임 에러 방어
        df.index = pd.to_datetime(df.index)

    # 3. 데이터 타입 변환 (콤마 제거 및 숫자형 변환)
    # 종가, 시가 등에 콤마(,)가 있다면 제거 후 float 변환
    cols_to_fix = ["종가", "시가", "고가", "저가", "거래량", "체결량"]
    for col in cols_to_fix:
        if col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].str.replace(",", "").astype(float)
            else:
                try:
                    df[col] = df[col].astype(float)
                except Exception:
                    pass

    return df


def get_most_recent_trading_day_start(today=None, hour=12, minute=0):
    """KRX 휴장일을 고려하여 '직전 영업일 12:00' 타임스탬프 산출"""
    from config.system_params import KRX_HOLIDAYS

    if today is None:
        today = dt.datetime.now()

    # 정확히 "직전" 영업일을 찾기 위해 어제부터 탐색 시작
    current = today - timedelta(days=1)

    while True:
        # 1. 주말 체크 (토=5, 일=6)
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


def setUnitInvestment(account_info, main_window):
    """
    [Hardening] 1/N 포트폴리오 슬롯 기반 단위 투자금 산출.
    남은 슬롯이 아닌 '전체 슬롯' 대비 총자산 배분 방식을 채택하여 일관된 사이징을 유지합니다.
    """
    SAFE_BUYABLE_AMT = get_actual_buying_power(account_info)

    total_slot_cnt = 5  # Default fallback
    if main_window is not None and hasattr(main_window, "gooses"):
        # AIGoose(3) + GuardianGoose(2) 등의 설정값 합산
        total_slot_cnt = sum(
            int(getattr(r, "포트폴리오수", 0)) for r in main_window.gooses
        )

        ai_slots = main_window.gooses[0].portfolio.keys()
        guard_slots = main_window.gooses[1].portfolio.keys()
        running_slot_cnt = len(ai_slots) + len(guard_slots)
    if total_slot_cnt <= 0:
        total_slot_cnt = 5
    remain_slots = max(0, total_slot_cnt - running_slot_cnt)

    try:
        # [Strategy] 89M / 5 = 17.8M 식의 정적 1/N 배분
        # 가용 자금(Buying Power)이 아닌 총 자산 기준으로 사이징하여 일관성 유지
        net_asset = to_numeric_safe(account_info.get("추정순자산", 0))
        if net_asset < 50_000_000:  # 자산 인식 실패 시 Buying Power 활용
            net_asset = SAFE_BUYABLE_AMT

        단위투자금 = int(net_asset // total_slot_cnt)

        # [Risk Control] 1종목 최대 투자금 제한 (자산의 30%)
        max_unit_limit = int(net_asset * 0.30)
        단위투자금 = min(단위투자금, max_unit_limit)

        if 단위투자금 < 0:
            단위투자금 = 0

        update_capital(단위투자금)

    except Exception as e:
        if logger:
            logger.error(f"setUnitInvestment 계산 오류: {e}")
        단위투자금 = 0

    # 모든 goose 객체 동기화
    if main_window is not None and hasattr(main_window, "gooses"):
        for goose in main_window.gooses:
            goose.단위투자금 = 단위투자금

    # 4. [Multi-Layer Verification] 매수 금지 판정
    MIN_ORDER_THRESHOLD = 100_000
    MAX_ASSET_UTILIZATION = 0.93  # 총 자산 대비 최대 매입 비중 (현금 확보)

    is_insufficient_cash = SAFE_BUYABLE_AMT < MIN_ORDER_THRESHOLD
    is_too_small_order = 0 < 단위투자금 < MIN_ORDER_THRESHOLD

    current_buying_amt = to_numeric_safe(account_info.get("매입금액", 0))
    net_asset_val = to_numeric_safe(account_info.get("추정순자산", 0))
    is_full_exposure = (
        current_buying_amt > (net_asset_val * MAX_ASSET_UTILIZATION)
        if net_asset_val > 0
        else False
    )

    if (
        is_insufficient_cash
        or is_too_small_order
        or is_full_exposure
        or remain_slots <= 0
    ):
        if main_window is not None:
            main_window.주문가능금액부족 = True

        if remain_slots <= 0:
            reason = f"잔여 슬롯 없음 ({running_slot_cnt}/{total_slot_cnt})"
        elif is_insufficient_cash:
            reason = f"가용자금({SAFE_BUYABLE_AMT:,.0f}) 부족"
        elif is_too_small_order:
            reason = f"최소주문금액({MIN_ORDER_THRESHOLD:,.0f}) 미달"
        else:
            reason = f"계좌 최대 노출(93%) 도달"

        if remain_slots <= 0 or (단위투자금 > 0 and current_buying_amt > 0):
            if logger:
                logger.warning(f"⚠️ 매수 중단: {reason} (단위투자금: {단위투자금:,.0f})")
    else:
        if main_window is not None:
            main_window.주문가능금액부족 = False

    return {
        "단위투자금": 단위투자금,
        "주문가능금액": SAFE_BUYABLE_AMT,
        "total_slot_cnt": total_slot_cnt,
        "running_slot_cnt": running_slot_cnt,
        "aigoose_slots": ai_slots,
        "guardian_slots": guard_slots,
        "remain_slots": remain_slots,
    }


class AccountGuard:
    def __init__(self, start_equity, max_daily_loss_pct=3.0):
        self.max_daily_loss_pct = max_daily_loss_pct
        self.is_emergency_mode = False
        self.start_day_equity = start_equity
        self.error_count = 0  # [Internal Audit] 연속 오류 확인용
        logger.info(f"AccountGuard initialized: {self.start_day_equity}")

    def check_guard_status(self, current_equity, index_change):
        # if SYS_ID == 1: #모의투자일 경우 보호 로직 해제
        #    return False
        # [Internal Audit] 장 시작 자산이 0인 경우 나눗셈 에러 발생
        if self.start_day_equity == 0:
            logger.warning(
                "⚠️ AccountGuard: start_day_equity가 0입니다. 현재 자산으로 초기화합니다."
            )
            self.start_day_equity = current_equity
            return

        # 1. 데이터 유효성 검사 (Sanity Check)
        # 시작 자산의 50% 미만으로 찍히면 데이터 오류로 간주하고 스킵
        if current_equity < (self.start_day_equity * 0.5):
            logger.warning(f"⚠️ 비정상 자산 데이터 감지 (무시): {current_equity}")
            return self.is_emergency_mode

        start_equity = self.start_day_equity
        # daily_return_pct = (current_equity - start_equity) / start_equity * 100
        # 수익률 계산 시 분모(start_day_equity) 보호
        daily_return_pct = (
            (current_equity - self.start_day_equity) / self.start_day_equity * 100
        )

        # 2. 보호 스위치 판단 (연속 3회 적중 시 작동)
        is_hit = daily_return_pct <= -self.max_daily_loss_pct or index_change <= -3.0

        if is_hit:
            self.error_count += 1
            logger.warning(
                f"⚠️ 계좌 보호 조건 충족 ({self.error_count}/3): {daily_return_pct:.2f}%"
            )
        else:
            self.error_count = 0  # 정상 범위면 카운트 리셋

        # 3회 연속일 때만 실제 모드 전환
        if self.error_count >= 3:
            if not self.is_emergency_mode:
                logger.error(
                    f"🚨 [계좌 보호 스위치 최종 작동] 현재 손실률: {daily_return_pct:.2f}%"
                )
                self.is_emergency_mode = True
        else:
            # 복구 조건 (더 완만하게 설정 가능)
            if self.is_emergency_mode and daily_return_pct > -1.0:
                self.is_emergency_mode = False
                self.error_count = 0

        return self.is_emergency_mode


def sanitize(value):
    if hasattr(value, "item"):  # NumPy 타입인 경우
        return value.item()
    return value


def sanitize_dict(key, obj):
    if key in obj:
        eval_data = obj[key]
        #  특정 키는 수치 변환에서 제외 (문자열 유지)
        exclude_keys = [
            "final_grade",
            "grade",
            "final_reason",
            "reason",
            "msg",
            "message",
            "is_buy",
            "energy_status",
            "is_energy_dryup",
            "volume_dry",
            "status",
            "trigger_hit",
            "breakout_info",
            "final_can_buy",
            "is_acc_sync",
            "is_limit_up_mode",
            "is_high_vol_warning",
            "audit_report",
            "vol_adjusted_sl",
            "special_grade",
        ]

        for k, v in eval_data.items():
            if k in exclude_keys:
                continue
            if hasattr(v, "item"):  # NumPy 타입인 경우
                eval_data[k] = v.item()
            elif isinstance(v, (float, int, object)):  # 수치형 데이터 대상
                eval_data[k] = to_numeric_safe(v)


def updateSearchStock(code, status):
    """분별주가 검색대상 제외/상태 업데이트 (검색종목 테이블)"""
    try:
        from config.ai_settings import SYS_ID, 거래환경

        현재일자 = dt.datetime.now().strftime("%Y-%m-%d")
        with get_db_connection() as conn:
            query = "UPDATE 검색종목 SET 매매구분 = ? WHERE 종목코드 = ? AND 등록일시 = ? AND SYS_ID = ?"
            data = [status, code, 현재일자, SYS_ID]
            cursor = conn.cursor()
            cursor.execute(query, data)
            if logger:
                # logger.info(f"updateSearchStock {code} status {status}")
                logger.debug(
                    f"updateSearchStock cursor.rowcount: {cursor.rowcount}, SYS_ID: {SYS_ID}, 거래환경: {거래환경}"
                )
            cursor.close()
    except Exception as e:
        if logger:
            logger.error(f"[updateSearchStock Error] {e}")


def updateSearchStock_bulk(codes, status):
    """분별주가 검색대상 제외/상태 대량 업데이트 (검색종목 테이블)"""
    if not codes:
        return
    try:
        from config.ai_settings import SYS_ID

        현재일자 = dt.datetime.now().strftime("%Y-%m-%d")
        with get_db_connection() as conn:
            query = "UPDATE 검색종목 SET 매매구분 = ? WHERE 종목코드 = ? AND 등록일시 = ? AND SYS_ID = ?"
            data = [(status, code, 현재일자, SYS_ID) for code in codes]
            cursor = conn.cursor()
            cursor.executemany(query, data)
            if logger:
                logger.debug(
                    f"updateSearchStock_bulk updated {cursor.rowcount} rows, SYS_ID: {SYS_ID}"
                )
            cursor.close()
    except Exception as e:
        if logger:
            logger.error(f"[updateSearchStock_bulk Error] {e}")


# 매수 조건 설정: 변동폭과 평균의 결합 평가
# 현재가가 평균보다 크고 변동폭이 평균보다 낮을 때 매수 조건 성립
def check_buy_signal(K3_S3_index):
    try:
        # 기본 가격 정보 직접 딕셔너리에서 추출
        시가 = float(K3_S3_index["시가"])
        고가 = float(K3_S3_index["고가"])
        저가 = float(K3_S3_index["저가"])
        현재가 = float(K3_S3_index["종가"])

        # 평균 계산
        시저평 = (시가 + 저가) / 2
        시고평 = (시가 + 고가) / 2
        저고평 = (저가 + 고가) / 2
        평균3 = (시저평 + 시고평 + 저고평) / 3

        # 변동폭 계산
        변동폭_시저 = abs(시가 - 저가)
        변동폭_시고 = abs(시가 - 고가)
        변동폭_저고 = abs(고가 - 저가)
        평균변동폭 = (변동폭_시저 + 변동폭_시고 + 변동폭_저고) / 3

        # 매수 조건 검사
        매수조건1 = 현재가 > 평균3  # 현재가가 평균보다 높음
        매수조건2 = 현재가 < (고가 - 평균변동폭 * 0.5)  # 고가에서 너무 멀지 않음
        매수조건3 = 현재가 > (저가 + 평균변동폭 * 0.3)  # 저가에서 적절히 상승

        # 로깅
        logger.info(
            f"현재가: {현재가}, 3평균: {평균3:.2f}, "
            f"평균변동폭: {평균변동폭:.2f}, "
            f"조건1: {매수조건1}, 조건2: {매수조건2}, 조건3: {매수조건3}"
        )

        # 모든 조건을 만족할 때만 매수 신호
        return 매수조건1 and 매수조건2 and 매수조건3

    except KeyError as e:
        logger.error(f"KeyError 발생: {e}")
        return False
    except ValueError as e:
        logger.error(f"ValueError 발생 (숫자 변환 실패): {e}")
        return False
    except Exception as e:
        logger.error(f"예기치 못한 오류: {e}")
        return False


def safe_get(obj, key, default=None):
    if obj is None:
        return default
    # 1. Try Attribute Access (Fastest for Dataclasses)
    if hasattr(obj, key):
        return getattr(obj, key)
    # 2. Try Dictionary get()
    try:
        if hasattr(obj, "get"):
            return obj.get(key, default)
    except Exception:
        pass
    # 3. Try Item Access
    try:
        return obj[key]
    except (KeyError, TypeError, IndexError, AttributeError):
        return default


def safe_set(obj, key, val):
    if obj is None:
        return
    # 1. Try Attribute Set (Dataclasses)
    if hasattr(obj, "__dataclass_fields__") or not isinstance(obj, dict):
        try:
            setattr(obj, key, val)
            return
        except (AttributeError, TypeError):
            pass
    # 2. Try Item Assignment (Dicts)
    try:
        obj[key] = val
    except (TypeError, KeyError, AttributeError):
        pass


def safe_update(obj, data: dict):
    if obj is None or data is None:
        return
    for k, v in data.items():
        safe_set(obj, k, v)
