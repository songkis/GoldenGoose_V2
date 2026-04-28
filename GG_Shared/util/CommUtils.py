# -*- coding: utf-8 -*-
import os
import concurrent.futures
import threading
import logging
import pandas as pd

# Import modularized utilities
from .db_utils import (
    get_db_connection, get_db_stats, close_db_connection, 
    getAIConfVal, update_capital, updateSearchStock, updateSearchStock_bulk,
    setConfig
)
from .crypto_utils import (
    generate_key, encrypt_data, decrypt_data, is_encrypted, SECRET_KEY, cipher_suite
)
from .data_processor import (
    deep_get, safe_float, to_numeric_safe, transform_to_daily_volume, 
    prepare_minute_df, make_json_safe, sanitize, sanitize_dict,
    safe_get, safe_set, safe_update, get_first_tuple
)
from .time_utils import (
    kst_now, get_lookback_start_time, isOverCloseTime, isBeforeOpenTime, 
    isOverBackupTime, get_most_recent_trading_day_start, get_trading_day_offset
)
from .account_utils import (
    parse_avail_cash, get_actual_buying_power, setUnitInvestment, AccountGuard
)
from .trading_logic_utils import (
    update_dynamic_exit_levels_v2, is_buyable, check_buy_signal
)

# Shared Global Objects
GLOBAL_THREAD_POOL = concurrent.futures.ThreadPoolExecutor(
    max_workers=max(4, (os.cpu_count() or 4) * 2)
)
MINUTE_DF_CACHE = {}
MINUTE_DF_CACHE_LOCK = threading.Lock()
logger = logging.getLogger(__name__)

def set_window_context(window, ext_logger=None):
    """
    메인 윈도우 인스턴스를 주입하고 설정을 로드하는 원본 Facade 함수 복구.
    GoldenGoose.pyw에서 호출됨.
    """
    from .crypto_utils import decrypt_data, is_encrypted
    import inspect

    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            # 1. 상세 계좌 정보 로드 (SELECT *)
            cursor.execute(
                """SELECT *
                   FROM TB_ACC_INFO tai 
                   WHERE tai.SYS_iD = (SELECT tac.CONF_VALUE 
                                      FROM TB_AI_CONF tac 
                                      WHERE tac.CONF_ID = '1')"""
            )
            row = cursor.fetchone()
            if row:
                columns = [desc[0] for desc in cursor.description]
                row_dict = dict(zip(columns, row))

                # 데이터 복호화 처리
                for column in ["ACC_NO", "USER_ID", "PWD", "CERT_PWD", "TRX_PWD"]:
                    if column in row_dict and row_dict[column] is not None:
                        row_dict[column] = (
                            decrypt_data(row_dict[column])
                            if is_encrypted(row_dict[column])
                            else row_dict[column]
                        )

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

            # 2. AI 설정 정보 로드
            cursor.execute(
                """SELECT CONF_ID, CONF_KEY, CONF_VALUE
                   FROM TB_AI_CONF tac
                   ORDER BY tac.CONF_ID """
            )
            rows = cursor.fetchall()
            if rows:
                window.ai_conf_dict = {row[1]: row[2] for row in rows}
            cursor.close()

        except Exception as e:
            if hasattr(window, "logger") and window.logger:
                클래스이름 = window.__class__.__name__
                함수이름 = inspect.currentframe().f_code.co_name
                window.logger.error(
                    f"{클래스이름}-{함수이름} {inspect.currentframe().f_lineno}: {e}"
                )
            else:
                logger.error(f"[set_window_context Error] {e}")

    # 3. 로거 동기화
    if ext_logger:
        set_commutils_logger(ext_logger)

def set_commutils_logger(external_logger):
    global logger
    from . import db_utils, account_utils, trading_logic_utils, data_processor
    logger = external_logger
    db_utils.logger = external_logger
    account_utils.logger = external_logger
    trading_logic_utils.logger = external_logger
    data_processor.logger = external_logger

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

def get_linenumber():
    import inspect
    try:
        frame = inspect.currentframe()
        if frame and frame.f_back:
            return frame.f_back.f_lineno
    except Exception:
        pass
    return 0

def get_funcname():
    import inspect
    try:
        frame = inspect.currentframe()
        if frame and frame.f_back:
            return frame.f_back.f_code.co_name
    except Exception:
        pass
    return "unknown"
