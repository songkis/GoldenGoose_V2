# -*- coding: utf-8 -*-
import sqlite3
import threading
import logging
import contextlib
from queue import Empty, Full, Queue
from config.comm_settings import DATABASE

logger = logging.getLogger(__name__)

# 글로벌 설정 객체 (Zero-Defect Config Injection)
config = None

def kst_now():
    import datetime as dt
    from datetime import timedelta
    return (dt.datetime.utcnow() + timedelta(hours=9)).strftime("%Y-%m-%d %H:%M:%S")

class ConnectionPool:
    """SQLite 연결 풀 클래스 - 성능 개선을 위한 연결 재사용 (Thread-safe)"""
    def __init__(self, max_connections=10, database=None):
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
            conn = sqlite3.connect(
                self.database,
                timeout=30.0,
                isolation_level=None,
                check_same_thread=False,
            )
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA busy_timeout = 30000;")
            conn.create_function("kst_now", 0, kst_now)
            return conn
        except Exception as e:
            if logger:
                logger.error(f"연결 생성 실패: {e}")
            raise

    def get_connection(self):
        self._total_requests += 1
        try:
            conn = self._pool.get_nowait()
            self._pool_hits += 1
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

        with self._lock:
            if self._created < self.max_connections:
                conn = self._create_new_connection()
                self._created += 1
                return conn

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
                return self._create_new_connection()
        except Empty:
            if logger:
                logger.warning("연결 풀 고갈 - 임시 연결 생성")
            return self._create_new_connection()

    def return_connection(self, conn):
        if not conn:
            return
        try:
            conn.execute("SELECT 1")
            try:
                self._pool.put_nowait(conn)
            except Full:
                conn.close()
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            with self._lock:
                if self._created > 0:
                    self._created -= 1

    def get_stats(self):
        hit_rate = (self._pool_hits / self._total_requests * 100) if self._total_requests > 0 else 0
        return {
            "created": self._created,
            "pool_size": self._pool.qsize(),
            "total_requests": self._total_requests,
            "pool_hits": self._pool_hits,
            "hit_rate": f"{hit_rate:.2f}%",
        }

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
        yield conn
    except Exception as e:
        if logger:
            logger.error(f"DB 작업 중 오류 발생: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        _db_pool.return_connection(conn)

def get_db_stats():
    if _db_pool:
        return _db_pool.get_stats()
    return {"status": "Not initialized"}

def close_db_connection(conn):
    if _db_pool and conn:
        _db_pool.return_connection(conn)

def getAIConfVal(key):
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """SELECT CONF_ID, CONF_KEY, CONF_VALUE
                            FROM TB_AI_CONF tac WHERE 1=1
                            ORDER BY tac.CONF_ID """
            )
            rows = cursor.fetchall()
            if rows:
                ai_conf_dict = {row[1]: row[2] for row in rows}
            cursor.close()
        return ai_conf_dict[key]
    except Exception:
        return None

def update_capital(capital):
    from SQL.sql import UPDATE_BUYABLE_AMT
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(UPDATE_BUYABLE_AMT, (capital,))
        cursor.close()

def updateSearchStock(code, status):
    """분별주가 검색대상 제외/상태 업데이트 (검색종목 테이블)"""
    import datetime as dt
    try:
        from config.ai_settings import SYS_ID, 거래환경
        현재일자 = dt.datetime.now().strftime("%Y-%m-%d")
        with get_db_connection() as conn:
            query = "UPDATE 검색종목 SET 매매구분 = ? WHERE 종목코드 = ? AND 등록일시 = ? AND SYS_ID = ?"
            data = [status, code, 현재일자, SYS_ID]
            cursor = conn.cursor()
            cursor.execute(query, data)
            if logger:
                logger.debug(
                    f"updateSearchStock cursor.rowcount: {cursor.rowcount}, SYS_ID: {SYS_ID}, 거래환경: {거래환경}"
                )
            cursor.close()
    except Exception as e:
        if logger:
            logger.error(f"[updateSearchStock Error] {e}")

def updateSearchStock_bulk(codes, status):
    """분별주가 검색대상 제외/상태 대량 업데이트 (검색종목 테이블)"""
    import datetime as dt
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
            cursor.close()
    except Exception as e:
        if logger:
            logger.error(f"[updateSearchStock_bulk Error] {e}")

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
