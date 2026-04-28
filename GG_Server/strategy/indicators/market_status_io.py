import pandas as pd
import logging
from datetime import datetime

def fetch_trade_data(market_tp_cd, logger=None):
    from SQL.sql import GET_MARKET_PRICE_HIST
    try:
        from util.CommUtils import get_db_connection
        with get_db_connection() as conn:
            query = GET_MARKET_PRICE_HIST
            df = pd.read_sql_query(query, conn, params=(int(market_tp_cd),))
            return df
    except Exception as e:
        if logger: logger.info(f"거래 데이터 조회 중 오류: {e}")
        return pd.DataFrame()

def save_market_status_to_db(market_type, regime, change, adr, logger=None):
    """최종 결정된 시장 정보를 DB에 기록 (SSOT)"""
    try:
        from util.CommUtils import get_db_connection
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS TB_MARKET_STATUS (
                    MARKET_TYPE TEXT PRIMARY KEY,
                    REGIME TEXT,
                    INDEX_CHANGE REAL,
                    ADR_RATIO REAL,
                    LAST_UPDATED DATETIME
                )
            """)
            cursor.execute("""
                INSERT OR REPLACE INTO TB_MARKET_STATUS (MARKET_TYPE, REGIME, INDEX_CHANGE, ADR_RATIO, LAST_UPDATED)
                VALUES (?, ?, ?, ?, ?)
            """, (market_type, regime, change, adr, datetime.now()))
            conn.commit()
    except Exception as e:
        if logger: logger.error(f"save_market_status_to_db Error: {e}")
