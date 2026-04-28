import logging
from util.CommUtils import get_db_connection, load_data, updateSearchStock
from config.ai_settings import TIME_STEP
from SQL.sql import UPDATE_매수후고가

logger = logging.getLogger(__name__)

def set_logger(external_logger):
    global logger
    logger = external_logger

def fetch_active_positions(sys_id: str) -> dict:
    """
    Fetches active positions (regist, open, closed) from DB for bulk processing.
    Returns: {stock_cd: dict} where dict has keys matching column names.
    """
    active_positions_map = {}
    query = """
        SELECT A.종목코드, A.진입가, A.수량 AS 포지션수량, A.청산가, A.목표가1, A.목표가2, A.최고가, A.매매기법, A.상태,
               IFNULL(B.종목명, '') AS 종목명, 
               IFNULL(B.매수가, 0) AS 매수가, 
               IFNULL(B.수량, 0) AS 수량, 
               IFNULL(B.매수후고가, 0) AS 매수후고가,
               (SELECT 구분
                FROM 종목코드
                WHERE ETF구분=0
                AND 종목코드 = A.종목코드) AS 시장구분
        FROM 포지션 A
        LEFT OUTER JOIN 포트폴리오 B 
        ON A.종목코드 = B.종목코드 AND A.SYS_ID = B.SYS_ID
        WHERE A.SYS_ID = ? AND A.상태 IN ('regist', 'open', 'closed', 'cancel')
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, (sys_id,))
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            for row in rows:
                row_dict = dict(zip(columns, row))
                p_code = row_dict.pop("종목코드")
                active_positions_map[p_code] = row_dict
    except Exception as e:
        if logger:
            logger.error(f"[fetch_active_positions] Bulk read failed: {e}")
    return active_positions_map


def execute_batch_updates(
    cursor,
    batch_data_trace_signal=None,
    batch_update_open=None,
    batch_update_closed=None,
    batch_update_high=None,
    batch_update_high_port=None,
    batch_upsert_position=None,
    batch_update_targets=None,
):
    """
    Executes collected batch operations.
    """
    try:
        # 1. TB_TRADE_SIGNAL Insert
        if batch_data_trace_signal:
            query_tsg = """
                INSERT INTO TB_TRADE_SIGNAL(종목코드, 예측일자, 시간, 
                                        COMBINED_SCORE, COMBINED_SCORE_CNFDNC,
                                        TSP, STOP_LOSS_PRCE, TAKE_PROFIT1, TAKE_PROFIT2, RISK_REWARD_RATIO,
                                        TRADE_STRATEGY, UP_DOWN_TREND, CAPITAL, TRADE_INFO_JSON)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(종목코드, 예측일자, 시간) DO UPDATE SET
                    COMBINED_SCORE = excluded.COMBINED_SCORE,
                    COMBINED_SCORE_CNFDNC = excluded.COMBINED_SCORE_CNFDNC,
                    TSP = excluded.TSP,
                    STOP_LOSS_PRCE = TB_TRADE_SIGNAL.STOP_LOSS_PRCE,
                    TAKE_PROFIT1 = TB_TRADE_SIGNAL.TAKE_PROFIT1,
                    TAKE_PROFIT2 = TB_TRADE_SIGNAL.TAKE_PROFIT2,
                    RISK_REWARD_RATIO = excluded.RISK_REWARD_RATIO,
                    TRADE_STRATEGY = excluded.TRADE_STRATEGY,
                    UP_DOWN_TREND = excluded.UP_DOWN_TREND,
                    CAPITAL = TB_TRADE_SIGNAL.CAPITAL,
                    TRADE_INFO_JSON = excluded.TRADE_INFO_JSON
            """
            cursor.executemany(query_tsg, batch_data_trace_signal)
            if logger:
                logger.info(f"[Batch] TB_TRADE_SIGNAL inserted {len(batch_data_trace_signal)} records")

        # 2. Position Status -> Open
        if batch_update_open:
            cursor.executemany(
                "UPDATE 포지션 SET 상태='open' WHERE 종목코드=? AND 상태='regist' AND SYS_ID=?",
                batch_update_open,
            )
            if logger:
                logger.info(f"[Batch] Position->Open updated {len(batch_update_open)} records")

        # 3. Position Status -> Closed
        if batch_update_closed:
            cursor.executemany(
                "UPDATE 포지션 SET 상태='closed', 청산가=?, 실현손익=?, 청산일자=? WHERE 종목코드=? AND 상태='open' AND SYS_ID=?",
                batch_update_closed,
            )
            if logger:
                logger.info(f"[Batch] Position->Closed updated {len(batch_update_closed)} records")

        # 4. Highest Price Update (Position Table)
        if batch_update_high:
            cursor.executemany(
                "UPDATE 포지션 SET 최고가=? WHERE 종목코드=? AND 상태='open' AND SYS_ID=?",
                batch_update_high,
            )
            if logger:
                logger.info(f"[Batch] HighestPrice(Pos) updated {len(batch_update_high)} records")

        # 5. Highest Price Update (Portfolio Table)
        if batch_update_high_port:
            cursor.executemany(UPDATE_매수후고가, batch_update_high_port)
            if logger:
                logger.info(f"[Batch] HighestPrice(Port) updated {len(batch_update_high_port)} records")

        # 6. Position Upsert (Regist/New)
        if batch_upsert_position:
            query_upsert = """
                INSERT INTO 포지션(
                    종목코드, 진입일자, 진입가, 수량, 청산가, 목표가1, 목표가2, 최고가, 상태, 매매기법, SYS_ID
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(종목코드)
                DO UPDATE SET
                    진입일자 = excluded.진입일자,
                    진입가 = excluded.진입가,
                    수량 = excluded.수량,
                    청산가 = excluded.청산가,
                    목표가1 = excluded.목표가1,
                    목표가2 = excluded.목표가2,
                    최고가 = excluded.최고가,
                    상태 = excluded.상태,
                    매매기법 = excluded.매매기법,
                    SYS_ID = excluded.SYS_ID
            """
            cursor.executemany(query_upsert, batch_upsert_position)
            if logger:
                logger.info(f"[Batch] Position Upsert {len(batch_upsert_position)} records")

        # 7. Update Targets (Existing Position)
        if batch_update_targets:
            query_update_targets = """
                UPDATE 포지션
                SET 청산가 = ?, 목표가1 = ?, 목표가2 = ?
                WHERE 종목코드 = ? AND SYS_ID = ?
            """
            cursor.executemany(query_update_targets, batch_update_targets)
            if logger:
                logger.info(f"[Batch] Update Targets {len(batch_update_targets)} records")

    except Exception as e:
        if logger:
            logger.error(f"[execute_batch_updates] Failed: {e}")
        raise e


def fetch_datas(tickers: list) -> dict:
    """
    tickers 리스트를 돌면서 각 종목의 일봉 DataFrame을 가져와
    {종목코드: DataFrame} 형태의 dict로 반환.
    """
    target_data_pool = {}
    for t in tickers:
        try:
            df = load_data(t, "일별주가")
            if len(df) < TIME_STEP:
                updateSearchStock(t, -9)
                continue
            target_data_pool[t] = df
        except Exception as e:
            if logger:
                logger.error(f"{t} 종목 일봉 로드 오류: {e}")
    return target_data_pool
