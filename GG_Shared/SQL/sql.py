from util.CommUtils import get_trading_day_offset

offset = get_trading_day_offset()

INIT_ALL_STK_ITEM = """--주가예측 전 초기화
UPDATE 종목코드 
SET COMBINED_SCORE = NULL
    ,COMBINED_SCORE_CNFDNC = NULL
	,예측일시 = NULL
WHERE LENGTH(COALESCE(예측일시,'')) > 0
AND 예측일시 < DATE('now','+9 hours')
"""

DEL_POSITION = """--주가예측 전 포지션 초기화
DELETE FROM 포지션 
WHERE 종목코드 NOT IN (SELECT 종목코드 FROM 포트폴리오 WHERE SYS_ID = ? )
AND SYS_ID = ?
"""
INIT_POSITION = """--주가예측 전 포지션 삭제 후 초기화
UPDATE 포지션 SET 상태 = 'open' WHERE SYS_ID = ? 
"""
GET_POSITION = """
SELECT 진입가, 수량, 청산가, 목표가1, 목표가2, 최고가, 매매기법, 상태, 매수현재회차, 매수전체회차, 매도현재회차, 매도전체회차
FROM 포지션 WHERE 종목코드=? AND SYS_ID=?
"""

# 종목코드 리스트
GET_STOCK_LIST = """
SELECT A.종목코드, A.종목명, A.ETF구분, A.구분
FROM 종목코드 A
INNER JOIN 일별주가 B ON A.종목코드 = B.종목코드 
    AND B.날짜 = (SELECT MAX(날짜) FROM 일별주가 WHERE 종목코드 = A.종목코드)
WHERE A.ETF구분 = 0 
AND A.구분 != 9              --  지수 데이터 격리
AND A.상한가 > 0 
AND A.하한가 > 0
AND A.종목명 NOT LIKE '%스팩%'
AND A.종목명 NOT LIKE '%KODEX%'
AND A.종목명 NOT LIKE '%TIGER%'
AND A.종목명 NOT LIKE '%200%'
AND B.종가 >= 1000           --  동전주 1차 차단
AND B.누적거래대금 >= 8000     --  거래대금 80억 이상만 통과 (단위: 백만원 기준)
AND A.종목코드 NOT IN (
    --상폐되어 일별주가가 백업되지 않는 데이터 제외
	SELECT 종목코드
	FROM 종목코드
	EXCEPT 
    SELECT DISTINCT 종목코드 --, MAX(날짜)
    FROM 일별주가
    WHERE 날짜 > strftime('%Y%m%d',DATE('now','-30 DAYS'))
)
"""

# [Phase 2.1] RS 계산용 지수 일봉 데이터 로드
GET_INDEX_DAILY_PRICE = """
SELECT 날짜, 종가 
FROM 일별주가 
WHERE 종목코드 = ? 
AND 날짜 >= strftime('%Y%m%d', DATE('now', '-12 months'))
ORDER BY 날짜
"""

GET_STOCK_LIST_300 = """
select 종목코드-- , DATE('now','+9 hours') AS 등록일자
from 종목코드
WHERE ETF구분 = 0
AND 상한가 > 0
AND 하한가 > 0
and COMBINED_SCORE_CNFDNC > 0
ORDER BY COMBINED_SCORE_CNFDNC
limit 300"""

# 일별주가 백업대상
GET_DAILY_BACKUP_TARGET = """
SELECT A.종목코드, A.종목명,
    CASE  
        WHEN MAX(B.날짜) IS NULL THEN 300
        ELSE floor(julianday('now') - julianday(substr( MAX(B.날짜), 1, 4) || '-' || substr( MAX(B.날짜), 5, 2) || '-' || substr( MAX(B.날짜), 7, 2)))
    END  AS days_difference
FROM 종목코드 A
    LEFT OUTER JOIN 
    일별주가 B
ON A.ETF구분 = 0
AND A.상한가 > 0 
AND A.하한가 > 0
AND A.종목명 NOT LIKE '%스팩%'
AND A.종목명 NOT LIKE '%KODEX%'
AND A.종목명 NOT LIKE '%TIGER%'
AND A.종목명 NOT LIKE '%200%'
WHERE A.종목코드 = B.종목코드
GROUP BY A.종목코드"""

# 검색 -> 가격정보요청 -> 매수 종목 -> 가격정보요청대상
GET_REAL_PRICE_INFO_STOCK = """
--가격정보요청 종목들, 매수상태, 포트롤리오에 있는 종목
SELECT DISTINCT 종목코드
FROM 포트폴리오
WHERE GG_NM = '%s'
AND SYS_ID = %d
"""

# 매수대기종목 수 : 포트폴리오 + R1_BUY_READY_CNT개 유지.
SET_BUY_READY_STK_ITEM = """
insert or replace into 검색종목(종목코드, 종목명, GEN_GG_TP, 등록일시, 매매구분, SYS_ID, SORT_NO) 
select
    C.종목코드 
    , (SELECT 종목명
        FROM 종목코드
        WHERE 종목코드 = A.종목코드) AS 종목명
    , 1, DATE('now', '+9 hours')
    ,0
    ,? AS SYS_ID
    ,A.COMBINED_SCORE_CNFDNC
from 종목코드 A
INNER JOIN 일별주가 C 
ON A.종목코드 = C.종목코드
AND C.날짜 = (SELECT MAX(날짜) FROM 일별주가 WHERE 종목코드 = A.종목코드 )
WHERE A.ETF구분 = 0 
AND A.상한가 > 0 
AND A.하한가 > 0
AND A.종목명 NOT LIKE '%스팩%'
AND A.종목명 NOT LIKE '%KODEX%'
AND A.종목명 NOT LIKE '%TIGER%'
AND A.종목명 NOT LIKE '%200%'
AND A.종목코드 NOT IN (
    --상폐되어 일별주가가 백업되지 않는 데이터 제외
	SELECT 종목코드
	FROM 종목코드
	EXCEPT 
    SELECT DISTINCT 종목코드
    FROM 일별주가
    WHERE 날짜 > strftime('%Y%m%d',DATE('now','-30 DAYS'))
)
AND A.종목코드 NOT IN (
	SELECT 종목코드
	FROM 포트폴리오
    WHERE SYS_ID = ?
	UNION ALL
	SELECT 종목코드
	FROM 검색종목
	WHERE SUBSTR(등록일시, 1, 10) = DATE('now', '+9 hours')
    AND SYS_ID = ?
	--AND 매매구분 IN (0, 2) --0:대기, 2:매수 
)
AND A.COMBINED_SCORE_CNFDNC > 0
ORDER BY A.COMBINED_SCORE_CNFDNC, A.AI_PROB DESC, C.등락율 DESC, C.거래증가율 DESC, C.체결강도 DESC, C.누적거래량 DESC, C.소진율 DESC, C.회전율 DESC
LIMIT 300
"""

GET_MIN_DATA_GET_READY_CNT = """SELECT COUNT() AS READY_CNT
            FROM(
                SELECT 종목코드
                FROM 포트폴리오
                WHERE SYS_ID = %d
                UNION ALL
                SELECT 종목코드
                FROM 검색종목
                WHERE SUBSTR(등록일시, 1, 10) = DATE('now', '+9 hours')
                AND 매매구분 = 0 --0:대기, 2:매수 
                AND SYS_ID = %d
            )"""

GET_GAIN_LAST_60_STK_LIST = """--분별주가 지난 분봉 60개 백업용
WITH TARGET AS (
        select 종목코드 , DATE('now',  "+9 hours") AS 등록일자
            from 종목코드
            WHERE ETF구분 = 0
            AND 상한가 > 0
            AND 하한가 > 0
            --and COMBINED_SCORE > 70 --not in('')
            and COMBINED_SCORE_CNFDNC > 0
            ORDER BY COMBINED_SCORE_CNFDNC
            limit 300
)
select A.종목코드, (SELECT 종목명 FROM 종목코드 SC WHERE SC.종목코드 = A.종목코드) AS 종목명, 
        ? AS tick, 0 as GEN_GG_TP순서, 10 AS 매매구분,
        (SELECT CASE  
		        WHEN MAX(등록일자||시간 ) IS NULL THEN 
					CAST(((
					        julianday(
								CASE 
								    -- 1. 현재 시간이 15:30:00 이후이면, 기준 시점을 당일 15:30:00으로 고정
								    WHEN strftime('%H:%M:%S', 'now', '+9 hours') >= '15:30:00' 
								    THEN strftime('%Y-%m-%d ', 'now', '+9 hours') || '15:30:00'
								    -- 2. 장중(15:30:00 이전)이면, 현재 시각을 그대로 사용
								    ELSE strftime('%Y-%m-%d %H:%M', 'now', '+9 hours') || ':00'
								END
					        )
					        - 
					        julianday(strftime('%Y-%m-%d', 'now')||' 09:00:00')
					    ) * 24 * 60) / ? AS INTEGER)
		        ELSE CAST(((
					julianday(CASE 
					    -- 1. 현재 시간이 15:30:00 이후이면, 기준 시점을 당일 15:30:00으로 고정
					    WHEN strftime('%H:%M:%S', 'now', '+9 hours') >= '15:30:00' 
					    THEN strftime('%Y-%m-%d ', 'now', '+9 hours') || '15:30:00'
					    -- 2. 장중(15:30:00 이전)이면, 현재 시각을 그대로 사용
					    ELSE strftime('%Y-%m-%d %H:%M', 'now', '+9 hours') || ':00'
					END
					)
		        - 
		            julianday(
		                substr(MAX(등록일자||시간 ), 1, 10) || ' ' || 
		                substr(MAX(등록일자||시간 ), 11, 2) || ':' || 
		                substr(MAX(등록일자||시간 ), 13, 2) || ':' || 
		                substr(MAX(등록일자||시간 ), 15, 2)
		            )) * 24 * 60) / ? AS INTEGER)
		        END AS missing_min_candles
			FROM 분별주가 C 
		    WHERE C.종목코드 = A.종목코드 
		    AND datetime(										
		            substr(등록일자||시간 , 1, 10) || ' ' || 
		            substr(등록일자||시간 , 11, 2) || ':' || 
		            substr(등록일자||시간 , 13, 2) || ':' || 
		            substr(등록일자||시간 , 15, 2)
		        ) < strftime('%Y-%m-%d %H:%M', 'now', '+9 hours')||':00'
		    AND C.등록일자 = DATE('now', '+9 hours')
		    ORDER BY C.등록일자, C.시간
		) AS missing_min_candles
FROM(
    SELECT * FROM TARGET
    EXCEPT
    SELECT A.종목코드, A.등록일자--, COUNT(A.시간) AS CNT
    FROM 분별주가 A, TARGET 
    WHERE A.종목코드 = TARGET.종목코드
    AND A.등록일자 = DATE('now', "+9 hours")
    AND A.시간 >= '130000'
    GROUP BY A.종목코드, A.등록일자
    HAVING COUNT(A.시간) >= 129
)A
WHERE missing_min_candles > 0
LIMIT ?
"""


GET_MIN_TERM_GAIN_STK_LIST = """
SELECT DISTINCT AA.*
FROM (
    SELECT 0 AS SORT_NO, 0 AS SEQ, 종목코드, 종목명, ? AS tick, 0 as GEN_GG_TP순서, 10 AS 매매구분, 
    (SELECT CASE  
                WHEN MAX(등록일자||시간 ) IS NULL THEN 
                    CAST(((
                            julianday(
                                CASE 
                                    -- 1. 현재 시간이 15:30:00 이후이면, 기준 시점을 당일 15:30:00으로 고정
                                    WHEN strftime('%H:%M:%S', 'now', '+9 hours') >= '15:30:00' 
                                    THEN strftime('%Y-%m-%d ', 'now', '+9 hours') || '15:30:00'
                                    -- 2. 장중(15:30:00 이전)이면, 현재 시각을 그대로 사용
                                    ELSE strftime('%Y-%m-%d %H:%M', 'now', '+9 hours') || ':00'
                                END
                            )
                            - 
                            julianday(strftime('%Y-%m-%d', 'now')||' 09:00:00')
                        ) * 24 * 60) / ? AS INTEGER)
                ELSE CAST(((
                    julianday(CASE 
                        -- 1. 현재 시간이 15:30:00 이후이면, 기준 시점을 당일 15:30:00으로 고정
                        WHEN strftime('%H:%M:%S', 'now', '+9 hours') >= '15:30:00' 
                        THEN strftime('%Y-%m-%d ', 'now', '+9 hours') || '15:30:00'
                        -- 2. 장중(15:30:00 이전)이면, 현재 시각을 그대로 사용
                        ELSE strftime('%Y-%m-%d %H:%M', 'now', '+9 hours') || ':00'
                    END
                    )
                - 
                    julianday(
                        substr(MAX(등록일자||시간 ), 1, 10) || ' ' || 
                        substr(MAX(등록일자||시간 ), 11, 2) || ':' || 
                        substr(MAX(등록일자||시간 ), 13, 2) || ':' || 
                        substr(MAX(등록일자||시간 ), 15, 2)
                    )) * 24 * 60) / ? AS INTEGER)
                END AS missing_min_candles
        FROM 분별주가 C 
            WHERE C.종목코드 = P.종목코드 
            AND datetime(										
                    substr(등록일자||시간 , 1, 10) || ' ' || 
                    substr(등록일자||시간 , 11, 2) || ':' || 
                    substr(등록일자||시간 , 13, 2) || ':' || 
                    substr(등록일자||시간 , 15, 2)
                ) < strftime('%Y-%m-%d %H:%M', 'now', '+9 hours')||':00'
            AND C.등록일자 = DATE('now', '+9 hours')
            ORDER BY C.등록일자, C.시간
        ) AS missing_min_candles,
        (	
            SELECT COUNT(시간) 
            FROM 분별주가 C 
            WHERE C.종목코드 = P.종목코드 
            AND C.등록일자 >= (SELECT COALESCE(MAX(등록일자), strftime('%Y-%m-%d', DATE('now', '+9 hours'))) FROM 분별주가 WHERE 등록일자 < strftime('%Y-%m-%d', DATE('now', '+9 hours')) AND 종목코드 = P.종목코드)
        ) AS MINUTE_CNT
    FROM 포트폴리오 P
    WHERE SYS_ID = ?
    UNION ALL
    SELECT DISTINCT A.SORT_NO, 100 AS SEQ, A.종목코드, A. 종목명,
                    ? AS tick, 1 AS GEN_GG_TP순서, A.매매구분, 
                    (SELECT CASE  
                        WHEN MAX(등록일자||시간 ) IS NULL THEN 
                            CAST(((
                                    julianday(
                                        CASE 
                                            -- 1. 현재 시간이 15:30:00 이후이면, 기준 시점을 당일 15:30:00으로 고정
                                            WHEN strftime('%H:%M:%S', 'now', '+9 hours') >= '15:30:00' 
                                            THEN strftime('%Y-%m-%d ', 'now', '+9 hours') || '15:30:00'
                                            -- 2. 장중(15:30:00 이전)이면, 현재 시각을 그대로 사용
                                            ELSE strftime('%Y-%m-%d %H:%M', 'now', '+9 hours') || ':00'
                                        END
                                    )
                                    - 
                                    julianday(strftime('%Y-%m-%d', 'now')||' 09:00:00')
                                ) * 24 * 60) / ? AS INTEGER)
                        ELSE CAST(((
                            julianday(CASE 
                                -- 1. 현재 시간이 15:30:00 이후이면, 기준 시점을 당일 15:30:00으로 고정
                                WHEN strftime('%H:%M:%S', 'now', '+9 hours') >= '15:30:00' 
                                THEN strftime('%Y-%m-%d ', 'now', '+9 hours') || '15:30:00'
                                -- 2. 장중(15:30:00 이전)이면, 현재 시각을 그대로 사용
                                ELSE strftime('%Y-%m-%d %H:%M', 'now', '+9 hours') || ':00'
                            END
                            )
                        - 
                            julianday(
                                substr(MAX(등록일자||시간 ), 1, 10) || ' ' || 
                                substr(MAX(등록일자||시간 ), 11, 2) || ':' || 
                                substr(MAX(등록일자||시간 ), 13, 2) || ':' || 
                                substr(MAX(등록일자||시간 ), 15, 2)
                            )) * 24 * 60) / ? AS INTEGER)
                        END AS missing_min_candles
                FROM 분별주가 C 
                    WHERE C.종목코드 = A.종목코드 
                    AND datetime(										
                            substr(등록일자||시간 , 1, 10) || ' ' || 
                            substr(등록일자||시간 , 11, 2) || ':' || 
                            substr(등록일자||시간 , 13, 2) || ':' || 
                            substr(등록일자||시간 , 15, 2)
                        ) < strftime('%Y-%m-%d %H:%M', 'now', '+9 hours')||':00'
                    AND C.등록일자 = DATE('now', '+9 hours')
                    ORDER BY C.등록일자, C.시간
                ) AS missing_min_candles,
                (	
                    SELECT COUNT(시간) 
                    FROM 분별주가 C 
                    WHERE C.종목코드 = A.종목코드 
                    AND datetime(										
                            substr(등록일자||시간 , 1, 10) || ' ' || 
                            substr(등록일자||시간 , 11, 2) || ':' || 
                            substr(등록일자||시간 , 13, 2) || ':' || 
                            substr(등록일자||시간 , 15, 2)
                        ) >= (SELECT CASE WHEN MAX(등록일자) IS NULL 
                                    THEN strftime('%Y-%m-%d',DATE('now', '+9 hours'))||' 09:00:00'
                                    ELSE substr(MAX(등록일자||시간) , 1, 10)||' 12:00:00'
                                END	
                                FROM 분별주가
                                WHERE 1=1
                                AND 등록일자 < strftime('%Y-%m-%d',DATE('now', '+9 hours'))
                                AND 종목코드 = A.종목코드
                                )
                ) AS MINUTE_CNT
    FROM (    
            SELECT *--COUNT(1)
            FROM 검색종목
            WHERE SUBSTR(등록일시, 1, 10) = DATE('now', '+9 hours')
            AND 매매구분 IN (0) --0:대기, 2:매수 
            AND SYS_ID = ?
        ) A
)AA
WHERE 1=1
AND AA.missing_min_candles > 0
ORDER BY  AA.SORT_NO, AA.SEQ, AA.MINUTE_CNT DESC, AA.missing_min_candles
LIMIT ?
"""

UPDATE_SEARCH_STK_SORT_NO = """
UPDATE 검색종목
SET SORT_NO = ?
WHERE SUBSTR(등록일시, 1, 10) = DATE('now', '+9 hours')
AND 종목코드 = ?
AND SYS_ID = ?
"""

GET_PRE_INTRADAY_TRGT_STK_LIST = """--분봉 받은 것 미리 돌려 순위 매기기 용.
WITH TARGET AS (
        select 종목코드 , DATE('now','+9 hours') AS 등록일자
            from 종목코드
            WHERE ETF구분 = 0
            AND 상한가 > 0
            AND 하한가 > 0
            --and COMBINED_SCORE > 70 --not in('')
            and COMBINED_SCORE_CNFDNC > 0
            ORDER BY COMBINED_SCORE_CNFDNC
            limit 300
)
select A.종목코드, (SELECT 종목명 FROM 종목코드 SC WHERE SC.종목코드 = A.종목코드) AS 종목명, 2 AS tick, 0 as GEN_GG_TP순서, 10 AS 매매구분
FROM(
    --SELECT * FROM TARGET
    --EXCEPT
    SELECT A.종목코드, A.등록일자--, COUNT(A.시간) AS CNT
    FROM 분별주가 A, TARGET 
    WHERE A.종목코드 = TARGET.종목코드
    AND A.등록일자 = DATE('now','+9 hours')
    AND A.시간 >= '130000'
    GROUP BY A.종목코드, A.등록일자
    HAVING COUNT(A.시간) >= 30
)A
"""

GET_PRED_TRGT_STK_LIST = """
        SELECT DISTINCT AA.*
        FROM (
            SELECT 0 AS SEQ, 종목코드, 종목명, ? AS tick, 0 as GEN_GG_TP순서, 10 AS 매매구분, 매수일, 매수가, 수량, 300 AS MINUTE_CNT
            FROM 포트폴리오
            WHERE SYS_ID = ?
            UNION ALL
            SELECT DISTINCT A.SEQ, A.종목코드, A. 종목명,
                            ? AS tick, 1 AS GEN_GG_TP순서, A.매매구분,  '', 0, 0,
                            (SELECT COUNT(시간) FROM 분별주가 C WHERE C.종목코드 = A.종목코드 
                            AND C.등록일자 >= (SELECT COALESCE(MAX(등록일자), strftime('%Y-%m-%d', DATE('now', '+9 hours'))) FROM 분별주가 WHERE 등록일자 < strftime('%Y-%m-%d', DATE('now', '+9 hours')) AND 종목코드 = A.종목코드)
                            ) AS MINUTE_CNT
            FROM (    
                  SELECT *--COUNT(1)
	                FROM 검색종목
	                WHERE SUBSTR(등록일시, 1, 10) = DATE('now', '+9 hours')
	                AND 매매구분 IN (0) --0:대기, 2:매수 
                    AND SYS_ID = ?
                ) A
        )AA     
        ORDER BY AA.SEQ, AA.MINUTE_CNT DESC
		LIMIT ?
"""


GET_RSLT_TRADE_SIGNAL = """
    SELECT A.종목코드, A.시간, A.COMBINED_SCORE, A.COMBINED_SCORE_CNFDNC,
        A.TSP, A.STOP_LOSS_PRCE, A.TAKE_PROFIT1, A.TAKE_PROFIT2, A.RISK_REWARD_RATIO, A.TRADE_STRATEGY, A.UP_DOWN_TREND,
        A.CAPITAL, A.TRADE_INFO_JSON
    FROM TB_TRADE_SIGNAL A
    WHERE 1=1
    AND A.종목코드 = '%s'
    AND SUBSTR(A.예측일자, 1, 10) = DATE('now', '+9 hours')
"""
GET_BUYABLE_AMT = """
    SELECT COALESCE(DISTINCT CAPITAL, 100000000) AS CAPITAL
    FROM TB_TRADE_SIGNAL
    WHERE 1=1
    AND SUBSTR(예측일자, 1, 10) = DATE('now', '+9 hours')
"""

UPDATE_BUYABLE_AMT = """
    UPDATE TB_TRADE_SIGNAL
    SET CAPITAL = ?
    WHERE 1=1
    AND SUBSTR(예측일자, 1, 10) = DATE('now', '+9 hours')
"""

UPDATE_TRADE_TP_STAT = """--일별주가가 없는 건들 제외 
UPDATE 검색종목
SET 매매구분 = -9
WHERE 등록일시 = DATE('now', '+9 hours')
AND 종목코드 = ?
AND SYS_ID = ?
"""

UPDATE_BUY_READY_STAT = """--매수/매도되지 않은 종목을 다시 대기상태로 변경 
UPDATE 검색종목
SET 매매구분 = 0
WHERE 등록일시 = DATE('now', '+9 hours')
AND 매매구분 NOT IN (0, 1, 2) --매도/매수/대기 되지 않은 종목 (즉, -1, -9 등)만 0으로 원복
AND SYS_ID = ?
AND NOT EXISTS(
	SELECT 종목코드
    FROM 검색종목 A
    WHERE SUBSTR(A.등록일시, 1, 10) = DATE('now', '+9 hours')
	AND 매매구분 = 0
    AND SYS_ID = ?
)"""


GET_RSLT_BUY_READY_STK_ITEM = """    
SELECT A.종목코드, '1' AS GEN_GG_TP, B.매매구분,
             (SELECT 종목명
                     FROM 종목코드
                     WHERE 종목코드 = A.종목코드) AS 종목명, A.COMBINED_SCORE, A.COMBINED_SCORE_CNFDNC,
         A.TSP, A.STOP_LOSS_PRCE, A.TAKE_PROFIT1, A.TAKE_PROFIT2, A.RISK_REWARD_RATIO, A.TRADE_STRATEGY, A.UP_DOWN_TREND,
         A.CAPITAL, A.TRADE_INFO_JSON,
         C.구분 AS 시장구분,
		 C.AI_PROB,
         C.COMBINED_SCORE AS COMBINED_SCORE,
        0 AS 종가
	FROM TB_TRADE_SIGNAL A, 검색종목 B, 종목코드 C
    WHERE 1=1
    AND A.종목코드 = B.종목코드
    AND A.종목코드 = C.종목코드
    AND B.매매구분 = 0
    AND B.SYS_ID = ?
    AND SUBSTR(A.예측일자, 1, 10) = DATE()
    AND SUBSTR(A.예측일자, 1, 10) = B.등록일시
"""

REPLACE_TB_AI_CONF = """
    REPLACE INTO TB_AI_CONF (CONF_ID, CONF_KEY, CONF_VALUE, CONF_CMNT)
    VALUES (?, ?, ?, ?)
"""

REPLACE_포트폴리오 = """INSERT or REPLACE INTO 포트폴리오 (GG_ID, GG_NM, 포트폴리오키, 매수일, 종목코드, 종목명, 매수가, 수량, 매수후고가, STATUS, SYS_ID) 
            VALUES (?, ?, ?, datetime(?), ?, ?, ?, ?, ?, ?, ?)"""

UPDATE_매수후고가 = """
    UPDATE 포트폴리오 SET
        매수후고가 = ?
    WHERE 포트폴리오키 = ? 
    AND SYS_ID = ?
    AND 매수후고가 < ?
    AND 매수가 < ?
"""

UPDATE_BUY_SPLIT_STEP = """
    UPDATE 포지션 SET
        매수현재회차 = ?,
        매수전체회차 = ?
    WHERE 종목코드 = ? AND SYS_ID = ?
"""

UPDATE_SELL_SPLIT_STEP = """
    UPDATE 포지션 SET
        매도현재회차 = ?,
        매도전체회차 = ?
    WHERE 종목코드 = ? AND SYS_ID = ?
"""


SYNC_포트폴리오 = """INSERT INTO 포트폴리오 (GG_ID, GG_NM, 포트폴리오키, 매수일, 종목코드, 종목명, 매수가, 수량, 매수후고가, STATUS, SYS_ID)
                VALUES (?, ?, ?, datetime(?), ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(SYS_ID, GG_ID, 포트폴리오키) DO UPDATE SET
                    매수가 = excluded.매수가,
                    수량 = excluded.수량,
                    매수후고가 = excluded.매수후고가  -- 어차피 포트폴리오 객체에서 읽어옴.
                    """

PORTFOLIO_DELETE = """
DELETE FROM 포트폴리오 
WHERE GG_ID = '%s' AND SYS_ID = '%d'
"""
PORTFOLIO_DELETE_BY_KEY = (
    """delete from 포트폴리오 where GG_ID=? and SYS_ID = ? and 포트폴리오키=?"""
)


# 코스피, 코스닥 가격변동이력
GET_MARKET_PRICE_HIST = """
    WITH LatestPrices AS (
            SELECT strftime('%Y-%m-%d', 주문시각) AS 날짜, 
                MAX(strftime('%H:%M:%S', 주문시각)) AS 마지막시간,
                시장구분
            FROM 거래주문내역
            WHERE CAST(시장구분 AS INTEGER) = CAST(? AS INTEGER)
            AND 주문시각 >= date('now', '+9 hours', '-20 days')
            AND 매매구분 = '시장정보'
            GROUP BY strftime('%Y-%m-%d', 주문시각)
        )
        SELECT DISTINCT
            strftime('%Y-%m-%d', t.주문시각) AS 날짜,
            t.현재지수,
            t.전일지수,
            t.상승종목수,
            t.하락종목수,
            t.거래량전일대비,
            t.거래대금전일대비,
            t.전일대비구분,
            t.지수등락율,
            t.첫번째등락율,
            t.두번째등락율,
            t.세번째등락율,
            t.네번째등락율,
            t.시장구분
        FROM 거래주문내역 t
        JOIN LatestPrices lp
        ON date(t.주문시각) = lp.날짜
        AND t.주문시각 = (
            SELECT MAX(주문시각)
            FROM 거래주문내역
            WHERE date(주문시각) = lp.날짜
                AND 시장구분 = lp.시장구분
                AND 매매구분 = '시장정보'
        )
        WHERE t.현재지수 > 0
        AND t.시장구분 = lp.시장구분
        AND t.매매구분 = '시장정보'
        ORDER BY t.주문시각
        """

# [Optimizer-Sync] 백테스트용 전체 기간 시장 지수 조회 (최적화 버전)
GET_MARKET_PRICE_HIST_FULL = """
    SELECT 
        strftime('%Y-%m-%d', t1.주문시각) AS 날짜,
        t1.현재지수, t1.전일지수, t1.상승종목수, t1.하락종목수, t1.거래량전일대비, t1.거래대금전일대비,
        t1.전일대비구분, t1.지수등락율, t1.첫번째등락율, t1.두번째등락율, t1.세번째등락율, t1.네번째등락율, t1.시장구분
    FROM 거래주문내역 t1
    INNER JOIN (
        SELECT MAX(주문시각) as max_time, 시장구분
        FROM 거래주문내역
        WHERE 매매구분 = '시장정보'
        AND CAST(시장구분 AS INTEGER) = CAST(? AS INTEGER)
        GROUP BY SUBSTR(주문시각, 1, 10)
    ) t2 ON t1.주문시각 = t2.max_time AND t1.시장구분 = t2.시장구분
    WHERE t1.매매구분 = '시장정보'
    ORDER BY t1.주문시각
"""
# 코스피, 코스닥 가격변동이력
GET_MARKET_PRICE_TODAY_HIST = """
SELECT DISTINCT
    t.주문시각,
    t.현재지수,
    t.전일지수,
    t.상승종목수,
    t.하락종목수,
    t.거래량전일대비,
    t.거래대금전일대비,
    t.전일대비구분,
    t.지수등락율,
    t.첫번째등락율,
    t.두번째등락율,
    t.세번째등락율,
    t.네번째등락율,
    t.시장구분
FROM 거래주문내역 t
WHERE t.매매구분 = '시장정보'
AND SUBSTR(t.주문시각, 1, 10) = date('now', '+9 hours')
AND t.현재지수 > 0
AND t.시장구분 = ?
ORDER BY t.주문시각
"""
# 포트폴리오
GET_PORTFOLIO_LIST = """SELECT * FROM 포트폴리오 WHERE SYS_ID = %d"""

# 일별 주가 데이터 조회 쿼리
GET_DAY_PRICE_BY_STK_CD = """
	SELECT A.날짜, A.시가, A.고가, A.저가, A.종가, A.전일대비구분, A.전일대비, A.등락율, A.누적거래량, A.거래증가율, A.체결강도, A.소진율, A.회전율, A.외인순매수, A.기관순매수, A.종목코드, A.누적거래대금, A.개인순매수, A.시가대비구분, A.시가대비, A.시가기준등락율, A.고가대비구분, A.고가대비, A.고가기준등락율, A.저가대비구분, A.저가대비, A.저가기준등락율, A.시가총액, 
		B.구분 AS 시장구분, B.AI_PROB, B.COMBINED_SCORE
	FROM 일별주가 A
	INNER JOIN 종목코드 B
	ON A.종목코드 = B.종목코드
        WHERE A.종목코드 = ?
        -- 수정된 쿼리 조건
        AND A.날짜 >= strftime('%Y%m%d',DATE('now', '-12 months'))  -- 200일 이상 필수
        --AND A.날짜 < strftime('%Y%m%d', DATE('now')) -- ⚠️ 오늘 장중 데이터는 제외 (일봉 분석의 무결성 확보)
        --AND A.날짜 < strftime('%Y%m%d',DATE('now','-1 days')) -- 오늘 날짜는 제외: 검증용
        ORDER BY A.날짜
    """

# 분별 주가 데이터 조회 쿼리
GET_MIN_PRICE_BY_STK_CD = """
SELECT datetime(
            substr(등록일자||시간 , 1, 10) || ' ' || 
            substr(등록일자||시간 , 11, 2) || ':' || 
            substr(등록일자||시간 , 13, 2) || ':' || 
            substr(등록일자||시간 , 15, 2)
        ) AS 등록일시, 종목코드, 등록일자, 시간, 종가, 전일대비구분, 전일대비 , 등락율 , 체결강도 , 매도체결수량 , 매수체결수량 , 순매수체결량 , 매도체결건수,
        매수체결건수, 순체결건수, 거래량 , 시가, 고가, 저가, 체결량, 매도체결건수시간, 매수체결건수시간, 매도잔량 , 매수잔량 , 
        시간별매도체결량, 시간별매수체결량 
FROM 분별주가 
WHERE 종목코드 = ?
AND datetime(
        substr(등록일자||시간 , 1, 10) || ' ' || 
        substr(등록일자||시간 , 11, 2) || ':' || 
        substr(등록일자||시간 , 13, 2) || ':' || 
        substr(등록일자||시간 , 15, 2)
    ) >= ?
ORDER BY 등록일자, 시간
"""

# Phase 7: 증분 업데이트 캐시 전용 분별 주가 데이터 조회 쿼리 (가장 최근 로드한 일시 이후의 데이터만)
GET_MIN_PRICE_SINCE_TIME = """
SELECT datetime(
            substr(등록일자||시간 , 1, 10) || ' ' || 
            substr(등록일자||시간 , 11, 2) || ':' || 
            substr(등록일자||시간 , 13, 2) || ':' || 
            substr(등록일자||시간 , 15, 2)
        ) AS 등록일시, 종목코드, 등록일자, 시간, 종가, 전일대비구분, 전일대비 , 등락율 , 체결강도 , 매도체결수량 , 매수체결수량 , 순매수체결량 , 매도체결건수,
        매수체결건수, 순체결건수, 거래량 , 시가, 고가, 저가, 체결량, 매도체결건수시간, 매수체결건수시간, 매도잔량 , 매수잔량 , 
        시간별매도체결량, 시간별매수체결량 
FROM 분별주가 
WHERE 종목코드 = ?
AND datetime(
        substr(등록일자||시간 , 1, 10) || ' ' || 
        substr(등록일자||시간 , 11, 2) || ':' || 
        substr(등록일자||시간 , 13, 2) || ':' || 
        substr(등록일자||시간 , 15, 2)
    ) > ?
ORDER BY 등록일자, 시간
"""

# 거래 분석
GET_TRADE_ANALYZE = """
SELECT MIN(AAA.매수시각) AS SEQ, AAA.*
FROM(
    SELECT AA.매수시각, AA.매도시각, AA.종목코드, AA.종목명, AA.매매구분, AA.매수가 , AA.매도가, AA.수량, AA.수익금액 ,
            CASE
            WHEN AA.매도가 > 0 THEN ROUND((AA.매도가 - AA.매수가) * 100.0 / AA.매수가,2)
            ELSE 0
        END AS 수익률,
        AA.등락율,
        AA.전일지수,
        AA.현재지수,
        AA.시가지수,
        AA.저가지수,
        AA.고가지수,
        AA.전일대비구분,
        AA.지수등락율,
        AA.거래량전일대비,
        AA.거래대금전일대비,
        AA.시가시간,
        AA.시가등락율,
        AA.고가시간,
        AA.고가등락율,
        AA.저가시간,
        AA.저가등락율,
        AA.첫번째등락율,
        AA.두번째등락율,
        AA.세번째등락율,
        AA.네번째등락율,
        AA.상승종목수,
        AA.보합종목수,
        AA.하락종목수
    FROM (
        --매도된건
        WITH TRADE_COMP AS (
            SELECT  A.매수일 AS 매수시각, A.주문시각 AS 매도시각, A.*
            FROM 거래주문내역 A 
            WHERE SUBSTRING(A.매수일,1,10) >= strftime('%Y-%m-%d',date('now', '-1 months'))
            AND A.매매구분 != '매수'
            AND  A.매수일 != ''
            AND  A.매매구분 != '일괄매도'
        )
        SELECT  A.주문시각 AS 매수시각, '' AS 매도시각, A.*
        FROM 거래주문내역 A 
        WHERE SUBSTRING(A.주문시각,1,10) >= strftime('%Y-%m-%d',date('now', '-1 months'))
        AND A.매매구분 = '매수'
        AND SUBSTRING(A.주문시각,1,10)||A.종목코드 NOT IN (
            SELECT  SUBSTRING(매수시각,1,10)||종목코드
            FROM TRADE_COMP
        )
        UNION ALL
        SELECT  *
        FROM TRADE_COMP 
    )AA
    WHERE 1=1
    AND AA.매도가 > 0
)AAA
GROUP BY SUBSTRING(AAA.매수시각,1,10),  AAA.종목코드
order by AAA.매도시각, AAA.매수시각 
"""

GET_TODAY_LOSS_STOCKS = """
SELECT 종목코드, MAX(주문시각) as LAST_LOSS_TIME, COUNT(*) as LOSS_COUNT
FROM 거래주문내역
WHERE SUBSTR(주문시각, 1, 10) = DATE('now', '+9 hours')
AND (매도가 - 매수가) < 0
AND (매도가 > 0)
GROUP BY 종목코드
"""
