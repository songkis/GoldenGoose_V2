# -*- coding: utf-8 -*-
"""
Improved combined selector v2
- 기존 adaptive_swing_trading 기반 결합
- 추가: 사용자 제안 '2-day breakout + 6-month average + volume surge'
- 추가: trend-align(MA), volume surge, RS vs market, ADX, RSI 보너스/패널티
- 반환: 각 종목 entry/stop/tp1/tp2, qty, combined_score, debug fields
"""
import os
import sys
# 부모 디렉토리(GoldenGoose_V2)를 거쳐 GG_Shared를 참조
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'GG_Shared')))

import datetime
import gc

import multiprocessing

import sqlite3
import sys
from pathlib import Path


import pandas as pd


from config.ai_settings import (
    BASED_PURCHASE_PRICE,
    PARAMS,
    BUY_TOP_CNT,
    SYS_ID,
    TIME_STEP,
)
from config.log_settings import setup_logger
from SQL.sql import (
    DEL_POSITION,
    GET_BUYABLE_AMT,
    GET_PORTFOLIO_LIST,
    GET_STOCK_LIST,
    INIT_ALL_STK_ITEM,
    INIT_POSITION,
)
from strategy.core.TradingComm import select_candidates_parallel, tradingComm_set_logger
from util.CommUtils import (
    get_db_connection,
    load_data,
    set_commutils_logger,
    to_numeric_safe,
)
from config.telegram_setting import ToTelegram

logger = setup_logger(Path(__file__).stem)
set_commutils_logger(logger)
tradingComm_set_logger(logger)

logger.debug(f"{Path(__file__).name}사용된 모듈: {sys.modules.keys()}")


# 3) 전체 종목코드로 data_pool 구성


def fetch_datas(tickers: list) -> dict:
    """
    [ v2.2] 5일 평균 유동성 가드 (Flash Volume Defense)
    - SQL 1차 프루닝 이후, 5일 평균 거래대금이 80억 미만인 '가짜 수급' 종목을 2차로 차단.
    """
    data_pool = {}
    for t in tickers:
        try:
            df = load_data(t, "일별주가")
            if len(df) < TIME_STEP:
                continue

            # [Pandas Layer] 5일 평균 거래대금 80억 허들 체크
            if "누적거래대금" in df.columns:
                avg_turnover_5d = df["누적거래대금"].tail(5).mean()
                if avg_turnover_5d < 8000:  # 80억 미만 Drop
                    # logger.info(f"[{t}] 5일 평균 유동성 부족 ({avg_turnover_5d:.1f}억) -> PASS")
                    continue

            data_pool[t] = df
        except Exception as e:
            logger.error(f"{t} 종목 일봉 로드 오류: {e}")

    return data_pool


# def picking_stocks(stock_cd, params):
def picking_stocks(candidates, params):
    #'ticker': '002620', 'entry_price': 8790.0, 'exit_levels': {'stop_loss': 8300.6, 'take_profit1': 9768.8, 'take_profit2': 9136.866666666667, 'risk_reward_ratio': 2.0}, 'swing_score': np.float64(76.0), 'lw_score': 50, 'lw_strength': 2, 'combined_score': np.float64(69.6), 'qty': 1422, 'est_risk_pct': 5.568, 'latest_row': {'날짜': '20251002', '시가': 8690, '고가': 8820, '저가': 8610, '종가': 8790, '전일대비구분': '2', '전일대비': 20, '등락율': 0.23, '거래량': 10856, '거래증가율': -24.51, '체결강도': 194.45, '소진율': 10.04, '회전율': 0.07, '외인순매수': 657, '기관순매수': -1310, '종목코드': '002620', '누적거래대금': 95, '개인순매수': 753, '시가대비구분': '5', '시가대비': -80, '시가기준등락율': -0.91, '고가대비구분': '2', '고가대비': 50, '고가기준등락율': 0.57, '저가대비구분': '5', '저가대비': -160, '저가기준등락율': -1.82, '시가총액': 140406, '시장구분': 1, 'SMA_5': 8700.0, 'EMA_5': 8729.683046732895, 'SMA_10': 8723.0, 'EMA_10': 8722.569948932382, 'SMA_20': 8739.0, 'EMA_20': 8728.579543652762, 'SMA_50': 8722.8, 'EMA_50': 8725.535235978048, 'RSI_7': 56.271120704055036, 'RSI_14': 52.46127720050192, 'RSI_21': 51.62974496141036, 'MACD': -8.98140209309895, 'MACD_Signal': -11.833477325187477, 'MACD_Hist': 2.852075232088527, 'BB_Upper': 8928.515170896688, 'BB_Lower': 8549.484829103312, 'BB_Width': 4.337227849792603, 'BB_%B': 0.6345538717525736, 'ATR': 279.91274640691046, 'ATR_Percent': 3.1844453516144533, 'Volume_SMA': 23638.05, 'Volume_Ratio': 0.45925954129041946, 'OBV': 1020060, 'OBV_SMA': 984062.9, 'Swing_High': 0, 'Swing_Low': 0, 'Support': 8470.0, 'Resistance': 9323.333333333334, 'Market_Regime': 'neutral', 'Swing_Score': 9, 'Price_Rate': 0.01501154734411081, 'Volume_Rate': 0.5123989969350795}}
    with get_db_connection() as conn:  # with 문을 사용하여 자동으로 conn.close()
        #  옵티마이저의 최하단 방어선(B급 커트라인)을 야간 배치 필터에 동기화
        if params is None:
            params = {}
        base_floor = params.get("b_threshold_floor", 45.0)
        try:
            conn.execute(
                "UPDATE 종목코드 SET COMBINED_SCORE_CNFDNC = 0.0"
            )  # 당일 배치에서 처리되었으므로 초기화
            conn.commit()
            idx = 0
            combined_score = 0.0
            day_acc_score = 0.0
            # for candidate_data in candidate:
            for candidate_data in candidates:
                idx += 1
                # 1. [Fail-Fast] 데이터가 불완전하거나 유효하지 않으면 즉시 스킵
                stock_cd = candidate_data.get("ticker", "000000")
                latest_daily = candidate_data.get("latest_row")
                is_valid = candidate_data.get("is_valid", True)

                if not is_valid or latest_daily is None:
                    fail_reason = candidate_data.get(
                        "fail_reason", "데이터 누락(latest_row)"
                    )
                    logger.warning(f"[{stock_cd}] 분석 스킵: {fail_reason}")
                    continue

                entry_price = candidate_data.get("entry_price", 0)
                combined_score = candidate_data.get("combined_score", 0.0)
                day_acc_score = candidate_data.get("day_acc_score", 0.0)
                prev_day_close_price = to_numeric_safe(latest_daily.get("종가", 0))

                #  야간/장전 배치 시점에는 실시간 매수세가 없어 trigger_hit=False로
                # combined_score가 0점으로 반환됩니다. 따라서 일봉 기초체력인 daily_score를 끌어와 사용합니다.
                v3_indicators = candidate_data.get("v3_indicators", {})
                daily_score = float(
                    v3_indicators.get("final_stock_eval", {}).get("daily_score", 0.0)
                    or 0.0
                )

                # [Batch-Fallback] 야간 배치 모듈이 생성한 swing_score 끌어오기
                if daily_score <= 0.0:
                    daily_score = float(candidate_data.get("swing_score", 0.0) or 0.0)
                if daily_score <= 0.0 and latest_daily:
                    daily_score = float(latest_daily.get("Swing_Score", 0.0) or 0.0)

                # combined_score가 0점으로 깎였다면 원래의 기초체력(daily/swing) 점수로 대체하여 판별
                combined_score = float(combined_score)
                evaluation_score = (
                    daily_score if combined_score <= 0.0 else combined_score
                )

                quantity, stop_loss, take_profit1, take_profit2, trade_strategy = (
                    0,
                    0,
                    0,
                    0,
                    "breakout",
                )
                logger.debug(
                    f"[{stock_cd}] evaluation_score(daily): {evaluation_score}, day_acc_score: {day_acc_score}"
                )

                if (
                    evaluation_score >= base_floor
                    and day_acc_score >= (base_floor * 0.8)  # 가속도 허들도 동반 완화
                    and prev_day_close_price > BASED_PURCHASE_PRICE
                ):
                    logger.info(
                        f"{idx} ===>>> {stock_cd} 매매대상 확정 ===>>> score: {evaluation_score}, day_acc: {day_acc_score}"
                    )
                    # DB 저장 (highest_price 추가)

                # StockPicking에서 등록한 기본정보 0으로 등록된 건 갱신
                query = """
                    INSERT INTO 포지션(
                        종목코드, 진입일자, 진입가, 수량, 청산가, 목표가1, 목표가2, 최고가, 상태, 매매기법, SYS_ID
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(종목코드)
                DO UPDATE SET
                    진입일자 = CASE WHEN 포지션.상태 = 'open' THEN 포지션.진입일자 ELSE excluded.진입일자 END,
                    진입가 = CASE WHEN 포지션.상태 = 'open' THEN 포지션.진입가 ELSE excluded.진입가 END,
                    수량 = CASE WHEN 포지션.상태 = 'open' THEN 포지션.수량 ELSE excluded.수량 END,
                    청산가 = CASE WHEN 포지션.상태 = 'open' THEN 포지션.청산가 ELSE excluded.청산가 END,
                    목표가1 = CASE WHEN 포지션.상태 = 'open' THEN 포지션.목표가1 ELSE excluded.목표가1 END,
                    목표가2 = CASE WHEN 포지션.상태 = 'open' THEN 포지션.목표가2 ELSE excluded.목표가2 END,
                    최고가 = CASE WHEN 포지션.상태 = 'open' THEN 포지션.최고가 ELSE excluded.최고가 END,
                    상태 = CASE WHEN 포지션.상태 = 'open' THEN 포지션.상태 ELSE excluded.상태 END,
                    매매기법 = CASE WHEN 포지션.상태 = 'open' THEN 포지션.매매기법 ELSE excluded.매매기법 END,
                    SYS_ID = excluded.SYS_ID
                """
                conn.execute(
                    query,
                    (
                        stock_cd,
                        datetime.datetime.now().strftime("%Y-%m-%d"),
                        round(float(entry_price), 2),
                        quantity,
                        stop_loss,
                        take_profit1,
                        take_profit2,
                        round(float(entry_price), 2),  # 최초 진입가를 최고가로 설정
                        "regist",
                        trade_strategy,
                        SYS_ID,
                    ),
                )

                update_score_info(
                    stock_cd,
                    evaluation_score,
                    idx,
                )

                # del daily_data  # , features, data, scaler, scaled_data, X, y
        except sqlite3.OperationalError as e:
            logger.error(f"picking_stocks error: {e}")
        finally:
            gc.collect()


def update_score_info(
    stock_cd,
    combined_score,
    combined_score_cnfdnc=0,
):
    with get_db_connection() as conn:  # with 문을 사용하여 자동으로 conn.close()
        logger.info(
            f"[update_score_info] 종목코드: {stock_cd}, combined_score: {combined_score}, combined_score_cnfdnc: {combined_score_cnfdnc}"
        )
        curr_dt = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            query = f"""UPDATE 종목코드 SET COMBINED_SCORE = {combined_score}, 
                                        COMBINED_SCORE_CNFDNC = {combined_score_cnfdnc}, 
                                        예측일시='{curr_dt}'
                                        WHERE 종목코드='{stock_cd}'
                                        """
            conn.execute(query)
            # 백업&분석용
            query = f"""UPDATE 일별주가 SET COMBINED_SCORE = {combined_score}, 
                                        COMBINED_SCORE_CNFDNC = {combined_score_cnfdnc}, 
                                        예측일시='{curr_dt}'
                                        WHERE 종목코드='{stock_cd}'
                                        AND   날짜 = (SELECT MAX(날짜) FROM 일별주가 WHERE 종목코드='{stock_cd}')
                                        """
            conn.execute(query)
        except sqlite3.OperationalError as e:
            logger.error(f"SQLite error: {e}")


def process_in_chunks(func, data_list, pool, chunk_size, 포트폴리오리스트, params):
    for i in range(0, len(data_list), chunk_size):
        chunk = data_list[i : i + chunk_size]
        # logger.info(f"Processing chunk: {i}, {chunk}, {params}")
        starmap_args = [
            (candidate_data, 포트폴리오리스트, params) for candidate_data in chunk
        ]
        pool.starmap(func, starmap_args)
    gc.collect()


def notify_result():
    try:
        with get_db_connection() as conn:  # with 문을 사용하여 자동으로 conn.close()
            #  텔레그램 알림 카운트 기준도 옵티마이저 하한선에 맞춤
            query = GET_STOCK_LIST + f" AND A.COMBINED_SCORE_CNFDNC > 0"
            종목코드테이블 = pd.read_sql_query(query, con=conn)
        kospi_codes = 종목코드테이블.query("구분==1")["종목코드"].values.tolist()
        kosdaq_codes = 종목코드테이블.query("구분==2")["종목코드"].values.tolist()
        # resultMessage = f"GoldenGoose 어제+{PREDICTION_HORIZON}일 후 {float(RISE_RATE-1)*100:.02f}%이상 상승 예측되는 종목찾기 종료됨!"
        resultMessage = f"GoldenGoose 최고점 종목추출 종료됨!"
        resultMessage += f"\n코스피 매수대상 종목 수 : {len(kospi_codes)}"
        resultMessage += f", 코스닥 매수대상 종목 수 : {len(kosdaq_codes)}"
        ToTelegram(f"" + resultMessage)
        logger.info(f"resultMessage : {resultMessage}")

    except sqlite3.OperationalError as e:
        logger.error(f"SQLite error: {e}")
        return


def extract_target_tickers(data_pool: dict) -> list:
    """
    [Math Rigor] 상승초입 수급 눌림목 무결성 검증 로직
    - 조건 1: 최근 5일 내 60일선/120일선 상향 돌파
    - 조건 2: 최근 3일 누적 기관/외인 순매수 > 0
    - 조건 3: 가격 눌림(P < P-1) & 거래량 급감(V < V-1 * 0.5)
    """
    candidates = []
    import json

    for ticker, df in data_pool.items():
        try:
            if len(df) < 120:
                continue

            # 1. 기술적 지표 계산 (MA)
            df["ma60"] = df["종가"].rolling(60).mean()
            df["ma120"] = df["종가"].rolling(120).mean()

            # 조건 1: 5일 내 돌파 이력
            recent_5 = df.tail(5)
            # 단순 근사: 현재가 > MA60 이면서 5일 전에는 아래였거나, 5일 내 돌파 발생
            has_ma_breakout = (recent_5["종가"] > recent_5["ma60"]).any() or (
                recent_5["종가"] > recent_5["ma120"]
            ).any()

            if not has_ma_breakout:
                continue

            # 조건 2: 수급 (최근 3일 누적)
            recent_3 = df.tail(3)
            #  Series.sum() 호출 무결성 확보
            inst_net = pd.to_numeric(recent_3["기관순매수"], errors="coerce").sum()
            fore_net = pd.to_numeric(recent_3["외인순매수"], errors="coerce").sum()
            supply_power = float(inst_net + fore_net)

            if supply_power <= 0:
                continue

            # 조건 3: 눌림목 무결성 (당일 데이터)
            curr = df.iloc[-1]
            prev = df.iloc[-2]

            is_pullback = curr["종가"] < prev["종가"]
            is_vol_dry = curr["거래량"] < prev["거래량"] * 0.5

            if not (is_pullback and is_vol_dry):
                continue

            # 모든 조건 통과 시 후보 등록
            candidates.append(
                {
                    "ticker": ticker,
                    "supply_power": float(supply_power),
                    "last_close": float(curr["종가"]),
                }
            )

        except Exception as e:
            logger.error(f"[{ticker}] extract_target_tickers filtering error: {e}")

    # 수급 강도 순 정렬 후 상위 100개 추출
    candidates = sorted(candidates, key=lambda x: x["supply_power"], reverse=True)[
        :BUY_TOP_CNT
    ]

    # [Data Handover] ticker_candidates.json 형태로 Export
    handover_data = {c["ticker"]: c["supply_power"] for c in candidates}
    try:
        with open("ticker_candidates.json", "w", encoding="utf-8") as f:
            json.dump(handover_data, f, indent=4, ensure_ascii=False)
        logger.info(
            f"✨ [Alpha Sync] {len(candidates)} tickers exported to ticker_candidates.json"
        )
    except Exception as e:
        logger.error(f"Failed to export ticker_candidates.json: {e}")

    return candidates


class StockPicking:
    def __init__(self):
        self.initialize()

    def initialize(self):
        self.main()

    def main(self):
        with get_db_connection() as conn:  # with 문을 사용하여 자동으로 conn.close()
            cursor = conn.cursor()
            try:
                # 날짜가 바뀌면 모두 NULL
                query = INIT_ALL_STK_ITEM
                cursor.execute(query)

                query = DEL_POSITION  # strftime('%Y-%m-%d', 'now',  '-120 days')
                data = [SYS_ID, SYS_ID]
                cursor.execute(query, data)

                # 포지션 상태 업데이트 삭제후 남은 것은 내일 또 사용
                query = INIT_POSITION  # strftime('%Y-%m-%d', 'now',  '-120 days')
                data = [SYS_ID]
                cursor.execute(query, data)

                # 같은 날짜면 남은 것만 진행.
                query = (
                    GET_STOCK_LIST
                    + " AND COALESCE(A.예측일시, DATE('now', '-1 days')) < DATE('now','+9 hours')"
                )
                # 포트폴리오 검증용
                # query = GET_PORTFOLIO_LIST%(SYS_ID)
                종목코드테이블 = pd.read_sql_query(query, con=conn)

                query = GET_PORTFOLIO_LIST % (SYS_ID)
                포트폴리오테이블 = pd.read_sql_query(query, con=conn)
                포트폴리오리스트 = 포트폴리오테이블["종목코드"].tolist()

                # 날짜가 바뀌면 모두 NULL
                query = GET_BUYABLE_AMT
                cursor.execute(query)
                row = cursor.fetchone()
                BUYABLE_AMT = row[0] if row else None

                if not BUYABLE_AMT:
                    BUYABLE_AMT = 20_000_000
                종목코드리스트 = 종목코드테이블["종목코드"].tolist()  # [:50]

                logger.info(
                    f"작업종목코드리스트: {len(종목코드리스트)},  BUYABLE_AMT: {BUYABLE_AMT}"
                )  # .empty()는 리스트에 사용할 수 없습니다.

                if len(종목코드리스트) == 0:  # 리스트의 길이를 확인
                    logger.warning("종목코드리스트가 비어 있습니다. 작업을 종료합니다.")
                    return
                # [ v2.2] RS 지수 데이터 로딩 및 정합성 확보 ---
                from SQL.sql import GET_INDEX_DAILY_PRICE

                index_data = {}
                try:
                    for idx_cd in ["001", "201"]:
                        idx_df = pd.read_sql_query(
                            GET_INDEX_DAILY_PRICE, conn, params=(idx_cd,)
                        )
                        if not idx_df.empty:
                            idx_df["날짜"] = pd.to_datetime(idx_df["날짜"])
                            idx_df.set_index("날짜", inplace=True)
                            # fillna for safety
                            idx_df = idx_df.ffill().bfill()
                            index_data[idx_cd] = idx_df
                    logger.info(
                        f"지수 데이터 로드 완료 (KOSPI: {len(index_data.get('001', []))}, KOSDAQ: {len(index_data.get('201', []))})"
                    )
                except Exception as e:
                    logger.error(f"지수 데이터 로드 중 오류 발생: {e}")

                # [Parallel Optimization]
                # Use parallel processing for candidate selection
                target_data_pool = fetch_datas(종목코드리스트)
                logger.info(f"추천가능종목리스트 : {len(target_data_pool)}")

                if not target_data_pool:
                    logger.warning(
                        "유동성 및 기초 조건을 통과한 추천가능종목이 0건입니다. 예측을 종료합니다."
                    )
                    return

                candidates_pool = select_candidates_parallel(
                    data_pool=target_data_pool,
                    index_data=index_data,  # [v2.2] RS 계산용 지수 전달
                    min_candidates=len(종목코드리스트),  # 전체에서 고르기.
                    params=PARAMS,  #  Inject global PARAMS
                    capital=BUYABLE_AMT,
                    port_list=포트폴리오리스트,
                    tp="picking",
                )
                logger.info(
                    f"==------->> 매수추천(손절,목표1,2 생성) 종목코드수: {len(candidates_pool)}"
                )
                # [New] Alpha Homogeneity: 100종목 수급 압축 및 ticker_candidates.json 생성
                # extract_target_tickers expects a dict of DataFrames. We filter the pool to scored candidates.
                scored_data_pool = {
                    c["ticker"]: target_data_pool[c["ticker"]]
                    for c in candidates_pool
                    if c["ticker"] in target_data_pool
                }
                alpha_results = extract_target_tickers(scored_data_pool)
                passed_tickers = [r["ticker"] for r in alpha_results]

                # [Math Rigor] 1. Prioritize stocks that passed strict Alpha filtering (sorted by supply_power)
                alpha_ticker_map = {c["ticker"]: c for c in candidates_pool}
                candidate_final = [
                    alpha_ticker_map[t] for t in passed_tickers if t in alpha_ticker_map
                ]

                # [Math Rigor] 2. Fill the gap to reach BUY_TOP_CNT using next best candidates from general pool (sorted by combined_score)
                if len(candidate_final) < BUY_TOP_CNT:
                    logger.info(
                        f"Alpha-Filtering yielded {len(candidate_final)} stocks. Filling gap to {BUY_TOP_CNT} from top-scored pool."
                    )
                    already_selected = set(passed_tickers)
                    remaining = [
                        c
                        for c in candidates_pool
                        if c["ticker"] not in already_selected
                    ]
                    # candidates_pool is already ranked by combined_score
                    gap_fill = remaining[: BUY_TOP_CNT - len(candidate_final)]
                    candidate_final.extend(gap_fill)

                logger.info(
                    f"==------->> 최종 매수대상 종목코드수: {len(candidate_final)}"
                )
                params = PARAMS
                picking_stocks(candidate_final, params)

                notify_result()
                del target_data_pool
                del candidates_pool
                gc.collect()
            except sqlite3.OperationalError as e:
                logger.error(f"SQLite error: {e}")
                return
            finally:
                if "cursor" in locals() and cursor:
                    cursor.close()

                logger.info(f"[{self.__class__.__name__}] Finalizing process...")

                for handler in logger.handlers[:]:
                    handler.flush()
                    logger.removeHandler(handler)
                    handler.close()


if __name__ == "__main__":
    import multiprocessing
    import sys

    #  exe 환경에서의 멀티프로세싱 크래시 방어 (유지)
    multiprocessing.freeze_support()

    try:
        stockPicking = StockPicking()
    except Exception as e:
        print(f"StockPicking Execution Fatal Error: {e}")
        sys.exit(1)

    sys.exit(0)
