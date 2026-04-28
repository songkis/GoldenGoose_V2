import pandas as pd
import numpy as np
import datetime
from datetime import timedelta
from strategy.core.trading_models import MarketConditions

logger = None

# 캐시 저장용 딕셔너리
market_cache = {}  # { 종목코드: (데이터프레임, 마지막갱신시간) }
MIN_TERM = 1  # 1분마다 데이터 갱신
M_CACHE_EXPIRY = timedelta(minutes=MIN_TERM)

market_index_changes_cache = {}


def set_logger(external_logger):
    """외부 로거 설정"""
    global logger
    logger = external_logger


__all__ = [
    "analyze_market_conditions",
    "get_market_energy_score_df_v6_2",
    "save_market_status_to_db",
    "get_segmented_market_regime",
    "format_combined_market_status",
    "get_market_momentum_status",
]


def format_combined_market_status(kospi_cond, kosdaq_cond):
    """
    KOSPI/KOSDAQ 시장 상태를 하나의 메시지로 결합 (Telegram용)
    """

    def get_emoji(regime):
        regime = str(regime).upper()
        if regime == "BULL":
            return "🚀"
        if regime == "BEAR":
            return "📉"
        if regime == "CRASH":
            return "🚨"
        return "⚖️"

    # Regime에 따라 색상과 이모지를 조합한 텍스트 생성
    k_regime = (
        kospi_cond.market_regime if hasattr(kospi_cond, "market_regime") else "NEUTRAL"
    )
    q_regime = (
        kosdaq_cond.market_regime
        if hasattr(kosdaq_cond, "market_regime")
        else "NEUTRAL"
    )

    k_change = (
        kospi_cond.current_index_change
        if hasattr(kospi_cond, "current_index_change")
        else 0.0
    )
    q_change = (
        kosdaq_cond.current_index_change
        if hasattr(kosdaq_cond, "current_index_change")
        else 0.0
    )

    k_adr = kospi_cond.adr_ratio if hasattr(kospi_cond, "adr_ratio") else 0.0
    q_adr = kosdaq_cond.adr_ratio if hasattr(kosdaq_cond, "adr_ratio") else 0.0

    msg = (
        f"📊 **Market Pulse**\n"
        f"▫️ KOSPI: {get_emoji(k_regime)} {k_regime} ({k_change:+.2f}%, ADR: {k_adr:.2f})\n"
        f"▫️ KOSDAQ: {get_emoji(q_regime)} {q_regime} ({q_change:+.2f}%, ADR: {q_adr:.2f})"
    )
    return msg


def fetch_trade_data(market_tp_cd):
    from SQL.sql import GET_MARKET_PRICE_HIST

    try:
        # 데이터베이스 연결
        from util.CommUtils import get_db_connection

        with get_db_connection() as conn:  # with 문을 사용하여 자동으로 conn.close()
            query = GET_MARKET_PRICE_HIST
            df = pd.read_sql_query(query, conn, params=(int(market_tp_cd),))
            return df
    except Exception as e:
        if logger:
            logger.info(f"거래 데이터 조회 중 오류: {e}")
        return pd.DataFrame()


# Hysteresis State Storage (Per Market)
market_regime_state = {
    "KOSPI": {
        "current_regime": "NEUTRAL",
        "pending_regime": "NEUTRAL",
        "confirmation_count": 0,
        "last_updated": None,
    },
    "KOSDAQ": {
        "current_regime": "NEUTRAL",
        "pending_regime": "NEUTRAL",
        "confirmation_count": 0,
        "last_updated": None,
    },
}


def save_market_status_to_db(market_type, regime, change, adr):
    """최종 결정된 시장 정보를 DB에 기록 (SSOT)"""
    try:
        from util.CommUtils import get_db_connection

        with get_db_connection() as conn:
            cursor = conn.cursor()
            # 테이블 생성 (없을 경우)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS TB_MARKET_STATUS (
                    MARKET_TYPE TEXT PRIMARY KEY,
                    REGIME TEXT,
                    INDEX_CHANGE REAL,
                    ADR_RATIO REAL,
                    LAST_UPDATED DATETIME
                )
            """)
            cursor.execute(
                """
                INSERT OR REPLACE INTO TB_MARKET_STATUS (MARKET_TYPE, REGIME, INDEX_CHANGE, ADR_RATIO, LAST_UPDATED)
                VALUES (?, ?, ?, ?, ?)
            """,
                (market_type, regime, change, adr, datetime.datetime.now()),
            )
            conn.commit()
    except Exception as e:
        if logger:
            logger.error(f"save_market_status_to_db Error: {e}")


def get_segmented_market_regime():
    """
    [ SSOT] 각 시장별(KOSPI/KOSDAQ) 현재 국면 정보를 반환
    하나의 지수가 무너지더라도 다른 지수의 고유 국면을 보존하여 개별 종목 필터링에 활용함.
    """
    global market_regime_state
    return {
        "KOSPI": market_regime_state["KOSPI"]["current_regime"],
        "KOSDAQ": market_regime_state["KOSDAQ"]["current_regime"],
    }


def analyze_market_conditions(market_tp_cd_or_df):
    """
    시장상황 데이터를 가져오고 캐싱하는 함수 (Dictionary 리턴 방식)
    [Enhanced] Hysteresis, Regime Classification, Fail-Safe
    [Backtest-Sync] DataFrame 직접 입력 지원
    """
    global market_regime_state
    now = datetime.datetime.now()

    if isinstance(market_tp_cd_or_df, (pd.DataFrame, pd.Series)):
        market_data = market_tp_cd_or_df
        market_type = "KOSDAQ"  # 시뮬레이션 기본값
        is_simulation = True
    else:
        market_tp_cd = market_tp_cd_or_df
        market_type = "KOSPI" if market_tp_cd == 1 else "KOSDAQ"
        is_simulation = False

    # 1. 캐시 확인 및 반환 (시뮬레이션 환경에서는 캐시 우회)
    if not is_simulation and market_type in market_cache:
        cached_data = market_cache[market_type]
        if now - cached_data.last_updated < M_CACHE_EXPIRY:
            # Hysteresis 상태 병합하여 반환
            cached_data.market_regime = market_regime_state[market_type][
                "current_regime"
            ]
            return cached_data

    # 2. 데이터 페칭 (시뮬레이션 시에는 입력받은 데이터 사용)
    if not is_simulation:
        market_data = fetch_trade_data(market_tp_cd)

    # [Fail-Safe] 데이터 페칭 실패 시 -> CRASH 모드 (보수적 대응)
    if market_data.empty:
        logger.error(
            f"[{market_type}] Market Data Fetch Failed -> Defaulting to CRASH Regime"
        )
        return MarketConditions(
            market_type=market_type,
            buy_condition=False,
            sell_condition=True,
            current_index=0.0,
            current_index_change=0.0,
            high_rate=0.0,
            low_rate=0.0,
            gap_rate=0.0,
            market_ma5_dist=0.0,
            index_return_5d=0,
            market_dip_rate=3.0,
            rising_stocks=0,
            falling_stocks=0,
            adr_ratio=0.0,
            recent_index_changes=[],
            market_data=None,
            last_updated=now,
            market_score=0,
            market_regime="CRASH",
        )

    try:
        #  Default Regime is NEUTRAL
        raw_regime = "NEUTRAL"

        # 최근 데이터 및 수치 정규화
        recent_data = market_data.iloc[-1]
        current_index = float(str(recent_data["현재지수"]).replace(",", ""))
        current_index_change = float(recent_data["지수등락율"])
        rising_stocks = int(recent_data["상승종목수"])
        falling_stocks = int(recent_data["하락종목수"])

        #  데이터 정합성 검증 (상승+하락 종목이 0이면 아직 장중 데이터가 오지 않은 것)
        if (rising_stocks + falling_stocks == 0) and not is_simulation:
            if logger:
                logger.warning(
                    f"⚠️ [{market_type}] Breadth data is zero. Skipping update to prevent stale 'CRASH' regime."
                )
            return market_cache.get(
                market_type,
                MarketConditions(
                    market_type=market_type, market_regime="NEUTRAL", market_energy=1.0, last_updated=now
                ),
            )

        money_diff = float(recent_data["거래대금전일대비"])

        high_rate = float(recent_data["두번째등락율"])
        low_rate = float(recent_data["세번째등락율"])
        gap_rate = float(recent_data["네번째등락율"])

        # MA5 & MA20 Trend Calculation
        close_series = (
            market_data["현재지수"].astype(str).str.replace(",", "").astype(float)
        )
        if len(close_series) >= 20:
            ma5 = close_series.rolling(window=5).mean().iloc[-1]
            ma20 = close_series.rolling(window=20).mean().iloc[-1]
            market_ma5_dist = (current_index / ma5) - 1.0 if ma5 > 0 else 0.0
            market_ma20_dist = (current_index / ma20) - 1.0 if ma20 > 0 else 0.0
            index_slope = (
                (current_index - close_series.iloc[-5]) / close_series.iloc[-5] * 100
            )
        elif len(close_series) >= 5:
            ma5 = close_series.rolling(window=5).mean().iloc[-1]
            ma20 = ma5
            market_ma5_dist = (current_index / ma5) - 1.0 if ma5 > 0 else 0.0
            market_ma20_dist = market_ma5_dist
            index_slope = 0.0
        else:
            ma5 = ma20 = current_index
            market_ma5_dist = market_ma20_dist = 0.0
            index_slope = 0.0

        # ADR 계산
        if falling_stocks > 0:
            adr_ratio = (rising_stocks if rising_stocks > 0 else 0.1) / falling_stocks
        else:
            adr_ratio = float(min(rising_stocks, 20.0)) if rising_stocks > 0 else 1.0

        # [New] Market Score & Regime Classification

        market_score = 50.0  # Neutral Base

        # A. Score Calculation (Breadth + Trend)
        if market_ma20_dist > 0:
            market_score += 15
        if index_slope > 0.1:
            market_score += 10
        elif index_slope < -0.1:
            market_score -= 10

        # [Breadth Divergence Check]
        if adr_ratio > 1.2 and current_index_change < 0:
            market_score += 10  # Hidden Strength (Accumulation)
        elif adr_ratio < 0.8 and current_index_change > 0:
            market_score -= 15  # Fake Strength (Samsung Effect/Distribution)

        #  Strong Bounce 기준 완화 (Index > 0.8% or (Index > 0.5% and ADR > 1.0))
        is_strong_bounce = (current_index_change >= 0.8) or (
            current_index_change >= 0.5 and adr_ratio >= 1.0
        )

        #  Default Regime is NEUTRAL
        raw_regime = "NEUTRAL"

        # CRASH: 극심한 수급 붕괴(ADR 0.5 이하) OR (지수 급락 AND ADR 약세)
        # OR 리스크 관리 하드 한계점 (Systemic Risk: -3.0% 폭락 OR ADR 0.35 이하)
        is_panic = (current_index_change <= -3.0) or (adr_ratio <= 0.35)

        if (
            adr_ratio <= 0.5
            or (current_index_change <= -2.0 and adr_ratio < 0.7)
            or is_panic
        ):
            raw_regime = "CRASH"
        # BEAR: 마켓 슬로프 하락 + (ADR 0.85 미만 OR MA20 하회)
        elif index_slope < -0.2 or (market_ma20_dist < 0 and adr_ratio < 0.85):
            # [Exception] 당일 강한 반등 및 수급 폭발 시 BEAR 전면 차단 완화
            if is_strong_bounce:
                raw_regime = "NEUTRAL"
                if logger:
                    logger.info(
                        f"💡 [{market_type}] Strong Bounce Detected (Change: {current_index_change:.2f}%, ADR: {adr_ratio:.2f}) -> Relieving BEAR to NEUTRAL"
                    )
            else:
                raw_regime = "BEAR"
        # BULL: ADR 강세(1.1 초과) AND MA20 상회 (진정한 주도주 장세)
        elif adr_ratio >= 1.1 and market_ma20_dist > 0:
            raw_regime = "BULL"

        # C. Hysteresis (Smoothing) Implementation
        is_opening_noise = False
        if not is_simulation:
            if now.time() < datetime.time(9, 0, 5):
                is_opening_noise = True
            if rising_stocks + falling_stocks == 0 or current_index == 0:
                is_opening_noise = True

        # D. Pre-emptive Switch Logic (Systemic Risk Guard)

        # E. Final Regime Assignment with Hysteresis

        regime_state = market_regime_state[market_type]

        if is_opening_noise:
            final_regime = regime_state["current_regime"]
        elif is_simulation:
            final_regime = raw_regime
        else:
            # [Layer 2: Contradiction Resolution] CRASH/BEAR 탈출 가속화
            fast_recovery = (
                regime_state["current_regime"] in ["CRASH", "BEAR"]
                and current_index_change >= 0.5
                and adr_ratio >= 1.0
            )

            if fast_recovery:
                regime_state["current_regime"] = "NEUTRAL"
                regime_state["confirmation_count"] = 0
                if logger:
                    logger.info(
                        f"🚀 [{market_type}] Fast-Recovery {regime_state['current_regime']} to NEUTRAL (Change: {current_index_change:+.2f}%, ADR: {adr_ratio:.2f})"
                    )
                final_regime = "NEUTRAL"
            elif raw_regime != regime_state["current_regime"]:
                #  CRASH(폭락) 상태는 지연 없이 즉시 반영하여 리스크 관리 우선
                # 단, 지수가 강하게 반등(+0.5% 이상) 중이면 CRASH 진입을 원천 차단
                if raw_regime == "CRASH":
                    if current_index_change >= 0.5:
                        regime_state["current_regime"] = "NEUTRAL"
                        if logger:
                            logger.info(
                                f"🛡️ [{market_type}] CRASH detected but Index is Recovering (+{current_index_change:.2f}%). Forcing NEUTRAL."
                            )
                    else:
                        regime_state["current_regime"] = "CRASH"
                        regime_state["confirmation_count"] = 0
                        if logger:
                            logger.critical(
                                f"🚨 [EMERGENCY] [{market_type}] Market CRASH Detected (Index: {current_index_change:.2f}%, ADR: {adr_ratio:.2f}). Immediate Switch."
                            )
                elif raw_regime == regime_state["pending_regime"]:
                    regime_state["confirmation_count"] += 1
                else:
                    regime_state["pending_regime"] = raw_regime
                    regime_state["confirmation_count"] = 1

                # Threshold: 2 confirmations (Recoveries skip this for speed)
                is_recovering = (
                    raw_regime in ["BULL", "NEUTRAL"] and current_index_change >= 0.3
                )
                if regime_state["confirmation_count"] >= 2 or is_recovering:
                    regime_state["current_regime"] = raw_regime
                    regime_state["confirmation_count"] = 0
                    if logger:
                        logger.info(
                            f"⚡ [{market_type}] Market Regime Switched: {raw_regime} (Score: {market_score:.1f}, Fast-Track: {is_recovering})"
                        )
            else:
                regime_state["confirmation_count"] = 0  # Reset if steady
                if logger:
                    # logger.info(
                    #     f"⚡ [{market_type}] Market Regime is stable: {raw_regime} (Score: {market_score:.1f})"
                    # )
                    pass

            final_regime = regime_state["current_regime"]

        # [SSOT] DB에 현재 시장 국면 기록 (텔레그램 및 타 프로세스 공유용)
        if not is_simulation:
            save_market_status_to_db(
                market_type, final_regime, current_index_change, adr_ratio
            )

        # 3. 매수/매도 조건 설정 (Legacy Support)
        buy_condition = final_regime in ["BULL", "NEUTRAL"]
        sell_condition = final_regime in ["BEAR", "CRASH"]

        # 4. 시장 상황에 따른 권장 낙폭(dip_rate) 계산
        if current_index_change < -1.0:
            market_dip_rate = 3.0
        elif current_index_change < -0.5:
            market_dip_rate = 2.25
        elif current_index_change > 0.5:
            market_dip_rate = 1.2
        else:
            market_dip_rate = 1.5

        if current_index_change < 0:
            #  Handle signed change (0 baseline) and ratio (1.0 baseline)
            is_energy_failing = (money_diff < 0) or (0 < money_diff < 0.9)

            if is_energy_failing:
                if adr_ratio <= 0.6:  # Relaxed slightly for earlier detection
                    market_dip_rate *= 1.5
                    if logger:
                        logger.info(
                            "🚨 [Energry Exhausted] Volume Decline & Breadth Collapse -> Surging Dip-Threshold"
                        )
                else:
                    market_dip_rate *= 1.2
            elif money_diff > 0:
                if adr_ratio > 0.8:
                    market_dip_rate *= 0.85
                    if logger:
                        logger.info(
                            "💰 [저가 매수세] 거래대금 증가 및 하락 방어 감지 -> 낙폭 기준 하향"
                        )

        if gap_rate > 1.5 and current_index_change < gap_rate:
            market_dip_rate *= 1.2

        # 결과 딕셔너리 구성
        result = {
            "market_type": market_type,
            "buy_condition": buy_condition,
            "sell_condition": sell_condition,
            "current_index": current_index,
            "current_index_change": current_index_change,
            "high_rate": high_rate,
            "low_rate": low_rate,
            "gap_rate": gap_rate,
            "market_ma5_dist": market_ma5_dist,
            "index_return_5d": 0,  # Placeholder
            "market_dip_rate": market_dip_rate,
            "rising_stocks": rising_stocks,
            "falling_stocks": falling_stocks,
            "adr_ratio": adr_ratio,
            "recent_index_changes": close_series.tail(10).tolist(),
            "market_data": market_data,
            "last_updated": now,
            "market_score": market_score,
            "market_regime": final_regime,
            "momentum_state": "STRONG_BOUNCE" if is_strong_bounce else "NORMAL",
            "market_energy": get_market_energy_score_df_v6_2(market_data),
        }

        # 캐시 업데이트
        market_cond = MarketConditions(**result)
        market_cache[market_type] = market_cond
        return market_cond

    except Exception as e:
        if logger:
            logger.error(f"[analyze_market_conditions Error] {e}")
        # [Fail-Safe] Exception -> CRASH Mode
        return MarketConditions(
            market_type=market_type,
            buy_condition=False,
            sell_condition=True,
            current_index=0.0,
            current_index_change=0.0,
            market_dip_rate=3.0,
            last_updated=now,
            market_score=0,
            market_regime="CRASH",
        )


def get_market_energy_score_df_v6_2(df: pd.DataFrame) -> float:
    """
    Calculates market energy score based on volume and price direction.
    """
    if df is None or len(df) < 5:
        return 1.0

    try:
        # 컬럼명 유연하게 처리
        amount_col = (
            "거래대금"
            if "거래대금" in df.columns
            else ("거래량" if "거래량" in df.columns else None)
        )
        change_col = "지수등락율" if "지수등락율" in df.columns else "change_out"

        if amount_col is None:
            return 1.0

        current_vol = float(df[amount_col].iloc[-1])
        avg_vol = float(df[amount_col].tail(10).mean())
        idx_change = float(df[change_col].iloc[-1])

        # 거래대금 실리면서 상승 시 에너지 가점, 하락 시 감점
        vol_ratio = current_vol / avg_vol if avg_vol > 0 else 1.0

        energy = 1.0
        if vol_ratio > 1.3:
            if idx_change > 0:
                energy = 1.15
            else:
                energy = 0.85
        elif vol_ratio < 0.7:
            energy = 0.95

        return round(energy, 2)
    except Exception as e:
        if logger:
            logger.error(f"[get_market_energy_score_df_v6_2 Error] {e}")
        return 1.0


def get_market_momentum_status(data_pool, min_term):
    """
    현재 분석 대상 종목들의 통합 가속도 평균을 병렬로 산출함
    """
    from concurrent.futures import as_completed
    from util.CommUtils import GLOBAL_THREAD_POOL
    from strategy.indicators.technical_indicators import (
        calculate_intraday_acceleration_v5_6,
    )

    all_acc_scores = []

    def calculate_single_ticker(ticker):
        """단일 종목의 가속도를 계산하는 함수"""
        try:
            from util.CommUtils import load_data_incremental

            minute_df = load_data_incremental(ticker, "분별주가")
            if minute_df is None or minute_df.empty:
                return None
            intra_acc = calculate_intraday_acceleration_v5_6(minute_df, min_term)
            return intra_acc
        except Exception as e:
            if logger:
                logger.debug(f"Momentum scan failed for {ticker}: {e}")
            return None

    futures = {
        GLOBAL_THREAD_POOL.submit(calculate_single_ticker, t): t for t in data_pool
    }
    for future in as_completed(futures):
        result = future.result()
        if result is not None:
            all_acc_scores.append(result)

    market_avg_acc = np.mean(all_acc_scores) if all_acc_scores else 0.0
    return float(round(market_avg_acc, 2)), all_acc_scores
