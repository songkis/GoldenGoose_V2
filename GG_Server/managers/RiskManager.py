import threading
import logging
import numpy as np
import pandas as pd
from util.CommUtils import get_db_connection

logger = logging.getLogger(__name__)


def set_logger(external_logger):
    global logger
    logger = external_logger


class PortfolioRiskManager:
    """
    [Phase 10] 포트폴리오 리스크 파리티(Risk Parity) 매니저
    - 특정 업종/테마에 자본이 몰빵(Over-concentration)되는 것을 방지
    - : DB 컨럼이 다르거나 없어도 시스템이 멈추지 않도록 동적 탐색 적용
    """

    def __init__(self, max_per_sector=2):
        self.lock = threading.Lock()  #  Concurrency Guard
        self.ticker_sector_map = {}
        self.market_type_map = {}  #  For Benchmark Matching
        self.beta_cache = {}  # [Phase 1] EOD Beta Cache
        self.max_per_sector = max_per_sector
        self.max_portfolio_beta = 1.25  # [Phase 1] Aggregate Beta Cap
        self._load_sector_map()

    def _load_sector_map(self):
        try:
            with get_db_connection() as conn:
                # 1. DB의 종목코드 테이블에서 동적으로 업종/테마 쿼리
                cursor = conn.cursor()
                cursor.execute("PRAGMA table_info(종목코드)")
                columns = [row[1] for row in cursor.fetchall()]
                target_col = None
                for col in ["업종명", "테마", "섹터", "업종", "WICS"]:
                    if col in columns:
                        target_col = col
                        break

                if target_col:
                    df = pd.read_sql_query(
                        f"SELECT 종목코드, {target_col}, 구분 FROM 종목코드", conn
                    )
                    for _, row in df.iterrows():
                        sector_val = str(row[target_col]).strip()
                        if sector_val and sector_val != "None":
                            self.ticker_sector_map[row["종목코드"]] = sector_val
                        if "구분" in df.columns:
                            self.market_type_map[row["종목코드"]] = int(row["구분"])
                    if logger:
                        logger.info(
                            f"🛡️ [Risk Parity] {len(self.ticker_sector_map)}개 종목 섹터({target_col}) 및 {len(self.market_type_map)}개 시장구분 매핑 완료"
                        )
                else:
                    df = pd.read_sql_query("SELECT 종목코드, 구분 FROM 종목코드", conn)
                    for _, row in df.iterrows():
                        self.market_type_map[row["종목코드"]] = int(row["구분"])
                    if logger:
                        logger.warning(
                            "⚠️ DB에 업종/테마 컨럼이 없어 Risk Parity가 최소 모드로 작동하지만, 시장구분은 매핑되었습니다."
                        )
        except Exception as e:
            if logger:
                logger.error(f"Sector Map Load Error: {e}")

    def preload_beta_cache(self):
        """
        [Phase 1] Preload Beta Cache to avoid Cold Start
        Calculate trailing 60-day Covariance with KOSPI/KOSDAQ Index Proxies (KODEX 200, KODEX 코스닥150).
        """
        try:
            if logger:
                logger.info(
                    "🛡️ [Risk Manager] Preloading Beta Cache with Benchmark Matching..."
                )
            with get_db_connection() as conn:
                # 1. Fetch ETF Proxies: 069500 (KODEX 200), 229200 (KODEX 코스닥150)
                df_indices = pd.read_sql_query(
                    "SELECT 종목코드, 날짜, 등락율 FROM 일별주가 WHERE 종목코드 IN ('069500', '229200') ORDER BY 날짜 DESC",
                    conn,
                )
                if df_indices.empty:
                    if logger:
                        logger.warning(
                            "⚠️ [Risk Manager] No index data ('069500', '229200') found for Beta. Using Fallback Beta = 1.0."
                        )
                    return
                df_indices["등락율"] = pd.to_numeric(
                    df_indices["등락율"], errors="coerce"
                ).fillna(0)
                # Separate index returns
                kospi_returns = df_indices[
                    df_indices["종목코드"] == "069500"
                ].set_index("날짜")["등락율"]
                kosdaq_returns = df_indices[
                    df_indices["종목코드"] == "229200"
                ].set_index("날짜")["등락율"]
                # 2. Fetch Stock Returns joined with '시장구분'
                query_stocks = """
                    SELECT A.종목코드, A.날짜, A.등락율, B.구분 AS 시장구분
                    FROM 일별주가 A
                    JOIN 종목코드 B ON A.종목코드 = B.종목코드
                    WHERE A.날짜 >= ?
                    ORDER BY A.날짜 DESC
                """
                min_date = df_indices["날짜"].min()
                df_stocks = pd.read_sql_query(
                    query_stocks, params=(min_date,), con=conn
                )
                if df_stocks.empty:
                    if logger:
                        logger.warning(
                            "⚠️ [Risk Manager] No stocks data found for Beta."
                        )
                    return
                df_stocks["등락율"] = pd.to_numeric(
                    df_stocks["등락율"], errors="coerce"
                ).fillna(0)
                # 3. Calculate Beta per Ticker
                count = 0
                for ticker, group in df_stocks.groupby("종목코드"):
                    try:
                        market_type = (
                            int(group["시장구분"].iloc[0])
                            if not pd.isna(group["시장구분"].iloc[0])
                            else 1
                        )
                        market_returns = (
                            kospi_returns if market_type == 1 else kosdaq_returns
                        )
                        stock_returns = group.set_index("날짜")["등락율"]
                        combined = pd.concat(
                            [stock_returns, market_returns],
                            axis=1,
                            keys=["stock", "market"],
                        ).dropna()
                        if len(combined) > 20:
                            cov = np.cov(combined["stock"], combined["market"])[0][1]
                            var = np.var(combined["market"])
                            beta = cov / var if var > 0 else 1.0
                            self.beta_cache[ticker] = float(np.clip(beta, 0.5, 2.5))
                            count += 1
                        else:
                            self.beta_cache[ticker] = 1.0
                    except Exception:
                        self.beta_cache[ticker] = 1.0
            if logger:
                logger.info(
                    f"🛡️ [Risk Manager] Beta Cache preloaded for {count} stocks."
                )
        except Exception as e:
            if logger:
                logger.error(f"Beta Preload Error: {e}")

    def check_beta_limit(
        self, target_ticker, current_portfolio, target_beta=1.0, index_drop_pct=0.0
    ):
        """
        [Phase 1] Aggregate Portfolio Beta Exposure Capping
        - Enforces Max Aggregate Beta of 1.25.
        -  Shock Multiplier (1.5x) applied if Index Drop <= -1.5%.
        """
        multiplier = 1.0
        if index_drop_pct <= -1.5:
            multiplier = 1.5
            if logger:
                logger.warning(
                    f"🚨 [Circuit Breaker] Shock Multiplier 1.5x triggered (Index Drop: {index_drop_pct:.2f}%)"
                )
        with self.lock:
            # Use target_beta if provided and valid, otherwise fallback to cache
            try:
                f_beta = float(target_beta)
                if pd.isna(f_beta) or f_beta == 1.0:
                    f_beta = self.beta_cache.get(target_ticker, 1.0)
            except (ValueError, TypeError):
                f_beta = self.beta_cache.get(target_ticker, 1.0)
            target_beta_val = f_beta * multiplier
            total_beta = target_beta_val
            count = 1
            for held_ticker in current_portfolio:
                held_beta = self.beta_cache.get(held_ticker, 1.0) * multiplier
                total_beta += held_beta
                count += 1
            avg_beta = total_beta / count
            self.aggregate_beta = avg_beta
            if avg_beta > self.max_portfolio_beta:
                return (
                    False,
                    f"Aggregate Beta {avg_beta:.2f} > {self.max_portfolio_beta:.2f}",
                )
            return True, f"Aggregate Beta {avg_beta:.2f} safe"

    def check_sector_limit(self, target_ticker, current_portfolio):
        """매수 대상 종목이 포트폴리오 내 동일 섹터 한도를 초과하는지 검사"""
        # 1. 매수 대상 종목의 섹터 파악
        target_sector = self.ticker_sector_map.get(target_ticker, "UNKNOWN")
        # 정보가 없는 종목은 리스크 파리티 예외 처리 (블로킹하지 않음)
        if target_sector == "UNKNOWN":
            return True, "UNKNOWN_SECTOR"
        # 2. 현재 계좌(portfolio) 내 동일 섹터 개수 카운트
        sector_count = 0
        for held_ticker in current_portfolio:
            held_sector = self.ticker_sector_map.get(held_ticker, "UNKNOWN")
            if held_sector == target_sector:
                sector_count += 1
        # 3. 리스크 한도 초과 시 차단
        if sector_count >= self.max_per_sector:
            return False, f"[{target_sector}] 섹터 포화 (현재 {sector_count}개 보유중)"
        return (
            True,
            f"[{target_sector}] 섹터 안전 ({sector_count}/{self.max_per_sector})",
        )

    def check_stop_loss_conditions(
        self, ticker, current_price, entry_price, entry_time, atr_14, k_vol=None
    ):
        """
        [Phase 11] Zero-Defect Risk Architecture
        1. Minimum Hold Time Defense: 3초 미만 보유 시 노이즈에 의한 손절 방지 (단, -3% 급락 시 화성 탈출)
        2. Volatility-Adaptive Stop: ATR 기반 동적 손절선 (K-Value 스케일링)
        """
        import datetime

        try:
            # [TLVI] Initializing variables
            if entry_time is None:
                return False, "NO_ENTRY_TIME"

            now = datetime.datetime.now()
            # entry_time이 str인 경우 처리
            if isinstance(entry_time, str):
                try:
                    entry_time = datetime.datetime.strptime(
                        entry_time, "%Y-%m-%d %H:%M:%S"
                    )
                except ValueError:
                    # '20260420 09:00:00' 형태 또는 '2026-04-20 09:00:00' 등 보정
                    try:
                        entry_time = datetime.datetime.fromisoformat(entry_time)
                    except:
                        return False, "INVALID_TIME_FORMAT"

            hold_seconds = (now - entry_time).total_seconds()
            price_drop_pct = (
                (entry_price - current_price) / entry_price * 100.0
                if entry_price > 0
                else 0
            )

            # 1. [Min Hold Time Defense] (3s)
            if hold_seconds < 3:
                # Flash Crash Guard: -3% 이상 급락 시 즉각 대응
                if price_drop_pct >= 3.0:
                    if logger:
                        logger.warning(
                            f"🚨 [Flash Crash] {ticker} -3% drop in {hold_seconds:.1f}s. Bypassing Min Hold Time."
                        )
                    return True, "FLASH_CRASH_EXIT"
                return False, f"BYPASS_NOISE ({hold_seconds:.1f}s < 3s)"

            # 2. [Volatility-Adaptive Stop]
            # K_vol Dynamic Scaling (0.5 ~ 1.5)
            # ATR %가 크면 변동성 장세 -> K 축소 (보수적), ATR %가 작으면 저변동성 -> K 확대
            if k_vol is None:
                atr_pct = (atr_14 / entry_price * 100.0) if entry_price > 0 else 2.0
                k_vol = np.clip(1.5 - (atr_pct * 0.1), 0.5, 1.5)

            stop_loss_price = entry_price - (atr_14 * k_vol)

            if current_price <= stop_loss_price:
                return (
                    True,
                    f"VOL_ADAPTIVE_SL (Price:{current_price} <= SL:{stop_loss_price:.2f}, K:{k_vol:.2f})",
                )

            return False, "STABLE"
        except Exception as e:
            if logger:
                logger.error(f"❌ [RiskManager] check_stop_loss error: {e}")
            return False, f"ERROR: {e}"

