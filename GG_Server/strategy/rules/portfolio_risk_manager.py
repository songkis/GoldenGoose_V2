import logging
import threading
import numpy as np
import pandas as pd
from util.CommUtils import get_db_connection

logger = logging.getLogger("GoldenGoose.PortfolioRiskManager")

class PortfolioRiskManager:
    """
    [Phase 10] Portfolio Risk Parity Manager
    - Sector concentration limit
    - Aggregate Beta capping
    """
    def __init__(self, max_per_sector=2):
        self.lock = threading.Lock()
        self.ticker_sector_map = {}
        self.market_type_map = {}
        self.beta_cache = {}
        self.max_per_sector = max_per_sector
        self.max_portfolio_beta = 1.25
        self._load_sector_map()
        self.preload_beta_cache()

    def _load_sector_map(self):
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("PRAGMA table_info(종목코드)")
                columns = [row[1] for row in cursor.fetchall()]
                target_col = next((col for col in ["업종명", "테마", "섹터", "업종", "WICS"] if col in columns), None)
                if target_col:
                    df = pd.read_sql_query(f"SELECT 종목코드, {target_col}, 구분 FROM 종목코드", conn)
                    for _, row in df.iterrows():
                        sector_val = str(row[target_col]).strip()
                        if sector_val and sector_val != "None":
                            self.ticker_sector_map[row["종목코드"]] = sector_val
                        self.market_type_map[row["종목코드"]] = int(row["구분"])
        except Exception as e:
            logger.error(f"Sector Map Load Error: {e}")

    def preload_beta_cache(self):
        try:
            with get_db_connection() as conn:
                df_indices = pd.read_sql_query("SELECT 종목코드, 날짜, 등락율 FROM 일별주가 WHERE 종목코드 IN ('069500', '229200') ORDER BY 날짜 DESC LIMIT 200", conn)
                if df_indices.empty: return
                kospi_returns = df_indices[df_indices["종목코드"] == "069500"].set_index("날짜")["등락율"]
                kosdaq_returns = df_indices[df_indices["종목코드"] == "229200"].set_index("날짜")["등락율"]
                
                # Fetch recent returns for all stocks
                df_stocks = pd.read_sql_query("SELECT 종목코드, 날짜, 등락율 FROM 일별주가 WHERE 날짜 >= ? ORDER BY 날짜 DESC", params=(df_indices["날짜"].min(),), con=conn)
                for ticker, group in df_stocks.groupby("종목코드"):
                    try:
                        m_type = self.market_type_map.get(ticker, 1)
                        m_ret = kospi_returns if m_type == 1 else kosdaq_returns
                        s_ret = group.set_index("날짜")["등락율"]
                        comb = pd.concat([s_ret, m_ret], axis=1).dropna()
                        if len(comb) > 20:
                            cov = np.cov(comb.iloc[:,0], comb.iloc[:,1])[0][1]
                            var = np.var(comb.iloc[:,1])
                            self.beta_cache[ticker] = float(np.clip(cov/var if var > 0 else 1.0, 0.5, 2.5))
                        else: self.beta_cache[ticker] = 1.0
                    except: self.beta_cache[ticker] = 1.0
        except Exception as e:
            logger.error(f"Beta Preload Error: {e}")

    def check_beta_limit(self, target_ticker, current_portfolio, target_beta=1.0, index_drop_pct=0.0):
        multiplier = 1.5 if index_drop_pct <= -1.5 else 1.0
        with self.lock:
            b = float(target_beta) if target_beta and target_beta != 1.0 else self.beta_cache.get(target_ticker, 1.0)
            total_beta = b * multiplier
            for tk in current_portfolio:
                total_beta += self.beta_cache.get(tk, 1.0) * multiplier
            avg_beta = total_beta / (len(current_portfolio) + 1)
            if avg_beta > self.max_portfolio_beta:
                return False, f"Beta {avg_beta:.2f} > {self.max_portfolio_beta:.2f}"
            return True, f"Beta {avg_beta:.2f} safe"

    def check_sector_limit(self, target_ticker, current_portfolio):
        s = self.ticker_sector_map.get(target_ticker, "UNKNOWN")
        if s == "UNKNOWN": return True, "UNKNOWN_SECTOR"
        cnt = sum(1 for tk in current_portfolio if self.ticker_sector_map.get(tk) == s)
        if cnt >= self.max_per_sector:
            return False, f"Sector {s} limit reached ({cnt}/{self.max_per_sector})"
        return True, "Sector limit safe"

    def calculate_position_size(
        self,
        p,
        b,
        spread,
        atr_percentile,
        has_pardon,
        total_capital,
        max_slot_ratio=0.2,
    ):
        """
        [Phase 12] Asymmetric Kelly & Pardon Guard Penalty
        - f* = (p*b - q) / b
        - Half-Kelly applied for pardon-entry with high liquidity risk (Spread/Vol)
        """
        try:
            # [TLVI] Initializing variables
            q = 1.0 - p
            if b <= 0:
                return 0.0, 0.0, False

            # 1. Optimal Kelly Fraction (f*)
            f_star = (p * b - q) / b
            f_star = float(np.clip(f_star, 0.0, 1.0))

            # 2. Pardon Guard Penalty (Half-Kelly)
            # Condition: Pardon AND (Spread > 0.5% OR ATR Top 10%)
            is_penalty_applied = False
            if has_pardon:
                if spread > 0.005 or atr_percentile >= 90:
                    f_star = f_star / 2.0
                    is_penalty_applied = True

            # 3. Apply Max Slot Ratio (e.g., 20% of Portfolio)
            f_star = min(f_star, max_slot_ratio)

            # [Absolute Kelly Reject] 산출된 비중이 5% 미만인 경우 자본 기아 방지를 위해 매수 기각
            if f_star < 0.05:
                return 0.0, 0.0, False


            final_amount = total_capital * f_star

            if logger and is_penalty_applied:
                logger.info(
                    f"⚠️ [Kelly Penalty] Half-Kelly enforced (Spread:{spread*100:.2f}%, ATR_pct:{atr_percentile}) -> f*:{f_star:.4f}"
                )

            return float(final_amount), f_star, is_penalty_applied
        except Exception as e:
            if logger:
                logger.error(f"❌ [PortfolioRiskManager] calculate_position_size error: {e}")
            return 0.0, 0.0, False

