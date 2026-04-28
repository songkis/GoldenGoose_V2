import pandas as pd

def get_market_energy_score_df_v6_2(df: pd.DataFrame, logger=None) -> float:
    """Calculates market energy score based on volume and price direction."""
    if df is None or len(df) < 5: return 1.0
    try:
        amount_col = "거래대금" if "거래대금" in df.columns else ("거래량" if "거래량" in df.columns else None)
        change_col = "지수등락율" if "지수등락율" in df.columns else "change_out"
        if amount_col is None: return 1.0
        current_vol = float(df[amount_col].iloc[-1])
        avg_vol = float(df[amount_col].tail(10).mean())
        idx_change = float(df[change_col].iloc[-1])
        vol_ratio = current_vol / avg_vol if avg_vol > 0 else 1.0
        energy = 1.0
        if vol_ratio > 1.3: energy = 1.15 if idx_change > 0 else 0.85
        elif vol_ratio < 0.7: energy = 0.95
        return round(energy, 2)
    except Exception as e:
        if logger: logger.error(f"[get_market_energy_score_df_v6_2 Error] {e}")
        return 1.0
