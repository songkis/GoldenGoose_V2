import logging
logger = logging.getLogger(__name__)

# 공통 시스템 파라미터 (휴장일 및 등급/로직 기준값)
KRX_HOLIDAYS = [
    "2026-01-01", "2026-01-28", "2026-01-29", "2026-01-30",
    "2026-03-01", "2026-05-05", "2026-10-03", "2026-10-09",
    "2026-12-25", "2026-12-31"
]

SYSTEM_PARAMS = {
    # Grade & Market Thresholds
    "s_threshold_normal": 85,
    "a_threshold_normal": 55,
    "b_threshold_floor": 45,
    "pivot_reject_limit": 15,
    # Strategy Rules & Filters
    "vcp_contraction_threshold": 0.0934,
    "max_buy_per_sector": 3,
    "tick_acc_min": 60.0,
    "tick_acc_bonus": 120.0,
    # Standard Params
    "min_score": 2.0,
    "volume_surge_threshold": 1.3304,
    "rs_overheat_limit": 18,
    "trailing_stop_pct": 9.0,
    "hard_stop_loss_pct": -2.5,
    "hard_take_profit_pct": 2.5,
    # System Constraints
    "max_surge_pct": 14.5,
    "lunch_time_start": 1030,
    "lunch_time_end": 1400,
    "market_close_buffer": 1520,
    "crash_index_threshold": -2.0,
    "vdu_strong_threshold": 0.5,
    "vdu_weak_threshold": 0.7,
    # Standard Exit Factors
    "tp_factor_s": 2.5,
    "sl_factor_s": 2.5,
    "tp_factor_a": 4.0,
    "sl_factor_a": 1.7,
    "allow_bear_market_entry": False,
    "bear_market_weight_penalty": 1.0,
    "vwap_margin_pct": 0.0,
    "ai_prob_threshold": 0.2638,
    "max_holding_days": 3,
    "max_vcp_ratio": 0.08,
    "volume_multiplier_threshold": 2.0,
    # Zombie-Position Defense & Breakeven
    "time_stop_bars": 40,
    "time_stop_min_profit": 1.0,
    "breakeven_trigger": 1.5,
    "breakeven_cut": 0.3,
    "ofi_damping_slope": 0.05,
    "TIME_STOP_BARS": 40,
    "TIME_STOP_MIN_PROFIT": 1.0,
    "BREAKEVEN_TRIGGER": 1.5,
    "BREAKEVEN_CUT": 0.3,
}

def get_system_param(key: str, default=None):
    return SYSTEM_PARAMS.get(key, default)
