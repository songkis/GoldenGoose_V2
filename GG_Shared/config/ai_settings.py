from dataclasses import dataclass, field, asdict
from typing import Any, Dict
import numpy as np

from config.ai_config_manager import config_manager
from config.system_params import get_system_param


from core.schemas import Config, AIParameters


config = Config(account_dict=None, ai_conf_dict=None)


def init_ai_config():
    from util.CommUtils import setConfig

    setConfig(config)


def get_optimized_window(interval_min: int, mode: str = "trend") -> int:
    """분석 주기(분봉) 기반 윈도우 산출"""
    try:
        if mode == "trend":
            target_minutes = 180 if interval_min <= 3 else 200
            min_sample, max_sample = 40, 100
        else:
            target_minutes = 45 if interval_min <= 3 else 50
            min_sample, max_sample = 10, 30

        calculated_window = int(target_minutes / interval_min)
        return int(np.clip(calculated_window, min_sample, max_sample))
    except Exception:
        return 30 if mode == "trend" else 12


def get_trend_window(interval_minutes: int) -> int:
    return get_optimized_window(interval_minutes, mode="trend")


def recommended_momentum_len(interval_min: int) -> int:
    return get_optimized_window(interval_min, mode="momentum")


init_ai_config()

# Safe config access
_acc_dict = config.account_dict if config.account_dict is not None else {}
_ai_dict = config.ai_conf_dict if config.ai_conf_dict is not None else {}

SYS_ID = int(_acc_dict.get("SYS_ID", 0))
거래환경 = _acc_dict.get("거래환경", "Unknown")
TIME_STEP = int(_ai_dict.get("학습일단위", 20))
PREDICTION_HORIZON = int(_ai_dict.get("이후예측일자", 3))
RISE_RATE = float(_ai_dict.get("상승예측률", 1.05))
BASED_PURCHASE_PRICE = int(_ai_dict.get("매수기준주가", 3))
MIN_TERM = int(_ai_dict.get("수집분봉단위", 3))

term_min_mapping = {1: 1, 3: 2, 5: 3, 10: 4}
MIN_TERM_CD = term_min_mapping.get(MIN_TERM, 3)

TREND_MINUTE_WIN_CNT = get_trend_window(MIN_TERM)
ACC_TERM = 10
MOMENTUM_MINUTE_WIN_CNT = recommended_momentum_len(MIN_TERM)
MIN_TERM_GAIN_STK_CNT = int(MIN_TERM * 60 / 6)

BUY_BASE = float(config_manager.get("BUY_BASE", 0.9))
SELL_BASE = float(config_manager.get("SELL_BASE", -0.7))
BUY_CNFDNC = float(config_manager.get("BUY_CNFDNC", 0.9))
SELL_CNFDNC = float(config_manager.get("SELL_CNFDNC", -0.7))

MARKET_ADAPTIVE_SCORING = True
BULL_MARKET_SCORE_BONUS = float(config_manager.get("BULL_MARKET_SCORE_BONUS", 1.0))
BEAR_MARKET_SCORE_PENALTY = float(config_manager.get("BEAR_MARKET_SCORE_PENALTY", -1.0))

LOSSCUT_BASE = float(config_manager.get("LOSSCUT_BASE", 8.0))
TRAILSTOP_BASE = float(config_manager.get("TRAILSTOP_BASE", 0.025))

DAY_LOSS_CUT_BASE = float(_ai_dict.get("일봉손절기준", -3.0))
MIN_LOSS_CUT_BASE = float(_ai_dict.get("분봉손절기준", -2.0))

TELEGRAM_TOKEN = str(_ai_dict.get("TELEGRAM_TOKEN", ""))
TELEGRAM_CHAT_ID = str(_ai_dict.get("TELEGRAM_CHAT_ID", ""))
PC_POWER_STAT = int(_ai_dict.get("일별주가수집후PC전원", 0))

DOWN_MKT_BUY_YN = str(_ai_dict.get("하락장매수여부", "N"))
SRCH_RSLT_BUY_YN = str(_ai_dict.get("검색결과종목매수여부", "Y"))
NWS_RSLT_BUY_YN = str(_ai_dict.get("뉴스종목매수여부", "Y"))

GRADE_PRIORITY = {"S": 0, "A": 1, "B": 2, "C": 3, "None": 99}
R1_BUY_READY_CNT = 5
ORDER_INTERVAL = 3
SELL_THROTTLE_SECONDS = 5
BUY_TOP_CNT = 60  # Directive 3: Concentrated trading enforcement
MAX_ACTIVE_POSITIONS = 60  # Hard-cap for single-bullet execution

BATCH_PRGS = [
    "C:\\GoldenGoose\\dist\\StockPicking.exe",
    "C:\\GoldenGoose\\dist\\PredictStockDailyRate.exe",
]

GLOBAL_TREND_BASE = float(config_manager.get("GLOBAL_TREND_BASE", -0.25))
GLOBAL_PANIC_BASE = float(config_manager.get("GLOBAL_PANIC_BASE", -0.65))

LSTM_SETTINGS = {
    "EPOCHS": 20,
    "BATCH_SIZE": 256,
    "VALIDATION_SPLIT": 0.3,
    "MIN_DATA_POINTS": 60,
    "TIME_STEPS": 20,
    "PREDICTION_HORIZON": 5,
}

EXCLUDE_KEYWORDS = ["%스팩%", "%KODEX%", "%TIGER%", "%200%"]
CHUNK_SIZE = 100
NUM_WORKERS = 4

RSI_OVERSOLD = int(config_manager.get("RSI_OVERSOLD", 30))
RSI_OVERBOUGHT = int(config_manager.get("RSI_OVERBOUGHT", 70))
RSI_OVERSOLD_WEAK = int(config_manager.get("RSI_OVERSOLD_WEAK", 35))
RSI_OVERBOUGHT_WEAK = int(config_manager.get("RSI_OVERBOUGHT_WEAK", 65))

VOLUME_INCREASE_RATIO = float(config_manager.get("VOLUME_INCREASE_RATIO", 1.5))
VOLUME_RATE_MIN = float(config_manager.get("VOLUME_RATE_MIN", 0.15))

EMA_THRESHOLD = float(config_manager.get("EMA_THRESHOLD", 0.01))
EMA_THRESHOLD_WEAK = float(config_manager.get("EMA_THRESHOLD_WEAK", 0.005))

SURGE_RETURN = float(config_manager.get("SURGE_RETURN", 5.0))
SURGE_VOLUME_RATIO = float(config_manager.get("SURGE_VOLUME_RATIO", 2.0))
SURGE_CLOSING_HIGH_RATIO = float(config_manager.get("SURGE_CLOSING_HIGH_RATIO", 0.9))

S_THRESHOLD_NORMAL = float(
    config_manager.get(
        "s_threshold_normal", get_system_param("s_threshold_normal", 70.0)
    )
)
S_THRESHOLD_BAD = float(
    config_manager.get("s_threshold_bad", get_system_param("s_threshold_bad", 73.0))
)
A_THRESHOLD_NORMAL = float(
    config_manager.get(
        "a_threshold_normal", get_system_param("a_threshold_normal", 55.0)
    )
)
A_THRESHOLD_BAD = float(
    config_manager.get("a_threshold_bad", get_system_param("a_threshold_bad", 58.0))
)
B_THRESHOLD_FLOOR = float(
    config_manager.get("b_threshold_floor", get_system_param("b_threshold_floor", 40.0))
)
LEADER_BONUS_REQ_NORMAL = float(
    config_manager.get(
        "leader_bonus_req_normal", get_system_param("leader_bonus_req_normal", 15.0)
    )
)
LEADER_BONUS_REQ_BAD = float(
    config_manager.get(
        "leader_bonus_req_bad", get_system_param("leader_bonus_req_bad", 20.0)
    )
)
EMERGENCY_A_THRESHOLD = float(
    config_manager.get(
        "emergency_a_threshold", get_system_param("emergency_a_threshold", 130.0)
    )
)
SCALING_LOG_BASE = float(
    config_manager.get("scaling_log_base", get_system_param("scaling_log_base", 120.0))
)
CONSECUTIVE_LOSS_PLATINUM_BOOST = float(config_manager.get("platinum_boost", 10.0))
OVERHEAT_HARD_LIMIT_S = float(config_manager.get("overheat_limit_s", 4.5))
OVERHEAT_HARD_LIMIT_A = float(config_manager.get("overheat_limit_a", 3.5))
OVERHEAT_HARD_LIMIT_B = float(config_manager.get("overheat_limit_b", 2.8))

MAX_VCP_RATIO = float(
    config_manager.get(
        "vcp_contraction_threshold", get_system_param("vcp_contraction_threshold", 0.15)
    )
)
MAX_VCP_RATIO_HIGH_SCORE = float(config_manager.get("MAX_VCP_RATIO_HIGH_SCORE", 0.30))
MIN_SUPPLY_INTRA = float(config_manager.get("MIN_SUPPLY_INTRA", 20.0))
MIN_SUPPLY_INTRA_HIGH_SCORE = float(
    config_manager.get("MIN_SUPPLY_INTRA_HIGH_SCORE", 15.0)
)
SUPPLY_COLLAPSE_PEAK = float(config_manager.get("SUPPLY_COLLAPSE_PEAK", 85.0))

# Unified PARAMS Instance
try:
    PARAMS = AIParameters(
        # Legacy/New fields and WFO sync
        min_hold_minutes=config_manager.get(
            "min_hold_minutes", get_system_param("min_hold_minutes", 6)
        ),
        rsi_buy_threshold=config_manager.get(
            "rsi_buy_threshold", get_system_param("rsi_buy_threshold", 35)
        ),
        macd_signal_gap=config_manager.get(
            "macd_signal_gap", get_system_param("macd_signal_gap", 0.7)
        ),
        volume_rate_threshold=config_manager.get(
            "volume_rate_threshold", get_system_param("volume_rate_threshold", 1.2)
        ),
        min_score=config_manager.get("min_score", get_system_param("min_score", 2.0)),
        trailing_stop_pct=config_manager.get(
            "trailing_stop_pct", get_system_param("trailing_stop_pct", 2.2)
        ),
        rsi_daily_thresh=config_manager.get(
            "rsi_daily_thresh", get_system_param("rsi_daily_thresh", 35)
        ),
        rsi_min_thresh=config_manager.get(
            "rsi_min_thresh", get_system_param("rsi_min_thresh", 30)
        ),
        order_flow_threshold=config_manager.get(
            "order_flow_threshold", get_system_param("order_flow_threshold", 1.5)
        ),
        min_rs_gap=config_manager.get(
            "min_rs_gap", get_system_param("min_rs_gap", 1.5)
        ),
        vcp_contraction_threshold=config_manager.get(
            "vcp_contraction_threshold",
            get_system_param("vcp_contraction_threshold", 0.18),
        ),
        min_supply_b_grade=config_manager.get(
            "min_supply_b_grade", get_system_param("min_supply_b_grade", 75.0)
        ),
        max_rsi=config_manager.get("max_rsi", get_system_param("max_rsi", 82.0)),
        min_volume_ratio=config_manager.get(
            "min_volume_ratio", get_system_param("min_volume_ratio", 1.5)
        ),
        min_power=config_manager.get("min_power", get_system_param("min_power", 105.0)),
        max_chase_pct=config_manager.get(
            "max_chase_pct", get_system_param("max_chase_pct", 1.5)
        ),
        ai_bonus_threshold=config_manager.get(
            "ai_bonus_threshold", get_system_param("ai_bonus_threshold", 0.70)
        ),
        ai_penalty_threshold=config_manager.get(
            "ai_penalty_threshold", get_system_param("ai_penalty_threshold", 0.35)
        ),
        max_buy_per_sector=config_manager.get(
            "max_buy_per_sector", get_system_param("max_buy_per_sector", 2)
        ),
        volume_surge_threshold=config_manager.get(
            "volume_surge_threshold", get_system_param("volume_surge_threshold", 1.5)
        ),
        hard_stop_loss_pct=config_manager.get(
            "hard_stop_loss_pct", get_system_param("hard_stop_loss_pct", 3.5)
        ),
        tp_factor_s=config_manager.get("tp_factor_s", 4.0),
        sl_factor_s=config_manager.get("sl_factor_s", 2.2),
        tp_factor_a=config_manager.get("tp_factor_a", 3.0),
        sl_factor_a=config_manager.get("sl_factor_a", 1.8),
        allow_bear_market_entry=config_manager.get("allow_bear_market_entry", False),
        bear_market_weight_penalty=config_manager.get(
            "bear_market_weight_penalty", 0.5
        ),
        vwap_margin_pct=config_manager.get("vwap_margin_pct", 0.0),
        ai_prob_threshold=config_manager.get("ai_prob_threshold", 0.35),
        max_surge_pct=config_manager.get(
            "max_surge_pct", get_system_param("max_surge_pct", 15.0)
        ),
        lunch_time_start=config_manager.get(
            "lunch_time_start", get_system_param("lunch_time_start", 1030)
        ),
        lunch_time_end=config_manager.get(
            "lunch_time_end", get_system_param("lunch_time_end", 1400)
        ),
        market_close_buffer=config_manager.get(
            "market_close_buffer", get_system_param("market_close_buffer", 1520)
        ),
        crash_index_threshold=config_manager.get(
            "crash_index_threshold", get_system_param("crash_index_threshold", -2.0)
        ),
        vdu_strong_threshold=config_manager.get(
            "vdu_strong_threshold", get_system_param("vdu_strong_threshold", 0.5)
        ),
        vdu_weak_threshold=config_manager.get(
            "vdu_weak_threshold", get_system_param("vdu_weak_threshold", 0.7)
        ),
        s_threshold_normal=S_THRESHOLD_NORMAL,
        s_threshold_bad=S_THRESHOLD_BAD,
        a_threshold_normal=A_THRESHOLD_NORMAL,
        a_threshold_bad=A_THRESHOLD_BAD,
        b_threshold_floor=B_THRESHOLD_FLOOR,
        leader_bonus_req_normal=LEADER_BONUS_REQ_NORMAL,
        leader_bonus_req_bad=LEADER_BONUS_REQ_BAD,
        emergency_a_threshold=EMERGENCY_A_THRESHOLD,
        scaling_log_base=SCALING_LOG_BASE,
        tick_acc_min=config_manager.get(
            "tick_acc_min", get_system_param("tick_acc_min", 60.0)
        ),
        tick_acc_bonus=config_manager.get(
            "tick_acc_bonus", get_system_param("tick_acc_bonus", 120.0)
        ),
        max_vcp_ratio=config_manager.get(
            "max_vcp_ratio", get_system_param("max_vcp_ratio", 0.08)
        ),
        volume_multiplier_threshold=config_manager.get(
            "volume_multiplier_threshold",
            get_system_param("volume_multiplier_threshold", 2.0),
        ),
        hard_take_profit_pct=config_manager.get(
            "hard_take_profit_pct", get_system_param("hard_take_profit_pct", 2.5)
        ),
        max_holding_days=config_manager.get(
            "max_holding_days", get_system_param("max_holding_days", 3)
        ),
        ofi_damping_slope=config_manager.get(
            "ofi_damping_slope", get_system_param("ofi_damping_slope", 0.05)
        ),
        max_positions=MAX_ACTIVE_POSITIONS,
        max_active_positions=MAX_ACTIVE_POSITIONS,
    )
except TypeError as te:
    print(f"CRITICAL: AIParameters instantiation failed: {te}")
    # Emergency fallback
    PARAMS = AIParameters()

# WFO Parameter Grid
WFO_PARAMETER_GRID = {
    "tick_acc_min": [60.0, 80.0, 100.0],
    "tick_acc_bonus": [120.0, 150.0],
    "max_vcp_ratio": [0.08, 0.12, 0.15],
    "hard_tp_pct": [2.5, 3.5, 4.5],
    "hard_sl_pct": [-1.5, -2.0, -2.5],
    # Indicators & Sync
    "volume_multiplier_threshold": [1.5, 2.0, 3.0],
    "pb_quality_threshold": [0.4, 0.6, 0.8],
    "vol_surge_threshold": [1.5, 2.5, 3.5],
    "min_amount_threshold": [5_000_000_000, 10_000_000_000],
    "ofi_damping_slope": [0.03, 0.05, 0.07, 0.09],
}
