from dataclasses import dataclass, field, asdict
from typing import Dict, Any, Optional, List
import pandas as pd
import numpy as np
import datetime

# --- 1. Evaluation Results & Scores (Merges RuleResult, ScoreResult) ---


@dataclass
class EvaluationScore:
    """규칙 평가 또는 엔진 스코어링의 표준 결과 형식"""

    total_score: float = 0.0
    is_passed: bool = False
    breakdown: Dict[str, float] = field(default_factory=dict)
    reason: str = ""
    score_modifier: float = 0.0
    market_regime: str = "NEUTRAL"
    regime: str = "NEUTRAL"
    current_index_change: float = 0.0
    index_change: float = 0.0
    extra_info: Dict[str, Any] = field(default_factory=dict)

    def __float__(self):
        return float(self.total_score)

    def __post_init__(self):
        # Sync regime
        if self.market_regime == "NEUTRAL" and self.regime != "NEUTRAL":
            self.market_regime = self.regime
        elif self.regime == "NEUTRAL" and self.market_regime != "NEUTRAL":
            self.regime = self.market_regime

        # Sync index_change
        if self.current_index_change == 0.0 and self.index_change != 0.0:
            self.current_index_change = self.index_change
        elif self.index_change == 0.0 and self.current_index_change != 0.0:
            self.index_change = self.current_index_change

    def to_dict(self) -> dict:
        return {
            "total_score": self.total_score,
            "is_passed": self.is_passed,
            "breakdown": self.breakdown,
            "reason": self.reason,
            "extra_info": self.extra_info,
        }


# Aliases for backward compatibility
RuleResult = EvaluationScore
ScoreResult = EvaluationScore


# --- 2. Market State (Merges MarketStatus, MarketContextData, MarketConditions) ---


@dataclass
class MarketState:
    """시장 전체의 상태 및 기술적 지표를 통합 관리"""

    market_type: str = "KOSDAQ"
    regime: str = "NEUTRAL"
    index_change: float = 0.0  # 당일 등락률 (%) - Unified from current_index_change
    is_bull: bool = True
    is_panic: bool = False
    drop_rate: float = 0.0  # 고점 대비 하락률
    buy_condition: bool = True
    sell_condition: bool = False

    # MarketConditions Detailed Fields
    market_regime: str = "NEUTRAL"  # Alias for regime
    current_index: float = 0.0
    current_index_change: float = 0.0  # Keep for compatibility
    high_rate: float = 0.0
    low_rate: float = 0.0
    gap_rate: float = 0.0
    market_ma5_dist: float = 0.0
    index_return_5d: float = 0.0
    market_dip_rate: float = 1.5
    rising_stocks: int = 0
    falling_stocks: int = 0
    adr_ratio: float = 1.0
    recent_index_changes: List[float] = field(default_factory=list)
    market_data: Optional[pd.DataFrame] = None
    last_updated: datetime.datetime = field(default_factory=datetime.datetime.now)
    market_score: float = 50.0
    momentum_state: str = "NEUTRAL"
    system_params: Optional[Dict[str, Any]] = None
    conditions: Dict[str, Any] = field(default_factory=dict)
    market_energy: float = 1.0


# Aliases
MarketStatus = MarketState
MarketContextData = MarketState
MarketConditions = MarketState


# --- 3. Position Information (Merges PositionState, PositionData, SurvivalPositionState, OrderExecutionInfo) ---


@dataclass
class PositionInfo:
    """개별 종목의 포지션 상태 및 수익성 지표 통합 관리"""

    ticker: str
    entry_price: float = 0.0  # 진입가 (Standard Name)
    purchase_price: float = 0.0  # Alias for entry_price (Backward Compatibility)
    current_price: float = 0.0
    qty: int = 0  # 수량 (Standard Name)
    quantity: int = 0  # Alias for qty (Backward Compatibility)
    initial_qty: int = 0
    initial_quantity: int = 0  # Alias

    # 수익 및 변동성 지표
    max_price_after_buy: float = 0.0
    max_profit_pct: float = 0.0
    profit_rate: float = 0.0
    atr_5m: float = 0.0
    vwap: float = 0.0

    # 시간 및 상태 지표
    bars_since_entry: int = 0
    days_since_entry: int = 0
    status: str = "none"
    entry_time: Optional[str] = None
    last_updated: datetime.datetime = field(default_factory=datetime.datetime.now)

    # 리스크 및 목표 관리
    stop_loss: float = 0.0
    take_profit1: float = 0.0
    take_profit2: float = 0.0
    market_regime: str = "NEUTRAL"
    current_time_str: str = ""
    tick_acc: float = 100.0
    grade: str = "B"
    reason: str = ""

    # OrderExecutionInfo Serial Fields
    cl_ord_id: str = ""
    side: str = ""
    timestamp: float = 0.0
    capital: float = 0.0
    order_type: str = "00"

    def __post_init__(self):
        # Synchronize compatibility aliases if not provided
        if self.purchase_price == 0.0 and self.entry_price != 0.0:
            self.purchase_price = self.entry_price
        elif self.entry_price == 0.0 and self.purchase_price != 0.0:
            self.entry_price = self.purchase_price

        if self.quantity == 0 and self.qty != 0:
            self.quantity = self.qty
        elif self.qty == 0 and self.quantity != 0:
            self.qty = self.quantity

        if self.initial_quantity == 0 and self.initial_qty != 0:
            self.initial_quantity = self.initial_qty
        elif self.initial_qty == 0 and self.initial_quantity != 0:
            self.initial_qty = self.initial_quantity

    def update_profit(self, current_profit_pct: float):
        """최고 수익률 실시간 갱신 로직"""
        self.max_profit_pct = max(self.max_profit_pct, current_profit_pct)

    def increment_bars(self):
        """캔들 완성 시 경과 시간 증가"""
        self.bars_since_entry += 1


# Aliases
PositionState = PositionInfo
PositionData = PositionInfo
SurvivalPositionState = PositionInfo
OrderExecutionInfo = PositionInfo


# --- 4. Stock Evaluation Context (Merges StockEvaluationData, StockEvaluationResult, TradingDecisionParam, FuseScoreResult, TradingCandidate) ---


@dataclass
class StockEvaluation:
    """종목의 평가 지표, 엔진 스코어, 의사결정 컨텍스트 통합"""

    ticker: str
    idx: int = 0
    current_price: float = 0.0
    market_regime: str = "NEUTRAL"
    day_open: float = 0.0
    day_high: float = 0.0
    day_low: float = 0.0
    vol_surge_ratio: float = 1.0
    atr_pct: float = 2.0

    # 스코어링 및 등급
    score: float = 0.0  # 통합 점수 (combined_score, total_score 통합)
    combined_score: float = 0.0  # Alias
    daily_score: float = 0.0
    grade: str = "F"  # 통합 등급 (fuse_grade, intrinsic_grade 통합)
    intrinsic_grade: str = "C"
    fuse_grade: str = "C"  # Alias

    # 가부 결정 플래그
    is_buy_eligible: bool = False  # 최종 매도 가능 여부 (Standard)
    is_buy: bool = False  # Alias
    final_can_buy: bool = False  # Alias
    is_valid: bool = False  # Alias
    is_blocked: bool = False
    decision_state: str = "UNKNOWN"
    reason: str = ""

    # 기술적 지표 및 수급
    is_breakout: bool = False
    is_limit_up_trade: bool = False
    is_recovering_leader: bool = False
    is_true_bounce: bool = False
    is_vwap_pullback: bool = False
    track_a_swing: bool = False
    track_b_momentum: bool = False
    is_bottom_breakout: bool = False
    breakout_strength: float = 0.0
    pullback_quality: float = 0.0
    pb_quality: float = 0.0  # Alias for pullback_quality
    energy_status: str = ""
    trend_score: float = 0.0
    meets_basic_criteria: bool = False
    supply_s: float = 0.0
    limit_up_data: Dict[str, Any] = field(default_factory=dict)

    ai_surge_probability: float = 0.5
    ai_prob: float = 0.5  # Alias
    expected_win_rate: float = 0.8
    noise_ratio: float = 0.5

    avg_volume_5: float = 0.0
    current_volume: float = 0.0
    supply_intra: float = 0.0
    intra_acc: float = 0.0
    tick_acc: float = 0.0
    day_acc_score: float = 0.0
    intraday_score: float = 0.0

    recent_low: float = 0.0
    bid_sum: float = 0.0
    ask_sum: float = 0.0
    atr_val: float = 0.0
    atr_5m: float = 0.0  # Alias/Context
    rs_gap: float = 0.0
    bb_dist: float = 0.0
    surge_rate: float = 0.0
    vcp_ratio: float = 0.0
    volume_dry: bool = False
    volume_multiplier: float = 1.0
    volume_surge_ratio: float = 1.0
    position_size_ratio: float = 1.0

    # 복합 데이터 컨테이너
    minute_df: Optional[pd.DataFrame] = None
    daily_df: Optional[pd.DataFrame] = None
    indicator_data: Dict[str, Any] = field(default_factory=dict)
    support_levels: Dict[str, float] = field(default_factory=dict)
    breakout_info: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        # Sync aliases
        if self.combined_score == 0.0 and self.score != 0.0:
            self.combined_score = self.score
        elif self.score == 0.0 and self.combined_score != 0.0:
            self.score = self.combined_score

        if self.is_buy is False and self.is_buy_eligible is True:
            self.is_buy = True

        if self.vol_surge_ratio == 1.0 and self.volume_surge_ratio != 1.0:
            self.vol_surge_ratio = self.volume_surge_ratio
        elif self.volume_surge_ratio == 1.0 and self.vol_surge_ratio != 1.0:
            self.volume_surge_ratio = self.vol_surge_ratio
        elif self.is_buy_eligible is False and self.is_buy is True:
            self.is_buy_eligible = True

        # Sync pullback_quality aliases
        if self.pb_quality == 0.0 and self.pullback_quality != 0.0:
            self.pb_quality = self.pullback_quality
        elif self.pullback_quality == 0.0 and self.pb_quality != 0.0:
            self.pullback_quality = self.pb_quality

    def get(self, key: str, default: Any = None) -> Any:
        """딕셔너리 스타일 조회를 위한 헬퍼 (Legacy 호환성)"""
        return getattr(self, key, default)

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)

    def __setitem__(self, key: str, value: Any):
        setattr(self, key, value)

        if self.final_can_buy is False and self.is_buy_eligible is True:
            self.final_can_buy = True
        elif self.is_buy_eligible is False and self.final_can_buy is True:
            self.is_buy_eligible = True


# Aliases
TradingCandidate = StockEvaluation
StockEvaluationData = StockEvaluation
StockEvaluationResult = StockEvaluation
TradingDecisionParam = StockEvaluation
FuseScoreResult = StockEvaluation


# --- 5. Trading Decision DTOs (保持) ---


@dataclass
class EntryDecisionDTO:
    stock_code: str
    final_can_buy: bool
    decision_state: str
    combined_score: float
    grade: str
    reason: str
    approved_quantity: int = 0
    extra_info: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ExitDecisionDTO:
    stock_code: str
    final_can_sell: bool
    action_type: str  # STOP_LOSS, TRAILING_STOP, PROFIT_TAKE 등
    order_type: str  # "00"(지정가), "03"(시장가)
    target_price: float
    reason: str
    sell_ratio: float = 1.0


# --- 6. Configuration & Other Params ---


@dataclass
class ExitLevelsData:
    stop_loss: float
    take_profit1: float
    take_profit2: float
    trailing_stop_pct: float
    chandelier_stop: float = 0.0
    hard_take_profit_ratio: float = 0.0
    max_holding_days: int = 0


@dataclass
class BridgeAnalysisResult:
    momentum: float = 0.0
    confidence: float = 0.5
    recommendation: str = "pass"
    gap_risk: str = "low"
    regime_change: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class BridgeAnalysisReport:
    bonus: float = 0.0
    vwap: float = 0.0
    gap_risk: str = "low"
    gap_result: Optional[BridgeAnalysisResult] = None


@dataclass
class ExecutionTriggerResult:
    intra_acc: float = 0.0
    tick_acc: float = 0.0
    supply_intra: float = 0.0
    fake_score: float = 0.0
    approved_quantity: int = 0
    intraday_score: float = 0.0
    pullback_score: float = 0.0
    pb_quality: float = 0.0
    pullback_quality: float = 0.0  # Alias for pb_quality
    vol_surge_ratio: float = 1.0
    trigger_hit: bool = False
    msg: str = "초기화"

    def __post_init__(self):
        # Sync pullback_quality aliases
        if self.pb_quality == 0.0 and self.pullback_quality != 0.0:
            self.pb_quality = self.pullback_quality
        elif self.pullback_quality == 0.0 and self.pb_quality != 0.0:
            self.pullback_quality = self.pb_quality


@dataclass
class Config:
    account_dict: dict
    ai_conf_dict: dict


@dataclass
class AIParameters:
    # Core Params
    min_hold_minutes: int = 6
    rsi_buy_threshold: int = 35
    macd_signal_gap: float = 0.7
    volume_rate_threshold: float = 1.2
    min_score: float = 2.0
    trailing_stop_pct: float = 9.0
    rsi_daily_thresh: int = 35
    rsi_min_thresh: int = 30
    order_flow_threshold: float = 1.5
    min_rs_gap: float = 1.5
    vcp_contraction_threshold: float = 0.0934
    min_supply_b_grade: float = 75.0
    max_rsi: float = 82.0
    min_volume_ratio: float = 1.5
    min_power: float = 105.0
    max_chase_pct: float = 1.5
    ai_bonus_threshold: float = 0.70
    ai_penalty_threshold: float = 0.35
    max_buy_per_sector: int = 2

    # Optimizer Sync Params
    volume_surge_threshold: float = 1.3304
    hard_stop_loss_pct: float = -2.5
    tp_factor_s: float = 2.5
    sl_factor_s: float = 2.5
    tp_factor_a: float = 4.0
    sl_factor_a: float = 1.7
    ofi_damping_slope: float = 0.05

    # System Constraints (WFO)
    max_surge_pct: float = 15.0
    lunch_time_start: int = 1030
    lunch_time_end: int = 1400
    market_close_buffer: int = 1520
    crash_index_threshold: float = -2.0
    vdu_strong_threshold: float = 0.5
    vdu_weak_threshold: float = 0.7

    # Plateau Breaker
    allow_bear_market_entry: bool = False
    bear_market_weight_penalty: float = 1.0
    vwap_margin_pct: float = 0.0
    ai_prob_threshold: float = 0.2638

    # WFO Fields
    tick_acc_min: float = 60.0
    tick_acc_bonus: float = 120.0
    max_vcp_ratio: float = 0.08
    volume_multiplier_threshold: float = 2.0
    hard_take_profit_pct: float = 2.5
    max_holding_days: int = 3

    # [Fix] 추가된 전역/슬롯 관리 속성 명시 (AIParameters 에러 해결)
    max_positions: int = 15
    max_active_positions: int = 15

    # Essential Thresholds
    s_threshold_normal: float = 85.0
    s_threshold_bad: float = 88.0
    a_threshold_normal: float = 55.0
    a_threshold_bad: float = 58.0
    b_threshold_floor: float = 45.0
    leader_bonus_req_normal: float = 15.0
    leader_bonus_req_bad: float = 20.0
    emergency_a_threshold: float = 130.0
    scaling_log_base: float = 120.0

    # Container for dynamic params
    _extra_params: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        pass

    def update(self, key: str, value: Any):
        if hasattr(self, key):
            setattr(self, key, value)
        else:
            self._extra_params[key] = value

    def __getitem__(self, key: str) -> Any:
        if hasattr(self, key):
            return getattr(self, key)
        if key in self._extra_params:
            return self._extra_params[key]
        raise KeyError(f"'{key}' not found in AIParameters")

    def __setitem__(self, key: str, value: Any):
        self.update(key, value)

    def get(self, key: str, default: Any = None) -> Any:
        try:
            if hasattr(self, key):
                return getattr(self, key)
            return self._extra_params.get(key, default)
        except Exception:
            return default

    def items(self):
        return self.to_dict().items()

    def keys(self):
        return self.to_dict().keys()

    def to_dict(self) -> Dict[str, Any]:
        """직렬화 dict 변환"""
        d = asdict(self)
        d.update(self._extra_params)
        if "_extra_params" in d:
            del d["_extra_params"]
        return d


# --- 7. Signal Emission ---


@dataclass
class SignalPayload:
    """주문 시그널 전송을 위한 데이터 구조체 (Non-blocking ZMQ PUSH용)"""

    ticker: str
    order_type: str  # BUY, SELL
    price: float
    quantity: int
    cl_ord_id: str
    reason: str = ""
    iceberg_params: Dict[str, Any] = field(default_factory=dict)
    timestamp: str = field(
        default_factory=lambda: datetime.datetime.now().strftime("%H:%M:%S.%f")
    )

    def to_dict(self) -> dict:
        return asdict(self)
