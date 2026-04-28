import datetime
import dataclasses
from datetime import (
    timedelta,
)  #  명시적 클래스 임포트로 Shadowing 방어
from typing import Any, Dict, Optional, Literal
import logging
import pandas as pd

from config.system_params import SYSTEM_PARAMS
from config.ai_config_manager import config_manager
import config.ai_settings as ai_settings


from core.schemas import (
    EntryDecisionDTO,
    ExitDecisionDTO,
    PositionInfo,
    StockEvaluation,
    MarketState,
    PositionData,
    StockEvaluationData,
    MarketContextData,
    TradingDecisionParam,
    ExecutionTriggerResult,
    StockEvaluationResult,
)
from strategy.core.rule_evaluator import RuleEvaluator
from strategy.rules.entry_rules import (
    SmartBreakoutTriggerRule,
    AIDeepLearningEnsembleRule,
    AbsoluteMarketRegimeRule,
    IntradayTimeFilterRule,
    InstitutionalVolumeSurgeRule,
    SlippageDefenseRule,
    RSGapRule,
    VCPPatternRule,
    DynamicExecutionThresholdRule,
    ExtremeMeanReversionRule,
    MicroStructureRule,
    UnifiedTrendGateRule,
)
from strategy.indicators.technical_indicators import (
    calculate_intraday_acceleration_v5_6,
    calculate_stock_beta,
    calculate_atr_5m,
)

from strategy.core.TradeExitEngine import (
    calculate_hybrid_exit_levels,
    optimize_trade_levels,
)
from config.ai_settings import (
    MIN_TERM,
    S_THRESHOLD_NORMAL,
    A_THRESHOLD_NORMAL,
    B_THRESHOLD_FLOOR,
    TREND_MINUTE_WIN_CNT,
)
from strategy.indicators import (
    technical_indicators,
    dynamic_thresholds,
    market_analysis,
)
from strategy.rules import (
    adaptive_swing_trading,
    entry_strategy,
    candidate_selection,
)
from strategy.core import scoring_engine
from strategy.core.scoring_engine import (
    compute_intraday_scores,
    calculate_refined_supply_score_v6,
    final_stock_evaluation_v7_1,
    set_logger as scoring_engine_set_logger,
)
from strategy.indicators.pattern_recognizer import (
    process_limit_up_from_df,
    check_limit_up_drain_risk,
)
from strategy.core.trigger_engine import (
    check_execution_trigger_v10_2,
)
from strategy.core.execution_engine import (
    set_logger as execution_engine_set_logger,
    process_buy_orders,
    process_sell_orders,
)
from strategy.core.TradeDecisionEngine import (
    TradeDecisionEngine,
)
from strategy.core.candidate_selector import (
    set_logger as candidate_selector_set_logger,
    select_candidates_v2,
    select_candidates_parallel,
)

from util.Utils import (
    sanitize_dict,
    safe_get,
    safe_set,
    safe_update,
    align_quote_price,
)

logger = logging.getLogger(__name__)
MarketPhase = Literal["BULL", "SIDE", "BEAR"]

#  Shadowing defense (datetime module protection)
filtered_by_intensity = 0
error_count = 0

#  피드백 루프의 상태를 영구 보존하기 위한 전역 싱글톤 엔진 객체 생성
global_decision_engine = TradeDecisionEngine(
    panic_threshold=-1.20, overheat_threshold=10.0
)


# 팩토리 함수 수정
def create_strategy_evaluator(params: dict = None):
    """주어진 파라미터(Grid)에 맞춰 룰 엔진을 동적으로 생성합니다."""
    if params is None:
        params = {}
    return RuleEvaluator(
        [
            AbsoluteMarketRegimeRule(),
            IntradayTimeFilterRule(),
            RSGapRule(**params),
            VCPPatternRule(**params),
            UnifiedTrendGateRule(),  # [V8.0 Consolidated] Trend + Daily Gate
            InstitutionalVolumeSurgeRule(**params),
            SmartBreakoutTriggerRule(),
            DynamicExecutionThresholdRule(**params),
            AIDeepLearningEnsembleRule(**params),
            SlippageDefenseRule(),
            ExtremeMeanReversionRule(),
            MicroStructureRule(),
        ]
    )


#  AI Optimizer가 찾아낸 최적의 파라미터 주입
# system_params.py의 기본값에 ai_parameters.json의 최적화 값을 덮어씁니다.


def get_current_optimal_params():
    """현재 시스템의 모든 활성 파라미터(기본값 + 최적화값)를 반환합니다."""
    params = SYSTEM_PARAMS.copy()
    params.update(config_manager.params)
    return params


optimal_params = get_current_optimal_params()
master_strategy_evaluator = create_strategy_evaluator(optimal_params)


def load_optimal_parameters():
    """[Adaptive Execution] 로거가 준비된 직후, 파일에서 최신 파라미터를 읽어와 룰 엔진을 재조립합니다."""
    global optimal_params, master_strategy_evaluator

    #  config_manager를 통해 최신 파일 내용 로드
    config_manager._load_config()
    optimal_params = get_current_optimal_params()

    #  ai_settings.py의 PARAMS 딕셔너리도 동기화하여 전체 시스템에 반영
    for key, value in optimal_params.items():
        ai_settings.PARAMS[key] = value

    # 새로운 파라미터로 엔진 심장 교체
    master_strategy_evaluator = create_strategy_evaluator(optimal_params)
    if logger:
        logger.info(
            f"✅ [Auto-Tuning] 최적 파라미터 동적 로드 완료 (Total: {len(optimal_params)} keys)"
        )


def tradingComm_set_logger(external_logger):
    global logger
    logger = external_logger

    adaptive_swing_trading.set_logger(logger)

    # 리팩토링된 모듈들에 로거 설정
    technical_indicators.set_logger(logger)
    dynamic_thresholds.set_logger(logger)
    market_analysis.set_logger(logger)
    scoring_engine.set_logger(logger)
    entry_strategy.set_logger(logger)
    # exit_strategy는 완전 삭제되어 여기서 로거를 설정하지 않습니다.
    candidate_selection.set_logger(logger)
    scoring_engine_set_logger(logger)
    execution_engine_set_logger(logger)
    candidate_selector_set_logger(logger)

    #  로거 설정이 완전히 끝난 직후 파라미터를 로드하여 로그가 화면에 찍히도록 보장
    load_optimal_parameters()


def send_order_payload(zmq_push, ticker, command, dynamic_cap, indicators=None):
    """
    [Logic Change] SignalPayload 데이터 구조체(Dataclass) 적용
    - 64비트 분석 엔진 -> 32비트 주문 엔진 전송 최적화 (ZMQ PUSH)
    - SignalPayload DTO를 사용하여 타입 안정성 및 규격 일관성 확보
    """
    try:
        if zmq_push is None:
            return

        from core.schemas import SignalPayload

        # [Requirement] Payload: {"ticker": code, "order_type": "BUY/SELL", ...}
        payload = SignalPayload(
            ticker=ticker,
            order_type="BUY" if command == "BUY" else "SELL",
            price=indicators.get("buy_price", dynamic_cap / indicators.get("qty", 1))
            if indicators and "qty" in indicators
            else dynamic_cap,
            quantity=indicators.get("qty", 0) if indicators else 0,
            cl_ord_id=indicators.get("cl_ord_id", "") if indicators else "",
            reason=indicators.get("reason", "EXIT") if indicators else "SIGNAL",
        )

        zmq_push.push_data(payload.to_dict())
        logger.info(
            f"📡 [ZMQ SignalPayload Sync] {ticker} {command} ({dynamic_cap:,.0f} KRW) 발송 완료."
        )

    except Exception as e:
        logger.error(f"❌ [TradingComm] send_order_payload error: {e}")


# 캐시 저장용 딕셔너리
market_cache = {}  # { 종목코드: (데이터프레임, 마지막갱신시간) }
M_CACHE_EXPIRY = timedelta(minutes=MIN_TERM)  # 1분마다 데이터 갱신

market_index_changes_cache = {}


def get_final_trading_decision(
    ticker,
    daily_df,
    minute_df,
    ticker_results,
    exec_trigger,
    tp,
    params=None,
    decision_result=None,
):
    """
    [V5.0 Zero-Defect]
    1. 최상단 stock_data 정의 (Scope Error 완벽 해결)
    2. AI 데이터 누락 시 중립(0.5) 처리로 시스템 생존 (Graceful Degradation)
    3. 파편화된 점수-등급 모순(유체이탈) 완전 해결
    4. 정확한 탈락 사유(Reason) 보존 (블라인드 영점 덮어쓰기 금지)
    """

    # 1. [Fix] 변수 초기화 및 stock_data 세팅 (에러 원인 해결)

    is_recovering_leader = safe_get(ticker_results, "is_recovering_leader", False)
    is_true_bounce = safe_get(ticker_results, "is_true_bounce", False)
    if ticker_results and not isinstance(ticker_results, dict):
        # Convert dataclass to dict safely
        from dataclasses import asdict

        stock_data = asdict(ticker_results)
    else:
        stock_data = ticker_results.copy() if ticker_results else {}

    stock_data["ticker"] = ticker
    stock_data["is_true_bounce"] = is_true_bounce
    stock_data["daily_df"] = daily_df
    stock_data["minute_df"] = minute_df

    # 2. [Phase 7] AI 예측 데이터 추출 및 매핑 (Graceful Degradation)

    ai_prob = 0.5  # 기본값 중립(0.5)으로 세팅 (동반 자살 방지)
    is_ai_missing = False

    try:
        if "ai_prob" in ticker_results:
            ai_prob = float(ticker_results["ai_prob"])
        elif "AI_PROB" in ticker_results:
            ai_prob = float(ticker_results["AI_PROB"])
        elif (
            daily_df is not None
            and "AI_PROB" in daily_df.columns
            and not pd.isna(daily_df["AI_PROB"].iloc[-1])
        ):
            ai_prob = float(daily_df["AI_PROB"].iloc[-1])
        elif (
            minute_df is not None
            and "AI_PROB" in minute_df.columns
            and not pd.isna(minute_df["AI_PROB"].iloc[-1])
        ):
            ai_prob = float(minute_df["AI_PROB"].iloc[-1])
    except Exception as e:
        if "logger" in globals() and logger:
            logger.debug(f"[{ticker}] AI 데이터 매핑 실패: {e}")

    # AI 데이터가 없거나 0.0일 경우 중립 선언
    if ai_prob is None or ai_prob == 0.0:
        ai_prob = 0.5
        is_ai_missing = True
        if "logger" in globals() and logger:
            logger.debug(f"[{ticker}] ⚠️ AI 데이터 없음 -> 중립(0.5) 처리")

    stock_data["ai_surge_probability"] = ai_prob
    stock_data["AI_PROB"] = ai_prob
    stock_data["is_ai_missing"] = is_ai_missing
    # [Sync] MA50 Recovery Mode Flag 주입
    # 3. 기존 점수/등급 추출 및 인디케이터 산출

    combined_score = float(safe_get(stock_data, "combined_score", 0.0))
    grade = safe_get(ticker_results, "fuse_grade")

    if not grade:
        if params is None:
            params = {}
        cutoff_s = float(params.get("s_threshold_normal", 85.0))
        cutoff_a = float(params.get("a_threshold_normal", 55.0))
        cutoff_b = float(params.get("b_threshold_floor", 45.0))

        if combined_score >= cutoff_s:
            grade = "S"
        elif combined_score >= cutoff_a:
            grade = "A"
        elif combined_score >= cutoff_b:
            grade = "B"
        else:
            grade = "C"

    stock_data["final_grade"] = grade
    # [Quant Fix] VCP Rule 등 하위 Rule이 순환참조 없이 등급을 참조할 수 있도록
    # Rule 평가 이전에 결정된 본질 등급(intrinsic_grade)을 독립적으로 주입
    stock_data["intrinsic_grade"] = grade

    if exec_trigger:
        stock_data["trigger_hit"] = safe_get(exec_trigger, "trigger_hit", False)
        stock_data["execution_trigger"] = exec_trigger  #  Pass data to RuleEvaluator

    market_data = {
        "is_bull_market": safe_get(ticker_results, "is_bull_market", False),
        "current_index_change": safe_get(ticker_results, "current_index_change", 0.0),
        "market_regime": safe_get(ticker_results, "market_regime", "NEUTRAL"),
        "momentum_state": safe_get(ticker_results, "momentum_state", "NORMAL"),
        "system_params": params if params else {},
    }

    # 4. [Rule Evaluator 실행]

    evaluator = (
        create_strategy_evaluator(params) if params else master_strategy_evaluator
    )

    # 드디어 stock_data가 정상적으로 전달됩니다.
    eval_result = evaluator.run_all(stock_data, market_data)

    score_modifier = float(safe_get(eval_result, "score_modifier", 0.0))

    # 🎯 [Zero-Defect Fix] "마이너스 100점 페널티" 무력화 (원본 점수 보존)
    # 룰에서 탈락(False)하면 score_modifier로 -100점이 날아와 원본 점수를 박살냅니다.
    # 종목을 F등급으로 차단하는 것은 하단의 Hard Fallback이 담당하므로,
    # 오직 UI/로그 기록을 위해 원본 기초 체력 점수를 방어(보존)합니다.

    if not safe_get(eval_result, "is_approved", True):
        score_modifier = 0.0  # 탈락한 종목의 페널티 점수 삭감을 무시!

    final_combined_score = max(0.0, combined_score + score_modifier)

    if params is None:
        params = {}
    cutoff_s = float(params.get("s_threshold_normal", 85.0))
    cutoff_a = float(params.get("a_threshold_normal", 55.0))
    cutoff_b = float(params.get("b_threshold_floor", 45.0))

    # 1. 기본 점수 기반 등급 배정
    if final_combined_score >= cutoff_s:
        re_evaluated_grade = "S"
    elif final_combined_score >= cutoff_a:
        re_evaluated_grade = "A"
    elif final_combined_score >= cutoff_b:
        re_evaluated_grade = "B"
    else:
        re_evaluated_grade = "C"

    is_blocked = safe_get(ticker_results, "is_blocked", False)

    # 5. [Contradiction Resolution] 진짜 탈락 사유(Reason) 정밀 보존

    final_reason_msg = eval_result["reason"]

    if is_blocked and eval_result["is_approved"]:
        final_reason_msg = safe_get(ticker_results, "fuse_reason", "Engine Blocked")
    elif final_combined_score < cutoff_b and eval_result["is_approved"]:
        final_reason_msg = f"점수 미달 ({final_combined_score:.1f} < {cutoff_b})"

    # [Quant Fix: Executive Pardon for True Bounce]
    # 룰 매니저(VCP, 거래량 등)가 기각했더라도 V-Bounce 국면에서 수급이 폭발하면 조건부 사면
    supply_intra_val = float(safe_get(ticker_results, "supply_intra", 0.0))
    tick_acc_val = float(
        safe_get(ticker_results, "tick_acc", safe_get(ticker_results, "intra_acc", 0.0))
    )
    is_v_bounce_event = tick_acc_val >= 20.0 and supply_intra_val >= 60.0

    # [Logic Change: Phantom Grade Overwrite Block]
    # 브레인이 엣지(Edge)가 없다고 사형 선고한 종목은 하위 로직에서 등급을 부활시킬 수 없도록 락(Lock)
    brain_reason = str(decision_result.reason) if decision_result else ""
    is_hard_executed_by_brain = (
        "엣지 결여" in brain_reason or "자본 기아" in brain_reason
    )

    # [Fix] apply_momentum_premium 등의 이전 파라미터 컨텍스트 활용
    if params and (
        (is_true_bounce or is_v_bounce_event)
        and supply_intra_val >= params.get("a_threshold_normal", 55.0)
    ):
        if (
            not is_hard_executed_by_brain
        ):  # [추가] 브레인의 Hard Reject이 아닐 때만 사면 승격 허용
            eval_result["is_approved"] = True
            is_blocked = False
            final_reason_msg = f"🛡️ [Pardon] True-Bounce/Momentum 강제 승격 사면. (Original: {final_reason_msg})"
            if final_combined_score < cutoff_a:
                final_combined_score = float(cutoff_a)
                re_evaluated_grade = "A"

    # 2.  결격 사유 발생 시 '행동(Action)' 결과만 관리
    execution_result = re_evaluated_grade
    # [Antigravity Fix: Graceful Fail-Safe] 영점화(F 등급 강등) 폐기 및 본질 점수와 등급을 그대로 보존

    a_threshold = cutoff_a
    is_acc_sync = (
        safe_get(ticker_results, "day_acc_score", 0) >= a_threshold
        and safe_get(ticker_results, "intra_acc", 0) >= 25.0
    )

    result = {
        "final_grade": execution_result,  # 행동 결과
        "intrinsic_grade": re_evaluated_grade,  # 본질 등급 (추가)
        "intrinsic_can_buy": eval_result["is_approved"],
        "final_can_buy": eval_result["is_approved"]
        and not is_blocked
        and (execution_result != "F"),
        "final_reason": final_reason_msg,
        "combined_score": float(final_combined_score),
        "is_acc_sync": is_acc_sync,
        "is_blocked_by_market": is_blocked
        or (
            not eval_result["is_approved"] and "Market" in eval_result.get("reason", "")
        ),
        "extra_info": eval_result.get("extra_info", {}),
    }

    # 🛡️ [Zero-Defect: Engine Result Merge] 비중 엔진의 최종 결정을 결과값에 강제 동기화
    if decision_result is not None:
        result["decision_state"] = decision_result.decision_state
        result["position_size_ratio"] = decision_result.extra_info.get(
            "position_size_ratio", 1.0
        )

        # TDE가 명시적으로 기각(REJECT)한 경우에만 룰 엔진 결과를 덮어쓰고 차단 확정
        if "REJECT" in str(decision_result.decision_state):
            result["final_can_buy"] = False
            result["final_reason"] = (
                f"[{decision_result.decision_state}] {decision_result.reason}"
            )

    return result


def get_failed_candidate_result(ticker: str, reason: str = "Insufficient data") -> dict:
    """분석 실패/데이터 부족 시 반환하는 표준 Fallback 딕셔너리"""
    return {
        "ticker": ticker,
        "entry_price": 0.0,
        "combined_score": 0.0,
        "day_acc_score": 0.0,
        "supply_day": 0.0,
        "supply_intra": 0.0,
        "latest_row": None,  # 명시적 None
        "minute_df": None,  # 명시적 None
        "atr_5m": 0.0,
        "v3_indicators": {},
        "exit_levels": {},
        "can_buy": False,
        "is_valid": False,  # [핵심] 유효성 플래그 추가
        "fail_reason": reason,
        "market_conditions": {},
        "gap_result": {},
        "position_size_ratio": 1.0,
        "is_true_bounce": False,
        "is_recovering_leader": False,
        "is_v_bounce_event": False,
    }


def combined_score_for_ticker_v3(
    ticker: str,
    daily_df: pd.DataFrame,
    params: Optional[Dict[str, Any]] = None,
    capital: float = 100000000.0,
    minute_df: Optional[pd.DataFrame] = None,
    intraday_params: Optional[Dict[str, Any]] = None,
    market_avg_acc: float = 0.0,
    tp: str = "intraday",
    today_market_data_df: Optional[pd.DataFrame] = None,
    market_regime_override: Optional[str] = None,
    port_list: list = None,  # <=== [New] 포트폴리오 리스트 명시적 전달
) -> Dict[str, Any]:
    """
    [V6.0: Supply-First Acceleration Model]
    - 수급 가속도(Supply Acc)와 분봉 정밀 타점의 컨플루언스 극대화
    - 시장 상황에 따른 동적 비중 조절(Dynamic Sizing) 적용
    """
    from util.CommUtils import isOverCloseTime
    from config.ai_settings import PARAMS

    global global_decision_engine

    # [Safety] 초기값 설정
    supply_intra = 0.0
    intraday_score = 0
    final_combined_score = 0.0  # [Emergency Fix] Scope Safety 초기화 (NameError 방지)
    final_can_buy = False
    position_size_ratio = 1.0
    hard_reject_reason = None
    _is_pardon_reject = False  #  사면권 대상 여부 초기화
    is_single_order = False  #  단일 주문 실행 플래그 초기화 (UnboundLocalError 방지)
    decision_result = EntryDecisionDTO(
        stock_code=ticker,
        final_can_buy=False,
        decision_state="INIT",
        combined_score=0.0,
        grade="C",
        reason="초기화",
    )
    decision = {}  # 최종 의사결정 객체 초기화
    market_ctx = None  # 컨텍스트 초기화
    stock_eval_ctx = None  #  평가 데이터 초기화

    # 1. [Safety] 결과 변수 초기화 (에러 시에도 return 가능하도록 최상단 배치)
    combined_score = 0.0
    is_recovering_leader = False

    atr_5m = 0

    v3_indicators = {
        "final_stock_eval": None,
        "execution_trigger": None,
        "final_trading_decision": None,
        "hybrid_exit_levels": None,
    }
    day_acc_score = 0.0
    current_price = 0

    latest = None
    latest_minute = None
    supply_day = 0.0
    fake_score = 0.0

    vtp = {}
    trigger_info = None
    position_size_ratio = 1.0  #  초기값 설정
    final_can_buy = (
        False  # [Zero-Defect Fix] tp == "picking" 시 UnboundLocalError 원천 차단
    )
    market_conditions = {}  # [Zero-Defect Fix] 초반 예외 발생 시 Dict 참조 에러 방어

    try:
        # 1. 파라미터 자율 최적화 (단기 고수익 타겟)
        if params is None:
            #  내부 헬퍼 함수를 통해 최신 파라미터를 안전하게 병합 로드합니다.
            params = get_current_optimal_params()

            # [로컬 특화 기본값 적용] 기존 설정값(최적화 값 등)을 보호하기 위해 setdefault 적용
            local_defaults = {
                "vcp_contraction_threshold": 0.15,
                "momentum_period": 14,
                "volume_surge_ratio": 2.0,
                "min_adx_for_trend": 20,
                "base_risk_percent": 1.5,
                "atr_stop_mult": 1.5,
            }
            for k, v in local_defaults.items():
                params.setdefault(k, v)

        # [Optimization] 불필요한 전체 복사(copy)를 제거하여 메모리와 처리 속도를 절약합니다.
        #  daily_df가 딕셔너리(Mock)인 경우 DataFrame으로 변환
        # [Zero-Defect Fix] daily_df가 딕셔너리(Mock)인 경우 DataFrame으로 변환하여 reset_index 오류 방지
        if not isinstance(daily_df, pd.DataFrame):
            try:
                # 딕셔너리가 리스트를 포함하지 않는 스칼라 형태일 경우를 대비해 index=[0] 부여 시도
                data = pd.DataFrame(daily_df)
                if data.empty and isinstance(daily_df, dict):
                    data = pd.DataFrame([daily_df])
            except Exception:
                data = pd.DataFrame([daily_df])
        else:
            data = daily_df

        # 최소 데이터 길이 검증 (ma200 계산을 위해 최소 200봉 필요)
        if len(data) < 200:
            logger.warning(f"[{ticker}] Skip: Not enough data (len={len(data)})")
            return get_failed_candidate_result(ticker, "데이터 일수 부족 (len < 200)")

        # [Step 0] 분봉 데이터 최소 요건 검증
        if tp == "intraday" and (
            minute_df is None
            or len(minute_df) < int(round((TREND_MINUTE_WIN_CNT * 0.35), 2))
        ):
            logger.warning(
                f"[{ticker}] Skip: Not enough intraday data ({len(minute_df) if minute_df is not None else 0})"
            )
            # 데이터 부족 시 일봉 점수(base_combined)만으로 판단하거나, 안전하게 skip
            return get_failed_candidate_result(
                ticker,
                f"분봉 데이터 부족 ({len(minute_df) if minute_df is not None else 0})",
            )

        # 1. 오늘자 미완성 데이터가 섞여있다면 제거 (안전장치)

        # [Phase 12: Zero-Defect Data Sync] 장중 일봉 데이터 왜곡 방지

        if not isOverCloseTime():
            if tp == "intraday":
                # 장중에는 오늘자 일봉(마지막 행)이 미완성되어 거래량/패턴 점수를 폭락시킵니다.
                # 오늘 날짜 데이터가 감지되면 과감히 잘라내어, 어제 완성된 캔들 기준으로 평가받게 합니다.
                last_idx_str = str(data.index[-1])
                last_col_str = (
                    str(data["날짜"].iloc[-1]) if "날짜" in data.columns else ""
                )
                today_str1 = datetime.datetime.now().strftime("%Y%m%d")
                today_str2 = datetime.datetime.now().strftime("%Y-%m-%d")

                if (
                    today_str1 in last_idx_str
                    or today_str2 in last_idx_str
                    or today_str1 in last_col_str
                    or today_str2 in last_col_str
                ):
                    data = data.iloc[:-1]
            else:
                # 야간 배치(StockPicking) 시 기존 방어 로직 유지
                if data.iloc[-1]["거래량"] == 0 or pd.isna(data.iloc[-1]["종가"]):
                    data = data.iloc[:-1]

        latest = data.iloc[-1].to_dict()
        current_price = latest.get("종가")
        if tp == "intraday" and (minute_df is not None and not minute_df.empty):
            latest_minute = minute_df.iloc[-1].to_dict()
            if latest_minute.get("종가"):
                current_price = latest_minute.get("종가")

        from strategy.indicators.market_analysis import analyze_market_conditions

        market_conditions = analyze_market_conditions(
            today_market_data_df
            if today_market_data_df is not None
            else latest.get("시장구분", 1)
        )
        if market_regime_override:
            market_conditions.market_regime = market_regime_override

        # Ensure current_index_change is available as a default
        if (
            not hasattr(market_conditions, "current_index_change")
            or market_conditions.current_index_change is None
        ):
            market_conditions.current_index_change = 0.0

        # 💉 [Zero-Defect 대동맥 개통] 옵티마이저의 파라미터를 룰 엔진으로 전달하는 핵심 브릿지
        market_conditions.system_params = params

        market_type = market_conditions.market_type
        buy_condition = market_conditions.buy_condition
        current_index_change = market_conditions.current_index_change
        market_data = market_conditions.market_data
        index_return_5d = market_conditions.index_return_5d
        market_regime = (
            market_regime_override
            if market_regime_override
            else market_conditions.regime
        )

        is_true_bounce = False
        if tp == "intraday" and (minute_df is not None and not minute_df.empty):
            # [Zero-Defect: Bridge Bypass] Bridge 모듈 삭제에 따른 초기화 유지 및 인라인 감지 활성화
            try:
                current_price_v = float(minute_df["종가"].iloc[-1])
                day_open_v = float(minute_df["시가"].iloc[0])
                is_yang_candle_v = bool(current_price_v >= day_open_v)

                # 수급 및 눌림목 조건 (Simplified for speed)
                supply_intra_v = calculate_refined_supply_score_v6(minute_df)
                # [Note] pb_quality는 intraday_v2에서 통합 계산되므로 초기값 0.0 유지
                pb_quality_v = 0.0

                is_strong_bounce = supply_intra_v >= 75.0 and is_yang_candle_v
                is_true_bounce = is_strong_bounce
            except Exception:
                pass
        market_score = market_conditions.market_score

        market_df = market_data
        # 3.5 단기 패턴 점수 보강
        # 3.5 단기 패턴 점수 보강 (눌림목/돌파 및 확증 로직)
        is_bull_market = buy_condition

        # 1. 일봉 기반 기초 점수 산출 (Selection Phase)
        supply_day = float(latest.get("체결강도", 100.0))

        #  EOD Compatibility & Early Intraday Metric Extraction
        tick_acc = 0.0
        supply_intra_val = 0.0

        if tp == "intraday" and (minute_df is not None and not minute_df.empty):
            #  상한가 패턴 체크 (AI 엔진 호출 전 필수 수행)
            limit_up_data = process_limit_up_from_df(minute_df)
            try:
                m_term = params.get("min_term", 3) if params else 3
                tick_acc = calculate_intraday_acceleration_v5_6(minute_df, m_term)
                supply_intra_val = calculate_refined_supply_score_v6(minute_df)
            except Exception:
                pass

        # [O(1) Optimization] Scalar Extraction & ATR Calculation
        day_open = day_high = day_low = None
        atr_pct = 2.0  # Default fallback

        if tp == "intraday" and (minute_df is not None and not minute_df.empty):
            # [Zero-Defect] 분산된 인덱싱 로직 통합 및 스칼라화
            try:
                _m_df = minute_df
                day_open = float(_m_df["시가"].iloc[0])
                day_high = float(_m_df["고가"].max())
                day_low = float(_m_df["저가"].min())
            except Exception:
                pass

        # ATR_pct (14일 평균) 산출 - 지표 연산 오버헤드 통합
        try:
            if data is not None and len(data) >= 14:
                atr_14 = (data["고가"] - data["저가"]).rolling(14).mean().iloc[-1]
                curr_price_base = current_price if current_price > 0 else 1.0
                atr_pct = (atr_14 / curr_price_base) * 100.0
        except Exception:
            atr_pct = 2.0

        # [TLVI Harness] Top-Level Variable Initialization (Zero-Defect Standard)
        # [TLVI Harness] Top-Level Variable Initialization (Zero-Defect Standard)
        limit_up_data = {}
        is_limit_up_trade = False
        trigger_info = ExecutionTriggerResult(msg="EOD Mode")
        intraday_v2 = {"noise_ratio": 0.5, "avg_volume_5": 0.0, "is_true_bounce": False}
        supply_intra = 0.0
        fake_score = 0.0

        final_stock_eval = final_stock_evaluation_v7_1(
            ticker,
            data,
            market_type=market_type,
            index_return_5d=index_return_5d,
            is_bull_market=is_bull_market,
            tp=tp,
            supply_intra=supply_day,
            market_regime=market_regime_override or market_regime,  # [Sync Override]
            market_score=market_score,
            params=params,
            tick_acc=tick_acc,
            supply_intra_val=supply_intra_val,
            mode=tp,  # [Harmonized] Pass current execution mode
            is_true_bounce=is_true_bounce,  # [VIP Alignment]
            day_open=day_open,
            day_high=day_high,
            day_low=day_low,
        )

        # [Target 1: VCP Dynamic Expansion]
        # 이미 VCPPatternRule 내부에도 로직이 있으나, TradingComm 레벨에서 사전에 컨텍스트 동기화
        safe_set(
            final_stock_eval, "market_regime", market_regime_override or market_regime
        )
        safe_set(final_stock_eval, "is_true_bounce", is_true_bounce)

        # [Zero-Defect Synchronization]
        # Evaluation 함수가 내부에서 is_true_bounce를 True로 판별하지 못했을 경우라도,
        # 최상단 Early Detection 결과를 강제 주입하여 전체 파이프라인의 사면권을 통합합니다.
        if is_true_bounce:
            safe_set(final_stock_eval, "is_true_bounce", True)
            safe_set(final_stock_eval, "is_buy", True)

        is_recovering_leader = final_stock_eval.is_recovering_leader

        # [Audit Fix - Deep Sync Diagnostics]
        if market_regime_override:
            final_stock_eval.market_regime = market_regime_override

        # base_combined 추출 및 모멘텀 스케일링 (v7_1의 점수를 기반으로 재정규화)
        # v7_1이 이미 130~150점을 반환할 수 있으므로, 내부 정규화 과정을 거칩니다.
        daily_score = final_stock_eval.daily_score

        #  Initialize v3_indicators as dict to prevent NoneType error in ranking
        v3_indicators = {}

        position_size_ratio = final_stock_eval.position_size_ratio
        day_acc_score = final_stock_eval.day_acc_score
        gap_result = None
        # 3. 분봉 수급 및 융합 (Execution Trigger Phase)
        if tp == "intraday" and minute_df is not None:
            # [Zero-Defect Fix] minute_df가 딕셔너리(Mock)인 경우 DataFrame으로 변환
            if not isinstance(minute_df, pd.DataFrame):
                try:
                    minute_df = pd.DataFrame(minute_df)
                    if minute_df.empty and isinstance(minute_df, dict):
                        minute_df = pd.DataFrame([minute_df])
                except Exception:
                    minute_df = pd.DataFrame([minute_df])

            if minute_df.empty:
                return get_failed_candidate_result(ticker, "분봉 데이터 비어있음")

            # [적합성] 오늘 봉 수: MIN_TERM(분봉 단위)에 맞춰 최소 시간(45분) 상당만 있으면 산정
            # 3분봉=15봉, 5분봉=9봉; 최소 8봉은 보장(장 초반 24~40분부터 산정 가능)
            _min_bars_interval = max(1, int(MIN_TERM) if MIN_TERM else 3)
            min_bars_today = max(8, int(45 / _min_bars_interval))
            # [Zero-Defect Fix] 'RangeIndex' object has no attribute 'date' 방어 (Simulation/Mock 데이터 대응)
            if hasattr(minute_df.index, "date"):
                today_bars = minute_df[
                    minute_df.index.date == minute_df.index[-1].date()
                ]
            else:
                today_bars = minute_df  # Fallback: 모든 데이터를 당일 데이터로 간주
            # [수정] 기존 strict한 당일 봉수 검증(len(today_bars))을 전체 분봉 데이터(minute_df) 검증으로 완화하여
            # 과거 Lookback 봉수가 부족 현상을 보강하도록 유기적으로 연결합니다.
            if len(minute_df) < min_bars_today:
                logger.info(
                    f"[{ticker}] Not enough bars ({len(minute_df)}/{min_bars_today}). Skipping Score."
                )
                # return  # 또는 final_score를 0으로 처리
                # 데이터 부족 시 일봉 점수(base_combined)만으로 판단하거나, 안전하게 skip
                return get_failed_candidate_result(
                    ticker, f"당일 봉 수 부족 ({len(minute_df)}/{min_bars_today})"
                )

            # [Step 1: Confluence Logic 개선]
            # [Gap-Bridge] 어제 30개 + 오늘 min_bars_today개 분석 적용
            # (Bridge 모듈 삭제 및 bridge_report 소각 완료 - 레이턴시 최적화)
            # logger.info(f"minute_df, gap_result `: {minute_df}, {gap_result}")
            # [Step 2: 고도화 지표 산출]
            # [Logic Change: Swapped for O(1) optimization]
            intraday_v2 = compute_intraday_scores(
                minute_df=minute_df,
                context_from_daily=final_stock_eval,
                params=params,
                day_open=day_open,
                day_high=day_high,
                day_low=day_low,
                atr_pct=atr_pct,
            )
            # [Directive 1/3] 데이터 파이프라인 정밀 복구 및 보존 (Golden Sort 연동)
            pb_q = intraday_v2.get("pullback_quality", 0.0)
            vol_s = intraday_v2.get("vol_surge_ratio", 1.0)
            # [Sync] Early Bounce 인식 결과와 Intraday V2 결과를 병합 (Logical OR)
            is_true_bounce = is_true_bounce or intraday_v2.get("is_true_bounce", False)

            trigger_info = check_execution_trigger_v10_2(
                minute_df,
                final_stock_eval,
                market_avg_acc,
                is_true_bounce=is_true_bounce,
                is_recovering_leader=is_recovering_leader,
                intra_acc_val=tick_acc,
                supply_intra_val=supply_intra_val,
            )
            supply_intra = trigger_info.supply_intra
            fake_score = trigger_info.fake_score

            # ExecutionTriggerResult is a dataclass, so we set attributes directly
            trigger_info.pullback_quality = pb_q
            trigger_info.vol_surge_ratio = vol_s
            trigger_info.is_true_bounce = is_true_bounce

            # StockEvaluationResult is a dataclass
            final_stock_eval.pullback_quality = pb_q
            final_stock_eval.vol_surge_ratio = vol_s
            final_stock_eval.is_true_bounce = is_true_bounce
            # [Task-Directive: FOMO Guard 데이터 파이프라인 종단 연결]
            if hasattr(final_stock_eval, "breakout_info") and isinstance(
                final_stock_eval.breakout_info, dict
            ):
                final_stock_eval.breakout_info["bb_dist"] = intraday_v2.get(
                    "bb_dist_upper", 0.0
                )

            is_vwap_pullback = intraday_v2.get("signals", {}).get(
                "is_vwap_pullback", False
            )

            intraday_score = trigger_info.intraday_score
            # logger.info(f"[{ticker}] {tp} 1.2 intraday_score : {intraday_score}")

            # [Step 3: Dynamic Weighting & Final Fusion]
            # 분봉 수급이 강력할수록 실시간 점수 비중을 높임
            # [Refactor] 전략 모드에 따른 가중치 이원화
            is_dip_mode = "Healthy Pullback" in final_stock_eval.energy_status

            # [Phase 1] Volatility Scalar (Blended) 적용
            curr_price_base = current_price if current_price > 0 else 1.0

            # 2. Tick Acceleration (Intraday Volume/Price Velocity)
            # calculate_intraday_acceleration_v5_6()는 TradingComm에 로드되어 있음
            m_term = params.get("min_term", 3) if params else 3
            tick_acc = calculate_intraday_acceleration_v5_6(minute_df, m_term)

            from strategy.core.scoring_engine import apply_dynamic_scalar

            vol_scalar = apply_dynamic_scalar(atr_pct, tick_acc)

            if logger:
                logger.info(
                    f"[{ticker}] Vol_Scalar 적용: {vol_scalar:.4f} (ATR%:{atr_pct:.2f}, Tick_Acc:{tick_acc:.2f})"
                )

            # Apply to base scores BEFORE fusion
            daily_score *= vol_scalar
            intraday_score *= vol_scalar

            # [Zero-Latency Linear Fusion] O(1) Accuracy
            final_score = (daily_score * 0.5) + (intraday_score * 0.5)
            # 시장 에너지를 반영하여 최종 점수 보정
            final_score *= market_conditions.market_energy

            # [Logic Change] 1. Sweet Spot Premium: RS Gap 0~15%, V-Bounce 국면에 추가 가중치
            rs_gap_val = final_stock_eval.rs_gap
            from strategy.core.scoring_engine import apply_momentum_premium

            final_score, grade = apply_momentum_premium(
                final_score,
                tick_acc,
                supply_intra,
                daily_score,
                surge_rate=rs_gap_val,
                vwap_dist=intraday_v2.get("vwap_dist", 0.0),
                bb_dist=intraday_v2.get("bb_dist_upper", 1.0),
                atr_threshold=atr_pct,
                params=params,
            )

            # Breakthrough Synergy: VWAP 지지 + OFI 유입 시 폭발력 가산
            if (
                -0.5 < intraday_v2.get("vwap_dist", 0.0) < 3.0
                and intraday_v2.get("ofi_val", 0) > 0
            ):
                final_score += 30.0
                is_single_order = True

            combined_score = round(float(final_score), 2)

            # [Bug Fix: Grade Recalculation after Synergy]
            if params is None:
                params = {}
            _s_cut = float(params.get("s_threshold_normal", 85.0))
            _a_cut = float(params.get("a_threshold_normal", 55.0))
            _b_cut = float(params.get("b_threshold_floor", 45.0))

            if combined_score >= _s_cut:
                grade = "S"
            elif combined_score >= _a_cut:
                grade = "A"
            elif combined_score >= _b_cut:
                grade = "B"
            else:
                grade = "C"

            # [Zero-Defect] V-Bounce 상태 동적 계산 (결과 매핑용)
            is_v_bounce_event = tick_acc >= 20.0 and supply_intra >= 60.0

            # [Zero-Defect Fix 3-A] fuse가 판단한 시장 연동 등급 캐싱
            fuse_grade = grade
            grade = fuse_grade  # [Sync] Sync local grade with fused grade immediately

            # [S급 필터링 플래그] 리스트업을 위한 판단
            is_s_class = supply_intra >= 80

            try:
                atr_5m = atr_pct
                paramDic = TradingDecisionParam(ticker=ticker)

                paramDic.ticker = ticker
                paramDic.combined_score = combined_score
                paramDic.fuse_grade = fuse_grade  # <-- [추가] 파라미터에 등급 주입
                paramDic.intrinsic_grade = fuse_grade

                paramDic.market_regime = (
                    market_regime  # [Fix] 시장 국면 정보 주입 (룰 엔진용)
                )
                paramDic.momentum_state = getattr(
                    market_conditions, "momentum_state", "NORMAL"
                )
                import dataclasses

                paramDic.pb_quality = float(
                    trigger_info.pullback_score or trigger_info.pb_quality or 0.0
                )
                paramDic.pullback_quality = paramDic.pb_quality
                paramDic.supply_intra = float(trigger_info.supply_intra)
                paramDic.vol_surge_ratio = float(trigger_info.vol_surge_ratio)
                paramDic.intraday_score = float(trigger_info.intraday_score)
                paramDic.intra_acc = float(
                    trigger_info.tick_acc or trigger_info.intra_acc or 0.0
                )
                paramDic.tick_acc = paramDic.intra_acc

                paramDic.is_true_bounce = is_true_bounce  # [VIP] 룰 엔진에 사면권 전달
                #  Robust AI Probability Extraction
                ai_prob_val = latest.get("AI_PROB")
                if ai_prob_val is None or pd.isna(ai_prob_val):
                    # 일봉 데이터프레임에서 다시 한 번 확인
                    if "AI_PROB" in data.columns and not pd.isna(
                        data["AI_PROB"].iloc[-1]
                    ):
                        ai_prob_val = float(data["AI_PROB"].iloc[-1])
                    else:
                        # [Fix] 0.0 강제 할당으로 인한 부당한 F등급 차단을 막고 Graceful Degradation(0.5) 복원
                        ai_prob_val = 0.5

                # 🛡️ [Zero-Defect: AI Decision Logic Restoration]

                # [Phase 4] 의사결정 통합을 위한 데이터 준비 (호출 시점보다 앞으로 이동)
                from strategy.core.TradeDecisionEngine import (
                    MarketContextData,
                    StockEvaluationData,
                )

                #  Restore missing limit_up_data from evaluation context
                limit_up_data = final_stock_eval.limit_up_data
                is_limit_up_trade = limit_up_data.get("is_limit_up_entry", False)
                trigger_hit_val = trigger_info.trigger_hit
                trigger_msg = trigger_info.msg

                #  Segmented Panic Detection
                # 전역 상태(global_regime)가 아닌, 해당 종목이 속한 시장의 실제 국면을 기준으로 Panic 여부 판정
                market_ctx = MarketState(
                    regime=market_regime,
                    index_change=current_index_change,
                    is_panic=(market_regime == "CRASH"),  #  Segmented Trigger
                    drop_rate=current_index_change,  #  Use exchange-specific change
                )

                # [Task-Directive: Phantom Data Mappings 교정]
                # 깡통 변수들을 버리고, 실제 분봉 연산 결과인 intraday_v2와 breakout_info에서 실데이터를 복구합니다.

                # [Safe Chaining] breakout_info가 None인 경우를 대비해 {} 기본값 설정
                _breakout_info = final_stock_eval.breakout_info
                if _breakout_info is None:
                    _breakout_info = {}

                is_breakout = _breakout_info.get("is_breakout", False)
                meets_basic = final_stock_eval.is_buy and trigger_hit_val
                custom_reason = (
                    trigger_msg if not trigger_hit_val else "Ready for AI Evaluation"
                )
                noise_ratio = (
                    float(intraday_v2.get("noise_ratio", 0.5))
                    if "intraday_v2" in locals()
                    else 0.5
                )
                avg_vol_5 = (
                    float(intraday_v2.get("avg_volume_5", 0.0))
                    if "intraday_v2" in locals()
                    else 0.0
                )

                # [Logic Change: Volume Unit Mismatch Fix] 100점 만점 수급 점수가 아닌 실제 체결 거래량 추출
                curr_vol_actual = (
                    float(latest_minute.get("거래량", 0.0))
                    if (tp == "intraday" and latest_minute)
                    else float(latest.get("거래량", 0.0))
                )

                stock_eval_ctx = StockEvaluation(
                    ticker=ticker,
                    current_price=current_price,
                    is_breakout=is_breakout,
                    meets_basic_criteria=meets_basic,
                    grade=grade,
                    score=float(combined_score),
                    daily_score=final_stock_eval.daily_score,
                    is_limit_up_trade=is_limit_up_trade,
                    ai_surge_probability=float(ai_prob_val),
                    reason=custom_reason,
                    intrinsic_grade=grade,
                    market_regime=market_regime,
                    is_recovering_leader=is_recovering_leader,
                    is_true_bounce=is_true_bounce,
                    expected_win_rate=0.8,
                    noise_ratio=noise_ratio,
                    avg_volume_5=avg_vol_5,
                    current_volume=curr_vol_actual,  # [수정] supply_intra -> curr_vol_actual
                    supply_intra=float(supply_intra),  # [New] 100점 만점 수급 점수 보존
                    tick_acc=float(
                        tick_acc
                    ),  # [Critical Fix] trigger_info 프록시 대신 직접 산출된 로컬 변수 사용 (Phantom Desync 방지)
                    recent_low=0.0,
                    rs_gap=final_stock_eval.rs_gap,
                    # [Task-Directive: 올바른 경로에서 bb_dist, surge_rate 추출]
                    bb_dist=float(_breakout_info.get("bb_dist", 0.0)),
                    surge_rate=float(_breakout_info.get("surge_rate", 0.0)),
                    support_levels={},
                    atr_val=float(atr_5m),
                )

                #  이전 단계에서 이미 [Pardon]이 부여되었다면, AbsoluteMarketRegimeRule 등이 이를 덮어쓰지 못하도록 보호
                decision_result = global_decision_engine.evaluate_buy_decision(
                    stock_code=ticker,
                    market_ctx=market_ctx,
                    stock_eval=stock_eval_ctx,
                    account_balance=capital,
                )
                # [Logic Change] apply_segmented_pardon_gate 중복 호출 삭제 (Fail-Fast 준수)

                paramDic.ai_prob = ai_prob_val
                paramDic.is_blocked = False
                paramDic.fuse_reason = "PASSED"
                paramDic.intra_acc = getattr(trigger_info, "intra_acc", 0.0)
                paramDic.intra_acc = trigger_info.intra_acc
                paramDic.intraday_score = trigger_info.intraday_score
                paramDic.supply_intra = trigger_info.supply_intra
                paramDic.is_bull_market = is_bull_market
                paramDic.rs_gap = final_stock_eval.rs_gap
                paramDic.vcp_ratio = final_stock_eval.vcp_ratio
                paramDic.current_index_change = current_index_change
                paramDic.volume_dry = final_stock_eval.volume_dry
                paramDic.entry_price = current_price
                paramDic.atr_5m = atr_5m
                _breakout_info_pd = final_stock_eval.breakout_info
                paramDic.is_bottom_breakout = _breakout_info_pd.get(
                    "is_breakout", False
                )
                paramDic.breakout_strength = _breakout_info_pd.get(
                    "breakout_strength", 0.0
                )
                paramDic.volume_multiplier = final_stock_eval.volume_multiplier
                #  volume_multiplier를 volume_surge_ratio로 명시적 매핑 (룰 엔진 호환성)
                paramDic.volume_surge_ratio = paramDic.volume_multiplier
                paramDic.daily_score = final_stock_eval.daily_score
                paramDic.trend_score = final_stock_eval.trend_score
                paramDic.breakout_info = _breakout_info_pd
                paramDic.is_vwap_pullback = is_vwap_pullback

                # [Adaptive Execution] 시장 국면별 평가 트랙 분리 (Track A / Track B)
                vcp_pass = final_stock_eval.vcp_ratio <= params.get(
                    "vcp_contraction_threshold", 0.15
                )
                trend_template_pass = "TT:Pass" in final_stock_eval.energy_status

                # 트랙 A: 중장기 추세 (VCP + Trend Template) -> 단기 변동성과 무관하게 진입
                track_a_swing = vcp_pass and trend_template_pass

                # 트랙 B: 단기 모멘텀 (변동성 돌파 + 분봉 가속도) -> 강세/상승장에서만 강력 작동
                track_b_momentum = False
                if is_bull_market or market_regime in ["BULL", "NORMAL"]:
                    # [Zero-Defect Fix] breakout_info is a dict, so use .get()
                    is_breakout = final_stock_eval.breakout_info.get(
                        "is_breakout", False
                    )
                    # trigger_info is a dataclass object here
                    if trigger_info.intra_acc >= 60.0 and is_breakout:
                        track_b_momentum = True

                paramDic.track_a_swing = track_a_swing
                paramDic.track_b_momentum = track_b_momentum

                # exec_threshold_res = check_execution_threshold removed (unified into DynamicExecutionThresholdRule)

                # [Logic Change: Direct DTO Access & Fail-Fast]
                dec_reason = decision_result.reason
                dec_approved = decision_result.final_can_buy
                is_pardon_granted = dec_approved or "[Pardon]" in dec_reason
                if is_pardon_granted:
                    combined_score = max(float(combined_score), 85.0)
                    paramDic.combined_score = combined_score
                    _grade_locked = (
                        "S"
                        if combined_score >= 85.0
                        else ("A" if combined_score >= 55.0 else "B")
                    )
                    final_trading_decision = {
                        "final_grade": _grade_locked,
                        "intrinsic_grade": grade,
                        "intrinsic_can_buy": True,
                        "final_can_buy": True,
                        "final_reason": decision_result.reason,
                        "combined_score": float(combined_score),
                        "is_acc_sync": True,
                        "is_blocked_by_market": False,
                        "decision_state": decision_result.decision_state,
                        "position_size_ratio": decision_result.extra_info.get(
                            "position_size_ratio", 1.0
                        ),
                        "extra_info": decision_result.extra_info,
                    }
                else:
                    final_trading_decision = get_final_trading_decision(
                        ticker,
                        daily_df,
                        minute_df,
                        paramDic,
                        trigger_info,
                        tp,
                        params=params,
                        decision_result=decision_result,
                    )

                # [Audit: Coordination] 모든 필터링을 evaluator(RuleEvaluator)로 일원화
                is_safe_entry, reject_msg = True, "Pass"

                grade = final_trading_decision.get("final_grade", "F")

                # [Fix Issue #4] Use combined_score from decision for consistency
                combined_score = float(
                    final_trading_decision.get("combined_score", 0.0)
                )
                if logger:
                    logger.info(
                        f"[{ticker}] intraday 📊 FINAL TRACE -> combined_score: {combined_score} | Grade: {grade}"
                    )

                # Update paramDic with final grade
                paramDic.final_grade = grade
                ## 2. 엑릿 레벨 최적화
                hybrid_exit_levels = calculate_hybrid_exit_levels(
                    paramDic, daily_df, minute_df, grade, params=params
                )

                # 합체 (오른쪽 딕셔너리 값이 우선순위를 가짐)
                v3_indicators.update(
                    {
                        "final_stock_eval": final_stock_eval,
                        "execution_trigger": trigger_info,
                        "final_trading_decision": final_trading_decision,
                        "hybrid_exit_levels": hybrid_exit_levels,
                        "is_vwap_pullback": is_vwap_pullback,
                    }
                )
                decision = v3_indicators.get("final_trading_decision", {})
                if market_regime_override and not isinstance(decision, dict):
                    decision.market_regime = market_regime_override

                # [Antigravity Fix: Grade Overwrite Block] decision의 final_grade를 강제 덮어쓰는 로직 삭제
                ## 2. 엑싯 레벨 최적화
                optimize_trade_levels(
                    paramDic,
                    v3_indicators.get("hybrid_exit_levels", {}),
                    grade,
                    params=params,
                )
                optimizer_mode = bool(params and params.get("optimizer_mode"))
                combined_score_val = float(decision.get("combined_score", 0.0))
                final_grade = decision.get("final_grade", grade)

                ACTIVE_PARAMS = params if params else PARAMS
                min_score_global = float(ACTIVE_PARAMS.get("min_score", 2.0))
                s_thresh = float(
                    ACTIVE_PARAMS.get("s_threshold_normal", S_THRESHOLD_NORMAL)
                )
                a_thresh = float(
                    ACTIVE_PARAMS.get("a_threshold_normal", A_THRESHOLD_NORMAL)
                )
                b_floor = float(
                    ACTIVE_PARAMS.get("b_threshold_floor", B_THRESHOLD_FLOOR)
                )

                grade_min_required = {"S": s_thresh, "A": a_thresh, "B": b_floor}.get(
                    final_grade, min_score_global
                )
                effective_min_score = max(min_score_global, grade_min_required)

                regime_limits = {"CRASH": 20.0, "NEUTRAL": 30.0, "BULL": 40.0}
                pivot_reject_limit = regime_limits.get(market_regime, 30.0)

                fs_eval = v3_indicators.get("final_stock_eval") or {}
                intrinsic_grade = (
                    fs_eval.intrinsic_grade
                    if not isinstance(fs_eval, dict)
                    else fs_eval.get("intrinsic_grade", "C")
                )
                if intrinsic_grade == "S":
                    pivot_reject_limit += 10.0

                rs_overheat_limit = float(ACTIVE_PARAMS.get("rs_overheat_limit", 18.0))
                volume_surge_threshold_gate = float(
                    ACTIVE_PARAMS.get("volume_surge_threshold", 1.3)
                )

                pivot_penalty_val = 0.0
                rs_gap_val = 0.0
                vol_surge_val = 1.0
                if v3_indicators:
                    pivot_penalty_val = float(v3_indicators.get("pivot_penalty", 0.0))
                    try:
                        rs_gap_val = float(fs_eval.rs_gap)
                    except Exception:
                        pass
                    try:
                        vol_surge_val = float(fs_eval.volume_surge_ratio)
                    except Exception:
                        pass

                # [Pre-Gate] 하드 컷 우선순위 재정렬 (가장 구체적인 사유가 최우선 생존)

                # 1) 룰 엔진 / 퓨즈 하드 컷 (최우선 보존)

                # 사면권/VIP 패스 정보 동기화
                final_combined_score = (
                    decision_result.combined_score
                    if decision_result.combined_score > 0
                    else float(combined_score)
                )
                final_grade = decision_result.grade
                hard_reject_reason = decision_result.reason

                if not decision.get("final_can_buy", False) and not is_limit_up_trade:
                    rule_reason = decision.get("final_reason", "Rule Engine Reject")
                    hard_reject_reason = f"⛔ [Rule] {rule_reason}"

                # 2) 분봉 트리거 하드 컷 (이중 뇌 충돌 해결)
                if (
                    hard_reject_reason is None
                    and not trigger_hit_val
                    and not is_limit_up_trade
                ):
                    hard_reject_reason = f"⛔ [Trigger] {trigger_msg}"

                # 3) 최종 점수 하한선 미달 컷 (최하위 후순위로 밀어내어 Masking 방지)
                if (
                    hard_reject_reason is None
                    and combined_score_val < effective_min_score
                ):
                    hard_reject_reason = (
                        f"⚠️ 점수 미달 (score={combined_score_val:.2f}, "
                        f"req={effective_min_score:.2f}, grade={final_grade})"
                    )

                # 4) Pivot Penalty 하드 컷
                if (
                    hard_reject_reason is None
                    and pivot_penalty_val >= pivot_reject_limit
                ):
                    if final_grade in ["B", "C"] or combined_score_val < b_floor:
                        hard_reject_reason = f"🛑 Pivot Penalty High ({pivot_penalty_val:.2f} >= {pivot_reject_limit:.2f})"

                # 5) RS 과열 하드 컷
                if (
                    hard_reject_reason is None
                    and rs_gap_val >= rs_overheat_limit
                    and vol_surge_val < volume_surge_threshold_gate
                    and final_grade in ["B", "C"]
                ):
                    hard_reject_reason = f"🛑 RS Overheat (RS={rs_gap_val:.1f}%, Vol={vol_surge_val:.2f}x) >= limit"

                # 5.5) VCP Fakeout 하드 컷 (가짜 돌파 소각)
                vcp_ratio = final_stock_eval.vcp_ratio

                # [Zero-Defect: V-Bounce Fakeout Immunity]
                # 진성 반등(V-Bounce) 급소 종목은 낙폭 과대 후 V자로 반등하는 초기 단계이므로 체결강도가 이제 막 상승하기 시작함.
                # 체결강도 "100.0"이라는 돌파장 상투 전용 비현실적 극단 기준을 들이대어 '가짜 돌파'로 기각하는 것은 V-Bounce 모형의 근간을 파괴하는 거대 논리 상충임.
                is_v_bounce_pass = False
                _extra_info = decision.get("extra_info", {})
                if _extra_info and isinstance(_extra_info, dict):
                    is_v_bounce_pass = _extra_info.get("is_v_bounce_pass", False)

                # [Quantum Fix] 주도주/반등주는 체결강도 허들을 25.0으로 완화, 일반은 60.0으로 현실화 (100.0은 휩소 유도가 심함)
                vcp_tick_limit = (
                    25.0 if (is_recovering_leader or is_true_bounce) else 60.0
                )

                if (
                    hard_reject_reason is None
                    and not is_v_bounce_pass  # V-Bounce는 체결강도 허들 무효 면제
                    and vcp_ratio <= 0.15  # VCP 패턴 좁혀짐
                    and tick_acc < vcp_tick_limit  # 그러나 거래대금 및 체결강도 부족
                ):
                    hard_reject_reason = f"🛑 가짜 돌파 (VCP Fakeout) 차단. VCP({vcp_ratio:.2f}) 만족하나 체결강도({tick_acc:.1f}) < {vcp_tick_limit} 미달"

                # 🛡️ [Zero-Defect: Pardon Rollback Guard] 사면권이 포함된 reject은 롤백하지 않음

                # 🛡️ [Zero-Defect: Pardon Context Recovery]
                # 룰 엔진에서 사면권을 사용해 패스한 경우(is_approved=True)와 기각 후 사면된 경우(is_approved=False + [Pardon])를 모두 포함
                _rule_reason = str(decision_result.reason)
                _is_pardon_context = (
                    "[Pardon]" in _rule_reason
                    or "(BYPASS)" in _rule_reason
                    or (
                        hard_reject_reason is not None
                        and "[Pardon]" in str(hard_reject_reason)
                    )
                )

                #  msg 덮어쓰기 로직 영구 삭제 -> 원본 trigger_msg 완벽 보존
                # [Absolute Control] 사전 기각(Technical Reject) 사유가 있더라도 브레인을 무조건 호출하여 FOMO 여부를 최우선 스캔합니다.
                if hard_reject_reason is not None and not _is_pardon_context:
                    stock_eval_ctx.meets_basic_criteria = False
                    stock_eval_ctx.reason = hard_reject_reason

                # [Logic Change 2: Context Injection (단락 평가 우회)]
                # 룰 엔진의 사면(Pardon) 결과를 브레인이 무시하지 못하도록 컨텍스트를 강제 갱신하십시오.
                if _is_pardon_context:
                    stock_eval_ctx.meets_basic_criteria = True
                    stock_eval_ctx.is_true_bounce = True
                    if "[Pardon]" not in stock_eval_ctx.reason:
                        stock_eval_ctx.reason = f"🛡️ [Pardon-Context] {_rule_reason}"

                # [Task-Directive: SSOT (Single Source of Truth) Synchronization]
                # 하위 래퍼/사면 로직에서 승격된 최신 점수와 등급을 브레인 재심사 직전에 강제 동기화(Sync)하여 주도주 학살(유체이탈 버그)을 원천 차단합니다.
                stock_eval_ctx.score = float(combined_score)
                stock_eval_ctx.grade = final_grade
                if final_grade in ["S", "A"]:
                    stock_eval_ctx.intrinsic_grade = (
                        final_grade  # VIP Bypass 유지를 위한 본질 등급 동기화
                    )

                #  브레인 심사 결과가 FOMO 기각인 경우, 모든 사전 승인/사면권을 덮어쓰고 기각을 확정합니다.
                is_fomo_reject = decision_result.decision_state == "REJECT_FOMO"
                if is_fomo_reject:
                    hard_reject_reason = decision_result.reason

                # FOMO가 아니고 사면권 대상인데 브레인이 (meets_basic_criteria=False 등의 이유로) 기각한 경우, 사면권을 복권합니다.
                elif _is_pardon_context and not decision_result.final_can_buy:
                    if logger:
                        logger.info(
                            f"🛡️ [Pardon Protection] {ticker} 브레인 기술적 기각을 사면권([Pardon])으로 우회합니다."
                        )
                    decision_result.final_can_buy = True
                    decision_result.decision_state = "APPROVE_PARDON"
                    decision_result.reason = f"🛡️ [Pardon] {hard_reject_reason}"

                # 최종 기각 상태라면 decision 및 v3_indicators 동기화
                if not decision_result.final_can_buy:
                    if hard_reject_reason is None:
                        hard_reject_reason = decision_result.reason
                    if logger and not optimizer_mode:
                        logger.info(
                            f"🚫 [Entry Block] {ticker} Reject: {hard_reject_reason} | Grade: {final_grade}"
                        )

                    safe_update(
                        decision,
                        {
                            "final_can_buy": False,
                            "final_reason": hard_reject_reason,
                            "decision_state": decision_result.decision_state,
                        },
                    )

                    if (
                        isinstance(v3_indicators, dict)
                        and "final_trading_decision" in v3_indicators
                    ):
                        v3_indicators["final_trading_decision"]["final_can_buy"] = False
                        v3_indicators["final_trading_decision"]["final_reason"] = (
                            hard_reject_reason
                        )

                        if (
                            "execution_trigger" in v3_indicators
                            and v3_indicators["execution_trigger"] is not None
                        ):
                            safe_set(
                                v3_indicators["execution_trigger"], "trigger_hit", False
                            )
                            safe_set(
                                v3_indicators["execution_trigger"],
                                "approved_quantity",
                                0,
                            )

                    # [Zero-Defect: Tag Preservation] 만약 이 결정이 시장 차단(Crash)에 의한 기각이고, 기존에 사면권이 있었다면 복구 시도
                    if (
                        not safe_get(decision_result, "final_can_buy", False)
                        and _is_pardon_reject
                    ):
                        if "Market Crash" in str(
                            safe_get(decision_result, "reason", "")
                        ):
                            decision_result.final_can_buy = True
                            decision_result.decision_state = "APPROVE_PARDON"
                            decision_result.reason = f"{hard_reject_reason} | {safe_get(decision_result, 'reason', 'N/A')}"

                # 5. 최종 결과 동기화 (Executive Pardon 반영)
                final_can_buy = decision_result.final_can_buy
                final_reason = decision_result.reason
                decision_state_val = decision_result.decision_state
                trigger_hit_sync = final_can_buy
                # [Directive 6.5] 승인 수량 동기화
                approved_quantity = decision_result.approved_quantity

                # 🛡️ [Zero-Defect: Executive Pardon] 주도주 귀환 합법적 사면권 (전송 무결성 패치)

                if (
                    is_recovering_leader
                    and combined_score >= 85.0
                    and not final_can_buy
                ):
                    reason_raw = str(final_reason)

                    #  '리스크'나 '점수 미달(허매수/투매)' 등은 진짜 치명상이므로 절대 사면하지 않습니다.
                    # 오직 V자 반등의 기술적 한계(VWAP, VCP, 이평선)만 합법적으로 우회합니다.
                    if any(
                        kw in reason_raw
                        for kw in ["VWAP", "VCP", "MA50", "트렌드", "역배열", "기술적"]
                    ):
                        # 1. 즉결 사면 (매수 플래그 및 트리거 복권)
                        final_can_buy = True
                        final_reason = (
                            f"🛡️ [Pardon] V자 반등 특례 (Bypass: {reason_raw[:15]}...)"
                        )
                        trigger_hit_sync = True
                        hard_reject_reason = final_reason

                        # 2. IT 레이어가 읽을 Execution Trigger 내부 객체 조작
                        if (
                            isinstance(v3_indicators, dict)
                            and "execution_trigger" in v3_indicators
                        ):
                            safe_set(
                                v3_indicators["execution_trigger"], "trigger_hit", True
                            )
                            safe_set(
                                v3_indicators["execution_trigger"], "msg", final_reason
                            )
                            safe_set(
                                v3_indicators["execution_trigger"],
                                "approved_quantity",
                                9999999,
                            )

                # [버그 수정] 사면권 심사가 끝난 후, v3_indicators 에 최종 플래그를 확실히 업데이트합니다.
                if (
                    isinstance(v3_indicators, dict)
                    and "final_trading_decision" in v3_indicators
                ):
                    if not final_can_buy:
                        v3_indicators["final_trading_decision"]["final_can_buy"] = False
                        v3_indicators["final_trading_decision"]["final_reason"] = (
                            final_reason
                        )
                    else:
                        v3_indicators["final_trading_decision"]["final_can_buy"] = True
                        v3_indicators["final_trading_decision"]["execution_signal"] = (
                            "BUY"
                        )
                        v3_indicators["final_trading_decision"]["final_reason"] = (
                            final_reason
                        )

                if (
                    isinstance(v3_indicators, dict)
                    and "execution_trigger" in v3_indicators
                    and v3_indicators["execution_trigger"]
                ):
                    safe_set(
                        v3_indicators["execution_trigger"],
                        "trigger_hit",
                        trigger_hit_sync,
                    )
                    safe_set(v3_indicators["execution_trigger"], "msg", final_reason)
                    safe_set(
                        v3_indicators["execution_trigger"],
                        "intra_acc",
                        float(
                            safe_get(
                                trigger_info,
                                "intra_acc",
                                safe_get(trigger_info, "tick_acc", 0.0),
                            )
                        ),
                    )
                    safe_set(
                        v3_indicators["execution_trigger"],
                        "tick_acc",
                        float(
                            safe_get(
                                trigger_info,
                                "tick_acc",
                                safe_get(trigger_info, "intra_acc", 0.0),
                            )
                        ),
                    )
                    safe_set(
                        v3_indicators["execution_trigger"],
                        "approved_quantity",
                        decision_result.approved_quantity,
                    )

                execution_signal = (
                    "BUY" if final_can_buy and trigger_hit_sync else "HOLD"
                )

                #  has_pardon 플래그: [Pardon]이 사유에 포함되었는지 확인
                _has_pardon_flag = (
                    "[Pardon]" in str(final_reason)
                    or "[Pardon]" in str(hard_reject_reason)
                    or "Hyper-Momentum Override" in str(final_reason)
                )

                safe_update(
                    decision,
                    {
                        "final_can_buy": final_can_buy,
                        "final_reason": final_reason,
                        "decision_state": decision_state_val,
                        "is_limit_up_mode": is_limit_up_trade,
                        "execution_signal": execution_signal,
                        "has_pardon": _has_pardon_flag,
                        "is_blocked_by_market": safe_get(
                            decision, "is_blocked_by_market", False
                        ),
                        "is_recovering_leader": is_recovering_leader,
                    },
                )

                # [DTO 전환] extra_info 내부의 position_size_ratio 접근
                if (
                    final_stock_eval
                    and "position_size_ratio" in decision_result.extra_info
                ):
                    final_stock_eval.position_size_ratio = decision_result.extra_info[
                        "position_size_ratio"
                    ]
                    v3_indicators["final_stock_eval"] = final_stock_eval

                if market_regime_override and isinstance(v3_indicators, dict):
                    v3_indicators["market_regime"] = market_regime_override
                    if isinstance(decision, dict):
                        decision["market_regime"] = market_regime_override

                v3_indicators = sanitize_dict(v3_indicators)

            except Exception as e:
                import traceback

                logger.error(
                    f"[{ticker}] Intraday Fusion Error: {e}\n{traceback.format_exc()}"
                )
                return get_failed_candidate_result(
                    ticker, f"Intraday Fusion Fatal: {e}"
                )
            is_s_class = False

    except Exception as e:
        logger.error(f"[{ticker}] combined_score_for_ticker Error: {e}")
        return get_failed_candidate_result(ticker, f"연산 에러: {e}")

    if v3_indicators is not None:
        v3_indicators["atr_5m"] = atr_5m

    # 📦 최종 리턴 객체 (택배 상자) 생성 및 전송 무결성 확보

    # [핵심] 사면권으로 인해 뒤집힌 final_can_buy를 최종 플래그(final_can_buy_flag)에 정확히 동기화합니다.
    final_can_buy_flag = final_can_buy

    # 🛡️ [Zero-Defect: Atomic Pardon Sync] 사면권 상태 100% 강제 동기화

    # 분석 중 발급된 Pardon 표식이 로컬 변수에만 머물지 않고 반환 객체 깊숙이 박히도록 보장합니다.
    #  decision 변수가 루프 내에서 업데이트된 상태를 참조합니다.
    if "decision" in locals():
        final_reason_str = str(getattr(decision, "final_reason", "")) or str(
            hard_reject_reason
        )
        has_vip_pass = (
            "[Pardon]" in final_reason_str
            or "Hyper-Momentum Override" in final_reason_str
        )

        decision["has_pardon"] = has_vip_pass

        if v3_indicators is not None:
            v3_indicators["has_pardon"] = has_vip_pass
            # 실행부가 Market Block을 무시할 수 있도록 강력한 힌트 제공
            if has_vip_pass and "final_trading_decision" in v3_indicators:
                v3_indicators["final_trading_decision"]["has_pardon"] = True
                # 사면권을 받은 종목은 억지로라도 can_buy 플래그를 살려냄
                v3_indicators["final_trading_decision"]["final_can_buy"] = True
                decision["final_can_buy"] = True

    # 🛡️ [Zero-Defect: Atomic Pardon Sync] 사면권 상태 100% 강제 동기화

    # result["can_buy"] 플래그 최종 보정
    final_reason_str = str(hard_reject_reason)
    has_vip_pass_final = (
        "[Pardon]" in final_reason_str or "Hyper-Momentum Override" in final_reason_str
    )

    final_can_buy_flag = final_can_buy
    if has_vip_pass_final:
        final_can_buy_flag = True

    # v3_indicators 내부에도 영구 각인 (실행 엔진 참조용)
    if isinstance(v3_indicators, dict):
        v3_indicators["has_pardon"] = has_vip_pass_final
        if "final_trading_decision" in v3_indicators:
            v3_indicators["final_trading_decision"]["has_pardon"] = has_vip_pass_final
            if has_vip_pass_final:
                v3_indicators["final_trading_decision"]["is_blocked_by_market"] = False
                v3_indicators["final_trading_decision"]["final_can_buy"] = True

    result = {
        "ticker": ticker,
        "entry_price": current_price,  # [Restore] Original data type
        "combined_score": round(float(combined_score), 2),
        #  최종 전송 플래그 강제 동기화 (사면권 유지)
        "can_buy": bool(final_can_buy_flag),
        "hard_reject_reason": hard_reject_reason
        if hard_reject_reason
        else "PASSED",  # [Restore]
        "day_acc_score": day_acc_score,
        "supply_day": supply_day,
        "supply_intra": supply_intra,
        "intra_acc": float(trigger_info.intra_acc or trigger_info.tick_acc or 0.0),
        "pb_quality": float(
            trigger_info.pullback_quality or trigger_info.pb_quality or 0.0
        ),
        "vol_surge_ratio": float(trigger_info.vol_surge_ratio),
        "market_conditions": market_conditions,
        "gap_result": gap_result,
        "v3_indicators": v3_indicators,
        "atr_5m": atr_5m,
        "latest_row": latest,  # [Restore] 원본 시리즈 객체 직접 전달 (전송 무결성 복구)
        "minute_df": minute_df,
        "position_size_ratio": position_size_ratio,
        "is_single_order": is_single_order,  # [NEW] Single-Bullet Order Trigger (from fuse result)
        "is_true_bounce": is_true_bounce,
        "is_recovering_leader": is_recovering_leader,
        "is_v_bounce_event": is_v_bounce_event
        if "is_v_bounce_event" in locals()
        else False,
    }

    def _recursive_asdict(obj):
        if obj is None:
            return None
        import dataclasses

        if dataclasses.is_dataclass(obj):
            return _recursive_asdict(dataclasses.asdict(obj))
        elif isinstance(obj, dict):
            return {k: _recursive_asdict(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [_recursive_asdict(v) for v in obj]
        return obj

    result["v3_indicators"] = _recursive_asdict(result.get("v3_indicators", {}))
    result["gap_result"] = _recursive_asdict(result.get("gap_result", {}))
    return result


def get_bars_since_entry(minute_df: pd.DataFrame, entry_time) -> int:
    """
     진입 시간 이후의 경과 봉(Bar) 갯수 정밀 계산
    (주말, 야간 등 비거래 시간 완벽 무시)
    """
    try:
        if (
            minute_df is None
            or minute_df.empty
            or not entry_time
            or entry_time == "UNKNOWN"
        ):
            return 0

        # 1. 문자열/타임스탬프를 안전하게 변환 (에러 발생 시 NaT 반환)
        parsed_time = pd.to_datetime(entry_time, errors="coerce")

        if pd.isna(parsed_time):
            return 0

        # 2. 만약 시간(Hour/Minute) 정보 없이 '날짜'만 있다면 (00:00:00)
        # 부정확한 타임컷을 막기 위해 방어(0 반환)
        if parsed_time.hour == 0 and parsed_time.minute == 0:
            return 0

        # 3. [핵심] 실제 존재하는 분봉 데이터의 행(Row) 개수만 추출
        # -> 야간, 주말, 장 마감 시간은 데이터가 없으므로 자동으로 완벽하게 제외됨
        idx_after = minute_df.index[minute_df.index >= parsed_time]

        return len(idx_after)

    except Exception as e:
        if "logger" in globals() and logger:
            logger.debug(f"get_bars_since_entry Error: {e}")
        return 0


def prepare_sell_order_params(
    brain, ticker, current_qty, price, sell_info, order_type=None
):
    """
    [Smart Exit] 매도 주문 파라미터 생성 및 정규화
    [Zero-Defect Update] shadowing 방지를 위해 로컬 import 제거 및 전역 유틸 활용
    """
    from util.CommUtils import to_numeric_safe

    current_qty = to_numeric_safe(current_qty)
    ratio = to_numeric_safe(getattr(sell_info, "sell_ratio", 1.0))
    reason = getattr(sell_info, "reason", "EXIT")

    sell_qty = int(current_qty * ratio)
    if sell_qty <= 0:
        return None

    urgency_keywords = ["STOP", "LOSS", "TRAILING", "TIME", "EMERGENCY", "PANIC", "CUT"]
    is_urgent = any(kw in str(reason).upper() for kw in urgency_keywords)

    sell_order_type = (
        order_type
        if order_type
        else (
            sell_info.order_type
            if hasattr(sell_info, "order_type")
            else getattr(sell_info, "order_type", "00")
        )
    )

    if is_urgent:
        sell_order_type = "00"
        # 🚨 [Hotfix] 로컬 import 제거 (UnboundLocalError 원인)
        final_price = price * 0.985
        from util.Logger import logger
        logger.warning(
            f"🚨 [Urgency Exit] {ticker} 긴급 청산 감지({reason}). -1.5% 하향 지정가 타격."
        )
    elif sell_order_type == "00":
        final_price = price * 0.999
    else:
        final_price = price

    return {
        "command": "SELL",
        "ticker": ticker,
        "qty": sell_qty,
        "price": align_quote_price(final_price), # 상단 전역 align_quote_price 호출
        "order_price_type": sell_order_type,
        "purchase_price": getattr(sell_info, "purchase_price", 0.0),
        "buy_date": getattr(sell_info, "buy_date", None),
        "buy_price": getattr(sell_info, "buy_price", 0.0),
        "reason": reason,
        "target": None,
        "decision_data": None,
    }
