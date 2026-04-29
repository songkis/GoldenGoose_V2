# -*- coding: utf-8 -*-
"""
[trading_engine.py] 원본 TradingComm.py의 combined_score_for_ticker_v3 함수를 100% 복원.
"""

import datetime
import dataclasses
from datetime import timedelta
from typing import Any, Dict, Optional, Literal
import logging
import pandas as pd

from config.ai_settings import (
    MIN_TERM,
    TREND_MINUTE_WIN_CNT,
    S_THRESHOLD_NORMAL,
    A_THRESHOLD_NORMAL,
    B_THRESHOLD_FLOOR,
    PARAMS,
)
from core.schemas import (
    EntryDecisionDTO,
    ExecutionTriggerResult,
    MarketState,
    StockEvaluation,
    TradingDecisionParam,
    MarketContextData,
    StockEvaluationData,
)
from strategy.indicators.technical_indicators import (
    calculate_intraday_acceleration_v5_6,
)
from strategy.core.scoring_engine import (
    calculate_refined_supply_score_v6,
    final_stock_evaluation_v7_1,
    compute_intraday_scores,
    apply_dynamic_scalar,
    apply_momentum_premium,
)
from strategy.indicators.pattern_recognizer import (
    process_limit_up_from_df,
)
from strategy.core.trigger_engine import (
    check_execution_trigger_v10_2,
)
from strategy.core.TradeExitEngine import (
    calculate_hybrid_exit_levels,
    optimize_trade_levels,
)

from util.Utils import (
    safe_get,
    safe_set,
    sanitize_dict,
    safe_update,
)
from .trading_factory import get_current_optimal_params
from .trading_decision_gate import (
    get_failed_candidate_result,
    get_final_trading_decision,
)
from .market_context_provider import extract_market_context
from .stock_feature_extractor import calculate_stock_features
from .trading_score_integrator import integrate_and_grade_scores, assemble_decision_params

logger = logging.getLogger(__name__)


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
    port_list: list = None,
    global_decision_engine=None,
    master_strategy_evaluator=None,
) -> Dict[str, Any]:
    """
    [V6.0: Supply-First Acceleration Model]
    - 수급 가속도(Supply Acc)와 분봉 정밀 타점의 컨플루언스 극대화
    - 시장 상황에 따른 동적 비중 조절(Dynamic Sizing) 적용
    """
    from util.CommUtils import isOverCloseTime
    from config.ai_settings import PARAMS

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

        market_conditions, market_regime = extract_market_context(
            latest, today_market_data_df, params, market_regime_override
        )
        market_type = market_conditions.market_type
        buy_condition = market_conditions.buy_condition
        current_index_change = market_conditions.current_index_change
        market_data = market_conditions.market_data
        index_return_5d = market_conditions.index_return_5d

        market_score = market_conditions.market_score
        market_df = market_data
        is_bull_market = buy_condition
        supply_day = float(latest.get("체결강도", 100.0))

        features = calculate_stock_features(
            ticker, data, minute_df, tp, params, current_price
        )
        is_true_bounce = features["is_true_bounce"]
        tick_acc = features["tick_acc"]
        supply_intra_val = features["supply_intra_val"]
        limit_up_data = features["limit_up_data"]
        day_open = features["day_open"]
        day_high = features["day_high"]
        day_low = features["day_low"]
        atr_pct = features["atr_pct"]

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
            curr_price_base = current_price if current_price > 0 else 1.0

            m_term = params.get("min_term", 3) if params else 3
            tick_acc = calculate_intraday_acceleration_v5_6(minute_df, m_term)

            combined_score, grade = integrate_and_grade_scores(
                ticker, daily_score, intraday_score, tick_acc, supply_intra,
                atr_pct, final_stock_eval.rs_gap, market_conditions, params, intraday_v2
            )

            if (
                -0.5 < intraday_v2.get("vwap_dist", 0.0) < 3.0
                and intraday_v2.get("ofi_val", 0) > 0
            ):
                is_single_order = True

            # [Zero-Defect] V-Bounce 상태 동적 계산 (결과 매핑용)
            is_v_bounce_event = tick_acc >= 20.0 and supply_intra >= 60.0

            # [Zero-Defect Fix 3-A] fuse가 판단한 시장 연동 등급 캐싱
            fuse_grade = grade
            grade = fuse_grade  # [Sync] Sync local grade with fused grade immediately

            # [S급 필터링 플래그] 리스트업을 위한 판단
            is_s_class = supply_intra >= 80

            try:
                atr_5m = atr_pct
                paramDic, market_ctx, stock_eval_ctx = assemble_decision_params(
                    ticker, combined_score, grade, market_regime, market_conditions,
                    trigger_info, is_true_bounce, latest, data, final_stock_eval,
                    current_price, tp, latest_minute, intraday_v2
                )
                
                # local variable recovery for things needed later
                ai_prob_val = stock_eval_ctx.ai_surge_probability
                limit_up_data = final_stock_eval.limit_up_data
                is_limit_up_trade = limit_up_data.get("is_limit_up_entry", False)
                trigger_hit_val = trigger_info.trigger_hit
                trigger_msg = trigger_info.msg

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
