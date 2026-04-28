import logging
import datetime
from enum import Enum
from dataclasses import dataclass, field
from typing import Dict, Any
import numpy as np
from core.schemas import EntryDecisionDTO, MarketContextData, StockEvaluationData
from config.ai_settings import (
    CONSECUTIVE_LOSS_PLATINUM_BOOST,
    OVERHEAT_HARD_LIMIT_S,
    OVERHEAT_HARD_LIMIT_A,
    OVERHEAT_HARD_LIMIT_B,
)
from config.ai_settings import PARAMS

logger = logging.getLogger("GoldenGoose.Brain")


class DecisionState(Enum):
    REJECT_PANIC = "🚫 Panic-Shield: 지수 급락 감지. 리스크 관리 차단."
    REJECT_OVERHEAT = "🛡️ Market-Sync: 과열장 차단. 주도주만 진입 허용."
    REJECT_CRITERIA = "⚠️ 기본 매수 조건 미충족."
    APPROVE_PLATINUM_PASS = "👑 Platinum Pass: 패닉장 역행 주도주 강력 매수."
    APPROVE_GOLD_PASS = "🚀 Gold Pass: 과열장 무시 및 강력 매수."
    APPROVE_NORMAL = "✅ 정상 진입 승인."
    APPROVE_NORMAL_LIMIT_UP = "🔥 상한가 패턴 진입 승인."
    APPROVE_HYPER_MOMENTUM = "🛡️ Hyper-Momentum VIP: 수급 폭발로 인한 상시 승인."


class TradePersona(Enum):
    BULL_AGGRESSIVE = "BULL_AGGRESSIVE"  # 공격적 (비중 1.5배, 허들 완화)
    BEAR_DEFENSIVE = "BEAR_DEFENSIVE"  # 방어적 (비중 0.5배, 허들 강화)
    NEUTRAL_BALANCED = "NEUTRAL_BALANCED"  # 균형형


class TradeDecisionEngine:
    """
    중앙 의사결정 브레인 (Central Decision Engine)
    다중 페르소나 및 실시간 성과 피드백 루프 적용
    """

    def __init__(
        self, panic_threshold: float = -1.20, overheat_threshold: float = 10.0
    ):
        self.panic_threshold = panic_threshold
        self.overheat_threshold = overheat_threshold

        self.consecutive_losses = 0
        self.wfo_params = {}  # [TLVI Harness] Ensure wfo_params exists before first evaluation

        s_thresh = getattr(PARAMS, "s_threshold_normal", 85.0)

        self.base_platinum_score = s_thresh + 45.0  # 기존 115점 수준 유지 (70+45)
        self.adaptive_boost = 0.0

    def reset_platinum_score(self):

        s_thresh = PARAMS.s_threshold_normal
        self.base_platinum_score = s_thresh + 45.0
        self.adaptive_boost = 0.0

    def update_performance_feedback(self, is_win: bool):
        """성과 피드백 루프: 매도 결과 발생 시 호출되어 엔진의 성향을 조절합니다."""
        if is_win:
            self.reset_platinum_score()
        else:
            self.consecutive_losses += 1
            if self.consecutive_losses >= 3:
                self.adaptive_boost = CONSECUTIVE_LOSS_PLATINUM_BOOST

                s_thresh = PARAMS.s_threshold_normal
                self.base_platinum_score = (
                    s_thresh + 45.0 + self.adaptive_boost
                )  # 3연패 시 Platinum 기준을 대폭 상향하여 보수적 진입 유도
                logger.warning(
                    f"🧠 [Feedback Loop] 3연속 손실 감지! Platinum Pass 컷오프 상향 ({self.base_platinum_score}점)"
                )

    def update_params(self, params: dict):
        """
        WFO 전략 파라미터 수신 및 동기화
        """
        self.wfo_params.update(params)

        # 파라미터를 시스템 최상위 PARAMS에 동기화하여 전역 파이프라인 정합성 유지
        for k, v in params.items():
            if hasattr(PARAMS, k):
                setattr(PARAMS, k, v)
            else:
                PARAMS[k] = v

        logger.info(
            f"🔄 [Decision Brain] Strategy Parameters Updated and Synced: {params}"
        )

    def _determine_persona(self, market_regime: str, drop_rate: float) -> TradePersona:
        """지수 급락률을 반영한 다이나믹 페르소나 결정 (Adaptive Filter)"""
        if market_regime in ["BEAR", "CRASH"] or drop_rate <= -1.5:
            # 시장 하락 추세이거나 당일 지수가 -1.5% 이상 크게 빠질 때 -> 극강의 방어 모드
            return TradePersona.BEAR_DEFENSIVE
        elif market_regime == "BULL" and drop_rate > -0.5:
            # 상승장이고 당일 지수도 멀쩡할 때 -> 공격 모드
            return TradePersona.BULL_AGGRESSIVE

        return TradePersona.NEUTRAL_BALANCED

    def evaluate_buy_decision(
        self,
        stock_code: str,
        market_ctx: MarketContextData,
        stock_eval: StockEvaluationData,
        account_balance: float,
        **kwargs  # [WFO Adapter] Support for dynamic optimizer parameters
    ) -> EntryDecisionDTO:
        # [WFO Adapter] Unpacking dynamic parameters into local state if needed
        if kwargs:
            for k, v in kwargs.items():
                if hasattr(PARAMS, k):
                    setattr(PARAMS, k, v)

        # [TLVI Harness] Define VIP Bypass condition first (Zero-Defect standard)
        is_true_bounce = stock_eval.is_true_bounce
        is_recovering_leader = stock_eval.is_recovering_leader
        is_vip_grade = stock_eval.intrinsic_grade in ["S", "A"]
        is_vip_bypass = is_true_bounce or is_recovering_leader or is_vip_grade
        tick_acc = stock_eval.tick_acc

        # [Logic Change: True Alpha Guard & Zero-Edge Annihilation (Fixed)]
        # VIP 면제권(is_vip_bypass)이 있더라도 스코어가 0.0 이하인 종목은 원칙적으로 차단하되,
        # 진성 반등(True-Bounce)이거나 주도주로서 강력한 미시 수급(Tick_Acc >= 20.0)이 유입되는 경우 "알파 디커플링"을 인정하여 기각을 면제합니다.
        if stock_eval.score <= 0.0:
            if not is_vip_bypass or tick_acc < 20.0:
                return self._build_result(
                    stock_code,
                    False,
                    DecisionState.REJECT_CRITERIA,
                    0.0,
                    custom_reason=f"🚫 실제 엣지 결여 (Score {stock_eval.score:.1f}). 강력수급 부재(Tick:{tick_acc:.1f}). VIP 특권 무효화.",
                )

        # [Phase 0] 기대수익 산출 및 가용 자본 리밸런싱 (Hoisted for Edge-First validation)
        win_rate = stock_eval.expected_win_rate
        atr_val = stock_eval.atr_val if stock_eval.atr_val > 0 else 2.0

        # [Level 0] Absolute Microstructure Kill-Switch
        # 등급이나 사면권과 무관하게 매수 체결 실종(Tick_Acc < 10) 시 즉시 기각
        if tick_acc < 10.0:
            return self._build_result(
                stock_code,
                False,
                DecisionState.REJECT_CRITERIA,
                0.0,
                custom_reason=f"🚫 [Microstructure Kill-Switch] 매수 체결 실종 (Tick_Acc: {tick_acc:.1f} < 10.0). VIP 특권 영구 박탈 및 즉시 기각.",
            )

        k_mul = self.calculate_dynamic_k_multiplier(
            market_ctx.market_regime, market_ctx.current_index_change
        )

        # [Ghost Trade Prevention] 체결강도 15.0 미만 시 주도주 및 진성반등 사면권 원천 무효화
        if tick_acc < 15.0:
            is_true_bounce = False
            is_vip_bypass = False
            stock_eval.is_true_bounce = False

        # [Directive: 비대칭 켈리 증폭 로직 호출]
        base_kelly = calculate_dynamic_kelly(win_rate, atr_val, tick_acc=tick_acc)
        # Fix: 명시적으로 as-is 비중을 2배 상향 적용하고 상한선을 1.0(100%)으로 확장
        base_kelly = float(np.clip(base_kelly * k_mul * 2.0, 0.0, 1.0))

        # [Logic Change: Post-Scalar Kelly Floor]
        # 스칼라 곱셈 이후 최종 비중이 5% 미만으로 찌그러지면 자본 기아 방어를 위해 즉각 기각
        if base_kelly < 0.05:
            return self._build_result(
                stock_code,
                False,
                DecisionState.REJECT_CRITERIA,
                0.0,
                custom_reason=f"🚫 자본 기아 방어 (최종 비중 {base_kelly * 100:.1f}% < 5%)",
            )

        # [Logic Change: Edge-First Priority Override]
        # 기대수익 0 이하(Negative Edge) 시 어떠한 VIP 조건도 평가하지 않고 즉시 기각
        if base_kelly <= 0.0:
            return self._build_result(
                stock_code,
                False,
                DecisionState.REJECT_CRITERIA,
                0.0,
                custom_reason="🚫 기대수익 0 이하 (Negative Edge). 진입 원천 차단.",
            )

        # FOMO Kill-Switch (최상단 배치)
        # 과열권 종목은 하위 로직 평가 없이 즉시 사형 선고
        rs_gap = stock_eval.rs_gap
        bb_dist = stock_eval.bb_dist
        surge_rate = stock_eval.surge_rate

        # [Logic Change 2: FOMO-Reject Trap 소각]
        # Surge (당일 상승률)가 5.0% 미만이거나 is_vip_bypass == True인 종목은 FOMO 기각 대상에서 완전히 제외(Bypass)
        is_fomo_reject = rs_gap >= 20.0 or bb_dist >= 1.0 or surge_rate >= 15.0
        if (surge_rate < 5.0) or is_vip_bypass:
            is_fomo_reject = False

        if is_fomo_reject:
            return EntryDecisionDTO(
                stock_code=stock_code,
                final_can_buy=False,
                decision_state="REJECT_FOMO",
                combined_score=stock_eval.score,
                grade="F",
                reason=f"⛔ [FOMO-Reject] VIP 특권 박탈. 수치적 과열 제한 초과 (RS_Gap:{rs_gap:.1f}, BB_Dist:{bb_dist:.2f}, Surge:{surge_rate:.1f}%)",
                approved_quantity=0,
                extra_info={
                    "hard_reject": True,
                    "is_blocked_by_market": market_ctx.is_panic,
                },
            )

        # 다중 페르소나 결정
        persona = self._determine_persona(market_ctx.regime, market_ctx.drop_rate)

        intrinsic_grade = stock_eval.intrinsic_grade

        # WFO Sync 및 K-Multiplier 연동
        s_thresh_base = self.wfo_params.get(
            "s_threshold_normal", PARAMS.s_threshold_normal
        )
        tick_min_base = self.wfo_params.get(
            "tick_acc_min", PARAMS.get("tick_acc_min", 60.0)
        )

        k_mul = self.calculate_dynamic_k_multiplier(
            market_ctx.regime, market_ctx.index_change
        )
        s_thresh = s_thresh_base * k_mul
        tick_min = tick_min_base * k_mul

        if k_mul != 1.0:
            logger.debug(
                f"⚙️ [K-Multiplier] Throttling Thresholds by {k_mul:.2f}x (S:{s_thresh:.1f}, Tick:{tick_min:.1f})"
            )

        # S급 턱걸이가 아니라, 컷오프보다 10점 이상 높은 확실한 초강력 주도주에게만 Gold Pass 부여
        is_gold_pass = stock_eval.grade == "S" and stock_eval.score >= (s_thresh + 10.0)

        # Platinum은 Gold Pass 중에서도 최상위 종목 (동적 스케일링)
        is_platinum_pass = is_gold_pass and stock_eval.score >= max(
            self.base_platinum_score, s_thresh + 15.0
        )

        # [Level 0] System Kill-Switch: Panic Shield

        # drop_rate 연동
        # is_vip_bypass is already defined at the entry point of evaluate_buy_decision

        # 기술적 성숙도 혹은 바닥권 돌파 혹은 VIP Bypass 중 하나라도 충족 시 승인
        is_technically_ready = (
            stock_eval.meets_basic_criteria or stock_eval.is_breakout or is_vip_bypass
        )

        if market_ctx.is_panic:
            # 주도주 또는 주도주 귀환은 폭락장에서도 기술적 준비 완료 시 'Platinum Pass' 허용
            is_recovering_leader = stock_eval.is_recovering_leader
            is_high_conviction_leader = (
                intrinsic_grade in ["S", "A"]
            ) or is_recovering_leader

            if is_high_conviction_leader and is_technically_ready:
                # 폭락장 주도주 전용 비중 페르소나 및 동적 할당
                persona = "BEAR_CONVICTION"

                # 폭락장: Kelly 기준 절반 이하 강제 축소 (단, 주도주 귀환은 최소 0.5배 보장)
                min_weight = 0.5 if is_recovering_leader else 0.1
                weight_multiplier = max(min_weight, base_kelly * 0.5)

                return self._build_result(
                    stock_code,
                    True,
                    DecisionState.APPROVE_PLATINUM_PASS,
                    weight_multiplier,
                    custom_reason=f"👑 {stock_eval.intrinsic_grade}급 주도주 기술적 돌파로 인한 예외 승인",
                )

            # 그 외 종목 또는 기술적 미준수 주도주는 차단
            return self._build_result(stock_code, False, DecisionState.REJECT_PANIC, 0)

        # [Level 1 & 2] Market Filter & Alpha Privilege

        # Super-Trend Synergy: S급 4.5%, A급 3.5%까지 하드 리밋 완화
        overheat_hard_limit = (
            OVERHEAT_HARD_LIMIT_S
            if stock_eval.grade == "S"
            else (
                OVERHEAT_HARD_LIMIT_A
                if stock_eval.grade == "A"
                else OVERHEAT_HARD_LIMIT_B
            )
        )

        is_overheat_base = (
            market_ctx.market_regime == "BULL"
            and market_ctx.current_index_change >= self.overheat_threshold
        )

        # [Super Trend Relaxation] 지수가 +3.0% 이상 초강세일 때는 하드 리밋(Gap) 기준을 3.0%p 상향하여 주도주 진입 허용 (공포 해소)
        dynamic_overheat_limit = overheat_hard_limit
        if market_ctx.index_change >= 3.0:
            dynamic_overheat_limit += 3.0

        is_overheat = (
            is_overheat_base
            or market_ctx.current_index_change >= dynamic_overheat_limit
        )

        # Silver Pass (BULL Scout Mode): 완만한 강세장에서 A급 주도주 진입 허용
        is_silver_pass = (
            market_ctx.regime == "BULL"
            and 0.0 <= market_ctx.index_change < 3.0
            and (intrinsic_grade in ["S", "A"])
            and stock_eval.meets_basic_criteria
        )

        if is_overheat:
            if (is_gold_pass or is_silver_pass) and stock_eval.meets_basic_criteria:
                # Silver Pass인 경우 정찰병(0.3x) 비중 강제 적용
                weight_multiplier = np.clip(base_kelly * 1.5, 0.1, 1.5)
                if is_silver_pass:
                    weight_multiplier = np.clip(base_kelly * 0.3, 0.05, 0.5)
                    logger.info(
                        "🥈 [Silver-Pass] 완만한 강세장 A급 주도주 정찰병 투입 (x0.30)"
                    )

                return self._build_result(
                    stock_code,
                    True,
                    DecisionState.APPROVE_GOLD_PASS
                    if is_gold_pass
                    else DecisionState.APPROVE_NORMAL,
                    weight_multiplier,
                )
            else:
                return self._build_result(
                    stock_code, False, DecisionState.REJECT_OVERHEAT, 0
                )

        # [Level 3] Normal Execution Check

        # Dead-Cat Guard (하드 컷오프)
        # [Adaptive Tolerance] S/A급 주도주 및 A급 주도주는 수급 하락(Dead-Cat)에 대한 내성(Tolerance)를 각각 5.0, 3.0으로 상향
        tolerance = (
            5.0
            if stock_eval.intrinsic_grade == "S"
            else (3.0 if stock_eval.intrinsic_grade == "A" else 1.5)
        )

        # Early-Bird True Bounce Spatial Constraint
        # 반등이 주요 지지선(Open, VWAP, MA20) 반경 1.5% 이내에서 발생했는지 엄격히 검증
        if stock_eval.is_true_bounce:
            dip_p = stock_eval.recent_low
            supports = stock_eval.support_levels

            if dip_p > 0 and supports:
                # 지지선들과의 최소 이격도 계산
                min_dist = min(
                    [abs(dip_p / s - 1) for s in supports.values() if s > 0] or [999.0]
                )
                if min_dist > 0.015:
                    stock_eval.is_true_bounce = False
                    logger.info(
                        f"🚫 [Spatial-Guard] {stock_code} 허공(이격 {min_dist * 100:.2f}%)에서의 반등 기각. 주요 지지선 근접 필요."
                    )
                else:
                    # 오후장 VWAP 트랩 회피 (Time-Weighted Pullback Guard)
                    now = datetime.datetime.now().time()
                    if now >= datetime.time(13, 30):
                        stock_eval.is_true_bounce = False
                        logger.info(
                            f"🚫 [Time-Guard] {stock_code} 13시 30분 이후 오후장 가짜 반등(VWAP 설거지 트랩) 승인 거부."
                        )
                        # [Directive 2] OFI 교집합 탐지 보강 (진성 눌림목 필터링)
                        bid_sum = stock_eval.bid_sum
                        ask_sum = stock_eval.ask_sum
                        if ask_sum > 0 and (bid_sum / ask_sum) < 1.2:
                            stock_eval.is_true_bounce = False
                            logger.info(
                                f"🚫 [Liquidity-Guard] {stock_code} 진성 반등 거부. 호가 잔량비 미달 ({bid_sum / ask_sum:.2f} < 1.2)"
                            )
                        else:
                            logger.info(
                                f"✨ [Spatial-Approved] {stock_code} 지지선 근처(이격 {min_dist * 100:.2f}%) 진성 반등 확정 (OFI:{bid_sum / ask_sum:.2f})."
                            )

        # [Logic Change: Alpha Decoupling Fix]
        # 잘못된 current_volume 매핑을 삭제하고 진짜 수급 점수(supply_intra)를 직접 참조
        real_supply_score = stock_eval.supply_intra

        # 압도적인 수급(80 이상)이 확인되면 등급(intrinsic_grade)과 무관하게 지수 디커플링 인정
        is_alpha_decoupled = real_supply_score >= 80.0

        actual_tick_min = (
            (tick_min_base - tolerance)
            if is_alpha_decoupled
            else (tick_min - tolerance)
        )

        # [Logic Change: Total Power Guard]
        # 통합 수급(Supply + Tick) 기반의 엄격한 판정으로 전면 교체
        total_power = stock_eval.supply_intra + stock_eval.tick_acc
        is_true_bounce_verified = stock_eval.is_true_bounce

        if (
            total_power < (actual_tick_min * 1.5)
            and stock_eval.tick_acc < 20.0
            and not is_true_bounce_verified
        ):
            return self._build_result(
                stock_code,
                False,
                DecisionState.REJECT_CRITERIA,
                0,
                custom_reason=f"🚫 [Dead-Cat Guard] 통합 수급 미달 (Total:{total_power:.1f}, Acc:{stock_eval.tick_acc:.1f})",
            )

        if (
            stock_eval.intrinsic_grade in ["S", "A"]
            and (tick_min - tolerance) <= stock_eval.tick_acc < tick_min
        ):
            logger.info(
                f"🛡️ [Hysteresis Guard] {stock_code} 주도주 프리미엄으로 미세 수급 오차(TickAcc:{stock_eval.tick_acc:.1f}) 면제."
            )
            # 승인 유지 (Hard Reject 회피)

        if is_technically_ready:
            is_aggressive = stock_eval.intrinsic_grade in ["S", "A"]

            if market_ctx.is_panic and is_aggressive:
                persona = "BEAR_CONVICTION"

            weight_multiplier = base_kelly  # [TLVI] Default initialization

            if base_kelly <= 0.0:
                pass  # Already checked at top

            # S/A급 주도주 + 강력 수급 발생 시 기술적 지표 기각 무효화
            supply_intra = stock_eval.supply_intra
            tick_acc = stock_eval.tick_acc

            if (
                intrinsic_grade in ["S", "A"]
                and (supply_intra >= 50.0 and tick_acc >= 50.0)
                and not is_fomo_reject
            ):
                weight_multiplier = (
                    np.clip(base_kelly * 1.5, 0.1, 1.5)
                    if intrinsic_grade == "S"
                    else np.clip(base_kelly * 1.2, 0.1, 1.2)
                )
                return self._build_result(
                    stock_code,
                    True,
                    DecisionState.APPROVE_HYPER_MOMENTUM,
                    weight_multiplier,
                    custom_reason=f"🛡️ [Hyper-Momentum] {intrinsic_grade}급 VIP 사면 승인 (Supply:{supply_intra:.1f}, Acc:{tick_acc:.1f})",
                )

            if stock_eval.is_limit_up_trade:
                return self._build_result(
                    stock_code,
                    True,
                    DecisionState.APPROVE_NORMAL_LIMIT_UP,
                    weight_multiplier,
                )
            return self._build_result(
                stock_code, True, DecisionState.APPROVE_NORMAL, weight_multiplier
            )

        rejection_msg = (
            stock_eval.reason
            if stock_eval.reason
            else DecisionState.REJECT_CRITERIA.value
        )
        return self._build_result(
            stock_code,
            False,
            DecisionState.REJECT_CRITERIA,
            0.0,
            custom_reason=rejection_msg,
        )

    def calculate_dynamic_k_multiplier(
        self, market_regime: str, index_change: float
    ) -> float:
        """[Dynamic K-value] 국면 적응형 K-허들 배수 산출"""
        k_multiplier = 1.0
        if market_regime == "CRASH" or index_change <= -1.5:
            k_multiplier = 1.0 + min(0.25, abs(index_change) * 0.1)
        elif market_regime == "BULL" or index_change >= 1.0:
            k_multiplier = 1.0 - min(0.15, index_change * 0.1)
        else:
            k_multiplier = 1.0 - (index_change * 0.05)
        return float(k_multiplier)

    def _build_result(
        self,
        stock_code: str,
        is_approved: bool,
        state: DecisionState,
        weight_multiplier: float,
        custom_reason: str = None,
    ) -> EntryDecisionDTO:
        """[DTO 전환] Dict 기반 데드 코드 소각 및 EntryDecisionDTO 반환"""
        final_reason = custom_reason if custom_reason else state.value
        log_msg = f"[{stock_code}] {final_reason} | 비중배율: {weight_multiplier:.2f}"
        if is_approved:
            logger.info(log_msg)
        else:
            logger.debug(log_msg)

        # 등급 판정 (S, A, B, C, F)
        if not is_approved:
            grade = "F"
        else:
            # 기본적으로 APPROVE 시의 등급을 추론 (추후 TradingComm에서 정밀 판정)
            if "PLATINUM" in state.name or "HYPER" in state.name:
                grade = "S"
            elif "GOLD" in state.name:
                grade = "A"
            else:
                grade = "B"

        return EntryDecisionDTO(
            stock_code=stock_code,
            final_can_buy=is_approved,
            decision_state=state.name,
            combined_score=0.0,  # [Sync] TradingComm에서 최종 점수와 병합됨
            grade=grade,
            reason=final_reason,
            approved_quantity=0,  # [Sync] TradingComm에서 자본금 기반 계산됨
            extra_info={
                "has_pardon": "Pardon" in str(final_reason)
                or "Hyper-Momentum" in str(final_reason),
                "position_size_ratio": weight_multiplier,
                "enforce_stop_loss": True,
                "time_based_exit_limit": PARAMS.get("time_based_exit_limit", "14:45"),
            },
        )


def calculate_dynamic_kelly(
    win_rate: float,
    atr_val: float,
    tick_acc: float = 0.0,  # [Added] 수급 가속도 파라미터 추가
    tp_multiplier: float = 2.0,
    sl_multiplier: float = 1.0,
) -> float:
    # [Logic Change: Kelly Zero-Bound 보호]
    # 에너지 붕괴 시 자본 투입 완전 차단
    if tick_acc < 10.0:
        return 0.0
    """
    [V7.0 Asymmetric Momentum Kelly]
    기존의 단순 변동성 페널티를 삭제하고, 수급 가속도(Tick_Acc)에 의한 비대칭 증폭 엔진 적용.
    """
    epsilon = 1e-8
    sl_dist = atr_val * sl_multiplier
    tp_dist = atr_val * tp_multiplier
    b = tp_dist / (sl_dist + epsilon)

    p = np.clip(win_rate, 0.0, 1.0)
    q = 1.0 - p

    expected_value = (p * b) - q
    if expected_value <= 0:
        return 0.0

    kelly_f = expected_value / (b + epsilon)
    half_kelly = kelly_f / 2.0

    # [Logic Change: Linear Momentum Dampening]
    # 가짜 반등 시 E(x) 노출도를 선형적으로 억제하기 위해 하한선을 0.4로 하향 조정
    energy_multiplier = float(max(0.4, np.log1p(tick_acc / 15.0)))

    # ATR이 비정상적으로 높을 때만 제한적 페널티 적용 (기본 4~8% 구간은 1.0에 수렴)
    vol_guard = 1.0 if atr_val < 8.0 else (8.0 / atr_val)

    adjusted_kelly = half_kelly * energy_multiplier * vol_guard

    # [Logic Change] Absolute Kelly Floor 적용 (5% 미만 소각)
    # Fix: 외부에서 2배 곱셈이 적용될 때 상단이 잘리지 않도록 상한선을 1.0으로 확장
    return 0.0 if adjusted_kelly < 0.05 else float(min(adjusted_kelly, 1.0))
