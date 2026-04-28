import logging
from typing import Dict, Any
import math
from strategy.rules.base_rule import BaseRule, RuleResult
from util.Utils import safe_get

logger = logging.getLogger(__name__)


class UnifiedTrendGateRule(BaseRule):
    """
    [V8.0 Architecture] Unified Trend & Daily Gate Rule
    - Consolidation of minute-level and daily alignment filters.
    - Pre-calculates MA20 and MA60 once to reduce computational overhead.
    - Comprehensive Bypass Logic: V-Bounce, True-Bounce, and Recovering Leader.
    """

    def evaluate(
        self, stock_data: Dict[str, Any], market_data: Dict[str, Any]
    ) -> RuleResult:
        """
        [O(1) Zero-Latency Trend Gate]
        - Replacing lagging MA20/MA60 with Microstructure OFI & VWAP
        - Reject: cur_price < vwap AND ofi_ratio < -0.1
        - Exempt if is_vcp_stabilized is True
        """
        vcp_ratio = float(
            safe_get(
                stock_data, "vcp_ratio", safe_get(stock_data, "VCP_Ratio", 1.0)
            )
        )
        regime = getattr(market_data, "market_regime", "NEUTRAL")
        is_vcp_stabilized = (regime in ["RANGE", "NEUTRAL"]) and (vcp_ratio <= 0.20)

        if is_vcp_stabilized:
            return RuleResult(
                is_passed=True,
                score_modifier=0.0,
                reason=f"🧪 [Gate-Stabilized] {regime} 국면 VCP 응축({vcp_ratio:.2f})으로 추세 필터 면제",
            )

        # price_dist_vwap is (price/vwap - 1)*100
        vwap_dist = float(safe_get(stock_data, "vwap_dist", 0.0))
        ofi_ratio = float(safe_get(stock_data, "ofi_ratio", 0.0))

        # cur_price < vwap (vwap_dist < 0) and ofi_ratio < -0.1 -> Strong Sell Pressure
        if vwap_dist < 0.0 and ofi_ratio < -0.1:
            return RuleResult(
                is_passed=False,
                score_modifier=-100.0,
                reason=f"⛔ [Gate Fatal] 하락 추세 가속 (VWAP 이격:{vwap_dist:.2f}%, OFI:{ofi_ratio:.2f})",
            )

        return RuleResult(
            is_passed=True,
            score_modifier=10.0,
            reason="✅ [Trend Gate] 추세 유지 (Micro)",
        )


class SmartBreakoutTriggerRule(BaseRule):
    """
    [Phase 12] 스마트 기관 타점 트리거 (VWAP + Adaptive ORB)
    - 시장 상태에 따라 ORB(초기 고가 돌파) 시간을 동적으로 조절
    - 주가가 세력의 당일 평균가(VWAP) 아래에 있으면 절대 매수 금지 (지하실 방어)
    """

    def evaluate(
        self, stock_data: Dict[str, Any], market_data: Dict[str, Any]
    ) -> RuleResult:
        regime = getattr(market_data, "market_regime", "NEUTRAL")
        #  Strong Bounce 추출
        is_strong_bounce = getattr(market_data, "momentum_state", "") == "STRONG_BOUNCE"

        minute_df = stock_data.get("minute_df")
        if minute_df is None or minute_df.empty:
            return RuleResult(
                is_passed=False, score_modifier=0.0, reason="[Trigger] 분봉 데이터 없음"
            )

        # 1. 당일 데이터만 분리
        today_date = minute_df.index[-1].date()
        today_bars = minute_df[minute_df.index.date == today_date]

        if len(today_bars) < 3:
            return RuleResult(
                is_passed=False, score_modifier=0.0, reason="[Trigger] 장 초반 3분 관망"
            )

        current_price = today_bars["종가"].iloc[-1]
        open_price = today_bars["시가"].iloc[0]

        # 0. 🛡️ [Opening Range High Override for VIPs]
        # [Antigravity Fix] S/A 등급 및 True-Bounce 종목은 시초가 돌파 여부와 무관하게 수급만으로 통과 가능하도록 amnesty 부여
        grade = safe_get(
            stock_data, "intrinsic_grade", safe_get(stock_data, "grade", "C")
        )
        is_true_bounce = safe_get(stock_data, "is_true_bounce", False)
        is_vip_amnesty = (grade in ["S", "A"]) or is_true_bounce

        if is_vip_amnesty:
            # [Caution] VIP라도 VWAP-DUMP와 같은 치명적 리스크는 하단에서 별도로 체크함
            pass

        # [ Safety] 음봉 휩쏘 방어
        # [Directive 2] 룰 엔진의 월권 박탈 (Yin/Yang Sync Override)
        # 채점 엔진(scoring_engine)에서 진성 반등으로 판명되었거나(score >= 45), 에너지가 통과된 경우 음봉 차단 무시
        intraday_score = float(safe_get(stock_data, "intraday_score", 0.0))
        energy_status = str(safe_get(stock_data, "energy_status", ""))
        vol_surge = float(safe_get(stock_data, "vol_surge_ratio", 1.0))

        # [Directive 3] ATR 기반 음봉 휩쏘 방어 (Antigravity Core)
        # 시가 대비 1%와 같은 하드코딩 대신 변동성(ATR) 기준으로 진짜 추세 붕괴와 노이즈 분리
        atr_val = float(safe_get(stock_data, "atr_val", open_price * 0.02))
        limit_drop = open_price - (atr_val * 0.5)

        if is_strong_bounce and current_price < limit_drop:
            if intraday_score >= 45.0 or "PASSED" in energy_status:
                # [Bypass] 진성 반등 가점을 받은 경우 또는 에너지 통과 시 음봉 차단 면제
                pass
            elif vol_surge < 1.0:
                # [Bypass] 거래량 없는 음봉은 하이브리드 타점으로 승인
                pass
            else:
                return RuleResult(
                    is_passed=False,
                    score_modifier=-60.0,
                    reason=f"⛔ 폭등장 완화 적용 불가: 변동성 허들({limit_drop:.0f}) 하회 (ATR 휩쏘 방어)",
                )

        # 2.  VWAP (당일 거래량 가중평균가) 3단계 Fallback 계산
        latest_bar = today_bars.iloc[-1]
        market_vwap = float(getattr(market_data, "market_vwap", 0.0))
        vwap = float(latest_bar.get("VWAP", 0.0))

        # 1순위: 직접 계산 (누적거래대금/누적거래량)
        if vwap == 0.0:
            vol = float(latest_bar.get("누적거래량", today_bars["거래량"].sum()))
            amount = float(latest_bar.get("누적거래대금", 0.0))
            if amount == 0.0:
                amount = (today_bars["종가"] * today_bars["거래량"]).sum()

            if vol > 0 and amount > 0:
                vwap_calc = amount / vol
                if 0.1 <= (vwap_calc / current_price) <= 10.0:
                    vwap = vwap_calc

        # 2순위: Typical Price (고가+저가+현재가)/3
        if vwap == 0.0 or vwap != vwap:  # NaN check
            high_val = float(latest_bar.get("고가", current_price))
            low_val = float(latest_bar.get("저가", current_price))
            vwap = (high_val + low_val + current_price) / 3.0

        #  Recovery Bypass 적용 (VWAP 필터링 전 수행)
        is_recovering_leader = safe_get(stock_data, "is_recovering_leader", False)

        # [Standardized Extraction]
        tick_acc = float(
            safe_get(stock_data, "intra_acc", safe_get(stock_data, "tick_acc", 0.0))
        )

        if (
            is_recovering_leader
            and (current_price >= vwap * 0.98)
            and (tick_acc >= 20.0)
        ):
            return RuleResult(
                is_passed=True,
                score_modifier=0.0,
                reason=f"🔥 [Recovery Bypass] VWAP({vwap:.0f}) 하회 극복. 강력한 수급 승인",
            )

        params = getattr(market_data, "system_params", {})
        vwap_margin_pct = params.get("vwap_margin_pct", 0.0)
        adjusted_vwap = vwap * (1 + vwap_margin_pct / 100.0)

        if current_price < adjusted_vwap:
            # [Bull Market Relaxation] 강세장/반등장에서는 VWAP 하회 시 하드 차단 대신 큰 감점 후 관찰
            if regime == "BULL" or is_strong_bounce:
                return RuleResult(
                    is_passed=True,
                    score_modifier=-40.0,
                    reason=f"⚠️ [Trigger-VWAP] Adjusted VWAP {adjusted_vwap:.0f} 하회 중이나 강세장/반등장 예외 승인",
                )

            # [Elite Leader Bypass] S/A급 주도주 & 강력한 수급(tick_acc >= 25.0) 시 VWAP 3% 하회까지는 타점 승인 (Neutral 국면 구제)
            if (
                (safe_get(stock_data, "intrinsic_grade") in ["S", "A"])
                and tick_acc >= 25.0
                and (current_price >= adjusted_vwap * 0.97)
            ):
                return RuleResult(
                    is_passed=True,
                    score_modifier=-15.0,
                    reason=f"🚀 [Trigger-VWAP Bypass] {safe_get(stock_data, 'intrinsic_grade')}급 주도주 세력선(VWAP) 인접 및 수급 폭발 승인",
                )

            return RuleResult(
                is_passed=False,
                score_modifier=-35.0,
                reason=f"📉 [Trigger] 세력 이탈 (Adjusted VWAP {adjusted_vwap:.0f} 하회)",
            )

        # 3. Market Regime Adaptive ORB (시장 상태 연동형 시간 조절)
        if regime == "BULL" or is_strong_bounce:
            orb_minutes = 5  # 불장: 5분 고가만 뚫어도 즉시 공격적 진입
        elif regime == "BEAR" or regime == "CRASH":
            orb_minutes = 30  # 하락장: 30분 동안 확실히 지지받는지 깐깐하게 확인
        else:
            orb_minutes = 15  # 평장: 15분 (정석)

        # 지정된 ORB 시간(예: 30분)이 경과하지 않았다면 섣부른 가짜 돌파를 차단합니다.
        if len(today_bars) < orb_minutes:
            if regime == "BULL" or is_strong_bounce:
                # 불장일 때만 예외적으로 선제 진입(감점 동반) 허용
                return RuleResult(
                    is_passed=True,
                    score_modifier=-25.0,
                    reason=f"⏳ [Trigger-Wait] ORB({orb_minutes}m) 형성 전이나 강세장/반등장 선제 진입",
                )
            return RuleResult(
                is_passed=False,
                score_modifier=0.0,
                reason=f"⏳ [Trigger] ORB({orb_minutes}m) 형성 대기중 (현재 {len(today_bars)}분)",
            )

        orb_bars = today_bars.iloc[:orb_minutes]
        orb_high = orb_bars["고가"].max()

        # 4. 돌파 및 수급(체결강도) 확인
        if current_price >= orb_high:
            power = (
                today_bars["체결강도"].iloc[-1]
                if "체결강도" in today_bars.columns
                else 105.0
            )
            if power >= 100.0:
                return RuleResult(
                    is_passed=True,
                    score_modifier=30.0,
                    reason=f"🔥 [Trigger] ORB({orb_minutes}m) 돌파({orb_high:.0f}) & VWAP 지지",
                )
            else:
                # [: High-Conviction Amnesties]
                # S/A급 주도주이거나 진성 반등 종목은 체결강도가 100에 살짝 못 미치더라도 진입 승인 (97.8 등의 케이스 구제)
                grade = stock_data.get(
                    "intrinsic_grade", stock_data.get("final_grade", "C")
                )
                is_vip = (grade in ["S", "A"]) or stock_data.get(
                    "is_true_bounce", False
                )
                # [Refinement] S/A 리더의 경우 체결 가속도(tick_acc)가 높으면 체결강도 허들을 70까지 대폭 완화 (기회비용 최소화)
                vip_power_floor = (
                    (70.0 if tick_acc >= 25.0 else 85.0)
                    if regime == "NEUTRAL"
                    else 100.0
                )

                if is_vip and power >= vip_power_floor:
                    return RuleResult(
                        is_passed=True,
                        score_modifier=10.0,
                        reason=f"🚀 [Trigger VIP] {grade}급/반등주 체결강도 허들 완화 입성 (Power:{power:.1f} >= {vip_power_floor})",
                    )

                # [Bull Market Relaxation] 강세장에서는 돌파 후 체결강도 약세라도 일단 승인 (후행 수급 기대)
                if regime == "BULL" or is_strong_bounce:
                    return RuleResult(
                        is_passed=True,
                        score_modifier=-15.0,
                        reason=f"⏳ [Trigger-Power] 강세장/반등장 내 돌파 성공하나 체결강도({power:.1f}) 약세",
                    )
                return RuleResult(
                    is_passed=False,
                    score_modifier=-10.0,
                    reason=f"⏳ [Trigger] 돌파했으나 체결강도 미달 ({power:.1f})",
                )

        # [Bull Market Relaxation] 강세장에서는 ORB 돌파 전이라도 기대감으로 진입 허용 (Penalty 부여)
        if regime == "BULL" or is_strong_bounce:
            return RuleResult(
                is_passed=True,
                score_modifier=-25.0,
                reason=f"⏳ [Trigger-Wait] ORB({orb_minutes}m) 저항권이나 강세장/반등장 선제 진입",
            )

        # [: Executive Pardon for Leaders]
        # 주도주 귀환(Recovering Leader)인 경우, ORB 돌파 전이라도 수급(Tick Acc)이 뒷받침되면 선제 진입 허용
        if is_recovering_leader and tick_acc >= 30.0:
            return RuleResult(
                is_passed=True,
                score_modifier=-10.0,
                reason=f"🚀 [Recovery Trigger] ORB({orb_minutes}m) 저항({orb_high:.0f}) 대기 중이나 강력한 수급(Acc:{tick_acc:.1f})으로 선제 승인",
            )

        return RuleResult(
            is_passed=False,
            score_modifier=0.0,
            reason=f"⏳ [Trigger] ORB({orb_minutes}m) 저항({orb_high:.0f}) 대기중",
        )


class AIDeepLearningEnsembleRule(BaseRule):
    """
    [Phase 7 AI Ensemble] 딥러닝 예측 모델의 내일/단기 상승 확률을 룰 엔진에 결합
    """

    def __init__(self, **kwargs):
        self.ai_bonus_threshold = 0.70
        self.ai_penalty_threshold = 0.35

    def evaluate(
        self, stock_data: Dict[str, Any], market_data: Dict[str, Any]
    ) -> RuleResult:
        #  AI 확률 키 매핑 보강 (Casing Defense)
        ai_prob = safe_get(
            stock_data,
            "ai_surge_probability",
            safe_get(stock_data, "ai_prob", safe_get(stock_data, "AI_PROB", 0.5)),
        )

        if ai_prob >= 0.85:
            return RuleResult(
                is_passed=True,
                score_modifier=0.0,
                reason=f"🤖 [AI-Super] 초강력 상승 예측 (Prob: {ai_prob * 100:.1f}%)",
            )
        elif ai_prob >= self.ai_bonus_threshold:
            return RuleResult(
                is_passed=True,
                score_modifier=0.0,
                reason=f"🤖 [AI-Good] 긍정적 상승 예측 (Prob: {ai_prob * 100:.1f}%)",
            )
        elif ai_prob <= self.ai_penalty_threshold:
            return RuleResult(
                is_passed=True,
                score_modifier=0.0,
                reason=f"⚠️ [AI-Warn] 하락 위험군 분류 (Prob: {ai_prob * 100:.1f}%)",
            )

        return RuleResult(
            is_passed=True, score_modifier=0.0, reason="[AI-Neutral] 예측 확률 중립구간"
        )


class AbsoluteMarketRegimeRule(BaseRule):
    """
    [Market Filter] 지수 폭락 시 전면 금지 (주도주 예외 적용)
    - CRASH(폭락장)에서는 무조건 방어
    - BEAR(하락장)에서는 일반 종목은 차단하되, S/A급 주도주는 예외적으로 사냥 허용
    """

    def evaluate(
        self, stock_data: Dict[str, Any], market_data: Dict[str, Any]
    ) -> RuleResult:
        #  소속 시장에 맞는 지수 데이터 추출 (KOSPI/KOSDAQ 분리)
        target_market = safe_get(stock_data, "market_type", "KOSPI")

        # [Sync] market_analysis에서 제공하는 세그먼트 국면 정보를 최우선 참조
        from strategy.indicators.market_analysis import get_segmented_market_regime

        segmented_regimes = get_segmented_market_regime()
        market_trend = segmented_regimes.get(
            target_market, getattr(market_data, "market_regime", "NEUTRAL")
        )

        # [Fallback] 만약 market_data에 구체적인 수치가 있다면 덮어쓰기
        drop_rate = getattr(market_data, "current_index_change", 0.0)
        momentum_state = getattr(market_data, "momentum_state", "NORMAL")

        if isinstance(market_data, list):
            for m in market_data:
                if getattr(m, "market_type", "") == target_market:
                    market_trend = getattr(m, "market_regime", market_trend)
                    drop_rate = getattr(m, "current_index_change", drop_rate)
                    momentum_state = getattr(m, "momentum_state", momentum_state)
                    break

        is_strong_bounce = momentum_state == "STRONG_BOUNCE"

        #  주도주 판별 (혈관에서 파라미터 꺼내기)
        params = getattr(market_data, "system_params", {})
        crash_threshold = safe_get(params, "crash_index_threshold", -2.0)

        grade = safe_get(
            stock_data, "intrinsic_grade", safe_get(stock_data, "final_grade", "C")
        )
        is_leader = grade in ["S", "A"]

        # 1. 찐 폭락장(CRASH 또는 임계치 이하 급락): 자비 없이 전면 매수 중단
        # [Exception] 단, Index가 +0.5% 이상 반등 중이면 CRASH 하드 필터 해제 (사이드 이펙트 방지)
        if (
            market_trend == "CRASH" or drop_rate <= crash_threshold
        ) and not is_strong_bounce:
            #  주도주 & 정배열 또는 주도주 귀환(Recovering Leader)인 경우에만 CRASH 장세에서 예외 승인
            # [V12.1 Refactoring] 후행성 MA 대신 선행 마이크로스트럭처 팩터로 CRASH 방어
            vwap_dist = float(safe_get(stock_data, "vwap_dist", 0.0))
            ofi_ratio = float(safe_get(stock_data, "ofi_ratio", 0.0))
            is_trend_intact = (vwap_dist > -2.0) and (ofi_ratio > 0.0)
            is_recovering_leader = safe_get(stock_data, "is_recovering_leader", False)

            # [Platinum Pass] S급 주도주 중 수급(combined_score)이 임계치를 넘으면 정배열 전이라도 CRASH 예외 승인
            combined_score = safe_get(stock_data, "combined_score", 0.0)
            s_thresh_cutoff = safe_get(params, "s_threshold_normal", 85.0)
            is_platinum = (grade == "S") and (combined_score >= s_thresh_cutoff)

            if (is_leader and is_trend_intact) or is_recovering_leader or is_platinum:
                penalty = -10.0 if (is_recovering_leader or is_platinum) else -20.0
                reason_tag = (
                    "플래티넘"
                    if is_platinum
                    else ("주도주귀환" if is_recovering_leader else "주도주&정배열")
                )
                return RuleResult(
                    is_passed=True,
                    score_modifier=penalty,
                    reason=f"🛡️ [Crash-Pass] {reason_tag} {target_market} 예외 승인 (Score:{combined_score:.1f})",
                )

            reason_str = f"⛔ [Market Crash] {target_market} 지수 임계치({drop_rate}%) 돌파 진입 차단"
            return RuleResult(
                is_passed=False,
                score_modifier=0.0,
                reason=reason_str,
            )

        # [Strong Bounce] 국면 최우선 순위 (CRASH/BEAR 판정보다 우선)
        if is_strong_bounce:
            return RuleResult(
                is_passed=True,
                score_modifier=15.0,
                reason=f"🚀 [Market Bounce] {target_market} 반등 국면 적극 진입 승인 (+{drop_rate}%)",
            )

        # 2. 일반 하락장(BEAR): 주도주는 프리패스, 잡주는 차단 (설정 시 완화)
        allow_bear = safe_get(params, "allow_bear_market_entry", False)
        bear_penalty = safe_get(params, "bear_market_weight_penalty", 0.5)

        if market_trend == "BEAR":
            if is_leader:
                return RuleResult(
                    is_passed=True,
                    score_modifier=0.0,
                    reason=f"🛡️ [Market] BEAR 하락장이나 {grade}급 주도주 예외 승인",
                )
            elif allow_bear:
                return RuleResult(
                    is_passed=True,
                    score_modifier=-20.0,
                    reason="🛡️ [Market] BEAR 하락장 일반 종목 조건부 승인 (수량 패널티)",
                    extra_info={"qty_multiplier": bear_penalty},
                )
            else:
                return RuleResult(
                    is_passed=False,
                    score_modifier=-50.0,
                    reason="⛔ [Market BEAR] 하락장 일반 종목 진입 차단",
                )

        return RuleResult(
            is_passed=True, score_modifier=0.0, reason="[Market] 시장 환경 양호"
        )


class IntradayTimeFilterRule(BaseRule):
    """[K-Market 특화] 휩쏘 시간대 매수 금지"""

    def evaluate(
        self, stock_data: Dict[str, Any], market_data: Dict[str, Any]
    ) -> RuleResult:
        current_time_str = safe_get(stock_data, "current_time", "")

        if not current_time_str:
            return RuleResult(
                is_passed=True,
                score_modifier=0.0,
                reason="[Time] 시간 정보 없음 (패스)",
            )

        try:
            if " " in current_time_str:
                time_part = current_time_str.split(" ")[1]
            else:
                time_part = current_time_str

            hour = int(time_part.split(":")[0])
            minute = int(time_part.split(":")[1])
            current_hm = hour * 100 + minute

            # [Adaptive] S급 주도주는 시간 필터를 무시하고 진입 허용 (Phase 12 Pass)
            is_leader = (
                safe_get(stock_data, "final_grade") == "S"
                or safe_get(stock_data, "daily_score", 0) >= 90
            )

            params = getattr(market_data, "system_params", {})
            l_start = safe_get(params, "lunch_time_start", 1030)
            l_end = safe_get(params, "lunch_time_end", 1400)
            m_close_buffer = safe_get(params, "market_close_buffer", 1520)

            if l_start <= current_hm < l_end:
                if is_leader:
                    return RuleResult(
                        is_passed=True,
                        score_modifier=-5.0,
                        reason=f"⏳ [Time Filter] 횡보 시간대({l_start}~{l_end})이나 주도주 예외 승인",
                    )
                return RuleResult(
                    is_passed=False,
                    score_modifier=-35.0,
                    reason=f"⏳ [Time Filter] 마의 횡보 시간대 ({l_start}~{l_end}) 매수 금지",
                )
        except Exception:
            pass

        return RuleResult(
            is_passed=True, score_modifier=0.0, reason="[Time] 유효한 매매 시간대"
        )


class InstitutionalVolumeSurgeRule(BaseRule):
    """[기관급 수급 폭발] 가짜 돌파 차단 (옵티마이저 파라미터 동적 연동)"""

    def __init__(self, **kwargs):
        self.min_volume_ratio = safe_get(kwargs, "volume_surge_threshold", 1.3304)
        self.min_power = safe_get(kwargs, "min_power", 105.0)

    """
     1분봉 단일 캔들의 노이즈를 제거하고, 
    최근 3~5분 누적 또는 일일 가속도를 활용한 진짜 수급 폭발 검증
    """

    def evaluate(self, stock_data: dict, market_data: dict) -> RuleResult:
        minute_df = safe_get(stock_data, "minute_df")
        if minute_df is None or minute_df.empty or len(minute_df) < 20:
            return RuleResult(
                is_passed=True, score_modifier=0.0, reason="[Volume] 데이터 부족 Bypass"
            )

        # [Adaptive Execution] Momentum Volume Bypass (후행성 거래량 지표 무력화)

        # execution_trigger (trigger_info)는 combined_score_for_ticker_v3에서 계산됨

        #  intra_acc와 tick_acc를 통합 추출하여 누락 방어
        tick_acc = float(
            safe_get(stock_data, "intra_acc", safe_get(stock_data, "tick_acc", 0.0))
        )
        supply_intra = float(safe_get(stock_data, "supply_intra", 0.0))

        # [Quant Fix: Leader Volume Amnesty]
        # S/A급 주도주이거나 강력한 수급 감지 시 거래량 허들 면제 요건 대폭 완화
        leader_amnesty = (
            (safe_get(stock_data, "grade") in ["S", "A"])
            and tick_acc >= 15.0
            and supply_intra >= 60.0
        )
        if (tick_acc >= 20.0 and supply_intra >= 80.0) or leader_amnesty:
            amnesty_reason = "Leader Amnesty" if leader_amnesty else "Momentum Bypass"
            return RuleResult(
                is_passed=True,
                score_modifier=0.0,
                reason=f"🔥 [{amnesty_reason}] 수급 폭발로 거래량 허들 면제 (Acc: {tick_acc:.1f}, Sup: {supply_intra:.1f})",
            )

        params = getattr(market_data, "system_params", {})
        surge_threshold = safe_get(params, "volume_surge_threshold", 1.3304)

        #  폭등장 컨텍스트 수신 및 거래량 필터 대폭 완화
        is_strong_bounce = getattr(market_data, "momentum_state", "") == "STRONG_BOUNCE"
        regime = getattr(market_data, "market_regime", "NEUTRAL")

        if is_strong_bounce or regime == "BULL":
            surge_threshold *= 0.8  # 불장 20% 할인

        if safe_get(stock_data, "grade") in ["S", "A"] or safe_get(
            stock_data, "is_true_bounce", False
        ):
            #  진성 반등(True-Bounce) 종목은 수급이 이제 막 생성되는 단계이므로 허들을 50% 추가 할인
            # [Refinement] S/A 리더 역시 30% 추가 할인하여 기관급 수급 유입 초기 포착 강화
            surge_threshold *= (
                0.5 if safe_get(stock_data, "is_true_bounce", False) else 0.7
            )

        if is_strong_bounce:
            surge_threshold = max(
                0.3, surge_threshold - 0.3
            )  # 바운스 시 최소 허들 0.3 하향 보정 (기존 0.4)

        # [O(1) Optimization] 후행성 롤링 연산 대신 이미 산출된 vol_surge_ratio 재활용
        intra_ratio = float(safe_get(stock_data, "vol_surge_ratio", 1.0))

        # 1차 구명조끼: 당일 예상 거래량 가속도(Volume Multiplier)가 1.5배 이상이면 (폭등장 1.0 이상)
        day_vol_multiplier = safe_get(stock_data, "volume_multiplier", 1.0)
        day_vol_required = 1.0 if is_strong_bounce else 1.5

        if intra_ratio >= surge_threshold or day_vol_multiplier >= day_vol_required:
            return RuleResult(
                is_passed=True,
                score_modifier=5.0,
                reason=f"✅ [Volume] 수급 검증 통과 (Intra:{intra_ratio:.2f}x, DayAcc:{day_vol_multiplier:.2f}x)",
            )

        reason_msg = f"⛔ [Volume] 수급 검증 실패: 거래량 미달 (Intra:{intra_ratio:.2f} < {surge_threshold:.2f}, DayAcc:{day_vol_multiplier:.2f})"
        if is_strong_bounce:
            reason_msg = "💡 [Bounce Vol] " + reason_msg

        return RuleResult(
            is_passed=False,
            score_modifier=-100.0,
            reason=reason_msg,
        )


class SlippageDefenseRule(BaseRule):
    """[호가창 슬리피지 방어] 비정상적 고점 추격 매수 금지"""

    def __init__(self, **kwargs):
        self.max_chase_pct = 1.5

    def evaluate(
        self, stock_data: Dict[str, Any], market_data: Dict[str, Any]
    ) -> RuleResult:
        current_price = safe_get(
            stock_data, "current_price", safe_get(stock_data, "종가", 0)
        )
        open_price = safe_get(
            stock_data,
            "시가",
            safe_get(stock_data, "오늘시가", safe_get(stock_data, "open_price", 0)),
        )

        # [Robust Extraction]
        if open_price == 0:
            minute_df = safe_get(stock_data, "minute_df")
            if minute_df is not None and not minute_df.empty:
                try:
                    today_date = minute_df.index[-1].date()
                    today_df = minute_df[minute_df.index.date == today_date]
                    if not today_df.empty:
                        open_price = float(today_df["시가"].iloc[0])
                except Exception:
                    pass

        if open_price == 0:
            open_price = current_price

        if open_price <= 0:
            return RuleResult(
                is_passed=True,
                score_modifier=0.0,
                reason="🛡️ [Slippage] 시가 정보 없음 (통과)",
            )

        # 당일 시가 대비 급등률 체크
        surge_rate = ((current_price - open_price) / open_price) * 100

        params = getattr(market_data, "system_params", {}) or {}
        max_surge = params.get("max_surge_pct", 15.0)

        # [Antigravity Fix] S/A 등급 및 True-Bounce 종목은 당일 변동성 허들을 25.0%까지 대폭 완화
        # 주도주가 바닥권에서 강하게 치솟을 때 조기 기각되는 것을 방어 (EV 극대화)
        is_true_bounce = safe_get(stock_data, "is_true_bounce", False)
        # 등급 추출 Fallback 강화
        grade = safe_get(
            stock_data,
            "intrinsic_grade",
            safe_get(stock_data, "final_grade", safe_get(stock_data, "grade", "C")),
        )

        if grade in ["S", "A"] or is_true_bounce:
            max_surge = max(max_surge, 25.0)
            # [Dynamic Extension] 강세장/폭등장 내 S급 주도주는 최대 29%까지 추가 숨통 부여 (상한가 직전까지 수용)
            regime = getattr(market_data, "market_regime", "NEUTRAL")
            is_strong_bounce = (
                getattr(market_data, "momentum_state", "") == "STRONG_BOUNCE"
            )
            if (regime == "BULL" or is_strong_bounce) and grade == "S":
                max_surge = 29.0

        if surge_rate >= max_surge:
            return RuleResult(
                is_passed=False,
                score_modifier=-35.0,
                reason=f"⛔ [Slippage] 당일 이미 폭등({surge_rate:.1f}% >= {max_surge}%) -> 리스크 초과",
            )

        return RuleResult(
            is_passed=True,
            score_modifier=0.0,
            reason="🛡️ [Slippage] 호가 안정권, 추격매수 아님",
        )


class RSGapRule(BaseRule):
    """[RS Gap Filter] 시장 주도주 여부 확인"""

    def __init__(self, **kwargs):
        self.min_rs_gap = 1.5

    def evaluate(
        self, stock_data: Dict[str, Any], market_data: Dict[str, Any]
    ) -> RuleResult:
        # [Case-Mismatch Fix] rs_gap vs RS_Gap
        rs_gap = safe_get(stock_data, "rs_gap", safe_get(stock_data, "RS_Gap", 0.0))
        market_regime = getattr(market_data, "market_regime", "NEUTRAL")

        current_index_change = getattr(market_data, "current_index_change", 0.0)
        adr_ratio = getattr(market_data, "adr_ratio", 1.0)
        is_strong_bounce = current_index_change >= 0.9 and adr_ratio >= 1.2

        # [Directive 1] Dynamic RS 보정 (Antigravity Core)
        # 강세장 평준화 오류 방지를 위해 지수 상승분(1.5배 가중)에 비례하여 하한선 자동 하향
        effective_min_rs = self.min_rs_gap
        if market_regime == "BULL" or is_strong_bounce:
            effective_min_rs -= max(0, 1.5 * current_index_change)

        # [ Scoring] 0건 방지를 위해 하드 필터 -> 감점 방식으로 변경
        if rs_gap < effective_min_rs:
            # -10% 이하의 심각한 역배열이 아니면 일단 통과시키되 큰 감점 부여 (성능 검증용)
            if rs_gap >= -10.0:
                return RuleResult(
                    is_passed=True,
                    score_modifier=-30.0,
                    reason=f"📉 Weak RS (Gap: {rs_gap}) - Penalty Applied",
                )

            return RuleResult(
                is_passed=False,
                score_modifier=-50.0,
                reason=f"⛔ Critical Weak RS (Gap: {rs_gap} < -10.0)",
            )
        return RuleResult(is_passed=True, score_modifier=10.0, reason="📈 RS Gap 통과")




class VCPPatternRule(BaseRule):
    """
     VCP Pattern Rule (Volatility Contraction)
    - WFO 최적화 파라미터(max_vcp_ratio) 동기화
    - 소프트 룰 전환 + 절대 방어선(Hard Cap) 도입
    """

    def __init__(self, **kwargs):
        # 최적화 파라미터 주입을 위해 kwargs 수용 (TradingComm.py 호환성)
        self.params = kwargs

    def evaluate(
        self, stock_data: Dict[str, Any], market_data: Dict[str, Any]
    ) -> RuleResult:
        ticker = safe_get(stock_data, "ticker", "Unknown")
        vcp_ratio = safe_get(
            stock_data, "vcp_ratio", safe_get(stock_data, "VCP_Ratio", 1.0)
        )
        regime = getattr(market_data, "market_regime", "NEUTRAL")
        # [Quant Fix] intrinsic_grade 참조로 순환논리(Circular Logic) 차단
        grade = safe_get(
            stock_data,
            "intrinsic_grade",
            safe_get(stock_data, "final_grade", safe_get(stock_data, "grade", "B")),
        )

        # 1. [Sync] WFO 최적화 파라미터 로드
        params = getattr(market_data, "system_params", {}) or {}
        # [VCP Rigidity Fix]: WFO 파라미터 max_vcp_ratio를 로드한 직후 동적 스케일링
        max_vcp_ratio = safe_get(params, "max_vcp_ratio", 0.08)
        is_recovering_leader = safe_get(stock_data, "is_recovering_leader", False)
        is_true_bounce = safe_get(stock_data, "is_true_bounce", False)

        # [Quant Fix] ADR 케이스 센서티브 맵핑 방어 및 Silent Default 보정
        raw_adr = float(getattr(market_data, "adr_ratio", 1.0) * 100.0)
        normalized_adr = raw_adr * 100.0 if raw_adr < 10.0 else raw_adr

        # [Quant Fix: Regime-Aware VCP Scaling]
        # 시장이 STRONG_BOUNCE 또는 BULL 국면일 경우, 변동성 확대를 수용하기 위해 Cap을 1.5배 확장
        momentum_state = getattr(market_data, "momentum_state", "NORMAL")
        adaptive_multiplier = (
            1.5 if momentum_state == "STRONG_BOUNCE" or regime == "BULL" else 1.0
        )

        # [Fix: Proxy ADR] ADR 데이터가 불충분할 경우 지수 수익률로 대리 ADR 산출
        current_index_change = float(getattr(market_data, "current_index_change", 0.0))
        proxy_adr = (
            100.0 + (current_index_change * 40.0) if current_index_change > 0 else 100.0
        )
        effective_adr = max(normalized_adr, proxy_adr)

        dynamic_vcp_cap = min(
            0.35 * adaptive_multiplier,
            max_vcp_ratio
            * adaptive_multiplier
            * math.exp(0.5 * max(0, effective_adr - 120.0) / 100.0),
        )

        # [Target 1: VCP Dynamic Expansion]
        # 시장 레지임이 NEUTRAL 이상(NEUTRAL, BULL)이고, 해당 종목이 True Bounce 판정을 받은 경우 Cap을 1.5배 추가 확장
        if (regime in ["NEUTRAL", "BULL"]) and is_true_bounce:
            dynamic_vcp_cap *= 1.5
            if logger:
                logger.info(
                    f"✨ [Dynamic-VCP] {safe_get(stock_data, 'ticker')} True-Bounce detected. Regime: {regime}. VCP Cap 1.5x Relaxation 적용."
                )

        # [Quant Fix: V-Bounce Floor Hardening] S/A급 주도주가 Strong Bounce 국면일 경우 최소 25%의 변동성을 무조건 허용 (Neutral 국면 20%)
        if grade in ["S", "A"] or is_recovering_leader:
            vcp_floor = (
                0.25
                if (momentum_state == "STRONG_BOUNCE" or regime == "BULL")
                else 0.20
            )
            dynamic_vcp_cap = max(dynamic_vcp_cap, vcp_floor)

        # 3. [Quant Fix] 등급별 Hard Cap 차등화 -> 이제 dynamic_vcp_cap 기반으로 Base 확장
        is_bounce = momentum_state == "STRONG_BOUNCE"
        tick_acc_val = float(
            safe_get(
                stock_data,
                "intra_acc",
                safe_get(
                    stock_data, "tick_acc", safe_get(stock_data, "Tick_Acc", 0.0)
                ),
            )
        )
        if grade == "F" and tick_acc_val < 60.0:
            grade_cap_multiplier = 1.0
        elif is_recovering_leader or is_true_bounce or grade in ["S", "A"]:
            # [Zero-Defect Fix] 기존 15.0배 팽창 소각. VCP 본질(수렴) 훼손 방지를 위해 최대 3.0배 하드 캡 적용.
            # 정상적인 주도주 돌파 매수 기능은 유지하되, 고점의 극단적 변동성은 필터링함.
            grade_cap_multiplier = 3.0 
            if logger:
                logger.info(
                    f"✨ [VCP-Stabilized] {ticker} {grade}등급 VIP/Bounce 포착. Cap 3.0x 제한적 확장 적용."
                )
        else:
            grade_cap_multiplier = {"B": 2.0}.get(grade, 2.5)

        # [Elite Amnesty] S/A급 주도주의 경우 계산된 absolute_max_vcp에 20%의 추가 숨통(Grace Margin)을 부여하여 정밀한 기각 방지
        grace_multiplier = 1.2 if grade in ["S", "A"] else 1.0
        absolute_max_vcp = dynamic_vcp_cap * grade_cap_multiplier * grace_multiplier

        # [Antigravity Directive: Dynamic Cap Scaling]
        # 급등장(BULL)이거나 진성 반등인 경우, VCP 팽창을 에너지가 모이는 과정으로 해석하여 50% 추가 가산
        if regime == "BULL" or is_true_bounce:
            absolute_max_vcp *= 1.5
            if logger:
                logger.debug(
                    f"✨ [Dynamic-VCP] {ticker} True-Bounce detected. Regime: {regime}. VCP Cap 1.5x Relaxation 적용."
                )
        pb_quality = float(
            safe_get(stock_data, "pullback_quality", 0.0)
        )

        # 4. [Task-Directive] VCP 붕괴 면제 (Bypass) 로직
        is_vcp_collapsed = vcp_ratio > absolute_max_vcp

        if is_vcp_collapsed:
            # [Standardized Extraction]
            supply_intra = float(safe_get(stock_data, "supply_intra", 0.0))
            intra_acc = float(safe_get(stock_data, "intra_acc", safe_get(stock_data, "tick_acc", 0.0)))

            # [Bypass 1] 상승장(BULL) 프리미엄 (Supply 50+)
            if regime == "BULL" and supply_intra >= 50.0:
                is_vcp_collapsed = False
                logger.info(
                    f"🚀 [VCP Bypass] {ticker} 상승장 프리미엄(Supply:{supply_intra:.1f}) 면제."
                )
            # [Bypass 2] 강력한 개별 주도주 돌파 (Supply 65+, Acc 25+)
            elif supply_intra >= 65.0 and intra_acc >= 25.0:
                is_vcp_collapsed = False
                logger.info(
                    f"🚀 [VCP Bypass] {ticker} 주도주 돌파(Supply:{supply_intra:.1f}, Acc:{intra_acc:.1f}) 면제."
                )
            # [Bypass 3] S/A급 주도주의 깊은 눌림목
            elif grade in ["S", "A"] and pb_quality >= 0.5:
                is_vcp_collapsed = False
                logger.info(
                    f"🚀 [VCP Bypass] {ticker} | {grade}급 주도주 눌림목(pb_q:{pb_quality:.2f}) 사면."
                )

        if is_vcp_collapsed:
            return RuleResult(
                is_passed=False,
                score_modifier=-50.0,
                reason=f"⛔ [Fatal] VCP 팽창 (Ratio: {vcp_ratio:.2f} > Cap: {absolute_max_vcp:.2f})",
            )

        # 4. 소프트 룰 (감점 구조)
        if vcp_ratio > dynamic_vcp_cap:
            # 최대 30점 감점 캡 적용 (사용자 지침)
            penalty = min((vcp_ratio - dynamic_vcp_cap) * 50, 30.0)
            return RuleResult(
                is_passed=True,
                score_modifier=-penalty,
                reason=f"⚠️ [VCP Warning] 변동성 높음 ({vcp_ratio:.2f} > {dynamic_vcp_cap:.2f} < {absolute_max_vcp:.2f})",
            )

        return RuleResult(
            is_passed=True,
            score_modifier=10.0,
            reason=f"💎 [VCP] 에너지 응축 구역 (Ratio: {vcp_ratio:.2f})",
        )


class DynamicExecutionThresholdRule(BaseRule):
    """[Dynamic Execution Threshold] 등급별 실시간 수급 및 탄력성 검증"""

    def __init__(self, **kwargs):
        self.min_supply_b = 75.0
        self.min_power = 90.0

    def evaluate(
        self, stock_data: Dict[str, Any], market_data: Dict[str, Any]
    ) -> RuleResult:
        #  등급 키 매핑 보강
        grade = safe_get(stock_data, "final_grade", safe_get(stock_data, "grade", "B"))
        supply_intra = safe_get(
            stock_data, "supply_intra", safe_get(stock_data, "Supply_Intra", 0)
        )
        minute_df = safe_get(stock_data, "minute_df")

        if minute_df is None or minute_df.empty:
            return RuleResult(
                is_passed=False, score_modifier=0, reason="분봉 데이터 없음"
            )

        current_price = minute_df["종가"].iloc[-1]
        last_3min_high = minute_df["고가"].tail(3).max()
        is_breaking_out = current_price >= last_3min_high

        can_buy = False
        reason = ""

        # 💉 [혈관에서 실시간 파라미터 추출]
        params = getattr(market_data, "system_params", {}) or {}
        min_supply = safe_get(params, "min_supply_b_grade", self.min_supply_b)
        min_power = safe_get(params, "min_power", self.min_power)

        # [Standardized Extraction]
        supply_intra = float(
            safe_get(
                stock_data, "supply_intra", safe_get(stock_data, "supply_intra", 0.0)
            )
        )
        intra_acc = float(
            safe_get(stock_data, "intra_acc", safe_get(stock_data, "tick_acc", 0.0))
        )
        total_power = supply_intra + intra_acc

        # [Refinement] 전략 모드에 따른 가중치 이원화 및 시장 국면 반영 (Regime Scaling)
        regime = getattr(market_data, "market_regime", "NEUTRAL")
        if regime == "BULL":
            min_supply_b = 60.0
        elif regime == "BEAR":
            min_supply_b = 85.0
        else:
            min_supply_b = 75.0

        if grade == "S":
            # S등급은 수급 50점만 넘어도 통과
            if supply_intra >= 50:
                can_buy = True
                reason = "S급 주도주 수급 확인"
        elif grade == "A":
            if supply_intra >= 75 or (supply_intra >= 65 and is_breaking_out):
                can_buy = True
                reason = "A급 눌림목 수급/돌파 확인"
        elif grade == "B":
            if supply_intra >= min_supply_b:
                can_buy = True
                reason = f"⚡ [Dynamic] B급 {regime} 수급 통과 (Req:{min_supply_b})"
        else:
            reason = f"등급 미달 ({grade})"

        if not can_buy:
            return RuleResult(is_passed=False, score_modifier=-60.0, reason=reason)

        return RuleResult(is_passed=True, score_modifier=10.0, reason=reason)


class ExtremeMeanReversionRule(BaseRule):
    """
    [V8.0 Architecture] Rubber-Band Floor Turnaround Rule
    - Removed lagging RSI dependency.
    - Implementing OFI (Order Flow Imbalance) + VWAP Distance + Volume Surge.
    - Targets extreme sell-side exhaustion (Climax) for high-probability counter-trend entries.
    """

    def evaluate(
        self, stock_data: Dict[str, Any], market_data: Dict[str, Any]
    ) -> RuleResult:
        regime = getattr(market_data, "market_regime", "NEUTRAL")

        # BULL 국면에서는 기회비용 방지를 위해 역추세 진입 기각
        if regime == "BULL":
            return RuleResult(
                is_passed=True, score_modifier=0.0, reason="[Mean-Rev] 강세장 패스"
            )

        # 1. 🛡️ [TLVI] Microstructure Data Extraction
        minute_df = stock_data.get("minute_df")
        ofi_val = float(safe_get(stock_data, "ofi_val", 0.0))
        vwap_dist = float(safe_get(stock_data, "vwap_dist", 0.0))
        vol_surge = float(safe_get(stock_data, "vol_surge_ratio", 1.0))

        # 2. 🎯 [Rule Matrix] Sell-side Climax Capture
        # [V8.2 Fix] Scale-Invariant OFI Normalization (Math Rigor)
        # 하드코딩된 -5000 대신 최근 5분 평균 거래량 대비 매도 호가 압도 비율로 측정
        avg_vol_5 = (
            float(minute_df["거래량"].tail(5).mean()) if minute_df is not None else 1.0
        )
        if avg_vol_5 < 1.0:
            avg_vol_5 = 1.0

        ofi_ratio = ofi_val / avg_vol_5

        # OFI 비율이 -0.15 (평균 거래량의 15% 이상 순매도 호가 우위) 이하일 때 투매 판정
        # VWAP_Dist < -5.0 (극단적 가격 이격)
        # Vol_Surge >= 3.0 (투매 클라이막스 거래량)
        is_climax_bottom = (
            (ofi_ratio < -0.15) and (vwap_dist < -5.0) and (vol_surge >= 3.0)
        )

        if is_climax_bottom:
            return RuleResult(
                is_passed=True,
                score_modifier=200.0,
                reason=f"🔥 [Extreme-Climax] 투매 클라이막스 포착 (OFI_Ratio:{ofi_ratio:.2f}, VWAP:{vwap_dist:.1f}%, Vol:{vol_surge:.1f}x)",
            )

        return RuleResult(
            is_passed=True, score_modifier=0.0, reason="🛡️ [Mean-Rev] 일반 흐름 진행 중"
        )


class MicroStructureRule(BaseRule):
    """
     분별가격정보(Tick_Acc)를 활용한 호가창 미시구조 필터
    - 거래량 동반 여부에 따른 하드/소프트 리젝트 분기 (노이즈 방어)
    """

    def evaluate(
        self, stock_data: Dict[str, Any], market_data: Dict[str, Any]
    ) -> RuleResult:
        # [Standardized Extraction]
        tick_acc = float(
            safe_get(
                stock_data,
                "intra_acc",
                safe_get(
                    stock_data, "tick_acc", safe_get(stock_data, "Tick_Acc", 0.0)
                ),
            )
        )

        # 정보가 없는 종목은 중립 처리
        if tick_acc <= 0.0:
            return RuleResult(
                is_passed=True, score_modifier=0.0, reason="➖ [Micro] 틱 데이터 없음"
            )

        # 1. 틱 노이즈 방어 (거래량 멀티플라이어 교차 검증)
        volume_multiplier = safe_get(stock_data, "volume_multiplier", 1.0)

        # [Sync] WFO 최적화 파라미터 (tick_acc_min: 60.0 / tick_acc_bonus: 120.0) 연동
        params = getattr(market_data, "system_params", {}) or {}
        tick_min = safe_get(params, "tick_acc_min", 60.0)
        tick_bonus_thresh = safe_get(params, "tick_acc_bonus", 120.0)
        bonus_points = safe_get(params, "tick_acc_bonus_points", 15.0)

        if tick_acc < tick_min:
            if volume_multiplier > 1.5:
                # 거래량이 터지면서 체결강도가 낮다 = 진짜 폭포수 (Hard Reject)
                return RuleResult(
                    is_passed=False,
                    score_modifier=-50.0,
                    reason=f"⛔ [Micro] 대량 매도세 동반 (체결강도: {tick_acc:.1f} < {tick_min}, Vol:{volume_multiplier:.1f})",
                )
            else:
                # 거래량 없는 단순 틱 저하 = 일시적 노이즈 (Soft Penalty)
                return RuleResult(
                    is_passed=True,
                    score_modifier=-10.0,
                    reason=f"⚠️ [Micro] 일시적 매도 우위 (체결강도: {tick_acc:.1f})",
                )
        elif tick_acc > tick_bonus_thresh:
            # 매수세 폭발 (Bonus)
            return RuleResult(
                is_passed=True,
                score_modifier=bonus_points,
                reason=f"🔥 [Micro] 매수세 폭발 (체결강도: {tick_acc:.1f} > {tick_bonus_thresh})",
            )

        return RuleResult(
            is_passed=True,
            score_modifier=5.0,
            reason=f"✅ [Micro] 체결강도 양호 ({tick_acc:.1f})",
        )
