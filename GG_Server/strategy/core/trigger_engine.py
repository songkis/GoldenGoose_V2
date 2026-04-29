import pandas as pd
import numpy as np
import logging
from strategy.indicators.technical_indicators import (
    calculate_intraday_acceleration_v5_6,
)
from strategy.core.scoring_engine import (
    calculate_refined_supply_score_v6,
)

from core.schemas import ExecutionTriggerResult

logger = logging.getLogger(__name__)


def check_execution_trigger_v10_2(
    minute_df,
    base_info,
    market_avg_acc=0.0,
    is_true_bounce=False,
    is_recovering_leader=False,
    supply_intra_val=0.0,
    intra_acc_val=0.0,  # [추가] 중복 연산을 방지하기 위해 1회 산출된 가속도를 주입받음
    vwap_val=0.0,  # [추가] scoring_engine에서 산출된 VWAP 주입
):
    """
    [V10.2 Refined] 수급 밀집 + 가속도 + 허매수 필터 통합 (Consolidated from TradingComm)
    - 034810(수급 밀집), 008700(가속도 우위) 케이스 완벽 대응
    """
    result = ExecutionTriggerResult()

    # [Quant Fix] Params 정의 최상단 배치 (NameError 완벽 차단)
    if isinstance(base_info, dict):
        params = base_info.get("system_params", base_info.get("params") or {})
    else:
        params = getattr(base_info, "system_params", getattr(base_info, "params", {}))
    if not params:
        params = {}

    try:
        # [Step 0] 바닥권 돌파 정보 선추출 (페널티 예외 적용용)
        if isinstance(base_info, dict):
            breakout_info = base_info.get("breakout_info", {}) if base_info else {}
        else:
            breakout_info = getattr(base_info, "breakout_info", {}) if base_info else {}
        is_bottom_breakout = (
            breakout_info.get("is_breakout", False)
            if isinstance(breakout_info, dict)
            else getattr(breakout_info, "is_breakout", False)
        )

        # 1. 수급 및 가속도 데이터 산출
        supply_intra = (
            supply_intra_val
            if supply_intra_val > 0
            else calculate_refined_supply_score_v6(minute_df)
        )

        # 기존 O(N) 중복 연산 호출 블록 삭제 및 상수 치환 (O(1))
        intra_acc = float(intra_acc_val)

        # [Critical Fix: Phantom Desync Prevention]
        # intra_acc와 tick_acc를 Early Return 이전에 즉시 할당하여, 조기 리턴 시에도 TDE에 정확한 값이 전달되도록 보장
        _val = round(intra_acc, 2)
        result.intra_acc = _val
        result.tick_acc = _val
        result.supply_intra = round(float(supply_intra), 2)

        # [Alpha Fix] 삭제된 verify_intraday_support_v2를 초경량 VWAP 지지 로직으로 대체
        curr_price = float(minute_df["종가"].iloc[-1])

        # [Zero-Latency] vwap 변수 부재로 인한 NameError 방지 (O(1) 프록시 정의)
        vwap = vwap_val if vwap_val > 0 else curr_price
        support_confirm = 10.0 if is_true_bounce else 5.0

        # [Refactor] RS Gap 기반 추격 매수 방지 (Anti-Chasing)
        if isinstance(base_info, dict):
            rs_gap = base_info.get("rs_gap", 0.0)
            daily_score = base_info.get("daily_score", 0)
            _is_true_bounce_base = base_info.get("is_true_bounce", False)
            market_regime_slope = base_info.get("market_regime", "NEUTRAL")
            grade = base_info.get("fuse_grade", "C")
            vol_mult = base_info.get("volume_multiplier", 1.0)
        else:
            rs_gap = getattr(base_info, "rs_gap", 0.0)
            daily_score = getattr(base_info, "daily_score", 0)
            _is_true_bounce_base = getattr(base_info, "is_true_bounce", False)
            market_regime_slope = getattr(base_info, "market_regime", "NEUTRAL")
            grade = getattr(base_info, "fuse_grade", "C")
            vol_mult = getattr(base_info, "volume_multiplier", 1.0)

        # [VIP Amnesty] 진성 반등 주도주는 30%까지 허용 (V-Bounce capturing)
        is_true_bounce = is_true_bounce or _is_true_bounce_base

        # [Zero-Latency] vwap 이격도 검증용 스칼라 (vwap이 0인 경우 방어)
        # intraday_scores에서 이미 산출되었으나, trigger 레이어의 독립성을 위해 단순 이격도만 사용
        rs_chasing_threshold = 25.0 if is_bottom_breakout else 20.0
        if is_true_bounce or is_recovering_leader:
            rs_chasing_threshold = 30.0

        if rs_gap > rs_chasing_threshold:
            result.trigger_hit = False
            result.msg = (
                f"REJECT: 과열권 추격 매수 금지 (RS Gap:{rs_gap:.1f}%) -> 눌림목 대기"
            )
            result.supply_intra = supply_intra
            return result

        # [제안 로직] 수급 점수 산출 직후 적용
        supply_volatility = (
            minute_df["종가"].tail(20).pct_change().std() * 100
        )  # 최근 20봉 선행 변동성으로 O(1) 고속화
        vol_penalty_factor = np.clip(1.0 - (supply_volatility / 2.0), 0.5, 1.0)
        supply_intra *= vol_penalty_factor

        # [Inline Refactoring] O(1) Fake Order Anomaly Detection (Moved from pattern_recognizer)
        try:
            latest = minute_df.iloc[-1]
            prev_rows = minute_df.iloc[-5:-1]
            price_change_abs = abs((latest["종가"] / latest["시가"]) - 1)

            avg_vol_prev = prev_rows["거래량"].mean() + 1e-9
            is_divergent = (latest["체결강도"] < 80) and (
                latest["종가"] > latest["시가"]
            )

            fake_score = 0.0
            if price_change_abs > 0.015 and latest["거래량"] < avg_vol_prev * 0.4:
                fake_score += 40.0
            if is_divergent:
                fake_score += 25.0

            if fake_score >= 50.0:
                result.msg = f"REJECT: 허매수 의심({fake_score})"
                return result
        except Exception:
            fake_score = 0.0

        intraday_score = (
            (intra_acc * 0.4) + (supply_intra * 0.4) + (support_confirm * 0.2)
        )
        prev_high = minute_df["고가"].iloc[-20:-1].max()
        # [TLVI Guard] Forced float conversion to prevent 'NoneType' in abs()
        safe_market_acc = float(market_avg_acc or 0.0)
        market_offset = np.clip(abs(safe_market_acc) * 0.5, 0, 15)

        if grade == "S":
            acc_threshold = 8.0 + market_offset
        elif grade == "A":
            acc_threshold = 18.0 + market_offset
        else:
            acc_threshold = 28.0 + market_offset

        # [Optimization] 거래대금 배열 전체 곱셈 병목 제거 (Scope reduction)
        temp_tail = minute_df.tail(15)
        temp_amount = (temp_tail["거래량"] * temp_tail["종가"]).clip(lower=0)
        recent_amt_avg = temp_amount.tail(3).mean()
        base_amt_avg = temp_amount.tail(15).mean()

        if minute_df["체결강도"].iloc[-1] > 140:
            intraday_score += 5
        intraday_score = float(np.clip(intraday_score, 0, 100))

        result.intra_acc = round(intra_acc, 2)
        result.intraday_score = round(intraday_score, 2)
        result.supply_intra = round(float(supply_intra), 2)
        result.fake_score = fake_score

        breakout_ratio = 0.992 if supply_intra >= 85 else 0.997
        is_breakout = curr_price >= (prev_high * breakout_ratio)

        # Trigger Scenarios
        if supply_intra >= 94.0 and intra_acc > 28.0 and recent_amt_avg >= 50_000_000:
            result.trigger_hit = True
            result.msg = f"BYPASS: 수급 초폭발 (S:{supply_intra:.1f})"
            return result

        breakout_strength_curr = breakout_info.get("breakout_strength", 0.0)

        if grade == "S" and is_breakout:
            if (vol_mult >= 1.5 or breakout_strength_curr >= 30.0) and (
                intra_acc >= acc_threshold or supply_intra >= 60
            ):
                result.trigger_hit = True
                result.msg = f"S등급 주도주 진입 (Grade:{grade}, Acc:{intra_acc})"
                result.special_grade = "S"
                return result

        if is_bottom_breakout and is_breakout:
            breakout_acc_threshold = acc_threshold * 0.8
            if supply_intra >= 70.0 or intra_acc >= breakout_acc_threshold:
                result.trigger_hit = True
                result.msg = f"🚀 바닥권 돌파 승인 (Str:{breakout_strength_curr:.0f}, S:{supply_intra:.1f})"
                return result

        elif is_breakout and supply_intra >= 80 and intra_acc >= (12 + market_offset):
            result.trigger_hit = True
            result.msg = f"수급 밀집 승인 (S:{supply_intra:.1f})"
            return result

        elif (
            daily_score >= 70
            and curr_price >= (vwap * 0.98)
            and (supply_intra >= 55)
            and support_confirm >= 6.0
        ):
            is_rebound_confirmed = (
                minute_df["종가"].iloc[-1] > minute_df["시가"].iloc[-1]
            ) and (recent_amt_avg > base_amt_avg * 1.3)
            if is_rebound_confirmed:
                result.trigger_hit = True
                result.msg = f"눌림목 반등 포착 (S:{supply_intra:.1f})"
                return result

        elif (
            is_breakout
            and support_confirm >= 7.0
            and supply_intra >= 60
            and intra_acc > acc_threshold
        ):
            result.trigger_hit = True
            result.msg = f"정석 돌파 승인 (Acc:{intra_acc:.1f})"
            return result

        if is_true_bounce:
            result.trigger_hit = True
            result.msg = f"🛡️ [V-Bounce VIP Amnesty] 진성 반등 감지로 트리거 자동 승인 (Sup:{supply_intra:.1f})"
            result.special_grade = "S"
            return result

        result.msg = "조건 미충족 또는 시장과다동기화 (트리거 미발동)"
        return result

    except Exception as e:
        logger.error(f"check_execution_trigger Error: {e}")
        return result
