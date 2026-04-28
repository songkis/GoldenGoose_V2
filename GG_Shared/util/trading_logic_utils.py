# -*- coding: utf-8 -*-
import logging
import math
from .data_processor import to_numeric_safe

logger = logging.getLogger(__name__)

def update_dynamic_exit_levels_v2(
    current_price, entry_price, stop_loss, combined_score
):
    """
    [V7.6 Aggressive Profit Protection]
    수익률 구간별로 손절선을 공격적으로 상향하여 수익 반납 방어
    """
    # Safe value check
    current_price = to_numeric_safe(current_price)
    entry_price = to_numeric_safe(entry_price)
    stop_loss = to_numeric_safe(stop_loss)

    if entry_price == 0:
        return stop_loss

    conf = combined_score / 100.0
    profit_rate = (current_price - entry_price) / entry_price
    new_stop_loss = stop_loss

    # 1단계: 수익 3% 이상 - 본절가 방어 (Breakeven)
    if profit_rate >= 0.03:
        # 본절가 -1% 수준으로 올려서 손실 거래 전환 방지
        new_stop_loss = max(new_stop_loss, entry_price * 0.99)

    # 2단계: 수익 5% 이상 - 수익금의 절반 이상 보전
    if profit_rate >= 0.05:
        # 기존 0.95 -> 0.96으로 상향 (신뢰도에 따라 최대 98%까지)
        base_trailing = 0.96 + (0.02 * conf)
        new_stop_loss = max(new_stop_loss, current_price * base_trailing)

    # 3단계: 수익 7% 이상 - 초강력 추격 (98% 고정)
    # 이때부터는 '날아가는 말'에서 떨어지지 않도록 타이트하게 붙음
    if profit_rate >= 0.07:
        # 고점 대비 2% 하락 시 즉시 전량 익절 구조
        ultra_trailing = 0.98 if conf > 0.8 else 0.975
        new_stop_loss = max(new_stop_loss, current_price * ultra_trailing)

    return round(new_stop_loss, 2)

def is_buyable(
    can_buy,
    is_buy,
    trigger_hit,
    grade,
    decision_state=None,
    is_recovering_leader=False,
    reason_str="",
):
    """
    [V10.5 Optimized] 최종 매수 실행 게이트웨이 (명령 복종 패턴)
    - [Zero-Defect: Executive Pardon Honor] 뇌의 사면권([Pardon]) 확인 시 무조건 즉시 승인
    - 뇌가 기각(can_buy=False)한 종목은 어떤 경우에도 진입 불가 (단일 진실 공급원 준수)
    """
    clean_grade = str(grade).strip().upper()
    actual_can_buy = str(can_buy).upper() in ["TRUE", "1", "YES"] or can_buy is True

    # 🛡️ [Zero-Defect: Executive Pardon Honor] 브레인 사면권 절대 복종 (VIP 프리패스)
    # 뇌가 명시적으로 사면([Pardon])을 결정했다면, 하위 지표(trigger_hit 등) 미달을 무시하고 즉시 문을 엽니다.
    if (
        "[Pardon]" in str(reason_str)
        or "Hyper-Momentum Override" in str(reason_str)
        or is_recovering_leader
    ):
        if logger:
            logger.info(
                f"🛡️ [Gateway Bypass] {clean_grade}등급 주도주/사면권 확인. 최종 승인."
            )
        return True

    # 1. 뇌(Brain)의 최종 결정 플래그 확인 (단일 진실 공급원)
    if not actual_can_buy:
        return False

    # 0. 전략 엔진 최종 승인 바이패스 (APPROVE 계열 상태 대응)
    if decision_state and str(decision_state).startswith("APPROVE"):
        return True

    # 2. 불필요한 등급 제외 (B등급 이상만 진입 허용)
    if clean_grade not in ["S", "A"]:
        if not is_recovering_leader:
            return False

    # 3. 실제 실행 트리거 확인 (Adaptive Execution)
    actual_is_buy = str(is_buy).upper() in ["TRUE", "1", "YES"] or is_buy is True
    actual_trigger_hit = (
        str(trigger_hit).upper() in ["TRUE", "1", "YES"] or trigger_hit is True
    )

    return actual_trigger_hit or actual_is_buy

def check_buy_signal(K3_S3_index):
    try:
        # 기본 가격 정보 직접 딕셔너리에서 추출
        시가 = float(K3_S3_index["시가"])
        고가 = float(K3_S3_index["고가"])
        저가 = float(K3_S3_index["저가"])
        현재가 = float(K3_S3_index["종가"])

        # 평균 계산
        시저평 = (시가 + 저가) / 2
        시고평 = (시가 + 고가) / 2
        저고평 = (저가 + 고가) / 2
        평균3 = (시저평 + 시고평 + 저고평) / 3

        # 변동폭 계산
        변동폭_시저 = abs(시가 - 저가)
        변동폭_시고 = abs(시가 - 고가)
        변동폭_저고 = abs(고가 - 저가)
        평균변동폭 = (변동폭_시저 + 변동폭_시고 + 변동폭_저고) / 3

        # 매수 조건 검사
        매수조건1 = 현재가 > 평균3  # 현재가가 평균보다 높음
        매수조건2 = 현재가 < (고가 - 평균변동폭 * 0.5)  # 고가에서 너무 멀지 않음
        매수조건3 = 현재가 > (저가 + 평균변동폭 * 0.3)  # 저가에서 적절히 상승

        # 로깅
        if logger:
            logger.info(
                f"현재가: {현재가}, 3평균: {평균3:.2f}, "
                f"평균변동폭: {평균변동폭:.2f}, "
                f"조건1: {매수조건1}, 조건2: {매수조건2}, 조건3: {매수조건3}"
            )

        # 모든 조건을 만족할 때만 매수 신호
        return 매수조건1 and 매수조건2 and 매수조건3

    except KeyError as e:
        if logger:
            logger.error(f"KeyError 발생: {e}")
        return False
    except ValueError as e:
        if logger:
            logger.error(f"ValueError 발생 (숫자 변환 실패): {e}")
        return False
    except Exception as e:
        if logger:
            logger.error(f"알 수 없는 에러 발생: {e}")
        return False
