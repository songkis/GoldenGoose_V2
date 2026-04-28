# -*- coding: utf-8 -*-
import logging
import datetime as dt
from .data_processor import to_numeric_safe
from .db_utils import update_capital

logger = logging.getLogger(__name__)

def parse_avail_cash(data: dict) -> float:
    """
    [Zero-Defect] 증권사별 다양한 주문가능금액 키워드를 탐색하여 가용 자산을 추출합니다.
    """
    if not data or not isinstance(data, dict):
        return 0.0

    priority_keys = [
        "총평가금액",
        "D+2추정예수금",
        "추정D2예수금",
        "D+2예수금",
        "현금주문가능금액",
        "주문가능금액",
        "D+2금액",
        "추정순자산",
        "d2avamt",
        "dnams",
        "d2_wash_avail",
        "d2_avail",
        "예수금",
        "현금잔고",
        "d2_available",
        "buyable_amt",
        "avail_cash",
    ]

    for key in priority_keys:
        val = data.get(key)
        if val is not None:
            numeric_val = to_numeric_safe(val)
            if float(numeric_val) > 0:
                return float(numeric_val)

    return 0.0

def get_actual_buying_power(account_info, open_buy_amt=0, reserve_ratio=0.95):
    """
    [Hardening] 자산(Net Asset) 기반 사이징 파워 산출 로직.
    """
    try:
        # 1. 명시적 키 이름 탐색
        net_asset = 0.0
        for k in ["추정순자산", "총자산", "d_sunjasan", "total_asset"]:
            val = to_numeric_safe(account_info.get(k, 0))
            if val >= 50_000_000:  # 5천만원 이상 자산 기준
                net_asset = val
                break

        # 2. 브루트포스 최대값 탐색 (인코딩 깨짐 대비)
        if net_asset == 0:
            vals = [to_numeric_safe(v) for v in account_info.values()]
            if vals:
                net_asset = max(vals)

        d2_cash = to_numeric_safe(account_info.get("추정D2예수금", 0))

        # [Strategy] 순자산에서 미체결 주문과 슬리피지 버퍼(reserve_ratio)를 적용하여 베팅 여력 산출
        available_cash = d2_cash
        buying_power = int(max(0, (net_asset - open_buy_amt) * reserve_ratio))

        # 5.  소액 주문 노이즈 필터링
        # 자산의 0.5% 미만이거나 10만원 미만인 경우 효율성을 위해 0 처리
        if buying_power < max(net_asset * 0.005, 100_000):
            buying_power = 0

        current = dt.datetime.now()
        if current.minute % 10 == 0 and current.second < 10:
            if logger:
                logger.info(
                    f" [BuyingPower V9] Asset:{net_asset:,.0f} | D2:{d2_cash:,.0f} | "
                    f"Avail_Cash:{available_cash:,.0f} | Result:{buying_power:,.0f}"
                )

        return float(max(0, buying_power))

    except Exception as e:
        if logger:
            logger.error(f"get_actual_buying_power error: {e}")
        return 0.0

def setUnitInvestment(account_info, main_window):
    """
    [Hardening] 1/N 포트폴리오 슬롯 기반 단위 투자금 산출.
    남은 슬롯이 아닌 '전체 슬롯' 대비 총자산 배분 방식을 채택하여 일관된 사이징을 유지합니다.
    """
    SAFE_BUYABLE_AMT = get_actual_buying_power(account_info)

    total_slot_cnt = 5  # Default fallback
    running_slot_cnt = 0
    if main_window is not None and hasattr(main_window, "gooses"):
        # AIGoose(3) + GuardianGoose(2) 등의 설정값 합산
        total_slot_cnt = sum(
            int(getattr(r, "포트폴리오수", 0)) for r in main_window.gooses
        )

        ai_slots = main_window.gooses[0].portfolio.keys()
        guard_slots = main_window.gooses[1].portfolio.keys()
        running_slot_cnt = len(ai_slots) + len(guard_slots)
    
    if total_slot_cnt <= 0:
        total_slot_cnt = 5
    remain_slots = max(0, total_slot_cnt - running_slot_cnt)

    try:
        # [Strategy] 89M / 5 = 17.8M 식의 정적 1/N 배분
        # 가용 자금(Buying Power)이 아닌 총 자산 기준으로 사이징하여 일관성 유지
        net_asset = to_numeric_safe(account_info.get("추정순자산", 0))
        if net_asset < 50_000_000:  # 자산 인식 실패 시 Buying Power 활용
            net_asset = SAFE_BUYABLE_AMT

        단위투자금 = int(net_asset // total_slot_cnt)

        # [Risk Control] 1종목 최대 투자금 제한 (자산의 30%)
        max_unit_limit = int(net_asset * 0.30)
        단위투자금 = min(단위투자금, max_unit_limit)

        if 단위투자금 < 0:
            단위투자금 = 0

        update_capital(단위투자금)

    except Exception as e:
        if logger:
            logger.error(f"setUnitInvestment 계산 오류: {e}")
        단위투자금 = 0

    # 모든 goose 객체 동기화
    if main_window is not None and hasattr(main_window, "gooses"):
        for goose in main_window.gooses:
            goose.단위투자금 = 단위투자금

    # 4. [Multi-Layer Verification] 매수 금지 판정
    MIN_ORDER_THRESHOLD = 100_000
    MAX_ASSET_UTILIZATION = 0.93  # 총 자산 대비 최대 매입 비중 (현금 확보)

    is_insufficient_cash = SAFE_BUYABLE_AMT < MIN_ORDER_THRESHOLD
    is_too_small_order = 0 < 단위투자금 < MIN_ORDER_THRESHOLD

    current_buying_amt = to_numeric_safe(account_info.get("매입금액", 0))
    net_asset_val = to_numeric_safe(account_info.get("추정순자산", 0))
    is_full_exposure = (
        current_buying_amt > (net_asset_val * MAX_ASSET_UTILIZATION)
        if net_asset_val > 0
        else False
    )

    if (
        is_insufficient_cash
        or is_too_small_order
        or is_full_exposure
        or remain_slots <= 0
    ):
        if main_window is not None:
            main_window.주문가능금액부족 = True

        if remain_slots <= 0:
            reason = f"잔여 슬롯 없음 ({running_slot_cnt}/{total_slot_cnt})"
        elif is_insufficient_cash:
            reason = f"가용자금({SAFE_BUYABLE_AMT:,.0f}) 부족"
        elif is_too_small_order:
            reason = f"최소주문금액({MIN_ORDER_THRESHOLD:,.0f}) 미달"
        else:
            reason = f"계좌 최대 노출(93%) 도달"

        if remain_slots <= 0 or (단위투자금 > 0 and current_buying_amt > 0):
            if logger:
                logger.warning(f"⚠️ 매수 중단: {reason} (단위투자금: {단위투자금:,.0f})")
    else:
        if main_window is not None:
            main_window.주문가능금액부족 = False

    return {
        "단위투자금": 단위투자금,
        "주문가능금액": SAFE_BUYABLE_AMT,
        "total_slot_cnt": total_slot_cnt,
        "running_slot_cnt": running_slot_cnt,
        "aigoose_slots": ai_slots,
        "guardian_slots": guard_slots,
        "remain_slots": remain_slots,
    }

class AccountGuard:
    def __init__(self, start_equity, max_daily_loss_pct=3.0):
        self.max_daily_loss_pct = max_daily_loss_pct
        self.is_emergency_mode = False
        self.start_day_equity = start_equity
        self.error_count = 0  # [Internal Audit] 연속 오류 확인용
        if logger:
            logger.info(f"AccountGuard initialized: {self.start_day_equity}")

    def check_guard_status(self, current_equity, index_change):
        # [Internal Audit] 장 시작 자산이 0인 경우 나눗셈 에러 발생
        if self.start_day_equity == 0:
            if logger:
                logger.warning(
                    "⚠️ AccountGuard: start_day_equity가 0입니다. 현재 자산으로 초기화합니다."
                )
            self.start_day_equity = current_equity
            return

        # 1. 데이터 유효성 검사 (Sanity Check)
        # 시작 자산의 50% 미만으로 찍히면 데이터 오류로 간주하고 스킵
        if current_equity < (self.start_day_equity * 0.5):
            if logger:
                logger.warning(f"⚠️ 비정상 자산 데이터 감지 (무시): {current_equity}")
            return self.is_emergency_mode

        start_equity = self.start_day_equity
        # 수익률 계산 시 분모(start_day_equity) 보호
        daily_return_pct = (
            (current_equity - self.start_day_equity) / self.start_day_equity * 100
        )

        # 2. 보호 스위치 판단 (연속 3회 적중 시 작동)
        is_hit = daily_return_pct <= -self.max_daily_loss_pct or index_change <= -3.0

        if is_hit:
            self.error_count += 1
            if logger:
                logger.warning(
                    f"⚠️ 계좌 보호 조건 충족 ({self.error_count}/3): {daily_return_pct:.2f}%"
                )
        else:
            self.error_count = 0  # 정상 범위면 카운트 리셋

        # 3회 연속일 때만 실제 모드 전환
        if self.error_count >= 3:
            if not self.is_emergency_mode:
                if logger:
                    logger.error(
                        f"🚨 [계좌 보호 스위치 최종 작동] 현재 손실률: {daily_return_pct:.2f}%"
                    )
                self.is_emergency_mode = True
        else:
            # 복구 조건 (더 완만하게 설정 가능)
            if self.is_emergency_mode and daily_return_pct > -1.0:
                self.is_emergency_mode = False
                self.error_count = 0

        return self.is_emergency_mode
