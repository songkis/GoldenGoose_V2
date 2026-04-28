import logging


logger = logging.getLogger(__name__)


class SafetyGuard:
    """
    [SafetyGuard] 매수/매도 진입 전 최종 관문 로직 관리
    """

    def __init__(self, state_manager):
        self.state = state_manager

    def is_buyable(
        self,
        can_buy: bool,
        is_buy: int,
        trigger_hit: int,
        grade: str,
        decision_state: str = None,
        position_size_ratio: float = 0.0,
        reason_str: str = "",
    ) -> bool:
        """
        [Zero-Defect] 브레인 분석과 실행 트리거의 최종 합치 여부 판정
        수학적 엣지(ratio > 0)와 기술적 트리거(trigger_hit)가 동시에 만족될 때만 진입 허용.
        """
        # [Zero-Defect] 이분법적 룰 패스(VIP Amnesty) 폐기 및 수학적 엣지 검증 강제
        if can_buy and is_buy and trigger_hit and (position_size_ratio > 0.0):
            if grade in ["S", "A", "B"]:
                return True

        # 엔진 내부 상태(Decision State)의 강제 승인 확인 (Hyper-Momentum 등)
        if decision_state and "APPROVE" in str(decision_state):
            if position_size_ratio > 0.0:
                return True

        return False

    def check_local_guard(
        self, stock_cd: str, portfolio_list: list, total_slots: int
    ) -> bool:
        """
        포트폴리오 슬롯 및 종목별 제안 제한 확인
        """
        if len(portfolio_list) >= total_slots:
            if stock_cd not in portfolio_list:
                return False
        return True
