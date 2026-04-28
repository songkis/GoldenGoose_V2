from abc import ABC, abstractmethod
from typing import Dict, Any
from core.schemas import RuleResult


class BaseRule(ABC):
    """
    [Code Integrity & Linting] 모든 전략 룰의 최상위 부모 클래스
    """

    @abstractmethod
    def evaluate(
        self, stock_data: Dict[str, Any], market_data: Dict[str, Any]
    ) -> RuleResult:
        """
        조건을 평가하여 통과 여부와 가산점, 사유를 반환합니다.
        """
        pass
