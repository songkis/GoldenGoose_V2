# -*- coding: utf-8 -*-
"""
락 컨트롤러 모듈 (Lock Controllers)

BaseGoose.py에서 분리된 락 관리 클래스들.
매수/매도 락, 분할 주문 관리, 레드카드(매수 금지) 관리 등을 제공합니다.
"""

import datetime
import sys
import traceback
from enum import IntEnum

logger = None


def set_logger(external_logger):
    """외부 로거 설정"""
    global logger
    logger = external_logger


__all__ = [
    "BuyResult",
    "LockController",
    "StockRedCardController",
    "PortfolioManager",
]


class BuyResult(IntEnum):
    ERROR = -1
    WAIT = 0
    SELL = 1
    BUY = 2


class LockController:
    """매수/매도 락 관리 컨트롤러"""

    def __init__(self, default_timeout_seconds=60):
        self.locks = {"buy": {}, "sell": {}}
        self.default_timeout = datetime.timedelta(seconds=default_timeout_seconds)

    def _lock(self, lock_type, stock_code):
        self.locks[lock_type][stock_code] = datetime.datetime.now()
        if logger:
            logger.debug(f"🔒 {lock_type.upper()} 락 설정: {stock_code}")

    def _unlock(self, lock_type, stock_code):
        if stock_code in self.locks[lock_type]:
            self.locks[lock_type].pop(stock_code, None)
            if logger:
                logger.debug(f"🔓 {lock_type.upper()} 락 해제: {stock_code}")

    def _is_locked(self, lock_type, stock_code):
        locked_at = self.locks[lock_type].get(stock_code)
        if not locked_at:
            return False
        if datetime.datetime.now() - locked_at > self.default_timeout:
            self._unlock(lock_type, stock_code)
            if logger:
                logger.debug(
                    f"⏰ {lock_type.upper()} 락 자동 해제(시간초과): {stock_code}"
                )
            return False
        return True

    # 외부용 API
    def lock_buy(self, stock_code):
        self._lock("buy", stock_code)

    def unlock_buy(self, stock_code):
        self._unlock("buy", stock_code)

    def is_buy_locked(self, stock_code):
        return self._is_locked("buy", stock_code)

    def lock_sell(self, stock_code):
        self._lock("sell", stock_code)

    def unlock_sell(self, stock_code):
        self._unlock("sell", stock_code)

    def is_sell_locked(self, stock_code):
        return self._is_locked("sell", stock_code)

    def get_all_locks(self):
        return {"buy": self.locks["buy"].copy(), "sell": self.locks["sell"].copy()}

    def get_all(self, lock_type=None):
        """
        락 목록 조회
        :param lock_type: 'buy' 또는 'sell' 또는 None
        :return: 락에 걸린 종목코드 리스트 또는 전체 딕셔너리
        """
        if lock_type == "buy":
            return list(self.locks["buy"].keys())
        elif lock_type == "sell":
            return list(self.locks["sell"].keys())
        else:
            return {
                "buy": list(self.locks["buy"].keys()),
                "sell": list(self.locks["sell"].keys()),
            }

    @staticmethod
    def my_exception_hook(exctype, value, tb):
        with open("fatal.log", "a") as f:
            traceback.print_exception(exctype, value, tb, file=f)
        sys.__excepthook__(exctype, value, tb)


class StockRedCardController:
    """종목 레드카드(매수 금지) 관리 컨트롤러"""

    def __init__(self):
        self.red_cards = {}  # {stock_code: {"reason": str, "timestamp": datetime}}

    def issue_red_card(self, stock_code, reason):
        """특정 종목에 레드카드를 부여 (당일 매수 금지)"""
        self.red_cards[stock_code] = {
            "reason": reason,
            "timestamp": datetime.datetime.now(),
        }
        if logger:
            logger.warning(f"🚩 [RED CARD] {stock_code} 종목 매수 금지 발령: {reason}")

    def is_banned(self, stock_code):
        """매수 금지 종목인지 확인"""
        return stock_code in self.red_cards

    def revoke_red_card(self, stock_code):
        """레드카드 해제"""
        if stock_code in self.red_cards:
            del self.red_cards[stock_code]
            if logger:
                logger.info(f"✅ [RED CARD] {stock_code} 종목 매수 금지 해제")

    def get_all_banned(self):
        """모든 매수 금지 종목 조회"""
        return list(self.red_cards.keys())

    def get_ban_info(self, stock_code):
        """특정 종목의 레드카드 정보 조회"""
        return self.red_cards.get(stock_code)

    def clear_all(self):
        """모든 레드카드 해제 (새로운 거래일 시작 시)"""
        count = len(self.red_cards)
        self.red_cards.clear()
        if logger and count > 0:
            logger.info(f"🧹 [RED CARD] 모든 매수 금지 해제 ({count}건)")


class PortfolioManager:
    def __init__(self, total_slots, _grade_weights=None):
        self.total_slots = total_slots
        self._grade_weights = _grade_weights or {"S": 1.5, "A": 1.0, "B": 0.6, "C": 0.0}

    def calc_max_allocation(self, total_capital):
        """
        [전략] 종목당 최대 할당 금액 계산 (예: 1,000만원 상한)
        """
        # 기본적으로 total_capital의 일정 비율(예: 10~20%) 또는 고정 금액 상한 적용
        # 여기서는 종목당 최대 1,000만원 상한선 적용
        return min(total_capital * 0.2, 10_000_000)

    def get_market_qty_multiplier(self, market_conditions, is_high_vol_warning=False):
        """
        [Adaptive Filter] 시장 상황 및 종목별 변동성 경고를 반영한 비중 승수 계산
        is_high_vol_warning: 개별 종목의 변동성 과열 여부 (v3_indicators에서 전달)
        """
        current_index_change = market_conditions.get("current_index_change", 0.0)
        market_ma5_dist = market_conditions.get("market_market_ma5_dist", 0.0)
        adr_ratio = market_conditions.get("adr_ratio", 1.0)
        high_rate = market_conditions.get("high_rate", 0.0)
        low_rate = market_conditions.get("low_rate", 0.0)

        try:
            # 1. 기본 가중치 설정
            multiplier = 1.0

            # 2. 시장 기본 상황 (추세 및 ADR)
            if current_index_change > 0.8 and adr_ratio > 1.5:
                multiplier = 1.2  # 강세장
            elif current_index_change < -1.2 or adr_ratio < 0.7:
                multiplier = 0.8  # 약세장

            # 3. 지수 5일선 이격도 필터
            if market_ma5_dist < -0.02:
                multiplier *= 0.7
            elif market_ma5_dist > 0.01:
                multiplier *= 1.2

            # 4. 장중 시장 변동성 필터 (Market Level)
            intraday_range = high_rate - low_rate
            if intraday_range > 2.5:
                multiplier *= 0.85

            # 5. [신규] 개별 종목 변동성 경고 필터 (Stock Level)
            # 고변동성 경고 시 비중을 30% 감축하여 슬리피지 및 급락 리스크 대응
            if is_high_vol_warning:
                multiplier *= 0.7
                # logger.info("⚠️ [Risk Alert] High Volatility Warning: 비중 30% 축소 적용")

            # 6. ADR 보정
            if adr_ratio < 0.5:
                multiplier *= 0.9

            # 7. 최종 범위 제한
            final_multiplier = max(0.4, min(1.5, multiplier))
            return final_multiplier

        except Exception:
            return 0.8

    def calculate_optimal_qty(
        self, brain, ticker, current_price, _grade, market_conditions, v3_indicators
    ):
        """
        [Deprecated] 모든 사이징 로직은 strategy/core/TradingComm.py의
        calculate_optimized_position_size로 통합되었습니다.
        """
        #  하위 호환성을 위해 TradingComm의 중앙 엔진을 호출하도록 브릿지 역할만 수행
        from GG_Server.strategy.core.TradingComm import calculate_optimized_position_size

        # 64bit 브레인이 관리하는 자본금 확보
        total_capital = getattr(brain, "buyable_amt", 0)
        max_stocks = getattr(self, "total_slots", 4)

        # ticker_results 구조에 맞게 데이터 패킹
        eval_data = v3_indicators.get("final_stock_eval", {})
        ticker_results = {
            "ticker": ticker,
            "combined_score": eval_data.get("daily_score", 0),
            "current_price": current_price,
            "intrinsic_grade": eval_data.get("intrinsic_grade", _grade),
            "grade": _grade,
        }

        res = calculate_optimized_position_size(
            ticker_results, total_capital, max_stocks
        )
        return res["buy_qty"], res["target_investment"]
