import logging
import datetime
import uuid as _uuid
from typing import Dict, Optional
from util.zmq_manager import make_json_safe

logger = logging.getLogger(__name__)


class OrderService:
    """
    [OrderService] ZMQ 주문 전송 및 페이로드 생성 전담 서비스
    """

    def __init__(self, zmq_push):
        self.zmq_push = zmq_push

    def send_order(
        self, ticker: str, side: str, amount: float, indicators: Optional[Dict] = None
    ):
        """
        [Standard] 주문 페이로드 생성 및 발송
        """
        try:
            if not self.zmq_push:
                return

            cl_ord_id = (
                indicators.get("cl_ord_id", str(_uuid.uuid4())[:8])
                if indicators
                else str(_uuid.uuid4())[:8]
            )

            payload = {
                "command": side.upper(),
                "ticker": ticker,
                "order_type": f"SINGLE_{side.upper()}",
                "target_amt": amount,
                "urgency": "HIGH",
                "timestamp": datetime.datetime.now().strftime("%H:%M:%S.%f"),
                "cl_ord_id": cl_ord_id,
            }

            if indicators:
                for key in ["grade", "qty", "order_price_type", "buy_date", "buy_price", "reason"]:
                    if key in indicators:
                        payload[key] = indicators[key]

            self.zmq_push.push_data(make_json_safe(payload))
            logger.info(
                f"📡 [OrderService] {ticker} {side} ({amount:,.0f} KRW) 페이로드 사격 완료. ID: {cl_ord_id}"
            )
            return cl_ord_id

        except Exception as e:
            logger.error(f"❌ [OrderService] send_order error: {e}")
            return None

    def prepare_sell_params(
        self,
        ticker: str,
        qty: int,
        price: float,
        sell_info: Dict,
        order_type: str = "00",
    ):
        """
        매도 주문 파라미터 규격화
        """
        return {
            "ticker": ticker,
            "qty": qty,
            "price": price,
            "order_type": order_type,
            "reason": sell_info.get("reason", "EXIT"),
            "buy_date": sell_info.get("buy_date"),
            "buy_price": sell_info.get("buy_price"),
        }
