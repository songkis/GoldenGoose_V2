# -*- coding: utf-8 -*-
import logging
from core.schemas import SignalPayload
from util.Utils import align_quote_price
from util.CommUtils import to_numeric_safe

logger = logging.getLogger(__name__)

def send_order_payload(zmq_push, ticker, command, dynamic_cap, indicators=None):
    """
    [Logic Change] SignalPayload 데이터 구조체(Dataclass) 적용
    - 64비트 분석 엔진 -> 32비트 주문 엔진 전송 최적화 (ZMQ PUSH)
    - SignalPayload DTO를 사용하여 타입 안정성 및 규격 일관성 확보
    """
    try:
        if zmq_push is None:
            return

        # [Requirement] Payload: {"ticker": code, "order_type": "BUY/SELL", ...}
        payload = SignalPayload(
            ticker=ticker,
            order_type="BUY" if command == "BUY" else "SELL",
            price=indicators.get("buy_price", dynamic_cap / indicators.get("qty", 1))
            if indicators and "qty" in indicators
            else dynamic_cap,
            quantity=indicators.get("qty", 0) if indicators else 0,
            cl_ord_id=indicators.get("cl_ord_id", "") if indicators else "",
            reason=indicators.get("reason", "EXIT") if indicators else "SIGNAL",
        )

        zmq_push.push_data(payload.to_dict())
        if logger:
            logger.info(
                f"📡 [ZMQ SignalPayload Sync] {ticker} {command} ({dynamic_cap:,.0f} KRW) 발송 완료."
            )

    except Exception as e:
        if logger:
            logger.error(f"❌ [TradingComm] send_order_payload error: {e}")

def prepare_sell_order_params(
    brain, ticker, current_qty, price, sell_info, order_type=None
):
    """
    [Smart Exit] 매도 주문 파라미터 생성 및 정규화
    [Zero-Defect Update] shadowing 방지를 위해 로컬 import 제거 및 전역 유틸 활용
    """
    current_qty = to_numeric_safe(current_qty)
    ratio = to_numeric_safe(getattr(sell_info, "sell_ratio", 1.0))
    reason = getattr(sell_info, "reason", "EXIT")

    sell_qty = int(current_qty * ratio)
    if sell_qty <= 0:
        return None

    urgency_keywords = ["STOP", "LOSS", "TRAILING", "TIME", "EMERGENCY", "PANIC", "CUT"]
    is_urgent = any(kw in str(reason).upper() for kw in urgency_keywords)

    sell_order_type = (
        order_type
        if order_type
        else (
            sell_info.order_type
            if hasattr(sell_info, "order_type")
            else getattr(sell_info, "order_type", "00")
        )
    )

    if is_urgent:
        sell_order_type = "00"
        # 🚨 [Hotfix] 로컬 import 제거 (UnboundLocalError 원인)
        final_price = price * 0.985
        if logger:
            logger.warning(
                f"🚨 [Urgency Exit] {ticker} 긴급 청산 감지({reason}). -1.5% 하향 지정가 타격."
            )
    elif sell_order_type == "00":
        final_price = price * 0.999
    else:
        final_price = price

    return {
        "command": "SELL",
        "ticker": ticker,
        "qty": sell_qty,
        "price": align_quote_price(final_price),
        "order_price_type": sell_order_type,
        "purchase_price": getattr(sell_info, "purchase_price", 0.0),
        "buy_date": getattr(sell_info, "buy_date", None),
        "buy_price": getattr(sell_info, "buy_price", 0.0),
        "reason": reason,
        "target": None,
        "decision_data": None,
    }
