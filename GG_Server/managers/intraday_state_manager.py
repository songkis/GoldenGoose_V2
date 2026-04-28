import logging
from typing import Dict, Any
from util.zmq_manager import ZMQPushPull, ZMQ_STATUS_PORT

logger = logging.getLogger(__name__)

class IntradayStateManager:
    """
    [Step 4.2] 64-bit 서버용 Shadow State 매니저 (Consumer)
    - 클라이언트(32-bit)로부터 PULL 소켓을 통해 수신한 실시간 체결 데이터를 반영
    - XING API 직접 조회를 대체하여 지연 시간 최소화 및 물리적 분리 달성
    """
    def __init__(self, engine):
        self.en = engine
        self.running = False

    def listen_state_feedback(self):
        """
        [C/S Architecture] 클라이언트의 피드백을 대기하는 전용 백그라운드 리스너
        """
        try:
            self.zmq_pull = ZMQPushPull(mode="PULL", port=ZMQ_STATUS_PORT)
            logger.info(f"🛰️ [Shadow Listener] Starting ZMQ PULL on port {ZMQ_STATUS_PORT}")
            
            def on_feedback(data):
                if data:
                    self.sync_virtual_state(data)
            
            self.zmq_pull.start_pull_listener(on_feedback)
            self.running = True
        except Exception as e:
            logger.error(f"❌ [Shadow Listener] Failed to start: {e}")

    def sync_virtual_state(self, data: Dict[str, Any]):
        """
        ZMQ 피드백 수신 시 서버 내부 메모리 상태(Shadow State)를 업데이트합니다.
        - 대상 변수: self.en.active_positions_map, self.en.virtual_avail_cash
        """
        try:
            msg_type = data.get("type")
            if msg_type == "FILL":
                ticker = data.get("ticker")
                filled_qty = int(data.get("filled_qty", 0))
                avg_price = float(data.get("avg_price", 0))
                side = data.get("side", "buy").lower()

                # [Step 4.2] 3배 슬리피지 방어 및 무결성 보장을 위한 Lock 획득
                with self.en.capital_lock:
                    if side == "buy":
                        # 매수 체결 -> 가용 현금 차감, 포지션 추가/갱신
                        cost = filled_qty * avg_price
                        self.en.virtual_avail_cash -= cost
                        
                        if ticker not in self.en.active_positions_map:
                            self.en.active_positions_map[ticker] = {
                                "상태": "open",
                                "진입가": avg_price,
                                "포지션수량": filled_qty,
                                "진입일자": data.get("timestamp", ""),
                                "최고가": avg_price,
                                "매매기법": "breakout"
                            }
                        else:
                            pos = self.en.active_positions_map[ticker]
                            # 기존 수량/단가 가중평균 갱신 (추가 매수 대응)
                            old_qty = int(pos.get("포지션수량", 0))
                            old_price = float(pos.get("진입가", 0))
                            new_qty = old_qty + filled_qty
                            new_price = (old_price * old_qty + avg_price * filled_qty) / new_qty
                            pos["포지션수량"] = new_qty
                            pos["진입가"] = new_price
                            pos["최고가"] = max(float(pos.get("최고가", 0)), avg_price)
                        
                        # [Step 4.3] portfolio_list 즉시 업데이트 (루프 반영 보장)
                        if ticker not in self.en.portfolio_list:
                            self.en.portfolio_list.append(ticker)
                        
                        logger.info(f"✅ [Shadow Sync] {ticker} Buy Fill Reflect: {filled_qty}주 @ {avg_price:.0f}. Virtual Cash: {self.en.virtual_avail_cash:,.0f}원")

                    elif side == "sell":
                        # 매도 체결 -> 가용 현금 증가, 포지션 차감
                        revenue = filled_qty * avg_price
                        self.en.virtual_avail_cash += revenue
                        
                        if ticker in self.en.active_positions_map:
                            pos = self.en.active_positions_map[ticker]
                            current_qty = int(pos.get("포지션수량", 0))
                            new_qty = max(0, current_qty - filled_qty)
                            pos["포지션수량"] = new_qty
                            
                            if new_qty == 0:
                                self.en.active_positions_map.pop(ticker, None)
                                # 포트폴리오 리스트에서도 즉시 제거하여 루프에서 제외
                                if ticker in self.en.portfolio_list:
                                    self.en.portfolio_list.remove(ticker)
                        
                        logger.info(f"✅ [Shadow Sync] {ticker} Sell Fill Reflect: {filled_qty}주 @ {avg_price:.0f}. Virtual Cash: {self.en.virtual_avail_cash:,.0f}원")

        except Exception as e:
            logger.error(f"❌ [Shadow State] Sync Processing Error: {e}")
