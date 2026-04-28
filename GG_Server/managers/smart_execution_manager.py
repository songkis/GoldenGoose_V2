import logging
import random
import time
import threading
from PyQt5.QtCore import QObject, QTimer

logger = logging.getLogger(__name__)


class SmartExecutionManager(QObject):
    """
    [Phase 3] 32-bit 전진 배치형 초정밀 분할 주문 매니저 (Smart Iceberg)
    OBI(Orderbook Imbalance)를 반영하여 PyQt Non-blocking 상태 머신으로 구동됩니다.
    """

    # 🛡️  클래스 변수로 전역 쿨타임 및 락 선언 (모든 스레드가 공유)
    _global_order_lock = threading.Lock()
    _last_order_time = {}

    def __init__(self, goose):
        super().__init__()
        self.goose = goose
        self.tick_cache = {}
        self.active_tasks = {}  # ticker -> task_dict
        self.active_timers = {}  # ticker -> QTimer

    def _get_tick_size(self, price):
        """Standard KRW Tick Size (KOSPI/KOSDAQ)"""
        if price < 2000:
            return 1
        elif price < 5000:
            return 5
        elif price < 20000:
            return 10
        elif price < 50000:
            return 50
        elif price < 200000:
            return 100
        elif price < 500000:
            return 500
        else:
            return 1000

    def _add_ticks(self, price, ticks):
        current_price = price
        for _ in range(abs(ticks)):
            tick_size = self._get_tick_size(current_price)
            if ticks > 0:
                current_price += tick_size
            else:
                current_price -= tick_size
        return current_price

    def update_tick(self, szTrCode, result):
        """
        KOSPI(H1_) / KOSDAQ(HA_) 호가 잔량 수급 파서
        """
        ticker = result.get("종목코드")
        if not ticker:
            return

        try:
            ask_vol = int(result.get("총매도호가잔량", 0))
            bid_vol = int(result.get("총매수호가잔량", 0))

            #  연산 예외(ZeroDivisionError) 방어
            if ask_vol + bid_vol == 0:
                obi = 0.0
            else:
                obi = (bid_vol - ask_vol) / (ask_vol + bid_vol)

            self.tick_cache[ticker] = {
                "obi": obi,
                "ask_vol": ask_vol,
                "bid_vol": bid_vol,
                "update_ts": time.time(),
            }
        except Exception as e:
            logger.error(f"[Smart-Exec] update_tick error for {ticker}: {e}")

    def execute_smart_order(self, payload):
        """
        64-bit 뇌(Brain)에서 ZMQ로 구동 지시가 도착했을 때 호출됨.
        반드시 PyQt 메인 스레드에서 돌아야 함 (Thread-Safety).
        """
        ticker = payload.get("ticker")
        side = payload.get("command", "BUY")
        qty = payload.get("qty", 0)
        price = payload.get("price", 0)
        reason = payload.get("reason", "breakout").lower()
        is_buy = side == "BUY"

        if qty <= 0:
            return

        # 🛡️  스레드 세이프(Thread-Safe) 중복 주문 차단 (매도 전용)
        if side == "SELL":
            current_time = time.time()
            with self._global_order_lock:
                last_time = self._last_order_time.get(ticker, 0)
                if (
                    current_time - last_time < 5.0
                ):  # 5초 이내 동일 종목 중복 주문 절대 금지
                    logger.warning(
                        f"🚫 [Spam Guard] {ticker} 매도 주문 무시 (쿨타임 작동 중)"
                    )
                    return
                # 쿨타임 갱신
                self._last_order_time[ticker] = current_time

        # [Adaptive Execution] Breakout 강세 돌파 시 +3호가 위로 공격적 주문
        execution_price = price
        if is_buy and reason == "breakout":
            execution_price = self._add_ticks(price, 3)
            logger.info(
                f"🚀 [Smart-Exec] Breakout detected. Price adjusted: {price} -> {execution_price} (+3 ticks)"
            )

        logger.info(
            f"🧊 [Smart-Exec] {ticker} {side} {qty}주 분할 주문 접수. (Price: {execution_price}, Reason: {reason})"
        )
        logger.debug(f"🧊 [Smart-Exec] 💼 Task Payload: {payload}")

        #  State Override: 기존 활성화된 주문 취소
        if ticker in self.active_timers:
            logger.warning(
                f"🚨 [Smart-Exec] {ticker} 기존 분할 수행 중 새 명령 도착. 타이머 Stop."
            )
            self.active_timers[ticker].stop()
            del self.active_timers[ticker]

        #  Regime & ClOrdID Extraction
        market_regime = payload.get("indicators", {}).get("market_regime", "NEUTRAL")
        cl_ord_id = payload.get("cl_ord_id")

        # 상태 모델 초기화
        task = {
            "ticker": ticker,
            "side": side,
            "total_qty": qty,
            "remaining_qty": qty,
            "base_price": execution_price,
            "original_brain_price": price,  # Fallback용 원본 가격
            "reason": reason,
            "is_buy": is_buy,
            "start_time": time.time(),
            "payload": payload,
            "state": "RUNNING",
            "market_regime": market_regime,
            "cl_ord_id": cl_ord_id,
        }
        self.active_tasks[ticker] = task

        # Non-blocking 실행 연쇄 시작
        self._dispatch_next_chunk(ticker)

    def cancel_task(self, ticker, status="CANCELLED"):
        """외부(BaseGoose 등)에서 비동기적으로 작업을 강제 중지할 때 호출"""
        if ticker in self.active_tasks:
            logger.warning(f"🛑 [Smart-Exec] Manual cancellation requested for {ticker}. Status: {status}")
            self._finalize_task(ticker, status)

    def _get_dynamic_delay(self, task):
        """OBI 기반 동적 딜레이 계산 (Slowing/Pacing)"""
        ticker = task["ticker"]
        cache = self.tick_cache.get(ticker, {})
        obi = cache.get("obi", 0.0)

        base_delay = random.uniform(1.2, 2.5)

        # 🚨 [Emergency Dump] 긴급 청산 사유 감지 시 대기 시간(Delay) 0초 강제 할당
        exit_reason = task.get("reason", "").upper()
        if any(
            keyword in exit_reason
            for keyword in ["EMERGENCY", "SLIPPAGE", "TIMECUT", "STOP_LOSS"]
        ):
            return 0.0

        #  CRASH Regime: 초고속 집행 (0.3s ~ 0.7s)
        if task.get("market_regime") == "CRASH" and not task["is_buy"]:
            return random.uniform(0.3, 0.7)

        # [Directional Sync Mirroring]
        # 매수 시 OBI > 0.3 (매수잔량 우세): 가격 상승 압박 -> 매집 속도 증가
        # 매도 시 OBI > 0.3 (매수잔량 우세): 호가 지지선 탄탄 -> 공격적 매도 가능
        if obi > 0.3:
            delay = base_delay * 0.6  # 1.6x 배속 가속
        elif obi < -0.3:
            delay = base_delay * 1.5  # passive 감속
        else:
            delay = base_delay

        return max(0.5, min(5.0, delay))

    def _get_dynamic_chunk(self, task):
        """OBI 기반 동적 물량(청크) 계산 (Smart Iceberg)"""
        remaining = task["remaining_qty"]
        total = task["total_qty"]
        ticker = task["ticker"]
        obi = self.tick_cache.get(ticker, {}).get("obi", 0.0)

        # 기본 15%~25% 랜덤 분할
        fraction = random.uniform(0.15, 0.25)

        # 🚨 [Emergency Dump] 긴급 청산 사유 감지 시 분할 매도 로직을 무시하고 전량(100%) 반환
        exit_reason = task.get("reason", "").upper()
        if any(
            keyword in exit_reason
            for keyword in ["EMERGENCY", "SLIPPAGE", "TIMECUT", "STOP_LOSS"]
        ):
            return remaining

        #  CRASH Regime: 공격적 전량 매도 (Market Dump)
        if task.get("market_regime") == "CRASH" and not task["is_buy"]:
            fraction = 0.5  # 50%씩 뭉텅이로 던짐

        base_chunk = int(total * fraction)

        if obi > 0.3:
            scaled = base_chunk * 1.3  # Aggressive 증량
        elif obi < -0.3:
            scaled = base_chunk * 0.7  # Defensive 감량
        else:
            scaled = base_chunk

        # [Dust Order Protection] Zero-qty 및 먼지 주문 에러 방지
        chunk = max(1, int(scaled))
        chunk = min(remaining, chunk)

        # [Sweep Logic] 남은 수량이 전체의 10% 미만일 때 잔량 일괄 전량 발사
        if (remaining - chunk) < (total * 0.1):
            return remaining

        return chunk

    def _dispatch_next_chunk(self, ticker):
        """순환형 상태 실행기 (QTimer.singleShot 순회)"""
        task = self.active_tasks.get(ticker)
        if not task or task["state"] != "RUNNING":
            return

        # [Adaptive Callback] 3초 경과 시 Breakout -> Pullback (Passivity) 전환
        now = time.time()
        elapsed = now - task["start_time"]

        if task["reason"] == "breakout" and elapsed > 3.0:
            logger.warning(
                f"🕒 [Smart-Exec] {ticker} Breakout fill failed for 3s. Falling back to Pullback (Passive Price)."
            )
            task["reason"] = "pullback"
            task["base_price"] = task[
                "original_brain_price"
            ]  # 공격적 +3틱에서 원본가로 후퇴
            # 이 시점부터는 무리하게 위에서 사지 않고 대기함

        remaining = task["remaining_qty"]
        if remaining <= 0:
            self._finalize_task(ticker, "DONE")
            return

        # [Starvation Timeout] 최대 60초 대기 한도 (CRASH 모드 시 12초로 단축)
        timeout_limit = 12.0 if task.get("market_regime") == "CRASH" else 60.0
        if time.time() - task["start_time"] > timeout_limit:
            logger.warning(
                f"⏰ [Smart-Exec] {ticker} {timeout_limit}초 타임아웃 도달. 남은 수량 스윕!"
            )
            self._sweep_remaining(task)
            self._finalize_task(ticker, "TIMEOUT_CLEARED")
            return

        # 1. 동적 수량 산출
        chunk_qty = self._get_dynamic_chunk(task)
        price = task["base_price"]

        logger.info(
            f"🧊 [Smart-Exec] {ticker} {task['side']} {chunk_qty}주 발사! (Price: {price}, Reason: {task['reason']}, Remaining: {remaining}주)"
        )

        #  Chunk Execution Logic with Result Handling Callback
        def handle_chunk_result(nRet):
            """BaseGoose.query_buy/sell에서 실제 API 호출 완료 후 실행되는 콜백"""
            # [Zero-Defect Fix] Xing API는 성공 시 0 이상의 Handle ID를 반환함. 0만 성공으로 보면 오류로 오판함.
            if nRet is not None and nRet >= 0:
                # 3. 잔여 수량 차감 및 상태 루프 예약
                task["remaining_qty"] -= chunk_qty

                if task["remaining_qty"] <= 0:
                    self._finalize_task(ticker, "DONE")
                else:
                    delay_ms = int(self._get_dynamic_delay(task) * 1000)

                    # PyQt 단일 지연 실행 머신 가동 (Event Loop 프리징 방어)
                    timer = QTimer()
                    timer.setSingleShot(True)
                    # 람다 레퍼런스로 가비지 보존
                    timer.timeout.connect(lambda: self._dispatch_next_chunk(ticker))
                    timer.start(delay_ms)
                    self.active_timers[ticker] = timer
            else:
                logger.error(
                    f"❌ [Smart-Exec] Chunk execution failed for {ticker} with code {nRet}. Halting loop."
                )
                self._finalize_task(ticker, f"ERROR_{nRet}")

        # 2. XING API 다이렉트 쿼리 (BaseGoose 래핑 메서드 활용)
        order_type = task["payload"].get("order_type")

        #  CRASH Regime: 매도시 시장가 강제 집행 ("03")
        if task.get("market_regime") == "CRASH" and not task["is_buy"]:
            logger.info(f"🚨 [Panic-Exit] {ticker} CRASH 모드 시장가 매도 집행")
            self.goose.query_sell(
                ticker, chunk_qty, 0, order_type="03", on_done=handle_chunk_result
            )
        elif task["is_buy"]:
            self.goose.query_buy(ticker, chunk_qty, price, on_done=handle_chunk_result)
        else:
            self.goose.query_sell(
                ticker,
                chunk_qty,
                price,
                order_type=order_type,
                on_done=handle_chunk_result,
            )

    def _sweep_remaining(self, task):
        ticker = task["ticker"]
        remaining = task["remaining_qty"]
        price = task["base_price"]
        logger.info(f"🚨 [Smart-Exec] {ticker} Sweep Remaining Qty: {remaining}")
        if task["is_buy"]:
            self.goose.query_buy(ticker, remaining, price)
        else:
            self.goose.query_sell(ticker, remaining, price)
        task["remaining_qty"] = 0

    def _finalize_task(self, ticker, status):
        """작업 종료 청소 및 64-bit 락 해제를 위한 ZMQ ACK 탑승"""
        task = self.active_tasks.get(ticker, {})
        cl_ord_id = task.get("cl_ord_id")

        if ticker in self.active_timers:
            self.active_timers[ticker].stop()
            del self.active_timers[ticker]

        if ticker in self.active_tasks:
            del self.active_tasks[ticker]

        # [ZMQ ACK] 64-bit 뇌에 아이스버그 테스크 완료 이벤트 Push
        if hasattr(self.goose, "zmq_pub") and self.goose.zmq_pub:
            try:
                payload = {
                    "type": "ICEBERG_DONE",
                    "ticker": ticker,
                    "status": status,
                    "cl_ord_id": cl_ord_id,
                    "timestamp": time.strftime("%H:%M:%S"),
                }
                # BaseGoose의 TOPIC_ORDER_RESULT b'ORD_RES' 활용
                self.goose.zmq_pub.publish_data(self.goose.TOPIC_ORDER_RESULT, payload)
                logger.info(
                    f"📡 [ZMQ] Published ICEBERG_DONE to 64-bit: {ticker} (ClOrdID: {cl_ord_id})"
                )
            except Exception as e:
                logger.error(f"[Smart-Exec] _finalize_task ACK push error: {e}")
