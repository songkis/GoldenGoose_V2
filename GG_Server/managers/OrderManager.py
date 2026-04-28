# -*- coding: utf-8 -*-
import time as _tm


class OrderManager:
    def __init__(self, engine):
        self.en = engine
        self.logger = engine.logger
        self.last_sync = _tm.time()

    def handle_order_result(self, res):
        """ZMQ 주문 결과 수신 및 포지션 동기화"""
        try:
            t = res.get("ticker")
            side = res.get("side")  # 'buy' or 'sell'
            status = res.get("status")  # 'filled', 'canceled', etc
            qty = int(res.get("filled_qty", 0))
            price = float(res.get("avg_price", 0))

            if status == "filled" and t:
                # [Directive 3] Atomic State Reconciliation & Audit
                cl_ord_id = res.get("cl_ord_id")
                expected_data = None
                
                # [Zero-Defect Fix] 기존 cl_ord_lock 외에 포지션 맵 전용 Lock(pos_lock) 획득
                pos_lock = getattr(self.en, "pos_lock", self.en.cl_ord_lock)
                
                with self.en.cl_ord_lock:
                    if cl_ord_id in self.en.pending_cl_orders:
                        expected_data = self.en.pending_cl_orders.pop(cl_ord_id)
                    elif t in self.en.pending_cl_orders: # Fallback
                        expected_data = self.en.pending_cl_orders.pop(t)

                if expected_data:
                    exp_qty = expected_data.get("qty", 0)
                    exp_price = expected_data.get("price", 0)
                    
                    # Audit Check (Slippage/Calculation Mismatch)
                    qty_diff = abs(qty - exp_qty)
                    price_slip = abs(price - exp_price) / exp_price if exp_price > 0 else 0
                    
                    if qty_diff > 0 or price_slip > 0.01: # 1% slippage threshold
                        self.logger.warning(
                            f"⚠️ [Audit-Warning] {t} Order Mismatch! "
                            f"Actual({qty}주 @ {price:.0f}) vs Expected({exp_qty}주 @ {exp_price:.0f}). "
                            "Shadow Portfolio Reconciled to Broker Reality."
                        )
                
                with pos_lock: # 매수/매도 포지션 맵 업데이트 원자성(Atomicity) 보장
                    if side == "buy":
                        existing_pos = self.en.active_positions_map.get(t)
                        if existing_pos:
                            # [Directive 2] Single-Bullet Enforcement: 추가 매수 무시 및 로그 출력
                            self.logger.warning(
                                f"🛡️ [Single-Bullet] {t} Duplicate Buy Signal Blocked. Maintaining original position."
                            )
                        else:
                            # New Position (Single Entry Only)
                            self.en.active_positions_map[t] = {
                                "상태": "open",
                                "진입가": price,
                                "수량": qty,
                                "최고가": price,
                                "매매기법": "breakout",
                                "진입일자": _tm.strftime("%Y-%m-%d %H:%M:%S", _tm.localtime()),
                            }
                        if hasattr(self.en, "bought_stocks"):
                            self.en.bought_stocks[t] = _tm.time()
                    elif side == "sell":
                        # [Directive 3] Audit Catch before memory deletion
                        existing_pos = self.en.active_positions_map.pop(t, None)
                        if existing_pos:
                            # 1. DB Settlement Trigger (Zero-Loss Audit)
                            if hasattr(self.en, "db_writer") and self.en.db_writer:
                                try:
                                    settlement_data = {
                                        "ticker": t,
                                        "buy_date": existing_pos.get("진입일자", existing_pos.get("entry_time")),
                                        "buy_price": float(existing_pos.get("진입가", 0)),
                                        "sell_date": _tm.strftime("%Y-%m-%d %H:%M:%S", _tm.localtime()),
                                        "sell_price": price,
                                        "qty": qty,
                                        "pnl_pct": (price - float(existing_pos.get("진입가", price))) / float(existing_pos.get("진입가", price)) * 100 if float(existing_pos.get("진입가", 0)) > 0 else 0
                                    }
                                    # [Task] Trigger async DB update thread
                                    self.en.db_writer.put(settlement_data)
                                except Exception as log_e:
                                    self.logger.error(f"❌ [Forced-Audit] Logging Failed: {log_e}")

                            # 2. Performance Feedback Loop
                            entry_price = float(existing_pos.get("진입가", price))
                            is_win = price > entry_price
                            # Duck-Typing 기반 동적 브레인 탐색 및 피드백 주입
                            brain = getattr(self.en, "decision_engine", None)
                            if not brain and hasattr(self.en, "tc"):
                                brain = getattr(self.en.tc, "brain", None)

                            if brain and hasattr(brain, "update_performance_feedback"):
                                brain.update_performance_feedback(is_win)

                        if hasattr(self.en, "bought_stocks"):
                            self.en.bought_stocks.pop(t, None)

                self.en.save_state()
                self.logger.info(
                    f"🎯 [Order Filled] {t} | {side} | {qty}주 @ {price:.0f}"
                )

            #  어떤 상태(filled, canceled 등)든 결과가 오면 스마트 주문 락 해제
            if t and hasattr(self.en, "active_smart_orders"):
                if isinstance(self.en.active_smart_orders, dict):
                    self.en.active_smart_orders.pop(t, None)
                elif isinstance(self.en.active_smart_orders, set):
                    self.en.active_smart_orders.discard(t)

        except Exception as e:
            self.logger.error(f"[OrderManager] Error handling result: {e}")

    def sync_with_db(self):
        """엔진 상태와 DB/계좌 정합성 강제 동기화"""
        if _tm.time() - self.last_sync < 60:
            return
        self.en.reconcile_holdings()  # IntradayTrading의 재조합 로직 호출
        self.last_sync = _tm.time()

    def execute_buy_order(self, ticker, price, amount):
        """
         VIP Fast-Track용 주문 페이로드 생성
        - ticker: 종목코드
        - price: 현재가 (주문가)
        - amount: 투자금액
        """
        qty = int(amount / price) if price > 0 else 0
        if qty <= 0:
            return None

        # [Standard Payload] TradingComm 및 ZMQ Worker 규격 준수
        payload = {
            "ticker": ticker,
            "side": "buy",
            "order_type": "03",  # 시장가 (또는 사용자 설정에 따름)
            "price": float(price),
            "qty": qty,
            "amount": float(amount),
            "timestamp": _tm.time(),
        }
        return payload

    def execute_sell_order(self, ticker, price, qty):
        """
         매도 ZMQ 파이프라인 페이로드 생성
        - ticker: 종목코드
        - price: 현재가 (주문가)
        - qty: 매도 수량
        """
        qty = int(qty)
        if qty <= 0:
            return None

        # [Directive 1] ZMQ Trace Key Injection (The 0-Second Bug Fix)
        # 포지션 맵에서 진입 당시의 원본 시점과 가격을 추출하여 매도 페이로드에 합체
        existing_pos = self.en.active_positions_map.get(ticker, {})
        buy_date = existing_pos.get("진입일자", existing_pos.get("entry_time", _tm.strftime("%Y-%m-%d %H:%M:%S", _tm.localtime())))
        buy_price = existing_pos.get("진입가", price)

        # [Standard Payload] Sell side symmetry with trace keys (Self-Destruction of 0-Sec Bug)
        payload = {
            "ticker": ticker,
            "side": "sell",
            "order_type": "03",  # 시장가 매도
            "price": float(price),
            "qty": qty,
            "buy_date": buy_date,
            "buy_price": float(buy_price),
            "timestamp": _tm.time(),
        }
        return payload
