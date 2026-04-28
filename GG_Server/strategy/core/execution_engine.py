import datetime
import json
import logging
import pandas as pd
import numpy as np

from util.Utils import to_numeric_safe
from strategy.core.TradeExitEngine import (
    TradeExitEngine,
    PositionData,
    ExitLevelsData,
    calculate_dynamic_chandelier_stop,
)
from strategy.core.TradeDecisionEngine import (
    TradePersona,
)

logger = logging.getLogger(__name__)


def set_logger(external_logger):
    global logger
    logger = external_logger


def checkBuyingRule(
    brain,
    stockCode,
    현재가,
    trade_signal,
    market_conditions,
    params: dict = None,
):
    if params is None:
        params = {}

    try:
        if trade_signal:
            _t_info = trade_signal.get("trade_info_json_dict", {})
            _f_dec = _t_info.get("final_trading_decision", {})
            _f_re = str(_f_dec.get("final_reason", ""))
            _h_res = str(_t_info.get("hard_reject_reason", ""))

            if (
                "[Pardon]" in _f_re
                or "[Pardon]" in _h_res
                or "Hyper-Momentum Override" in _f_re
            ):
                logger.info(
                    f"🛡️ [Bypass] {stockCode} 뇌(Brain)의 사면권([Pardon]) 확인. 즉시 매수(buy_sign:2) 발송."
                )
                return {"buy_sign": 2, "매수수량": 0, "reason": "VIP_PARDON_BYPASS"}

        if (
            hasattr(brain, "account_guard")
            and brain.account_guard
            and brain.account_guard.is_emergency_mode
        ):
            logger.info(
                f"🚫 {stockCode} 종목은 계좌 전체 보호 상태(Emergency)로 매수를 건너뜜"
            )
            return {"buy_sign": 1, "매수수량": 0}

        if (
            hasattr(brain, "red_card_controller")
            and brain.red_card_controller
            and brain.red_card_controller.is_banned(stockCode)
        ):
            logger.info(f"🚫 {stockCode} 종목은 레드카드(Banned) 상태로 매수를 건너뜜")
            return {"buy_sign": 1, "매수수량": 0}

        if trade_signal is None:
            return {"buy_sign": int(0), "매수수량": 0}

        trade_info = trade_signal.get("trade_info_json_dict", {})
        if not isinstance(trade_info, dict):
            if isinstance(trade_info, str):
                trade_info = json.loads(trade_info)
            else:
                trade_info = {}

        decision = trade_info.get("final_trading_decision", {})
        grade = decision.get("final_grade", "F")
        can_buy = decision.get("final_can_buy", False)

        if grade in ["F", "C"]:
            return {"buy_sign": 0, "매수수량": 0}

        approved_qty = trade_info.get("execution_trigger", {}).get(
            "approved_quantity", 0
        )
        capital = trade_signal.get("capital", 0)

        if can_buy and (capital > 0 or approved_qty > 0):
            if hasattr(brain, "active_positions_map"):
                db_pos = brain.active_positions_map.get(stockCode, {})
                if db_pos.get("상태") == "open":
                    logger.info(
                        f"🚫 [DCA Block] {stockCode} 기보유 종목입니다. 물타기를 차단합니다."
                    )
                    return {"buy_sign": 0, "매수수량": 0}

            if capital > 0:
                total_buy_amt = capital
                total_buy_qty = int(total_buy_amt // 현재가) if 현재가 > 0 else 0
            else:
                total_buy_qty = approved_qty

            if total_buy_qty <= 0:
                logger.warning(
                    f"⚠️ [Capital Guard] {stockCode} 최종 할당액 0원. 매수 보류."
                )
                return {"buy_sign": 0, "매수수량": 0}

            logger.info(
                f"🎯 [1-Shot Entry] {stockCode} {grade}등급 단일 진입 결정: {total_buy_qty}주"
            )
            return {"buy_sign": 2, "매수수량": total_buy_qty}

        return {"buy_sign": 0, "매수수량": 0}

    except Exception as e:
        logger.error(f"Error in checkBuyingRule: {e}")
        return {"buy_sign": int(-1), "매수수량": 0}


def checkSellRule(
    brain,
    stockCode,
    portfolio_info,
    현재가,
    trade_signal,
    params: dict = None,
):
    from config.system_params import KRX_HOLIDAYS

    if params is None:
        params = {}
    try:
        if trade_signal is None:
            return {"is_sell": False, "reason": "HOLD", "ratio": 0.0}

        trade_info = trade_signal.get(
            "brain_context", trade_signal.get("trade_info_json_dict", {})
        )
        if isinstance(trade_info, str):
            trade_info = json.loads(trade_info)

        매수가 = to_numeric_safe(portfolio_info.get("매수가", 0))
        현재가 = to_numeric_safe(현재가)
        매수후고가 = to_numeric_safe(portfolio_info.get("매수후고가", 현재가))
        수량 = portfolio_info.get("수량", 0)
        profit_rate = ((현재가 - 매수가) / 매수가) if 매수가 > 0 else 0.0

        max_holding = params.get("max_holding_days", 5)
        hard_tp_pct = params.get("hard_take_profit_pct", 10.0) / 100.0

        entry_date_str = portfolio_info.get("매수일", "")
        current_date_str = datetime.datetime.now().strftime("%Y-%m-%d")
        days_since = 0

        if entry_date_str and entry_date_str not in ["UNKNOWN", "None", "null", ""]:
            try:
                if " " in entry_date_str:
                    entry_date_str = entry_date_str.split(" ")[0]
                if len(entry_date_str) == 8 and "-" not in entry_date_str:
                    entry_date_str = f"{entry_date_str[:4]}-{entry_date_str[4:6]}-{entry_date_str[6:]}"

                entry_d = (
                    pd.to_datetime(entry_date_str)
                    .to_datetime64()
                    .astype("datetime64[D]")
                )
                curr_d = (
                    pd.to_datetime(current_date_str)
                    .to_datetime64()
                    .astype("datetime64[D]")
                )
                if KRX_HOLIDAYS:
                    holidays_d = np.array(
                        [
                            pd.to_datetime(h).to_datetime64().astype("datetime64[D]")
                            for h in KRX_HOLIDAYS
                        ]
                    )
                    days_since = np.busday_count(entry_d, curr_d, holidays=holidays_d)
                else:
                    days_since = np.busday_count(entry_d, curr_d)
                days_since = max(0, int(days_since))
            except Exception as e:
                logger.warning(f"🚨 [Date Parse Error] {stockCode} 날짜 연산 실패: {e}")
                days_since = 0

        final_decision = trade_info.get("final_trading_decision", {})
        stock_eval = trade_info.get("final_stock_eval", {})
        atr_5m = to_numeric_safe(trade_info.get("atr_5m", 0.0))
        market_regime = final_decision.get("market_regime", "NORMAL")
        grade = final_decision.get("final_grade", "B")

        v3_indicators = trade_info.get("v3_indicators", {})
        execution_trigger = trade_info.get("execution_trigger", {})
        tick_acc = to_numeric_safe(
            v3_indicators.get("tick_acc", execution_trigger.get("intra_acc", 100.0))
        )

        position_data = PositionData(
            ticker=stockCode,
            purchase_price=매수가,
            current_price=현재가,
            max_price_after_buy=매수후고가,
            quantity=수량,
            bars_since_entry=trade_info.get("bars_since_entry", 0),
            profit_rate=profit_rate,
            grade=grade,
            atr_5m=atr_5m,
            initial_quantity=portfolio_info.get("포지션수량", 수량),
            market_regime=market_regime,
            days_since_entry=days_since,
            tick_acc=tick_acc,
            vwap=to_numeric_safe(portfolio_info.get("vwap", 현재가)),
            max_profit_pct=to_numeric_safe(portfolio_info.get("max_profit_pct", 0.0)),
        )

        exit_levels = trade_info.get("hybrid_exit_levels", {})
        levels_data = ExitLevelsData(
            stop_loss=trade_info.get("current_sl", exit_levels.get("stop_loss", 0.0)),
            take_profit1=exit_levels.get(
                "take_profit1", trade_signal.get("take_profit1", 0.0)
            ),
            take_profit2=exit_levels.get(
                "take_profit2", trade_signal.get("take_profit2", 0.0)
            ),
            trailing_stop_pct=params.get("trailing_stop_pct", 2.5),
            chandelier_stop=calculate_dynamic_chandelier_stop(
                current_high=매수후고가,
                atr=atr_5m if atr_5m > 0 else (현재가 * 0.02),
                beta=getattr(stock_eval, "beta", 1.0)
                if not isinstance(stock_eval, dict)
                else getattr(stock_eval, "beta", 1.0),
                grade=grade,
                market_regime=market_regime,
                current_time=datetime.datetime.now().time(),
                profit_rate=profit_rate,
            ),
            hard_take_profit_ratio=hard_tp_pct,
            max_holding_days=max_holding,
        )

        persona = TradePersona.NEUTRAL_BALANCED
        if market_regime == "BULL":
            persona = TradePersona.BULL_AGGRESSIVE
        elif market_regime in ["BEAR", "CRASH"]:
            persona = TradePersona.BEAR_DEFENSIVE

        is_limit_up = trade_info.get("limit_up_data", {}).get(
            "is_limit_up_entry", False
        )
        signal_msg = trade_info.get("signal", "HOLD")

        exit_engine = TradeExitEngine()
        result = exit_engine.evaluate_sell_decision(
            stock_code=stockCode,
            position=position_data,
            levels=levels_data,
            persona=persona,
            signal_msg=signal_msg,
            is_limit_up_entry=is_limit_up,
        )
        return result
    except Exception as e:
        logger.error(f"Error in checkSellRule: {e}")
        return {"is_sell": False, "reason": "HOLD", "ratio": 0.0}


def process_buy_orders(
    brain, stock_cd, current_price, trade_signal, market_conditions, params=None
):
    try:
        return checkBuyingRule(
            brain,
            stock_cd,
            current_price,
            trade_signal,
            market_conditions,
            params=params,
        )
    except Exception as e:
        logger.error(f"[ExecutionEngine-Buy] {stock_cd} Error: {e}")
        return {"buy_sign": 0, "매수수량": 0}


def process_sell_orders(
    brain, stock_cd, portfolio_info, current_price, trade_signal, params=None
):
    try:
        return checkSellRule(
            brain, stock_cd, portfolio_info, current_price, trade_signal, params=params
        )
    except Exception as e:
        logger.error(f"[ExecutionEngine-Sell] {stock_cd} Error: {e}")
        return {"is_sell": False, "reason": "HOLD", "ratio": 0.0}
