# -*- coding: utf-8 -*-
import logging
from datetime import timedelta
import config.ai_settings as ai_settings

# 모듈화된 구성 요소들 임포트
from .trading_factory import (
    create_strategy_evaluator,
    get_current_optimal_params,
    load_optimal_parameters as _load_optimal_params,
)
from .trading_payload_manager import (
    send_order_payload,
    prepare_sell_order_params,
)
from .trading_decision_gate import (
    get_final_trading_decision as _get_final_trading_decision,
    get_failed_candidate_result,
    get_bars_since_entry,
)
from .trading_engine import (
    combined_score_for_ticker_v3 as _combined_score_for_ticker_v3,
)
from strategy.core.candidate_selector import select_candidates_v2

# TradeDecisionEngine 임포트
from strategy.core.TradeDecisionEngine import TradeDecisionEngine

logger = logging.getLogger(__name__)

# 전역 상태 변수들 (Facade 유지)
optimal_params = get_current_optimal_params()
master_strategy_evaluator = create_strategy_evaluator(optimal_params)
global_decision_engine = TradeDecisionEngine(
    panic_threshold=-1.20, overheat_threshold=10.0
)

# 캐시 저장용 딕셔너리
market_cache = {}
M_CACHE_EXPIRY = timedelta(minutes=ai_settings.MIN_TERM)
market_index_changes_cache = {}

def load_optimal_parameters():
    """최적 파라미터 로드 및 글로벌 엔진 동기화"""
    global optimal_params, master_strategy_evaluator
    optimal_params, master_strategy_evaluator = _load_optimal_params()

def tradingComm_set_logger(external_logger):
    """모든 서브 모듈의 로거 동기화"""
    global logger
    logger = external_logger

    # 서브 모듈 로거 설정 (모듈 내부에 logger 변수가 있는 경우)
    from . import trading_factory, trading_payload_manager, trading_decision_gate, trading_engine
    for mod in [trading_factory, trading_payload_manager, trading_decision_gate, trading_engine]:
        if hasattr(mod, "logger"):
            mod.logger = logger

    # 기존 리팩토링된 모듈들에 로거 설정
    from strategy.rules import adaptive_swing_trading
    from strategy.indicators import technical_indicators, dynamic_thresholds, market_analysis
    from strategy.core import scoring_engine, execution_engine, candidate_selector
    
    adaptive_swing_trading.set_logger(logger)
    technical_indicators.set_logger(logger)
    dynamic_thresholds.set_logger(logger)
    market_analysis.set_logger(logger)
    scoring_engine.set_logger(logger)
    execution_engine.set_logger(logger)
    candidate_selector.set_logger(logger)

    # 파라미터 로드
    load_optimal_parameters()

def get_final_trading_decision(ticker, daily_df, minute_df, ticker_results, exec_trigger, tp, params=None, decision_result=None):
    """의사결정 게이트웨이 대행"""
    return _get_final_trading_decision(
        ticker, daily_df, minute_df, ticker_results, exec_trigger, tp, 
        params=params, decision_result=decision_result, 
        master_strategy_evaluator=master_strategy_evaluator
    )

def combined_score_for_ticker_v3(ticker, daily_df, params=None, capital=100000000.0, minute_df=None, intraday_params=None, market_avg_acc=0.0, tp="intraday", today_market_data_df=None, market_regime_override=None, port_list=None):
    """메인 분석 엔진 대행"""
    return _combined_score_for_ticker_v3(
        ticker, daily_df, params=params, capital=capital, minute_df=minute_df, 
        intraday_params=intraday_params, market_avg_acc=market_avg_acc, tp=tp, 
        today_market_data_df=today_market_data_df, market_regime_override=market_regime_override, 
        port_list=port_list, global_decision_engine=global_decision_engine, 
        master_strategy_evaluator=master_strategy_evaluator
    )
