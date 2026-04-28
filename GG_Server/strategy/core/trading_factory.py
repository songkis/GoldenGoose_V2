# -*- coding: utf-8 -*-
import logging
from config.system_params import SYSTEM_PARAMS
from config.ai_config_manager import config_manager
import config.ai_settings as ai_settings
from strategy.core.rule_evaluator import RuleEvaluator
from strategy.rules.entry_rules import (
    AbsoluteMarketRegimeRule,
    IntradayTimeFilterRule,
    RSGapRule,
    VCPPatternRule,
    UnifiedTrendGateRule,
    InstitutionalVolumeSurgeRule,
    SmartBreakoutTriggerRule,
    DynamicExecutionThresholdRule,
    AIDeepLearningEnsembleRule,
    SlippageDefenseRule,
    ExtremeMeanReversionRule,
    MicroStructureRule,
)

logger = logging.getLogger(__name__)

# 글로벌 싱글톤 객체들
optimal_params = {}
master_strategy_evaluator = None

def create_strategy_evaluator(params: dict = None):
    """주어진 파라미터(Grid)에 맞춰 룰 엔진을 동적으로 생성합니다."""
    if params is None:
        params = {}
    return RuleEvaluator(
        [
            AbsoluteMarketRegimeRule(),
            IntradayTimeFilterRule(),
            RSGapRule(**params),
            VCPPatternRule(**params),
            UnifiedTrendGateRule(),  # [V8.0 Consolidated] Trend + Daily Gate
            InstitutionalVolumeSurgeRule(**params),
            SmartBreakoutTriggerRule(),
            DynamicExecutionThresholdRule(**params),
            AIDeepLearningEnsembleRule(**params),
            SlippageDefenseRule(),
            ExtremeMeanReversionRule(),
            MicroStructureRule(),
        ]
    )

def get_current_optimal_params():
    """현재 시스템의 모든 활성 파라미터(기본값 + 최적화값)를 반환합니다."""
    params = SYSTEM_PARAMS.copy()
    params.update(config_manager.params)
    return params

def load_optimal_parameters():
    """[Adaptive Execution] 로거가 준비된 직후, 파일에서 최신 파라미터를 읽어와 룰 엔진을 재조립합니다."""
    global optimal_params, master_strategy_evaluator

    #  config_manager를 통해 최신 파일 내용 로드
    config_manager._load_config()
    optimal_params = get_current_optimal_params()

    #  ai_settings.py의 PARAMS 딕셔너리도 동기화하여 전체 시스템에 반영
    for key, value in optimal_params.items():
        ai_settings.PARAMS[key] = value

    # 새로운 파라미터로 엔진 심장 교체
    master_strategy_evaluator = create_strategy_evaluator(optimal_params)
    if logger:
        logger.info(
            f"✅ [Auto-Tuning] 최적 파라미터 동적 로드 완료 (Total: {len(optimal_params)} keys)"
        )
    return optimal_params, master_strategy_evaluator
