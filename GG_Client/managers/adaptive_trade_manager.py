import logging
import threading

logger = logging.getLogger(__name__)

_param_lock = threading.Lock()  #  전역 파라미터 락


class AdaptiveTradeManager:
    """
    승률(Win Rate)과 손익비(Reward/Risk Ratio)를 기반으로
    Half-Kelly Criterion을 적용하여 동적 포지션 사이즈를 계산하는 매니저.
    """

    # [Phase 1] 시장 국면(Regime) 기반 가중치 설정 (Regime Multipliers)
    REGIME_CONFIG = {
        "BULL": {"capital_mult": 1.2, "sl_mult": 1.2, "tp_mult": 1.5},
        "NEUTRAL": {"capital_mult": 1.0, "sl_mult": 1.0, "tp_mult": 1.0},
        "BEAR": {"capital_mult": 0.7, "sl_mult": 0.8, "tp_mult": 0.8},
        "CRASH": {"capital_mult": 0.4, "sl_mult": 0.5, "tp_mult": 0.6},
    }
    # 종목 등급별 승률(p) 및 기본 한도(투자 비중)
    GRADE_PROBABILITIES = {
        "S": 0.65,  # 예상 승률 65%
        "A": 0.55,  # 예상 승률 55%
        "B": 0.45,  # 예상 승률 45%
    }

    # [수익률 극대화] 3분할 진입을 감안하여 초기 파이 크기 상향 조정
    GRADE_MAX_ALLOCATION = {
        "S": 0.40,
        "A": 0.15,
        "B": 0.09,
    }

    # 기본 손익비 설정 (타점 대비 목표가와 손절가의 비율)
    DEFAULT_REWARD_RISK_RATIO = 2.0  # 기본 손익비 2:1

    @classmethod
    def hot_swap_parameters(cls, new_params: dict):
        """
        [Phase 4] ai_parameters.json 변경 감지 시 엔진 재시작 없이 실시간 파라미터 주입
        """
        with _param_lock:
            try:
                # 1. 승률(Probabilities) 업데이트
                if "grade_probabilities" in new_params:
                    cls.GRADE_PROBABILITIES.update(new_params["grade_probabilities"])

                # 2. 최대 비중(Allocation) 업데이트
                if "grade_max_allocation" in new_params:
                    cls.GRADE_MAX_ALLOCATION.update(new_params["grade_max_allocation"])

                # 3. 기본 손익비 업데이트
                if "default_reward_risk_ratio" in new_params:
                    cls.DEFAULT_REWARD_RISK_RATIO = float(
                        new_params["default_reward_risk_ratio"]
                    )

                logger.info(
                    f"🔥 [WFO Hot-Swap] AdaptiveTradeManager parameters updated: {new_params}"
                )
                return True
            except Exception as e:
                logger.error(f"❌ [WFO Hot-Swap] Failed to swap parameters: {e}")
                return False

    @classmethod
    def calculate_kelly_fraction(
        cls, win_rate: float, reward_risk_ratio: float
    ) -> float:
        """
        Kelly Criterion 공식을 통해 최적의 베팅 비율(f*)을 계산합니다.
        f* = (b * p - q) / b
        """
        if reward_risk_ratio <= 0:
            return 0.0

        p = win_rate
        q = 1.0 - p
        b = reward_risk_ratio

        f_star = (b * p - q) / b
        return max(0.0, f_star)

    @classmethod
    def calculate_position_size(
        cls,
        grade: str,
        total_capital: float,
        reward_risk_ratio: float = None,
        is_recovering_leader: bool = False,
        has_pardon: bool = False,
        market_regime: str = "NEUTRAL",
        atr_pct: float = 0.0,
        avg_volume_amt_5m: float = 0.0,
    ) -> float:
        """
         Centralized Position Sizing Guardian
        1. Half-Kelly 기반 비중 산출
        2. Regime Multiplier 적용 (상승/하락장 배팅 조절)
        3. Spread-Sizer 적용 (변동성/슬리피지 비용 방어)
        4. Dynamic Impact-Cap 적용 (유동성 함정 방어 - 마이크로스트럭처 리깅)
        """
        grade_upper = str(grade).upper() if grade else "B"
        market_regime = str(market_regime).upper() if market_regime else "NEUTRAL"

        # 1. 국면 가중치 (Regime Multipliers)
        regime_meta = cls.REGIME_CONFIG.get(market_regime, cls.REGIME_CONFIG["NEUTRAL"])
        capital_multiplier = regime_meta["capital_mult"]

        # 2. 켈리 비중 (f*) 계산
        with _param_lock:
            p = cls.GRADE_PROBABILITIES.get(grade_upper, cls.GRADE_PROBABILITIES["B"])
            max_alloc = cls.GRADE_MAX_ALLOCATION.get(
                grade_upper, cls.GRADE_MAX_ALLOCATION["B"]
            )
            default_rr = cls.DEFAULT_REWARD_RISK_RATIO

        b = reward_risk_ratio if reward_risk_ratio is not None else default_rr
        kelly_f = cls.calculate_kelly_fraction(p, b)
        half_kelly_f = kelly_f / 2.0 # Half-Kelly applied

        # [Directive 4] Volatility Risk Parity (ATR 기반 자본 할당)
        # Target_Volatility(3.0%) / 개별종목_ATR% 비율 도출
        # ATR이 높으면 작게, ATR이 낮으면 크게 (단, 20% 캡은 유지)
        vol_scalar = 1.0
        if atr_pct > 0:
            vol_scalar = 3.0 / atr_pct
        
        final_fraction = half_kelly_f * vol_scalar

        # [Safety Guard] Hard Cap: 20% max allocation per position
        final_fraction = min(final_fraction, 0.20)

        # 3. 배팅금 기초 산출
        raw_recommended_amount = total_capital * final_fraction * capital_multiplier

        # 등급별/사면권 비중 (Single Entry - 분할매수 로직 제거됨)
        if market_regime == "CRASH" and (is_recovering_leader or has_pardon):
            SPLIT_ENTRY_RATIO = 0.50
        else:
            SPLIT_ENTRY_RATIO = 1.0

        actual_entry_amount = raw_recommended_amount * SPLIT_ENTRY_RATIO

        # 4. [Spread-Sizer] 변동성 기반 비중 축소 (Slippage Defense)
        spread_multiplier = 1.0
        if not (is_recovering_leader or has_pardon) and grade_upper != "S":
            if atr_pct > 6.0:
                spread_multiplier = 0.0
            elif atr_pct > 2.0:
                spread_multiplier = max(0.25, 1.0 - (atr_pct - 2.0) * 0.18)

        actual_entry_amount *= spread_multiplier

        # 5. [Dynamic Impact-Cap] 유동성 함정 방어 (변동성 가중 스케일링) - [Task-Directive] 삭제
        # if avg_volume_amt_5m > 0:
        #     impact_cap = cls.calculate_impact_cap(avg_volume_amt_5m, atr_pct or 2.0)
        #     if actual_entry_amount > impact_cap:
        #         logger.info(f"🛡️ [Dynamic Impact-Cap] ATR:{atr_pct:.2f}% -> Ratio Scaling. Limit: {impact_cap:,.0f}")
        #         actual_entry_amount = impact_cap

        # 7. [Survival Patch] Market Regime Kelly Dampening
        if market_regime in ["BEAR", "CRASH"]:
            dampened_amount = actual_entry_amount * 0.5
            logger.info(
                f"📉 [Survival-Dampening] {market_regime} 감지: 비중 50% 축소 ({actual_entry_amount:,.0f} -> {dampened_amount:,.0f})"
            )
            actual_entry_amount = dampened_amount

        return float(actual_entry_amount)

    def apply_wfo_parameters(
        self, latest_wfo_params: dict, strategy_evaluator=None, trade_exit_engine=None
    ):
        """
        [Phase 1 Upgrade] WFO 파라미터에 시장 국면(Regime Multipliers)을 투영하여 최종 엔진에 주입합니다.
        """
        if not latest_wfo_params:
            return

        from config.ai_settings import PARAMS

        # 0. 현재 국면 파악
        market_regime = "NEUTRAL"
        if strategy_evaluator:
            market_regime = getattr(strategy_evaluator, "global_regime", "NEUTRAL")
        else:
            try:
                from util.Utils import get_global_regime

                market_regime = get_global_regime()
            except Exception:
                pass
        market_regime = str(market_regime).upper()
        regime_meta = self.REGIME_CONFIG.get(
            market_regime, self.REGIME_CONFIG["NEUTRAL"]
        )

        # 1. WFO 파라미터 로드
        target_profit = latest_wfo_params.get("hard_take_profit_pct", 3.0)
        stop_loss = latest_wfo_params.get("hard_stop_loss_pct", -2.0)
        if stop_loss > 0:
            stop_loss = -stop_loss  # SL 음수 보정

        # 2. [Phase 1] 국면 가중치 적용 (Regime Multipliers)
        final_tp = target_profit * regime_meta["tp_mult"]
        final_sl = (
            stop_loss * regime_meta["sl_mult"]
        )  # BEAR/CRASH 일 때 0.8배 등으로 절대값 축소 (음수이므로 절대값이 작아짐)

        # CRASH 국면 하드 캡 (최대 -1.5% 손절 제한)
        if market_regime == "CRASH":
            final_sl = max(final_sl, -1.5)

        # 3. 엔진 주입 (Hot-Swap)
        if trade_exit_engine:
            trade_exit_engine.update_hard_limits(
                target_profit_pct=final_tp / 100.0, stop_loss_pct=final_sl / 100.0
            )

        # PARAMS 전역 객체 업데이트
        PARAMS.update("hard_take_profit_pct", final_tp)
        PARAMS.update("hard_stop_loss_pct", final_sl)

        logger.info(
            f"🔄 [Regime-WFO Sync] Applied {market_regime} Multipliers | TP: {final_tp:.2f}% (x{regime_meta['tp_mult']}), SL: {final_sl:.2f}% (x{regime_meta['sl_mult']})"
        )
