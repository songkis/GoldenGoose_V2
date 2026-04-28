from typing import List
from strategy.rules.base_rule import BaseRule, RuleResult
import logging
from util.Utils import safe_get

logger = logging.getLogger("GoldenGoose.RuleEngine")


class RuleEvaluator:
    """[Adaptive Execution] 다수의 룰을 체인 형태로 검사하는 엔진"""

    def __init__(self, rules: List[BaseRule]):
        self.rules = rules

    def run_all(self, stock_data: dict, market_data: dict) -> dict:
        total_score_modifier = 0.0
        failed_reasons = []
        passed_reasons = []
        extra_info_agg = {}

        # [Logic Change: Microstructure Filter]
        # Zero-Tick 사면권을 원천 박탈하기 위해 호가 데이터(Tick_Acc)가 최소 기준에 미달하면 VIP 특권 해제
        tick_acc_val = float(
            safe_get(stock_data, "intra_acc", safe_get(stock_data, "tick_acc", 0.0))
        )
        if tick_acc_val < 15.0:
            stock_data["is_true_bounce"] = False
            stock_data["has_pardon"] = False

        # [Hard-Risk Guard] Bypass 허용 대상 Rule 목록 (이외의 리스크 룰은 절대 사면 불가)
        BYPASS_ALLOWED_RULES = [
            "DailyGateRule",
            "MicroTrendTemplateRule",
            "SmartBreakoutTriggerRule",
            "DynamicExecutionThresholdRule",
        ]

        for rule in self.rules:
            result = rule.evaluate(stock_data, market_data)
            rule_name = rule.__class__.__name__

            # [Target 2: VIP 사면권과 실행 트리거의 강제 동기화]
            # SmartBreakoutTriggerRule이 실패하더라도 has_pardon이 있다면 사면 로직 가동
            if (
                not result.is_passed
                and safe_get(stock_data, "has_pardon", False)
                and rule_name == "SmartBreakoutTriggerRule"
            ):
                result = self.apply_pardon_logic(rule_name, result, stock_data)

            total_score_modifier += result.score_modifier

            # 💉 Aggregate extra_info if present
            if hasattr(result, "extra_info") and result.extra_info:
                extra_info_agg.update(result.extra_info)

            if not result.is_passed:
                # [Pardon Guard] 주도주 특격(Whitelist) 적용 여부 검사
                is_leader = (
                    safe_get(stock_data, "is_recovering_leader", False)
                    or safe_get(stock_data, "is_true_bounce", False)
                    or safe_get(stock_data, "has_pardon", False)
                )

                # [Pardon Revocation] 실시간 수급/지지선 붕괴 시 사면권 즉시 박탈
                exec_trigger = safe_get(stock_data, "execution_trigger", {})
                tick_acc = float(
                    safe_get(
                        exec_trigger,
                        "intra_acc",
                        safe_get(exec_trigger, "tick_acc", 0.0),
                    )
                )
                current_price = float(safe_get(stock_data, "current_price", 0.0))
                vwap = float(
                    safe_get(stock_data, "vwap", safe_get(stock_data, "VWAP", 0.0))
                )

                if is_leader and not safe_get(stock_data, "is_true_bounce", False) and (
                    tick_acc < 15.0 or (vwap > 0 and current_price < vwap * 0.98)
                ):
                    is_leader = False
                    logger.info(
                        "🚫 [Pardon Revoked] 수급/지지선 붕괴로 주도주 특권 박탈"
                    )

                if is_leader and rule_name in BYPASS_ALLOWED_RULES:
                    logger.info(
                        f"🛡️ [Pardon Guard] 주도주/진성반등 특권으로 {rule_name} 룰 강제 패스."
                    )
                    passed_reasons.append(f"{rule_name}: {result.reason} (BYPASS)")
                    continue  # 다음 룰(Risk Guard 등) 계속 평가

                failed_reasons.append(f"{rule_name}: {result.reason}")

                # 하나라도 필수 룰을 통과 못하면 즉시 차단 (Fast Fail)
                return {
                    "is_approved": False,
                    "score_modifier": total_score_modifier,
                    "reason": " | ".join(failed_reasons),
                    "extra_info": extra_info_agg,
                }
            else:
                passed_reasons.append(result.reason)

        # [True Alpha Guard] 실제 퀀트 산출 엣지(combined_score)가 없는 거래는 최종적으로 차단
        combined_score = float(safe_get(stock_data, "combined_score", 0.0))
        if combined_score <= 0.0:
            return {
                "is_approved": False,
                "score_modifier": total_score_modifier,
                "reason": f"ZERO_ALPHA_SCORE (실제 엣지 결여: {combined_score})",
                "extra_info": extra_info_agg,
            }

        return {
            "is_approved": True,
            "score_modifier": total_score_modifier,
            "reason": " | ".join(passed_reasons),
            "extra_info": extra_info_agg,
        }


    def apply_pardon_logic(
        self, rule_name: str, result: RuleResult, stock_data: dict
    ) -> RuleResult:
        """
        [Target 2 Logic]
        has_pardon이 True인 경우, SmartBreakoutTriggerRule의 VWAP 하회 조건을 -1.5%까지 일시 확장하여 사면
        """
        if rule_name == "SmartBreakoutTriggerRule":
            current_price = float(
                safe_get(
                    stock_data, "current_price", safe_get(stock_data, "price", 0.0)
                )
            )
            vwap = float(
                safe_get(stock_data, "vwap", safe_get(stock_data, "VWAP", 0.0))
            )

            # VWAP 근처(-1.5%)의 일시적 눌림목인 경우 사면권 발동
            if vwap > 0 and current_price >= vwap * 0.985:
                logger.info(
                    f"🛡️ [Pardon-Sync] {safe_get(stock_data, 'ticker')} VIP 사면권 발동. VWAP 이격 허용치(-1.5%) 내 지지 확인."
                )
                return RuleResult(
                    is_passed=True,
                    score_modifier=max(-5.0, result.score_modifier),  # 감점 완화
                    reason=f"🛡️ [Pardon-Sync] VWAP 하회(-1.5% 이내)하나 VIP 사면권으로 통과 (P:{current_price:.0f}/V:{vwap:.0f})",
                )

        return result
