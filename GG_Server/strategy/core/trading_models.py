from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
import pandas as pd


#  WFO 및 파라미터 최적화용 전역 적합도 평가 함수
def calculate_custom_fitness(
    trades_df: pd.DataFrame, min_trades_required: int = 10
) -> float:
    """
    승률 45% 달성 및 안정적 우상향을 위한 퀀트 표준 적합도 함수
    - Win-Rate Penalty: 승률 45% 미만 시 지수함수적 감점
    - RR Penalty: 손익비 1.5 미만 시 감점
    """
    if trades_df is None or trades_df.empty or len(trades_df) < min_trades_required:
        return -9999.0  # 거래수 미달 무조건 도태

    # '수익률' 컬럼이 없는 경우 대비 방어 로직 (컬럼명 유연화)
    return_col = "수익률" if "수익률" in trades_df.columns else "profit_rate"
    if return_col not in trades_df.columns:
        return -9999.0

    win_trades = trades_df[trades_df[return_col] > 0]
    loss_trades = trades_df[trades_df[return_col] <= 0]

    win_rate = len(win_trades) / len(trades_df)
    avg_return = trades_df[return_col].mean()

    gross_profit = win_trades[return_col].sum()
    gross_loss = abs(loss_trades[return_col].sum())

    # 수익요인(Profit Factor)
    profit_factor = (
        (gross_profit / gross_loss) if gross_loss > 0 else (gross_profit + 1.0)
    )

    # 🛡️ [Win-Rate Enforcer] 승률 45% 방어선 로직
    target_win_rate = 0.45
    if win_rate < target_win_rate:
        # 45% 미만 시 기하급수적 페널티 부여
        win_rate_penalty = (win_rate / target_win_rate) ** 2
    else:
        # 달성 시 보너스 가중치
        win_rate_penalty = 1.0 + (win_rate - target_win_rate)

    # 🛡️ [Risk-Reward Enforcer] 손익비 1.5 방어선 로직
    avg_win = win_trades[return_col].mean() if not win_trades.empty else 0
    avg_loss = abs(loss_trades[return_col].mean()) if not loss_trades.empty else 0.0001
    rr_ratio = avg_win / avg_loss

    rr_penalty = 1.0 if rr_ratio >= 1.5 else (rr_ratio / 1.5)

    # 📊 최종 적합도 점수 (수익성 * 안정성 * 승률 * 손익비)
    fitness_score = (profit_factor * avg_return) * win_rate_penalty * rr_penalty

    return float(fitness_score)
