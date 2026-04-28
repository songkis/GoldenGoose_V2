# -*- coding: utf-8 -*-
"""
업계 표준 전진 분석(Walk-Forward Optimization) 파이프라인 [V3.2 라이브 100% 동기화]
- [Live Engine Sync] 변수명/네이밍 컨벤션 라이브와 완전 일치 (volume_surge_threshold 등)
- [1-Shot Strict] 물타기 완전 배제 및 단일 진입 고정
- [Gating & Bypass] VCP, MA50, VWAP 기각 시 주도주(85점 이상) 강제 구제 모사
"""

import os
import json
import sqlite3
import pandas as pd
import numpy as np
import optuna
import shutil
import math
import logging
import warnings

from config.ai_settings import WFO_PARAMETER_GRID
from strategy.core.trading_models import calculate_custom_fitness

warnings.filterwarnings("ignore")

DB_PATH = "C:/GoldenGoose/db/GoldenGoose.sqlite"
CONFIG_PATH = "C:/GoldenGoose/config/ai_parameters.json"
BACKUP_PATH = "C:/GoldenGoose/config/ai_parameters_backup.json"

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("WalkForward")


class WFOQuantSimulator:
    def __init__(self):
        self.slippage = 0.0015
        self.fee_tax = 0.0023

    def calculate_global_indicators(self, df):
        logger.info(
            "전체 데이터셋 기술적 지표 사전 연산 중 (Gating & Pardon Vectorized)..."
        )
        df = df.copy()
        grouped = df.groupby("종목코드")

        df["ma20"] = grouped["종가"].transform(lambda x: x.rolling(20).mean())
        df["ma50"] = grouped["종가"].transform(lambda x: x.rolling(50).mean())
        df["ma60"] = grouped["종가"].transform(lambda x: x.rolling(60).mean())
        df["ma150"] = grouped["종가"].transform(lambda x: x.rolling(150).mean())
        df["ma200"] = grouped["종가"].transform(lambda x: x.rolling(200).mean())

        df["rolling_high"] = grouped["고가"].transform(lambda x: x.rolling(20).max())
        df["rolling_low"] = grouped["저가"].transform(lambda x: x.rolling(20).min())
        df["vcp_ratio"] = (df["rolling_high"] - df["rolling_low"]) / df[
            "rolling_low"
        ].replace(0, np.nan)

        df["prev_close_5d"] = grouped["종가"].shift(5)
        df["stock_ret_5d"] = ((df["종가"] / df["prev_close_5d"]) - 1) * 100

        if "거래대금" not in df.columns:
            df["거래대금"] = df["종가"] * df["거래량"]
        df["amt_ma5"] = grouped["거래대금"].transform(lambda x: x.rolling(5).mean())

        df["vol_ma20"] = grouped["거래량"].transform(lambda x: x.rolling(20).mean())
        df["vol_acc"] = df["거래량"] / df["vol_ma20"].replace(0, 1)

        df["prev_close"] = grouped["종가"].shift(1)
        df["rs_gap"] = ((df["종가"] / df["prev_close"].replace(0, np.nan)) - 1) * 100

        tr1 = df["고가"] - df["저가"]
        tr2 = abs(df["고가"] - df["prev_close"])
        tr3 = abs(df["저가"] - df["prev_close"])
        df["TR"] = np.maximum(tr1, np.maximum(tr2, tr3))
        df["ATR"] = grouped["TR"].transform(lambda x: x.rolling(14).mean())

        df["disparity"] = df["종가"] / df["ma20"].replace(0, np.nan)
        df["pb_proxy"] = (df["rolling_high"] - df["종가"]) / (
            df["rolling_high"] - df["rolling_low"] + 1e-9
        )
        df["is_yin"] = df["종가"] < df["시가"]

        df["max_oc"] = np.maximum(df["시가"], df["종가"])
        df["upper_shadow_ratio"] = (df["고가"] - df["max_oc"]) / (
            df["고가"] - df["저가"] + 1e-9
        )
        df["intraday_drop_pct"] = (
            (df["고가"] - df["종가"]) / (df["고가"] + 1e-9) * 100.0
        )

        return df.dropna()

    def run_simulation(self, df_stock, params, regime_map=None):
        import numpy as np

        df = df_stock.copy()

        if regime_map is not None:
            if len(df) > 0 and hasattr(df["날짜"].iloc[0], "date"):
                df["date_str"] = df["날짜"].dt.date.astype(str)
            else:
                df["date_str"] = df["날짜"].astype(str).str[:10]
            df["market_regime"] = df["date_str"].map(regime_map).fillna("NEUTRAL")
        else:
            df["market_regime"] = "NEUTRAL"

        # [Sync Fix] 라이브와 정확히 일치하는 파라미터명 (volume_surge_threshold)
        pb_thresh = params.get("pb_quality_threshold", 0.5)
        vol_thresh = params.get("volume_surge_threshold", 2.0)
        tick_acc_bonus = params.get("tick_acc_bonus", 15.0)

        df["vol_score"] = np.clip(df["vol_acc"] * 10.0, 0, 40.0)
        df["rs_score"] = np.clip(df["stock_ret_5d"] * 3.0, 0, 60.0)
        df["daily_score"] = df["vol_score"] + df["rs_score"]

        # [Sync Fix] 라이브 True Bounce 동기화 (투매 캔들 회피)
        is_true_bounce = (
            (df["pb_proxy"] >= pb_thresh)
            & (df["vol_acc"] >= vol_thresh)
            & ~df["is_yin"]
            & (df["intraday_drop_pct"] < 2.5)
        )
        is_dump_alert = (
            (df["pb_proxy"] >= pb_thresh) & (df["vol_acc"] >= vol_thresh) & df["is_yin"]
        )
        is_shooting_star = (df["upper_shadow_ratio"] >= 0.6) & (df["vol_acc"] > 1.5)

        df.loc[is_true_bounce, "daily_score"] += 40.0
        df.loc[is_dump_alert, "daily_score"] -= 500.0

        rs_gap_limit = np.where(is_true_bounce, 30.0, 20.0)
        is_overheated = df["rs_gap"] >= rs_gap_limit

        df["is_recovering"] = (df["종가"] < df["ma150"]) & (df["vol_acc"] > 2.5)
        df.loc[df["is_recovering"], "daily_score"] += tick_acc_bonus

        df.loc[df["disparity"] > 1.05, "daily_score"] -= 20.0
        df.loc[
            (df["disparity"] >= 0.98) & (df["disparity"] <= 1.015), "daily_score"
        ] += 35.0
        df.loc[(df["vol_acc"] < 0.8) & (df["pb_proxy"] >= 0.7), "daily_score"] += 40.0

        df["k_multiplier"] = np.where(
            df["market_regime"] == "CRASH",
            1.25,
            np.where(df["market_regime"] == "BULL", 0.85, 1.0),
        )

        df["s_thresh_dyn"] = params.get("s_threshold_normal", 85.0) * df["k_multiplier"]
        df["a_threshold_dyn"] = (
            params.get("a_threshold_normal", 55.0) * df["k_multiplier"]
        )
        df["b_threshold_dyn"] = (
            params.get("b_threshold_floor", 45.0) * df["k_multiplier"]
        )

        cond_s = df["daily_score"] >= df["s_thresh_dyn"]
        cond_a = (df["daily_score"] >= df["a_threshold_dyn"]) & ~cond_s
        cond_b = (df["daily_score"] >= df["b_threshold_dyn"]) & ~cond_s & ~cond_a
        cond_c = ~(cond_s | cond_a | cond_b)

        vcp_thresh = params.get(
            "vcp_contraction_threshold", params.get("max_vcp_ratio", 0.15)
        )
        allow_bear = params.get("allow_bear_market_entry", False)

        vwap_bypass = df["vol_acc"] >= 2.5
        target_buy_price = df["종가"] * (1 + params.get("vwap_margin_pct", 0.0) / 100.0)
        vwap_reject = (df["저가"] > target_buy_price) & ~vwap_bypass

        max_surge = params.get("max_surge_pct", 15.0)
        min_amt_day = params.get("min_amount_threshold", 5_000_000_000)

        df["day_surge"] = (
            (df["종가"] - df["prev_close"]) / df["prev_close"].replace(0, np.nan)
        ) * 100

        reject_mask = (
            ((df["종가"] < df["ma20"]) & (df["ma20"] < df["ma60"]))
            | ((df["종가"] < df["ma50"]) & ~allow_bear)
            | (df["stock_ret_5d"] < 0)
            | (df["vcp_ratio"] > vcp_thresh)
            | (df["amt_ma5"] < min_amt_day)
            | (cond_c)
            | (vwap_reject)
            | (df["day_surge"] >= max_surge)
            | (is_overheated)
            | (is_dump_alert)
            | (is_shooting_star)
        )

        pardon_mask = (cond_s & df["is_recovering"]) | (
            is_true_bounce & (df["daily_score"] >= 65.0)
        )

        fatal_reject = (
            ((df["종가"] < df["ma20"]) & (df["ma20"] < df["ma60"]))
            | (df["stock_ret_5d"] < 0)
            | (df["amt_ma5"] < min_amt_day)
            | (df["종가"] > params.get("max_price_threshold", 1_500_000))
        )
        reject_mask = np.where(pardon_mask & ~fatal_reject, False, reject_mask)
        df["can_buy"] = ~reject_mask

        trades = []
        in_position = False
        locked_sl_line, locked_tp_line = 0.0, 0.0
        entry_price, max_price_since_entry = 0.0, 0.0
        holding_days = 0
        atr_5m_hybrid = 0.0

        dates = df["날짜"].values
        opens = df["시가"].values
        highs = df["고가"].values
        lows = df["저가"].values
        closes = df["종가"].values
        atrs = df["ATR"].values
        can_buys = df["can_buy"].values
        market_regimes = df["market_regime"].values
        cond_s_arr = cond_s.values
        vol_accs = (
            df["vol_acc"].values if "vol_acc" in df.columns else np.zeros(len(df))
        )
        amt_days = (
            df["거래대금"].values if "거래대금" in df.columns else np.zeros(len(df))
        )

        sl_pct_base = params.get("hard_stop_loss_pct", -2.5)
        if sl_pct_base < 0:
            sl_pct_base = abs(sl_pct_base)
        tp_pct_base = params.get("hard_take_profit_pct", 2.5)
        ofi_slope = params.get("ofi_damping_slope", 0.05)
        max_hold_days = params.get("max_holding_days", 3)

        for i in range(len(df)):
            op, hi, lo, cl, atr_val = opens[i], highs[i], lows[i], closes[i], atrs[i]

            if in_position:
                holding_days += 1

                be_trigger = entry_price * 1.015
                be_cut = entry_price * 1.003
                if max_price_since_entry >= be_trigger:
                    locked_sl_line = max(locked_sl_line, be_cut)

                profit_rate = (max_price_since_entry - entry_price) / entry_price
                if profit_rate < 0.02:
                    ts_line = max_price_since_entry * 0.99
                else:
                    profit_pct = profit_rate * 100.0
                    is_ofi_strong = (vol_accs[i] >= 2.0) and cond_s_arr[i]
                    current_slope = ofi_slope if is_ofi_strong else 0.15
                    ts_multiplier = max(1.0, 2.0 - ((profit_pct - 2.0) * current_slope))
                    ts_line = max_price_since_entry - (atr_5m_hybrid * ts_multiplier)

                final_sl_line = max(locked_sl_line, ts_line)

                if holding_days >= max_hold_days:
                    trades.append(
                        (cl * (1 - self.slippage) - entry_price) / entry_price
                        - self.fee_tax
                    )
                    in_position = False
                    continue

                if op >= locked_tp_line:
                    trades.append(
                        (op * (1 - self.slippage) - entry_price) / entry_price
                        - self.fee_tax
                    )
                    in_position = False
                    continue
                if op <= final_sl_line:
                    trades.append(
                        (op * (1 - self.slippage) - entry_price) / entry_price
                        - self.fee_tax
                    )
                    in_position = False
                    continue
                if lo <= final_sl_line:
                    trades.append(
                        (final_sl_line * (1 - self.slippage) - entry_price)
                        / entry_price
                        - self.fee_tax
                    )
                    in_position = False
                elif hi >= locked_tp_line:
                    trades.append(
                        (locked_tp_line * (1 - self.slippage) - entry_price)
                        / entry_price
                        - self.fee_tax
                    )
                    in_position = False
                else:
                    if hi > max_price_since_entry:
                        max_price_since_entry = hi

            if not in_position and can_buys[i]:
                amt_day = amt_days[i]
                dynamic_slippage = self.slippage
                if amt_day > 0:
                    dynamic_slippage = np.where(
                        amt_day >= 30000000000,
                        0.001,
                        np.where(amt_day <= 10000000000, 0.005, 0.003),
                    )
                dynamic_slippage = float(dynamic_slippage)

                entry_price = cl * (1 + dynamic_slippage)
                max_price_since_entry = entry_price
                in_position = True
                holding_days = 0

                day_range = hi - lo
                intra_proxy = min(day_range * 0.5, atr_val * 0.3)
                atr_5m_hybrid = (atr_val * 0.8 + intra_proxy * 0.2) / math.sqrt(78)

                if cond_s_arr[i]:
                    raw_tp, raw_sl = (
                        params.get("tp_factor_s", 3.0),
                        params.get("sl_factor_s", 2.5),
                    )
                else:
                    raw_tp, raw_sl = (
                        params.get("tp_factor_a", 4.0),
                        params.get("sl_factor_a", 1.7),
                    )

                sl_f = raw_sl
                tp_f = raw_tp

                sl_pct_val, tp_pct_val = sl_pct_base, tp_pct_base
                m_regime = market_regimes[i]
                if m_regime == "CRASH":
                    sl_pct_val *= 0.5
                    tp_pct_val *= 0.7
                elif m_regime == "BULL":
                    if cond_s_arr[i]:
                        tp_pct_val = 9999.0
                    else:
                        tp_pct_val *= 1.5

                sl_pct = entry_price * (1 + sl_pct_val / 100)
                tp_pct = entry_price * (1 + tp_pct_val / 100)
                sl_atr = entry_price - (atr_5m_hybrid * sl_f)
                tp_atr = entry_price + (atr_5m_hybrid * tp_f)

                locked_sl_line = max(sl_pct, sl_atr)
                locked_tp_line = min(tp_pct, tp_atr)

        return trades


def load_data_and_prepare():
    logger.info("DB 데이터 로드 시작...")
    conn = sqlite3.connect(DB_PATH)
    query = "SELECT 날짜, 종목코드, 시가, 고가, 저가, 종가, 누적거래량, 거래대금 FROM 일별주가 WHERE 날짜 >= '2023-01-01'"
    try:
        df = pd.read_sql_query(query, conn)
    except Exception:
        query = "SELECT 날짜, 종목코드, 시가, 고가, 저가, 종가, 누적거래량 FROM 일별주가 WHERE 날짜 >= '2023-01-01'"
        df = pd.read_sql_query(query, conn)
    conn.close()

    df["날짜"] = pd.to_datetime(df["날짜"])
    df = df.sort_values(["종목코드", "날짜"]).reset_index(drop=True)
    df["거래량"] = df.groupby("종목코드")["누적거래량"].diff().fillna(df["누적거래량"])
    df["거래량"] = np.where(df["거래량"] < 0, df["누적거래량"], df["거래량"])
    df = df[df["거래량"] > 0]

    simulator = WFOQuantSimulator()
    df = simulator.calculate_global_indicators(df)

    logger.info("시장 지수(001) 데이터 로드 및 국면 분석 중...")
    conn = sqlite3.connect(DB_PATH)
    idx_df = pd.read_sql_query(
        "SELECT 날짜, 종가 FROM 일별주가 WHERE 종목코드 = '001' ORDER BY 날짜", conn
    )
    conn.close()

    idx_df["날짜"] = pd.to_datetime(idx_df["날짜"])
    idx_df["ma20"] = idx_df["종가"].rolling(20).mean()

    regime_map = {}
    for r in idx_df.itertuples():
        dt_str = str(r.날짜.date())
        if r.종가 > r.ma20:
            regime_map[dt_str] = "BULL"
        elif r.종가 < r.ma20 * 0.97:
            regime_map[dt_str] = "CRASH"
        else:
            regime_map[dt_str] = "NEUTRAL"

    return df, regime_map


def objective(trial, df_grouped):
    # [Sync Fix] WFO_PARAMETER_GRID 변수명 라이브 환경과 일치화
    params = {
        "tick_acc_min": trial.suggest_categorical(
            "tick_acc_min", WFO_PARAMETER_GRID["tick_acc_min"]
        ),
        "tick_acc_bonus": trial.suggest_categorical(
            "tick_acc_bonus", WFO_PARAMETER_GRID["tick_acc_bonus"]
        ),
        "max_vcp_ratio": trial.suggest_categorical(
            "max_vcp_ratio", WFO_PARAMETER_GRID["max_vcp_ratio"]
        ),
        "hard_take_profit_pct": trial.suggest_categorical(
            "hard_take_profit_pct", WFO_PARAMETER_GRID["hard_tp_pct"]
        ),
        "hard_stop_loss_pct": trial.suggest_categorical(
            "hard_stop_loss_pct", WFO_PARAMETER_GRID["hard_sl_pct"]
        ),
        "b_threshold_floor": trial.suggest_int("b_threshold_floor", 40, 50, step=5),
        "a_threshold_normal": trial.suggest_int("a_threshold_normal", 50, 65, step=5),
        "s_threshold_normal": trial.suggest_int("s_threshold_normal", 80, 95, step=5),
        "trailing_stop_pct": trial.suggest_float(
            "trailing_stop_pct", 3.0, 10.0, step=1.0
        ),
        "tp_factor_s": trial.suggest_float("tp_factor_s", 2.0, 4.0, step=0.5),
        "sl_factor_s": trial.suggest_float("sl_factor_s", 2.0, 3.5, step=0.5),
        "tp_factor_a": trial.suggest_float("tp_factor_a", 3.0, 5.0, step=0.5),
        "sl_factor_a": trial.suggest_float("sl_factor_a", 1.5, 2.5, step=0.5),
        "volume_multiplier_threshold": trial.suggest_categorical(
            "volume_multiplier_threshold",
            WFO_PARAMETER_GRID["volume_multiplier_threshold"],
        ),
        "ai_prob_threshold": trial.suggest_float(
            "ai_prob_threshold", 0.2, 0.5, step=0.05
        ),
        "allow_bear_market_entry": trial.suggest_categorical(
            "allow_bear_market_entry", [True, False]
        ),
        "bear_market_weight_penalty": trial.suggest_float(
            "bear_market_weight_penalty", 0.5, 1.0, step=0.1
        ),
        "vwap_margin_pct": trial.suggest_float("vwap_margin_pct", -1.0, 1.0, step=0.5),
        "max_holding_days": trial.suggest_int("max_holding_days", 2, 7),
        "pb_quality_threshold": trial.suggest_categorical(
            "pb_quality_threshold", WFO_PARAMETER_GRID["pb_quality_threshold"]
        ),
        # [Sync Fix] 라이브 환경 파라미터명 준수
        "volume_surge_threshold": trial.suggest_categorical(
            "volume_surge_threshold", WFO_PARAMETER_GRID["vol_surge_threshold"]
        ),
        "min_amount_threshold": trial.suggest_categorical(
            "min_amount_threshold", WFO_PARAMETER_GRID["min_amount_threshold"]
        ),
        "ofi_damping_slope": trial.suggest_categorical(
            "ofi_damping_slope", WFO_PARAMETER_GRID["ofi_damping_slope"]
        ),
    }

    params["vcp_contraction_threshold"] = params["max_vcp_ratio"]

    simulator = WFOQuantSimulator()
    all_trades = []
    regime_map = df_grouped[0][1].get("regime_map_ref", {})

    for ticker, stock_df in df_grouped:
        if len(stock_df) < 20:
            continue
        all_trades.extend(
            simulator.run_simulation(stock_df, params, regime_map=regime_map)
        )

    if not all_trades:
        return -9999.0

    trades_df = pd.DataFrame(all_trades, columns=["수익률"])
    return calculate_custom_fitness(trades_df, min_trades_required=15)


def save_production_params(best_params):
    try:
        if os.path.exists(CONFIG_PATH):
            shutil.copy(CONFIG_PATH, BACKUP_PATH)
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                params_data = json.load(f)
        else:
            params_data = {}

        for k, v in best_params.items():
            params_data[k] = round(v, 4) if isinstance(v, float) else v

        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(params_data, f, indent=4, ensure_ascii=False)
        logger.info(
            f"🏆 라이브 시스템(Pardon & 1-Shot) 파라미터 갱신 완료: {CONFIG_PATH}"
        )
    except Exception as e:
        logger.error(f"파라미터 갱신 실패: {e}")


def execute_auto_optimization_loop(df_global, global_regime_map, unique_dates):
    TRAIN_DAYS = 120
    logger.info("🚀 [Auto-Mutation] 파라미터 자가 수정 및 수렴 엔진 가동")

    prod_train_start = unique_dates[-TRAIN_DAYS]
    df_prod = df_global[df_global["날짜"] >= prod_train_start]

    def prepare_grouped_local(df, r_map):
        groups = []
        for ticker, group in df.groupby("종목코드"):
            group.regime_map_ref = r_map
            groups.append((ticker, group))
        return groups

    df_prod_grouped = prepare_grouped_local(df_prod, global_regime_map)

    prod_sampler = optuna.samplers.TPESampler(seed=42)
    prod_study = optuna.create_study(direction="maximize", sampler=prod_sampler)
    prod_study.optimize(
        lambda trial: objective(trial, df_prod_grouped), n_trials=30, n_jobs=4
    )

    current_params = prod_study.best_params.copy()

    max_iterations = 5
    for iteration in range(1, max_iterations + 1):
        logger.info(
            f"✨ [WFO Iteration {iteration}/{max_iterations}] 파라미터 검증 및 변이 개시"
        )

        simulator = WFOQuantSimulator()
        all_trades = []
        for ticker, stock_df in df_prod_grouped:
            all_trades.extend(
                simulator.run_simulation(
                    stock_df, current_params, regime_map=global_regime_map
                )
            )

        if not all_trades:
            logger.warning(
                "⚠️ No trades generated. Adjusting parameters for more candidates..."
            )
            current_params["s_threshold_normal"] = max(
                70, current_params.get("s_threshold_normal", 85) - 5
            )
            continue

        trades_df = pd.DataFrame(all_trades, columns=["수익률"])
        win_trades = trades_df[trades_df["수익률"] > 0]
        loss_trades = trades_df[trades_df["수익률"] <= 0]

        n_trades = len(trades_df)
        win_rate = len(win_trades) / n_trades if n_trades > 0 else 0
        ev = trades_df["수익률"].mean()

        gross_profit = win_trades["수익률"].sum()
        gross_loss = abs(loss_trades["수익률"].sum())
        pf = (gross_profit / gross_loss) if gross_loss > 0 else 9.9

        logger.info(
            f"📊 [Result] Iter:{iteration} | PF:{pf:.2f} | EV:{ev * 100:.2f}% | N:{n_trades} | W:{win_rate * 100:.1f}%"
        )

        if pf >= 1.5 and ev >= 0.02:
            logger.info("🎯 [Succeed] Target metrics achieved. Converged.")
            break

        if pf < 1.5:
            current_params["ofi_toxic_thresh"] = (
                current_params.get("ofi_toxic_thresh", -40.0) - 10.0
            )
            logger.info(
                f"🔧 [Mutation] Low PF detected. Hardening ofi_toxic_thresh to {current_params['ofi_toxic_thresh']}"
            )

        if ev < 0.02:
            current_params["vwap_prox_pct"] = max(
                0.1, current_params.get("vwap_prox_pct", 0.5) - 0.1
            )
            logger.info(
                f"🔧 [Mutation] Low EV detected. Tightening vwap_prox_pct to {current_params['vwap_prox_pct']}"
            )

        if win_rate < 0.45:
            current_params["s_threshold_normal"] = min(
                95, current_params.get("s_threshold_normal", 85) + 2
            )
            logger.info(
                f"🔧 [Mutation] Low Win-Rate. Raising s_threshold_normal to {current_params['s_threshold_normal']}"
            )

    save_production_params(current_params)
    return current_params


if __name__ == "__main__":

    def prepare_grouped(df, r_map):
        groups = []
        for ticker, group in df.groupby("종목코드"):
            group.regime_map_ref = r_map
            groups.append((ticker, group))
        return groups

    df_global, global_regime_map = load_data_and_prepare()
    unique_dates = sorted(df_global["날짜"].unique())

    TRAIN_DAYS = 120
    TEST_DAYS = 20

    if len(unique_dates) < TRAIN_DAYS + TEST_DAYS:
        logger.error("데이터 부족")
        exit()

    total_oos_trades = []
    num_windows_to_test = 3
    start_idx = len(unique_dates) - (TEST_DAYS * num_windows_to_test) - TRAIN_DAYS
    start_idx = max(0, start_idx)

    logger.info("=" * 60)
    logger.info(f"🔥 [WFO 백테스트: V3.2 Sync 버전] True Out-of-Sample 검증 시작")

    idx = start_idx
    while idx + TRAIN_DAYS + TEST_DAYS <= len(unique_dates):
        train_start = unique_dates[idx]
        train_end = unique_dates[idx + TRAIN_DAYS - 1]
        test_start = unique_dates[idx + TRAIN_DAYS]
        test_end = unique_dates[idx + TRAIN_DAYS + TEST_DAYS - 1]

        logger.info(
            f"👉 Window [Train: {train_start.date()}~{train_end.date()}] -> [Test: {test_start.date()}~{test_end.date()}]"
        )

        df_train = df_global[
            (df_global["날짜"] >= train_start) & (df_global["날짜"] <= train_end)
        ]
        df_test = df_global[
            (df_global["날짜"] >= test_start) & (df_global["날짜"] <= test_end)
        ]

        df_train_grouped = prepare_grouped(df_train, global_regime_map)
        df_test_grouped = prepare_grouped(df_test, global_regime_map)

        optuna.logging.set_verbosity(optuna.logging.WARNING)

        sampler = optuna.samplers.TPESampler(seed=42)
        study = optuna.create_study(direction="maximize", sampler=sampler)
        study.optimize(
            lambda trial: objective(trial, df_train_grouped), n_trials=30, n_jobs=4
        )

        simulator = WFOQuantSimulator()
        window_oos_trades = []

        oos_params = study.best_params.copy()
        if (
            "max_vcp_ratio" in oos_params
            and "vcp_contraction_threshold" not in oos_params
        ):
            oos_params["vcp_contraction_threshold"] = oos_params["max_vcp_ratio"]

        for ticker, stock_df in df_test_grouped:
            window_oos_trades.extend(
                simulator.run_simulation(
                    stock_df, oos_params, regime_map=global_regime_map
                )
            )

        total_oos_trades.extend(window_oos_trades)
        w_wr = (
            sum(1 for t in window_oos_trades if t > 0) / len(window_oos_trades)
            if window_oos_trades
            else 0
        )
        w_ev = np.mean(window_oos_trades) if window_oos_trades else 0
        logger.info(
            f"   [결과] OOS 거래수: {len(window_oos_trades)} | 승률: {w_wr * 100:.1f}% | 기댓값: {w_ev * 100:.2f}%"
        )

        idx += TEST_DAYS

    final_wr = (
        sum(1 for t in total_oos_trades if t > 0) / len(total_oos_trades)
        if total_oos_trades
        else 0
    )
    final_ev = np.mean(total_oos_trades) if total_oos_trades else 0

    print("\n" + "=" * 60)
    print("📊 [퀀트 표준] 전진 분석(WFO) 통합 검증 리포트 (Pardon Sync)")
    print("=" * 60)
    print(
        f"🔸 [True Out-of-Sample] 총 거래수: {len(total_oos_trades)} | 1-Shot 실전 승률: {final_wr * 100:.2f}% | 1회 평균수익: {final_ev * 100:.2f}%"
    )
    print("=" * 60)

    logger.info(
        "🔥 [Production Mode] 자가 수정 엔진(Auto-Mutation)을 통한 최종 수렴 파라미터 추출 개시"
    )
    final_params = execute_auto_optimization_loop(
        df_global, global_regime_map, unique_dates
    )

    logger.info("✅ WFO 파이프라인 전체 프로세스 완료.")
