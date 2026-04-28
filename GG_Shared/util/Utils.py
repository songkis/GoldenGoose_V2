# -*- coding: utf-8 -*-
from .CommUtils import *
import math
import numpy as np

def sanitize_dict(d):
    """
    [Absolute Defense] Recursive cleaner to ensure JSON/DB compatibility.
    Handles numpy/pandas objects by string-based type detection.
    """
    if d is None:
        return 0.0

    type_str = str(type(d))

    # 1. Dict
    if isinstance(d, dict):
        return {str(k): sanitize_dict(v) for k, v in d.items()}

    # 2. List/Tuple/Set (excluding Pandas)
    if isinstance(d, (list, tuple, set, np.ndarray)) and "pandas" not in type_str:
        if hasattr(d, "tolist"):
            return [sanitize_dict(v) for v in d.tolist()]
        return [sanitize_dict(v) for v in d]

    # 3. Pandas Specifically (Series/DataFrame/Index)
    if "pandas" in type_str or "Series" in type_str or "DataFrame" in type_str:
        if ("DataFrame" in type_str) and hasattr(d, "to_dict"):
            return [sanitize_dict(v) for v in d.to_dict(orient="records")]
        elif hasattr(d, "tolist"):
            res = d.tolist()
            if isinstance(res, list) and len(res) == 1:
                return sanitize_dict(res[0])
            return [sanitize_dict(v) for v in res]
        elif hasattr(d, "iloc") and hasattr(d, "__len__") and len(d) == 1:
            return sanitize_dict(d.iloc[0])

    # 4. Standard types + Numpy Scalars
    if "int" in type_str.lower():
        return int(d)
    if "float" in type_str.lower():
        val = float(d)
        if math.isnan(val) or math.isinf(val):
            return 0.0
        return val
    if "bool" in type_str.lower():
        return bool(d)
    if "Timestamp" in type_str or "datetime" in type_str:
        return str(d)

    if isinstance(d, (int, float, str, bool)):
        return d

    from dataclasses import is_dataclass, asdict

    if is_dataclass(d):
        # [Safe Receiver] Convert dataclass to dict for ZMQ/JSON compatibility
        dict_val = asdict(d)
        return {str(k): sanitize_dict(v) for k, v in dict_val.items()}

    return str(d)

def align_quote_price(price, market_type=1):
    """
    [V10.0] KRX Hoga Tick Alignment
    Aligns price to standard KRX tick size to prevent order rejection (ord_no missing).
    """
    price = float(price)
    if price < 2000:
        tick = 1
    elif price < 5000:
        tick = 5
    elif price < 10000:
        tick = 10
    elif price < 20000:
        tick = 50
    elif price < 50000:
        tick = 100
    elif price < 200000:
        tick = 500
    else:
        tick = 1000

    aligned = round(price / tick) * tick
    return int(aligned)

def is_market_open():
    """
    한국 주식 시장 운영 시간(09:00 ~ 15:30) 체크 (월~금, 공휴일 제외)
    """
    import datetime
    from config.system_params import KRX_HOLIDAYS

    now = datetime.datetime.now()
    today_str = now.strftime("%Y-%m-%d")

    # 1. 주말 체크
    if now.weekday() >= 5:
        return False

    # 2. 공휴일 체크 (KRX_HOLIDAYS)
    if today_str in KRX_HOLIDAYS:
        return False

    # 3. 운영 시간 체크
    current_time = now.time()
    start_time = datetime.time(9, 0, 0)
    end_time = datetime.time(15, 30, 0)
    return start_time <= current_time <= end_time

_global_regime = "NEUTRAL"

def get_global_regime():
    """전역 시장 국면 반환"""
    return _global_regime

def set_global_regime(val):
    """전역 시장 국면 갱신"""
    global _global_regime
    _global_regime = val
