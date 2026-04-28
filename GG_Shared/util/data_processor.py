# -*- coding: utf-8 -*-
import math
import struct
import logging
import numpy as np
import pandas as pd
from typing import Any, Optional, Tuple

logger = logging.getLogger(__name__)

def deep_get(obj, key, default=None):
    """
    [Zero-Defect Utility] 중첩된 딕셔너리 또는 객체에서 안전하게 값을 추출합니다.
    """
    if obj is None:
        return default

    if isinstance(obj, dict):
        return obj.get(key, default)

    return getattr(obj, key, default)

def safe_float(val, default=0.0):
    try:
        if val is None or (isinstance(val, str) and val.strip() == ""):
            return default
        return float(val)
    except Exception:
        return default

def to_numeric_safe(value):
    """
    다양한 데이터 타입(숫자, 텍스트, numpy, bytes)을 안전하게 숫자로 변환하는 함수
    """
    # 1. 파이썬 기본 숫자(int, float) 또는 넘파이 숫자
    if isinstance(value, (int, float, np.integer, np.floating)):
        try:
            val = float(value)
            if math.isnan(val) or math.isinf(val):
                return 0.0
            return round(val, 2)
        except (ValueError, TypeError):
            return 0.0

    # 2. 이진 데이터(bytes)
    if isinstance(value, bytes):
        try:
            return float(value.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            try:
                if len(value) == 8:
                    return struct.unpack("<q", value)[0]
            except struct.error:
                return 0.0
        return 0.0

    # 3. 문자열(string)
    if isinstance(value, str):
        try:
            return float(value.replace(",", ""))  # "1,234" 같은 경우 처리
        except ValueError:
            return 0.0

    # 4. 처리 불가능 → 0 반환
    return 0.0

def transform_to_daily_volume(df: pd.DataFrame) -> pd.DataFrame:
    """
    [V4.6.1 Adaptive Integrity]
    누적거래량 컬럼 존재 시 일일 거래량으로 변환, 미존재 시 기존 거래량 유효성 검사
    """
    # 1. 컬럼 존재 여부 체크 (방어적 설계)
    if "누적거래량" in df.columns:
        # 기존 로직 수행
        df["거래량"] = df["누적거래량"].diff()

        # 첫 번째 행 처리
        if len(df) > 0:
            df.loc[df.index[0], "거래량"] = df.iloc[0]["누적거래량"]

        # [Critical] 음수/리셋 구간 처리 - 승률 방어의 핵심
        # 누적이 전일보다 적다면, 데이터 리셋으로 판단하여 당일 수치 채택
        reset_mask = df["거래량"] <= 0
        df.loc[reset_mask, "거래량"] = df.loc[reset_mask, "누적거래량"]

        # 로그 기록 (데이터 보정 발생 시)
        if reset_mask.any():
            if logger:
                logger.debug(
                    f"[Data Integrity] {reset_mask.sum()}개 지점에서 거래량 리셋 보정 완료."
                )

    elif "거래량" in df.columns:
        # 누적거래량은 없지만 거래량 컬럼이 있는 경우, 기존 데이터 그대로 사용 (시스템 연속성)
        pass
    else:
        # 두 컬럼 모두 없을 경우 에러 대신 빈 값 처리하여 시스템 중단 방지
        df["거래량"] = 0.0
        if logger:
            logger.info("[Critical Warning] 거래량 관련 데이터가 전무합니다.")

    return df

def prepare_minute_df(db_rows):
    """
    DB에서 가져온 행 데이터를 분석용 DataFrame으로 변환
    """
    df = pd.DataFrame(db_rows)

    if df.empty:
        return df

    # [Zero-Defect Fix] 'RangeIndex' object has no attribute 'date' 오류 방지를 위해 인덱스 및 타입 강제
    if not df.empty and "등록일시" in df.columns:
        df["등록일시"] = pd.to_datetime(df["등록일시"], errors="coerce")
        df.set_index("등록일시", inplace=True)
    else:
        # 데이터가 비어있어도 인덱스 타입을 DatetimeIndex로 초기화하여 런타임 에러 방어
        df.index = pd.to_datetime(df.index)

    # 3. 데이터 타입 변환 (콤마 제거 및 숫자형 변환)
    # 종가, 시가 등에 콤마(,)가 있다면 제거 후 float 변환
    cols_to_fix = ["종가", "시가", "고가", "저가", "거래량", "체결량"]
    for col in cols_to_fix:
        if col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].str.replace(",", "").astype(float)
            else:
                try:
                    df[col] = df[col].astype(float)
                except Exception:
                    pass

    return df

def make_json_safe(obj):
    """
    NumPy 타입을 포함한 모든 비표준 객체를 JSON 직렬화 가능 타입으로 변환
    """
    # 1. 기본 타입 처리
    if obj is None:
        return None

    # 2. 딕셔너리/리스트 재귀 처리
    if isinstance(obj, dict):
        return {str(k): make_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [make_json_safe(i) for i in obj]

    # 3. NumPy/Pandas 특수 타입 처리
    cls_name = obj.__class__.__name__.lower()
    if "bool" in cls_name:
        return bool(obj)
    if "float" in cls_name:
        return float(obj)
    if "int" in cls_name:
        return int(obj)
    if isinstance(obj, np.ndarray):
        return make_json_safe(obj.tolist())

    # 4. 기타 객체 (문자열 변환)
    return obj

def sanitize(value):
    if hasattr(value, "item"):  # NumPy 타입인 경우
        return value.item()
    return value

def sanitize_dict(key, obj):
    if key in obj:
        eval_data = obj[key]
        #  특정 키는 수치 변환에서 제외 (문자열 유지)
        exclude_keys = [
            "final_grade",
            "grade",
            "final_reason",
            "reason",
            "msg",
            "message",
            "is_buy",
            "energy_status",
            "is_energy_dryup",
            "volume_dry",
            "status",
            "trigger_hit",
            "breakout_info",
            "final_can_buy",
            "is_acc_sync",
            "is_limit_up_mode",
            "is_high_vol_warning",
            "audit_report",
            "vol_adjusted_sl",
            "special_grade",
        ]
        for k, v in eval_data.items():
            if k in exclude_keys:
                continue
            if hasattr(v, "item"):  # NumPy 타입인 경우
                eval_data[k] = v.item()
            elif isinstance(v, (float, int, object)):  # 수치형 데이터 대상
                eval_data[k] = to_numeric_safe(v)

def safe_get(obj, key, default=None):
    if obj is None:
        return default
    if hasattr(obj, key):
        return getattr(obj, key)
    try:
        if hasattr(obj, "get"):
            return obj.get(key, default)
    except Exception:
        pass
    try:
        return obj[key]
    except (KeyError, TypeError, IndexError, AttributeError):
        return default

def safe_set(obj, key, val):
    if obj is None:
        return
    # 데이터 클래스 또는 일반 객체인 경우
    if hasattr(obj, "__dataclass_fields__") or not isinstance(obj, dict):
        try:
            setattr(obj, key, val)
            return
        except (AttributeError, TypeError):
            pass
    # 딕셔너리 또는 인덱스 접근 가능한 객체인 경우
    try:
        obj[key] = val
    except (TypeError, KeyError, AttributeError):
        pass

def safe_update(obj, data: dict):
    if obj is None or data is None:
        return
    for k, v in data.items():
        safe_set(obj, k, v)

def get_first_tuple(
    df: pd.DataFrame,
    key_col: str,
    key_value: Any,
    target_cols: list[str],
) -> Optional[Tuple[Any, ...]]:
    try:
        subset = df.loc[df[key_col] == key_value, target_cols]
        if subset.empty:
            return None
        return tuple(subset.iloc[0])
    except Exception:
        return None
