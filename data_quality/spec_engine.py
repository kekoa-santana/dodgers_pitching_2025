from __future__ import annotations
from typing import Callable

import numpy as np
import pandas as pd

from data_quality.statcast_specs import DTYPES, BOUNDS
from utils.statcast_utils import (
    assert_pk_unique,
    map_pitch_result,
    is_whiff, is_called_strike, is_bip, is_swing, is_ball
)

def _coerce_series(s: pd.Series, dtype: str) -> pd.Series:
    if dtype == "int64":
        return pd.to_numeric(s, errors="coerce").astype("Int64")
    if dtype == "float64":
        return pd.to_numeric(s, errors="coerce").astype("float64")
    if dtype == "string":
        return s.astype("string")
    if dtype == "boolean":
        return s.astype("boolean")
    if dtype == "datetime":
        return pd.to_datetime(s, errors="coerce")

    # fallback
    return s.astype(dtype)

def _apply_bounds(df: pd.DataFrame) -> dict[str, int]:
    invalid_counts = {}
    for col, (lo, hi) in BOUNDS.items():
        if col not in df.columns:
            continue
        # only makes sense for numeric
        s = pd.to_numeric(df[col], errors="coerce")
        mask = (s < lo) | (s > hi)
        n = int(mask.sum())
        if n:
            df.loc[mask, col] = np.nan
        invalid_counts[col] = n
    return invalid_counts

def _derive_columns(df: pd.DataFrame) -> None:
    if "description" in df.columns:
        desc = df["description"]
        df["is_whiff"] = desc.map(is_whiff).astype("boolean")
        df['is_bip'] = desc.map(is_bip).astype("boolean")
        df['is_swing'] = desc.map(is_swing).astype('boolean')
        df['is_ball'] = desc.map(is_ball).astype('boolean')
        df['is_called_strike'] = desc.map(is_called_strike).astype('boolean')

def _apply_special_rules(df: pd.DataFrame) -> dict[str, int]:
    invalid = {}

    # ----------------------------------
    #       STRIKE ZONE GEOMETRY
    # ----------------------------------
    if "sz_bot" in df.columns and "sz_top" in df.columns:
        sz_bot = pd.to_numeric(df['sz_bot'], errors="coerce")
        sz_top = pd.to_numeric(df['sz_top'], errors='coerce')

        # impossible inversion
        mask_inv = sz_bot.notna() & sz_top.notna() & (sz_bot > sz_top)
        n_inv = int(mask_inv.sum())
        if n_inv:
            df.loc[mask_inv, ['sz_bot', 'sz_top']] = np.nan
        invalid['sz_inverted'] = n_inv

        # implausible absolute values (broad, low false positives)
        mask_abs = (
            (sz_top.notna() & ((sz_top < 2.0) | (sz_top > 5.5))) |
            (sz_bot.notna() & ((sz_bot < 0.5) | (sz_bot > 3.5)))
        )
        n_abs = int(mask_abs.sum())
        if n_abs:
            df.loc[mask_abs, ['sz_top', 'sz_bot']] = np.nan
        invalid['sz_abs_outliers'] = n_abs

        # implausible zone height
        height = sz_top - sz_bot
        mask_h = height.notna() & ((height < 0.5) | (height > 5))
        n_h = int(mask_h.sum())
        if n_h:
            df.loc[mask_h, ['sz_top', 'sz_bot']] = np.nan
        invalid['sz_height_outliers'] = n_h

    # ----------------------------------
    #       EFFECTIVE SPEED RULE
    # ----------------------------------
    if ('effective_speed' in df.columns) & ('release_speed' in df.columns):
        eff = pd.to_numeric(df['effective_speed'], errors='coerce')
        rel = pd.to_numeric(df['release_speed'], errors = 'coerce')
        delta_speed = eff - rel
        mask_del = eff.notna() & rel.notna() & delta_speed.abs() > 6
        n_del = int(mask_del.sum())
        if n_del:
            df.loc[mask_del, 'effective_speed'] = np.nan
        invalid['effective_speed_invalid'] = n_del
    
    return invalid

def clean_statcast_df(df: pd.DataFrame, pk_cols: list[str]) -> tuple[pd.DataFrame, dict]:
    df = df.copy()

    # Coerce types
    for col, dtype in DTYPES.items():
        if col in df.columns:
            df[col] = _coerce_series(df[col], dtype)
        
    # Apply bounds
    invalid_bounds = _apply_bounds(df)

    # Special rules
    invalid_special = _apply_special_rules(df)

    # Derive columns
    _derive_columns(df)

    df = assert_pk_unique(df, pk_cols)

    report = {
        'rows': len(df),
        'invalid_bounds': invalid_bounds,
        'invalid_special': invalid_special
    }

    return df, report