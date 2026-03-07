from __future__ import annotations
import pandas as pd

def assert_required_cols(df: pd.DataFrame, cols: list[str], label: str = "") -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise KeyError(f"{label} missing required cols: {missing}. Columns={list(df.columns)}")

def assert_no_duplicates(df: pd.DataFrame, keys: list[str], label: str = "") -> None:
    if df.duplicated(subset=keys).any():
        dup = df[df.duplicated(subset=keys, keep=False)].sort_values(keys).head(20)
        raise ValueError(f"{label} duplicated rows on {keys}. Examples:\n{dup}")

def assert_week_monday(df: pd.DataFrame, week_col: str = "week_start", label: str = "") -> None:
    if (df[week_col].dt.weekday != 0).any():
        bad = df.loc[df[week_col].dt.weekday != 0, [week_col]].head(10)
        raise ValueError(f"{label} {week_col} contains non-Mondays. Examples:\n{bad}")

def assert_coverage_0_1(df: pd.DataFrame, col: str = "coverage", label: str = "") -> None:
    if col not in df.columns:
        return
    bad = df[(df[col] < 0) | (df[col] > 1)]
    if len(bad):
        raise ValueError(f"{label} {col} outside [0,1]. Examples:\n{bad.head(10)}")