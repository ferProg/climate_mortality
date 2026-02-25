# src/02_calima/step4_aggregate_weekly_province.py
from __future__ import annotations
import pandas as pd
import numpy as np
from pathlib import Path
from src.calima.calima_config import ROOT

IN_FP = ROOT / "data" / "processed" / "dust_days_sc_province_2016_2024.parquet"
OUT_DIR = ROOT / "data" / "processed"
OUT_DIR.mkdir(parents=True, exist_ok=True)

def week_start_monday(date_series: pd.Series) -> pd.Series:
    d = pd.to_datetime(date_series)
    return (d - pd.to_timedelta(d.dt.weekday, unit="D")).dt.normalize()

def level_from_days(x: pd.Series) -> pd.Series:
    x = x.fillna(0).astype(int)
    return pd.cut(x, bins=[-1, 0, 1, 3, 7], labels=[0, 1, 2, 3]).astype(int)

def main():
    df = pd.read_parquet(IN_FP).copy()
    df["date_utc"] = pd.to_datetime(df["date_utc"]).dt.normalize()
    df["week_start"] = week_start_monday(df["date_utc"])

    df["dust_any_day_sc"] = df["dust_day_sc"].astype(int)

    g = df.groupby("week_start", as_index=False)
    weekly = g.agg(
        dust_days_week_sc=("dust_any_day_sc", "sum"),
        dust_any_week_sc=("dust_any_day_sc", "max"),
        days_observed_week_sc=("date_utc", "count"),
        # optional: how many airports were dust-like (mean across days)
        dust_like_count_mean_week_sc=("dust_like_count_sc", "mean"),
    )
    weekly["dust_level_week_sc"] = level_from_days(weekly["dust_days_week_sc"])

    out = OUT_DIR / "dust_weekly_sc_province_2016_2024.parquet"
    weekly.to_parquet(out, index=False)
    print("Saved:", out)

if __name__ == "__main__":
    main()