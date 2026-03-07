from __future__ import annotations

from pathlib import Path
from typing import Optional
import pandas as pd

DEFAULT_OUT_DIR = Path("data/interim/visibility/step4_weekly")

def run_step4_aggregate_weekly(
    island_daily_fp: Path,
    out_dir: Path = DEFAULT_OUT_DIR,
    out_name: Optional[str] = None,
) -> Path:
    """
    Input: island daily with low_vis_confirmed_day / low_vis_possible_day / low_vis_any_day
    Output: weekly island metrics.
    """
    island_daily_fp = Path(island_daily_fp)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(island_daily_fp)
    df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")
    df = df.dropna(subset=["date"]).copy()

    # week_start = Monday 00:00 UTC
    df["week_start"] = df["date"] - pd.to_timedelta(df["date"].dt.weekday, unit="D")
    df["week_start"] = pd.to_datetime(df["week_start"], utc=True)

    weekly = (df.groupby(["island", "week_start"], as_index=False)
                .agg(
                    low_vis_confirmed_days_week=("low_vis_confirmed_day", "sum"),
                    low_vis_confirmed_any_week=("low_vis_confirmed_day", "max"),
                    low_vis_possible_days_week=("low_vis_possible_day", "sum"),
                    low_vis_possible_any_week=("low_vis_possible_day", "max"),
                    low_vis_any_days_week=("low_vis_any_day", "sum"),
                    low_vis_any_week=("low_vis_any_day", "max"),
                    confirmed_airports_max_week=("confirmed_airports", "max"),
                    possible_airports_max_week=("possible_airports", "max"),
                    airports_obs_max_week=("airports_obs", "max"),
                    vis_min_m_week=("vis_min_m", "min"),
                    rh_min_pct_week=("rh_min_pct", "min"),
                )
                .sort_values(["island", "week_start"]))

    if out_name is None:
        island = weekly["island"].iloc[0] if len(weekly) else "island"
        out_name = f"visibility_weekly_{island}.parquet"

    out_fp = out_dir / out_name
    weekly.to_parquet(out_fp, index=False)
    print("Saved island weekly:", out_fp, "rows:", len(weekly))
    return out_fp

