# src/02_calima/step4_aggregate_weekly.py

from __future__ import annotations

from pathlib import Path
import pandas as pd
import numpy as np


def find_project_root(start: Path) -> Path:
    p = start.resolve()
    while p != p.parent:
        if (p / "data").exists() and (p / "src").exists():
            return p
        p = p.parent
    raise RuntimeError("Could not find project root (folder with both 'data' and 'src').")


ROOT = find_project_root(Path.cwd())

IN_FP = ROOT / "data" / "processed" / "dust_days_tfe_tfs_tfn_2016_2024.parquet"
OUT_DIR = ROOT / "data" / "processed"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def week_start_monday(date_series: pd.Series) -> pd.Series:
    d = pd.to_datetime(date_series)
    return (d - pd.to_timedelta(d.dt.weekday, unit="D")).dt.normalize()


def dust_level_from_days(dust_days_week: pd.Series) -> pd.Series:
    """
    Provisional severity bins based on number of dust-days in the week.
    0: none
    1: 1 day
    2: 2–3 days
    3: 4–7 days
    """
    x = dust_days_week.fillna(0).astype(int)
    return pd.cut(
        x,
        bins=[-1, 0, 1, 3, 7],
        labels=[0, 1, 2, 3],
    ).astype(int)


def main():
    if not IN_FP.exists():
        raise FileNotFoundError(f"Missing input: {IN_FP}")

    df = pd.read_parquet(IN_FP).copy()
    df["date_utc"] = pd.to_datetime(df["date_utc"]).dt.normalize()

    # Week start (Monday, ISO-like)
    df["week_start"] = week_start_monday(df["date_utc"])

    # Convenience flags
    df["dust_any_day"] = df["dust_day_tfe_tfs_tfn"].astype(int)

    # Metrics restricted to dust-days (else NaN) so weekly agg min/mean reflect dust conditions
    df["vis_min_day_m"] = df[["vis_m_gcxo", "vis_m_gcts"]].min(axis=1)
    df["rh_max_day_pct"] = df[["rh_pct_gcxo", "rh_pct_gcts"]].max(axis=1)

    df["vis_min_day_m_if_dust"] = df["vis_min_day_m"].where(df["dust_any_day"] == 1, np.nan)
    df["rh_max_day_pct_if_dust"] = df["rh_max_day_pct"].where(df["dust_any_day"] == 1, np.nan)

    # Weekly aggregation
    g = df.groupby("week_start", as_index=False)

    weekly = g.agg(
        dust_days_week=("dust_any_day", "sum"),
        dust_any_week=("dust_any_day", "max"),
        # station-level dust-like counts (how many days each station was dust-like)
        dust_like_days_gcxo=("dust_like_gcxo", "sum"),
        dust_like_days_gcts=("dust_like_gcts", "sum"),
        # dust-day vis / RH summaries (only over dust-days)
        dust_vis_min_week_m=("vis_min_day_m_if_dust", "min"),
        dust_vis_mean_week_m=("vis_min_day_m_if_dust", "mean"),
        dust_rh_max_mean_week_pct=("rh_max_day_pct_if_dust", "mean"),
        # coverage sanity: how many days in that week exist in dataset (should be 7 mostly)
        days_observed_week=("date_utc", "count"),
    )

    weekly["dust_level_week"] = dust_level_from_days(weekly["dust_days_week"]).astype(int)

    # Basic checks
    print("Weekly rows:", len(weekly))
    print("Weeks with <7 observed days:", (weekly["days_observed_week"] < 7).sum())
    print("\nDust_any_week value counts:")
    print(weekly["dust_any_week"].value_counts().sort_index())
    print("\nDust_level_week value counts:")
    print(weekly["dust_level_week"].value_counts().sort_index())

    # Save
    out_parq = OUT_DIR / "dust_weekly_tfe_tfs_tfn_2016_2024.parquet"
    weekly.sort_values("week_start").to_parquet(out_parq, index=False)
    print("\nSaved:", out_parq)

    out_csv = OUT_DIR / "dust_weekly_tfe_tfs_tfn_2016_2024_min.csv"
    keep = [
        "week_start",
        "dust_any_week",
        "dust_days_week",
        "dust_level_week",
        "dust_vis_min_week_m",
        "dust_vis_mean_week_m",
        "dust_like_days_gcxo",
        "dust_like_days_gcts",
        "days_observed_week",
    ]
    weekly.sort_values("week_start")[keep].to_csv(out_csv, index=False)
    print("Saved:", out_csv)


if __name__ == "__main__":
    main()
