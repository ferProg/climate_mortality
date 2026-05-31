# audit_visibility.py
# Audits visibility weekly parquets for all islands, both periods.
#
# Key columns (from step4_aggregate_weekly_island.py):
#   vis_min_m_week, low_vis_confirmed_days_week, low_vis_any_days_week,
#   low_vis_confirmed_any_week, low_vis_any_week, rh_min_pct_week

import sys
import pandas as pd
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(r"C:/Users/fdora/RA_Career/Projects/climate_mortality")
PROCESSED    = PROJECT_ROOT / "data" / "processed"
LOG_DIR      = PROJECT_ROOT / "logs" / "audit_2004_2015"
LOG_DIR.mkdir(parents=True, exist_ok=True)

VIS_COLS = [
    "vis_min_m_week",
    "low_vis_confirmed_days_week",
    "low_vis_any_days_week",
    "low_vis_confirmed_any_week",
    "low_vis_any_week",
    "rh_min_pct_week",
]

ISLANDS = {
    "gomera":        "gom",
    "tenerife":      "tfe",
    "gran_canaria":  "gcan",
    "lanzarote":     "lzt",
    "fuerteventura": "ftv",
    "la_palma":      "lpa",
    "hierro":        "hie",
}

PERIODS = [
    ("2004-2015", "2004-01-05", "2015-12-28"),
    ("2016-2025", "2016-01-04", "2025-12-29"),
]


def audit_df(df: pd.DataFrame, exp_start: str, exp_end: str) -> None:
    df = df.copy()
    df["week_start"] = pd.to_datetime(df["week_start"], utc=True, errors="coerce").dt.tz_localize(None)

    print(f"  Shape     : {df.shape}")
    print(f"  Date range: {df['week_start'].min().date()} -> {df['week_start'].max().date()}")
    print(f"  Columns   : {df.columns.tolist()}")
    print(f"  Duplicates: {df['week_start'].duplicated().sum()}")

    full_range = pd.date_range(exp_start, exp_end, freq="W-MON")
    missing    = full_range.difference(df["week_start"])
    print(f"  Missing weeks: {len(missing)}" +
          (f" -> {missing[:5].date.tolist()}..." if len(missing) > 0 else ""))

    print(f"  Nulls (vis cols):")
    for col in VIS_COLS:
        if col in df.columns:
            n   = df[col].isna().sum()
            pct = n / len(df) * 100
            print(f"    {col:35s}: {n:4d} ({pct:.1f}%)")
        else:
            print(f"    {col:35s}: NOT IN FILE")

    # Physical range check on vis_min_m_week (should be > 0)
    if "vis_min_m_week" in df.columns:
        bad = (df["vis_min_m_week"] <= 0).sum()
        if bad:
            print(f"  vis_min_m_week <= 0: {bad} rows")


timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_path  = LOG_DIR / f"audit_visibility_{timestamp}.log"


class Tee:
    def __init__(self, file):
        self.file   = file
        self.stdout = sys.stdout
    def write(self, data):
        self.stdout.write(data)
        self.file.write(data)
    def flush(self):
        self.stdout.flush()
        self.file.flush()


with open(log_path, "w", encoding="utf-8") as f:
    sys.stdout = Tee(f)

    print(f"audit_visibility.py - {timestamp}")
    print(f"Log: {log_path}\n")

    for period_label, exp_start, exp_end in PERIODS:
        suffix = period_label.replace("-", "_")  # e.g. 2004_2015

        print("\n" + "=" * 50)
        print(f"AUDIT {period_label}")
        print("=" * 50)

        for island, code in ISLANDS.items():
            print(f"\n{'='*50}")
            print(f"ISLAND: {island.upper()}  [{period_label}]")

            path = PROCESSED / island / "visibility" / f"visibility_weekly_{code}_{suffix}.parquet"

            if not path.exists():
                print(f"  FILE NOT FOUND: {path}")
                continue

            df = pd.read_parquet(path)
            audit_df(df, exp_start, exp_end)

    print(f"\nLog saved -> {log_path}")

sys.stdout = sys.stdout.stdout
