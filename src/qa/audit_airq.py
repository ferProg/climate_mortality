# audit_airq.py
# Audits air quality weekly parquets for all islands, both periods.
#
# Coverage map (what actually exists):
#   2004-2015 period:
#     tenerife      → weekly_tfe_2004_2012  + weekly_tfe_2013_2015  (two files)
#     fuerteventura → weekly_ftv_2005_2015
#     gran_canaria  → weekly_gcan_2005_2015
#     gomera        → weekly_gom_2013_2015   (starts 2013, not 2005)
#     la_palma      → weekly_lpa_2005_2015
#     lanzarote     → weekly_lzt_2005_2015
#     hierro        → NO FILE (not available)
#
#   2016-2025 period:
#     tenerife      → weekly_tfe_2016_2025
#     fuerteventura → weekly_ftv_2016_2025
#     gran_canaria  → weekly_gcan_2016_2025
#     gomera        → weekly_gom_2016_2025_eac4  (different source)
#     la_palma      → weekly_lpa_2016_2025
#     lanzarote     → weekly_lzt_2016_2025
#     hierro        → weekly_hie_2016_2025

import sys
import pandas as pd
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(r"C:/Users/fdora/RA_Career/Projects/climate_mortality")
PROCESSED    = PROJECT_ROOT / "data" / "processed"
LOG_DIR      = PROJECT_ROOT / "logs" / "audit_2004_2015"
LOG_DIR.mkdir(parents=True, exist_ok=True)

POLLUTANT_COLS = ["PM10", "PM2.5", "SO2", "NO2", "O3"]

# (island, code, [file_stem, ...], period_label, expected_start, expected_end)
# Tenerife 2004-2015 is split in two files — listed as a list
DATASETS = [
    # ── 2004-2015 ─────────────────────────────────────────────────
    ("tenerife",      "tfe",  ["weekly_tfe_2004_2012", "weekly_tfe_2013_2015"], "2004-2015", "2004-01-05", "2015-12-28"),
    ("fuerteventura", "ftv",  ["weekly_ftv_2005_2015"],                         "2005-2015", "2005-01-03", "2015-12-28"),
    ("gran_canaria",  "gcan", ["weekly_gcan_2005_2015"],                        "2005-2015", "2005-01-03", "2015-12-28"),
    ("gomera",        "gom",  ["weekly_gom_2013_2015"],                         "2013-2015", "2013-01-07", "2015-12-28"),
    ("la_palma",      "lpa",  ["weekly_lpa_2005_2015"],                         "2005-2015", "2005-01-03", "2015-12-28"),
    ("lanzarote",     "lzt",  ["weekly_lzt_2005_2015"],                         "2005-2015", "2005-01-03", "2015-12-28"),
    ("hierro",        "hie",  [],                                               "2004-2015", None, None),
    # ── 2016-2025 ─────────────────────────────────────────────────
    ("tenerife",      "tfe",  ["weekly_tfe_2016_2025"],                         "2016-2025", "2016-01-04", "2025-12-29"),
    ("fuerteventura", "ftv",  ["weekly_ftv_2016_2025"],                         "2016-2025", "2016-01-04", "2025-12-29"),
    ("gran_canaria",  "gcan", ["weekly_gcan_2016_2025"],                        "2016-2025", "2016-01-04", "2025-12-29"),
    ("gomera",        "gom",  ["weekly_gom_2016_2025_eac4"],                    "2016-2025", "2016-01-04", "2025-12-29"),
    ("la_palma",      "lpa",  ["weekly_lpa_2016_2025"],                         "2016-2025", "2016-01-04", "2025-12-29"),
    ("lanzarote",     "lzt",  ["weekly_lzt_2016_2025"],                         "2016-2025", "2016-01-04", "2025-12-29"),
    ("hierro",        "hie",  ["weekly_hie_2016_2025"],                         "2016-2025", "2016-01-04", "2025-12-29"),
]


def audit_df(df: pd.DataFrame, expected_start: str, expected_end: str) -> None:
    df = df.copy()
    df["week_start"] = pd.to_datetime(df["week_start"])

    print(f"  Shape     : {df.shape}")
    print(f"  Date range: {df['week_start'].min().date()} → {df['week_start'].max().date()}")
    print(f"  Columns   : {df.columns.tolist()}")
    print(f"  Duplicates: {df['week_start'].duplicated().sum()}")

    if expected_start and expected_end:
        full_range = pd.date_range(expected_start, expected_end, freq="W-MON")
        missing = full_range.difference(df["week_start"])
        print(f"  Missing weeks: {len(missing)}" +
              (f" → {missing[:5].date.tolist()}..." if len(missing) > 0 else ""))

    print(f"  Nulls (pollutants):")
    for col in POLLUTANT_COLS:
        if col in df.columns:
            n   = df[col].isna().sum()
            pct = n / len(df) * 100
            print(f"    {col:8s}: {n:4d} ({pct:.1f}%)")
        else:
            print(f"    {col:8s}: ⚠️  NOT IN FILE")

    if "days_missing_pm10" in df.columns:
        high = (df["days_missing_pm10"] >= 4).sum()
        print(f"  Weeks with ≥4 days missing PM10: {high}")


timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_path  = LOG_DIR / f"audit_airq_{timestamp}.log"


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

    print(f"audit_airq.py — {timestamp}")
    print(f"Log: {log_path}\n")

    current_period = None

    for island, code, stems, period, exp_start, exp_end in DATASETS:

        if period != current_period:
            current_period = period
            print("\n" + "=" * 50)
            print(f"AUDIT {period}")
            print("=" * 50)

        print(f"\n{'='*50}")
        print(f"ISLAND: {island.upper()}  [{period}]")

        if not stems:
            print("  ⚠️  NO FILE AVAILABLE (not in scope for this period)")
            continue

        airq_dir = PROCESSED / island / "air_quality"
        frames   = []

        for stem in stems:
            path = airq_dir / f"{stem}.parquet"
            if not path.exists():
                print(f"  ❌ FILE NOT FOUND: {path}")
            else:
                frames.append(pd.read_parquet(path))

        if not frames:
            continue

        if len(frames) == 1:
            df = frames[0]
        else:
            df = pd.concat(frames, ignore_index=True).sort_values("week_start")
            dups = df["week_start"].duplicated().sum()
            if dups:
                print(f"  ⚠️  {dups} duplicate week(s) after concat — keeping first")
                df = df.drop_duplicates(subset="week_start", keep="first")

        audit_df(df, exp_start, exp_end)

    print(f"\nLog saved → {log_path}")

sys.stdout = sys.stdout.stdout
