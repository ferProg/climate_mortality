# audit_deaths.py
# Audits deaths weekly parquets for all islands, both periods.
#
# Coverage map:
#   2004-2015: tenerife, fuerteventura, gran_canaria, gomera, la_palma, lanzarote
#              hierro -> NO FILE
#   2016-2025: all 7 islands

import sys
import pandas as pd
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(r"C:/Users/fdora/RA_Career/Projects/climate_mortality")
PROCESSED    = PROJECT_ROOT / "data" / "processed"
LOG_DIR      = PROJECT_ROOT / "logs" / "audit_2004_2015"
LOG_DIR.mkdir(parents=True, exist_ok=True)

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
    df["week_start"] = pd.to_datetime(df["week_start"], errors="coerce")

    print(f"  Shape     : {df.shape}")
    print(f"  Date range: {df['week_start'].min().date()} -> {df['week_start'].max().date()}")
    print(f"  Columns   : {df.columns.tolist()}")
    print(f"  Duplicates: {df['week_start'].duplicated().sum()}")

    full_range = pd.date_range(exp_start, exp_end, freq="W-MON")
    missing    = full_range.difference(df["week_start"])
    print(f"  Missing weeks: {len(missing)}" +
          (f" -> {missing[:5].date.tolist()}..." if len(missing) > 0 else ""))

    # Nulls
    if "deaths_week" in df.columns:
        n   = df["deaths_week"].isna().sum()
        pct = n / len(df) * 100
        print(f"  Nulls deaths_week : {n} ({pct:.1f}%)")
    else:
        print(f"  deaths_week       : COLUMN NOT FOUND")

    # Distribution
    if "deaths_week" in df.columns:
        s = df["deaths_week"].dropna()
        if len(s):
            print(f"  Deaths stats      : min={s.min():.0f}  median={s.median():.1f}  mean={s.mean():.1f}  max={s.max():.0f}  p95={s.quantile(0.95):.1f}")

    # Flag / missing_week columns
    flag_cols = [c for c in df.columns if "miss" in c.lower() or "flag" in c.lower() or "imputed" in c.lower()]
    if flag_cols:
        print(f"  Flag columns      : {flag_cols}")
        for col in flag_cols:
            n = df[col].astype(str).str.lower().isin(["1", "true", "yes"]).sum()
            print(f"    {col}: {n} flagged rows")

    # Negative deaths check
    if "deaths_week" in df.columns:
        neg = (df["deaths_week"] < 0).sum()
        if neg:
            print(f"  Negative deaths   : {neg} rows")


timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_path  = LOG_DIR / f"audit_deaths_{timestamp}.log"


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

    print(f"audit_deaths.py - {timestamp}")
    print(f"Log: {log_path}\n")

    for period_label, exp_start, exp_end in PERIODS:
        suffix = period_label.replace("-", "_")

        print("\n" + "=" * 50)
        print(f"AUDIT {period_label}")
        print("=" * 50)

        for island, code in ISLANDS.items():
            print(f"\n{'='*50}")
            print(f"ISLAND: {island.upper()}  [{period_label}]")

            path = PROCESSED / island / "deaths" / f"deaths_weekly_{code}_{suffix}.parquet"

            if not path.exists():
                print(f"  FILE NOT FOUND: {path}")
                continue

            df = pd.read_parquet(path)
            audit_df(df, exp_start, exp_end)

    print(f"\nLog saved -> {log_path}")

sys.stdout = sys.stdout.stdout
