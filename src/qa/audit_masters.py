# audit_masters.py
# Audits master parquets: completeness, seam 2015->2016, and null counts.
#
# Checks per master:
#   1. Shape, date range, columns
#   2. Duplicates and missing weeks
#   3. Seam inspection: weeks 2015-12-14 to 2016-01-18 (±3 weeks around join)
#   4. Null counts for key columns
#   5. Sudden jumps in deaths_week at the seam (>3x median change)

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
}

# Key columns to check nulls
KEY_COLS = [
    "deaths_week",
    "temp_c_mean", "tmax_c_mean", "tmin_c_mean",
    "humidity_mean", "pressure_hpa_mean", "wind_ms_mean",
    "PM10", "PM2.5",
    "vis_min_m_week",
    "calima_dai_flag", "calima_level_week",
]

SEAM_START = pd.Timestamp("2015-12-14")
SEAM_END   = pd.Timestamp("2016-01-18")


def audit_master(df: pd.DataFrame, island: str, exp_start: str, exp_end: str) -> None:
    df = df.copy()
    df["week_start"] = pd.to_datetime(df["week_start"])

    print(f"  Shape     : {df.shape}")
    print(f"  Date range: {df['week_start'].min().date()} -> {df['week_start'].max().date()}")
    print(f"  Duplicates: {df['week_start'].duplicated().sum()}")

    full_range = pd.date_range(exp_start, exp_end, freq="W-MON")
    missing    = full_range.difference(df["week_start"])
    print(f"  Missing weeks: {len(missing)}" +
          (f" -> {missing[:5].date.tolist()}..." if len(missing) > 0 else ""))

    # ── Null counts for key columns ────────────────────────────────
    print(f"  Nulls (key cols):")
    for col in KEY_COLS:
        if col in df.columns:
            n   = df[col].isna().sum()
            pct = n / len(df) * 100
            flag = " <<<" if pct > 5 else ""
            print(f"    {col:30s}: {n:4d} ({pct:.1f}%){flag}")

    # ── Seam inspection ────────────────────────────────────────────
    seam = df[(df["week_start"] >= SEAM_START) & (df["week_start"] <= SEAM_END)].copy()
    print(f"\n  SEAM ({SEAM_START.date()} -> {SEAM_END.date()}):")
    print(f"  Rows in seam window: {len(seam)}")

    seam_cols = ["week_start", "deaths_week", "temp_c_mean", "tmax_c_mean",
                 "humidity_mean", "PM10", "vis_min_m_week"]
    seam_show = [c for c in seam_cols if c in seam.columns]
    print(seam[seam_show].to_string(index=False))

    # ── Deaths jump at seam ────────────────────────────────────────
    if "deaths_week" in df.columns:
        df_sorted  = df.sort_values("week_start")
        pre_seam   = df_sorted[df_sorted["week_start"] < pd.Timestamp("2016-01-01")]["deaths_week"].dropna()
        post_seam  = df_sorted[df_sorted["week_start"] >= pd.Timestamp("2016-01-01")]["deaths_week"].dropna()
        if len(pre_seam) and len(post_seam):
            pre_med  = pre_seam.median()
            post_med = post_seam.median()
            ratio    = post_med / pre_med if pre_med else float("nan")
            print(f"\n  Deaths median pre-2016 : {pre_med:.1f}")
            print(f"  Deaths median post-2016: {post_med:.1f}")
            print(f"  Ratio post/pre         : {ratio:.2f}" +
                  (" <<< JUMP" if ratio > 1.5 or ratio < 0.67 else " OK"))


timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_path  = LOG_DIR / f"audit_masters_{timestamp}.log"


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

    print(f"audit_masters.py - {timestamp}")
    print(f"Log: {log_path}\n")

    for island, code in ISLANDS.items():
        print(f"\n{'='*50}")
        print(f"ISLAND: {island.upper()}")

        path = PROCESSED / island / "master" / f"master_{code}_2004_2025.parquet"

        if not path.exists():
            print(f"  FILE NOT FOUND: {path}")
            continue

        df = pd.read_parquet(path)
        audit_master(df, island, "2004-01-05", "2025-12-29")

    print(f"\nLog saved -> {log_path}")

sys.stdout = sys.stdout.stdout
