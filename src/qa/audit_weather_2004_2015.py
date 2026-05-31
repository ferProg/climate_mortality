# audit_weather_2004_2015.py
import sys
import pandas as pd
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(r"C:/Users/fdora/RA_Career/Projects/climate_mortality")
PROCESSED = PROJECT_ROOT / "data" / "processed"
LOG_DIR = PROJECT_ROOT / "logs" / "audit_2004_2015"
LOG_DIR.mkdir(parents=True, exist_ok=True)

ISLANDS = {
    "gomera":        ("gom", "C329B"),
    "tenerife":      ("tfe", "C429I"),
    "gran_canaria":  ("gcan",  "C649I"),
    "lanzarote":     ("lzt", "C029O"),
    "fuerteventura": ("ftv", "C249I"),
    "la_palma":      ("lpa", "C139E"),
    "hierro":        ("hie", "C929I"),
}

WEATHER_COLS = ["tmax_c_mean", "tmin_c_mean", "temp_c_mean", "humidity_mean",
                "pressure_hpa_mean", "wind_ms_mean", "prec_sum"]

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_path = LOG_DIR / f"audit_weather_{timestamp}.log"

class Tee:
    """Write to both stdout and a log file."""
    def __init__(self, file):
        self.file = file
        self.stdout = sys.stdout
    def write(self, data):
        self.stdout.write(data)
        self.file.write(data)
    def flush(self):
        self.stdout.flush()
        self.file.flush()

with open(log_path, "w", encoding="utf-8") as f:
    sys.stdout = Tee(f)

    print(f"audit_weather_2004_2015.py — {timestamp}")
    print(f"Log: {log_path}\n")

    # ── 2004-2015 ──────────────────────────────────────────────────
    print("=" * 50)
    print("AUDIT 2004-2015")
    print("=" * 50)

    for island, (code, _) in ISLANDS.items():
        path = PROCESSED / island / "weather" / f"weather_weekly_{code}_2004_2015.parquet"
        print(f"\n{'='*50}")
        print(f"ISLAND: {island.upper()}")

        if not path.exists():
            print(f"  ❌ FILE NOT FOUND: {path}")
            continue

        df = pd.read_parquet(path)
        df["week_start"] = pd.to_datetime(df["week_start"])

        print(f"  Shape     : {df.shape}")
        print(f"  Date range: {df['week_start'].min().date()} → {df['week_start'].max().date()}")
        print(f"  Columns   : {df.columns.tolist()}")
        print(f"  Duplicates: {df['week_start'].duplicated().sum()}")

        full_range = pd.date_range("2004-01-05", "2015-12-28", freq="W-MON")
        missing = full_range.difference(df["week_start"])
        print(f"  Missing weeks: {len(missing)}" + (f" → {missing[:5].date.tolist()}..." if len(missing) > 0 else ""))

        print(f"  Nulls (weather cols):")
        for col in WEATHER_COLS:
            if col in df.columns:
                n = df[col].isna().sum()
                pct = n / len(df) * 100
                print(f"    {col:25s}: {n:3d} ({pct:.1f}%)")

    # ── 2016-2025 ──────────────────────────────────────────────────
    print("\n" + "=" * 50)
    print("AUDIT 2016-2025")
    print("=" * 50)

    for island, (code, _) in ISLANDS.items():
        path = PROCESSED / island / "weather" / f"weather_weekly_{code}_2016_2025.parquet"
        print(f"\n{'='*50}")
        print(f"ISLAND: {island.upper()}")

        if not path.exists():
            print(f"  ❌ FILE NOT FOUND: {path}")
            continue

        df = pd.read_parquet(path)
        df["week_start"] = pd.to_datetime(df["week_start"])

        print(f"  Shape     : {df.shape}")
        print(f"  Date range: {df['week_start'].min().date()} → {df['week_start'].max().date()}")
        print(f"  Columns   : {df.columns.tolist()}")
        print(f"  Duplicates: {df['week_start'].duplicated().sum()}")

        full_range = pd.date_range("2016-01-04", "2025-12-29", freq="W-MON")
        missing = full_range.difference(df["week_start"])
        print(f"  Missing weeks: {len(missing)}" + (f" → {missing[:5].date.tolist()}..." if len(missing) > 0 else ""))

        print(f"  Nulls (weather cols):")
        for col in WEATHER_COLS:
            if col in df.columns:
                n = df[col].isna().sum()
                pct = n / len(df) * 100
                print(f"    {col:25s}: {n:3d} ({pct:.1f}%)")

    print(f"\nLog saved → {log_path}")

sys.stdout = sys.stdout.stdout