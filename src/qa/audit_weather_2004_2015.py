# audit_weather_2004_2015.py
import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(r"C:/Users/fdora/RA_Career/Projects/climate_mortality")
PROCESSED = PROJECT_ROOT / "data" / "processed"

ISLANDS = {
    "gomera":        ("gom", "C329B"),
    "tenerife":      ("tfe", "C429I"),
    "gran_canaria":  ("gcan",  "C649I"),
    "lanzarote":     ("lzt", "C029O"),
    "fuerteventura": ("ftv", "C249I"),
    "la_palma":      ("lpa", "C139E"),
    "hierro":        ("hie", "C929I"),
}


PROXY_COLS = ["tmax_c_mean", "humidity_mean", "vis_min_m_week", "PM10"]

for island, (code, _) in ISLANDS.items():
    path = PROCESSED / island / "weather" / f"weather_weekly_{code}_2004_2015.parquet"
    print(f"\n{'='*50}")
    print(f"ISLAND: {island.upper()}")

    if not path.exists():
        print(f"  ❌ FILE NOT FOUND: {path}")
        continue

    df = pd.read_parquet(path)
    df["week_start"] = pd.to_datetime(df["week_start"])

    # Shape + date range
    print(f"  Shape     : {df.shape}")
    print(f"  Date range: {df['week_start'].min().date()} → {df['week_start'].max().date()}")

    # Duplicates
    dups = df["week_start"].duplicated().sum()
    print(f"  Duplicates: {dups}")

    # Missing weeks
    full_range = pd.date_range("2004-01-05", "2015-12-28", freq="W-MON")
    missing = full_range.difference(df["week_start"])
    print(f"  Missing weeks: {len(missing)}" + (f" → {missing[:5].date.tolist()}..." if len(missing) > 0 else ""))

    # Nulls for proxy columns
    print(f"  Nulls (proxy cols):")
    for col in PROXY_COLS:
        if col in df.columns:
            n = df[col].isna().sum()
            pct = n / len(df) * 100
            print(f"    {col:25s}: {n:3d} ({pct:.1f}%)")
        else:
            print(f"    {col:25s}: ⚠️  COLUMN NOT FOUND")

# Audit 2016-2025 parquets
CODES_2016 = {
    "gomera":        "gom",
    "tenerife":      "tfe",
    "gran_canaria":  "gcan",
    "lanzarote":     "lzt",
    "fuerteventura": "ftv",
    "la_palma":      "lpa",
    "hierro":        "hie",
}

print("\n" + "="*50)
print("AUDIT 2016-2025")
print("="*50)

for island, code in CODES_2016.items():
    path = PROCESSED / island / "weather" / f"weather_weekly_{code}_2016_2025.parquet"
    print(f"\nISLAND: {island.upper()}")

    if not path.exists():
        print(f"  ❌ FILE NOT FOUND: {path}")
        continue

    df = pd.read_parquet(path)
    df["week_start"] = pd.to_datetime(df["week_start"])

    print(f"  Shape     : {df.shape}")
    print(f"  Date range: {df['week_start'].min().date()} → {df['week_start'].max().date()}")
    print(f"  Columns   : {df.columns.tolist()}")
    print(f"  Duplicates: {df['week_start'].duplicated().sum()}")

    for col in PROXY_COLS:
        if col in df.columns:
            n = df[col].isna().sum()
            pct = n / len(df) * 100
            print(f"    {col:25s}: {n:3d} ({pct:.1f}%)")
        else:
            print(f"    {col:25s}: ⚠️  COLUMN NOT FOUND")