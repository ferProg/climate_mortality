# audit_visibility_2004_2015.py
import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(r"C:/Users/fdora/RA_Career/Projects/climate_mortality")
PROCESSED = PROJECT_ROOT / "data" / "processed"

ISLANDS = {
    "gomera":        "gom",
    "tenerife":      "tfe",
    "gran_canaria":  "gcan",
    "lanzarote":     "lzt",
    "fuerteventura": "ftv",
    "la_palma":      "lpa",
    "hierro":        "hie",
}
year_ini = 2016
year_end = 2025

for island, code in ISLANDS.items():
    path = PROCESSED / island / "visibility" / f"visibility_weekly_{code}_{year_ini}_{year_end}.parquet"
    
    print(f"\n{'='*50}")
    print(f"ISLAND: {island.upper()}")

    if not path.exists():
        print(f"  ❌ FILE NOT FOUND: {path}")
        continue

    df = pd.read_parquet(path)
    df["week_start"] = pd.to_datetime(df["week_start"], errors="coerce")

    print(f"  Shape     : {df.shape}")
    print(f"  Date range: {df['week_start'].min().date()} → {df['week_start'].max().date()}")
    print(f"  Columns   : {df.columns.tolist()}")
    print(f"  Duplicates: {df['week_start'].duplicated().sum()}")
    print(f"  Null weeks: {df['week_start'].isna().sum()}")
    for col in df.columns:
        if col != "week_start":
            n = df[col].isna().sum()
            pct = n / len(df) * 100
            print(f"    {col:30s}: {n:3d} ({pct:.1f}%)")