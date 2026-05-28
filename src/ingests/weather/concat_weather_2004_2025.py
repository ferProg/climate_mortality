# concat_weather_2004_2025.py
# Concatenates 2004-2015 + 2016-2025 weather parquets for all 7 islands
# Output: data/processed/<island>/weather/weather_weekly_<code>_2004_2025.parquet

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

KEEP_COLS = [
    "week_start", "n_days", "temp_c_mean", "tmax_c_mean", "tmax_c_max",
    "tmin_c_mean", "tmin_c_min", "humidity_mean", "pressure_hpa_mean",
    "wind_ms_mean", "prec_sum", "coverage"
]

for island, code in ISLANDS.items():
    print(f"\n{'='*50}")
    print(f"ISLAND: {island.upper()}")

    p_old = PROCESSED / island / "weather" / f"weather_weekly_{code}_2004_2015.parquet"
    p_new = PROCESSED / island / "weather" / f"weather_weekly_{code}_2016_2025.parquet"

    if not p_old.exists():
        print(f"  ❌ Missing 2004-2015: {p_old}")
        continue
    if not p_new.exists():
        print(f"  ❌ Missing 2016-2025: {p_new}")
        continue

    df_old = pd.read_parquet(p_old)
    df_new = pd.read_parquet(p_new)

    # Normalize week_start
    df_old["week_start"] = pd.to_datetime(df_old["week_start"])
    df_new["week_start"] = pd.to_datetime(df_new["week_start"])

    # Keep only common columns
    cols_old = [c for c in KEEP_COLS if c in df_old.columns]
    cols_new = [c for c in KEEP_COLS if c in df_new.columns]
    df_old = df_old[cols_old]
    df_new = df_new[cols_new]

    # Concatenate + deduplicate overlap (keep 2016-2025)
    df = pd.concat([df_old, df_new], ignore_index=True)
    df = df.sort_values("week_start")
    df = df.drop_duplicates(subset="week_start", keep="last")
    df = df.reset_index(drop=True)

    # Save
    out_path = PROCESSED / island / "weather" / f"weather_weekly_{code}_2004_2025.parquet"
    df.to_parquet(out_path, index=False)

    print(f"  Shape     : {df.shape}")
    print(f"  Date range: {df['week_start'].min().date()} → {df['week_start'].max().date()}")
    print(f"  Saved     : {out_path.name}")