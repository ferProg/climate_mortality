"""
build_master_provincial.py

Merges provincial deaths + calima proxy into master provincial datasets.
Adds population and deaths_per_100k columns.

Provinces:
    SC Tenerife: population 1,032,000
    Las Palmas:  population 1,123,000

Output:
    data/processed/provinces/master_provincial_sc_tenerife_2016_2025.parquet
    data/processed/provinces/master_provincial_las_palmas_2016_2025.parquet

Usage:
    python src/master/provinces/build_master_provincial.py
"""

import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
ROOT       = Path(__file__).resolve().parents[3]
PROV_DIR   = ROOT / "data/processed/provinces"
OUTPUT_DIR = ROOT / "data/processed/provinces"

START_YEAR = 2016
END_YEAR   = 2025

PROVINCES = {
    "sc_tenerife": {"population": 1_032_000},
    "las_palmas":  {"population": 1_123_000},
}

# ── Main ──────────────────────────────────────────────────────────────────────
def build_master(province_name: str, population: int) -> pd.DataFrame:
    log.info(f"\n[{province_name}] Building master...")

    deaths_path = PROV_DIR / f"deaths_weekly_{province_name}_{START_YEAR}_{END_YEAR}.parquet"
    calima_path = PROV_DIR / f"calima_proxy_provincial_{province_name}_{START_YEAR}_{END_YEAR}.parquet"

    if not deaths_path.exists():
        log.error(f"  Deaths file not found: {deaths_path}")
        return pd.DataFrame()
    if not calima_path.exists():
        log.error(f"  Calima file not found: {calima_path}")
        return pd.DataFrame()

    df_deaths = pd.read_parquet(deaths_path)
    df_calima = pd.read_parquet(calima_path)

    log.info(f"  Deaths rows: {len(df_deaths)}")
    log.info(f"  Calima rows: {len(df_calima)}")

    # Merge on week_start — spine is deaths (left join)
    df = pd.merge(df_deaths, df_calima, on="week_start", how="left")

    # Add population + deaths_per_100k
    df["population"]      = population
    df["deaths_per_100k"] = (df["deaths"] / population) * 100_000

    # Clean up duplicate province columns from merge
    if "province_y" in df.columns:
        df = df.drop(columns=["province_y"])
    if "province_x" in df.columns:
        df = df.rename(columns={"province_x": "province"})

    # Final column order
    df = df[[
        "week_start",
        "province",
        "population",
        "deaths",
        "deaths_per_100k",
        "pop_intense",
        "pct_exposed",
        "calima_intensa_provincial",
        "calima_score_provincial",
        "calima_level_provincial",
    ]].sort_values("week_start").reset_index(drop=True)

    # Sanity checks
    log.info(f"  Output rows: {len(df)}")
    log.info(f"  Deaths nulls: {df['deaths'].isna().sum()}")
    log.info(f"  Calima nulls: {df['calima_intensa_provincial'].isna().sum()}")
    log.info(f"  Deaths per 100k range: {df['deaths_per_100k'].min():.2f} – {df['deaths_per_100k'].max():.2f}")
    log.info(f"  Calima intensa episodes: {df['calima_intensa_provincial'].sum()} / {len(df)}")

    return df


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for province_name, config in PROVINCES.items():
        df = build_master(province_name, config["population"])

        if df.empty:
            log.error(f"[{province_name}] No output generated.")
            continue

        out_path = OUTPUT_DIR / f"master_provincial_{province_name}_{START_YEAR}_{END_YEAR}.parquet"
        df.to_parquet(out_path, index=False)
        log.info(f"  ✓ Saved to: {out_path}")

    log.info("\n=== DONE ===")


if __name__ == "__main__":
    main()
