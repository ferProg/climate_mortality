"""
build_master_ccaa.py

Builds master CCAA Canarias by aggregating both provincial masters.
Deaths and population are summed across provinces.
Calima columns:
    calima_sct      : intense episode in SC Tenerife
    calima_lp       : intense episode in Las Palmas
    calima_any      : TRUE if any province has intense episode (main variable)
    calima_both     : TRUE if both provinces have intense episode simultaneously

Output:
    data/processed/provinces/master_ccaa_canarias_2016_2025.parquet

Usage:
    python src/master/ccaa/build_master_ccaa.py
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

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    log.info("Building master CCAA Canarias...")

    # Load provincial masters
    sct_path = PROV_DIR / f"master_provincial_sc_tenerife_{START_YEAR}_{END_YEAR}.parquet"
    lp_path  = PROV_DIR / f"master_provincial_las_palmas_{START_YEAR}_{END_YEAR}.parquet"

    if not sct_path.exists():
        log.error(f"File not found: {sct_path}")
        return
    if not lp_path.exists():
        log.error(f"File not found: {lp_path}")
        return

    df_sct = pd.read_parquet(sct_path)
    df_lp  = pd.read_parquet(lp_path)

    log.info(f"  SC Tenerife rows: {len(df_sct)}")
    log.info(f"  Las Palmas rows:  {len(df_lp)}")

    # Rename calima column before merge to avoid collision
    df_sct = df_sct.rename(columns={"calima_intensa_provincial": "calima_sct"})
    df_lp  = df_lp.rename(columns={"calima_intensa_provincial": "calima_lp"})

    # Merge on week_start (inner — only weeks present in both)
    df = pd.merge(
        df_sct[["week_start", "deaths", "population", "calima_sct"]],
        df_lp[["week_start",  "deaths", "population", "calima_lp"]],
        on="week_start",
        how="inner",
        suffixes=("_sct", "_lp")
    )

    # Aggregate
    df["province"]        = "canarias"
    df["deaths"]          = df["deaths_sct"] + df["deaths_lp"]
    df["population"]      = df["population_sct"] + df["population_lp"]
    df["deaths_per_100k"] = (df["deaths"] / df["population"]) * 100_000

    # Calima columns
    df["calima_any"]  = df["calima_sct"] | df["calima_lp"]
    df["calima_both"] = df["calima_sct"] & df["calima_lp"]

    # Final column order
    df = df[[
        "week_start",
        "province",
        "population",
        "deaths",
        "deaths_per_100k",
        "calima_sct",
        "calima_lp",
        "calima_any",
        "calima_both",
    ]].sort_values("week_start").reset_index(drop=True)

    # Sanity checks
    log.info(f"  Output rows: {len(df)}")
    log.info(f"  Deaths nulls: {df['deaths'].isna().sum()}")
    log.info(f"  Deaths per 100k range: {df['deaths_per_100k'].min():.2f} – {df['deaths_per_100k'].max():.2f}")
    log.info(f"  calima_any episodes:  {df['calima_any'].sum()} / {len(df)}")
    log.info(f"  calima_both episodes: {df['calima_both'].sum()} / {len(df)}")
    log.info(f"  calima_sct only:      {(df['calima_sct'] & ~df['calima_lp']).sum()}")
    log.info(f"  calima_lp only:       {(df['calima_lp'] & ~df['calima_sct']).sum()}")

    # Save
    out_path = OUTPUT_DIR / f"master_ccaa_canarias_{START_YEAR}_{END_YEAR}.parquet"
    df.to_parquet(out_path, index=False)
    log.info(f"  ✓ Saved to: {out_path}")
    log.info("=== DONE ===")


if __name__ == "__main__":
    main()
