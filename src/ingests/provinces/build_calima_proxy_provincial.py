"""
build_calima_proxy_provincial.py

Builds weekly provincial calima proxy for Canary Islands.

Method:
    1. Load island-level calima_proxy_level for all islands in province
    2. Map levels to numeric scores: no_calima=0, possible=1, probable=2, intense=3
    3. Compute population-weighted mean score, normalized to 0–1 (divide by 3)
    4. Derive calima_level_provincial using same thresholds as proxy v2:
       >= 0.25 → possible | >= 0.50 → probable | >= 0.75 → intense
    5. Retain calima_intensa_provincial (bool) from original method:
       >= 50% of province population exposed to intense calima

Provinces:
    SC Tenerife: tenerife (916k), la_palma (83k), gomera (22k), hierro (11k)
    Las Palmas:  gran_canaria (852k), lanzarote (152k), fuerteventura (119k)

Output:
    data/processed/provinces/calima_proxy_provincial_sc_tenerife_2016_2025.parquet
    data/processed/provinces/calima_proxy_provincial_las_palmas_2016_2025.parquet

Usage:
    python src/ingests/provinces/build_calima_proxy_provincial.py
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
CALIMA_DIR = ROOT / "data/processed"
OUTPUT_DIR = ROOT / "data/processed/provinces"

START_YEAR = 2016
END_YEAR   = 2025

EXPOSURE_THRESHOLD = 0.50  # >= 50% population exposed to intense = provincial intense episode

SCORE_MAP = {
    "no_calima": 0,
    "possible":  1,
    "probable":  2,
    "intense":   3,
}

PROVINCES = {
    "sc_tenerife": {
        "islands": {
            "tenerife":   {"code": "tfe",  "population": 916_000},
            "la_palma":   {"code": "lpa",  "population": 83_000},
            "gomera":     {"code": "gom",  "population": 22_000},
            "hierro":     {"code": "hie",  "population": 11_000},
        }
    },
    "las_palmas": {
        "islands": {
            "gran_canaria":  {"code": "gcan", "population": 852_000},
            "lanzarote":     {"code": "lzt",  "population": 152_000},
            "fuerteventura": {"code": "ftv",  "population": 119_000},
        }
    },
}

# ── Helpers ───────────────────────────────────────────────────────────────────
def load_island_calima(island_name: str, island_code: str) -> pd.DataFrame:
    path = (
        CALIMA_DIR
        / island_name
        / "calima"
        / f"calima_proxy_v2_weekly_{island_code}_{START_YEAR}_{END_YEAR}.parquet"
    )
    if not path.exists():
        log.warning(f"  ⚠ File not found: {path}")
        return pd.DataFrame()

    df = pd.read_parquet(path)[["week_start", "calima_proxy_level"]].copy()
    df["island"] = island_name
    log.info(f"  ✓ Loaded {island_name}: {len(df)} weeks")
    return df


def build_province_calima(province_name: str, islands: dict) -> pd.DataFrame:
    log.info(f"\n[{province_name}] Building calima proxy...")

    total_population = sum(v["population"] for v in islands.values())
    log.info(f"  Total population: {total_population:,}")

    # Load all island calima files
    frames = []
    for island_name, meta in islands.items():
        df = load_island_calima(island_name, meta["code"])
        if df.empty:
            continue
        df["population"] = meta["population"]
        frames.append(df)

    if not frames:
        log.error(f"  No data loaded for {province_name}. Aborting.")
        return pd.DataFrame()

    df_all = pd.concat(frames, ignore_index=True)

    # Filter years
    df_all = df_all[
        (df_all["week_start"].dt.year >= START_YEAR) &
        (df_all["week_start"].dt.year <= END_YEAR)
    ].copy()

    # Map levels to numeric scores
    df_all["score"] = df_all["calima_proxy_level"].map(SCORE_MAP)
    unmapped = df_all["score"].isna().sum()
    if unmapped > 0:
        log.warning(f"  ⚠ {unmapped} rows with unmapped calima_proxy_level")

    # calima_intensa_provincial: >= 50% population exposed to intense
    df_all["is_intense"] = (df_all["calima_proxy_level"] == "intense").astype(int)
    df_all["pop_intense"] = df_all["is_intense"] * df_all["population"]

    # Population-weighted score per week
    df_all["weighted_score"] = df_all["score"] * df_all["population"]

    weekly = (
        df_all.groupby("week_start")
        .agg(
            pop_intense=("pop_intense", "sum"),
            weighted_score_sum=("weighted_score", "sum"),
            total_pop=("population", "sum"),
        )
        .reset_index()
    )

    # Normalize to 0–1
    weekly["calima_score_provincial"]     = (weekly["weighted_score_sum"] / weekly["total_pop"]) / 3
    weekly["pct_exposed"]                 = weekly["pop_intense"] / total_population
    weekly["calima_intensa_provincial"]   = weekly["pct_exposed"] >= EXPOSURE_THRESHOLD
    weekly["province"]                    = province_name

    # Derive calima_level_provincial using proxy v2 thresholds
    weekly["calima_level_provincial"] = "no_calima"
    weekly.loc[weekly["calima_score_provincial"] >= 0.25, "calima_level_provincial"] = "possible"
    weekly.loc[weekly["calima_score_provincial"] >= 0.50, "calima_level_provincial"] = "probable"
    weekly.loc[weekly["calima_score_provincial"] >= 0.75, "calima_level_provincial"] = "intense"

    # Summary
    log.info(f"  Calima intensa episodes: {weekly['calima_intensa_provincial'].sum()} / {len(weekly)}")
    log.info(f"  Score range: {weekly['calima_score_provincial'].min():.3f} – {weekly['calima_score_provincial'].max():.3f}")
    log.info(f"  Level distribution:\n{weekly['calima_level_provincial'].value_counts().to_string()}")

    return weekly[[
        "week_start",
        "province",
        "pop_intense",
        "pct_exposed",
        "calima_intensa_provincial",
        "calima_score_provincial",
        "calima_level_provincial",
    ]].sort_values("week_start").reset_index(drop=True)


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for province_name, config in PROVINCES.items():
        df = build_province_calima(province_name, config["islands"])

        if df.empty:
            log.error(f"[{province_name}] No output generated.")
            continue

        out_path = OUTPUT_DIR / f"calima_proxy_provincial_{province_name}_{START_YEAR}_{END_YEAR}.parquet"
        df.to_parquet(out_path, index=False)
        log.info(f"  ✓ Saved to: {out_path}")

    log.info("\n=== DONE ===")


if __name__ == "__main__":
    main()