"""
build_deaths_weekly_provincial.py

Builds weekly provincial deaths for Canary Islands from INE raw file.
Provinces:
    - 35 Palmas, Las       → las_palmas
    - 38 Santa Cruz de Tenerife → sc_tenerife

Output:
    data/processed/provinces/deaths_weekly_las_palmas_2016_2025.parquet
    data/processed/provinces/deaths_weekly_sc_tenerife_2016_2025.parquet

Usage:
    python src/ingests/provinces/build_deaths_weekly_provincial.py
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
ROOT = Path(__file__).resolve().parents[3]
INPUT = ROOT / "data/raw/deaths/ine_35178.csv"
OUTPUT_DIR = ROOT / "data/processed/provinces"

PROVINCES = {
    "35 Palmas, Las": "las_palmas",
    "38 Santa Cruz de Tenerife": "sc_tenerife",
}

START_YEAR = 2016
END_YEAR = 2025

# ── Helpers ───────────────────────────────────────────────────────────────────
def parse_periodo(periodo: str) -> pd.Timestamp | None:
    """
    Parse INE periodo format '2025SM52' → ISO week Monday timestamp.
    Returns None if parsing fails.
    """
    try:
        year, week = int(periodo[:4]), int(periodo[6:])
        return pd.Timestamp.fromisocalendar(year, week, 1)  # Monday
    except Exception:
        return None


def build_province(df: pd.DataFrame, province_key: str, province_name: str) -> pd.DataFrame:
    """Filter, parse and clean deaths for one province."""
    df_prov = df[df["Provincias"] == province_key].copy()
    df_prov = df_prov[df_prov["Tipo de dato"] == "Dato base"]
    df_prov = df_prov[df_prov["Islas"].isna()]
    log.info(f"[{province_name}] Raw rows: {len(df_prov)}")

    # Parse week_start
    df_prov["week_start"] = df_prov["Periodo"].apply(parse_periodo)
    df_prov = df_prov.dropna(subset=["week_start"])

    # Parse deaths — handle dots as thousands separator (INE uses '1.234')
    df_prov["deaths"] = (
        df_prov["Total"]
        .astype(str)
        .str.replace(".", "", regex=False)
        .str.strip()
        .replace("", pd.NA)
        .pipe(pd.to_numeric, errors="coerce")
    )

    # Filter years
    df_prov = df_prov[
        (df_prov["week_start"].dt.year >= START_YEAR) &
        (df_prov["week_start"].dt.year <= END_YEAR)
    ]

    # Keep only needed columns
    df_out = df_prov[["week_start", "deaths"]].copy()
    df_out["province"] = province_name
    df_out = df_out.sort_values("week_start").reset_index(drop=True)

    log.info(f"[{province_name}] Filtered rows ({START_YEAR}-{END_YEAR}): {len(df_out)}")
    log.info(f"[{province_name}] Deaths nulls: {df_out['deaths'].isna().sum()}")
    log.info(f"[{province_name}] Date range: {df_out['week_start'].min()} → {df_out['week_start'].max()}")
    log.info(f"[{province_name}] Deaths range: {df_out['deaths'].min()} – {df_out['deaths'].max()}")

    return df_out


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    log.info(f"Reading INE file: {INPUT}")
    df = pd.read_csv(INPUT, sep=";", dtype=str)
    log.info(f"Total rows loaded: {len(df)}")
    log.info(f"Provinces found: {df['Provincias'].unique().tolist()}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for province_key, province_name in PROVINCES.items():
        df_prov = build_province(df, province_key, province_name)

        out_path = OUTPUT_DIR / f"deaths_weekly_{province_name}_{START_YEAR}_{END_YEAR}.parquet"
        df_prov.to_parquet(out_path, index=False)
        log.info(f"[{province_name}] ✓ Saved to: {out_path}")
        print()

    log.info("=== DONE ===")


if __name__ == "__main__":
    main()
