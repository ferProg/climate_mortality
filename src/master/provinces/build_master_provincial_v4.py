"""
build_master_provincial_v4.py

Construye masters provinciales 2009-2025 aplicando Calima Proxy v4
(regresión logística calibrada, AUC=0.932) en lugar del proxy v2.

Metodología:
  - Muertes:      suma directa de deaths_week por isla del grupo
  - Temperatura:  promedio ponderado por población (temp_c_mean)
  - Calima v4:    promedio ponderado por población de PM10, PM2.5,
                  vis_min_m_week → aplicar proxy_v4_config.json
                  → calima_score [0-1], calima_level, calima_ordinal

Provincias:
  SC Tenerife:  Tenerife + La Palma + Gomera
  Las Palmas:   Gran Canaria + Lanzarote + Fuerteventura

Output:
  data/processed/provinces/master_provincial_sc_tenerife_v4_2009_2025.parquet
  data/processed/provinces/master_provincial_las_palmas_v4_2009_2025.parquet

Uso:
  python -m src.master.provinces.build_master_provincial_v4
  (desde el directorio raíz del proyecto)
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)

# ── Rutas ─────────────────────────────────────────────────────────────────────
ROOT       = Path(__file__).resolve().parents[3]
PROCESSED  = ROOT / "data" / "processed"
OUT_DIR    = PROCESSED / "provinces"
CONFIG_V4  = PROCESSED / "regional" / "proxy_v4_config.json"

START_YEAR = 2009
END_YEAR   = 2025

# ── Configuración provincial ──────────────────────────────────────────────────
PROVINCES = {
    "sc_tenerife": ["tenerife", "la_palma", "gomera", "hierro"],
    "las_palmas":  ["gran_canaria",  "lanzaftv"],
}

# Rutas a masters insulares (mismos que usó build_master_provincial_2009_2025)
ISLAND_MASTER = {
    "tenerife":      PROCESSED / "tenerife"     / "master" / "master_tfe_2004_2025.parquet",
    "gran_canaria":  PROCESSED / "gran_canaria"  / "master" / "master_gcan_2004_2025.parquet",
    "la_palma":      PROCESSED / "la_palma"      / "master" / "master_lpa_2004_2025.parquet",
    "gomera":        PROCESSED / "gomera"         / "master" / "master_gom_2004_2025.parquet",
    "hierro":        PROCESSED / "hierro" / "master" / "master_hie_2004_2025.parquet",
    "lanzaftv":      PROCESSED / "lanzaftv"      / "master" / "master_lztftv_2004_2025.parquet",
}

# Poblaciones medias por isla (INE / padrón, consistentes con v2)
POP = {
    "tenerife":      917_841,
    "gran_canaria":  851_231,
    "la_palma":       82_671,
    "gomera":         21_503,
    "hierro":        11_147,
    "lanzaftv":      275_544,
}

# Variables físicas que necesita el proxy v4
V4_FEATURES = ["PM10", "PM2.5", "vis_min_m_week"]


# ── Proxy v4 ──────────────────────────────────────────────────────────────────
def load_proxy_config(config_path: Path) -> dict:
    with open(config_path) as f:
        cfg = json.load(f)
    log.info(f"Proxy v4 config cargado — AUC={cfg['auc_calibration']}")
    return cfg


def apply_proxy_v4(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """
    Aplica proxy v4 sobre un DataFrame que ya tiene PM10, PM2.5, vis_min_m_week
    agregados a nivel provincial. Devuelve el mismo df con columnas añadidas:
        calima_score, calima_level, calima_ordinal
    """
    features   = cfg["features"]
    mean_      = np.array(cfg["scaler_mean"])
    std_       = np.array(cfg["scaler_std"])
    coef_      = np.array(cfg["logit_coef"])
    intercept_ = cfg["logit_intercept"]
    thr        = cfg["thresholds"]

    df = df.copy()

    # Imputar nulls con mediana del dataset (mismo approach que build_master_regional)
    for feat in features:
        nulls = df[feat].isna().sum()
        if nulls > 0:
            median_val = df[feat].median()
            df[feat] = df[feat].fillna(median_val)
            log.warning(f"  {feat}: {nulls} nulls imputados con mediana ({median_val:.2f})")

    X        = df[features].values
    X_scaled = (X - mean_) / std_
    log_odds  = X_scaled @ coef_ + intercept_
    prob      = 1 / (1 + np.exp(-log_odds))

    df["calima_score"] = prob.round(6)
    df["calima_level"] = pd.cut(
        df["calima_score"],
        bins=[-np.inf,
              thr["no_calima_max"],
              thr["possible_max"],
              thr["probable_max"],
              np.inf],
        labels=["no_calima", "possible", "probable", "intense"],
    )
    df["calima_ordinal"] = df["calima_level"].map(
        {"no_calima": 0, "possible": 1, "probable": 2, "intense": 3}
    ).astype(int)

    return df


# ── Carga y agregación por provincia ─────────────────────────────────────────
def load_island_masters(province_islands: list[str]) -> dict[str, pd.DataFrame]:
    dfs = {}
    for island in province_islands:
        path = ISLAND_MASTER[island]
        if not path.exists():
            raise FileNotFoundError(f"Master insular no encontrado: {path}")
        df = pd.read_parquet(path)
        df["week_start"] = pd.to_datetime(df["week_start"])
        # Filtrar al período fiable del proxy v4
        df = df[df["week_start"].dt.year >= START_YEAR].copy()
        df["deaths_week"] = df["deaths_week"].fillna(0)
        dfs[island] = df
        log.info(f"  {island}: {len(df)} semanas cargadas")
    return dfs


def aggregate_province(province_islands: list[str], island_dfs: dict) -> pd.DataFrame:
    """
    Agrega muertes, temperatura y variables v4 al nivel provincial.
    - deaths:        suma directa
    - temp_c_mean:   media ponderada por población
    - PM10, PM2.5, vis_min_m_week: media ponderada por población
    """
    total_pop = sum(POP[i] for i in province_islands)

    # --- Muertes ---
    deaths_frames = [
        island_dfs[i][["week_start", "deaths_week"]].copy()
        for i in province_islands
    ]
    deaths = (
        pd.concat(deaths_frames)
        .groupby("week_start", as_index=False)
        .agg(deaths=("deaths_week", "sum"))
    )

    # --- Variables ponderadas ---
    weighted_vars = ["temp_c_mean"] + V4_FEATURES
    weighted_frames = []

    for island in province_islands:
        w = POP[island] / total_pop
        df_i = island_dfs[island][["week_start"] + weighted_vars].copy()

        for var in weighted_vars:
            # Imputar nulls insulares antes de ponderar
            nulls = df_i[var].isna().sum()
            if nulls > 0:
                df_i[var] = df_i[var].fillna(df_i[var].median())
                log.debug(f"    {island}.{var}: {nulls} nulls imputados")
            df_i[f"{var}_w"] = df_i[var] * w

        weighted_frames.append(df_i[["week_start"] + [f"{v}_w" for v in weighted_vars]])

    combined = (
        pd.concat(weighted_frames)
        .groupby("week_start", as_index=False)
        .sum()
    )
    # Renombrar columnas ponderadas a nombre original
    combined.rename(columns={f"{v}_w": v for v in weighted_vars}, inplace=True)

    # --- Unir ---
    master = deaths.merge(combined, on="week_start", how="left")
    master = master.sort_values("week_start").reset_index(drop=True)

    return master


# ── QA básico ────────────────────────────────────────────────────────────────
def run_qa(df: pd.DataFrame, province: str) -> None:
    log.info(f"\n--- QA: {province} ---")
    log.info(f"  Shape: {df.shape}")
    log.info(f"  Rango: {df['week_start'].min().date()} → {df['week_start'].max().date()}")
    log.info(f"  Nulls totales: {df.isnull().sum().sum()}")
    log.info(f"  Muertes rango: {df['deaths'].min():.0f} – {df['deaths'].max():.0f}")
    log.info(f"  calima_score rango: {df['calima_score'].min():.4f} – {df['calima_score'].max():.4f}")
    dist = df["calima_level"].value_counts()
    log.info(f"  Distribución calima_level:\n{dist.to_string()}")
    dupes = df["week_start"].duplicated().sum()
    if dupes > 0:
        log.error(f"  ⚠ {dupes} semanas duplicadas!")
    else:
        log.info("  ✓ Sin semanas duplicadas")


# ── Main ─────────────────────────────────────────────────────────────────────
def main() -> None:
    log.info("=== build_master_provincial_v4 ===\n")
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    cfg = load_proxy_config(CONFIG_V4)

    for province, islands in PROVINCES.items():
        log.info(f"\n{'='*60}")
        log.info(f"Provincia: {province.upper()} — islas: {islands}")
        log.info(f"{'='*60}")

        # 1. Cargar masters insulares
        island_dfs = load_island_masters(islands)

        # 2. Agregar a nivel provincial
        log.info("Agregando variables provinciales...")
        master = aggregate_province(islands, island_dfs)

        # 3. Aplicar proxy v4
        log.info("Aplicando proxy v4...")
        master = apply_proxy_v4(master, cfg)

        # 4. Añadir columnas de contexto
        master["province"] = province
        master["proxy_version"] = "v4"

        # 5. Orden de columnas final
        cols = [
            "week_start", "province", "proxy_version",
            "deaths", "temp_c_mean",
            "PM10", "PM2.5", "vis_min_m_week",
            "calima_score", "calima_level", "calima_ordinal",
        ]
        master = master[cols]

        # 6. QA
        run_qa(master, province)

        # 7. Guardar
        out_path = OUT_DIR / f"master_provincial_{province}_v4_{START_YEAR}_{END_YEAR}.parquet"
        master.to_parquet(out_path, index=False)
        log.info(f"\n✅ Guardado: {out_path.name}  ({len(master)} filas)")

    log.info("\n=== DONE ===")


if __name__ == "__main__":
    main()
