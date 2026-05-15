# =============================================================================
# build_regression_tfe_gc.py
# =============================================================================
# Descripción:
#   Construye el dataset de regresión para Tenerife (TFE) y Gran Canaria (GC).
#   Mergea el master semanal de cada isla con su calima proxy v2, selecciona
#   las columnas relevantes para modelado y exporta un único parquet combinado.
#
# Input:
#   data/processed/tenerife/master/master_tfe_2016_2025.parquet
#   data/processed/tenerife/calima/calima_proxy_weekly_tfe_2016_2025.parquet
#   data/processed/gran_canaria/master/master_gcan_2016_2025.parquet
#   data/processed/gran_canaria/calima/calima_proxy_weekly_gcan_2016_2025.parquet
#
# Output:
#   data/processed/provinces/regression_tfe_gc_2016_2025.parquet
#
# Ejecución desde PowerShell (situarse en la raíz del proyecto):
#   cd C:\Users\fdora\RA_Career\Projects\climate_mortality
#   python src\master\ccaa\build_regression_tfe_gc.py
# =============================================================================

import pandas as pd
from pathlib import Path

# ---------------------------------------------------------------------------
# Rutas
# ---------------------------------------------------------------------------
BASE = Path(__file__).resolve().parents[3]  # raíz del proyecto

PATHS = {
    "master_tfe":  BASE / "data/processed/tenerife/master/master_tfe_2016_2025.parquet",
    "proxy_tfe":   BASE / "data/processed/tenerife/calima/calima_proxy_weekly_tfe_2016_2025.parquet",
    "master_gcan": BASE / "data/processed/gran_canaria/master/master_gcan_2016_2025.parquet",
    "proxy_gcan":  BASE / "data/processed/gran_canaria/calima/calima_proxy_weekly_gcan_2016_2025.parquet",
}

OUTPUT = BASE / "data/processed/provinces/regression_tfe_gc_2016_2025.parquet"

# ---------------------------------------------------------------------------
# Columnas a conservar del master (antes del merge)
# ---------------------------------------------------------------------------
MASTER_COLS = [
    "week_start",
    "year",
    "island",
    "deaths_week",
    "temp_c_mean",
    "tmax_c_mean",
    "tmin_c_mean",
    "humidity_mean",
    "prec_sum",
    "PM10",
    "PM2.5",
    "low_vis_any_days_week",
    "vis_min_m_week",
]

# ---------------------------------------------------------------------------
# Función: cargar y mergear una isla
# ---------------------------------------------------------------------------
def build_island(master_path: Path, proxy_path: Path) -> pd.DataFrame:
    master = pd.read_parquet(master_path)[MASTER_COLS]
    proxy  = pd.read_parquet(proxy_path)[["week_start", "calima_proxy_score", "calima_proxy_level"]]

    df = master.merge(proxy, on="week_start", how="left")

    # Verificación básica
    n_missing_proxy = df["calima_proxy_score"].isnull().sum()
    if n_missing_proxy > 0:
        print(f"  ⚠️  {master_path.stem}: {n_missing_proxy} semanas sin calima proxy tras el merge")

    return df

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print("=" * 60)
    print("build_regression_tfe_gc.py")
    print("=" * 60)

    # Verificar que existen los inputs
    for name, path in PATHS.items():
        if not path.exists():
            raise FileNotFoundError(f"Input no encontrado: {path}")
        print(f"  ✅ {name}: {path.name}")

    print("\nProcesando TFE...")
    tfe  = build_island(PATHS["master_tfe"],  PATHS["proxy_tfe"])
    print(f"  Filas TFE:  {len(tfe)}")

    print("\nProcesando GC...")
    gcan = build_island(PATHS["master_gcan"], PATHS["proxy_gcan"])
    print(f"  Filas GC:   {len(gcan)}")

    # Combinar
    df = pd.concat([tfe, gcan], ignore_index=True)
    df = df.sort_values(["island", "week_start"]).reset_index(drop=True)

    print(f"\nDataset combinado: {df.shape[0]} filas × {df.shape[1]} columnas")

    # Resumen de nulls
    missing = df.isnull().sum()
    missing = missing[missing > 0]
    if not missing.empty:
        print("\nNulls por columna:")
        print(missing.to_string())
    else:
        print("\n✅ Sin nulls en el dataset")

    # Exportar
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUTPUT, index=False)
    print(f"\n✅ Output guardado en: {OUTPUT}")
    print("=" * 60)


if __name__ == "__main__":
    main()