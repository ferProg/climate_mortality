# build_master_regional.py
#
# Construye el master regional semanal de Canarias 2004-2025.
#
# Qué hace:
# 1) Lee variables físicas (weather + air_quality + visibility) de las 6 islas
#    y las agrega como media simple regional.
# 2) Suma muertes semanales de todas las islas.
# 3) Aplica el proxy v4 (regresión logística calibrada) usando PM10, PM2.5,
#    vis_min_m_week → calima_score [0-1] + calima_level (ordinal 0-3).
# 4) Añade flag proxy_reliable (1 desde 2009, 0 antes).
# 5) Guarda dos parquets:
#    - master_regional_2004_2025.parquet  (completo)
#    - master_regional_2009_2025.parquet  (periodo fiable, para regresión)
#
# Uso: python -m src.master.build_master_regional

from __future__ import annotations

import json
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd

from src.utils.constants import island_code
from src.utils.dates import normalize_week_start

REGIONAL_ISLANDS = [
    "tenerife",
    "gran_canaria",
    "lanzaftv",
    "la_palma",
    "gomera",
    "hierro",
]

PROCESSED_DIR = Path("data/processed")
OUT_DIR = PROCESSED_DIR / "regional"

WEATHER_VARS = [
    "temp_c_mean", "tmax_c_mean", "tmin_c_mean",
    "humidity_mean", "pressure_hpa_mean", "wind_ms_mean",
]
AIRQ_VARS = ["PM10", "PM2.5"]
VIS_VARS  = ["vis_min_m_week", "low_vis_any_week"]


# ---------------------------------------------------------------------------
# Helpers (mismos que en build_calibration_dataset)
# ---------------------------------------------------------------------------

def read_concat(base_dir: Path, pattern: str) -> pd.DataFrame:
    paths = sorted(base_dir.glob(pattern))
    if not paths:
        raise FileNotFoundError(f"No files matching '{pattern}' in {base_dir}")
    df = pd.concat([pd.read_parquet(p) for p in paths], ignore_index=True)
    df["week_start"] = normalize_week_start(df["week_start"])
    df = (df.sort_values("week_start")
            .drop_duplicates(subset=["week_start"], keep="last")
            .reset_index(drop=True))
    return df


def extract_vars(df: pd.DataFrame, wanted: List[str]) -> pd.DataFrame:
    cols = ["week_start"] + [c for c in wanted if c in df.columns]
    return df[cols].copy()


# ---------------------------------------------------------------------------
# Carga por isla
# ---------------------------------------------------------------------------

def load_island_physical(island: str) -> pd.DataFrame:
    code = island_code(island)
    idir = PROCESSED_DIR / island

    weather = extract_vars(
        read_concat(idir / "weather", f"weather_weekly_{code}_*.parquet"),
        WEATHER_VARS,
    )
    airq = extract_vars(
        read_concat(idir / "air_quality", f"weekly_{code}_*.parquet"),
        AIRQ_VARS,
    )
    vis = extract_vars(
        read_concat(idir / "visibility", f"visibility_weekly_{code}_*.parquet"),
        VIS_VARS,
    )

    df = weather.merge(airq, on="week_start", how="outer")
    df = df.merge(vis, on="week_start", how="outer")
    return df.sort_values("week_start").reset_index(drop=True)


def load_island_deaths(island: str) -> pd.DataFrame:
    code = island_code(island)
    idir = PROCESSED_DIR / island / "deaths"
    df = read_concat(idir, f"deaths_weekly_{code}_*.parquet")
    if "deaths_week" not in df.columns:
        raise ValueError(f"{island}: columna 'deaths_week' no encontrada")
    return df[["week_start", "deaths_week"]].copy()


# ---------------------------------------------------------------------------
# Agregación regional
# ---------------------------------------------------------------------------

def build_regional(start_year: int = 2004, end_year: int = 2025) -> pd.DataFrame:
    # Calendario de referencia
    t_start = pd.Timestamp(f"{start_year}-01-01")
    t_end   = pd.Timestamp(f"{end_year}-12-31")
    t_start -= pd.to_timedelta(t_start.weekday(), unit="D")
    t_end   -= pd.to_timedelta(t_end.weekday(), unit="D")
    calendar = pd.DataFrame({"week_start": pd.date_range(t_start, t_end, freq="W-MON")})

    all_phys  = list(set(WEATHER_VARS + AIRQ_VARS + VIS_VARS))
    phys_frames  = []
    deaths_frames = []

    for island in REGIONAL_ISLANDS:
        print(f"  {island}...")

        # Variables físicas
        phys = load_island_physical(island)
        phys = phys[(phys["week_start"] >= t_start) & (phys["week_start"] <= t_end)]
        phys = calendar.merge(phys, on="week_start", how="left")
        phys_frames.append(phys)

        # Muertes
        try:
            deaths = load_island_deaths(island)
            deaths = deaths[(deaths["week_start"] >= t_start) & (deaths["week_start"] <= t_end)]
            deaths = calendar.merge(deaths, on="week_start", how="left")
            deaths = deaths.rename(columns={"deaths_week": f"deaths_{island}"})
            deaths_frames.append(deaths)
        except Exception as e:
            print(f"    [warn] muertes no disponibles: {e}")

    # Media simple de variables físicas
    stacked = pd.concat(phys_frames, ignore_index=True)
    regional_phys = stacked.groupby("week_start")[all_phys].mean().reset_index()
    regional = calendar.merge(regional_phys, on="week_start", how="left")

    # Suma de muertes
    deaths_merged = calendar.copy()
    for df in deaths_frames:
        deaths_merged = deaths_merged.merge(df, on="week_start", how="left")

    death_cols = [c for c in deaths_merged.columns if c.startswith("deaths_")]
    deaths_merged["deaths_week"] = deaths_merged[death_cols].sum(axis=1, min_count=1)

    regional = regional.merge(
        deaths_merged[["week_start", "deaths_week"]], on="week_start", how="left"
    )

    return regional


# ---------------------------------------------------------------------------
# Proxy v4
# ---------------------------------------------------------------------------

def apply_proxy_v4(df: pd.DataFrame, config_path: Path) -> pd.DataFrame:
    with open(config_path) as f:
        cfg = json.load(f)

    features   = cfg["features"]          # ['PM10', 'PM2.5', 'vis_min_m_week']
    mean_       = np.array(cfg["scaler_mean"])
    std_        = np.array(cfg["scaler_std"])
    coef_       = np.array(cfg["logit_coef"])
    intercept_  = cfg["logit_intercept"]
    thr         = cfg["thresholds"]

    df = df.copy()

    # Imputar nulls con mediana del propio dataset antes de aplicar proxy
    for feat in features:
        if feat in df.columns:
            df[feat] = df[feat].fillna(df[feat].median())
        else:
            raise ValueError(f"Feature '{feat}' no encontrada en el master regional")

    X = df[features].values
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
    )

    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("=== build_master_regional ===\n")
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Agregación regional
    print("Agregando variables insulares...")
    regional = build_regional(start_year=2004, end_year=2025)

    # 2. Aplicar proxy v4
    print("\nAplicando proxy v4...")
    config_path = OUT_DIR / "proxy_v4_config.json"
    regional = apply_proxy_v4(regional, config_path)

    # 3. Metadatos
    regional["year"]  = regional["week_start"].dt.year
    regional["month"] = regional["week_start"].dt.month
    regional["proxy_reliable"] = (regional["week_start"] >= "2009-01-01").astype(int)

    # 3b. Normalización demográfica: deaths per 100,000 inhabitants
    # Fuente: ISTAC Padrón Municipal — cifras oficiales anuales Canarias 2009–2025
    pop = pd.read_parquet(PROCESSED_DIR / "population" / "population_canarias_2009_2025.parquet")
    regional = regional.merge(pop, on="year", how="left")
    regional["deaths_per_100k"] = regional["deaths_week"] / (regional["population"] / 100_000)

    # Reordenar columnas
    first_cols = ["week_start", "year", "month", "deaths_week", "deaths_per_100k",
                  "calima_score", "calima_level", "calima_ordinal", "proxy_reliable"]
    other_cols = [c for c in regional.columns if c not in first_cols]
    regional = regional[first_cols + other_cols]

    # 4. Diagnóstico
    print(f"\nShape: {regional.shape}")
    print(f"Rango: {regional['week_start'].min().date()} → {regional['week_start'].max().date()}")
    print(f"\nNulls:")
    print(regional.isnull().sum()[regional.isnull().sum() > 0])
    print(f"\nDistribución calima_level:")
    print(regional["calima_level"].value_counts().sort_index())
    print(f"\nMuertes — suma total: {regional['deaths_week'].sum():,.0f}")
    print(f"Muertes — media semanal: {regional['deaths_week'].mean():.1f}")
    print(f"\nMuertes/100k — media semanal (2009+): {regional.loc[regional['proxy_reliable']==1, 'deaths_per_100k'].mean():.4f}")
    print(f"Muertes/100k — rango (2009+): {regional.loc[regional['proxy_reliable']==1, 'deaths_per_100k'].min():.4f} – {regional.loc[regional['proxy_reliable']==1, 'deaths_per_100k'].max():.4f}")
    n_pop_null = regional["deaths_per_100k"].isna().sum()
    if n_pop_null:
        print(f"[warn] deaths_per_100k nulos: {n_pop_null} filas (años sin datos de población)")

    # 5. Guardar
    path_full = OUT_DIR / "master_regional_2004_2025.parquet"
    regional.to_parquet(path_full, index=False)
    print(f"\nGuardado: {path_full}")

    path_reliable = OUT_DIR / "master_regional_2009_2025.parquet"
    reliable = regional[regional["proxy_reliable"] == 1].reset_index(drop=True)
    reliable.to_parquet(path_reliable, index=False)
    print(f"Guardado: {path_reliable}  ({len(reliable)} semanas)")


if __name__ == "__main__":
    main()
