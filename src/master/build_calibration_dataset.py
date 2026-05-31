# build_calibration_dataset.py
#
# Construye el dataset de calibración para el proxy regional de calima.
#
# Qué hace:
# 1) Lee variables físicas semanales (weather + air_quality + visibility) de las
#    6 unidades insulares: tenerife, gran_canaria, lanzaftv, la_palma, gomera, hierro.
# 2) Agrega a nivel regional como MEDIA SIMPLE por semana (todas las islas pesan igual).
# 3) Lee el DAI (calima_general_weekly.parquet) y los CAP insulares.
# 4) Construye el label de calima regional:
#      calima_label = 1 si DAI >= 1 OR CAP dust >= amarillo en alguna isla
# 5) Une variables físicas regionales + label en un único parquet de calibración.
#
# Output: data/processed/regional/calibration_dataset_2004_2025.parquet
#
# Uso: python -m src.master.build_calibration_dataset

from __future__ import annotations

from pathlib import Path
from typing import List

import pandas as pd

from src.utils.constants import island_code
from src.utils.dates import normalize_week_start

# Islas que entran en el master regional
# lanzaftv = lanzarote + fuerteventura combinadas (no usar lzt/ftv por separado)
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

# Variables físicas que queremos del weather
WEATHER_VARS = ["temp_c_mean", "tmax_c_mean", "tmin_c_mean",
                "humidity_mean", "pressure_hpa_mean", "wind_ms_mean"]

# Variables de air quality
AIRQ_VARS = ["PM10", "PM2.5"]

# Variables de visibility
VIS_VARS = ["vis_min_m_week", "low_vis_any_week"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def read_concat(base_dir: Path, pattern: str) -> pd.DataFrame:
    """Lee todos los parquets que matchean el patrón, concatena y deduplica por week_start."""
    paths = sorted(base_dir.glob(pattern))
    if not paths:
        raise FileNotFoundError(f"No files matching '{pattern}' in {base_dir}")
    frames = [pd.read_parquet(p) for p in paths]
    df = pd.concat(frames, ignore_index=True)
    df["week_start"] = normalize_week_start(df["week_start"])
    df = df.sort_values("week_start").drop_duplicates(subset=["week_start"], keep="last")
    return df.reset_index(drop=True)


def extract_vars(df: pd.DataFrame, wanted: List[str]) -> pd.DataFrame:
    """Devuelve solo week_start + las columnas disponibles de wanted."""
    cols = ["week_start"] + [c for c in wanted if c in df.columns]
    missing = [c for c in wanted if c not in df.columns]
    if missing:
        print(f"  [warn] columnas no disponibles: {missing}")
    return df[cols].copy()


# ---------------------------------------------------------------------------
# Carga por isla
# ---------------------------------------------------------------------------

def load_island_vars(island: str) -> pd.DataFrame:
    """
    Carga weather + air_quality + visibility para una isla,
    hace merge por week_start y devuelve un DataFrame con todas las variables.
    """
    code = island_code(island)
    idir = PROCESSED_DIR / island

    print(f"  Cargando {island} ({code})...")

    # Weather
    weather_raw = read_concat(idir / "weather", f"weather_weekly_{code}_*.parquet")
    weather = extract_vars(weather_raw, WEATHER_VARS)

    # Air quality
    airq_raw = read_concat(idir / "air_quality", f"weekly_{code}_*.parquet")
    airq = extract_vars(airq_raw, AIRQ_VARS)

    # Visibility
    vis_raw = read_concat(idir / "visibility", f"visibility_weekly_{code}_*.parquet")
    vis = extract_vars(vis_raw, VIS_VARS)

    # Merge por week_start (outer para no perder semanas)
    df = weather.merge(airq, on="week_start", how="outer")
    df = df.merge(vis, on="week_start", how="outer")
    df = df.sort_values("week_start").reset_index(drop=True)

    return df


# ---------------------------------------------------------------------------
# Agregación regional
# ---------------------------------------------------------------------------

def build_regional_vars(start_year: int = 2004, end_year: int = 2025) -> pd.DataFrame:
    """
    Carga todas las islas y calcula la media simple de cada variable por semana.
    Weeks sin ninguna isla con dato quedan como NaN (no se imputa aquí).
    """
    print("Cargando variables insulares...")
    island_dfs = {}
    for island in REGIONAL_ISLANDS:
        island_dfs[island] = load_island_vars(island)

    # Calendario de referencia
    analysis_start = pd.Timestamp(f"{start_year}-01-01")
    analysis_end   = pd.Timestamp(f"{end_year}-12-31")
    # Ajustar a inicio de semana (lunes)
    analysis_start -= pd.to_timedelta(analysis_start.weekday(), unit="D")
    analysis_end   -= pd.to_timedelta(analysis_end.weekday(), unit="D")

    all_weeks = pd.date_range(start=analysis_start, end=analysis_end, freq="W-MON")
    calendar = pd.DataFrame({"week_start": all_weeks})

    # Alinear cada isla al calendario
    aligned = []
    for island, df in island_dfs.items():
        df = df[(df["week_start"] >= analysis_start) & (df["week_start"] <= analysis_end)]
        df = calendar.merge(df, on="week_start", how="left")
        aligned.append(df)

    # Media simple: apilamos y agrupamos por semana
    all_vars = list(set(WEATHER_VARS + AIRQ_VARS + VIS_VARS))
    stacked = pd.concat(aligned, ignore_index=True)
    regional = stacked.groupby("week_start")[all_vars].mean().reset_index()
    regional = calendar.merge(regional, on="week_start", how="left")

    print(f"Variables regionales: {len(regional)} semanas, {len(regional.columns)} columnas")
    return regional


# ---------------------------------------------------------------------------
# Labels: DAI + CAP
# ---------------------------------------------------------------------------

def load_dai() -> pd.DataFrame:
    """
    Lee el DAI regional (calima_general_weekly.parquet).
    Devuelve week_start + dai_event (1 si calima_canarias_level_week >= 1).
    """
    path = PROCESSED_DIR / "calima" / "calima_general_weekly.parquet"
    df = pd.read_parquet(path)

    # Convertir timestamps si vienen en ms
    if df["week_start"].dtype == "int64":
        df["week_start"] = pd.to_datetime(df["week_start"], unit="ms")
    else:
        df["week_start"] = normalize_week_start(df["week_start"])

    df = df.sort_values("week_start").drop_duplicates(subset=["week_start"])
    df["dai_event"] = (df["calima_canarias_level_week"] >= 1).astype(int)

    return df[["week_start", "calima_canarias_dai_week", "calima_canarias_level_week", "dai_event"]]


def load_cap_regional() -> pd.DataFrame:
    """
    Lee los CAP de todas las islas con datos CAP disponibles y construye
    un indicador regional: cap_dust_event = 1 si alguna isla tuvo alerta
    de polvo nivel >= 2 (amarillo) esa semana.
    """
    cap_islands = ["tenerife", "gran_canaria", "la_palma", "gomera", "hierro"]
    frames = []

    for island in cap_islands:
        code = island_code(island)
        cap_dir = PROCESSED_DIR / island / "cap"
        paths = sorted(cap_dir.glob(f"cap_weekly_{code}_*.parquet"))
        if not paths:
            print(f"  [warn] CAP no disponible para {island}")
            continue
        df = pd.concat([pd.read_parquet(p) for p in paths], ignore_index=True)
        df["week_start"] = normalize_week_start(df["week_start"])
        df = df.sort_values("week_start").drop_duplicates(subset=["week_start"], keep="last")

        if "cap_dust_level_max_week" not in df.columns:
            print(f"  [warn] cap_dust_level_max_week no encontrada en {island}")
            continue

        df = df[["week_start", "cap_dust_level_max_week"]].rename(
            columns={"cap_dust_level_max_week": f"cap_dust_{code}"}
        )
        frames.append(df)

    if not frames:
        raise RuntimeError("No se encontraron datos CAP para ninguna isla")

    # Merge secuencial por week_start
    merged = frames[0]
    for f in frames[1:]:
        merged = merged.merge(f, on="week_start", how="outer")

    # cap_dust_event = 1 si alguna isla tiene nivel >= 2
    dust_cols = [c for c in merged.columns if c.startswith("cap_dust_")]
    merged["cap_dust_max_regional"] = merged[dust_cols].max(axis=1)
    merged["cap_dust_event"] = (merged["cap_dust_max_regional"] >= 2).astype(int)

    return merged[["week_start", "cap_dust_max_regional", "cap_dust_event"]]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("=== build_calibration_dataset ===\n")

    # 1. Variables físicas regionales
    regional = build_regional_vars(start_year=2004, end_year=2025)

    # 2. DAI
    print("\nCargando DAI...")
    dai = load_dai()
    print(f"  DAI: {len(dai)} semanas | eventos: {dai['dai_event'].sum()}")
    print(f"  Rango DAI: {dai['week_start'].min().date()} → {dai['week_start'].max().date()}")

    # 3. CAP regional
    print("\nCargando CAP...")
    cap = load_cap_regional()
    print(f"  CAP: {len(cap)} semanas | eventos dust: {cap['cap_dust_event'].sum()}")
    print(f"  Rango CAP: {cap['week_start'].min().date()} → {cap['week_start'].max().date()}")

    # 4. Unir todo
    print("\nUniendo datasets...")
    df = regional.merge(dai, on="week_start", how="left")
    df = df.merge(cap, on="week_start", how="left")

    # 5. Construir calima_label
    # calima_label = 1 si DAI OR CAP dust en ventana de solapamiento
    # Fuera de ventana DAI/CAP → NaN (no se puede etiquetar)
    dai_available  = df["dai_event"].notna()
    cap_available  = df["cap_dust_event"].notna()
    any_label_available = dai_available | cap_available

    df["calima_label"] = None
    mask_labeled = any_label_available
    df.loc[mask_labeled, "calima_label"] = (
        (df.loc[mask_labeled, "dai_event"].fillna(0) == 1) |
        (df.loc[mask_labeled, "cap_dust_event"].fillna(0) == 1)
    ).astype(int)

    # Flag de fiabilidad del proxy (PM10 muy esparso antes de 2009, documentado)
    df["proxy_reliable"] = (df["week_start"] >= pd.Timestamp("2009-01-01")).astype(int)

    # 6. Diagnóstico
    print(f"\nDataset final: {len(df)} semanas")
    print(f"Semanas con calima_label disponible: {df['calima_label'].notna().sum()}")
    labeled = df[df["calima_label"].notna()]
    print(f"  calima_label=1: {int(labeled['calima_label'].sum())}")
    print(f"  calima_label=0: {int((labeled['calima_label'] == 0).sum())}")
    print(f"  Rango labelado: {labeled['week_start'].min().date()} → {labeled['week_start'].max().date()}")
    print(f"\nNulls por variable física:")
    phys_vars = WEATHER_VARS + AIRQ_VARS + VIS_VARS
    for col in phys_vars:
        if col in df.columns:
            n = int(df[col].isna().sum())
            pct = 100 * n / len(df)
            print(f"  {col}: {n} ({pct:.1f}%)")

    # 7. Guardar
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    outpath = OUT_DIR / "calibration_dataset_2004_2025.parquet"
    df.to_parquet(outpath, index=False)
    print(f"\nGuardado: {outpath}")


if __name__ == "__main__":
    main()
