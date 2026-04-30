"""
build_calima_proxy_v2.py

Construye proxy de calima v2 en dos fases:

Fase 1: Calcula tmax_anomaly
  - Lee master de island
  - Calcula baseline (media + std de tmax_c_max por mes, excluyendo calima confirmada)
  - Guarda en data/interim/<island>/weather/tmax_anomaly_<island_code>_<YYYY>_<YYYY>.parquet

Fase 2: Construye calima proxy v2
  - Lee tmax_anomaly desde interim
  - Lee master para PM10, PM2.5, visibilidad, humedad
  - Interpola/imputa valores faltantes (interpolación lineal + media global)
  - Computa calima_proxy_score (pesos: PM10 1.0, PM2.5 0.75, vis 0.5, humedad 0.25, tmax 0.5)
  - Genera calima_proxy_level (0, 1, 2, 3)
  - Guarda en data/processed/<island>/calima/calima_proxy_v2_weekly_<island_code>_<YYYY>_<YYYY>.parquet

Uso:
   python src\master\calima_per_island\build_calima_proxy_v2.py --island <island> --start-year <YYYY> --end-year <YYYY>
"""

import argparse
import pandas as pd
import numpy as np
from pathlib import Path
import sys

# Island mapping
ISLAND_MAP = {
    "fuerteventura": "ftv",
    "lanzarote": "lzt",
    "tenerife": "tfe",
    "gran_canaria": "gcan",
    "la_palma": "lpa",
    "gomera": "gom",
    "hierro": "hie",
}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Build calima proxy v2 with tmax anomaly"
    )
    parser.add_argument(
        "--island",
        required=True,
        choices=ISLAND_MAP.keys(),
        help="Island name (e.g., tenerife)",
    )
    parser.add_argument(
        "--start-year", type=int, default=2016, help="Start year (default: 2016)"
    )
    parser.add_argument(
        "--end-year", type=int, default=2025, help="End year (default: 2025)"
    )
    return parser.parse_args()


def load_master(root, island, island_code, start_year, end_year):
    """Load master parquet for island"""
    fp = root / f"data/processed/{island}/master/master_{island_code}_{start_year}_{end_year}.parquet"
    if not fp.exists():
        raise FileNotFoundError(f"Master not found: {fp}")
    return pd.read_parquet(fp)


def phase1_calculate_tmax_anomaly(root, island, island_code, start_year, end_year, df):
    """
    Phase 1: Calculate tmax_anomaly
    - Baseline: mean + std of tmax_c_max per month, excluding calima_level_week >= 1
    - Anomaly: (tmax_c_max - baseline_mean) / baseline_std
    - Save to data/interim/<island>/weather/tmax_anomaly_<island_code>_<YYYY>_<YYYY>.parquet
    """
    print(f"\n{'='*70}")
    print(f"PHASE 1: Calculate tmax_anomaly for {island.upper()}")
    print(f"{'='*70}")

    # Extract month
    df["month"] = df["week_start"].dt.month

    # Filter to weeks without calima (calima_level_week == 0 or null)
    no_calima = df[
        (df["calima_level_week"] == 0) | (df["calima_level_week"].isna())
    ].copy()

    print(f"Total weeks: {len(df)}")
    print(f"Weeks without calima (for baseline): {len(no_calima)}")

    # Calculate baseline (mean + std) per month
    baseline = (
        no_calima.groupby("month")["tmax_c_max"]
        .agg(["mean", "std"])
        .reset_index()
    )
    baseline.columns = ["month", "baseline_mean", "baseline_std"]

    print(f"Baseline coverage: {len(baseline)} months")

    # Merge baseline back to full dataset
    df = df.merge(baseline, on="month", how="left")

    # Calculate anomaly
    df["tmax_anomaly"] = (
        (df["tmax_c_max"] - df["baseline_mean"]) / df["baseline_std"]
    )

    # Keep only necessary columns for interim output
    tmax_anom_df = df[
        [
            "week_start",
            "tmax_c_max",
            "month",
            "baseline_mean",
            "baseline_std",
            "tmax_anomaly",
        ]
    ].copy()

    # Save to interim
    interim_dir = root / f"data/interim/{island}/weather"
    interim_dir.mkdir(parents=True, exist_ok=True)
    interim_fp = interim_dir / f"tmax_anomaly_{island_code}_{start_year}_{end_year}.parquet"

    tmax_anom_df.to_parquet(interim_fp, index=False)
    print(f"✓ Saved tmax_anomaly to: {interim_fp}")

    return tmax_anom_df


def fill_missing_values(df):
    """
    Estrategia de imputación:
    1. Interpolar nulls aislados (media entre antes/después)
    2. Si variable completamente null, usar media global
    3. Si media global es null, usar 0 (sin efecto en calima)
    
    Retorna df con todas las variables necesarias sin nulls
    """
    variables = ["PM10", "PM2.5", "low_vis_confirmed_any_week", "humidity_mean", "tmax_anomaly"]
    
    print(f"\n  Imputación de valores faltantes:")
    
    for var in variables:
        if df[var].isna().sum() == 0:
            continue  # Sin nulls, skip
        
        # Contar nulls
        n_nulls = df[var].isna().sum()
        n_total = len(df)
        pct_null = (n_nulls / n_total) * 100
        
        print(f"    {var}: {n_nulls}/{n_total} nulls ({pct_null:.1f}%)", end="")
        
        if pct_null == 100:
            # Todo null → usar media global (o 0 si no hay datos)
            global_mean = df[var].mean()
            if pd.isna(global_mean):
                df[var] = 0
                print(f" → Completo null, sustituido con 0")
            else:
                df[var] = df[var].fillna(global_mean)
                print(f" → Completo null, sustituido con media global: {global_mean:.3f}")
        else:
            # Nulls aislados → interpolar linealmente
            df[var] = df[var].interpolate(method="linear", limit_direction="both")
            # Si quedan nulls (inicio/fin), usar media global
            remaining_nulls = df[var].isna().sum()
            if remaining_nulls > 0:
                global_mean = df[var].mean()
                if pd.isna(global_mean):
                    df[var] = df[var].fillna(0)
                    print(f" → Interpolado + sustituido con 0 para {remaining_nulls} valores restantes")
                else:
                    df[var] = df[var].fillna(global_mean)
                    print(f" → Interpolado + media global para {remaining_nulls} valores restantes")
            else:
                print(f" → Interpolado exitosamente")
    
    return df


def phase2_build_calima_proxy(root, island, island_code, start_year, end_year, master_df, tmax_anom_df):
    """
    Phase 2: Build calima proxy v2
    - Interpola/imputa variables faltantes
    - Calcula score normalizado por variables disponibles
    - Genera calima_proxy_level (0, 1, 2, 3)
    - Save to data/processed/<island>/calima/calima_proxy_v2_weekly_<island_code>_<YYYY>_<YYYY>.parquet
    """
    print(f"\n{'='*70}")
    print(f"PHASE 2: Build calima proxy v2 for {island.upper()}")
    print(f"{'='*70}")

    # Merge tmax_anomaly into master
    proxy_df = master_df[["week_start"]].copy()
    proxy_df = proxy_df.merge(
        tmax_anom_df[["week_start", "tmax_anomaly"]], on="week_start", how="left"
    )

    # Add air quality and weather variables from master
    proxy_df = proxy_df.merge(
        master_df[
            [
                "week_start",
                "PM10",
                "PM2.5",
                "low_vis_confirmed_any_week",
                "humidity_mean",
            ]
        ],
        on="week_start",
        how="left",
    )

    print(f"Total weeks: {len(proxy_df)}")
    print(f"\nMissing values ANTES de imputación:")
    print(f"  PM10: {proxy_df['PM10'].isna().sum()}")
    print(f"  PM2.5: {proxy_df['PM2.5'].isna().sum()}")
    print(f"  Visibility: {proxy_df['low_vis_confirmed_any_week'].isna().sum()}")
    print(f"  Humidity: {proxy_df['humidity_mean'].isna().sum()}")
    print(f"  Tmax anomaly: {proxy_df['tmax_anomaly'].isna().sum()}")

    # Imputar valores faltantes
    proxy_df = fill_missing_values(proxy_df)

    print(f"\nMissing values DESPUÉS de imputación:")
    print(f"  PM10: {proxy_df['PM10'].isna().sum()}")
    print(f"  PM2.5: {proxy_df['PM2.5'].isna().sum()}")
    print(f"  Visibility: {proxy_df['low_vis_confirmed_any_week'].isna().sum()}")
    print(f"  Humidity: {proxy_df['humidity_mean'].isna().sum()}")
    print(f"  Tmax anomaly: {proxy_df['tmax_anomaly'].isna().sum()}")

    # Initialize score components
    proxy_df["pm10_score"] = 0.0
    proxy_df["pm25_score"] = 0.0
    proxy_df["vis_score"] = 0.0
    proxy_df["humidity_score"] = 0.0
    proxy_df["tmax_score"] = 0.0

    # PM10 >= 50 µg/m³ (weight 1.0) + PM10 >= 100 µg/m³ (weight 1.0, cumulative)
    proxy_df.loc[proxy_df["PM10"] >= 50, "pm10_score"] += 1.0
    proxy_df.loc[proxy_df["PM10"] >= 100, "pm10_score"] += 1.0

    # PM2.5 >= 20 µg/m³ (weight 0.75)
    proxy_df.loc[proxy_df["PM2.5"] >= 20, "pm25_score"] = 0.75

    # Visibility: any confirmed day in week (weight 0.5)
    proxy_df.loc[proxy_df["low_vis_confirmed_any_week"] > 0, "vis_score"] = 0.5

    # Humidity <= 60% (weight 0.25)
    proxy_df.loc[proxy_df["humidity_mean"] <= 60, "humidity_score"] = 0.25

    # Tmax anomaly > +0.5 std (weight 0.5)
    proxy_df.loc[proxy_df["tmax_anomaly"] > 0.5, "tmax_score"] = 0.5

    # Compute total score (sum of weighted components, normalize by max possible weight 4.0)
    # Max weights: PM10=2.0, PM2.5=0.75, Vis=0.5, Humidity=0.25, Tmax=0.5 → Total=4.0
    max_weight = 2.0 + 0.75 + 0.5 + 0.25 + 0.5
    proxy_df["calima_proxy_score"] = (
        proxy_df["pm10_score"]
        + proxy_df["pm25_score"]
        + proxy_df["vis_score"]
        + proxy_df["humidity_score"]
        + proxy_df["tmax_score"]
    ) / max_weight

    # Generate calima_proxy_level based on score (as numeric first, then map to labels)
    proxy_df["calima_proxy_level"] = 0
    proxy_df.loc[proxy_df["calima_proxy_score"] >= 0.25, "calima_proxy_level"] = 1
    proxy_df.loc[proxy_df["calima_proxy_score"] >= 0.50, "calima_proxy_level"] = 2
    proxy_df.loc[proxy_df["calima_proxy_score"] >= 0.75, "calima_proxy_level"] = 3

    # Map numeric levels to string labels (for consistency with v1 format)
    level_map = {0: "no_calima", 1: "possible", 2: "probable", 3: "intense"}
    proxy_df["calima_proxy_level"] = proxy_df["calima_proxy_level"].map(level_map)

    # Output only week_start, score, and level
    output_df = proxy_df[
        ["week_start", "calima_proxy_score", "calima_proxy_level"]
    ].copy()

    # Save to processed/calima
    calima_dir = root / f"data/processed/{island}/calima"
    calima_dir.mkdir(parents=True, exist_ok=True)
    output_fp = calima_dir / f"calima_proxy_v2_weekly_{island_code}_{start_year}_{end_year}.parquet"

    output_df.to_parquet(output_fp, index=False)
    print(f"\n✓ Saved calima proxy v2 to: {output_fp}")

    # Summary statistics
    print(f"\nCalima proxy v2 distribution:")
    print(f"  Level 0 (no calima): {(output_df['calima_proxy_level'] == 0).sum()} weeks")
    print(f"  Level 1 (possible): {(output_df['calima_proxy_level'] == 1).sum()} weeks")
    print(f"  Level 2 (probable): {(output_df['calima_proxy_level'] == 2).sum()} weeks")
    print(f"  Level 3 (intense): {(output_df['calima_proxy_level'] == 3).sum()} weeks")
    print(
        f"  Score range: {output_df['calima_proxy_score'].min():.3f} - {output_df['calima_proxy_score'].max():.3f}"
    )


def main():
    args = parse_args()
    island = args.island
    island_code = ISLAND_MAP[island]
    start_year = args.start_year
    end_year = args.end_year

    # ROOT is current working directory (climate_mortality)
    root = Path.cwd()
    if root.name != "climate_mortality":
        print(
            f"⚠ Warning: Script should be run from climate_mortality directory. "
            f"Current: {root}"
        )

    print(f"\n{'='*70}")
    print(f"Build Calima Proxy v2 with Tmax Anomaly")
    print(f"{'='*70}")
    print(f"Island: {island} ({island_code})")
    print(f"Years: {start_year}-{end_year}")
    print(f"Root: {root}")

    try:
        # Load master
        print(f"\nLoading master for {island}...")
        master_df = load_master(root, island, island_code, start_year, end_year)
        print(f"✓ Loaded {len(master_df)} weeks")

        # Phase 1: Calculate tmax anomaly
        tmax_anom_df = phase1_calculate_tmax_anomaly(
            root, island, island_code, start_year, end_year, master_df.copy()
        )

        # Phase 2: Build calima proxy
        phase2_build_calima_proxy(
            root, island, island_code, start_year, end_year, master_df, tmax_anom_df
        )

        print(f"\n{'='*70}")
        print(f"✓ SUCCESS: Calima proxy v2 built for {island}")
        print(f"{'='*70}\n")

    except Exception as e:
        print(f"\n✗ ERROR: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()