# Builds a weekly calima proxy dataset from the island master dataset.
#
# Logic:
# - Compute island-specific thresholds from the full weekly master series:
#     PM10 p90, PM10 p95, humidity p25, pressure p75
# - Create binary component flags:
#     high PM10, very high PM10, low humidity, low visibility, high pressure
# - Combine them into a heuristic weekly score:
#     calima_proxy_score_v2
# - Map score to four interpretable levels:
#     no_calima / possible / probable / intense
# - Keep both source variables and derived proxy variables
# - Save one weekly calima dataset per island under data/processed/<island>/calima/

from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd


ISLAND_CODE = {
    "tenerife": "tfe",
    "gran_canaria": "gcan",
    "lanzarote": "lzt",
    "fuerteventura": "ftv",
    "la_palma": "lpa",
    "gomera": "gom",
    "hierro": "hie",
}


def score_to_level_v2(score: float) -> str:
    if score < 1:
        return "no_calima"
    elif score < 2:
        return "possible"
    elif score < 3:
        return "probable"
    return "intense"


def build_calima_proxy(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    df = df.copy()
    df["week_start"] = pd.to_datetime(df["week_start"], errors="coerce")

    required = [
        "week_start",
        "PM10",
        "PM2.5",
        "humidity_mean",
        "pressure_hpa_mean",
        "low_vis_any_week",
        "cap_dust_yellow_plus_week",
        "cap_dust_level_max_week",
        "calima_dai_flag",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    pm10_p90 = df["PM10"].quantile(0.90)
    pm10_p95 = df["PM10"].quantile(0.95)
    hum_p25 = df["humidity_mean"].quantile(0.25)
    pres_p75 = df["pressure_hpa_mean"].quantile(0.75)

    df["pm10_p90_flag"] = (df["PM10"] >= pm10_p90).astype(int)
    df["pm10_p95_flag"] = (df["PM10"] >= pm10_p95).astype(int)
    df["hum_low_flag"] = (df["humidity_mean"] <= hum_p25).astype(int)
    df["low_vis_flag"] = df["low_vis_any_week"].fillna(0).astype(int)
    df["pressure_high_flag"] = (df["pressure_hpa_mean"] >= pres_p75).astype(int)

    df["calima_proxy_score_v2"] = (
        1.0 * df["pm10_p90_flag"]
        + 1.0 * df["pm10_p95_flag"]
        + 1.0 * df["hum_low_flag"]
        + 1.0 * df["low_vis_flag"]
        + 0.5 * df["pressure_high_flag"]
    )

    df["calima_proxy_level_v2"] = df["calima_proxy_score_v2"].apply(score_to_level_v2)

    keep_cols = [
        "week_start",
        "deaths_week",
        "PM10",
        "PM2.5",
        "humidity_mean",
        "pressure_hpa_mean",
        "low_vis_any_week",
        "cap_dust_yellow_plus_week",
        "cap_dust_level_max_week",
        "calima_dai_flag",
        "pm10_p90_flag",
        "pm10_p95_flag",
        "hum_low_flag",
        "low_vis_flag",
        "pressure_high_flag",
        "calima_proxy_score_v2",
        "calima_proxy_level_v2",
    ]

    out = df[keep_cols].copy()

    meta = {
        "pm10_p90": pm10_p90,
        "pm10_p95": pm10_p95,
        "hum_p25": hum_p25,
        "pres_p75": pres_p75,
    }
    return out, meta


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Build weekly calima proxy dataset from island master dataset."
    )
    ap.add_argument(
        "--island",
        required=True,
        choices=sorted(ISLAND_CODE.keys()),
        help="Canonical island name, e.g. gran_canaria",
    )
    ap.add_argument(
        "--processed-dir",
        default="data/processed",
        help="Base processed directory",
    )
    ap.add_argument(
        "--master-file",
        default=None,
        help="Optional explicit master parquet path. If omitted, inferred from island.",
    )
    ap.add_argument(
        "--also-csv",
        action="store_true",
        help="Also save CSV output",
    )
    return ap.parse_args()


def main() -> None:
    args = parse_args()

    island = args.island
    code = ISLAND_CODE[island]
    processed_dir = Path(args.processed_dir)

    if args.master_file:
        master_fp = Path(args.master_file)
    else:
        master_fp = processed_dir / island / "master" / f"master_{code}_2015_2024.parquet"

    if not master_fp.exists():
        raise FileNotFoundError(f"Master file not found: {master_fp}")

    df = pd.read_parquet(master_fp)
    calima_df, meta = build_calima_proxy(df)

    out_dir = processed_dir / island / "calima"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_parquet = out_dir / f"calima_proxy_weekly_{code}_2015_2024_v2.parquet"
    calima_df.to_parquet(out_parquet, index=False)

    print(f"Island: {island} ({code})")
    print(f"Input:  {master_fp}")
    print(f"Output: {out_parquet}")
    print(f"Shape: {calima_df.shape}")
    print("Thresholds used:")
    for k, v in meta.items():
        print(f"  {k} = {round(v, 2)}")

    print("\nScore distribution:")
    print(calima_df["calima_proxy_score_v2"].value_counts().sort_index())

    print("\nLevel distribution:")
    print(calima_df["calima_proxy_level_v2"].value_counts())

    if args.also_csv:
        out_csv = out_dir / f"calima_proxy_weekly_{code}_2015_2024_v2.csv"
        calima_df.to_csv(out_csv, index=False)
        print(f"\nCSV: {out_csv}")


if __name__ == "__main__":
    main()