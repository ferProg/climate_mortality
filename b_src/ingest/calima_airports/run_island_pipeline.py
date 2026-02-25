#!/usr/bin/env python
"""
Run calima-airports proxy pipeline end-to-end for an island.

Steps:
  1) Step1: Download+parse NOAA ISD yearly files (minimal vars) -> per-year parquet + manifest
  2) Step2: Build daily per-station near TARGET_HOUR_UTC -> daily parquet
  3) Step3: Build island daily flags (confirmed/possible/any) -> island daily parquet
  4) Step4: Aggregate weekly (Mon week_start UTC) -> island weekly parquet

Example (PowerShell) from repo root:
  $env:PYTHONPATH="$PWD\b_src\ingest"
  python b_src\ingest\calima_airports\run_island_pipeline.py `
    --isla tenerife `
    --start_date 2016-01-01 `
    --end_date 2024-12-31 `
    --stations GCTS

Notes:
- If you don't pass --stations, it uses AIRPORTS_BY_ISLAND[isla].
- Tenerife: recommended to use only GCTS (TFS) to avoid TFN fog noise.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import List, Dict, Tuple

from calima_airports.calima_config import AIRPORTS_BY_ISLAND, ICAO_TO_ISD
from calima_airports.step1_load_isd_airports import run_step1_load_isd
from calima_airports.step2_filter_13utc_and_build_daily import run_step2_build_daily
from calima_airports.step3_build_dust_day_flag_island import run_step3_build_island_flags
from calima_airports.step4_aggregate_weekly_island import run_step4_aggregate_weekly


def norm(s: str) -> str:
    return s.strip().lower().replace(" ", "_").replace("-", "_")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run calima airports pipeline for an island.")
    p.add_argument("--isla", required=True, help="Island name (e.g. tenerife, la_palma, ...)")
    p.add_argument("--start_date", required=True, help="YYYY-MM-DD")
    p.add_argument("--end_date", required=True, help="YYYY-MM-DD")

    p.add_argument(
        "--stations",
        nargs="*",
        default=None,
        help="Override station ICAOs to use (e.g. --stations GCTS). If omitted, uses config AIRPORTS_BY_ISLAND[isla].",
    )

    p.add_argument("--raw_dir", default="b_data/raw/noaa_isd", help="Where .gz files are cached/downloaded")
    p.add_argument("--parsed_dir", default="b_data/interim/noaa_isd_parsed", help="Step1 output dir (per-year parquets)")
    p.add_argument("--daily_dir", default="b_data/interim/noaa_isd_daily", help="Step2 output dir (daily per station)")
    p.add_argument("--island_daily_dir", default="b_data/interim/calima_proxy_daily", help="Step3 output dir")
    p.add_argument("--weekly_dir", default="b_data/interim/calima_proxy_weekly", help="Step4 output dir")

    return p.parse_args()


def build_stations_map(icaos: List[str]) -> Dict[str, Tuple[str, str]]:
    missing = [icao for icao in icaos if icao not in ICAO_TO_ISD or ICAO_TO_ISD[icao] == (None, None)]
    if missing:
        raise SystemExit(f"Missing USAF/WBAN mapping for: {missing}. Fill ICAO_TO_ISD in calima_config.py.")
    return {icao: ICAO_TO_ISD[icao] for icao in icaos}


def main() -> None:
    args = parse_args()
    island = norm(args.isla)

    if island not in AIRPORTS_BY_ISLAND:
        raise SystemExit(f"Unknown island '{args.isla}'. Options: {sorted(AIRPORTS_BY_ISLAND.keys())}")

    # Stations to use
    icaos = args.stations if args.stations and len(args.stations) > 0 else AIRPORTS_BY_ISLAND[island]
    icaos = [x.strip().upper() for x in icaos]

    stations_map = build_stations_map(icaos)

    raw_dir = Path(args.raw_dir)
    parsed_dir = Path(args.parsed_dir)
    daily_dir = Path(args.daily_dir)
    island_daily_dir = Path(args.island_daily_dir)
    weekly_dir = Path(args.weekly_dir)

    # --- Step1 ---
    manifest_fp = run_step1_load_isd(
        stations=stations_map,
        start_date=args.start_date,
        end_date=args.end_date,
        raw_dir=raw_dir,
        out_dir=parsed_dir,
        out_name=f"isd_manifest_{island}_{args.start_date}_{args.end_date}.parquet",
    )

    # --- Step2 ---
    daily_fp = run_step2_build_daily(
        manifest_fp=Path(manifest_fp),
        start_date=args.start_date,
        end_date=args.end_date,
        out_dir=daily_dir,
        out_name=f"isd_daily_{island}_{args.start_date}_{args.end_date}.parquet",
    )

    # --- Step3 ---
    island_daily_fp = run_step3_build_island_flags(
        daily_fp=Path(daily_fp),
        island=island,
        stations=icaos,
        out_dir=island_daily_dir,
        out_name=f"calima_proxy_daily_{island}_{args.start_date}_{args.end_date}.parquet",
    )

    # --- Step4 ---
    weekly_fp = run_step4_aggregate_weekly(
        island_daily_fp=Path(island_daily_fp),
        out_dir=weekly_dir,
        out_name=f"calima_proxy_weekly_{island}_{args.start_date}_{args.end_date}.parquet",
    )

    print("\nDONE pipeline")
    print("island:", island)
    print("stations:", icaos)
    print("manifest:", manifest_fp)
    print("daily:", daily_fp)
    print("island_daily:", island_daily_fp)
    print("weekly:", weekly_fp)


if __name__ == "__main__":
    main()