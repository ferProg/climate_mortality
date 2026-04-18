#!/usr/bin/env python
r"""
Run calima-airports proxy pipeline end-to-end for an island.

Steps:
  1) Step1: Download+parse NOAA ISD yearly files (minimal vars) -> per-year parquet + manifest
  2) Step2: Build daily per-station near TARGET_HOUR_UTC -> daily parquet
  3) Step3: Build island daily flags (confirmed/possible/any) -> island daily parquet
  4) Step4: Aggregate weekly (Mon week_start UTC) -> island weekly parquet

Example (PowerShell) from repo root:
  $env:PYTHONPATH="$PWD/src/ingest"
  python src/ingest/calima_airports/run_island_pipeline.py `
    --isla tenerife `
    --start_date 2016-01-01 `
    --end_date 2024-12-31 `
    --stations GCTS

Notes:
- If you don't pass --stations, it uses AIRPORTS_BY_ISLAND[isla].
- Tenerife: recommended to use only GCTS (TFS) to avoid TFN fog noise.
"""

# Pipeline completo de visibilidad/calima por aeropuertos para una isla.
#
# Flujo:
# 1) Descarga y parsea archivos NOAA ISD anuales para las estaciones/aeropuertos.
# 2) Construye una serie diaria por estación cerca de la hora objetivo UTC.
# 3) Combina estaciones y genera flags diarios a nivel isla.
# 4) Agrega la serie diaria insular a frecuencia semanal (week_start = lunes).
#
# Entradas principales:
# - isla
# - start_date / end_date
# - estaciones ICAO opcionales
#
# Salidas:
# - step1_yearly: parquets anuales parseados + manifest
# - step2_daily: serie diaria por estación
# - step3_daily: serie diaria insular con flags
# - step4_weekly: serie semanal final de visibilidad
#
# Nota:
# Si no se pasan estaciones manualmente, usa las definidas en AIRPORTS_BY_ISLAND.

from __future__ import annotations
import logging
import argparse
from pathlib import Path
from typing import List, Dict, Tuple

from src.ingests.visibility.config import AIRPORTS_BY_ISLAND, ICAO_TO_ISD
from src.ingests.visibility.step1_load_isd_airports import run_step1_load_isd
from src.ingests.visibility.step2_filter_13utc_and_build_daily import run_step2_build_daily
from src.ingests.visibility.step3_build_dust_day_flag_island import run_step3_build_island_flags
from src.ingests.visibility.step4_aggregate_weekly_island import run_step4_aggregate_weekly
from src.utils.logging import setup_logging
from src.utils.constants import island_code
from src.utils.io import ensure_dir

LOGGER = logging.getLogger(__name__)

# after: island = norm(args.isla)
ISLAND_ALIAS = {
    # canonical short forms
    "tenerife": "tenerife",
    "gran_canaria": "gran_canaria",
    "lanzarote": "lanzarote",
    "fuerteventura": "fuerteventura",
    "la_palma": "la_palma",
    "gomera": "gomera",
    "hierro": "hierro",

    # accepted alternate inputs
    "la_gomera": "gomera",
    "el_hierro": "hierro",
    "palma": "la_palma",
    "grancanaria": "gran_canaria",
}


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

    p.add_argument("--raw_dir", default="data/raw/", help="Where .gz files are cached/downloaded")
    p.add_argument("--force", action="store_true", help="Re-parse yearly ISD parquets even if they exist")
    p.add_argument("--logfile", default=None, help="Optional log file path. Default: logs/<isla>/visibility_airports_<code>_<start>_<end>.log")
    return p.parse_args()


def build_stations_map(icaos: List[str]) -> Dict[str, Tuple[str, str]]:
    missing = [icao for icao in icaos if icao not in ICAO_TO_ISD or ICAO_TO_ISD[icao] == (None, None)]
    if missing:
        raise SystemExit(f"Missing USAF/WBAN mapping for: {missing}. Fill ICAO_TO_ISD in calima_config.py.")
    return {icao: ICAO_TO_ISD[icao] for icao in icaos}


def main() -> None:
    args = parse_args()
    project_root = Path(__file__).resolve().parents[3] 

    island = norm(args.isla)
    code = island_code(ISLAND_ALIAS.get(island, island))

    if args.logfile:
        log_fp = Path(args.logfile)
        if not log_fp.is_absolute():
            log_fp = (project_root / log_fp).resolve()
        ensure_dir(log_fp.parent)
    else:
        logs_dir = project_root / "logs" / island
        ensure_dir(logs_dir)
        log_fp = logs_dir / f"visibility_airports_{code}_{args.start_date}_{args.end_date}.log"

    setup_logging(log_fp)
    LOGGER.info("START calima_airports pipeline | island=%s code=%s %s..%s", island, code, args.start_date, args.end_date)
    LOGGER.info("logfile=%s", log_fp)
    
    

    if island not in AIRPORTS_BY_ISLAND:
        raise SystemExit(f"Unknown island '{args.isla}'. Options: {sorted(AIRPORTS_BY_ISLAND.keys())}")

    # Stations to use
    icaos = args.stations if args.stations and len(args.stations) > 0 else AIRPORTS_BY_ISLAND[island]
    icaos = [x.strip().upper() for x in icaos]

    stations_map = build_stations_map(icaos)

    base_interim = project_root / "data" / "interim" / island / "visibility"

    raw_dir = Path(args.raw_dir)
    if not raw_dir.is_absolute():
        raw_dir = (project_root / raw_dir).resolve()
        parsed_dir = base_interim / "step1_yearly"
        daily_dir = base_interim / "step2_daily"
        island_daily_dir = base_interim / "step3_daily"
        weekly_dir = project_root / "data" / "processed" / island / "visibility"

    # --- Step1 ---
    manifest_fp = run_step1_load_isd(
        stations=stations_map,
        start_date=args.start_date,
        end_date=args.end_date,
        raw_dir=raw_dir,
        out_dir=parsed_dir,
        out_name=f"isd_manifest_{island}_{args.start_date}_{args.end_date}.parquet",
        force=args.force
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
        out_name=f"visibility_daily_{code}_{args.start_date}_{args.end_date}.parquet",

    )

    # --- Step4 ---
    weekly_fp = run_step4_aggregate_weekly(
        island_daily_fp=Path(island_daily_fp),
        out_dir=weekly_dir,
        out_name=f"visibility_weekly_{code}_{args.start_date[:4]}_{args.end_date[:4]}.parquet",
    )

    LOGGER.info("DONE pipeline")
    LOGGER.info("island: %s", island)
    LOGGER.info("stations: %s", icaos)
    LOGGER.info("manifest: %s", manifest_fp)
    LOGGER.info("daily: %s", daily_fp)
    LOGGER.info("island_daily: %s", island_daily_fp)
    LOGGER.info("weekly: %s", weekly_fp)


if __name__ == "__main__":
    main()
