# src/ingests/air_quality_ingests/air_quality_canarias_station_excels_to_pm.py
from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

from src.utils.constants import island_code
from src.utils.io import ensure_dir
from src.utils.text import safe_slug
from src.utils.logging import setup_logging

LOGGER = logging.getLogger(__name__)


# ----------------------------
# Defaults (1 station per island, can override via CLI)
# ----------------------------

DEFAULT_STATION_BY_ISLAND = {
    "tenerife": "Tome Cano",
    "gran_canaria": "Mercado Central",
    "lanzarote": "Arrecife",
    "fuerteventura": "El Charco-Pto del Rosario",
    "la_palma": "El Pilar-Sta Cruz de La Palma",
    "gomera": "El Calvario-SS Gomera",
    "hierro": "Echedo-Valverde",
}


# ----------------------------
# Paths
# ----------------------------

def ensure_dirs(project_root: Path, island: str, domain: str = "air_quality") -> tuple[Path, Path, Path]:
    isl = safe_slug(island)
    dom = safe_slug(domain)

    raw_dir = project_root / "data" / "raw" / isl / dom
    proc_dir = project_root / "data" / "processed" / isl / dom
    logs_dir = project_root / "logs" / isl

    ensure_dir(raw_dir)
    ensure_dir(proc_dir)
    ensure_dir(logs_dir)
    return raw_dir, proc_dir, logs_dir


def year_excel_path(root: Path, year: int) -> Path:
    # root/Datos2016/Datos 2016.xlsx
    fp = root / f"Datos{year}" / f"Datos {year}.xlsx"
    return fp


# ----------------------------
# Column detection / normalization
# ----------------------------

def _norm(s: str) -> str:
    return (
        str(s).strip().lower()
        .replace(" ", "")
        .replace("-", "")
        .replace("/", "")
        .replace(".", "")
        .replace(",", "")
        .replace("_", "")
    )


def find_col_case_insensitive(df: pd.DataFrame, target: str) -> Optional[str]:
    t = target.strip().lower()
    for c in df.columns:
        if str(c).strip().lower() == t:
            return c
    return None


def detect_pollutant_columns(df: pd.DataFrame) -> Dict[str, str]:
    """
    Returns mapping canonical_name -> actual_column_name for pollutants found.
    Canonical names used:
      pm10, pm25, o3, no, no2, nox, so2
    """
    cols = list(df.columns)
    norm_map = {c: _norm(c) for c in cols}

    found: Dict[str, str] = {}

    for orig, n in norm_map.items():
        # PM10
        if n == "pm10":
            found["pm10"] = orig

        # PM2.5 common variants: pm25, pm2_5, pm2.5
        if n in ("pm25", "pm25ugm3", "pm25ugm", "pm25ug"):
            found["pm25"] = orig
        if n.startswith("pm2") and "5" in n:
            # prefer an exact match if not already found
            found.setdefault("pm25", orig)

        # O3
        if n == "o3" or "ozono" in n:
            found["o3"] = orig

        # NO, NO2, NOx
        if n == "no":
            found["no"] = orig
        if n == "no2":
            found["no2"] = orig
        if n == "nox":
            found["nox"] = orig

        # SO2 (sulfur dioxide). Sometimes "so2" or "dioxidoazufre"
        if n == "so2" or "dioxidoazufre" in n or "dioxido" in n and "azufre" in n:
            found["so2"] = orig

        # If your files *really* contain NO3, include it too (optional)
        if n == "no3":
            found["no3"] = orig  # keep as 'no3' if present

    return found


# ----------------------------
# Datetime build (Fecha + Hora)
# ----------------------------

def build_datetime_from_fecha_hora(df: pd.DataFrame) -> pd.Series:
    """
    Fecha: format like 01/01/16 (dayfirst)
    Hora: 1..24
    """
    fecha_col = find_col_case_insensitive(df, "fecha")
    if fecha_col is None:
        raise KeyError("No 'Fecha' column found (case-insensitive).")

    hora_col = find_col_case_insensitive(df, "hora")
    if hora_col is None:
        raise KeyError("No 'Hora' column found (case-insensitive).")

    fecha = pd.to_datetime(df[fecha_col], format="%d/%m/%y", errors="coerce").dt.floor("D")
    hora = pd.to_numeric(df[hora_col], errors="coerce")

    # Normalize 1..24 to 0..23
    # If values look like 1..24, shift down; convert 24 -> 0
    hora = hora.replace({24: 0})
    if hora.dropna().between(1, 24).all():
        hora = hora - 1

    hora = hora.fillna(0).astype(int)

    return fecha + pd.to_timedelta(hora, unit="h")


# ----------------------------
# Aggregations: hourly -> daily -> weekly
# ----------------------------

@dataclass(frozen=True)
class AggSpec:
    mean_suffix: str = "_mean"
    max_suffix: str = "_max"
    min_suffix: str = "_min"


POLLUTANTS_DEFAULT = ["pm10", "pm25", "o3", "no", "no2", "nox", "so2", "no3"]


def hourly_to_daily(df_hourly: pd.DataFrame, pollutants: List[str]) -> pd.DataFrame:
    """
    Input columns:
      datetime, plus pollutant columns (float)
    Output:
      date, {poll}_mean_day, {poll}_max_day, {poll}_min_day, n_hours, coverage_hours
    """
    df = df_hourly.copy()
    df["date"] = df["datetime"].dt.floor("D")

    # Any pollutant present this hour
    any_present = pd.Series(False, index=df.index)
    for p in pollutants:
        if p in df.columns:
            any_present = any_present | df[p].notna()
    df["has_data_hour"] = any_present.astype(int)

    agg: Dict[str, tuple] = {}
    for p in pollutants:
        if p not in df.columns:
            continue
        agg[f"{p}_mean_day"] = (p, "mean")
        agg[f"{p}_max_day"] = (p, "max")
        agg[f"{p}_min_day"] = (p, "min")

    daily = (
        df.groupby("date", as_index=False)
          .agg(**agg, n_hours=("has_data_hour", "sum"))
    )
    daily["coverage_hours"] = daily["n_hours"] / 24.0
    return daily


def daily_to_weekly(daily: pd.DataFrame, pollutants: List[str]) -> pd.DataFrame:
    """
    Weekly ISO Monday:
      - mean_week = mean(daily_mean)
      - max_week  = max(daily_max)
      - min_week  = min(daily_min)  (optional; included here)
    coverage:
      n_days where any pollutant has data / 7
    """
    d = daily.copy()
    d["week_start"] = d["date"] - pd.to_timedelta(d["date"].dt.weekday, unit="D")

    any_day = pd.Series(False, index=d.index)
    for p in pollutants:
        c = f"{p}_mean_day"
        if c in d.columns:
            any_day = any_day | d[c].notna()
    d["has_data_day"] = any_day.astype(int)

    n_days = (
        d.loc[d["has_data_day"] == 1]
         .groupby("week_start")["date"]
         .nunique()
    )

    agg: Dict[str, tuple] = {}
    for p in pollutants:
        mean_col = f"{p}_mean_day"
        max_col = f"{p}_max_day"
        min_col = f"{p}_min_day"

        if mean_col in d.columns:
            agg[f"{p}_mean_week"] = (mean_col, "mean")
        if max_col in d.columns:
            agg[f"{p}_max_week"] = (max_col, "max")
        if min_col in d.columns:
            agg[f"{p}_min_week"] = (min_col, "min")

    weekly = (
        d.groupby("week_start", as_index=False)
         .agg(**agg)
    )

    weekly["n_days"] = weekly["week_start"].map(n_days).fillna(0).astype(int)
    weekly["coverage"] = weekly["n_days"] / 7.0
    weekly["week_start"] = pd.to_datetime(weekly["week_start"], errors="coerce")
    weekly = weekly.sort_values("week_start").reset_index(drop=True)
    return weekly


# ----------------------------
# Sheet reading
# ----------------------------

def read_station_sheet(fp: Path, sheet: str) -> pd.DataFrame:
    """
    Excel format:
      - Row 1 contains station name (not header)
      - Row 2 contains column names -> header=1
      - 'Leyenda' sheet should be ignored
    """
    df = pd.read_excel(fp, sheet_name=sheet, header=1, engine="openpyxl")
    # Clean columns
    df.columns = (
        df.columns.astype(str)
        .str.replace("\ufeff", "", regex=False)
        .str.strip()
    )
    df = df.dropna(axis=1, how="all")
    return df


def list_sheets(fp: Path) -> List[str]:
    xl = pd.ExcelFile(fp, engine="openpyxl")
    return xl.sheet_names


# ----------------------------
# Main build
# ----------------------------

def build_air_quality_for_station(
    root: Path,
    station: str,
    start_year: int,
    end_year: int,
    pollutants: List[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns (daily, weekly).
    """
    frames_hourly: List[pd.DataFrame] = []

    for year in range(start_year, end_year + 1):
        fp = year_excel_path(root, year)
        if not fp.exists():
            raise FileNotFoundError(f"Missing yearly excel: {fp}")

        sheets = list_sheets(fp)
        if station not in sheets:
            # Provide helpful error
            sample = ", ".join(sheets[:15])
            raise ValueError(
                f"Station sheet '{station}' not found in {fp.name}. "
                f"First sheets: {sample}"
            )

        df_sheet = read_station_sheet(fp, station)

        dt = build_datetime_from_fecha_hora(df_sheet)
        out = pd.DataFrame({"datetime": dt})

        pol_cols = detect_pollutant_columns(df_sheet)
        # Keep only requested pollutants that exist
        for p in pollutants:
            if p in pol_cols:
               col = df_sheet[pol_cols[p]]
               # Si hay columnas duplicadas, pandas devuelve DataFrame -> nos quedamos con la primera
               if isinstance(col, pd.DataFrame):
                    col = col.iloc[:, 0]
               out[p] = pd.to_numeric(col, errors="coerce")

        out = out.dropna(subset=["datetime"])
        frames_hourly.append(out)

        LOGGER.info("Loaded %s | %s | rows=%s | cols=%s", year, station, len(out), list(out.columns))

    hourly = pd.concat(frames_hourly, ignore_index=True)
    hourly = hourly.sort_values("datetime").reset_index(drop=True)

    daily = hourly_to_daily(hourly, pollutants=pollutants)
    weekly = daily_to_weekly(daily, pollutants=pollutants)

    return daily, weekly


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True, help=r'Root folder with DatosYYYY/ (e.g. C:\data\...\Datos2016_2025)')
    ap.add_argument("--island", required=True, help="tenerife, gran_canaria, lanzarote, ...")
    ap.add_argument("--station", default=None, help="Excel sheet name for station. If omitted, uses default per island.")
    ap.add_argument("--start-year", type=int, required=True)
    ap.add_argument("--end-year", type=int, required=True)
    ap.add_argument("--save-daily", action="store_true", help="Also save daily raw parquet (recommended).")
    ap.add_argument("--format", choices=["parquet", "csv"], default="parquet")
    return ap.parse_args()


def main() -> None:
    args = _parse_args()

    project_root = Path(__file__).resolve().parents[2]
    root = Path(args.root)

    isl = safe_slug(args.island)
    code = island_code(isl)

    station = args.station or DEFAULT_STATION_BY_ISLAND.get(isl)
    if not station:
        raise ValueError(
            f"No default station for island '{isl}'. Pass --station explicitly."
        )

    raw_dir, proc_dir, logs_dir = ensure_dirs(project_root, isl, domain="air_quality")
    log_fp = logs_dir / f"air_quality_excel_{code}_{safe_slug(station)}_{args.start_year}_{args.end_year}.log"
    setup_logging(log_fp)

    LOGGER.info("START island=%s code=%s station=%s years=%s-%s root=%s", isl, code, station, args.start_year, args.end_year, root)

    pollutants = [p for p in POLLUTANTS_DEFAULT]  # include all; missing cols are ignored

    daily, weekly = build_air_quality_for_station(
        root=root,
        station=station,
        start_year=args.start_year,
        end_year=args.end_year,
        pollutants=pollutants,
    )

    # Save daily (optional, but you requested option A style for raw intermediates)
    if args.save_daily:
        start_s = f"{args.start_year}-01-01"
        end_s = f"{args.end_year}-12-31"
        daily_fp = raw_dir / f"air_quality_daily_{code}_{safe_slug(station)}_{start_s}_{end_s}.{args.format}"
        if args.format == "parquet":
            daily.to_parquet(daily_fp, index=False)
        else:
            daily.to_csv(daily_fp, index=False)
        LOGGER.info("Saved daily -> %s (rows=%s cols=%s)", daily_fp, len(daily), daily.shape[1])

    # Save weekly (contract output)
    weekly_fp = proc_dir / f"air_quality_weekly_{code}_{args.start_year}_{args.end_year}.{args.format}"
    if args.format == "parquet":
        weekly.to_parquet(weekly_fp, index=False)
    else:
        weekly.to_csv(weekly_fp, index=False)
    LOGGER.info("Saved weekly -> %s (rows=%s cols=%s)", weekly_fp, len(weekly), weekly.shape[1])

    # Basic QC logs
    LOGGER.info("Weekly week_start min/max: %s .. %s", weekly["week_start"].min(), weekly["week_start"].max())
    LOGGER.info("Weekly dup weeks: %s", int(weekly.duplicated(["week_start"]).sum()))
    LOGGER.info("Weekly missing week_start: %s", int(weekly["week_start"].isna().sum()))
    LOGGER.info("Weekly coverage < 1: %s", int((weekly["coverage"] < 1.0).sum()))
    LOGGER.info("DONE")


if __name__ == "__main__":
    main()