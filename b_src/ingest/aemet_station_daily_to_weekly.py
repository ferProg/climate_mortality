"""
AEMET OpenData: station daily -> ISO weekly.

PowerShell:
  $env:AEMET_API_KEY="YOUR_KEY"
  python .\src\ingest\aemet_station_daily_to_weekly.py `
    --station C429I --start 2016-01-01 --end 2024-12-31 `
    --island tenerife --suffix tfe

Outputs (default parquet):
  data/raw/<island>/aemet_<suffix>_<station>_daily_<start>_<end>.parquet
  data/processed/<island>/aemet_<suffix>_<station>_weekly_<start>_<end>.parquet
  b_logs/<island>/aemet_opendata_<suffix>_<station>_<start>_<end>.log

Optional:
  --format csv|parquet  (default parquet)
  --also-csv            (write CSV copy alongside parquet)
"""

from __future__ import annotations

import argparse
import os
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests


# -----------------------
# Project paths
# -----------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_RAW_ROOT = PROJECT_ROOT / "b_data" / "raw"
DATA_PROCESSED_ROOT = PROJECT_ROOT / "b_data" / "processed"
LOGS_ROOT = PROJECT_ROOT / "b_logs"

DATA_RAW_ROOT.mkdir(parents=True, exist_ok=True)
DATA_PROCESSED_ROOT.mkdir(parents=True, exist_ok=True)
LOGS_ROOT.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://opendata.aemet.es/opendata/api"

ISLAND_SUFFIX = {
    "tenerife": "tfe",
    "gran_canaria": "gc",
    "grancanaria": "gc",
    "la_palma": "lp",
    "lapalma": "lp",
    "la_gomera": "lg",
    "lagomera": "lg",
    "el_hierro": "eh",
    "elhierro": "eh",
    "lanzarote": "lz",
    "fuerteventura": "fvt",
}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="AEMET OpenData station daily -> ISO weekly (multi-island)")
    p.add_argument("--station", required=True, help="AEMET station code, e.g. C429I")
    p.add_argument("--start", required=True, help="YYYY-MM-DD")
    p.add_argument("--end", required=True, help="YYYY-MM-DD")
    p.add_argument("--island", required=True, help="Folder name, e.g. tenerife, gran_canaria, lanzarote")
    p.add_argument("--suffix", required=False, default=None, help="Optional override. If omitted, derived from --island.")
    p.add_argument("--chunk-days", type=int, default=180, help="Days per request chunk (default 180)")
    p.add_argument("--sleep", type=float, default=0.25, help="Seconds to sleep between chunks (default 0.25)")
    p.add_argument(
        "--coverage-rule",
        choices=["any_temp", "tmed_only"],
        default="any_temp",
        help="How to count n_days (default any_temp).",
    )
    p.add_argument("--format", choices=["parquet", "csv"], default="parquet", help="Output format (default parquet)")
    p.add_argument("--also-csv", action="store_true", help="Also write CSV copies alongside parquet outputs")
    return p.parse_args()


# -----------------------
# Logging
# -----------------------
def safe_slug(s: str) -> str:
    return s.strip().lower().replace(" ", "_")


def ensure_dirs(island: str) -> tuple[Path, Path, Path]:
    isl = safe_slug(island)
    raw_dir = DATA_RAW_ROOT / isl
    proc_dir = DATA_PROCESSED_ROOT / isl
    logs_dir = LOGS_ROOT / isl
    raw_dir.mkdir(parents=True, exist_ok=True)
    proc_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)
    return raw_dir, proc_dir, logs_dir


def make_logfile(logs_dir: Path, suffix: str, station: str, start: str, end: str) -> Path:
    return logs_dir / f"aemet_opendata_{suffix}_{station}_{start}_{end}.log"


def log(msg: str, logfile: Path) -> None:
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{stamp}] {msg}"
    print(line)
    with open(logfile, "a", encoding="utf-8") as f:
        f.write(line + "\n")


# -----------------------
# Helpers
# -----------------------
def daterange_chunks(start: date, end: date, chunk_days: int) -> Iterable[tuple[date, date]]:
    cur = start
    while cur <= end:
        b = min(cur + timedelta(days=chunk_days - 1), end)
        yield cur, b
        cur = b + timedelta(days=1)


def parse_comma_decimal(x):
    if pd.isna(x):
        return pd.NA
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip().replace('"', "")
    if s == "" or s.lower() in {"nan", "none"}:
        return pd.NA
    s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return pd.NA


def aemet_daily_endpoint(station: str, start_d: date, end_d: date) -> str:
    start_str = f"{start_d.isoformat()}T00:00:00UTC"
    end_str = f"{end_d.isoformat()}T23:59:59UTC"
    return (
        f"{BASE_URL}/valores/climatologicos/diarios/datos/"
        f"fechaini/{start_str}/fechafin/{end_str}/estacion/{station}"
    )


def fetch_json(url: str, api_key: str, timeout_s: int = 60) -> dict:
    r = requests.get(url, headers={"api_key": api_key, "accept": "application/json"}, timeout=timeout_s)
    r.raise_for_status()
    return r.json()


def fetch_data_url(data_url: str, timeout_s: int = 120):
    r = requests.get(data_url, timeout=timeout_s)
    r.raise_for_status()
    return r.json()


def make_paths(raw_dir: Path, proc_dir: Path, suffix: str, station: str, start: str, end: str, fmt: str):
    ext = "parquet" if fmt == "parquet" else "csv"
    daily = raw_dir / f"aemet_{suffix}_{station}_daily_{start}_{end}.{ext}"
    weekly = proc_dir / f"aemet_{suffix}_{station}_weekly_{start}_{end}.{ext}"
    # Optional CSV copies if fmt=parquet
    daily_csv = raw_dir / f"aemet_{suffix}_{station}_daily_{start}_{end}.csv"
    weekly_csv = proc_dir / f"aemet_{suffix}_{station}_weekly_{start}_{end}.csv"
    return daily, weekly, daily_csv, weekly_csv


def write_df(df: pd.DataFrame, path: Path, fmt: str) -> None:
    if fmt == "parquet":
        # Requires pyarrow or fastparquet installed
        df.to_parquet(path, index=False)
    else:
        df.to_csv(path, index=False, encoding="utf-8")


# -----------------------
# Download
# -----------------------
def download_daily_station(
    station: str,
    start: date,
    end: date,
    logfile: Path,
    chunk_days: int = 180,
    sleep_s: float = 0.25,
) -> pd.DataFrame:
    api_key = os.getenv("AEMET_API_KEY")
    if not api_key:
        raise RuntimeError("Missing env var AEMET_API_KEY. In PowerShell: $env:AEMET_API_KEY='YOUR_KEY'")

    rows: list[dict] = []
    chunks = list(daterange_chunks(start, end, chunk_days))
    log(f"Downloading station={station} in {len(chunks)} chunk(s), chunk_days={chunk_days}", logfile)

    for i, (a, b) in enumerate(chunks, start=1):
        url = aemet_daily_endpoint(station, a, b)
        log(f"[{i}/{len(chunks)}] metadata: {a}..{b}", logfile)
        meta = fetch_json(url, api_key=api_key)

        if int(meta.get("estado", 0)) != 200 or "datos" not in meta:
            raise RuntimeError(f"AEMET API error for chunk {a}..{b}: {meta}")

        data_url = meta["datos"]
        log(f"[{i}/{len(chunks)}] downloading datos URL", logfile)
        data = fetch_data_url(data_url)

        if not isinstance(data, list):
            raise RuntimeError(f"Unexpected payload type {type(data)} for {a}..{b}")

        rows.extend(data)
        time.sleep(sleep_s)

    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("Downloaded 0 rows. Check station and dates.")
    return df

#-----------------------
#  TIPEADO
#---------------------

def normalize_daily_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize AEMET daily schema to sensible dtypes for parquet + downstream ops.
    - fecha -> datetime
    - comma-decimal numeric fields -> float
    - some already-numeric fields -> numeric coercion
    """
    out = df.copy()

    if "fecha" in out.columns:
        out["fecha"] = pd.to_datetime(out["fecha"], errors="coerce")

    # Common comma-decimal numeric fields in AEMET daily
    comma_decimal_cols = [
        "tmed", "prec", "tmin", "tmax", "velmedia", "racha", "sol",
        "presMax", "presMin",
    ]
    for c in comma_decimal_cols:
        if c in out.columns:
            out[c] = out[c].map(parse_comma_decimal)

    # Often numeric but sometimes messy
    numeric_cols = ["hrMedia", "hrMax", "hrMin", "dir", "altitud"]
    for c in numeric_cols:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")

    return out

# -----------------------
# Transform
# -----------------------
def build_weekly(df_daily: pd.DataFrame, coverage_rule: str = "any_temp") -> pd.DataFrame:
    if "fecha" not in df_daily.columns:
        raise ValueError("Expected daily column 'fecha' not found.")

    df = df_daily.copy()
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    df = df.dropna(subset=["fecha"])

    mapping = {
        "tmed": "temp_c",
        "tmax": "tmax_c",
        "tmin": "tmin_c",
        "hrMedia": "humidity",
        "velmedia": "wind_ms",
        "prec": "prec",
        "presMax": "pres_max_hpa",
        "presMin": "pres_min_hpa",
    }

    for src, dst in mapping.items():
        df[dst] = df[src].map(parse_comma_decimal) if src in df.columns else pd.NA

    df["pressure_hpa"] = pd.NA
    a = pd.to_numeric(df.get("pres_max_hpa"), errors="coerce")
    b = pd.to_numeric(df.get("pres_min_hpa"), errors="coerce")
    if a is not None and b is not None:
        df["pressure_hpa"] = (a + b) / 2

    df["week_start"] = df["fecha"].dt.normalize() - pd.to_timedelta(df["fecha"].dt.weekday, unit="D")

    if coverage_rule == "tmed_only":
        has_day = df["temp_c"].notna()
    else:
        has_day = df["temp_c"].notna() | df["tmax_c"].notna() | df["tmin_c"].notna()
    df["has_data_day"] = has_day.astype(int)

    g = df.groupby("week_start", as_index=False)
    weekly = g.agg(
        n_days=("has_data_day", "sum"),
        temp_c_mean=("temp_c", "mean"),
        tmax_c_mean=("tmax_c", "mean"),
        tmax_c_max=("tmax_c", "max"),
        tmin_c_mean=("tmin_c", "mean"),
        tmin_c_min=("tmin_c", "min"),
        humidity_mean=("humidity", "mean"),
        pressure_hpa_mean=("pressure_hpa", "mean"),
        wind_ms_mean=("wind_ms", "mean"),
        prec_sum=("prec", "sum"),
    )

    weekly["coverage"] = weekly["n_days"] / 7.0
    weekly = weekly.sort_values("week_start").reset_index(drop=True)
    weekly["week_start"] = pd.to_datetime(weekly["week_start"]).dt.date.astype(str)

    cols = [
        "week_start", "n_days",
        "temp_c_mean", "tmax_c_mean", "tmax_c_max",
        "tmin_c_mean", "tmin_c_min",
        "humidity_mean", "pressure_hpa_mean",
        "wind_ms_mean", "prec_sum",
        "coverage",
    ]
    return weekly[cols]


def main():
    args = parse_args()

    station = args.station.strip().upper()
    island = args.island
    island_slug = safe_slug(island)

    # suffix: CLI override > dictionary > fallback
    if args.suffix:
        suffix = args.suffix.strip().lower()
    else:
        suffix = ISLAND_SUFFIX.get(island_slug)
        if not suffix:
            raise ValueError(
                f"Island '{island_slug}' not in ISLAND_SUFFIX map. Add it or pass --suffix manually."
            )
    start_s, end_s = args.start, args.end
    start_d, end_d = date.fromisoformat(start_s), date.fromisoformat(end_s)
    if end_d < start_d:
        raise ValueError("--end must be >= --start")

    raw_dir, proc_dir, logs_dir = ensure_dirs(island)
    logfile = make_logfile(logs_dir, suffix, station, start_s, end_s)

    daily_path, weekly_path, daily_csv, weekly_csv = make_paths(
        raw_dir, proc_dir, suffix, station, start_s, end_s, args.format
    )

    log(f"START island={island_slug} suffix={suffix} station={station} {start_s}..{end_s}", logfile)
    log(f"Daily out -> {daily_path}", logfile)
    log(f"Weekly out -> {weekly_path}", logfile)

    df_daily = download_daily_station(
        station=station,
        start=start_d,
        end=end_d,
        logfile=logfile,
        chunk_days=args.chunk_days,
        sleep_s=args.sleep,
    )
    # NEW: type-normalize daily for parquet + consistency
    df_daily = normalize_daily_types(df_daily)

    weekly = build_weekly(df_daily, coverage_rule=args.coverage_rule)

    write_df(df_daily, daily_path, args.format)
    write_df(weekly, weekly_path, args.format)

    if args.also_csv and args.format == "parquet":
        df_daily.to_csv(daily_csv, index=False, encoding="utf-8")
        weekly.to_csv(weekly_csv, index=False, encoding="utf-8")
        log(f"Also wrote CSV copies: {daily_csv.name}, {weekly_csv.name}", logfile)

    log("DONE", logfile)


if __name__ == "__main__":
    main()