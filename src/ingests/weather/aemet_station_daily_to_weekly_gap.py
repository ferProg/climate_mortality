from __future__ import annotations

import argparse
import time
import random
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable, Tuple
import logging
import pandas as pd
import requests
from src.utils.io import ensure_dir
from src.utils.constants import island_code
from src.utils.logging import setup_logging
from src.utils.text import safe_slug

# Safe gap-fill variant of aemet_station_daily_to_weekly.py
# Differences from the base script:
# - intended for short gap extractions from an alternative station
# - keeps the same CLI for station/start/end/island
# - writes the weekly output to a station-specific, gap-specific filename
#   to avoid overwriting the main weekly weather series
# - default weekly filename example:
#     weather_weekly_gom_c329z_2020_gap.parquet

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DATA_RAW_ROOT = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED_ROOT = PROJECT_ROOT / "data" / "processed"
LOGS_ROOT = PROJECT_ROOT / "logs"

DATA_RAW_ROOT.mkdir(parents=True, exist_ok=True)
DATA_PROCESSED_ROOT.mkdir(parents=True, exist_ok=True)
LOGS_ROOT.mkdir(parents=True, exist_ok=True)
LOGGER = logging.getLogger(__name__)
BASE_URL = "https://opendata.aemet.es/opendata/api"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="AEMET OpenData station daily -> ISO weekly GAP extractor")
    p.add_argument("--station", required=True, help="AEMET station code, e.g. C329Z")
    p.add_argument("--start", required=True, help="YYYY-MM-DD")
    p.add_argument("--end", required=True, help="YYYY-MM-DD")
    p.add_argument("--island", required=True, help="Folder name, e.g. gomera")
    p.add_argument("--chunk-days", type=int, default=60, help="Days per request chunk")
    p.add_argument("--sleep", type=float, default=1.0, help="Seconds to sleep between chunks")
    p.add_argument(
        "--coverage-rule",
        choices=["any_temp", "tmed_only"],
        default="any_temp",
        help="How to count n_days.",
    )
    p.add_argument("--format", choices=["parquet", "csv"], default="parquet", help="Output format")
    p.add_argument("--also-csv", action="store_true", help="Also write CSV copies alongside parquet outputs")
    p.add_argument(
        "--gap-label",
        default=None,
        help="Optional custom label for the weekly gap file, e.g. covid_gap. Default: <startyear>_gap",
    )
    return p.parse_args()


def ensure_dirs(island: str, domain: str) -> Tuple[Path, Path, Path]:
    isl = safe_slug(island)
    dom = safe_slug(domain)

    raw_dir = DATA_RAW_ROOT / isl / dom
    proc_dir = DATA_PROCESSED_ROOT / isl / dom
    logs_dir = LOGS_ROOT / isl

    ensure_dir(raw_dir)
    ensure_dir(proc_dir)
    ensure_dir(logs_dir)

    return raw_dir, proc_dir, logs_dir


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


def fetch_data_url(data_url: str, *, max_retries: int = 6, base_sleep: float = 1.5) -> list[dict]:
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(data_url, timeout=60)
            if 500 <= r.status_code < 600:
                raise requests.HTTPError(f"{r.status_code} Server Error", response=r)
            r.raise_for_status()
            return r.json()
        except (requests.RequestException, ValueError) as e:
            last_err = e
            sleep_s = base_sleep * (2 ** (attempt - 1)) + random.uniform(0, 0.75)
            LOGGER.warning("fetch_data_url failed (attempt %s/%s): %s | sleeping %.2fs", attempt, max_retries, e, sleep_s)
            time.sleep(sleep_s)
    raise last_err


def make_paths(raw_dir: Path, proc_dir: Path, code: str, station: str, start: str, end: str, fmt: str, gap_label: str | None):
    ext = "parquet" if fmt == "parquet" else "csv"
    station_slug = safe_slug(station).lower()
    start_year = start[:4]
    gap_tag = safe_slug(gap_label) if gap_label else f"{start_year}_gap"

    daily = raw_dir / f"weather_daily_{code}_{station_slug}_{start}_{end}.{ext}"
    weekly = proc_dir / f"weather_weekly_{code}_{station_slug}_{gap_tag}.{ext}"

    daily_csv = raw_dir / f"weather_daily_{code}_{station_slug}_{start}_{end}.csv"
    weekly_csv = proc_dir / f"weather_weekly_{code}_{station_slug}_{gap_tag}.csv"
    return daily, weekly, daily_csv, weekly_csv


def write_df(df: pd.DataFrame, path: Path, fmt: str) -> None:
    if fmt == "parquet":
        df.to_parquet(path, index=False)
    else:
        df.to_csv(path, index=False, encoding="utf-8")


def download_daily_station(station: str, start: date, end: date, chunk_days: int = 60, sleep_s: float = 1.0) -> pd.DataFrame:
    import os

    api_key = os.getenv("AEMET_API_KEY")
    if not api_key:
        raise RuntimeError("Missing env var AEMET_API_KEY. In PowerShell: $env:AEMET_API_KEY='YOUR_KEY'")

    rows: list[dict] = []
    chunks = list(daterange_chunks(start, end, chunk_days))
    LOGGER.info("Downloading station=%s in %s chunk(s), chunk_days=%s", station, len(chunks), chunk_days)

    failed_chunks: list[tuple[date, date, str]] = []
    max_chunk_retries = 6
    base_backoff_s = 1.5

    for i, (a, b) in enumerate(chunks, start=1):
        url = aemet_daily_endpoint(station, a, b)
        LOGGER.info("[%s/%s] metadata: %s..%s", i, len(chunks), a, b)

        attempt = 0
        while True:
            attempt += 1
            try:
                meta = fetch_json(url, api_key=api_key)
                estado = int(meta.get("estado", 0)) if isinstance(meta, dict) else 0
                if estado == 404:
                    LOGGER.warning("No data for chunk %s..%s (404). Skipping.", a, b)
                    break
                if estado != 200 or "datos" not in meta:
                    raise RuntimeError(f"AEMET API error for chunk {a}..{b}: {meta}")

                data = fetch_data_url(meta["datos"])
                if not isinstance(data, list):
                    raise RuntimeError(f"Unexpected payload type {type(data)} for {a}..{b}")

                rows.extend(data)
                time.sleep(sleep_s)
                break
            except Exception as e:
                if attempt >= max_chunk_retries:
                    msg = f"Chunk {a}..{b} failed after {max_chunk_retries} attempts: {e}"
                    LOGGER.error(msg)
                    failed_chunks.append((a, b, str(e)))
                    break
                backoff = base_backoff_s * (2 ** (attempt - 1)) + random.uniform(0, 0.75)
                LOGGER.warning(
                    "Chunk %s..%s failed (attempt %s/%s): %s | sleeping %.2fs",
                    a, b, attempt, max_chunk_retries, e, backoff
                )
                time.sleep(backoff)

    if failed_chunks:
        LOGGER.warning("Some chunks failed and were skipped. Count=%s", len(failed_chunks))
        for a, b, err in failed_chunks[:10]:
            LOGGER.warning("Skipped chunk %s..%s | %s", a, b, err)

    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("Downloaded 0 rows (all chunks failed?). Check station and dates / AEMET status.")
    return df


def normalize_daily_types(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "fecha" in out.columns:
        out["fecha"] = pd.to_datetime(out["fecha"], errors="coerce")

    comma_decimal_cols = [
        "tmed", "prec", "tmin", "tmax", "velmedia", "racha", "sol",
        "presMax", "presMin",
    ]
    for c in comma_decimal_cols:
        if c in out.columns:
            out[c] = out[c].map(parse_comma_decimal)

    numeric_cols = ["hrMedia", "hrMax", "hrMin", "dir", "altitud"]
    for c in numeric_cols:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")

    return out


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
        if src in df.columns:
            df[dst] = df[src].map(parse_comma_decimal)
        else:
            df[dst] = pd.NA

    for c in ["temp_c", "tmax_c", "tmin_c", "humidity", "wind_ms", "prec", "pres_max_hpa", "pres_min_hpa"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["pressure_hpa"] = (df["pres_max_hpa"] + df["pres_min_hpa"]) / 2.0
    df["week_start"] = df["fecha"].dt.normalize() - pd.to_timedelta(df["fecha"].dt.weekday, unit="D")

    if coverage_rule == "tmed_only":
        has_day = df["temp_c"].notna()
    else:
        has_day = df["temp_c"].notna() | df["tmax_c"].notna() | df["tmin_c"].notna()

    df["has_data_day"] = has_day.astype(int)
    df["day"] = df["fecha"].dt.date
    n_days = df.loc[df["has_data_day"] == 1].groupby("week_start")["day"].nunique()

    g = df.groupby("week_start", as_index=False)
    weekly = g.agg(
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

    weekly["n_days"] = weekly["week_start"].map(n_days).fillna(0).astype(int)
    weekly["coverage"] = weekly["n_days"] / 7.0
    weekly = weekly.sort_values("week_start").reset_index(drop=True)
    weekly["week_start"] = pd.to_datetime(weekly["week_start"], errors="coerce")

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
    code = island_code(island_slug)
    start_s, end_s = args.start, args.end
    start_d, end_d = date.fromisoformat(start_s), date.fromisoformat(end_s)
    if end_d < start_d:
        raise ValueError("--end must be >= --start")

    raw_dir, proc_dir, logs_dir = ensure_dirs(island, "weather")
    logfile = logs_dir / f"aemet_opendata_gap_{code}_{station}_{start_s}_{end_s}.log"
    setup_logging(logfile)

    daily_path, weekly_path, daily_csv, weekly_csv = make_paths(
        raw_dir, proc_dir, code, station, start_s, end_s, args.format, args.gap_label
    )

    LOGGER.info("START GAP island=%s code=%s station=%s %s..%s", island_slug, code, station, start_s, end_s)
    LOGGER.info("Daily out -> %s", daily_path)
    LOGGER.info("Weekly gap out -> %s", weekly_path)

    df_daily = download_daily_station(
        station=station,
        start=start_d,
        end=end_d,
        chunk_days=args.chunk_days,
        sleep_s=args.sleep,
    )
    df_daily = normalize_daily_types(df_daily)
    weekly = build_weekly(df_daily, coverage_rule=args.coverage_rule)

    write_df(df_daily, daily_path, args.format)
    write_df(weekly, weekly_path, args.format)

    if args.also_csv and args.format == "parquet":
        df_daily.to_csv(daily_csv, index=False, encoding="utf-8")
        weekly.to_csv(weekly_csv, index=False, encoding="utf-8")

    print(f"Saved daily:  {daily_path}")
    print(f"Saved weekly: {weekly_path}")


if __name__ == "__main__":
    main()
