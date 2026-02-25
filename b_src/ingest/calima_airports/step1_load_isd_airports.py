from __future__ import annotations

import gzip
import urllib.request
from pathlib import Path
from typing import Dict, Tuple, Iterator, Optional, List

import pandas as pd
from urllib.error import HTTPError

# isdparser
from isdparser import ISDRecordFactory  # as in your current code


# -----------------------
# Defaults (b_data)
# -----------------------
DEFAULT_RAW_DIR = Path("b_data/raw/noaa_isd")
DEFAULT_OUT_DIR = Path("b_data/interim/noaa_isd_parsed")

# Debug flags (keep yours)
#DEBUG_PRINT_FIRST_SCHEMA = False
#DEBUG_MAX_LINES_PER_FILE = None
DEBUG_PRINT_FIRST_SCHEMA = True
DEBUG_MAX_LINES_PER_FILE = 2000


def years_from_dates(start_date: str, end_date: str) -> List[int]:
    s = pd.to_datetime(start_date, utc=True)
    e = pd.to_datetime(end_date, utc=True)

    if e < s:
        raise ValueError(f"end_date must be >= start_date (got {end_date} < {start_date})")

    return list(range(int(s.year), int(e.year) + 1))

def download_isd_year(usaf: str, wban: str, year: int, raw_dir: Path) -> Path | None:
    url = f"https://www.ncei.noaa.gov/pub/data/noaa/{year}/{usaf}-{wban}-{year}.gz"
    out = raw_dir / f"{usaf}-{wban}-{year}.gz"
    if out.exists():
        return out
    try:
        print(f"Downloading: {url}")
        urllib.request.urlretrieve(url, out)
        return out
    except HTTPError as e:
        if e.code == 404:
            print(f"⚠️ Missing file (404): {usaf}-{wban}-{year}.gz")
            return None
        raise


def iter_isd_lines(gz_path: Path) -> Iterator[str]:
    with gzip.open(gz_path, "rt", encoding="utf-8", errors="replace") as f:
        for line in f:
            yield line.rstrip("\n")


def _get_section(schema: dict, section_name: str) -> dict | None:
    for sec in schema.get("sections", []):
        if isinstance(sec, dict) and sec.get("name") == section_name:
            return sec
    return None


def _measures_to_dict(section: dict) -> dict:
    out = {}
    for m in section.get("measures", []):
        if isinstance(m, dict) and "measure" in m:
            out[m["measure"]] = m.get("value")
    return out


def _get_dt_from_control_or_datestamp(schema: dict, control: dict | None) -> pd.Timestamp | None:
    # 1) Try CONTROL -> measures dict
    if control is not None:
        md = _measures_to_dict(control)

        candidates = [
            ("year", "month", "day", "hour", "minute"),
            ("date_year", "date_month", "date_day", "time_hour", "time_minute"),
        ]

        for yk, mok, dk, hk, mik in candidates:
            y = md.get(yk)
            mo = md.get(mok)
            d = md.get(dk)
            h = md.get(hk)
            mi = md.get(mik)
            if None not in (y, mo, d, h, mi):
                try:
                    return pd.Timestamp(int(y), int(mo), int(d), int(h), int(mi), tz="UTC")
                except Exception:
                    pass

    # 2) Fallback: schema datestamp
    ds = schema.get("datestamp")
    if ds is not None:
        try:
            return pd.to_datetime(ds, utc=True)
        except Exception:
            return None

    return None


def _find_measure_value(section: dict, contains_any: list[str]) -> float | None:
    measures = section.get("measures", [])
    for m in measures:
        if not isinstance(m, dict):
            continue
        mname = str(m.get("measure", "")).lower()
        if any(tok in mname for tok in contains_any):
            v = m.get("value")
            try:
                if v is None:
                    continue
                return float(v)
            except Exception:
                continue
    return None


def _debug_print_schema_once(schema: dict) -> None:
    control = _get_section(schema, "control")
    mandatory = _get_section(schema, "mandatory")

    print("\n=== DEBUG: first schema snapshot ===")
    print("Top keys:", list(schema.keys()))
    print("identifier:", schema.get("identifier"))
    print("datestamp:", schema.get("datestamp"))
    print("sections:", [sec.get("name") for sec in schema.get("sections", []) if isinstance(sec, dict)])

    if control:
        cm = control.get("measures", [])
        print(f"control measures count: {len(cm)}")
        print("control measure names (first 30):",
              [m.get("measure") for m in cm[:30] if isinstance(m, dict)])
        md = _measures_to_dict(control)
        print("control dict keys sample (first 30):", list(md.keys())[:30])

    if mandatory:
        mm = mandatory.get("measures", [])
        print(f"mandatory measures count: {len(mm)}")
        names = [m.get("measure") for m in mm if isinstance(m, dict) and m.get("measure")]
        rel = [n for n in names if any(k in n.lower() for k in ["vis", "temp", "dew", "point"])]
        print("mandatory measure names containing vis/temp/dew:", rel[:50])
        print("mandatory measure names (first 30):", names[:30])

    print("=== END DEBUG ===\n")


def parse_line_minimal(factory: ISDRecordFactory, line: str, debug_once: dict) -> Optional[dict]:
    try:
        rec = factory.create(line)
        s = rec.schema()
    except Exception:
        return None

    if DEBUG_PRINT_FIRST_SCHEMA and not debug_once.get("printed"):
        _debug_print_schema_once(s)
        debug_once["printed"] = True

    control = _get_section(s, "control")
    mandatory = _get_section(s, "mandatory")
    if mandatory is None:
        return None

    dt_utc = _get_dt_from_control_or_datestamp(s, control)
    if dt_utc is None:
        return None

    vis_m = _find_measure_value(mandatory, ["visibility_observation_distance_dimension"])
    temp_c = _find_measure_value(mandatory, ["air_temperature_observation_air_temperature"])
    dewpoint_c = _find_measure_value(mandatory, ["air_temperature_observation_dew_point_temperature"])
    vis_qc = _find_measure_value(mandatory, ["visibility_observation_distance_quality_code"])
    temp_qc = _find_measure_value(mandatory, ["air_temperature_observation_air_temperature_quality_code"])
    dew_qc  = _find_measure_value(mandatory, ["air_temperature_observation_dew_point_quality_code"])

    return {
        "dt_utc": dt_utc,
        "vis_m": vis_m,
        "temp_c": temp_c,
        "dewpoint_c": dewpoint_c,
        "vis_qc": vis_qc,
        "temp_qc": temp_qc,
        "dew_qc": dew_qc,
    }


def load_station_year(icao: str, usaf: str, wban: str, year: int, raw_dir: Path) -> pd.DataFrame:
    gz = download_isd_year(usaf, wban, year, raw_dir=raw_dir)
    if gz is None:
        return pd.DataFrame()

    factory = ISDRecordFactory()
    rows = []
    debug_once = {"printed": False}

    for i, line in enumerate(iter_isd_lines(gz), start=1):
        if DEBUG_MAX_LINES_PER_FILE is not None and i > DEBUG_MAX_LINES_PER_FILE:
            break

        d = parse_line_minimal(factory, line, debug_once)
        if d is None:
            continue

        d["station"] = icao
        d["year"] = year
        rows.append(d)

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["dt_utc"] = pd.to_datetime(df["dt_utc"], utc=True)
    for col in ["vis_m", "temp_c", "dewpoint_c"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def run_step1_load_isd(
    stations: Dict[str, Tuple[str, str]],
    start_date: str,
    end_date: str,
    raw_dir: Path = DEFAULT_RAW_DIR,
    out_dir: Path = DEFAULT_OUT_DIR,
    out_name: Optional[str] = None,
    force: bool = False
) -> Path:
    """
    Build per-station per-year parquet files + a small manifest parquet listing them.
    Years are derived from start_date/end_date.
    Returns manifest parquet path.
    """
    raw_dir = Path(raw_dir)
    out_dir = Path(out_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    years = years_from_dates(start_date, end_date)
    

    if out_name is None:
        out_name = f"isd_manifest_{years[0]}_{years[-1]}.parquet"
    manifest_fp = out_dir / out_name

    produced = []

    for icao, (usaf, wban) in stations.items():
        if usaf is None or wban is None:
            print(f"⚠️ Skip {icao}: missing USAF/WBAN")
            continue

        for y in years:
            out_path = out_dir / f"isd_{icao}_{y}.parquet"

            if out_path.exists() and not force:
                print(f"Skip (exists): {out_path.name}")
                produced.append(...)
                continue

            print(f"Parsing {icao} {y}...")
            df = load_station_year(icao, usaf, wban, y, raw_dir=raw_dir)
            print(f"  rows={len(df):,}")

            if df.empty:
                print("  (empty) nothing to save")
                continue

            df.to_parquet(out_path, index=False)
            print(f"  saved -> {out_path.name}")
            produced.append({"icao": icao, "year": y, "parquet": str(out_path)})

    manifest = pd.DataFrame(produced)
    manifest["start_date"] = start_date
    manifest["end_date"] = end_date
    manifest.to_parquet(manifest_fp, index=False)

    print("\nDone. Per-year parquet files in:", out_dir)
    print("Manifest:", manifest_fp)
    return manifest_fp


if __name__ == "__main__":
    # Standalone example (kept minimal)
    STATIONS = {
        "GCXO": ("600150", "99999"),
        "GCTS": ("600250", "99999"),
    }
    fp = run_step1_load_isd(
        stations=STATIONS,
        start_date="2016-01-01",
        end_date="2025-12-31",
    )
    print("Wrote:", fp)