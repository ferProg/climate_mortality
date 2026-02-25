"""
step1_load_isd_tfs_tfn.py

Load NOAA ISD "full" observations for Tenerife airports
- TFN / GCXO (Tenerife Norte): USAF 600150, WBAN 99999
- TFS / GCTS (Tenerife Sur):  USAF 600250, WBAN 99999

Outputs (interim):
- data/interim/noaa_isd_parsed/isd_tfs_tfn_minimal_2016_2025.parquet

Notes:
- Uses isdparser (git+https://github.com/cunybpl/isdparser.git)
- Robust to missing yearly files (HTTP 404) and will skip them.
- Includes debug to reveal measure names/paths if parsing yields 0 rows.
"""

from __future__ import annotations

import gzip
import urllib.request
from pathlib import Path
from typing import Iterator, Optional
from urllib.error import HTTPError

import pandas as pd

from isdparser import ISDRecordFactory  # type: ignore


RAW_DIR = Path("data/raw/noaa_isd")
OUT_DIR = Path("data/interim/noaa_isd_parsed")
RAW_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

STATIONS = {
    "GCXO": ("600150", "99999"),  # Tenerife Norte (TFN)
    "GCTS": ("600250", "99999"),  # Tenerife Sur  (TFS)
}

YEARS = list(range(2016, 2025))  # 2016–2025 inclusive

# Debug controls
DEBUG_PRINT_FIRST_SCHEMA = False    # set False once fixed
DEBUG_MAX_LINES_PER_FILE = None     # e.g. 50_000 to quick-test; None = all


def download_isd_year(usaf: str, wban: str, year: int) -> Path | None:
    url = f"https://www.ncei.noaa.gov/pub/data/noaa/{year}/{usaf}-{wban}-{year}.gz"
    out = RAW_DIR / f"{usaf}-{wban}-{year}.gz"
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
    """
    Prefer CONTROL fields, but fall back to schema['datestamp'] if needed.
    This avoids dropping all rows due to naming differences in control measures.
    """
    # 1) Try CONTROL -> measures dict
    if control is not None:
        md = _measures_to_dict(control)

        # Print available keys once if needed
        # (done in the global debug block)

        # Try common naming variants
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

    # 2) Fallback: schema datestamp (your earlier debug showed it exists)
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
        # Print only relevant-looking ones
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
        return None  # without mandatory we can't get vis/temp/dew

    dt_utc = _get_dt_from_control_or_datestamp(s, control)
    if dt_utc is None:
        return None

    # Extract vars from mandatory.measures (names depend on your parser; debug will show them)
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
        "vis_qc" : vis_qc,
        "temp_qc" : temp_qc,
        "dew_qc" : dew_qc
    }


def load_station_year(icao: str, usaf: str, wban: str, year: int) -> pd.DataFrame:
    gz = download_isd_year(usaf, wban, year)
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


def main():

    for icao, (usaf, wban) in STATIONS.items():
        for y in YEARS:
            out_path = OUT_DIR / f"isd_{icao}_{y}.parquet"

            #Renaudar: si ya existe, lo saltamos
            if out_path.exists():
                print(f"Skip (exists): {out_path.name}")
                continue

            print(f"Parsing {icao} {y}...")
            df = load_station_year(icao, usaf, wban, y)
            print(f"  rows={len(df):,}")
            
            if df.empty:
                print(" (empty) nothing to save")
                continue

            df.to_parquet(out_path, index=False)
            print(f" saved -> {out_path}")
    print("\nDone. You now have per-year parquet files in:", OUT_DIR)


if __name__ == "__main__":
    main()