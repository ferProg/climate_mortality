from __future__ import annotations

from pathlib import Path
import re

import numpy as np
import pandas as pd


IN_DIR = Path("data/interim/noaa_isd_parsed")
OUT_DIR = Path("data/processed")
OUT_DIR.mkdir(parents=True, exist_ok=True)

STATIONS = ["GCXO", "GCTS"]  # TFN, TFS
FILE_RE = re.compile(r"^isd_(GCXO|GCTS)_(\d{4})\.parquet$")

TARGET_HOUR = 13
TARGET_MINUTE = 0
MAX_MINUTES_FROM_TARGET = 90  # tolerancia por si no hay 13:00 exacto (ajustable)


def rh_from_t_td(temp_c: pd.Series, dewpoint_c: pd.Series) -> pd.Series:
    """
    Relative Humidity (%) from temperature and dew point (°C).
    Uses Magnus formula (common, good accuracy for typical ranges).
    """
    t = temp_c.astype(float)
    td = dewpoint_c.astype(float)

    # Handle NaNs gracefully
    rh = 100.0 * np.exp((17.625 * td) / (243.04 + td)) / np.exp((17.625 * t) / (243.04 + t))
    return rh.clip(lower=0, upper=100)


def load_all_years() -> pd.DataFrame:
    dfs = []
    for fp in sorted(IN_DIR.glob("isd_*.parquet")):
        m = FILE_RE.match(fp.name)
        if not m:
            continue
        station, year = m.group(1), int(m.group(2))
        df = pd.read_parquet(fp)
        df["station"] = station  # enforce
        df["year"] = year
        dfs.append(df)

    if not dfs:
        raise FileNotFoundError(f"No yearly parquet files found in {IN_DIR}")

    out = pd.concat(dfs, ignore_index=True)

    # Ensure datetime is UTC-aware
    out["dt_utc"] = pd.to_datetime(out["dt_utc"], utc=True, errors="coerce")
    out = out.dropna(subset=["dt_utc"])

    # Ensure numeric
    for col in ["vis_m", "temp_c", "dewpoint_c"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    return out


def pick_nearest_to_13utc(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each station-day, pick the observation nearest to 13:00 UTC.
    Adds minutes_from_13utc and is_13utc_exact.
    """
    df = df.copy()

    df["date_utc"] = df["dt_utc"].dt.floor("D")
    target = df["date_utc"] + pd.to_timedelta(TARGET_HOUR, unit="h") + pd.to_timedelta(TARGET_MINUTE, unit="m")
    df["minutes_from_13utc"] = (df["dt_utc"] - target).dt.total_seconds().abs() / 60.0
    df["is_13utc_exact"] = df["minutes_from_13utc"].eq(0)

    # Pick closest per station/day
    df = df.sort_values(["station", "date_utc", "minutes_from_13utc", "dt_utc"])
    picked = df.groupby(["station", "date_utc"], as_index=False).first()

    # Optional QC: drop days too far from target time
    picked["within_time_tolerance"] = picked["minutes_from_13utc"] <= MAX_MINUTES_FROM_TARGET
    # Keep them but flag; if you want to drop:
    # picked = picked[picked["within_time_tolerance"]].copy()

    return picked


def build_daily_outputs(picked: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns:
      - daily_long: one row per station-date (13 UTC representative)
      - daily_wide: one row per date with station-specific columns
    """
    daily = picked.copy()

    # Compute RH (if temp and dewpoint exist)
    daily["rh_pct"] = rh_from_t_td(daily["temp_c"], daily["dewpoint_c"])

    # Keep only stations of interest
    daily = daily[daily["station"].isin(STATIONS)].copy()

    # LONG
    daily_long = daily[[
        "date_utc", "station", "dt_utc",
        "vis_m", "temp_c", "dewpoint_c", "rh_pct",
        "minutes_from_13utc", "is_13utc_exact", "within_time_tolerance"
    ]].sort_values(["date_utc", "station"])

    # WIDE (columns per station)
    value_cols = ["vis_m", "temp_c", "dewpoint_c", "rh_pct", "minutes_from_13utc", "is_13utc_exact", "within_time_tolerance"]
    wide = daily_long.pivot(index="date_utc", columns="station", values=value_cols)

    # Flatten MultiIndex columns: (vis_m, GCXO) -> vis_m_gcxo
    wide.columns = [f"{v}_{s.lower()}" for (v, s) in wide.columns]
    wide = wide.reset_index().sort_values("date_utc")

    return daily_long, wide


def main():
    print("Loading yearly ISD parquet files...")
    raw = load_all_years()
    print(f"Raw rows: {len(raw):,} | years: {raw['year'].min()}–{raw['year'].max()}")

    print("Selecting nearest observation to 13:00 UTC per station-day...")
    picked = pick_nearest_to_13utc(raw)
    print(f"Picked rows (station-days): {len(picked):,}")

    print("Building daily outputs (long + wide) and computing RH...")
    daily_long, daily_wide = build_daily_outputs(picked)

    # Save
    out_long = OUT_DIR / "isd_tfs_tfn_daily_13utc_long_2016_2024.parquet"
    out_wide = OUT_DIR / "isd_tfs_tfn_daily_13utc_wide_2016_2024.parquet"
    daily_long.to_parquet(out_long, index=False)
    daily_wide.to_parquet(out_wide, index=False)

    print(f"\nSaved:\n- {out_long}\n- {out_wide}")
    print("\nPreview (wide):")
    print(daily_wide.head(5).to_string(index=False))


if __name__ == "__main__":
    main()