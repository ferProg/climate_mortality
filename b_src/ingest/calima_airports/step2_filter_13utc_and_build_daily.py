from __future__ import annotations

from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from calima_airports.calima_config import TARGET_HOUR_UTC, TIME_TOL_MIN


DEFAULT_OUT_DIR = Path("b_data/interim/noaa_isd_daily")


def compute_rh_percent(temp_c: pd.Series, dewpoint_c: pd.Series) -> pd.Series:
    """
    Approx RH from T and Td (Magnus formula).
    Returns RH in percent.
    """
    # Avoid overflow / garbage
    t = temp_c.astype(float)
    td = dewpoint_c.astype(float)

    # Magnus constants over water
    a = 17.625
    b = 243.04

    # e_s(T) proportional term cancels; compute ratio
    rh = 100.0 * np.exp((a * td) / (b + td) - (a * t) / (b + t))
    return rh


def _load_from_manifest(manifest_fp: Path) -> pd.DataFrame:
    man = pd.read_parquet(manifest_fp)

    # project root = parent of notebooks/ or where b_data lives; easiest:
    # manifest is in .../b_data/interim/noaa_isd_parsed/, so project root is 3 parents up:
    #   .../b_data/interim/noaa_isd_parsed -> project root
    project_root = Path(manifest_fp).resolve().parents[3]

    dfs = []
    for p in man["parquet"].tolist():
        pth = Path(p)

        if pth.is_absolute():
            fp = pth
        else:
            # If manifest already stores paths like "b_data/interim/noaa_isd_parsed/isd_....parquet"
            # resolve from project root
            fp = (project_root / pth).resolve()

        if not fp.exists():
            raise FileNotFoundError(f"Manifest parquet path not found: {fp}")

        dfs.append(pd.read_parquet(fp))

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def run_step2_build_daily(
    manifest_fp: Path,
    start_date: str,
    end_date: str,
    out_dir: Path = DEFAULT_OUT_DIR,
    out_name: Optional[str] = None,
    target_hour_utc: int = TARGET_HOUR_UTC,
    time_tol_min: int = TIME_TOL_MIN,
) -> Path:
    """
    Build daily table per station from ISD minimal observations.
    - Filters observations within ±time_tol_min of target_hour_utc.
    - Picks closest observation per station/day.
    - Computes RH%.
    """
    manifest_fp = Path(manifest_fp)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    s = pd.to_datetime(start_date, utc=True)
    e = pd.to_datetime(end_date, utc=True) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

    df = _load_from_manifest(manifest_fp)
    if df.empty:
        raise ValueError("Manifest resolved to 0 rows. Check manifest paths.")

    df["dt_utc"] = pd.to_datetime(df["dt_utc"], utc=True, errors="coerce")
    df = df.dropna(subset=["dt_utc"]).copy()
    df = df[(df["dt_utc"] >= s) & (df["dt_utc"] <= e)].copy()

    # day key (UTC)
    df["date"] = df["dt_utc"].dt.floor("D")

    # compute minutes distance to target hour
    target_minutes = target_hour_utc * 60
    obs_minutes = df["dt_utc"].dt.hour * 60 + df["dt_utc"].dt.minute
    df["abs_min_from_target"] = (obs_minutes - target_minutes).abs()

    # keep within tolerance
    df = df[df["abs_min_from_target"] <= time_tol_min].copy()

    # choose closest per station/day
    df = df.sort_values(["station", "date", "abs_min_from_target"])
    df = df.drop_duplicates(subset=["station", "date"], keep="first")

    # RH
    df["rh_pct"] = compute_rh_percent(df["temp_c"], df["dewpoint_c"])

    # Keep tidy columns
    keep = [
        "station", "date", "dt_utc",
        "vis_m", "temp_c", "dewpoint_c", "rh_pct",
        "vis_qc", "temp_qc", "dew_qc",
        "abs_min_from_target",
    ]
    out = df[keep].copy()

    if out_name is None:
        out_name = f"isd_daily_{start_date}_{end_date}.parquet".replace(":", "").replace("/", "-")

    out_fp = out_dir / out_name
    out.to_parquet(out_fp, index=False)
    print("Saved daily:", out_fp, "rows:", len(out))
    return out_fp


if __name__ == "__main__":
    # Example usage (update path to your manifest)
    fp = run_step2_build_daily(
        manifest_fp=Path("b_data/interim/noaa_isd_parsed/isd_manifest_tenerife_2016-01-01_2024-12-31.parquet"),
        start_date="2016-01-01",
        end_date="2024-12-31",
    )
    print(fp)