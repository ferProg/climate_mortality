from __future__ import annotations

from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from src.ingests.visibility.config import TARGET_HOUR_UTC, TIME_TOL_MIN


DEFAULT_OUT_DIR = Path("data/interim/noaa_isd_daily")


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
    m = pd.read_parquet(manifest_fp)

    # Auto-detect which column contains parquet file paths
    # Prefer columns that look like they contain paths and have ".parquet" in values.
    best_col = None
    best_score = 0

    for c in m.columns:
        s = m[c].astype(str)
        score = s.str.contains(r"\.parquet$", case=False, na=False).sum()
        if score > best_score:
            best_score = score
            best_col = c

    if best_col is None or best_score == 0:
        raise KeyError(
            "Manifest missing a recognizable parquet path column. "
            f"Columns={list(m.columns)}"
        )

    project_root = Path(__file__).resolve().parents[3]

    dfs = []
    for p in m[best_col].astype(str):
        pth = Path(p)

        if pth.is_absolute():
            resolved = pth
        else:
            s = p.replace("\\", "/")
            # Resolve relative paths from project root
            resolved = (project_root / s).resolve()

        if not resolved.exists():
            raise FileNotFoundError(f"Manifest parquet path not found: {resolved}")

        dfs.append(pd.read_parquet(resolved))

    return pd.concat(dfs, ignore_index=True)

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
    
    required = ["station", "dt_utc", "vis_m", "temp_c", "dewpoint_c"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in Step2 input: {missing}")


    df = df.dropna(subset=["dt_utc"]).copy()
    df = df[(df["dt_utc"] >= s) & (df["dt_utc"] <= e)].copy()

    df["dt_utc"] = pd.to_datetime(df["dt_utc"], utc=True, errors="coerce")
    df["vis_m"] = pd.to_numeric(df["vis_m"], errors="coerce")
    df["temp_c"] = pd.to_numeric(df["temp_c"], errors="coerce")
    df["dewpoint_c"] = pd.to_numeric(df["dewpoint_c"], errors="coerce")

    df["vis_qc"] = pd.to_numeric(df["vis_qc"], errors="coerce")

    # visibilidad no válida / código especial -> NaN
    df.loc[df["vis_qc"] == 9, "vis_m"] = np.nan

    # guardrail físico básico
    df.loc[df["vis_m"] < 0, "vis_m"] = np.nan

    df.loc[df["vis_m"] < 0, "vis_m"] = np.nan
    df["vis_is_capped_10km"] = df["vis_m"].isin([9999, 10000])

    # day key (UTC)
    df["date"] = df["dt_utc"].dt.floor("D")

    # compute minutes distance to target hour
    target_minutes = target_hour_utc * 60
    obs_minutes = df["dt_utc"].dt.hour * 60 + df["dt_utc"].dt.minute
    df["abs_min_from_target"] = (obs_minutes - target_minutes).abs()

    # keep within tolerance
    df = df[df["abs_min_from_target"] <= time_tol_min].copy()
    # choose closest per station/day
    df = df.sort_values(["station", "date", "abs_min_from_target", "dt_utc"])
    df = df.drop_duplicates(subset=["station", "date"], keep="first")

    # RH
    df["rh_pct"] = compute_rh_percent(df["temp_c"], df["dewpoint_c"])

    # Keep tidy columns
    keep = [
        "station", "date", "dt_utc",
        "vis_m", "vis_is_capped_10km",
        "temp_c", "dewpoint_c", "rh_pct",
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

