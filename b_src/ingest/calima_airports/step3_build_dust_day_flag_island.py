from __future__ import annotations

from pathlib import Path
from typing import Optional, List
import pandas as pd

DEFAULT_OUT_DIR = Path("b_data/interim/calima_proxy_daily")

# Rule C thresholds (hardcoded here to avoid config churn; move to config if you want)
VIS_STRONG_LT_M = 9_999      # < 10 km
VIS_MODERATE_LT_M = 20_000   # < 20 km
RH_DRY_LT_PCT = 70.0         # "dry"
RH_VERY_DRY_LT_PCT = 30.0    # "very dry" for possible when no vis


def run_step3_build_island_flags(
    daily_fp: Path,
    island: str,
    stations: List[str],
    out_dir: Path = DEFAULT_OUT_DIR,
    out_name: Optional[str] = None,
) -> Path:
    """
    Input: daily per station (Step2)
    Output: island daily with:
      - calima_confirmed_day (visibility-based, Rule C)
      - calima_possible_day (visibility missing but very dry)
      - calima_any_day = confirmed OR possible
    """
    daily_fp = Path(daily_fp)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(daily_fp)
    df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")
    df = df.dropna(subset=["date"]).copy()

    # keep only requested stations
    df = df[df["station"].isin(stations)].copy()

    v = df["vis_m"]
    rh = df["rh_pct"]

    # Confirmed: strong vis reduction OR moderate vis reduction + dry
    confirmed = (
        v.notna() & (
            (v < VIS_STRONG_LT_M) |
            ((v < VIS_MODERATE_LT_M) & rh.notna() & (rh < RH_DRY_LT_PCT))
        )
    )

    # Possible: no visibility reported but extremely dry (conservative)
    possible = (v.isna() & rh.notna() & (rh < RH_VERY_DRY_LT_PCT))

    df["calima_confirmed_station_day"] = confirmed.astype(int)
    df["calima_possible_station_day"] = possible.astype(int)

    island_daily = (df.groupby("date", as_index=False)
                      .agg(
                          confirmed_airports=("calima_confirmed_station_day", "sum"),
                          possible_airports=("calima_possible_station_day", "sum"),
                          airports_obs=("station", "nunique"),
                          vis_min_m=("vis_m", "min"),
                          rh_min_pct=("rh_pct", "min"),
                      ))

    # Since we're typically using 1 airport per island (TFS for Tenerife),
    # we treat >=1 as positive at island level.
    island_daily["calima_confirmed_day"] = (island_daily["confirmed_airports"] >= 1).astype(int)
    island_daily["calima_possible_day"] = (island_daily["possible_airports"] >= 1).astype(int)
    island_daily["calima_any_day"] = ((island_daily["calima_confirmed_day"] == 1) |
                                      (island_daily["calima_possible_day"] == 1)).astype(int)

    island_daily["island"] = island

    if out_name is None:
        out_name = f"calima_proxy_daily_{island}.parquet"

    out_fp = out_dir / out_name
    island_daily.to_parquet(out_fp, index=False)
    print("Saved island daily:", out_fp, "rows:", len(island_daily))
    return out_fp


if __name__ == "__main__":
    fp = run_step3_build_island_flags(
        daily_fp=Path("b_data/interim/noaa_isd_daily/isd_daily_2016-01-01_2024-12-31.parquet"),
        island="tenerife",
        stations=["GCTS"],  # Tenerife Sur (principal)
    )
    print(fp)