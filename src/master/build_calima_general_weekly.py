from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional, Tuple

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"

RAW_XLSX = DATA_DIR / "raw" / "calima_Heliyon" / "Envío_datos_Calima.xlsx"
SHEET_NAME = "Hoja1"

DATE_START_COL = "Fecha ini"
DATE_END_COL = "Fecha fin"
DAI_COL = "DAI"

OUT_DIR = DATA_DIR / "processed" / "calima"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def week_start_monday(d: pd.Series) -> pd.Series:
    d = pd.to_datetime(d, errors="coerce")
    return (d - pd.to_timedelta(d.dt.weekday, unit="D")).dt.normalize()


def expand_ranges_to_daily(df: pd.DataFrame) -> pd.DataFrame:
    """
    Expand each [Fecha ini, Fecha fin] row to daily rows carrying the event DAI.
    If multiple events overlap same day, keep the max DAI.
    """
    df = df.copy()

    df[DATE_START_COL] = pd.to_datetime(df[DATE_START_COL], errors="coerce", dayfirst=True)
    df[DATE_END_COL] = pd.to_datetime(df[DATE_END_COL], errors="coerce", dayfirst=True)
    df[DAI_COL] = pd.to_numeric(df[DAI_COL], errors="coerce")

    df = df.dropna(subset=[DATE_START_COL, DATE_END_COL, DAI_COL])
    df = df.loc[df[DATE_END_COL] >= df[DATE_START_COL]].copy()

    starts = df[DATE_START_COL].dt.normalize().to_list()
    ends = df[DATE_END_COL].dt.normalize().to_list()
    dais = df[DAI_COL].to_list()

    pieces = []
    for start, end, dai in zip(starts, ends, dais):
        days = pd.date_range(start=start, end=end, freq="D")
        pieces.append(pd.DataFrame({"date": days, "dai": dai}))

    if not pieces:
        return pd.DataFrame(columns=["date", "dai"])

    daily = pd.concat(pieces, ignore_index=True)

    # if multiple events same date, keep max
    daily = (
        daily.groupby("date", as_index=False)["dai"]
        .max()
        .sort_values("date")
        .reset_index(drop=True)
    )
    return daily


def make_complete_weekly_index(weekly: pd.DataFrame) -> pd.DataFrame:
    """Fill missing weeks with DAI=0 so level 0 is explicit."""
    wmin = weekly["week_start"].min()
    wmax = weekly["week_start"].max()
    full = pd.DataFrame({"week_start": pd.date_range(wmin, wmax, freq="W-MON")})
    out = full.merge(weekly, on="week_start", how="left")
    out["calima_canarias_dai_week"] = out["calima_canarias_dai_week"].fillna(0.0)
    return out


def dai_week_to_level(weekly_dai: pd.Series) -> Tuple[pd.Series, dict]:
    """
    Map weekly DAI -> 0..3 using terciles over DAI>0.
    0: no event (DAI==0)
    1/2/3: low/med/high terciles among event weeks
    """
    x = pd.to_numeric(weekly_dai, errors="coerce").fillna(0.0)

    event = x[x > 0]
    if event.empty:
        level = pd.Series(np.zeros(len(x), dtype=int), index=x.index)
        return level, {"q33": None, "q66": None}

    q33 = float(event.quantile(0.33))
    q66 = float(event.quantile(0.66))

    level = pd.Series(np.zeros(len(x), dtype=int), index=x.index)
    level[(x > 0) & (x <= q33)] = 1
    level[(x > q33) & (x <= q66)] = 2
    level[x > q66] = 3

    return level.astype(int), {"q33": q33, "q66": q66}


def main(argv: list[str]) -> int:
    if not RAW_XLSX.exists():
        raise FileNotFoundError(RAW_XLSX)

    df = pd.read_excel(RAW_XLSX, sheet_name=SHEET_NAME)

    for col in [DATE_START_COL, DATE_END_COL, DAI_COL]:
        if col not in df.columns:
            raise KeyError(f"Missing column '{col}' in Excel. Found: {list(df.columns)}")

    daily = expand_ranges_to_daily(df)
    if daily.empty:
        raise RuntimeError("No daily rows produced from the calima Excel (check date parsing).")

    daily["week_start"] = week_start_monday(daily["date"])

    # Weekly DAI: max adversity in the week (conservative, aligns with alerts)
    weekly = (
        daily.groupby("week_start", as_index=False)["dai"]
        .max()
        .rename(columns={"dai": "calima_canarias_dai_week"})
        .sort_values("week_start")
        .reset_index(drop=True)
    )

    weekly = make_complete_weekly_index(weekly)
    weekly["calima_canarias_level_week"], thr = dai_week_to_level(weekly["calima_canarias_dai_week"])

    out_fp = OUT_DIR / "calima_general_weekly.parquet"
    weekly.to_parquet(out_fp, index=False)

    print(f"💾 Saved: {out_fp}")
    print(f"   shape={weekly.shape} min={weekly['week_start'].min()} max={weekly['week_start'].max()}")
    print(f"   thresholds (event weeks): q33={thr['q33']} q66={thr['q66']}")
    print("   level counts:\n", weekly["calima_canarias_level_week"].value_counts().sort_index())

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))