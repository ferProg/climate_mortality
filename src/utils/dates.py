from __future__ import annotations
import re
import pandas as pd
from pandas._libs.tslibs.nattype import NaTType

def periodo_to_week_start(periodo: str) -> pd.Timestamp | NaTType:
    if not isinstance(periodo, str):
        return pd.NaT
    m = re.match(r"^(\d{4})SM(\d{2})$", periodo.strip())
    if not m:
        return pd.NaT
    year = int(m.group(1))
    week = int(m.group(2))
    try:
        return pd.Timestamp.fromisocalendar(year, week, 1)
    except ValueError:
        return pd.NaT

def to_week_start_from_datetime(dt: pd.Series) -> pd.Series:
    dt = pd.to_datetime(dt, errors="coerce", utc=True)
    dt = dt.dt.tz_localize(None)
    return dt.dt.floor("D") - pd.to_timedelta(dt.dt.weekday, unit="D")


def normalize_week_start(dt: pd.Series) -> pd.Series:
    dt = pd.to_datetime(dt, errors="coerce", utc=True)
    return dt.dt.tz_localize(None)