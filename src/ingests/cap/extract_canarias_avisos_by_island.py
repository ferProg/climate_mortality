# src/ingests/cap/extract_canarias_avisos_by_island.py
# Extrae avisos CAP de AEMET para una isla canaria a partir del dataset
# particionado de Canarias y construye un dataset semanal de alertas.
#
# Qué hace:
# 1) Lee el dataset parquet particionado de avisos CAP de Canarias.
# 2) Normaliza el nombre de la isla recibido por CLI.
# 3) Filtra los avisos de la isla:
#    - primero intenta usar la columna has_<isla> si es informativa
#    - si no, cae a un filtro por areaDesc.
# 4) Convierte onset/onset_dt a datetime UTC y aplica filtro de fechas.
# 5) Deriva variables de fenómeno:
#    - is_dust_event
#    - is_heat_event
#    a partir de event/headline.
# 6) Convierte severity textual a level_score numérico:
#    minor=1, moderate=2, severe=3, extreme=4
# 7) Agrega por semana (lunes) para obtener:
#    - cap_heat_level_max_week
#    - cap_heat_yellow_plus_week
#    - cap_dust_level_max_week
#    - cap_dust_yellow_plus_week
#    - cap_coverage_week
# 8) Opcionalmente guarda también el parquet de avisos filtrados.
# 9) Guarda el weekly final en data/processed/<island>/cap/.
#
# Salidas:
# - cap_alerts_<code>_<tag>.parquet   (opcional)
# - cap_weekly_<code>_<startyear>_<endyear>.parquet
#
# Nota:
# - Si se pasan --start y --end, el weekly se calendariza a todas las semanas
#   del rango observado entre min y max week_start.
# - week_start se deja tz-naive para facilitar merges posteriores.

from __future__ import annotations

import argparse
import logging
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd

from src.utils.constants import island_code
from src.utils.io import ensure_dir
from src.utils.text import safe_slug
from src.utils.logging import setup_logging

LOGGER = logging.getLogger(__name__)


# ----------------------------
# Island normalization
# ----------------------------

CANONICAL_ISLANDS = {
    "tenerife": ["tenerife"],
    "gran_canaria": ["gran_canaria", "gran canaria"],
    "lanzarote": ["lanzarote"],
    "fuerteventura": ["fuerteventura"],
    "la_palma": ["la_palma", "la palma", "palma, la"],
    "gomera": ["gomera", "la_gomera", "la gomera", "gomera, la"],
    "hierro": ["hierro", "el_hierro", "el hierro", "hierro, el"],
}

# matches columns created by the ingest generic
HAS_FLAG_COL = {
    "tenerife": "has_tenerife",
    "gran_canaria": "has_gran_canaria",
    "lanzarote": "has_lanzarote",
    "fuerteventura": "has_fuerteventura",
    "la_palma": "has_la_palma",
    "gomera": "has_la_gomera",
    "hierro": "has_el_hierro",
}

# ----------------------------
# Phenomenon + severity mapping
# ----------------------------

DUST_RE = r"(dust|polvo)"
HEAT_RE = r"(temper|high-temperature|calor)"

SEVERITY_MAP = {
    "minor": 1,
    "moderate": 2,
    "severe": 3,
    "extreme": 4,
}


def strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))


def normalize_island_name(s: str) -> str:
    s0 = strip_accents(s).strip().lower()
    s0 = s0.replace("-", " ").replace("_", " ")
    s0 = " ".join(s0.split())
    for canon, variants in CANONICAL_ISLANDS.items():
        if s0 == canon.replace("_", " "):
            return canon
        for v in variants:
            if s0 == v.replace("_", " "):
                return canon
    # fallback: try direct
    if s0 in CANONICAL_ISLANDS:
        return s0
    raise ValueError(f"Unknown island '{s}'. Expected one of: {list(CANONICAL_ISLANDS.keys())}")


# ----------------------------
# Paths
# ----------------------------

def processed_cap_dir(project_root: Path, island: str) -> Path:
    out_dir = project_root / "data" / "processed" / safe_slug(island) / "cap"
    ensure_dir(out_dir)
    return out_dir


def load_canarias_dataset(data_dir: Path) -> pd.DataFrame:
    """
    Reads partitioned parquet dataset directory: year_onset=YYYY/part_*.parquet
    """
    if not data_dir.exists():
        raise FileNotFoundError(f"Dataset dir not found: {data_dir}")

    # Read as a dataset (pyarrow) if available; fallback to manual glob
    try:
        df = pd.read_parquet(data_dir)
        return df
    except Exception:
        parts = list(data_dir.rglob("part_*.parquet"))
        if not parts:
            raise FileNotFoundError(f"No part_*.parquet found under {data_dir}")
        dfs = [pd.read_parquet(p) for p in parts]
        return pd.concat(dfs, ignore_index=True)


# ----------------------------
# Weekly builder
# ----------------------------

def to_week_start(dt: pd.Series) -> pd.Series:
    dt = pd.to_datetime(dt, errors="coerce", utc=True)
    # normalize to date then subtract weekday to Monday
    d = dt.dt.tz_convert("UTC").dt.floor("D")
    return d - pd.to_timedelta(d.dt.weekday, unit="D")


def build_cap_weekly(df_alerts: pd.DataFrame) -> pd.DataFrame:
    """
    Expects df_alerts contains:
      - onset_dt (datetime, UTC)
      - is_heat_event (0/1)
      - is_dust_event (0/1)
      - level_score (int)
    Returns weekly with:
      - cap_heat_level_max_week, cap_heat_yellow_plus_week
      - cap_dust_level_max_week, cap_dust_yellow_plus_week
      - cap_coverage_week
    """
    df = df_alerts.copy()

    if "onset_dt" not in df.columns:
        # fallback to onset string
        df["onset_dt"] = pd.to_datetime(df.get("onset"), errors="coerce", utc=True)

    df = df.dropna(subset=["onset_dt"]).copy()
    if df.empty:
        # return empty but well-formed
        return pd.DataFrame(columns=[
            "week_start",
            "cap_heat_level_max_week", "cap_heat_yellow_plus_week",
            "cap_dust_level_max_week", "cap_dust_yellow_plus_week",
            "cap_coverage_week",
        ])

    df["week_start"] = to_week_start(df["onset_dt"])

    # Ensure columns exist
    for c in ["is_heat_event", "is_dust_event", "level_score"]:
        if c not in df.columns:
            df[c] = 0

    df["level_score"] = pd.to_numeric(df["level_score"], errors="coerce").fillna(0).astype(int)
    df["is_heat_event"] = pd.to_numeric(df["is_heat_event"], errors="coerce").fillna(0).astype(int)
    df["is_dust_event"] = pd.to_numeric(df["is_dust_event"], errors="coerce").fillna(0).astype(int)

    # Aggregate max level per week per phenomenon
    heat = (
        df[df["is_heat_event"] == 1]
        .groupby("week_start")["level_score"]
        .max()
        .rename("cap_heat_level_max_week")
    )
    dust = (
        df[df["is_dust_event"] == 1]
        .groupby("week_start")["level_score"]
        .max()
        .rename("cap_dust_level_max_week")
    )

    weekly = pd.DataFrame({"week_start": pd.DatetimeIndex(df["week_start"].unique()).sort_values()})
    weekly = weekly.merge(heat.reset_index(), on="week_start", how="left")
    weekly = weekly.merge(dust.reset_index(), on="week_start", how="left")

    weekly["cap_heat_level_max_week"] = weekly["cap_heat_level_max_week"].fillna(0).astype(int)
    weekly["cap_dust_level_max_week"] = weekly["cap_dust_level_max_week"].fillna(0).astype(int)

    weekly["cap_heat_yellow_plus_week"] = (weekly["cap_heat_level_max_week"] >= 2).astype(int)
    weekly["cap_dust_yellow_plus_week"] = (weekly["cap_dust_level_max_week"] >= 2).astype(int)

    # Coverage: days in week that had any CAP record for that island/phenomenon
    df_days = df.copy()
    df_days["day"] = pd.to_datetime(df_days["onset_dt"], utc=True).dt.floor("D")
    any_day = df_days.groupby(["week_start", "day"]).size().reset_index(name="n")
    n_days = any_day.groupby("week_start")["day"].nunique()
    weekly["cap_coverage_week"] = weekly["week_start"].map(n_days).fillna(0).astype(int) / 7.0

    weekly["week_start"] = pd.to_datetime(weekly["week_start"], errors="coerce")  # keep tz-naive Monday
    weekly = weekly.sort_values("week_start").reset_index(drop=True)
    return weekly


# ----------------------------
# CLI
# ----------------------------

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Extract AEMET CAP alerts for a Canary Island from a partitioned parquet dataset and build weekly CAP flags."
    )
    ap.add_argument("--isla", required=True, help='Island name (e.g. "Tenerife", "El Hierro")')
    ap.add_argument("--data-dir", default="data/interim/cap/canarias", help="Partitioned dataset directory (year_onset=YYYY/...)")
    ap.add_argument("--start", default=None, help="YYYY-MM-DD (filter by onset_dt >= start)")
    ap.add_argument("--end", default=None, help="YYYY-MM-DD (filter by onset_dt <= end)")
    ap.add_argument("--save-alerts", action="store_true", help="Also save filtered alerts parquet in processed/<island>/cap/")
    ap.add_argument("--out-alerts", default=None, help="Optional explicit output path for alerts parquet")
    ap.add_argument("--out-weekly", default=None, help="Optional explicit output path for weekly parquet")
    ap.add_argument("--debug", action="store_true")
    return ap.parse_args()


def main() -> None:
    args = parse_args()

    project_root = Path(__file__).resolve().parents[3]  # src/ingests/cap -> project root
    data_dir = Path(args.data_dir)
    if not data_dir.is_absolute():
        data_dir = (project_root / data_dir).resolve()

    island = normalize_island_name(args.isla)
    code = island_code(island)

    # logging
    logs_dir = project_root / "logs" / island
    ensure_dir(logs_dir)
    log_fp = logs_dir / f"cap_extract_{code}_{args.start or 'NA'}_{args.end or 'NA'}.log"
    setup_logging(log_fp)

    LOGGER.info("START extract CAP | island=%s code=%s data_dir=%s", island, code, data_dir)

    # ----------------------------
    # Load and filter by island
    # ----------------------------
    df = load_canarias_dataset(data_dir)

    # Prefer has_* flag ONLY if it's informative; otherwise fallback to areaDesc.
    has_col = HAS_FLAG_COL.get(island)
    used_filter = None

    if has_col and has_col in df.columns:
        nunq = int(df[has_col].nunique(dropna=False))
        if nunq > 1:
            df = df[df[has_col].fillna(0).astype(int) == 1].copy()
            used_filter = f"flag:{has_col}"
            LOGGER.info("Filtered by flag %s -> rows=%s", has_col, len(df))
        else:
            LOGGER.warning(
                "Flag column %s is constant (nunique=%s). Falling back to areaDesc.",
                has_col, nunq
            )

    if used_filter is None:
        needle = strip_accents(island).replace("_", " ")
        area = df.get("areaDesc", "").fillna("").astype(str).map(strip_accents).str.lower()
        df = df[area.str.contains(needle, na=False)].copy()
        used_filter = "areaDesc"
        LOGGER.info("Filtered by areaDesc contains '%s' -> rows=%s", needle, len(df))

    # ----------------------------
    # Parse onset_dt and date-filter
    # ----------------------------
    if "onset_dt" in df.columns:
        df["onset_dt"] = pd.to_datetime(df["onset_dt"], errors="coerce", utc=True)
    else:
        df["onset_dt"] = pd.to_datetime(df.get("onset"), errors="coerce", utc=True)

    df = df.dropna(subset=["onset_dt"]).copy()

    if args.start:
        start_dt = pd.to_datetime(args.start, errors="coerce", utc=True)
        df = df[df["onset_dt"] >= start_dt]
    if args.end:
        end_dt = pd.to_datetime(args.end, errors="coerce", utc=True) + pd.Timedelta(hours=23, minutes=59, seconds=59)
        df = df[df["onset_dt"] <= end_dt]

    LOGGER.info("After date filter rows=%s", len(df))

    # ----------------------------
    # Derive phenomenon flags + level_score (REQUIRED for weekly aggregation)
    # ----------------------------
    event_s = df.get("event", "").fillna("").astype(str)
    head_s  = df.get("headline", "").fillna("").astype(str)

    DUST_RE = r"(?:dust|polvo)"
    HEAT_RE = r"(?:temper|high-temperature|calor)"

    df["is_dust_event"] = (
        event_s.str.contains(DUST_RE, case=False, na=False)
        | head_s.str.contains(DUST_RE, case=False, na=False)
    ).astype(int)

    df["is_heat_event"] = (
        event_s.str.contains(HEAT_RE, case=False, na=False)
        | head_s.str.contains(HEAT_RE, case=False, na=False)
    ).astype(int)

    SEV_MAP = {"minor": 1, "moderate": 2, "severe": 3, "extreme": 4}
    sev = df.get("severity", "").fillna("").astype(str).str.strip().str.lower()
    df["level_score"] = sev.map(SEV_MAP).fillna(0).astype(int)

    LOGGER.info(
        "Derived cols: dust_rows=%s heat_rows=%s severity_nonzero=%s",
        int(df["is_dust_event"].sum()),
        int(df["is_heat_event"].sum()),
        int((df["level_score"] > 0).sum()),
    )

    # ----------------------------
    # Output paths
    # ----------------------------
    out_dir = processed_cap_dir(project_root, island)

    # Save alerts (optional)
    if args.save_alerts:
        if args.out_alerts:
            out_alerts_fp = Path(args.out_alerts)
            if not out_alerts_fp.is_absolute():
                out_alerts_fp = (project_root / out_alerts_fp).resolve()
            ensure_dir(out_alerts_fp.parent)
        else:
            tag = f"{args.start or 'start'}_{args.end or 'end'}"
            out_alerts_fp = out_dir / f"cap_alerts_{code}_{tag}.parquet"

        df.to_parquet(out_alerts_fp, index=False)
        LOGGER.info("Saved alerts -> %s (rows=%s cols=%s)", out_alerts_fp, len(df), df.shape[1])

    # ----------------------------
    # Build weekly
    # ----------------------------
    weekly = build_cap_weekly(df)

    # Force tz-naive week_start for downstream merges
    if "week_start" in weekly.columns:
        ws = pd.to_datetime(weekly["week_start"], errors="coerce")
        # If tz-aware, drop tz; if already naive, this is a no-op.
        try:
            ws = ws.dt.tz_convert(None)
        except Exception:
            pass
        weekly["week_start"] = ws

    # Calendarize weekly to full range if start/end provided
    if args.start and args.end and not weekly.empty:
        min_w = weekly["week_start"].min()
        max_w = weekly["week_start"].max()
        full_weeks = pd.date_range(min_w, max_w, freq="W-MON")
        spine = pd.DataFrame({"week_start": full_weeks})
        weekly = spine.merge(weekly, on="week_start", how="left").sort_values("week_start").reset_index(drop=True)

        for c in [
            "cap_heat_level_max_week", "cap_heat_yellow_plus_week",
            "cap_dust_level_max_week", "cap_dust_yellow_plus_week"
        ]:
            if c in weekly.columns:
                weekly[c] = weekly[c].fillna(0).astype(int)
        if "cap_coverage_week" in weekly.columns:
            weekly["cap_coverage_week"] = weekly["cap_coverage_week"].fillna(0.0)

    # Output weekly
    if args.out_weekly:
        out_weekly_fp = Path(args.out_weekly)
        if not out_weekly_fp.is_absolute():
            out_weekly_fp = (project_root / out_weekly_fp).resolve()
        ensure_dir(out_weekly_fp.parent)
    else:
        sy = (args.start[:4] if args.start else "start")
        ey = (args.end[:4] if args.end else "end")
        out_weekly_fp = out_dir / f"cap_weekly_{code}_{sy}_{ey}.parquet"

    weekly.to_parquet(out_weekly_fp, index=False)
    LOGGER.info("Saved weekly -> %s (rows=%s cols=%s)", out_weekly_fp, len(weekly), weekly.shape[1])

    # QC
    if not weekly.empty:
        LOGGER.info("Weekly min/max week_start: %s .. %s", weekly["week_start"].min(), weekly["week_start"].max())
        LOGGER.info("Weekly dup weeks: %s", int(weekly.duplicated(["week_start"]).sum()))
        LOGGER.info("Weekly coverage<1: %s", int((weekly.get("cap_coverage_week", 0) < 1.0).sum()))

    LOGGER.info("DONE")


if __name__ == "__main__":
    main()