"""
build_pollutants_2000_2025.py
──────────────────────────────────────────────────────────────────────────────
Pipeline unificado de contaminantes para Canarias, 2000–2025.

Fuentes:
  • 2000–2012  →  EEA Historical parquets  (data/raw/historical/*.parquet)
  • 2013–2025  →  Excel Gobierno de Canarias  (C:/data/Air_Quallity_Canary/YYYY_stations.xlsx)

Salida por isla:
  data/processed/<island>/air_quality/weekly_<code>_2000_2025.parquet

Columnas de salida:
  week_start, year, PM10, PM2.5, SO2, NO2, O3,
  days_with_pm10, days_missing_pm10, source

Uso:
  # Una isla
  python build_pollutants_2000_2025.py --island tfe

  # Todas las islas
  python build_pollutants_2000_2025.py --all

  # Rango personalizado
  python build_pollutants_2000_2025.py --island gcan --start-year 2005 --end-year 2020

Requiere:
  pip install pandas pyarrow openpyxl tqdm
"""

from __future__ import annotations

import argparse
import re
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from tqdm import tqdm

# ── Rutas por defecto ──────────────────────────────────────────────────────────
DEFAULT_EEA_DIR   = Path(r"C:\Users\fdora\RA_Career\Projects\climate_mortality\data\raw\historical")
DEFAULT_EXCEL_DIR = Path(r"C:\data\Air_Quallity_Canary")
DEFAULT_OUT_DIR   = Path(r"C:\Users\fdora\RA_Career\Projects\climate_mortality\data\processed")

EEA_START  = 2000
EEA_END    = 2012
EXCEL_START = 2013
EXCEL_END   = 2025

POLLUTANTS = ["PM10", "PM2.5", "SO2", "NO2", "O3"]

# ── Estaciones EEA por isla (provincia 35 = Las Palmas, 38 = SC Tenerife) ─────
# Prefijo de sampling point ID → isla
EEA_STATIONS_BY_ISLAND: Dict[str, List[str]] = {
    "tfe": [
        "SP_38038010", "SP_38038012", "SP_38038021", "SP_38038022",
        "SP_38038023", "SP_38038024", "SP_38038025", "SP_38038026",
        "SP_38038027", "SP_38004001", "SP_38005001", "SP_38006001",
        "SP_38006002", "SP_38006003", "SP_38008001", "SP_38009001",
        "SP_38011004", "SP_38011005", "SP_38011006", "SP_38011007",
        "SP_38017001", "SP_38017002", "SP_38017003", "SP_38031001",
        "SP_38033001", "SP_38036001", "SP_38036002", "SP_38037001",
        "SP_38048001",
    ],
    "gcan": [
        "SP_35016010", "SP_35016011", "SP_35016012", "SP_35016013",
        "SP_35019001", "SP_35019002", "SP_35019004", "SP_35002001",
        "SP_35002002", "SP_35006001", "SP_35022002", "SP_35024001",
        "SP_35026001", "SP_35026003", "SP_35026004",
    ],
    "lzt": [
        "SP_35004001", "SP_35004002", "SP_35024002",
    ],
    "ftv": [
        "SP_35017001", "SP_35017002", "SP_35017003", "SP_35017004", "SP_35017005",
    ],
    "lpa": [
        "SP_38009001", "SP_38008001", "SP_38037001", "SP_38033001",
    ],
    "gom": [
        "SP_38036001", "SP_38036002",
    ],
    "hie": [
        "SP_38048001",
    ],
}

# ── Estaciones Excel (Gobierno de Canarias) por isla ──────────────────────────
STATIONS_BY_ISLAND: Dict[str, List[str]] = {
    "tfe": [
        "Casa cuna",
        "Vuelta Los Pájaros-Sta Cruz TF",
        "Depósito de Tristán-Sta Cruz TF",
        "García Escámez-Sta Cruz TF",
        "Parque La Granja-Sta Cruz TF",
        "Tome Cano",
        "La Hidalgo-Arafo",
        "Balsa de Zamora-Los Realejos",
        "Tena Artigas-Sta Cruz de TF",
        "Piscina Municipal-Sta Cruz TF",
        "Tio Pino-Sta Cruz de TF",
        "Barranco Hondo",
        "Caletillas",
        "Igueste",
        "Depósito La Guancha-Candelaria",
        "El Río",
        "Galletas",
        "Granadilla",
        "Médano",
        "San Isidro",
        "Tajao",
    ],
    "gcan": [
        "Mercado Central",
        "Parque San Juan-Telde",
        "Polideportivo Afonso-Arucas",
        "Agüimes",
        "Castillo del Romeral",
        "San Agustín",
        "Jinamar 3",
        "Pedro Lezcano",
        "La Loma-Telde",
        "San Nicolás",
        "Nestor Alamo",
        "ITC",
        "Observatorio Temisas",
    ],
    "lzt": [
        "Ciudad Deportiva-Arrecife",
        "Centro de Arte",
        "Arrecife",
        "Costa Teguise",
        "Las Caletas-Teguise",
    ],
    "ftv": [
        "Casa Palacio-Pto del Rosario",
        "Tefía-Pto del Rosario",
        "El Charco-Pto del Rosario",
    ],
    "lpa": [
        "San Antonio-Breña Baja",
        "El Pilar-Sta Cruz de La Palma",
        "La Grama-Breña Alta",
        "Las Balsas-S.Andrés y Sauces",
        "El Paso",
        "Las Manchas",
        "Hacienda",
    ],
    "gom": [
        "Residencia Escolar-La Gomera",
        "Las Galanas-SS Gomera",
        "Centro de Visitantes-SS Gomera",
    ],
    "hie": [
        "Echedo-Valverde",
    ],
}

ISLAND_SHEET_ALIASES: Dict[str, List[str]] = {
    "tfe":  ["tenerife", "tfe"],
    "gcan": ["gran_canaria", "gran canaria", "gcan"],
    "lzt":  ["lanzarote", "lzt"],
    "ftv":  ["fuerteventura", "ftv"],
    "lpa":  ["la_palma", "la palma", "lpa"],
    "gom":  ["gomera", "la_gomera", "la gomera", "gom"],
    "hie":  ["hierro", "el_hierro", "el hierro", "hie"],
}

ALL_ISLANDS = list(STATIONS_BY_ISLAND.keys())


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def strip_accents(text: str) -> str:
    text = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in text if not unicodedata.combining(ch))


def normalize_name(name: str) -> str:
    if name is None:
        return ""
    text = str(name).strip().lower()
    text = strip_accents(text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def to_week_start(dates: pd.Series) -> pd.Series:
    """Return Monday of ISO week for each date."""
    return dates.dt.to_period("W-MON").apply(lambda p: p.start_time)


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE A — EEA Historical parquets (2000–2012)
# ══════════════════════════════════════════════════════════════════════════════
def _eea_parquet_prefix(path: Path) -> str:
    """Extract SP_XXXXXXXX prefix from parquet filename."""
    m = re.match(r"(SP_\d+)", path.stem)
    return m.group(1) if m else ""


def load_eea_island(eea_dir: Path, island: str,
                    start_year: int, end_year: int) -> pd.DataFrame:
    """
    Load and aggregate EEA Historical parquets for one island.
    Returns daily DataFrame: date, PM10, PM2.5, SO2, NO2, O3, source='eea'
    """
    prefixes = set(EEA_STATIONS_BY_ISLAND.get(island, []))
    if not prefixes:
        print(f"  [EEA] No station prefixes defined for island={island}")
        return pd.DataFrame()

    parquets = [p for p in eea_dir.glob("*.parquet")
                if _eea_parquet_prefix(p) in prefixes]

    if not parquets:
        print(f"  [EEA] No parquet files found for island={island} in {eea_dir}")
        return pd.DataFrame()

    print(f"  [EEA] island={island} — {len(parquets)} parquet files")

    frames = []
    for pq in parquets:
        try:
            df = pd.read_parquet(pq)
            frames.append(df)
        except Exception as e:
            print(f"    WARNING: could not read {pq.name}: {e}")

    if not frames:
        return pd.DataFrame()

    raw = pd.concat(frames, ignore_index=True)

    # ── Normalise column names ────────────────────────────────────────────────
    raw.columns = [str(c).strip() for c in raw.columns]
    print(f"    EEA columns: {list(raw.columns)}")

    # Common EEA parquet columns: Start, End, Value, unit, ...
    # Date column may be 'Start', 'DatetimeBegin', 'date', etc.
    date_col = next(
        (c for c in raw.columns if c.lower() in ("start", "datetimebegin", "date", "datetime")),
        None
    )
    val_col = next(
        (c for c in raw.columns if c.lower() in ("value", "concentration", "val")),
        None
    )
    poll_col = next(
        (c for c in raw.columns if c.lower() in ("airpollutant", "pollutant", "component", "poll")),
        None
    )

    if date_col is None or val_col is None:
        print(f"    WARNING: cannot identify date/value columns. Skipping island={island} EEA.")
        return pd.DataFrame()

    raw["_date"] = pd.to_datetime(raw[date_col], errors="coerce").dt.normalize()
    raw["_value"] = pd.to_numeric(raw[val_col], errors="coerce")

    # Filter year range
    raw = raw[raw["_date"].dt.year.between(start_year, end_year)]

    # EEA numeric pollutant codes → standard names
    # Source: https://dd.eionet.europa.eu/vocabulary/aq/pollutant
    EEA_POLL_CODES = {
        5:    "PM10",
        6001: "PM2.5",
        1:    "SO2",
        8:    "NO2",
        7:    "O3",
        38:   "NO",
    }

    if poll_col:
        raw["_poll_std"] = pd.to_numeric(raw[poll_col], errors="coerce").map(EEA_POLL_CODES)
        raw = raw[raw["_poll_std"].isin(POLLUTANTS)].copy()

        daily = (
            raw.groupby(["_date", "_poll_std"], as_index=False)["_value"]
            .mean()
            .pivot(index="_date", columns="_poll_std", values="_value")
            .reset_index()
        )
        daily.columns.name = None
        daily = daily.rename(columns={"_date": "date"})
    else:
        # Single pollutant file — infer from filename (SP_XXXXXX_10 = PM10, _9 = PM2.5)
        daily = (
            raw.groupby("_date", as_index=False)["_value"]
            .mean()
            .rename(columns={"_date": "date", "_value": "PM10"})
        )

    # Ensure all pollutant columns exist
    for col in POLLUTANTS:
        if col not in daily.columns:
            daily[col] = float("nan")

    daily["source"] = "eea_historical"
    daily["date"] = pd.to_datetime(daily["date"])
    return daily[["date"] + POLLUTANTS + ["source"]]


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE B — Excel Gobierno de Canarias (2013–2025)
# ══════════════════════════════════════════════════════════════════════════════
def parse_mixed_date(value):
    if pd.isna(value):
        return pd.NaT
    if isinstance(value, pd.Timestamp):
        return value.normalize()
    if isinstance(value, (int, float)) and value > 20000:
        try:
            return pd.to_datetime(value, unit="D", origin="1899-12-30", errors="coerce").normalize()
        except Exception:
            return pd.NaT
    text = str(value).strip()
    if not text:
        return pd.NaT
    for fmt in (None, "%d/%m/%Y", "%Y-%m-%d"):
        try:
            dt = pd.to_datetime(text, dayfirst=True, errors="coerce") if fmt is None \
                 else pd.to_datetime(text, format=fmt, errors="coerce")
            if pd.notna(dt):
                return dt.normalize()
        except Exception:
            pass
    return pd.NaT


def read_station_sheet(excel_path: Path, sheet_name: str) -> pd.DataFrame:
    df = pd.read_excel(excel_path, sheet_name=sheet_name, header=1)
    raw_cols = [str(c).strip() for c in df.columns]
    df.columns = raw_cols

    # Drop duplicated block starting at second "FECHA" column
    fecha_positions = [i for i, c in enumerate(df.columns) if c == "FECHA"]
    if len(fecha_positions) > 1:
        df = df.iloc[:, :fecha_positions[1]].copy()

    # Drop fully blank columns
    df = df.loc[:, df.notna().any(axis=0)].copy()

    rename_map = {
        "Fecha": "date", "FECHA": "date",
        "Hora": "hour",
        "SO2": "SO2", "NO2": "NO2", "NO": "NO",
        "PM10": "PM10", "PM2,5": "PM2.5", "PM2.5": "PM2.5",
        "O3": "O3",
    }
    df = df.rename(columns=rename_map)

    for col in ["date", "hour", "PM10", "PM2.5", "SO2", "NO2", "O3"]:
        if col not in df.columns:
            df[col] = pd.NA

    df = df[["date", "hour", "PM10", "PM2.5", "SO2", "NO2", "O3"]].copy()
    df["date"] = df["date"].apply(parse_mixed_date)
    for col in POLLUTANTS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df[df["date"].notna()].copy()


def match_sheets(excel_path: Path, island: str) -> List[str]:
    xls = pd.ExcelFile(excel_path)
    available = xls.sheet_names
    avail_norm = {normalize_name(s): s for s in available}

    # Try island-level sheet alias first
    for alias in ISLAND_SHEET_ALIASES.get(island, []):
        match = avail_norm.get(normalize_name(alias))
        if match:
            return [match]

    # Fall back to station-level sheets
    matched = []
    for station in STATIONS_BY_ISLAND.get(island, []):
        match = avail_norm.get(normalize_name(station))
        if match:
            matched.append(match)
    return matched


def load_excel_island_year(excel_dir: Path, island: str, year: int) -> pd.DataFrame:
    excel_path = excel_dir / f"{year}_stations.xlsx"
    if not excel_path.exists():
        print(f"  [Excel] File not found: {excel_path.name}")
        return pd.DataFrame()

    sheets = match_sheets(excel_path, island)
    if not sheets:
        print(f"  [Excel] No sheets matched for island={island}, year={year}")
        return pd.DataFrame()

    station_frames = []
    for sheet in sheets:
        try:
            df = read_station_sheet(excel_path, sheet)
            daily = df.groupby("date", as_index=False)[POLLUTANTS].max()
            daily["pm10_ok"] = daily["PM10"].notna()
            daily["station"] = sheet
            station_frames.append(daily)
        except Exception as e:
            print(f"    WARNING: sheet '{sheet}' year={year}: {e}")

    if not station_frames:
        return pd.DataFrame()

    # Priority selection: first sheet with PM10 data for each date
    combined = pd.concat(station_frames, ignore_index=True)
    combined["priority"] = combined.groupby("station").ngroup()
    combined = (
        combined[combined["pm10_ok"]]
        .sort_values(["date", "priority"])
        .drop_duplicates(subset=["date"], keep="first")
    )
    return combined[["date"] + POLLUTANTS].copy()


def load_excel_island(excel_dir: Path, island: str,
                      start_year: int, end_year: int) -> pd.DataFrame:
    frames = []
    for year in range(start_year, end_year + 1):
        df = load_excel_island_year(excel_dir, island, year)
        if not df.empty:
            frames.append(df)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out["source"] = "gob_canarias"
    out["date"] = pd.to_datetime(out["date"])
    return out[["date"] + POLLUTANTS + ["source"]]


# ══════════════════════════════════════════════════════════════════════════════
# MERGE & AGGREGATE
# ══════════════════════════════════════════════════════════════════════════════
def build_daily_island(eea_dir: Path, excel_dir: Path,
                       island: str, start_year: int, end_year: int) -> pd.DataFrame:
    frames = []

    # EEA segment
    eea_end = min(EEA_END, end_year)
    if start_year <= eea_end:
        eea_start = max(start_year, EEA_START)
        print(f"\n[{island}] Loading EEA {eea_start}–{eea_end}...")
        df_eea = load_eea_island(eea_dir, island, eea_start, eea_end)
        if not df_eea.empty:
            frames.append(df_eea)

    # Excel segment
    excel_start = max(start_year, EXCEL_START)
    if excel_start <= end_year:
        print(f"\n[{island}] Loading Excel {excel_start}–{end_year}...")
        df_excel = load_excel_island(excel_dir, island, excel_start, end_year)
        if not df_excel.empty:
            frames.append(df_excel)

    if not frames:
        print(f"  WARNING: no data found for island={island}")
        return pd.DataFrame()

    daily = pd.concat(frames, ignore_index=True).sort_values("date").reset_index(drop=True)

    # Drop duplicates keeping eea first (already sorted by date; eea comes first)
    daily = daily.drop_duplicates(subset=["date"], keep="first")
    return daily


def aggregate_weekly(daily: pd.DataFrame) -> pd.DataFrame:
    daily = daily.copy()
    daily["week_start"] = to_week_start(daily["date"])

    weekly = (
        daily.groupby("week_start", as_index=False)
        .agg(
            PM10          =("PM10",   "mean"),
            **{"PM2.5":   ("PM2.5",  "mean")},
            SO2           =("SO2",    "mean"),
            NO2           =("NO2",    "mean"),
            O3            =("O3",     "mean"),
            days_with_pm10   =("PM10", lambda s: int(s.notna().sum())),
            days_missing_pm10=("PM10", lambda s: int(s.isna().sum())),
        )
        .sort_values("week_start")
        .reset_index(drop=True)
    )
    weekly["year"] = weekly["week_start"].dt.year
    return weekly[["week_start", "year", "PM10", "PM2.5", "SO2", "NO2", "O3",
                   "days_with_pm10", "days_missing_pm10"]]


def build_and_save(island: str, eea_dir: Path, excel_dir: Path,
                   out_dir: Path, start_year: int, end_year: int) -> Optional[Path]:
    print(f"\n{'='*60}")
    print(f"Island: {island} | {start_year}–{end_year}")
    print(f"{'='*60}")

    daily = build_daily_island(eea_dir, excel_dir, island, start_year, end_year)
    if daily.empty:
        print(f"  SKIP: no data for {island}")
        return None

    weekly = aggregate_weekly(daily)

    outpath = out_dir / island / "air_quality"
    outpath.mkdir(parents=True, exist_ok=True)
    fname = outpath / f"weekly_{island}_{start_year}_{end_year}.parquet"
    weekly.to_parquet(fname, index=False)

    print(f"\n  ✅ Saved: {fname}")
    print(f"  Rows: {len(weekly):,}")
    print(f"  Range: {weekly['week_start'].min()} → {weekly['week_start'].max()}")
    print(f"  PM10 nulls: {weekly['PM10'].isna().sum()} / {len(weekly)}")
    return fname


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build unified weekly pollutants dataset 2000–2025 for Canary Islands."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--island", choices=ALL_ISLANDS,
                       help="Single island code, e.g. tfe, gcan")
    group.add_argument("--all", action="store_true",
                       help="Process all islands")

    parser.add_argument("--start-year", type=int, default=2000)
    parser.add_argument("--end-year",   type=int, default=2025)
    parser.add_argument("--eea-dir",    default=str(DEFAULT_EEA_DIR))
    parser.add_argument("--excel-dir",  default=str(DEFAULT_EXCEL_DIR))
    parser.add_argument("--out-dir",    default=str(DEFAULT_OUT_DIR))
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    eea_dir   = Path(args.eea_dir)
    excel_dir = Path(args.excel_dir)
    out_dir   = Path(args.out_dir)

    islands = ALL_ISLANDS if args.all else [args.island]

    results = []
    for island in tqdm(islands, desc="Islands"):
        path = build_and_save(
            island=island,
            eea_dir=eea_dir,
            excel_dir=excel_dir,
            out_dir=out_dir,
            start_year=args.start_year,
            end_year=args.end_year,
        )
        results.append((island, path))

    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    for island, path in results:
        status = f"✅ {path}" if path else "❌ no data"
        print(f"  {island:8s} {status}")


if __name__ == "__main__":
    main()