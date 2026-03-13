# Construye un dataset diario insular de calidad del aire a partir de los Excel anuales
# de Canarias, recorriendo una lista priorizada de estaciones por isla.
#
# Lógica:
# 1) Para cada año, abre el Excel "Datos YYYY.xlsx".
# 2) Busca las hojas/estaciones asociadas a la isla según un diccionario de prioridad.
# 3) Lee cada hoja válida, limpia fechas y contaminantes principales:
#       PM10, PM2.5, SO2, NO2, O3
# 4) Resume cada estación a nivel diario usando el máximo diario por contaminante.
# 5) Para cada fecha, elige la primera estación de la lista que tenga PM10 disponible.
#    Si una estación no tiene datos ese día, pasa a la siguiente.
# 6) Devuelve una serie diaria insular con una sola fila por fecha y la estación elegida.
#
# Salida:
# - daily_<codigo_isla>.csv
#
# Nota:
# - La prioridad de estaciones está definida en STATIONS_BY_ISLAND.
# - La estación usada puede cambiar de un día a otro si la prioritaria no tiene datos.

from __future__ import annotations

import argparse
import re
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd


# -----------------------------------------------------------------------------
# Station dictionaries by island
# Keep the order: it defines priority when selecting a station for a given day.
# Output station names preserve the raw Excel sheet name.
# -----------------------------------------------------------------------------
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


POLLUTANTS = ["PM10", "PM2.5", "SO2", "NO2", "O3"]
OUTPUT_COLUMNS = ["date", "year", "PM10", "PM2.5", "SO2", "NO2", "O3", "station"]
RAW_RENAME_MAP = {
    "Fecha": "date",
    "Hora": "hour",
    "SO2": "SO2",
    "NO2": "NO2",
    "PM10": "PM10",
    "PM2,5": "PM2.5",
    "PM2.5": "PM2.5",
    "O3": "O3",
}


# -----------------------------------------------------------------------------
# Normalization helpers
# -----------------------------------------------------------------------------

def strip_accents(text: str) -> str:
    text = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in text if not unicodedata.combining(ch))


def normalize_station_name(name: str) -> str:
    if name is None:
        return ""
    text = str(name).strip().lower()
    text = strip_accents(text)
    text = text.replace("sta.", "sta").replace("sta ", "sta ")
    text = text.replace("s.", "s").replace("pto.", "pto").replace("pto ", "pto ")
    text = text.replace("s.andres", "san andres")
    text = text.replace("ss gomera", "san sebastian gomera")
    text = text.replace("la hidalgo", "la hidalga")
    text = text.replace("parque la granja", "parque de la granja")
    text = text.replace("tio pino", "tío pino")
    text = text.replace("nestor", "néstor")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


# -----------------------------------------------------------------------------
# Excel parsing
# -----------------------------------------------------------------------------

def build_year_file(root: Path, year: int) -> Path:
    return root / f"Datos{year}" / f"Datos {year}.xlsx"


def get_calendar_for_year(year: int) -> pd.DataFrame:
    dates = pd.date_range(start=f"{year}-01-01", end=f"{year}-12-31", freq="D")
    out = pd.DataFrame({"date": dates})
    out["year"] = year
    return out

def parse_mixed_excel_date(value):
    """
    Parse dates coming from messy Excel sheets:
    - Excel serial numbers
    - pandas Timestamp
    - dd/mm/yyyy strings
    - yyyy-mm-dd strings
    - datetimes with time
    Returns pandas.Timestamp normalized to midnight or NaT.
    """
    if pd.isna(value):
        return pd.NaT

    # Already datetime-like
    if isinstance(value, pd.Timestamp):
        return value.normalize()

    # Excel serial date as int/float
    if isinstance(value, (int, float)):
        # Avoid interpreting tiny numbers / junk as dates
        if value > 20000:
            try:
                return pd.to_datetime(value, unit="D", origin="1899-12-30", errors="coerce").normalize()
            except Exception:
                return pd.NaT

    text = str(value).strip()
    if not text:
        return pd.NaT

    # Try day-first first (common in Spanish Excel files)
    dt = pd.to_datetime(text, errors="coerce", dayfirst=True)
    if pd.notna(dt):
        return dt.normalize()

    # Fallback
    dt = pd.to_datetime(text, errors="coerce")
    if pd.notna(dt):
        return dt.normalize()

    return pd.NaT


def parse_date_series(series: pd.Series) -> pd.Series:
    return series.apply(parse_mixed_excel_date)


def read_station_sheet(excel_path: Path, sheet_name: str) -> pd.DataFrame:
    """
    Workbook structure:
    - row 0: station name only
    - row 1: real headers
    - then data rows
    - later, a second duplicated block starts at a column named 'FECHA'
      that must be ignored completely.
    """
    df = pd.read_excel(excel_path, sheet_name=sheet_name, header=1)

    # Normalize raw column labels to strings
    raw_cols = [str(c).strip() for c in df.columns]
    df.columns = raw_cols

    # Keep only the FIRST block of columns, i.e. everything before the second block
    # that starts with a column named 'FECHA'
    second_block_idx = None
    for i, col in enumerate(df.columns):
        if col == "FECHA":
            second_block_idx = i
            break

    if second_block_idx is not None:
        df = df.iloc[:, :second_block_idx].copy()

    # Drop completely unnamed/blank columns if present
    keep_nonblank = [c for c in df.columns if c and not c.lower().startswith("unnamed")]
    df = df[keep_nonblank].copy()

    # Rename only the columns we care about from the FIRST block
    rename_map = {
        "Fecha": "date",
        "Hora": "hour",
        "SO2": "SO2",
        "NO": "NO",
        "NO2": "NO2",
        "PM10": "PM10",
        "PM2,5": "PM2.5",
        "PM2.5": "PM2.5",
        "O3": "O3",
    }

    df = df.rename(columns=rename_map)

    # Ensure expected columns exist even if absent in a given sheet
    expected_cols = ["date", "hour", "PM10", "PM2.5", "SO2", "NO2", "O3"]
    for col in expected_cols:
        if col not in df.columns:
            df[col] = pd.NA

    # Keep only the columns needed downstream
    df = df[expected_cols].copy()

    # Parse dates
    df["date"] = parse_date_series(df["date"])
    bad_dates = df["date"].isna().sum()
    if bad_dates:
        print(f"WARNING: {sheet_name} -> bad date rows: {bad_dates} / {len(df)}")

    # Parse numeric pollutant columns
    for col in ["PM10", "PM2.5", "SO2", "NO2", "O3"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop rows with no valid date
    df = df[df["date"].notna()].copy()

    return df

def summarize_station_daily(df_station: pd.DataFrame, station_raw_name: str) -> pd.DataFrame:
    """
    Daily max for each pollutant.
    The station is considered valid for a day if PM10 has at least one non-null value.
    Other pollutants may remain NaN.
    """
    agg_map = {col: "max" for col in POLLUTANTS}
    daily = df_station.groupby("date", as_index=False).agg(agg_map)

    pm10_valid = df_station.groupby("date")["PM10"].apply(lambda s: s.notna().any()).reset_index(name="pm10_has_data")
    daily = daily.merge(pm10_valid, on="date", how="left")
    daily["station"] = station_raw_name
    return daily


def match_island_sheets(excel_path: Path, island_code: str) -> List[str]:
    station_priority = STATIONS_BY_ISLAND[island_code]
    wanted_norm = {normalize_station_name(name): name for name in station_priority}

    xls = pd.ExcelFile(excel_path)
    available_sheets = xls.sheet_names
    available_norm = {normalize_station_name(sheet): sheet for sheet in available_sheets}

    matched_sheets: List[str] = []
    missing: List[str] = []

    for raw_station in station_priority:
        station_norm = normalize_station_name(raw_station)
        match = available_norm.get(station_norm)
        if match is not None:
            matched_sheets.append(match)
        else:
            missing.append(raw_station)

    print(f"\n[{excel_path.name}] island={island_code} matched_sheets={len(matched_sheets)} missing={len(missing)}")
    if missing:
        print("  Missing stations:")
        for st in missing:
            print(f"    - {st}")

    return matched_sheets


# -----------------------------------------------------------------------------
# Year build logic
# -----------------------------------------------------------------------------

def build_island_year_daily(root: Path, island_code: str, year: int) -> pd.DataFrame:
    excel_path = build_year_file(root, year)
    if not excel_path.exists():
        raise FileNotFoundError(f"Year file not found: {excel_path}")

    matched_sheets = match_island_sheets(excel_path, island_code)
    if not matched_sheets:
        print(f"No sheets matched for island={island_code}, year={year}. Returning empty calendar.")
        calendar = get_calendar_for_year(year)
        for col in ["PM10", "PM2.5", "SO2", "NO2", "O3", "station"]:
            calendar[col] = pd.NA
        return calendar[OUTPUT_COLUMNS]

    station_daily_frames: List[pd.DataFrame] = []

    for sheet_name in matched_sheets:
        try:
            station_df = read_station_sheet(excel_path, sheet_name)
            station_daily = summarize_station_daily(station_df, station_raw_name=sheet_name)
            station_daily_frames.append(station_daily)
        except Exception as exc:
            print(f"WARNING: failed reading sheet '{sheet_name}' in {excel_path.name}: {exc}")

    calendar = get_calendar_for_year(year)

    if not station_daily_frames:
        print(f"No usable station data for island={island_code}, year={year}. Returning empty calendar.")
        for col in ["PM10", "PM2.5", "SO2", "NO2", "O3", "station"]:
            calendar[col] = pd.NA
        return calendar[OUTPUT_COLUMNS]

    # Combine in priority order.
    # For each day, first station with pm10_has_data=True wins.
    station_choice_rows: List[pd.DataFrame] = []
    for priority, daily in enumerate(station_daily_frames):
        tmp = daily.copy()
        tmp["priority"] = priority
        station_choice_rows.append(tmp)

    combined = pd.concat(station_choice_rows, ignore_index=True)
    combined = combined[combined["pm10_has_data"] == True].copy()  # noqa: E712
    combined = combined.sort_values(["date", "priority"]).drop_duplicates(subset=["date"], keep="first")

    out = calendar.merge(
        combined[["date", "PM10", "PM2.5", "SO2", "NO2", "O3", "station"]],
        on="date",
        how="left",
    )

    out = out[OUTPUT_COLUMNS].sort_values("date").reset_index(drop=True)
    return out


def build_island_daily(root: Path, island_code: str, start_year: int, end_year: int) -> pd.DataFrame:
    if island_code not in STATIONS_BY_ISLAND:
        valid = ", ".join(STATIONS_BY_ISLAND)
        raise ValueError(f"Invalid island '{island_code}'. Valid options: {valid}")
    if start_year > end_year:
        raise ValueError("start_year cannot be greater than end_year")

    frames: List[pd.DataFrame] = []
    for year in range(start_year, end_year + 1):
        print(f"\nBuilding island={island_code}, year={year} ...")
        df_year = build_island_year_daily(root=root, island_code=island_code, year=year)
        frames.append(df_year)

    out = pd.concat(frames, ignore_index=True)
    out = out.sort_values("date").reset_index(drop=True)
    return out


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build daily air quality dataset for a Canary Island from yearly Excel books.")
    parser.add_argument("--island", required=True, choices=sorted(STATIONS_BY_ISLAND.keys()), help="Island code, e.g. tfe, gcan, lzt")
    parser.add_argument("--start-year", required=True, type=int, help="First year inclusive")
    parser.add_argument("--end-year", required=True, type=int, help="Last year inclusive")
    parser.add_argument(
        "--root",
        default=r"C:\data\Air_Polution_GC_2015_2025_raw\Datos2016_2025",
        help="Root folder containing DatosYYYY/Datos YYYY.xlsx",
    )
    parser.add_argument(
        "--outdir",
        default="data/interim/air_q",
        help="Output directory for the final CSV",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    root = Path(args.root)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    df = build_island_daily(
        root=root,
        island_code=args.island,
        start_year=args.start_year,
        end_year=args.end_year,
    )

    outpath = outdir / f"daily_{args.island}.csv"
    df.to_csv(outpath, index=False)

    print("\nDone.")
    print(f"Output: {outpath}")
    print(f"Rows: {len(df):,}")
    print(f"Date range: {df['date'].min()} -> {df['date'].max()}")
    print("Non-null counts:")
    print(df[["PM10", "PM2.5", "SO2", "NO2", "O3", "station"]].notna().sum())


if __name__ == "__main__":
    main()
