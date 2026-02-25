# b_src/ingest/air_quality_canarias_station_excels_to_pm.py
"""
Ingest Canary Islands air quality Excel files (Datos YYYY.xlsx), extracting ONE station (one sheet)
across multiple years, best-effort (skip missing years/sheets), and save a merged hourly dataset.

Inputs (outside repo OK):
  --in-root points to folder containing year subfolders:
    <in-root>/Datos2024/Datos 2024.xlsx
    <in-root>/Datos2023/Datos 2023.xlsx
    ...

Example:
  $env:AIR_DATA_ROOT="C:\data\Air_Polution_GC_2015_2025_raw\Datos2016_2025\Datos2016_2025"
  python b_src\ingest\air_quality_canarias_station_excels_to_pm.py `
    --in-root $env:AIR_DATA_ROOT `
    --years 2016 2024 `
    --island gran_canaria `
    --station "Mercado Central" `
    --out-format parquet --also-csv --save-by-year

Outputs:
  b_data/interim/<island>/pm_hourly_<suffix>_<stationtag>_<y0>_<y1>.parquet (or .csv)
  b_logs/<island>/air_quality_ingest_<suffix>_<stationtag>_<y0>_<y1>.log
Optional:
  b_data/interim/<island>/by_year/pm_hourly_<suffix>_<stationtag>_<YYYY>.parquet
"""

from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
B_DATA = PROJECT_ROOT / "b_data"
B_LOGS = PROJECT_ROOT / "b_logs"

# island folder/name -> suffix used in filenames
ISLAND_SUFFIX = {
    "tenerife": "tfe",
    "gran_canaria": "gc",
    "grancanaria": "gc",
    "lanzarote": "lz",
    "fuerteventura": "fvt",
    "la_palma": "lp",
    "lapalma": "lp",
    "la_gomera": "lg",
    "lagomera": "lg",
    "el_hierro": "eh",
    "elhierro": "eh",
}


def safe_slug(s: str) -> str:
    return s.strip().lower().replace(" ", "_")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ingest Canary Islands air quality Excels (Datos YYYY.xlsx) for one station")
    p.add_argument("--in-root", required=True, help="Folder containing DatosYYYY subfolders (outside repo OK)")
    p.add_argument("--years", nargs=2, type=int, required=True, metavar=("START_YEAR", "END_YEAR"))
    p.add_argument("--island", required=True, help="Island name for output folder, e.g. gran_canaria, tenerife")
    p.add_argument("--station", required=True, help="Station name (Excel sheet). Example: 'Mercado Central'")
    p.add_argument("--out-format", choices=["parquet", "csv"], default="parquet")
    p.add_argument("--also-csv", action="store_true", help="If out-format=parquet, also write CSV copy")
    p.add_argument("--save-by-year", action="store_true", help="Also save one output file per year (best-effort)")
    return p.parse_args()


def log(msg: str, log_file: Path) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    ts = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def parse_comma_decimal(x):
    """Convert '18,8' -> 18.8 ; returns <NA> if blank/invalid."""
    if pd.isna(x):
        return pd.NA
    s = str(x).strip().replace('"', "")
    if s == "" or s.lower() in {"nan", "none"}:
        return pd.NA
    s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return pd.NA


def find_excel_for_year(in_root: Path, year: int) -> Path:
    """
    Expected structure:
      <in_root>/Datos2024/Datos 2024.xlsx
    Handles minor naming variations.
    """
    year_dir = in_root / f"Datos{year}"
    if not year_dir.exists():
        raise FileNotFoundError(f"Missing folder: {year_dir}")

    candidates = [
        year_dir / f"Datos {year}.xlsx",
        year_dir / f"Datos {year}.xls",
        year_dir / f"Datos{year}.xlsx",
        year_dir / f"Datos{year}.xls",
    ]
    for c in candidates:
        if c.exists():
            return c

    # fallback: any excel containing the year in name
    for c in year_dir.glob("*.xls*"):
        if str(year) in c.name:
            return c

    raise FileNotFoundError(f"No Excel found in {year_dir}")


def normalize_sheet_name(name: str) -> str:
    """Excel sheets sometimes have trailing spaces (e.g., 'Mercado Central ')."""
    return str(name).strip()


def resolve_sheet_name(xls: pd.ExcelFile, desired: str) -> str | None:
    """
    Try to find the desired station sheet name in this workbook.
    Matches after .strip() to handle trailing spaces.
    Returns the actual sheet name if found, else None.
    """
    desired_norm = normalize_sheet_name(desired)
    for s in xls.sheet_names:
        if normalize_sheet_name(s) == desired_norm:
            return s  # return actual sheet name in file
    return None


def load_station_year(fp_xlsx: Path, sheet_actual: str) -> pd.DataFrame:
    """
    Reads one station sheet for one year.
    Keeps the hourly block only. Some years include CO mg/m3, others don't.
    We do NOT rely on CO mg/m3. Instead:
      - read with header=1
      - cut the dataframe at the first 'FECHA' (uppercase) column if present (start of right-side summary block)
      - require Fecha + Hora
    """
    df = pd.read_excel(fp_xlsx, sheet_name=sheet_actual, header=1)
    df.columns = [str(c).strip() for c in df.columns]
    df = df.dropna(how="all")

    # Cut off the right-side summary block if present (usually starts with 'FECHA')
    if "FECHA" in df.columns:
        df = df.iloc[:, : df.columns.get_loc("FECHA")].copy()

    # Rename variants
    rename_map = {
        "PM2,5": "PM2_5",
        "PM2.5": "PM2_5",
        "CO mg/m3": "CO_mg_m3",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
    df.columns = [c.strip() for c in df.columns]

    if "Fecha" not in df.columns or "Hora" not in df.columns:
        raise ValueError("Expected 'Fecha' and 'Hora' columns not found in hourly block")

    # Fecha parsing
    df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")
    df = df[df["Fecha"].notna()].copy()

    # Hora: usually 1..24 -> 0..23
    hour = pd.to_numeric(df["Hora"], errors="coerce").astype("float")
    hour_adj = hour.where(~hour.between(1, 24), hour - 1)
    hour_adj = hour_adj.where(hour_adj != 24, 23)
    df["datetime"] = df["Fecha"] + pd.to_timedelta(hour_adj, unit="h")

    # Parse numeric columns if present (PM is the core; others optional)
    numeric_cols = ["PM10", "PM2_5", "SO2", "NO", "NO2", "O3", "CO_mg_m3", "Benceno", "Tolueno", "Xileno"]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = df[c].map(parse_comma_decimal)

    return df


def write_df(df: pd.DataFrame, path: Path, fmt: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fmt == "parquet":
        df.to_parquet(path, index=False)
    else:
        df.to_csv(path, index=False, encoding="utf-8")


def main():
    args = parse_args()

    in_root = Path(args.in_root)
    y0, y1 = args.years
    if y1 < y0:
        raise ValueError("--years END_YEAR must be >= START_YEAR")

    island_slug = safe_slug(args.island)
    suffix = ISLAND_SUFFIX.get(island_slug)
    if not suffix:
        raise ValueError(f"island '{island_slug}' not in ISLAND_SUFFIX map. Add it.")

    station_user = args.station.strip()
    station_tag = safe_slug(station_user)

    out_dir = B_DATA / "interim" / island_slug
    by_year_dir = out_dir / "by_year"
    log_file = (B_LOGS / island_slug) / f"air_quality_ingest_{suffix}_{station_tag}_{y0}_{y1}.log"

    log(f"START island={island_slug} suffix={suffix} station='{station_user}' years={y0}..{y1}", log_file)
    log(f"in_root={in_root}", log_file)

    parts = []
    years_loaded = 0
    years_missing_file = 0
    years_missing_sheet = 0
    years_failed_parse = 0

    for year in range(y0, y1 + 1):
        try:
            fp = find_excel_for_year(in_root, year)
        except Exception as e:
            years_missing_file += 1
            log(f"[WARN] year={year} missing excel: {e}", log_file)
            continue

        try:
            xls = pd.ExcelFile(fp)
            sheet_actual = resolve_sheet_name(xls, station_user)
            if not sheet_actual:
                years_missing_sheet += 1
                log(f"[WARN] year={year} station sheet not found in {fp.name}. Skipping.", log_file)
                continue

            df = load_station_year(fp, sheet_actual)
            df["year"] = year
            df["station"] = normalize_sheet_name(sheet_actual)  # consistent
            df["island"] = island_slug
            df["suffix"] = suffix

            # filter obviously broken timestamps
            df = df[df["datetime"].notna()].copy()

            if args.save_by_year:
                year_base = f"pm_hourly_{suffix}_{station_tag}_{year}"
                year_out = by_year_dir / f"{year_base}.{ 'parquet' if args.out_format=='parquet' else 'csv'}"
                write_df(df, year_out, args.out_format)
                if args.also_csv and args.out_format == "parquet":
                    df.to_csv(by_year_dir / f"{year_base}.csv", index=False, encoding="utf-8")

            parts.append(df)
            years_loaded += 1
            log(f"[OK] year={year} rows={len(df):,} file={fp.name} sheet='{sheet_actual}'", log_file)

        except Exception as e:
            years_failed_parse += 1
            log(f"[WARN] year={year} failed parse for file={fp.name}: {e}", log_file)
            continue

    if not parts:
        raise RuntimeError(
            "No data loaded at all. Check:\n"
            f"- station sheet name spelling: '{station_user}'\n"
            f"- in_root structure contains DatosYYYY/Datos YYYY.xlsx\n"
            f"- years range {y0}..{y1}\n"
        )

    pm = pd.concat(parts, ignore_index=True)

    # de-dup: same datetime duplicates sometimes happen
    # Keep first occurrence.
    before = len(pm)
    pm = pm.sort_values("datetime").drop_duplicates(subset=["datetime"], keep="first").reset_index(drop=True)
    dups_dropped = before - len(pm)

    # Output merged dataset
    base = f"pm_hourly_{suffix}_{station_tag}_{y0}_{y1}"
    out_main = out_dir / f"{base}.{ 'parquet' if args.out_format=='parquet' else 'csv'}"
    out_csv = out_dir / f"{base}.csv"

    write_df(pm, out_main, args.out_format)
    if args.also_csv and args.out_format == "parquet":
        pm.to_csv(out_csv, index=False, encoding="utf-8")

    summary = (
        f"SUMMARY\n"
        f"loaded_years: {years_loaded}\n"
        f"missing_excel_years: {years_missing_file}\n"
        f"missing_sheet_years: {years_missing_sheet}\n"
        f"failed_parse_years: {years_failed_parse}\n"
        f"rows_merged: {pm.shape[0]} cols: {pm.shape[1]}\n"
        f"dups_dropped_on_datetime: {dups_dropped}\n"
        f"datetime min/max: {pm['datetime'].min()} / {pm['datetime'].max()}\n"
        f"saved: {out_main}\n"
    )
    log(summary, log_file)

    print("\n" + summary)
    print(f"Log -> {log_file}")


if __name__ == "__main__":
    main()