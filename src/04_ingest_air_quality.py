'''
Función: Ingesta los datos de calidad del aire (PM10/PM2.5) desde la fuente que estés usando, limpia formatos, estandariza timestamps y guarda un dataset base para agregación.
Salida típica: PM a nivel horario/diario en data/raw o data/interim.

input: data/raw/air_quality/air_conditions_mercado_GC_2016_2025.xlsx

output: data/interim/pm_hourly_mercado_central_2016_2024.csv
'''
from pathlib import Path
import re
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_RAW = PROJECT_ROOT / "data" / "raw" / "air_quality"
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
LOGS = PROJECT_ROOT / "logs"

IN_FILE = DATA_RAW / "air_conditions_mercado_GC_2016_2025.xlsx"
OUT_FILE = DATA_INTERIM / "pm_hourly_mercado_central_2016_2024.csv"
LOG_FILE = LOGS / "01_ingest_air_quality_log.txt"


def load_mercado_year(path_xlsx: Path, sheet: str) -> pd.DataFrame:
    # Header real está en la fila 1 (fila 0 es título "Mercado Central")
    df = pd.read_excel(path_xlsx, sheet_name=sheet, header=1)

    # Clean column names
    df.columns = [str(c).strip() for c in df.columns]

    # Keep only the "hourly" block: from Fecha to CO mg/m3
    end_col = "CO mg/m3"
    if end_col not in df.columns:
        raise ValueError(f"[{sheet}] Missing '{end_col}'. Columns: {df.columns.tolist()}")

    df = df.iloc[:, : df.columns.get_loc(end_col) + 1].copy()

    # Standardize column names
    rename_map = {"PM2,5": "PM2_5", "CO mg/m3": "CO_mg_m3"}
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Strip spaces in known columns (e.g. 'PM10 ')
    df.columns = [c.strip() for c in df.columns]

    # Robust date parsing (mixed types)
    raw = df["Fecha"]
    fecha = pd.to_datetime(raw, errors="coerce", format="mixed")

    # Fallback for dd/mm/yy and dd/mm/yyyy strings
    mask = fecha.isna()
    if mask.any():
        txt = raw[mask].astype(str).str.strip()
        mask_slash = txt.str.contains("/", na=False)

        parsed1 = pd.to_datetime(txt[mask_slash], format="%d/%m/%y", errors="coerce")
        parsed2 = pd.to_datetime(txt[mask_slash], format="%d/%m/%Y", errors="coerce")
        parsed = parsed1.copy()
        m2 = parsed.isna()
        parsed.loc[m2] = parsed2.loc[m2]

        # assign back
        idx = txt[mask_slash].index
        fecha.loc[idx] = parsed.values

    df["Fecha"] = fecha
    df = df[df["Fecha"].notna()].copy()

    # Hour -> datetime (Hora often 1..24)
    hour = pd.to_numeric(df["Hora"], errors="coerce").astype("float")
    hour_adj = hour.where(~hour.between(1, 24), hour - 1)  # 1..24 -> 0..23
    hour_adj = hour_adj.where(hour_adj != 24, 23)

    df["datetime"] = df["Fecha"] + pd.to_timedelta(hour_adj, unit="h")

    # Numerics
    for c in ["PM10", "PM2_5"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c].astype(str).str.replace(",", ".", regex=False), errors="coerce")

    df["year_sheet"] = int(sheet)
    return df


def main():
    DATA_INTERIM.mkdir(parents=True, exist_ok=True)
    LOGS.mkdir(parents=True, exist_ok=True)

    if not IN_FILE.exists():
        raise FileNotFoundError(f"Missing input file: {IN_FILE}")

    # Safe: don't overwrite
    if OUT_FILE.exists():
        print(f"[SKIP] {OUT_FILE} already exists. Remove it if you want to regenerate.")
        return

    xls = pd.ExcelFile(IN_FILE)
    years = [s for s in xls.sheet_names if str(s).isdigit()]
    years = sorted(years, key=int)

    parts = []
    for y in years:
        parts.append(load_mercado_year(IN_FILE, str(y)))

    pm_hourly = pd.concat(parts, ignore_index=True)

    # Filter to 2016..2024 only (we explicitly ignore 2025 for now)
    pm_hourly = pm_hourly[(pm_hourly["datetime"] >= "2016-01-01") & (pm_hourly["datetime"] < "2025-01-01")].copy()

    pm_hourly.to_csv(OUT_FILE, index=False)

    log_text = (
        "01_ingest_air_quality.py\n"
        f"IN:  {IN_FILE}\n"
        f"OUT: {OUT_FILE}\n"
        f"shape: {pm_hourly.shape}\n"
        f"min/max datetime: {pm_hourly['datetime'].min()} / {pm_hourly['datetime'].max()}\n"
        f"years: {pm_hourly['datetime'].dt.year.value_counts().sort_index().to_dict()}\n"
    )
    LOG_FILE.write_text(log_text, encoding="utf-8")
    print(log_text)


if __name__ == "__main__":
    main()
