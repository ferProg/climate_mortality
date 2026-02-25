'''
Función: Ingesta la mortalidad semanal del INE (provincia Santa Cruz de Tenerife), filtra provincia, estandariza el identificador de semana (ISO week) y deja el dataset listo para merge.
Salida típica: tabla semanal de defunciones (week_start, week, deaths, etc.) en data/interim
'''
# reads data/raw/ine_deaths_sc_weekly_2016_2025.csv and creates data/interim/ie_deaths_weekly_2016_2025_clean.csv
from pathlib import Path
import pandas as pd


# ---------- Paths ----------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_RAW = PROJECT_ROOT / "data" / "raw"
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
LOGS = PROJECT_ROOT / "logs"

IN_FILE = DATA_RAW / "ine_deaths_sc_weekly_2016_2025.csv"
OUT_FILE = DATA_INTERIM / "ine_deaths_weekly_2016_2025_clean.csv"
LOG_FILE = LOGS / "01_ingest_ine_log.txt"


def periodo_to_week_start(periodo: pd.Series) -> pd.Series:
    """
    INE EDeS Periodo typically looks like '2025SM52' meaning ISO week 52 of 2025.
    Convert to Monday date (week_start).
    """
    s = periodo.astype(str).str.strip()

    year = pd.to_numeric(s.str.slice(0, 4), errors="coerce")

    # week = last 2 chars in 'YYYYSMWW'
    week = pd.to_numeric(s.str.extract(r"SM(\d{2})")[0], errors="coerce")

    # ISO week -> Monday
    # Using ISO calendar: Monday is day=1
    # Build strings like '2025-W52-1'
    iso_str = (
        year.astype("Int64").astype(str)
        + "-W"
        + week.astype("Int64").astype(str).str.zfill(2)
        + "-1"
    )
    return pd.to_datetime(iso_str, format="%G-W%V-%u", errors="coerce")


def main():
    DATA_INTERIM.mkdir(parents=True, exist_ok=True)
    LOGS.mkdir(parents=True, exist_ok=True)

    if not IN_FILE.exists():
        raise FileNotFoundError(f"Missing input file: {IN_FILE}")

    df = pd.read_csv(IN_FILE, sep=";", encoding="utf-8")

    # Expected columns from your previous load:
    # ['Total Nacional','Provincias','Tipo de dato','Periodo','Total']
    required = {"Periodo", "Total"}
    missing_cols = required - set(df.columns)
    if missing_cols:
        raise ValueError(f"INE file missing columns: {missing_cols}. Found: {df.columns.tolist()}")

    # Convert deaths
    df["deaths_week"] = pd.to_numeric(df["Total"], errors="coerce")
    df["week_start"] = periodo_to_week_start(df["Periodo"])

    out = (df[["week_start", "deaths_week"]]
           .dropna(subset=["week_start", "deaths_week"])
           .sort_values("week_start")
           .reset_index(drop=True))

    # Basic sanity
    shape = out.shape
    min_date = out["week_start"].min()
    max_date = out["week_start"].max()
    dup = out.duplicated(subset=["week_start"]).sum()

    # Year counts
    year_counts = out["week_start"].dt.year.value_counts().sort_index().to_dict()

    out.to_csv(OUT_FILE, index=False)

    log_text = (
        "01_ingest_ine.py\n"
        f"IN:  {IN_FILE}\n"
        f"OUT: {OUT_FILE}\n"
        f"shape: {shape}\n"
        f"min/max week_start: {min_date} / {max_date}\n"
        f"duplicate week_start rows: {dup}\n"
        f"year_counts: {year_counts}\n"
    )

    LOG_FILE.write_text(log_text, encoding="utf-8")
    print(log_text)


if __name__ == "__main__":
    main()
