'''
Función: Agrega meteorología AEMET (estación C429I u otra) a nivel semanal: medias, máximos/mínimos, percentiles si aplica, y variables útiles para epidemiología (temperaturas/HR/viento…).
Salida típica: dataset semanal meteo con una fila por semana.
 input data/raw/aemet_c4291_daily_2016_2025.csv and 
output data/processed/aemet_C4291_weekly_2016_2025.csv'''
from pathlib import Path
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_RAW = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"
LOGS = PROJECT_ROOT / "logs"

IN_FILE = DATA_RAW / "aemet_C429I_daily_2016_2025.csv"
OUT_FILE = DATA_PROCESSED / "aemet_C429I_weekly_2016_2025.csv"
LOG_FILE = LOGS / "03_aggregate_aemet_weekly_log.txt"


def to_float(s: pd.Series) -> pd.Series:
    # extrae número incluso si viene con texto; convierte a float
    return pd.to_numeric(s.astype(str).str.extract(r"(-?\d+\.?\d*)")[0], errors="coerce")


def main():
    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
    LOGS.mkdir(parents=True, exist_ok=True)

    if not IN_FILE.exists():
        raise FileNotFoundError(f"Missing input file: {IN_FILE}")

    df = pd.read_csv(IN_FILE, encoding="utf-8")

    if "fecha" not in df.columns:
        raise ValueError(f"Expected column 'fecha' in AEMET daily file. Found: {df.columns.tolist()}")

    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    df = df[df["fecha"].notna()].copy()

    # ---- Numeric coercions (only the columns we actually use) ----
    # Common AEMET columns you have: tmed, tmax, tmin, hrMedia, presMax, presMin, velmedia, etc.
    # We'll convert those if present.
    num_cols = [
        "tmed", "tmax", "tmin", "prec", "hrMedia", "presMax", "presMin", "velmedia", "racha"
    ]
    for c in num_cols:
        if c in df.columns:
            df[c] = to_float(df[c])

    # ---- Daily -> weekly ----
    df["day"] = df["fecha"].dt.floor("D")
    df["week_start"] = (df["day"] - pd.to_timedelta(df["day"].dt.weekday, unit="D")).dt.normalize()

    agg = {"day": ("day", "count")}
    if "tmed" in df.columns:     agg["temp_c_mean"] = ("tmed", "mean")
    if "tmax" in df.columns:     agg["tmax_c_mean"] = ("tmax", "mean"); agg["tmax_c_max"] = ("tmax", "max")
    if "tmin" in df.columns:     agg["tmin_c_mean"] = ("tmin", "mean"); agg["tmin_c_min"] = ("tmin", "min")
    if "hrMedia" in df.columns:  agg["humidity_mean"] = ("hrMedia", "mean")
    if "presMax" in df.columns and "presMin" in df.columns:
        # crude weekly mean pressure using both (you can refine later)
        df["pressure_hpa"] = pd.concat([df["presMax"], df["presMin"]], axis=1).mean(axis=1)
        agg["pressure_hpa_mean"] = ("pressure_hpa", "mean")
    elif "presMax" in df.columns:
        agg["pressure_hpa_mean"] = ("presMax", "mean")
    if "velmedia" in df.columns: agg["wind_ms_mean"] = ("velmedia", "mean")
    if "prec" in df.columns:     agg["prec_sum"] = ("prec", "sum")

    weekly = (df.groupby("week_start", as_index=False)
              .agg(**{k: v for k, v in agg.items()}))

    weekly = weekly.rename(columns={"day": "n_days"})
    weekly["coverage"] = weekly["n_days"] / 7

    # Keep only complete weeks by default (optional)
    # weekly = weekly[weekly["n_days"] == 7].copy()

    weekly = weekly.sort_values("week_start").reset_index(drop=True)
    weekly.to_csv(OUT_FILE, index=False)

    log_text = (
        "03_aggregate_aemet_weekly.py\n"
        f"IN:  {IN_FILE}\n"
        f"OUT: {OUT_FILE}\n"
        f"shape: {weekly.shape}\n"
        f"min/max week_start: {weekly['week_start'].min()} / {weekly['week_start'].max()}\n"
        f"n_days counts:\n{weekly['n_days'].value_counts().sort_index()}\n"
    )
    LOG_FILE.write_text(log_text, encoding="utf-8")
    print(log_text)


if __name__ == "__main__":
    main()
