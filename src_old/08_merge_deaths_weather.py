'''
Función: Hace el merge principal (semanal) entre mortalidad INE y meteorología AEMET, alineando por semana (week start / ISO week) y dejando un dataset coherente para EDA/modelo.
Salida típica: tabla semanal deaths + weather.
 input data/processed/aemet_C4291_weekly_2015_2025.csv + data/interim/ine_deaths_weeklyweekly_2016_2025_clean.csv
output data/processed/tfe_death_weather_weekly_2016_2025_C491.csv
'''
from pathlib import Path
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"
LOGS = PROJECT_ROOT / "logs"

INE_FILE = DATA_INTERIM / "ine_deaths_weekly_2016_2025_clean.csv"
AEMET_FILE = DATA_PROCESSED / "aemet_C429I_weekly_2016_2025.csv"

OUT_FILE = DATA_PROCESSED / "tfe_deaths_weather_weekly_2016_2025_C429I.csv"
LOG_FILE = LOGS / "04_merge_weather_mortality_log.txt"


def main():
    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
    LOGS.mkdir(parents=True, exist_ok=True)

    ine = pd.read_csv(INE_FILE, parse_dates=["week_start"])
    aemet = pd.read_csv(AEMET_FILE, parse_dates=["week_start"])

    # keep only complete weeks from AEMET
    if "n_days" in aemet.columns:
        aemet_full = aemet[aemet["n_days"] == 7].copy()
    else:
        aemet_full = aemet.copy()

    # sanity: duplicates
    if ine.duplicated("week_start").any():
        raise ValueError("INE has duplicate week_start rows.")
    if aemet_full.duplicated("week_start").any():
        raise ValueError("AEMET has duplicate week_start rows (after filtering).")

    merged = ine.merge(aemet_full, on="week_start", how="inner")

    merged = merged.sort_values("week_start").reset_index(drop=True)
    merged.to_csv(OUT_FILE, index=False)

    log_text = (
        "04_merge_weather_mortality.py\n"
        f"INE:   {INE_FILE} shape={ine.shape} min={ine['week_start'].min()} max={ine['week_start'].max()}\n"
        f"AEMET:  {AEMET_FILE} shape={aemet.shape} | full_weeks={aemet_full.shape}\n"
        f"MERGED: {OUT_FILE} shape={merged.shape} min={merged['week_start'].min()} max={merged['week_start'].max()}\n"
        f"INE only weeks (not in merge): {(set(ine['week_start']) - set(merged['week_start'])).__len__()}\n"
        f"AEMET only weeks (not in merge): {(set(aemet_full['week_start']) - set(merged['week_start'])).__len__()}\n"
    )
    LOG_FILE.write_text(log_text, encoding="utf-8")
    print(log_text)


if __name__ == "__main__":
    main()
