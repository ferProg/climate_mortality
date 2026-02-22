'''
Función: Orquesta/consolida todo para generar el “master dataset” semanal final (2018–2024): mortalidad + meteo + PM + flags CAP (y cualquier feature extra), con validaciones básicas.
Salida típica: tfe_deaths_weather_pm_cap_weekly_2018_2024.csv (tu dataset de 342 semanas).

'''
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"

IN_BASE = DATA_PROCESSED / "tfe_deaths_weather_weekly_2016_2025_C429I.csv"
IN_PM   = DATA_PROCESSED / "calima_pm_weekly_2016_2024_mercado_central_episodes.csv"
IN_CAP  = DATA_PROCESSED / "aemet_calima_alerts_tenerife_weekly.csv"

OUT = DATA_PROCESSED / "tfe_deaths_weather_pm_cap_weekly_2018_2024.csv"

START = pd.Timestamp("2018-06-18")  # lunes
END   = pd.Timestamp("2024-12-30")  # lunes (última semana completa 2018-2024)

PM_COLS = [
    "pm10_mean_week", "pm10_max_week",
    "pm25_mean_week", "pm25_max_week",
    "coarse_mean_week", "coarse_max_week",
    "calima_days_peak80", "calima_days_peak100",
    "calima_days_coarse20", "calima_days_coarse40max",
    "days_who", "days_legal", "days_ica",
    "n_days", "coverage",
]

# Tenerife CAP (nos quedamos con nivel semanal y derivamos amarillo)
CAP_COLS = ["dust_tfe_level_max_week"]

def main():
    base = pd.read_csv(IN_BASE, parse_dates=["week_start"])
    pm   = pd.read_csv(IN_PM,   parse_dates=["week_start"])
    cap  = pd.read_csv(IN_CAP,  parse_dates=["week_start"])

    # recorte temporal
    base = base[(base["week_start"] >= START) & (base["week_start"] <= END)].copy()
    pm   = pm[(pm["week_start"] >= START) & (pm["week_start"] <= END)].copy()
    cap  = cap[(cap["week_start"] >= START) & (cap["week_start"] <= END)].copy()

    # seleccionar columnas
    pm_keep  = ["week_start"] + [c for c in PM_COLS if c in pm.columns]
    cap_keep = ["week_start"] + [c for c in CAP_COLS if c in cap.columns]

    pm  = pm[pm_keep].copy()
    cap = cap[cap_keep].copy()

    # crear flag "amarillo" (>=2)
    if "dust_tfe_level_max_week" in cap.columns:
        cap["dust_tfe_level_max_week"] = pd.to_numeric(cap["dust_tfe_level_max_week"], errors="coerce").fillna(0).astype(int)
        cap["dust_tfe_is_yellow_week"] = (cap["dust_tfe_level_max_week"] >= 2).astype(int)
    else:
        # si por lo que sea no existe, lo creamos vacío
        cap["dust_tfe_level_max_week"] = 0
        cap["dust_tfe_is_yellow_week"] = 0

    # merges
    m = base.merge(pm, on="week_start", how="left")
    m = m.merge(cap, on="week_start", how="left")

    # fill NaN en features derivadas
    fill_cols = [c for c in (PM_COLS + ["dust_tfe_level_max_week", "dust_tfe_is_yellow_week"]) if c in m.columns]
    for c in fill_cols:
        m[c] = m[c].fillna(0)

    # cast a int donde aplica (contadores/flags)
    INT_COLS = [
        "calima_days_peak80", "calima_days_peak100",
        "calima_days_coarse20", "calima_days_coarse40max",
        "days_who", "days_legal", "days_ica",
        "n_days",
        "dust_tfe_level_max_week", "dust_tfe_is_yellow_week",
    ]
    for c in INT_COLS:
        if c in m.columns:
            m[c] = m[c].round(0).astype(int)

    m.to_csv(OUT, index=False)

    print("05_build_full_weekly_2018_2024.py")
    print(f"BASE: {IN_BASE} -> {base.shape} weeks {base['week_start'].min().date()}..{base['week_start'].max().date()}")
    print(f"PM:   {IN_PM}   -> {pm.shape}   weeks {pm['week_start'].min().date()}..{pm['week_start'].max().date()}")
    print(f"CAP:  {IN_CAP}  -> {cap.shape}  weeks {cap['week_start'].min().date()}..{cap['week_start'].max().date()}")
    print(f"OUT:  {OUT} -> {m.shape} weeks {m['week_start'].min().date()}..{m['week_start'].max().date()}")

    if "dust_tfe_is_yellow_week" in m.columns:
        print("CAP yellow weeks:", int(m["dust_tfe_is_yellow_week"].sum()))
        print("CAP max level counts:\n", m["dust_tfe_level_max_week"].value_counts().sort_index())

    print("Done.")

if __name__ == "__main__":
    main()
