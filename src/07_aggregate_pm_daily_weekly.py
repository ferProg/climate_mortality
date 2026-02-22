'''
Función: Convierte PM a diario y luego a semanal, generando métricas tipo media semanal, picos, y/o episodios (ej. pm_peak80, contadores de días sobre umbral).
Salida típica: dataset semanal PM (y a veces dataset diario si lo guardas).
input: data/interim/pm_hourly_mercado_central_2016_2024.csv

output:

data/processed/calima_pm_daily_2016_2024_mercado_central.csv

data/processed/calima_pm_weekly_2016_2024_mercado_central_episodes.csv
'''
from pathlib import Path
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_INTERIM = PROJECT_ROOT / "data" / "interim"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"
LOGS = PROJECT_ROOT / "logs"

IN_FILE = DATA_INTERIM / "pm_hourly_mercado_central_2016_2024.csv"

OUT_DAILY = DATA_PROCESSED / "calima_pm_daily_2016_2024_mercado_central.csv"
OUT_WEEKLY = DATA_PROCESSED / "calima_pm_weekly_2016_2024_mercado_central_episodes.csv"
LOG_FILE = LOGS / "03_aggregate_pm_daily_weekly_log.txt"


def main():
    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
    LOGS.mkdir(parents=True, exist_ok=True)

    if not IN_FILE.exists():
        raise FileNotFoundError(f"Missing input file: {IN_FILE}")

    # Safe: don't overwrite
    if OUT_DAILY.exists() or OUT_WEEKLY.exists():
        print("[SKIP] Output already exists. Remove output files if you want to regenerate:")
        print(" -", OUT_DAILY)
        print(" -", OUT_WEEKLY)
        return

    pm = pd.read_csv(IN_FILE, parse_dates=["datetime"])

    # numeric
    pm["PM10"] = pd.to_numeric(pm["PM10"], errors="coerce")
    pm["PM2_5"] = pd.to_numeric(pm["PM2_5"], errors="coerce")

    # hourly -> daily
    pm["day"] = pm["datetime"].dt.floor("D")

    daily = (pm.groupby("day", as_index=False)
             .agg(
                 pm10_mean_day=("PM10", "mean"),
                 pm10_max_day=("PM10", "max"),
                 pm25_mean_day=("PM2_5", "mean"),
                 pm25_max_day=("PM2_5", "max"),
                 n_obs=("PM10", "count")
             ))

    daily["coarse_mean_day"] = daily["pm10_mean_day"] - daily["pm25_mean_day"]
    daily["coarse_max_day"] = daily["pm10_max_day"] - daily["pm25_max_day"]

    # --- Health-style daily flags (mean 24h) ---
    daily["pm10_who_day"] = (daily["pm10_mean_day"] >= 45).astype(int)
    daily["pm10_legal_day"] = (daily["pm10_mean_day"] >= 50).astype(int)
    daily["pm10_ica_desf_day"] = (daily["pm10_mean_day"] >= 51).astype(int)

    # --- Episode daily flags (peaks) ---
    daily["pm10_peak80_day"] = (daily["pm10_max_day"] >= 80).astype(int)
    daily["pm10_peak100_day"] = (daily["pm10_max_day"] >= 100).astype(int)

    # --- Coarse dust proxies ---
    daily["coarse20_day"] = (daily["coarse_mean_day"] >= 20).astype(int)
    daily["coarse40max_day"] = (daily["coarse_max_day"] >= 40).astype(int)

    # daily -> weekly (Monday)
    daily["week_start"] = (daily["day"] - pd.to_timedelta(daily["day"].dt.weekday, unit="D")).dt.normalize()

    weekly = (daily.groupby("week_start", as_index=False)
              .agg(
                  pm10_mean_week=("pm10_mean_day", "mean"),
                  pm10_max_week=("pm10_max_day", "max"),
                  pm25_mean_week=("pm25_mean_day", "mean"),
                  pm25_max_week=("pm25_max_day", "max"),
                  coarse_mean_week=("coarse_mean_day", "mean"),
                  coarse_max_week=("coarse_max_day", "max"),

                  calima_days_peak80=("pm10_peak80_day", "sum"),
                  calima_days_peak100=("pm10_peak100_day", "sum"),
                  calima_days_coarse20=("coarse20_day", "sum"),
                  calima_days_coarse40max=("coarse40max_day", "sum"),

                  days_who=("pm10_who_day", "sum"),
                  days_legal=("pm10_legal_day", "sum"),
                  days_ica=("pm10_ica_desf_day", "sum"),

                  n_days=("day", "count")
              ))

    weekly["coverage"] = weekly["n_days"] / 7

    # Save
    daily.to_csv(OUT_DAILY, index=False)
    weekly.to_csv(OUT_WEEKLY, index=False)

    # log
    log_text = (
        "03_aggregate_pm_daily_weekly.py\n"
        f"IN:  {IN_FILE}\n"
        f"OUT daily:  {OUT_DAILY}\n"
        f"OUT weekly: {OUT_WEEKLY}\n"
        f"daily shape: {daily.shape} | min/max day: {daily['day'].min()} / {daily['day'].max()}\n"
        f"weekly shape: {weekly.shape} | min/max week_start: {weekly['week_start'].min()} / {weekly['week_start'].max()}\n"
        f"weekly n_days counts:\n{weekly['n_days'].value_counts().sort_index()}\n"
        f"flag sums (daily): who={int(daily['pm10_who_day'].sum())}, legal={int(daily['pm10_legal_day'].sum())}, ica={int(daily['pm10_ica_desf_day'].sum())}\n"
        f"flag sums (episodes): peak80={int(daily['pm10_peak80_day'].sum())}, peak100={int(daily['pm10_peak100_day'].sum())}, coarse20={int(daily['coarse20_day'].sum())}, coarse40max={int(daily['coarse40max_day'].sum())}\n"
    )
    LOG_FILE.write_text(log_text, encoding="utf-8")
    print(log_text)


if __name__ == "__main__":
    main()
