import pandas as pd
from pathlib import Path

daily_fp = Path("data/interim/aemet_heat_alert_daily.csv")
daily = pd.read_csv(daily_fp)
daily["day"] = pd.to_datetime(daily["day"])

# Crea calendario continuo de días
cal = pd.DataFrame({"day": pd.date_range(daily["day"].min(), daily["day"].max(), freq="D")})

# Left join y rellena faltantes como "no alert"
daily2 = cal.merge(daily, on="day", how="left")
daily2["heat_level_max_day"] = daily2["heat_level_max_day"].fillna(0).astype(int)
daily2["aemet_heat_alert_day"] = (daily2["heat_level_max_day"] > 0).astype(int)

# week_start Monday
daily2["week_start"] = daily2["day"] - pd.to_timedelta(daily2["day"].dt.weekday, unit="D")
daily2["week_start"] = daily2["week_start"].dt.normalize()

# Recompute weekly with n_days always 7 (except last partial week if you cut range)
weekly2 = daily2.groupby("week_start").agg(
    aemet_heat_alert_days=("aemet_heat_alert_day", "sum"),
    heat_level_max_week=("heat_level_max_day", "max"),
    heat_days_lvl_ge2=("heat_level_max_day", lambda s: int((s >= 2).sum())),
    heat_days_lvl_ge3=("heat_level_max_day", lambda s: int((s >= 3).sum())),
    heat_days_lvl_ge4=("heat_level_max_day", lambda s: int((s >= 4).sum())),
    n_days=("day", "count"),
).reset_index()

weekly2["aemet_heat_alert_any"] = (weekly2["aemet_heat_alert_days"] > 0).astype(int)
weekly2["heat_any_lvl_ge2"] = (weekly2["heat_level_max_week"] >= 2).astype(int)
weekly2["heat_any_lvl_ge3"] = (weekly2["heat_level_max_week"] >= 3).astype(int)
weekly2["heat_any_lvl_ge4"] = (weekly2["heat_level_max_week"] >= 4).astype(int)
weekly2["coverage"] = weekly2["n_days"] / 7.0

# Guardar versiones "calendarizadas"
daily2.to_csv("data/interim/aemet_heat_alert_daily_calendarized.csv", index=False)
weekly2.to_csv("data/interim/aemet_heat_alerts_weekly_calendarized.csv", index=False)