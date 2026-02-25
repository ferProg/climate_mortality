import pandas as pd
from pathlib import Path

# Input (calendarized)
in_daily = Path("data/interim/aemet_heat_alert_daily_calendarized.csv")

# Output (processed)
out_daily = Path("data/processed/aemet_heat_alert_daily.csv")
out_weekly = Path("data/processed/aemet_heat_alerts_weekly.csv")
out_daily.parent.mkdir(parents=True, exist_ok=True)

# Load
daily = pd.read_csv(in_daily)
daily["day"] = pd.to_datetime(daily["day"])

# 1) Cut to study period end (inclusive)
daily = daily[daily["day"] <= pd.Timestamp("2025-12-31")].copy()
daily = daily.sort_values("day").reset_index(drop=True)

print("Daily range:", daily["day"].min(), "->", daily["day"].max(), "days=", len(daily))

# 2) Recompute week_start (Monday)
daily["week_start"] = (daily["day"] - pd.to_timedelta(daily["day"].dt.weekday, unit="D")).dt.normalize()

# 3) Weekly aggregation
weekly = daily.groupby("week_start").agg(
    aemet_heat_alert_days=("aemet_heat_alert_day", "sum"),
    heat_level_max_week=("heat_level_max_day", "max"),
    heat_days_lvl_ge2=("heat_level_max_day", lambda s: int((s >= 2).sum())),
    heat_days_lvl_ge3=("heat_level_max_day", lambda s: int((s >= 3).sum())),
    heat_days_lvl_ge4=("heat_level_max_day", lambda s: int((s >= 4).sum())),
    n_days=("day", "count"),
).reset_index()

weekly["aemet_heat_alert_any"] = (weekly["aemet_heat_alert_days"] > 0).astype(int)
weekly["heat_any_lvl_ge2"] = (weekly["heat_level_max_week"] >= 2).astype(int)
weekly["heat_any_lvl_ge3"] = (weekly["heat_level_max_week"] >= 3).astype(int)
weekly["heat_any_lvl_ge4"] = (weekly["heat_level_max_week"] >= 4).astype(int)
weekly["coverage"] = weekly["n_days"] / 7.0

print("Weekly range:", weekly["week_start"].min(), "->", weekly["week_start"].max(), "weeks=", len(weekly))
print("Coverage counts:", weekly["coverage"].value_counts().head())

# 4) Save to processed
daily.to_csv(out_daily, index=False)
weekly.to_csv(out_weekly, index=False)

print("Wrote:", out_daily)
print("Wrote:", out_weekly)