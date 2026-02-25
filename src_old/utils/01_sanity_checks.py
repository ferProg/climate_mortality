import pandas as pd

heat = pd.read_csv("data/processed/aemet_heat_alerts_weekly.csv")
heat["week_start"] = pd.to_datetime(heat["week_start"])

print("Weeks:", len(heat), heat["week_start"].min(), "->", heat["week_start"].max())

# Distribución de niveles máximos semanales
print("\nheat_level_max_week value_counts:")
print(heat["heat_level_max_week"].value_counts().sort_index())

# Cuántas semanas amarillas+ (>=2)
print("\nWeeks yellow+ (>=2):", (heat["heat_level_max_week"] >= 2).sum())
print("Weeks orange+ (>=3):", (heat["heat_level_max_week"] >= 3).sum())
print("Weeks red (>=4):", (heat["heat_level_max_week"] >= 4).sum())

# Por si quieres ver ejemplos reales de amarillas+
print("\nSample yellow+ weeks:")
print(heat.loc[heat["heat_level_max_week"] >= 2, ["week_start","heat_level_max_week","aemet_heat_alert_days","heat_days_lvl_ge2"]].head(15))