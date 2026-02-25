# src/12_merge_cap_weather.py
from __future__ import annotations

from pathlib import Path
import pandas as pd


METEO_FP = Path("data/raw/tfe_deaths_weather_weekly_2016_2025_C429I.csv")
HEAT_FP  = Path("data/processed/aemet_heat_alerts_weekly.csv")

OUT_FP   = Path("data/interim/tfe_deaths_weather_weekly_2016_2025_C429I_plus_heatCAP.csv")


def main() -> None:
    if not METEO_FP.exists():
        raise FileNotFoundError(f"Missing meteo file: {METEO_FP}")
    if not HEAT_FP.exists():
        raise FileNotFoundError(f"Missing heat CAP weekly file: {HEAT_FP}")

    OUT_FP.parent.mkdir(parents=True, exist_ok=True)

    meteo = pd.read_csv(METEO_FP)
    heat  = pd.read_csv(HEAT_FP)

    meteo["week_start"] = pd.to_datetime(meteo["week_start"], errors="coerce")
    heat["week_start"]  = pd.to_datetime(heat["week_start"], errors="coerce")

    # CAP: yellow+ => level >= 2 (Moderate/Severe/Extreme)
    heat["heat_yellow_plus_week"] = (heat["heat_level_max_week"] >= 2).astype(int)

    # Avoid collision with meteo "coverage"
    if "coverage" in heat.columns:
        heat = heat.rename(columns={"coverage": "heat_cap_coverage"})

    keep = [
        "week_start",
        "aemet_heat_alert_days",
        "heat_level_max_week",
        "heat_yellow_plus_week",
        "heat_days_lvl_ge2",
        "heat_days_lvl_ge3",
        "heat_days_lvl_ge4",
        "n_days",
        "heat_cap_coverage",
        "aemet_heat_alert_any",
        "heat_any_lvl_ge2",
        "heat_any_lvl_ge3",
        "heat_any_lvl_ge4",
    ]

    # Keep only columns that actually exist (defensive)
    keep = [c for c in keep if c in heat.columns]

    merged = meteo.merge(heat[keep], on="week_start", how="left")

    # Fill CAP columns for weeks before CAP availability (pre-2018) with 0
    cap_int_cols = [
        "aemet_heat_alert_days",
        "heat_level_max_week",
        "heat_yellow_plus_week",
        "heat_days_lvl_ge2",
        "heat_days_lvl_ge3",
        "heat_days_lvl_ge4",
        "n_days",
        "aemet_heat_alert_any",
        "heat_any_lvl_ge2",
        "heat_any_lvl_ge3",
        "heat_any_lvl_ge4",
    ]
    for c in cap_int_cols:
        if c in merged.columns:
            merged[c] = merged[c].fillna(0).astype(int)

    if "heat_cap_coverage" in merged.columns:
        merged["heat_cap_coverage"] = merged["heat_cap_coverage"].fillna(0)

    # Keep meteo coverage as-is (if present)
    if "coverage" in merged.columns:
        merged["coverage"] = merged["coverage"].fillna(0)

    merged = merged.sort_values("week_start").reset_index(drop=True)

    # --- Sanity outputs ---
    print("Merged shape:", merged.shape)
    print("Range:", merged["week_start"].min(), "->", merged["week_start"].max())

    if "heat_yellow_plus_week" in merged.columns:
        n_yellow = int((merged["heat_yellow_plus_week"] == 1).sum())
        print("Weeks with yellow+ heat CAP:", n_yellow)

    # Save
    merged.to_csv(OUT_FP, index=False)
    print("Wrote:", OUT_FP)


if __name__ == "__main__":
    main()