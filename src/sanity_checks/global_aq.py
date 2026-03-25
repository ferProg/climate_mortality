import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
path_raw = (RAW_DIR / "tenerife" /"air_quality" / "cams_pm_6hourly_tfe_2025-01-01_2025-12-31.parquet")
path_daily = (PROCESSED_DIR / "tenerife" / "air_quality" / "cams_pm_daily_tfe_2025-01-01_2025-12-31.parquet")
path_weekly =(PROCESSED_DIR / "tenerife" / "air_quality" / "cams_pm_weekly_tfe_2025-01-01_2025-12-31.parquet" )

df6 = pd.read_parquet(path_raw)
dfd = pd.read_parquet(path_daily)
dfw = pd.read_parquet(path_weekly)

print(df6.shape, dfd.shape, dfw.shape)
print(df6.head())
print(dfd.head())
print(dfw.head())
print(dfw.tail())
print(dfw.isna().sum().sort_values(ascending=False).head(20))