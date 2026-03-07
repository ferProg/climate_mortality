import pandas as pd
df = pd.read_parquet("data/processed/tenerife/deaths_weekly_2016_2025.parquet")
print(df["week_start"].min(), df["week_start"].max(), len(df))
print("dupes:", df["week_start"].duplicated().sum())
