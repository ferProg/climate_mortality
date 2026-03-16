from pathlib import Path
import pandas as pd

fp = Path(r"data/processed/lanzarote/deaths/deaths_weekly_lzt_1974-12-30_2026-01-26.parquet")
df = pd.read_parquet(fp)

print("shape:", df.shape)
print("\ncolumns:", df.columns.tolist())
print("\ndtypes:\n", df.dtypes)

print("\nhead:")
print(df.head())

print("\ntail:")
print(df.tail())

# Asegurar tipo fecha
df["week_start"] = pd.to_datetime(df["week_start"], errors="coerce")

print("\nweek_start nulls:", df["week_start"].isna().sum())
print("duplicate week_start:", df["week_start"].duplicated().sum())
print("min/max:", df["week_start"].min(), "->", df["week_start"].max())

# Frecuencia semanal esperada
full_weeks = pd.date_range(df["week_start"].min(), df["week_start"].max(), freq="W-MON")
missing_from_index = full_weeks.difference(df["week_start"])

print("\nexpected weekly rows:", len(full_weeks))
print("actual rows:", len(df))
print("missing from calendar index:", len(missing_from_index))

if len(missing_from_index):
    print("\nMissing weeks:")
    print(pd.Series(missing_from_index, name="week_start").to_string(index=False))

# Buscar posibles columnas flag
flag_cols = [c for c in df.columns if "miss" in c.lower() or "flag" in c.lower()]
print("\npossible flag cols:", flag_cols)

for c in flag_cols:
    print(f"\nvalue counts for {c}:")
    print(df[c].value_counts(dropna=False))

# Si existe flag de missing, ver filas afectadas
candidate_cols = [c for c in df.columns if "miss" in c.lower()]
for c in candidate_cols:
    mask = df[c].astype(str).str.lower().isin(["1", "true", "yes"])
    if mask.any():
        print(f"\nRows flagged by {c}:")
        print(df.loc[mask].sort_values("week_start").to_string(index=False))
