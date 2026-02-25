import pandas as pd

csv_path = r"b_data\processed\tenerife\aemet_tfe_C429I_weekly_2016-01-01_2024-12-31.csv"
pq_path  = r"b_data\processed\tenerife\aemet_tfe_C429I_weekly_2016-01-01_2024-12-31.parquet"

a = pd.read_csv(csv_path)
b = pd.read_parquet(pq_path)

# alineamos por week_start
a = a.sort_values("week_start").reset_index(drop=True)
b = b.sort_values("week_start").reset_index(drop=True)

num_cols = [c for c in a.columns if c not in ["week_start"]]

diffs = {}
for c in num_cols:
    da = pd.to_numeric(a[c], errors="coerce")
    db = pd.to_numeric(b[c], errors="coerce")
    diffs[c] = (da - db).abs().max()

# imprime solo las que difieren
for k, v in diffs.items():
    if v and v > 0:
        print(k, v)