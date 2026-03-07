import pandas as pd
from pathlib import Path

def parse_comma_decimal(x):
    if pd.isna(x): return pd.NA
    s = str(x).strip().replace('"', '')
    if s == "" or s.lower() in {"nan","none"}: return pd.NA
    s = s.replace(",", ".")
    try: return float(s)
    except: return pd.NA

csv_path = Path(r"data\raw\tenerife\aemet_tfe_C429I_daily_2016-01-01_2024-12-31.csv")
pq_path  = Path(r"data\raw\tenerife\aemet_tfe_C429I_daily_2016-01-01_2024-12-31.parquet")

df = pd.read_csv(csv_path)
df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")

for col in ["tmed","prec","tmin","tmax","velmedia","racha","sol","presMax","presMin"]:
    if col in df.columns:
        df[col] = df[col].map(parse_comma_decimal)

for col in ["hrMedia","hrMax","hrMin","dir","altitud"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

df.to_parquet(pq_path, index=False)
print("Wrote typed parquet:", pq_path)
print(df.dtypes)
