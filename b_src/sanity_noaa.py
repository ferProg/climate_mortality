import pandas as pd
fp = r"C:\dev\projects\heat_mortality_analysis\b_data\interim\noaa_isd_parsed\isd_GCTS_2019.parquet"
df = pd.read_parquet(fp)
print(df.shape, df.columns.tolist(), df["dt_utc"].min(), df["dt_utc"].max())