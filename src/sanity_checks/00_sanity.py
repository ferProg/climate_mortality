import pandas as pd
w = pd.read_parquet(r"C:\dev\projects\heat_mortality_analysis\data\interim\calima_proxy_weekly\calima_proxy_weekly_tenerife.parquet")
print(w["calima_any_week"].value_counts())
