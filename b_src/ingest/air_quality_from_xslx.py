import pandas as pd
from pathlib import Path

fp = Path(r"C:\data\Air_Polution_GC_2015_2025_raw\Datos2016_2025\Datos2016_2025\Datos2024\Datos 2024.xlsx")
xls = pd.ExcelFile(fp)
print(xls.sheet_names)