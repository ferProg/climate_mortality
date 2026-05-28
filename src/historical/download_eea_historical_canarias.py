"""
download_eea_historical_canarias.py
------------------------------------
Descarga los 42 parquets EEA Historical (2000-2012) de estaciones
de Canarias (provincias 35 y 38) a una carpeta local.

Ejecutar desde PowerShell o Jupyter:
    python download_eea_historical_canarias.py

Requiere:
    pip install requests tqdm
"""

import re
import time
import requests
from pathlib import Path
from tqdm import tqdm

# ── Configuración ──────────────────────────────────────────────────────────────
OUTPUT_DIR = Path(r"C:\Users\fdora\RA_Career\Projects\climate_mortality\data\raw\historical")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# URLs de estaciones Canarias (provincias 35 y 38) — Historical EEA
URLS = [
    u for u in [
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_35016010_10_49.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_35016011_10_47.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_35016012_10_49.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_35019002_10_47.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_35019002_10_A.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_35019001_10_47.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_35002002_10_47.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_35002002_10_A.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_35002001_10_47.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_35004001_10_49.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_35004002_10_49.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_35006001_10_49.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_35016013_10_49.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_35017001_10_47.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_35017002_10_47.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_35017003_10_49.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_35017004_10_49.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_35017005_10_49.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_35019004_10_47.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_35022002_10_47.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_35022002_10_A.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_35024001_10_49.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_35026001_10_47.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_35026001_10_49.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_35026003_10_49.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_35026004_10_49.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_38004001_10_49.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_38005001_10_46.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_38006001_10_46.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_38006002_10_46.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_38006003_10_46.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_38008001_10_49.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_38009001_10_49.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_38011004_10_49.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_38011005_10_49.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_38011006_10_47.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_38011007_10_49.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_38017001_10_46.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_38017002_10_46.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_38017003_10_46.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_38031001_10_49.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_38038010_10_49.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_38038012_10_46.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_38038021_10_49.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_38038022_10_46.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_38038023_10_49.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_38038024_10_49.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_38038025_10_46.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_38038026_10_46.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_38038027_10_49.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_38033001_10_49.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_38036001_10_49.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_38036002_10_49.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_38037001_10_49.parquet",
        "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-airbase/ES/SP_38048001_10_49.parquet",
    ]
]

# ── Descarga ───────────────────────────────────────────────────────────────────
print(f"Destino: {OUTPUT_DIR}")
print(f"Total URLs: {len(URLS)}\n")

ok, fail = [], []

for url in tqdm(URLS, desc="Descargando"):
    fname = url.split("/")[-1]
    dest  = OUTPUT_DIR / fname

    if dest.exists():
        tqdm.write(f"  SKIP (ya existe): {fname}")
        ok.append(fname)
        continue

    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        dest.write_bytes(r.content)
        ok.append(fname)
    except Exception as e:
        tqdm.write(f"  ERROR {fname}: {e}")
        fail.append((fname, str(e)))

    time.sleep(0.3)  # pausa cortés al servidor

# ── Resumen ────────────────────────────────────────────────────────────────────
print(f"\n✅ Descargados: {len(ok)}")
print(f"❌ Fallidos:    {len(fail)}")
if fail:
    print("\nFallidos:")
    for f, e in fail:
        print(f"  {f}: {e}")
