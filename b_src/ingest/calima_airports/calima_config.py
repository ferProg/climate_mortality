# src/02_calima/calima_config.py
from __future__ import annotations
from pathlib import Path

def find_project_root(start: Path) -> Path:
    p = start.resolve()
    while p != p.parent:
        if (p / "b_data").exists() and (p / "b_src").exists():
            return p
        p = p.parent
    raise RuntimeError("Could not find project root (folder with both 'b_data' and 'b_src').")

ROOT = find_project_root(Path.cwd())

# Airports in SC Tenerife province (ICAO codes).
# IDs (USAF, WBAN) to be filled/verified later.
# src/calima/calima_config.py

# Airports by island (ICAO codes)
AIRPORTS_BY_ISLAND = {
    "tenerife": ["GCXO", "GCTS"],
    "la_palma": ["GCLA"],
    "la_gomera": ["GCGM"],
    "el_hierro": ["GCHI"],
    "gran_canaria": ["GCLP"],            
    "lanzarote": ["GCRR"],
    "fuerteventura": ["GCFV"],
}

# ICAO -> (USAF, WBAN) for NOAA ISD
# WBAN "99999" es común fuera de US; ok.
ICAO_TO_ISD = {
    "GCXO": ("600150", "99999"),  # TFN
    "GCTS": ("600250", "99999"),  # TFS
    "GCLA": ("600050", "99999"),  #LA PALMA
    "GCGM": ("600070", "99999"),  # La Gomera (ISD history no trae ICAO, pero este mapping es correcto)
    "GCHI": ("600010", "99999"),  # EL HIERRO
    "GCLP": ("600300", "99999"),  # GRAN CANARIA
    "GCRR": ("600400", "99999"),  # LANZAROTE
    "GCFV": ("600350", "99999"),  # FUERTEVENTURA
}

# Detection parameters (paper-based)
VIS_STRICT_LT_M = 9_999     # 9999 encodes >=10 km in METAR-ish
RH_LT_PCT = 70.0
TARGET_HOUR_UTC = 13
TIME_TOL_MIN = 90

# Island rule: at least K airports dust-like same day (for islands with >1 airport)
K_ISLAND_DEFAULT = 1
K_ISLAND_TENERIFE = 2   # puedes subir a 2 si quieres ser más estricto