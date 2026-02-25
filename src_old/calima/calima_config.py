# src/02_calima/calima_config.py
from __future__ import annotations
from pathlib import Path

def find_project_root(start: Path) -> Path:
    p = start.resolve()
    while p != p.parent:
        if (p / "data").exists() and (p / "src").exists():
            return p
        p = p.parent
    raise RuntimeError("Could not find project root (folder with both 'data' and 'src').")

ROOT = find_project_root(Path.cwd())

# Airports in SC Tenerife province (ICAO codes).
# IDs (USAF, WBAN) to be filled/verified later.
AIRPORTS_SC = {
    "GCXO": ("600150", "99999"),  # TFN ✅
    "GCTS": ("600250", "99999"),  # TFS ✅
    "GCLA": (None, None),         # La Palma  (to fill)
    "GCGM": (None, None),         # La Gomera (to fill)
    "GCHI": (None, None),         # El Hierro (to fill)
}

# Detection parameters (paper-based)
VIS_STRICT_LT_M = 9_999     # because 9999 encodes >=10 km
RH_LT_PCT = 70.0
TARGET_HOUR_UTC = 13
TIME_TOL_MIN = 90

# Provincial rule: at least K airports dust-like same day
K_PROVINCE = 2