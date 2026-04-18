# src/utils/constants.py
ISLAND_CODES = {
    "tenerife": "tfe",
    "gran_canaria": "gcan",
    "lanzarote": "lzt",
    "fuerteventura": "ftv",
    "la_palma": "lpa",
    "gomera": "gom",
    "hierro": "hie",
}

# Ordered list of fallback weather stations per island.
# The wrapper tries each station in order until gap data is found.
# Primary station (used in the main ingest) should NOT be listed here —
# these are the alternates to try when the primary has missing data.
# Confidence notes based on AEMET OpenData station catalogue:
#   tenerife     : C429I = Tenerife Sur Aeropuerto (high confidence)
#   gran_canaria : C649I = Gran Canaria Aeropuerto (medium)
#   lanzarote    : C029O = Lanzarote Aeropuerto (low-medium)
#   fuerteventura: C249I = Fuerteventura Aeropuerto (low-medium)
#   la_palma     : C139E = La Palma Aeropuerto (low-medium)
#   gomera       : C329B = La Gomera Aeropuerto, C449O = San Sebastián (low-medium)
#   hierro       : TBD — add station codes when confirmed
ISLAND_WEATHER_STATIONS: dict[str, list[str]] = {
    "tenerife":      ["C429I", "C449C", "C449I", "C449X", "C449Z", "C449E", "C449H"],
    "gran_canaria":  ["C649I", "C649O", "C649X", "C649Z"],
    "lanzarote":     ["C029O", "C829C", "C829I", "C829X"],
    # C249I = Fuerteventura Aeropuerto (confirmed AEMET). Distinct from C429I (Tenerife).
    "fuerteventura": ["C249I", "C829F", "C829H", "C829Z"],
    "la_palma":      ["C139E", "C229O", "C229I", "C229X", "C229Z"],
    # C329Z = San Sebastián de La Gomera — confirmed gap-filler for 2020 COVID gap
    "gomera":        ["C329B", "C329Z", "C449O", "C329I", "C329X"],
    "hierro":        ["C929I", "C939E", "C939A", "C939B"],
}

def island_code(island: str) -> str:
    try:
        return ISLAND_CODES[island]
    except KeyError:
        raise KeyError(f"Unknown island '{island}'. Expected one of: {list(ISLAND_CODES.keys())}")

def island_weather_stations(island: str) -> list[str]:
    """Return the ordered list of fallback weather stations for a given island."""
    try:
        return ISLAND_WEATHER_STATIONS[island]
    except KeyError:
        raise KeyError(f"Unknown island '{island}'. Expected one of: {list(ISLAND_WEATHER_STATIONS.keys())}")