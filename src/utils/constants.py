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

def island_code(island: str) -> str:
    try:
        return ISLAND_CODES[island]
    except KeyError:
        raise KeyError(f"Unknown island '{island}'. Expected one of: {list(ISLAND_CODES.keys())}")