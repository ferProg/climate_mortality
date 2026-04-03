from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, Optional

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]  # .../climate_mortality
DATA_DIR = PROJECT_ROOT / "data"

RAW_DEATHS = DATA_DIR / "raw" / "deaths" / "ine_35178.csv"

ISLANDS: Dict[str, str] = {
    "tfe": "tenerife",
    "gcan": "gran_canaria",
    "lzt": "lanzarote",
    "ftv": "fuerteventura",
    "lpa": "la_palma",
    "gom": "gomera",
    "hie": "hierro",
}

ALIASES: Dict[str, str] = {
    "tfe": "tfe",
    "tenerife": "tfe",
    "gcan": "gcan",
    "gran_canaria": "gcan",
    "gran canaria": "gcan",
    "lzt": "lzt",
    "lanzarote": "lzt",
    "ftv": "ftv",
    "fuerteventura": "ftv",
    "lpa": "lpa",
    "la_palma": "lpa",
    "palma": "lpa",
    "gom": "gom",
    "gomera": "gom",
    "la_gomera": "gom",
    "hie": "hie",
    "hierro": "hie",
    "el_hierro": "hie",
}

# INE columns
ISLAND_COL = "Islas"
PERIOD_COL = "Periodo"
VALUE_COL = "Total"
TYPE_COL = "Tipo de dato"


def parse_ine_week_to_monday(periodo: str) -> Optional[pd.Timestamp]:
    """
    INE weekly period often looks like '2025SM52' (year + 'SM' + week).
    Return Monday of ISO week.
    """
    if not isinstance(periodo, str):
        return None
    p = periodo.strip()
    if len(p) < 7:
        return None

    try:
        year = int(p[0:4])
    except Exception:
        return None

    tail = p[-2:]
    if not tail.isdigit():
        return None
    week = int(tail)

    if week < 1 or week > 53:
        return None

    try:
        import datetime as dt

        d = dt.date.fromisocalendar(year, week, 1)
        return pd.Timestamp(d)
    except Exception:
        return None


def clean_total(x) -> Optional[float]:
    # el INE usa formato europeo.
    #  En Europa el separador de miles es . y el decimal es ,.
    #  Python espera formato anglosajón — al revés.

    if pd.isna(x):
        return None
    s = str(x).strip()
    if s == "":
        return None
    # handle "1.234" thousands or "1.234,5" style
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None


def normalize_code(raw: str) -> str:
    key = str(raw).strip().lower()
    return ALIASES.get(key, key)


def build_one_island(df: pd.DataFrame, code: str, island_name: str, island_value: str) -> Optional[pd.DataFrame]:
    
    #  Recibe el dataset entero del INE, el código de isla, su nombre interno, y su nombre exacto en el CSV.
    #  Devuelve un DataFrame semanal limpio para esa isla, o None si algo falla.

    sub = df.loc[df[ISLAND_COL].astype(str).str.strip() == island_value].copy()
    if sub.empty:
        print(f"… {code}: no rows matched Islas='{island_value}'")
        return None

    # Keep only "Dato base" if it exists (avoid rates/indices)
    if TYPE_COL in sub.columns:
        types = set(sub[TYPE_COL].astype(str).str.strip().unique())
        if "Dato base" in types:
            sub = sub.loc[sub[TYPE_COL].astype(str).str.strip() == "Dato base"].copy()

    sub[PERIOD_COL] = sub[PERIOD_COL].astype(str).str.strip()
    sub["week_start"] = sub[PERIOD_COL].apply(parse_ine_week_to_monday)
    sub = sub.dropna(subset=["week_start"]).copy()

    # IMPORTANT: keep rows even when deaths_week is missing in raw.
    # Some islands/weeks in INE come with blank Total cells.
    sub["deaths_week"] = sub[VALUE_COL].apply(clean_total)

    # Keep one row per week as provided by source.
    weekly = (
        sub[["week_start", "deaths_week"]]
        .drop_duplicates(subset=["week_start"], keep="first")
        .sort_values("week_start")
        .reset_index(drop=True)
    )

    # Calendarize full weekly range so missing weeks remain explicit rows with NaN.
    full_calendar = pd.DataFrame(
        {"week_start": pd.date_range(weekly["week_start"].min(), weekly["week_start"].max(), freq="7D")}
    )
    weekly = full_calendar.merge(weekly, on="week_start", how="left")
    weekly["deaths_missing_week"] = weekly["deaths_week"].isna().astype(int)
    weekly["island_code"] = code

    weekly = weekly[["week_start", "deaths_week", "deaths_missing_week", "island_code"]].copy()
    weekly = weekly.sort_values("week_start").reset_index(drop=True)
    return weekly


def main(argv: list[str]) -> int:
    if not RAW_DEATHS.exists():
        raise FileNotFoundError(RAW_DEATHS)

    raw_codes = argv[1:] if len(argv) > 1 else list(ISLANDS.keys())
    codes = [normalize_code(x) for x in raw_codes]

    df = pd.read_csv(RAW_DEATHS, sep=";", dtype=str, encoding="utf-8")
    for col in [ISLAND_COL, PERIOD_COL, VALUE_COL]:
        if col not in df.columns:
            raise KeyError(f"Missing column '{col}'. Found: {list(df.columns)}")

    island_values = sorted(df[ISLAND_COL].astype(str).str.strip().unique())
    print("Found Islas values (sample):", island_values[:30], "..." if len(island_values) > 30 else "")

    ISLAS_FILTER: Dict[str, str] = {
        "tfe": "Tenerife",
        "gcan": "Gran Canaria",
        "lzt": "Lanzarote",
        "ftv": "Fuerteventura",
        "lpa": "Palma, La",
        "gom": "Gomera, La",
        "hie": "Hierro, El",
    }

    ok, fail = 0, 0
    for code in codes:
        island_name = ISLANDS.get(code)
        if not island_name:
            print(f"🔥 Unknown island code: {code}")
            fail += 1
            continue

        island_value = ISLAS_FILTER.get(code)
        if not island_value:
            print(f"… {code}: missing ISLAS_FILTER mapping")
            fail += 1
            continue

        print("\n" + "=" * 70)
        print(f"🏝️  deaths weekly: {code} ({island_name}) using Islas='{island_value}'")

        weekly = build_one_island(df, code, island_name, island_value)
        if weekly is None:
            fail += 1
            continue

        out_dir = DATA_DIR / "processed" / island_name / "deaths"
        out_dir.mkdir(parents=True, exist_ok=True)

        dmin = weekly["week_start"].min().date()
        dmax = weekly["week_start"].max().date()
        out_fp = out_dir / f"deaths_weekly_{code}_{dmin}_{dmax}.parquet"

        weekly.to_parquet(out_fp, index=False)
        print(f"💾 Saved: {out_fp} shape={weekly.shape}")
        print(f"   Missing weeks flagged: {int(weekly['deaths_missing_week'].sum())}")
        ok += 1

    print("\nDone. OK=", ok, "FAIL=", fail)
    return 0 if fail == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
