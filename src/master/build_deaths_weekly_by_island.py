from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, Optional

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]  # .../heat_mortality_analysis
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

    # Accept patterns like 2025SM01 / 2025W01 / 2025-SM01 (robust-ish)
    year = None
    week = None

    try:
        year = int(p[0:4])
    except Exception:
        return None

    # find last 2 digits as week
    tail = p[-2:]
    if not tail.isdigit():
        return None
    week = int(tail)

    if week < 1 or week > 53:
        return None

    # ISO week Monday
    try:
        import datetime as dt

        d = dt.date.fromisocalendar(year, week, 1)
        return pd.Timestamp(d)
    except Exception:
        return None


def clean_total(x) -> Optional[float]:
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


def build_one_island(df: pd.DataFrame, code: str, island_name: str, island_value: str) -> Optional[pd.DataFrame]:
    sub = df.loc[df[ISLAND_COL].astype(str).str.strip() == island_value].copy()
    if sub.empty:
        print(f"… {code}: no rows matched Islas='{island_value}'")
        return None

    # Keep only "Dato base" if it exists (avoid rates/indices)
    if TYPE_COL in sub.columns:
        # If multiple types exist, prefer 'Dato base'
        types = set(sub[TYPE_COL].astype(str).str.strip().unique())
        if "Dato base" in types:
            sub = sub.loc[sub[TYPE_COL].astype(str).str.strip() == "Dato base"].copy()

    sub["week_start"] = sub[PERIOD_COL].apply(parse_ine_week_to_monday)
    sub = sub.dropna(subset=["week_start"])

    sub["deaths_week"] = sub[VALUE_COL].apply(clean_total)
    sub = sub.dropna(subset=["deaths_week"])

    weekly = (
        sub.groupby("week_start", as_index=False)["deaths_week"]
        .sum()
        .sort_values("week_start")
        .reset_index(drop=True)
    )

    weekly["island_code"] = code
    return weekly


def main(argv: list[str]) -> int:
    if not RAW_DEATHS.exists():
        raise FileNotFoundError(RAW_DEATHS)

    codes = argv[1:] if len(argv) > 1 else list(ISLANDS.keys())

    df = pd.read_csv(RAW_DEATHS, sep=";", dtype=str, encoding="utf-8")
    for col in [ISLAND_COL, PERIOD_COL, VALUE_COL]:
        if col not in df.columns:
            raise KeyError(f"Missing column '{col}'. Found: {list(df.columns)}")

    # 🔧 IMPORTANT: map exact 'Islas' values in the CSV to your codes.
    # I can't guess the exact spelling/accents used by INE in your file,
    # so we’ll validate against what's actually in the CSV.
    island_values = sorted(df[ISLAND_COL].astype(str).str.strip().unique())
    print("Found Islas values (sample):", island_values[:30], "..." if len(island_values) > 30 else "")

    # Default mapping (very likely). If your CSV uses different labels, edit this dict.
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
        ok += 1

    print("\nDone. OK=", ok, "FAIL=", fail)
    return 0 if fail == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))