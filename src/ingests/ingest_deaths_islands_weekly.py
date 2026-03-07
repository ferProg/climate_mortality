from __future__ import annotations

import argparse
import re
import unicodedata
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from pandas._libs.tslibs.nattype import NaTType

INE_TABLE_ID = 35178
INE_CSV_URL = f"https://www.ine.es/jaxiT3/files/t/csv_bdsc/{INE_TABLE_ID}.csv"


# ---------------------------
# Helpers
# ---------------------------
def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _norm(s: str) -> str:
    s = str(s).strip().lower()
    s = "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))
    s = re.sub(r"\s+", " ", s)
    return s


def slug_island(s: str) -> str:
    s = _norm(s)

    # Canarias
    if "tenerife" in s:
        return "tenerife"
    if "gran canaria" in s:
        return "gran_canaria"
    if "lanzarote" in s:
        return "lanzarote"
    if "fuerteventura" in s:
        return "fuerteventura"
    if "gomera" in s:
        return "la_gomera"
    if "hierro" in s:
        return "el_hierro"
    # extra-safe against "Las Palmas"
    if "palma" in s and "las palmas" not in s:
        return "la_palma"

    # Baleares (por si aparecen; normalmente las filtraremos)
    if "mallorca" in s:
        return "mallorca"
    if "menorca" in s:
        return "menorca"
    if "ibiza" in s or "formentera" in s:
        return "ibiza_formentera"

    return re.sub(r"[^a-z0-9]+", "_", s).strip("_")[:60]


def periodo_to_week_start(periodo: str) -> pd.Timestamp | NaTType:
    """
    Periodo típico: '2024SM01' (año + 'SM' + semana).
    Convertimos a lunes ISO (semana ISO).
    """
    if not isinstance(periodo, str):
        return pd.NaT
    m = re.match(r"^(\d{4})SM(\d{2})$", periodo.strip())
    if not m:
        return pd.NaT
    year = int(m.group(1))
    week = int(m.group(2))
    try:
        return pd.Timestamp.fromisocalendar(year, week, 1)  # Monday
    except Exception:
        return pd.NaT


def download_ine_csv(out_fp: Path) -> None:
    r = requests.get(INE_CSV_URL, timeout=90)
    r.raise_for_status()
    out_fp.write_bytes(r.content)


# ---------------------------
# Core
# ---------------------------
CANARY_TERMS = [
    "tenerife",
    "gran canaria",
    "lanzarote",
    "fuerteventura",
    "gomera",
    "hierro",
    "palma",
]


def load_deaths_weekly(
    raw_fp: Path,
    start_year: int,
    end_year: int,
    *,
    canary_only: bool = True,
) -> pd.DataFrame:
    df = pd.read_csv(raw_fp, sep=";", encoding="latin1")
    df.columns = [c.strip() for c in df.columns]

    # Weekly rows only
    if "Periodo" not in df.columns:
        raise ValueError("Expected column 'Periodo' not found in INE CSV.")

    df["Periodo"] = df["Periodo"].astype(str).str.strip()
    df = df[df["Periodo"].str.match(r"^\d{4}SM\d{2}$", na=False)].copy()

    # Keep "Dato base" only
    if "Tipo de dato" in df.columns:
        df = df[df["Tipo de dato"].astype(str).str.contains("Dato base", case=False, na=False)].copy()

    # Week start + year filter
    df["week_start"] = df["Periodo"].apply(periodo_to_week_start)
    df["week_start"] = pd.to_datetime(df["week_start"], errors="coerce")
    df = df.dropna(subset=["week_start"]).copy()
    df["year"] = df["week_start"].dt.year
    df = df[(df["year"] >= start_year) & (df["year"] <= end_year)].copy()

    # Deaths
    if "Total" not in df.columns:
        raise ValueError("Expected column 'Total' not found in INE CSV. Check downloaded file format.")
    df["deaths_week"] = pd.to_numeric(df["Total"], errors="coerce")
    df = df.dropna(subset=["deaths_week"]).copy()
    df["deaths_week"] = df["deaths_week"].astype(int)

    # Island column
    if "Islas" not in df.columns:
        raise ValueError("Expected column 'Islas' not found in INE CSV columns.")
    df["island_raw"] = df["Islas"].astype(str).str.strip()

    # Optional: keep only Canary Islands
    if canary_only:
        mask_can = df["island_raw"].apply(lambda x: any(t in _norm(x) for t in CANARY_TERMS))
        df = df[mask_can].copy()

    # Canonical island slug
    df["island"] = df["island_raw"].apply(slug_island)

    # Output schema
    out = df[["week_start", "island", "deaths_week", "Periodo"]].rename(columns={"Periodo": "source_period"})
    out = out.sort_values(["island", "week_start"]).reset_index(drop=True)
    return out


def parse_island_arg(s: str) -> str:
    """
    Normalize user island input into our canonical slug.
    Accepts: 'Tenerife', 'Gran Canaria', 'La Palma', 'El Hierro', 'La Gomera', etc.
    """
    s_norm = _norm(s).replace("_", " ")
    # allow common forms
    if "tenerife" in s_norm:
        return "tenerife"
    if "gran canaria" in s_norm or s_norm == "grancanaria":
        return "gran_canaria"
    if "lanzarote" in s_norm:
        return "lanzarote"
    if "fuerteventura" in s_norm:
        return "fuerteventura"
    if "gomera" in s_norm:
        return "la_gomera"
    if "hierro" in s_norm:
        return "el_hierro"
    if "la palma" in s_norm or (s_norm == "palma" and "las palmas" not in s_norm):
        return "la_palma"
    raise ValueError(
        f"Unknown island '{s}'. Use one of: "
        f"tenerife, gran_canaria, lanzarote, fuerteventura, la_palma, la_gomera, el_hierro"
    )


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Download INE table and output weekly deaths for a selected Canary Island."
    )
    ap.add_argument("--data", default="data", help="Root folder for data/")
    ap.add_argument("--start_year", type=int, default=2016)
    ap.add_argument("--end_year", type=int, default=2025)
    ap.add_argument(
        "--isla",
        required=True,
        help="Island (e.g. Tenerife, Gran Canaria, Lanzarote, Fuerteventura, La Palma, La Gomera, El Hierro)",
    )
    ap.add_argument(
        "--out",
        default=None,
        help="Optional output file path (.parquet). Default: data/processed/<isla>/deaths_weekly_<start>_<end>.parquet",
    )
    ap.add_argument(
        "--canary_only",
        action="store_true",
        help="Keep only Canary Islands rows (recommended).",
    )
    args = ap.parse_args()

    data_root = Path(args.data).resolve()
    raw_dir = data_root / "raw" / "defunciones"
    ensure_dir(raw_dir)

    raw_fp = raw_dir / f"ine_{INE_TABLE_ID}.csv"
    if not raw_fp.exists():
        print(f"Downloading INE table {INE_TABLE_ID} CSV...")
        download_ine_csv(raw_fp)
        print("Saved raw:", raw_fp)
    else:
        print("Raw exists, skipping download:", raw_fp)

    island_slug = parse_island_arg(args.isla)

    df_out = load_deaths_weekly(
        raw_fp=raw_fp,
        start_year=args.start_year,
        end_year=args.end_year,
        canary_only=True if args.canary_only or True else False,  # keep as Canary-only by default
    )

    df_out = df_out[df_out["island"] == island_slug].copy()
    if df_out.empty:
        raise SystemExit(f"No rows found for island='{island_slug}' in years {args.start_year}-{args.end_year}.")

    if args.out:
        out_fp = Path(args.out)
        # Si es relativo, lo anclamos a data_root (decisión razonable en tu proyecto)
        if not out_fp.is_absolute():
            out_fp = (data_root / out_fp).resolve()
        else:
            out_fp = out_fp.resolve()

        ensure_dir(out_fp.parent)
    else:
        out_dir = (data_root / "processed" / island_slug).resolve()
        ensure_dir(out_dir)
        out_fp = out_dir / f"deaths_weekly_{args.start_year}_{args.end_year}.parquet"

    df_out.to_parquet(out_fp, index=False)
    print("DONE")
    print("island:", island_slug)
    print("rows:", len(df_out))
    print("out:", out_fp)


if __name__ == "__main__":
    main()