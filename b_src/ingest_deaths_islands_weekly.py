from __future__ import annotations

import argparse
from pathlib import Path
import re
import pandas as pd
import requests


INE_TABLE_ID = 35178
# CSV (separado por ;) – endpoint publicado por INE/Jaxi
INE_CSV_URL = f"https://www.ine.es/jaxiT3/files/t/csv_bdsc/{INE_TABLE_ID}.csv"


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def periodo_to_week_start(periodo: str) -> pd.Timestamp | pd.NaT:
    """
    Periodo típico: '2024SM01' (año + 'SM' + semana)
    Convertimos a lunes ISO.
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
    r = requests.get(INE_CSV_URL, timeout=60)
    r.raise_for_status()
    out_fp.write_bytes(r.content)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--b_data", default="b_data", help="Root folder for b_data")
    ap.add_argument("--start_year", type=int, default=2016)
    ap.add_argument("--end_year", type=int, default=2025)
    ap.add_argument(
        "--islands",
        default="all",
        help='Comma-separated island names (e.g. "Tenerife,Palma,La Gomera,El Hierro") or "all"',
    )
    ap.add_argument("--only_island_output", action="store_true", help="Write only the per-island file(s), skip combined output")
    args = ap.parse_args()

    b_data = Path(args.b_data)
    raw_dir = b_data / "raw" / "defunciones"
    ensure_dir(raw_dir)

    # 1) Download raw CSV
    raw_fp = raw_dir / f"ine_{INE_TABLE_ID}.csv"
    if not raw_fp.exists():
        print(f"Downloading INE table {INE_TABLE_ID} CSV...")
        download_ine_csv(raw_fp)
        print("Saved raw:", raw_fp)
    else:
        print("Raw exists, skipping download:", raw_fp)

    # 2) Read (INE CSV bdsc uses ';' separator)
    df = pd.read_csv(raw_fp, sep=";", encoding="latin1")
    # Typical columns include: Islas, Tipo de dato, Periodo, Total (and sometimes Total Nacional etc.)
    # We'll normalize names
    df.columns = [c.strip() for c in df.columns]

    # 3) Filter to weekly base deaths (Dato base)
    # Periodo like 2024SM01
    df["Periodo"] = df["Periodo"].astype(str).str.strip()
    df = df[df["Periodo"].str.match(r"^\d{4}SM\d{2}$", na=False)].copy()

    # Keep "Dato base" only (deaths count)
    if "Tipo de dato" in df.columns:
        df = df[df["Tipo de dato"].astype(str).str.contains("Dato base", case=False, na=False)].copy()

    # 4) Build week_start and restrict years
    df["week_start"] = df["Periodo"].apply(periodo_to_week_start)
    df = df.dropna(subset=["week_start"]).copy()
    df["year"] = df["week_start"].dt.year
    df = df[(df["year"] >= args.start_year) & (df["year"] <= args.end_year)].copy()

    # 5) Clean numeric deaths (INE column usually "Total")
    if "Total" not in df.columns:
        raise ValueError("Expected column 'Total' not found in INE CSV. Check the downloaded file format.")
    df["deaths_week"] = pd.to_numeric(df["Total"], errors="coerce")
    df = df.dropna(subset=["deaths_week"]).copy()
    df["deaths_week"] = df["deaths_week"].astype(int)

    # 6) Island selection
    if "Islas" not in df.columns:
        raise ValueError("Expected column 'Islas' not found. Check INE CSV columns.")
    df["island_raw"] = df["Islas"].astype(str).str.strip()

    if args.islands.lower() != "all":
        wanted = [x.strip() for x in args.islands.split(",") if x.strip()]
        # Match by substring to tolerate prefixes like "38 Santa Cruz de Tenerife Tenerife"
        mask = False
        for w in wanted:
            mask = mask | df["island_raw"].str.contains(w, case=False, na=False)
        df = df[mask].copy()

    # 7) Write per-island processed files
    out_root = b_data / "processed"
    ensure_dir(out_root)

    # Create a stable island slug (simple heuristic)
    def slug(s: str) -> str:
        s = s.lower()
        # keep canonical names if they appear
        if "tenerife" in s: return "tenerife"
        if "palma" in s: return "la_palma"
        if "gomera" in s: return "la_gomera"
        if "hierro" in s: return "el_hierro"
        return re.sub(r"[^a-z0-9]+", "_", s).strip("_")[:60]

    df["island"] = df["island_raw"].apply(slug)

    keep = ["week_start", "island", "deaths_week", "Periodo"]
    df_out = df[keep].rename(columns={"Periodo": "source_period"}).sort_values(["island", "week_start"])

    for isl, sub in df_out.groupby("island"):
        isl_dir = out_root / isl
        ensure_dir(isl_dir)
        out_fp = isl_dir / f"deaths_weekly_{args.start_year}_{args.end_year}.parquet"
        sub.to_parquet(out_fp, index=False)
        print(f"[OK] {isl}: {len(sub):,} rows -> {out_fp}")

    # Also write a combined file (optional, handy)
    if not args.only_island_output:
        combined_fp = out_root / f"deaths_weekly_islands_{args.start_year}_{args.end_year}.parquet"
        df_out.to_parquet(combined_fp, index=False)
        print("Combined saved:", combined_fp)


if __name__ == "__main__":
    main()