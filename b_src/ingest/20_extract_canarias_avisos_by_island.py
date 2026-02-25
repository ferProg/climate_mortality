"""
Extract AEMET CAP alerts for a Canary Island from an existing parquet dataset.

Inputs:
  - Parquet dataset directory produced earlier (partitioned by year_onset=YYYY)
  - Island name (case-insensitive; accepts spaces/underscores/hyphens; strips accents)
  - Optional date range (applied to onset_dt in UTC)

Outputs:
  - .parquet or .csv.gz with filtered rows

Example (PowerShell):
  python src\20_extract_canarias_avisos_by_island.py `
    --isla "El Hierro" `
    --input_dir "C:\dev\projects\heat_mortality_analysis\b_data\interim\avisos_canarias_parquet_flags" `
    --out "C:\dev\projects\heat_mortality_analysis\b_data\processed\avisos_el_hierro_2018_2024.parquet" `
    --start 2018-06-18 `
    --end 2024-12-31
"""

import argparse
import glob
import os
import re
import unicodedata
from typing import Dict, Set

import pandas as pd


def normalize_island(s: str) -> str:
    """Normalize island input: lowercase, strip accents, spaces/hyphens->underscore."""
    s = s.strip().lower()
    s = "".join(
        ch for ch in unicodedata.normalize("NFKD", s)
        if not unicodedata.combining(ch)
    )
    s = re.sub(r"[\s\-]+", "_", s)
    s = re.sub(r"_+", "_", s)
    return s


# Canonical islands + accepted aliases (all will be normalized via normalize_island)
ISLAND_ALIASES: Dict[str, Set[str]] = {
    "tenerife": {"tenerife", "tf", "tfe"},
    "gran_canaria": {"gran canaria", "gran_canaria", "grancanaria", "gc"},
    "la_palma": {"la palma", "la_palma", "lapalma", "lp"},
    "lanzarote": {"lanzarote", "lzt"},
    "fuerteventura": {"fuerteventura", "ftv"},
    "la_gomera": {"la gomera", "la_gomera", "lagomera", "lg"},
    "el_hierro": {"el hierro", "el_hierro", "elhierro", "eh"},
}

# Build normalized alias -> canonical island map
CANONICAL: Dict[str, str] = {}
for canon, aliases in ISLAND_ALIASES.items():
    CANONICAL[normalize_island(canon)] = canon
    for a in aliases:
        CANONICAL[normalize_island(a)] = canon


def parse_args():
    p = argparse.ArgumentParser(
        description="Extract AEMET CAP alerts for a Canary Island from parquet dataset."
    )
    p.add_argument(
        "--isla",
        required=True,
        help="Island name (e.g. Tenerife, gran_canaria, 'El Hierro', etc.)",
    )
    p.add_argument(
        "--input_dir",
        required=True,
        help="Path to avisos_canarias_parquet_flags (partitioned by year_onset=YYYY).",
    )
    p.add_argument(
        "--out",
        required=True,
        help="Output file path (.parquet or .csv.gz).",
    )
    p.add_argument(
        "--start",
        default="2018-06-18",
        help="Start date (YYYY-MM-DD), applied to onset_dt (UTC).",
    )
    p.add_argument(
        "--end",
        default="2024-12-31",
        help="End date (YYYY-MM-DD), applied to onset_dt (UTC).",
    )
    return p.parse_args()


def read_year_partitions(input_dir: str, years: Set[int]) -> pd.DataFrame:
    """Read only the year partitions needed."""
    dfs = []
    for y in sorted(years):
        ydir = os.path.join(input_dir, f"year_onset={y}")
        files = glob.glob(os.path.join(ydir, "part_*.parquet"))
        if not files:
            continue
        dfs.append(pd.concat([pd.read_parquet(f) for f in files], ignore_index=True))
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


def main():
    args = parse_args()

    isla_key = normalize_island(args.isla)
    if isla_key not in CANONICAL:
        allowed = sorted(ISLAND_ALIASES.keys())
        raise SystemExit(f"Unknown island '{args.isla}'. Use one of: {allowed}")

    isla = CANONICAL[isla_key]
    flag_col = f"has_{isla}"

    start = pd.Timestamp(args.start, tz="UTC")
    end = pd.Timestamp(args.end, tz="UTC") + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

    years = set(range(start.year, end.year + 1))

    df = read_year_partitions(args.input_dir, years)
    if df.empty:
        raise SystemExit("No data found for selected years / input_dir.")

    # Ensure datetime type (parquet should preserve it, but belt+suspenders)
    if "onset_dt" not in df.columns:
        raise SystemExit("Expected column 'onset_dt' not found. Did you build the flags dataset correctly?")
    df["onset_dt"] = pd.to_datetime(df["onset_dt"], utc=True, errors="coerce")

    if flag_col not in df.columns:
        raise SystemExit(f"Expected flag column '{flag_col}' not found in dataset.")

    m = (df[flag_col] == 1) & (df["onset_dt"] >= start) & (df["onset_dt"] <= end)
    out_df = df.loc[m].copy().sort_values(["onset_dt", "expires_dt", "identifier"])

    out_dir = os.path.dirname(args.out)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    out_lower = args.out.lower()
    if out_lower.endswith(".parquet"):
        out_df.to_parquet(args.out, index=False)
    elif out_lower.endswith(".csv.gz"):
        out_df.to_csv(args.out, index=False, compression="gzip")
    else:
        raise SystemExit("Output extension must be .parquet or .csv.gz")

    print("DONE")
    print("isla:", isla)
    print("rows:", len(out_df))
    print("out:", args.out)


if __name__ == "__main__":
    main()