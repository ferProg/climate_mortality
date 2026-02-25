import argparse
from pathlib import Path

import pandas as pd


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--csv", required=True, help="Path to weekly CSV or daily CSV")
    p.add_argument("--parquet", required=True, help="Path to weekly Parquet or daily Parquet")
    p.add_argument("--kind", choices=["weekly", "daily"], required=True)
    return p.parse_args()


def print_block(title, lines):
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)
    for ln in lines:
        print(ln)


def load_any(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    if path.suffix.lower() in {".parquet", ".pq"}:
        return pd.read_parquet(path)
    raise ValueError(f"Unsupported file type: {path}")


def common_checks(df: pd.DataFrame, name: str):
    print_block(f"{name}: shape/dtypes", [
        f"rows={len(df):,} cols={df.shape[1]}",
        f"columns={list(df.columns)}",
        "dtypes:",
        df.dtypes.to_string(),
    ])

    # Missingness quick look
    na = df.isna().mean().sort_values(ascending=False)
    top_na = na[na > 0].head(12)
    if len(top_na):
        print_block(f"{name}: top missingness (share)", [top_na.to_string()])
    else:
        print_block(f"{name}: missingness", ["No missing values detected."])


def daily_checks(df: pd.DataFrame, name: str):
    if "fecha" not in df.columns:
        print_block(f"{name}: daily checks", ["ERROR: expected column 'fecha' not found"])
        return

    d = df.copy()
    d["fecha"] = pd.to_datetime(d["fecha"], errors="coerce")
    bad_dates = d["fecha"].isna().sum()

    # duplicates by fecha+indicativo if available else fecha only
    key_cols = ["fecha"]
    if "indicativo" in d.columns:
        key_cols.append("indicativo")

    dup = d.duplicated(subset=key_cols).sum()

    min_date = d["fecha"].min()
    max_date = d["fecha"].max()
    n_unique_days = d["fecha"].dt.date.nunique()

    print_block(f"{name}: date integrity", [
        f"bad fecha parse: {bad_dates}",
        f"date range: {min_date} .. {max_date}",
        f"unique days: {n_unique_days}",
        f"duplicates on {key_cols}: {dup}",
    ])

    # crude physical sanity (after parsing comma decimals if present)
    def to_num(col):
        if col not in d.columns:
            return None
        s = d[col].astype(str).str.replace('"', '').str.replace(",", ".", regex=False)
        return pd.to_numeric(s, errors="coerce")

    tmed = to_num("tmed")
    tmax = to_num("tmax")
    tmin = to_num("tmin")
    pres_max = to_num("presMax")
    pres_min = to_num("presMin")
    wind = to_num("velmedia")
    gust = to_num("racha")
    prec = to_num("prec")
    rh = to_num("hrMedia")

    issues = []
    if tmax is not None and tmin is not None:
        issues.append(f"days with tmax < tmin: {(tmax < tmin).sum()}")
    if rh is not None:
        issues.append(f"rh outside 0–100: {((rh < 0) | (rh > 100)).sum()}")
    if pres_max is not None:
        issues.append(f"presMax outside 900–1100 hPa: {((pres_max < 900) | (pres_max > 1100)).sum()}")
    if pres_min is not None:
        issues.append(f"presMin outside 900–1100 hPa: {((pres_min < 900) | (pres_min > 1100)).sum()}")
    if wind is not None:
        issues.append(f"velmedia <0 or >40 m/s: {((wind < 0) | (wind > 40)).sum()}")
    if gust is not None:
        issues.append(f"racha <0 or >80 m/s: {((gust < 0) | (gust > 80)).sum()}")
    if prec is not None:
        issues.append(f"prec <0: {(prec < 0).sum()}")

    print_block(f"{name}: physical sanity counters (rough)", issues)

    # show a few extreme rows if any
    if tmax is not None:
        idx = tmax.nlargest(3).index
        print_block(f"{name}: top 3 tmax rows", [d.loc[idx, ["fecha", "tmax", "tmin", "tmed"]].to_string(index=False)])


def weekly_checks(df: pd.DataFrame, name: str):
    if "week_start" not in df.columns:
        print_block(f"{name}: weekly checks", ["ERROR: expected column 'week_start' not found"])
        return

    w = df.copy()
    w["week_start"] = pd.to_datetime(w["week_start"], errors="coerce")
    bad = w["week_start"].isna().sum()
    dup = w.duplicated(subset=["week_start"]).sum()

    min_ws = w["week_start"].min()
    max_ws = w["week_start"].max()

    # gaps: sort and check deltas
    w2 = w.sort_values("week_start")
    deltas = w2["week_start"].diff().dropna().dt.days
    gaps = (deltas != 7).sum()

    # coverage and n_days logic
    cov_bad = None
    if "coverage" in w.columns:
        cov_bad = ((w["coverage"] < 0) | (w["coverage"] > 1)).sum()

    nd_bad = None
    if "n_days" in w.columns:
        nd_bad = ((w["n_days"] < 0) | (w["n_days"] > 7)).sum()

    cov_mismatch = None
    if "coverage" in w.columns and "n_days" in w.columns:
        cov_mismatch = (abs(w["coverage"] - (w["n_days"] / 7.0)) > 1e-9).sum()

    lines = [
        f"bad week_start parse: {bad}",
        f"week_start range: {min_ws} .. {max_ws}",
        f"duplicates on week_start: {dup}",
        f"non-7-day gaps count: {gaps}",
    ]
    if cov_bad is not None:
        lines.append(f"coverage outside [0,1]: {cov_bad}")
    if nd_bad is not None:
        lines.append(f"n_days outside [0,7]: {nd_bad}")
    if cov_mismatch is not None:
        lines.append(f"coverage != n_days/7 mismatches: {cov_mismatch}")

    print_block(f"{name}: timeline + coverage sanity", lines)

    # quick descriptive stats for key metrics
    key_cols = [c for c in [
        "temp_c_mean", "tmax_c_mean", "tmax_c_max", "tmin_c_mean", "tmin_c_min",
        "humidity_mean", "pressure_hpa_mean", "wind_ms_mean", "prec_sum", "coverage", "n_days"
    ] if c in w.columns]

    if key_cols:
        desc = w[key_cols].describe(percentiles=[0.01, 0.5, 0.99]).T
        print_block(f"{name}: describe key columns", [desc.to_string()])


def main():
    args = parse_args()
    csv_path = Path(args.csv)
    pq_path = Path(args.parquet)

    df_csv = load_any(csv_path)
    df_pq = load_any(pq_path)

    # Basic: same columns?
    print_block("File comparison", [
        f"CSV: {csv_path}",
        f"PARQUET: {pq_path}",
        f"CSV shape: {df_csv.shape}",
        f"PARQUET shape: {df_pq.shape}",
        f"Same columns (set): {set(df_csv.columns) == set(df_pq.columns)}",
    ])

    # Row-level equality check (best effort): sort columns + rows, compare a sample hash
    common = sorted(set(df_csv.columns).intersection(df_pq.columns))
    a = df_csv[common].copy()
    b = df_pq[common].copy()

    # normalize datetime-like columns to string for stable compare
    for c in common:
        if "date" in c.lower() or "fecha" in c.lower() or "week" in c.lower():
            a[c] = a[c].astype(str)
            b[c] = b[c].astype(str)

    # if sizes match, compare checksums
    same_rows = (len(a) == len(b))
    if same_rows:
        a2 = a.sort_values(common).reset_index(drop=True)
        b2 = b.sort_values(common).reset_index(drop=True)
        equal = a2.equals(b2)
        print_block("Content equality (best-effort)", [f"Equal after normalization+sorting: {equal}"])
    else:
        print_block("Content equality (best-effort)", ["Row counts differ: skipping full equality check."])

    # Run checks
    common_checks(df_csv, "CSV")
    common_checks(df_pq, "PARQUET")

    if args.kind == "daily":
        daily_checks(df_csv, "CSV")
        daily_checks(df_pq, "PARQUET")
    else:
        weekly_checks(df_csv, "CSV")
        weekly_checks(df_pq, "PARQUET")


if __name__ == "__main__":
    main()