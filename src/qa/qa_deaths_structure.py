"""
QA validation for deaths dataset structure and completeness.

Checks:
  - Nulls in deaths_week
  - Duplicates (same week_start, multiple rows)
  - Missing weeks (gaps in timeline)
  - Flag columns (missing_week markers)
  
Output: CSV report + dict for integration into QA pipeline
"""

import argparse
from pathlib import Path
import pandas as pd
import numpy as np


def validate_deaths_structure(df: pd.DataFrame, island: str) -> tuple[dict, pd.DataFrame]:
    """
    Validate deaths dataset structure.
    
    Args:
        df: Deaths dataframe with week_start and deaths_week columns
        island: Island name for reporting
        
    Returns:
        qa_dict: Summary dict with counts and flags
        flags_df: DataFrame flagging problematic rows
    """
    
    df = df.copy()
    df["week_start"] = pd.to_datetime(df["week_start"], errors="coerce")
    
    # Basic checks
    qa_dict = {
        "island": island,
        "rows_total": int(len(df)),
        "deaths_week_nulls": int(df["deaths_week"].isna().sum()),
        "deaths_week_nulls_pct": float(df["deaths_week"].isna().mean() * 100),
        "week_start_nulls": int(df["week_start"].isna().sum()),
    }
    
    # Duplicates
    dup_mask = df["week_start"].duplicated(keep=False)
    qa_dict["duplicate_week_starts"] = int(dup_mask.sum())
    
    # Missing weeks (temporal continuity)
    df_clean = df.dropna(subset=["week_start"]).sort_values("week_start")
    
    if len(df_clean) > 0:
        min_date = df_clean["week_start"].min()
        max_date = df_clean["week_start"].max()
        qa_dict["date_min"] = str(min_date)
        qa_dict["date_max"] = str(max_date)
        
        # Expected weeks (full calendar)
        expected_weeks = pd.date_range(min_date, max_date, freq="W-MON")
        qa_dict["expected_weekly_rows"] = len(expected_weeks)
        qa_dict["actual_rows"] = len(df_clean)
        
        # Find missing
        missing_weeks = expected_weeks.difference(df_clean["week_start"])
        qa_dict["missing_weeks_count"] = len(missing_weeks)
        qa_dict["missing_weeks_pct"] = float(len(missing_weeks) / len(expected_weeks) * 100) if len(expected_weeks) > 0 else 0.0
        
        missing_df = pd.DataFrame({
            "week_start": missing_weeks,
            "status": "missing"
        })
    else:
        qa_dict["expected_weekly_rows"] = 0
        qa_dict["actual_rows"] = 0
        qa_dict["missing_weeks_count"] = 0
        qa_dict["missing_weeks_pct"] = 0.0
        missing_df = pd.DataFrame(columns=["week_start", "status"])
    
    # Flag columns
    flag_cols = [c for c in df.columns if "miss" in c.lower() or "flag" in c.lower()]
    qa_dict["n_flag_columns"] = len(flag_cols)
    
    # Build flags dataframe (rows with issues)
    flags_rows = []
    
    # Null rows
    for idx, row in df[df["deaths_week"].isna()].iterrows():
        flags_rows.append({
            "week_start": row.get("week_start"),
            "issue": "deaths_week_null",
            "details": ""
        })
    
    # Duplicate rows
    for idx, row in df[dup_mask].iterrows():
        flags_rows.append({
            "week_start": row.get("week_start"),
            "issue": "duplicate_week_start",
            "details": f"row {idx}"
        })
    
    # Flagged rows (if flag columns exist)
    for col in flag_cols:
        mask = df[col].astype(str).str.lower().isin(["1", "true", "yes"])
        for idx, row in df[mask].iterrows():
            flags_rows.append({
                "week_start": row.get("week_start"),
                "issue": f"flag_{col}",
                "details": f"{col}={row[col]}"
            })
    
    flags_df = pd.DataFrame(flags_rows) if flags_rows else pd.DataFrame(columns=["week_start", "issue", "details"])
    
    # Distribution of deaths
    if "deaths_week" in df.columns:
        deaths_clean = df["deaths_week"].dropna()
        if len(deaths_clean) > 0:
            qa_dict["deaths_mean"] = float(deaths_clean.mean())
            qa_dict["deaths_median"] = float(deaths_clean.median())
            qa_dict["deaths_min"] = float(deaths_clean.min())
            qa_dict["deaths_max"] = float(deaths_clean.max())
            qa_dict["deaths_p95"] = float(deaths_clean.quantile(0.95))
            qa_dict["deaths_p99"] = float(deaths_clean.quantile(0.99))
    
    return qa_dict, flags_df


def main():
    ap = argparse.ArgumentParser(
        description="Validate deaths dataset structure."
    )
    ap.add_argument("--master", required=True, help="Path to master parquet/csv with deaths_week column")
    ap.add_argument("--island", required=True, help="Island name for reporting, e.g., gcan")
    ap.add_argument("--outdir", default="reports/tables")
    args = ap.parse_args()
    
    fp_master = Path(args.master)
    outdir = Path(args.outdir) / "island" / args.island
    outdir.mkdir(parents=True, exist_ok=True)
    
    # Read
    if fp_master.suffix.lower() == ".parquet":
        df = pd.read_parquet(fp_master)
    else:
        df = pd.read_csv(fp_master)
    
    # Validate
    qa_dict, flags_df = validate_deaths_structure(df, args.island)
    
    # Write outputs
    qa_df = pd.DataFrame([qa_dict])
    qa_df.to_csv(outdir / f"qa_deaths_structure_{args.island}.csv", index=False)
    
    if not flags_df.empty:
        flags_df.to_csv(outdir / f"qa_deaths_structure_{args.island}_flags.csv", index=False)
    
    # Print summary
    print(f"Deaths QA Summary ({args.island}):")
    print(f"  Total rows: {qa_dict['rows_total']}")
    print(f"  Nulls: {qa_dict['deaths_week_nulls']} ({qa_dict['deaths_week_nulls_pct']:.1f}%)")
    print(f"  Duplicates: {qa_dict['duplicate_week_starts']}")
    print(f"  Missing weeks: {qa_dict['missing_weeks_count']} / {qa_dict['expected_weekly_rows']}")
    print(f"  Deaths range: {qa_dict.get('deaths_min', 'N/A')} - {qa_dict.get('deaths_max', 'N/A')}")
    print(f"\nOutputs:")
    print(f"  QA: {(outdir / f'qa_deaths_structure_{args.island}.csv').resolve()}")
    if not flags_df.empty:
        print(f"  Flags: {(outdir / f'qa_deaths_structure_{args.island}_flags.csv').resolve()}")


if __name__ == "__main__":
    main()
