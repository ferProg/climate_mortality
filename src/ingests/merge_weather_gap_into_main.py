from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Inject weekly gap rows from an alternate-station weather parquet into the main weekly parquet, "
            "replacing only matching week_start rows and adding a donor flag."
        )
    )
    p.add_argument(
        "--main",
        required=True,
        help="Path to main weekly weather parquet, e.g. data/processed/gomera/weather/weather_weekly_gom_2016_2024_calendarized.parquet",
    )
    p.add_argument(
        "--gap",
        required=True,
        help="Path to alternate-station weekly gap parquet, e.g. data/processed/gomera/weather/weather_weekly_gom_c329z_2020_gap.parquet",
    )
    p.add_argument(
        "--out",
        default=None,
        help="Optional output parquet path. Default: <main stem>_patched.parquet in same folder.",
    )
    p.add_argument(
        "--flag-col",
        default="imputed_from_c329z",
        help="Flag column to mark rows replaced from gap source. Default: imputed_from_c329z",
    )
    p.add_argument(
        "--donor-label",
        default="C329Z",
        help="Optional donor station label to write into donor_station column for patched rows. Default: C329Z",
    )
    p.add_argument(
        "--force-overlap",
        action="store_true",
        help="Allow gap rows whose weeks do not exist in the main dataset. By default, only overlapping weeks are used.",
    )
    return p.parse_args()


def read_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    df = pd.read_parquet(path)
    if "week_start" not in df.columns:
        raise ValueError(f"Missing required column 'week_start' in {path}")
    df = df.copy()
    df["week_start"] = pd.to_datetime(df["week_start"], errors="coerce").dt.tz_localize(None)
    if df["week_start"].isna().any():
        raise ValueError(f"Some week_start values could not be parsed in {path}")
    if df["week_start"].duplicated().any():
        dups = df.loc[df["week_start"].duplicated(), "week_start"].astype(str).tolist()
        raise ValueError(f"Duplicate week_start values found in {path}: {dups[:10]}")
    return df


def main() -> None:
    args = parse_args()

    main_path = Path(args.main)
    gap_path = Path(args.gap)
    out_path = Path(args.out) if args.out else main_path.with_name(f"{main_path.stem}_patched.parquet")

    main_df = read_parquet(main_path)
    gap_df = read_parquet(gap_path)

    if args.flag_col not in main_df.columns:
        main_df[args.flag_col] = 0
    else:
        main_df[args.flag_col] = pd.to_numeric(main_df[args.flag_col], errors="coerce").fillna(0).astype(int)

    if "donor_station" not in main_df.columns:
        main_df["donor_station"] = pd.NA

    main_cols = set(main_df.columns)
    gap_cols = [c for c in gap_df.columns if c in main_cols and c != args.flag_col]

    if "week_start" not in gap_cols:
        gap_cols = ["week_start"] + gap_cols

    overlap_weeks = set(main_df["week_start"]).intersection(set(gap_df["week_start"]))
    gap_overlap = gap_df.loc[gap_df["week_start"].isin(overlap_weeks), gap_cols].copy()
    gap_only = gap_df.loc[~gap_df["week_start"].isin(overlap_weeks), gap_cols].copy()

    if not args.force_overlap and gap_overlap.empty:
        raise ValueError(
            "No overlapping week_start values between main and gap datasets. "
            "If you intentionally want to append new weeks, rerun with --force-overlap."
        )

    patched = main_df.copy()
    patched = patched.set_index("week_start", drop=False)

    # Replace only overlapping rows, only for columns present in both datasets.
    replace_cols = [c for c in gap_overlap.columns if c != "week_start"]
    for _, row in gap_overlap.iterrows():
        wk = row["week_start"]
        for c in replace_cols:
            patched.at[wk, c] = row[c]
        patched.at[wk, args.flag_col] = 1
        patched.at[wk, "donor_station"] = args.donor_label

    # Optionally append non-overlapping weeks from the gap file.
    if args.force_overlap and not gap_only.empty:
        append = gap_only.copy()
        for c in patched.columns:
            if c not in append.columns:
                append[c] = pd.NA
        append[args.flag_col] = 1
        append["donor_station"] = args.donor_label
        append = append[patched.columns]
        append = append.set_index("week_start", drop=False)
        patched = pd.concat([patched, append], axis=0)

    patched = patched.sort_index().reset_index(drop=True)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    patched.to_parquet(out_path, index=False)

    print("Main rows:   ", len(main_df))
    print("Gap rows:    ", len(gap_df))
    print("Overlap used:", len(gap_overlap))
    print("Gap-only rows:", len(gap_only))
    print("Flag column: ", args.flag_col)
    print("Output:      ", out_path)
    if len(gap_overlap):
        print("Patched weeks:")
        for wk in sorted(gap_overlap["week_start"].dt.strftime("%Y-%m-%d").tolist()):
            print(" -", wk)


if __name__ == "__main__":
    main()
