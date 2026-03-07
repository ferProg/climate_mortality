from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from src.utils.constants import island_code
from src.utils.dates import normalize_week_start


ISLANDS = [
    "tenerife",
    "gran_canaria",
    "lanzarote",
    "fuerteventura",
    "la_palma",
    "gomera",
    "hierro",
]


ANALYSIS_START = pd.Timestamp("2015-12-28")
ANALYSIS_END = pd.Timestamp("2024-12-30")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build weekly master dataset for one island or all islands."
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--island",
        choices=ISLANDS,
        help="Canonical island name, e.g. tenerife",
    )
    group.add_argument(
        "--all",
        action="store_true",
        help="Build master datasets for all islands",
    )

    parser.add_argument(
        "--processed-dir",
        default="data/processed",
        help="Base processed directory",
    )
    parser.add_argument(
        "--interim-dir",
        default="data/interim",
        help="Base interim directory",
    )
    return parser.parse_args()


def week_calendar(start: str, end: str) -> pd.DataFrame:
    weeks = pd.date_range(start=start, end=end, freq="W-MON")
    return pd.DataFrame({"week_start": weeks})


def ensure_week_start(df: pd.DataFrame, source_name: str) -> pd.DataFrame:
    if "week_start" not in df.columns:
        raise ValueError(f"{source_name}: missing 'week_start'")

    df = df.copy()
    df["week_start"] = normalize_week_start(df["week_start"])

    if df["week_start"].isna().any():
        raise ValueError(f"{source_name}: invalid values in 'week_start'")

    if df["week_start"].duplicated().any():
        n = int(df["week_start"].duplicated().sum())
        raise ValueError(f"{source_name}: duplicated week_start rows = {n}")

    return df.sort_values("week_start").reset_index(drop=True)


def read_parquet_checked(path: Path, source_name: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"{source_name} not found: {path}")
    df = pd.read_parquet(path)
    return ensure_week_start(df, source_name)


def choose_single_match(base_dir: Path, pattern: str, source_name: str) -> Path:
    matches = sorted(base_dir.glob(pattern))
    if not matches:
        raise FileNotFoundError(
            f"{source_name}: no files matched pattern '{pattern}' under {base_dir}"
        )
    if len(matches) > 1:
        print(
            f"[warn] {source_name}: multiple matches found under {base_dir}; "
            f"using latest lexicographic match: {matches[-1].name}"
        )
    return matches[-1]


def build_paths(island: str, processed_dir: Path, interim_dir: Path) -> Dict[str, Path]:
    code = island_code(island)

    paths = {
        "deaths": choose_single_match(
            processed_dir / island / "deaths",
            f"deaths_weekly_{code}_*.parquet",
            "deaths",
        ),
        "weather": choose_single_match(
            processed_dir / island / "weather",
            f"weather_weekly_{code}_*.parquet",
            "weather",
        ),
        # Visibility now lives in interim, produced by step4 of the visibility pipeline.
        "visibility": choose_single_match(
            interim_dir / island / "visibility" / "step4_weekly",
            f"visibility_weekly_{code}_*.parquet",
            "visibility",
        ),
        "airq": choose_single_match(
            processed_dir / island / "air_quality",
            f"weekly_{code}_*.parquet",
            "air_quality",
        ),
        "cap": choose_single_match(
            processed_dir / island / "cap",
            f"cap_weekly_{code}_*.parquet",
            "cap",
        ),
        "heliyon": interim_dir / "heliyon" / f"heliyon_calima_dai_flag_weekly_{code}_2015w52_2024.parquet",
    }

    missing = [k for k, v in paths.items() if not Path(v).exists()]
    if missing:
        details = {k: str(paths[k]) for k in missing}
        raise FileNotFoundError(f"Missing required input(s): {details}")

    return {k: Path(v) for k, v in paths.items()}


def select_cap_columns(df: pd.DataFrame) -> pd.DataFrame:
    wanted = [
        "week_start",
        "cap_heat_level_max_week",
        "cap_dust_level_max_week",
        "cap_heat_yellow_plus_week",
        "cap_dust_yellow_plus_week",
        "cap_coverage_week",
    ]
    missing = [c for c in wanted if c not in df.columns]
    if missing:
        raise ValueError(f"cap feed missing expected columns: {missing}")
    return df[wanted].copy()


def select_heliyon_columns(df: pd.DataFrame, calendar: pd.DataFrame) -> pd.DataFrame:
    wanted = ["week_start", "calima_dai_flag"]
    missing = [c for c in wanted if c not in df.columns]
    if missing:
        raise ValueError(f"heliyon feed missing expected columns: {missing}")

    out = df[wanted].copy()
    out["calima_dai_flag"] = out["calima_dai_flag"].astype("string")

    # Merge to full calendar and fill no-data weeks as blue.
    out = calendar[["week_start"]].merge(out, on="week_start", how="left")
    out["calima_dai_flag"] = out["calima_dai_flag"].fillna("blue")
    return out


def prepare_generic_feed(df: pd.DataFrame, source_name: str) -> pd.DataFrame:
    """
    Keep all columns from a generic feed except obvious helper duplicates,
    preserving week_start.
    """
    df = df.copy()
    drop_cols: List[str] = []
    for col in df.columns:
        if col == "week_start":
            continue
        if col.lower() in {"year", "island", "code", "island_code"}:
            drop_cols.append(col)
    if drop_cols:
        df = df.drop(columns=drop_cols)
    return df


def select_weather_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep the core weekly weather variables requested by the user.
    """
    wanted = [
        "week_start",
        "temp_c_mean",
        "tmax_c_mean",
        "tmin_c_mean",
        "humidity_mean",
        "pressure_hpa_mean",
        "wind_ms_mean",
    ]
    missing = [c for c in wanted if c not in df.columns]
    if missing:
        raise ValueError(
            f"weather feed missing expected columns: {missing} | "
            f"available: {list(df.columns)}"
        )
    return df[wanted].copy()


def merge_master(calendar: pd.DataFrame, feeds: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    master = calendar.copy()
    merge_order = ["deaths", "weather", "visibility", "airq", "cap", "heliyon"]

    for name in merge_order:
        master = master.merge(feeds[name], on="week_start", how="left")

    return master


def add_metadata(master: pd.DataFrame, island: str) -> pd.DataFrame:
    master = master.copy()
    master["island"] = island
    master["island_code"] = island_code(island)
    master["year"] = master["week_start"].dt.year

    first_cols = ["week_start", "year", "island", "island_code"]
    other_cols = [c for c in master.columns if c not in first_cols]
    return master[first_cols + other_cols]


def validate_master(master: pd.DataFrame, island: str) -> None:
    if master["week_start"].isna().any():
        raise ValueError(f"{island}: master contains null week_start values")

    dupes = int(master.duplicated(subset=["week_start"]).sum())
    if dupes:
        raise ValueError(f"{island}: master contains duplicated week_start rows = {dupes}")

    expected_weeks = len(
        pd.date_range(start=ANALYSIS_START, end=ANALYSIS_END, freq="W-MON")
    )
    if len(master) != expected_weeks:
        raise ValueError(
            f"{island}: expected {expected_weeks} weeks, got {len(master)}"
        )


def build_master(island: str, processed_dir: Path, interim_dir: Path) -> Path:
    code = island_code(island)
    paths = build_paths(island=island, processed_dir=processed_dir, interim_dir=interim_dir)

    deaths = prepare_generic_feed(read_parquet_checked(paths["deaths"], "deaths"), "deaths")
    weather = select_weather_columns(read_parquet_checked(paths["weather"], "weather"))
    visibility = prepare_generic_feed(read_parquet_checked(paths["visibility"], "visibility"), "visibility")
    airq = prepare_generic_feed(read_parquet_checked(paths["airq"], "air_quality"), "air_quality")
    cap = select_cap_columns(read_parquet_checked(paths["cap"], "cap"))

    deaths = deaths[
        (deaths["week_start"] >= ANALYSIS_START) & (deaths["week_start"] <= ANALYSIS_END)
    ].copy()

    calendar = week_calendar(
        start=str(ANALYSIS_START.date()),
        end=str(ANALYSIS_END.date()),
    )

    heliyon_raw = read_parquet_checked(paths["heliyon"], "heliyon")
    heliyon = select_heliyon_columns(heliyon_raw, calendar=calendar)

    feeds = {
        "deaths": deaths,
        "weather": weather,
        "visibility": visibility,
        "airq": airq,
        "cap": cap,
        "heliyon": heliyon,
    }

    master = merge_master(calendar=calendar, feeds=feeds)
    master = add_metadata(master, island=island)
    validate_master(master, island=island)

    outdir = processed_dir / island / "master"
    outdir.mkdir(parents=True, exist_ok=True)

    start_year = int(master["week_start"].min().year)
    end_year = int(master["week_start"].max().year)
    outpath = outdir / f"master_{code}_{start_year}_{end_year}.parquet"
    master.to_parquet(outpath, index=False)

    print("Done.")
    print(f"Island: {island} ({code})")
    print(f"Output: {outpath}")
    print(f"Rows: {len(master):,}")
    print(f"Range: {master['week_start'].min()} -> {master['week_start'].max()}")
    print("Null counts (selected columns):")

    selected = [
        c
        for c in [
            "deaths_week",
            "PM10",
            "PM2.5",
            "temp_c_mean",
            "SO2",
            "NO2",
            "O3",
            "pressure_hpa_mean",
            "cap_heat_yellow_plus_week",
            "cap_dust_yellow_plus_week",
            "calima_dai_flag",
            "low_vis_any_week",
            "vis_min_m_week",
        ]
        if c in master.columns
    ]
    if selected:
        print(master[selected].isna().sum())
    else:
        print("No selected diagnostic columns found.")

    return outpath


def main() -> None:
    args = parse_args()

    processed_dir = Path(args.processed_dir)
    interim_dir = Path(args.interim_dir)

    if args.all:
        outputs: List[Path] = []
        for island in ISLANDS:
            print(f"\n=== Building master for {island} ===")
            outputs.append(
                build_master(
                    island=island,
                    processed_dir=processed_dir,
                    interim_dir=interim_dir,
                )
            )
        print("\nBuilt master files:")
        for p in outputs:
            print(f" - {p}")
    else:
        build_master(
            island=args.island,
            processed_dir=processed_dir,
            interim_dir=interim_dir,
        )


if __name__ == "__main__":
    main()