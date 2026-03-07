from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from src.utils.constants import island_code
from src.utils.dates import to_week_start_from_datetime


POLLUTANT_COLUMNS = ["PM10", "PM2.5", "SO2", "NO2", "O3"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build weekly air-quality parquet for one island from daily CSV."
    )
    parser.add_argument(
        "--island",
        required=True,
        choices=[
            "tenerife",
            "gran_canaria",
            "lanzarote",
            "fuerteventura",
            "la_palma",
            "gomera",
            "hierro",
        ],
        help="Canonical island name, e.g. tenerife",
    )
    parser.add_argument(
        "--daily-dir",
        default="data/interim/air_q",
        help="Directory containing daily_<code>.csv",
    )
    parser.add_argument(
        "--processed-dir",
        default="data/processed",
        help="Base processed directory",
    )
    return parser.parse_args()


def build_paths(island: str, daily_dir: Path, processed_dir: Path) -> tuple[str, Path, Path]:
    code = island_code(island)
    daily_path = daily_dir / f"daily_{code}.csv"
    outdir = processed_dir / island / "air_quality"
    outdir.mkdir(parents=True, exist_ok=True)
    return code, daily_path, outdir


def validate_daily(df: pd.DataFrame, daily_path: Path) -> None:
    required = {"date", "year", *POLLUTANT_COLUMNS, "station"}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in {daily_path}: {sorted(missing)}")

    if df["date"].isna().any():
        raise ValueError("Daily dataset contains NaT in 'date'")

    if df["date"].duplicated().any():
        n_dupes = int(df["date"].duplicated().sum())
        raise ValueError(f"Daily dataset contains duplicated dates: {n_dupes}")


def aggregate_weekly(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["week_start"] = to_week_start_from_datetime(df["date"])

    weekly = (
        df.groupby("week_start", as_index=False)
        .agg(
            PM10=("PM10", "mean"),
            **{
                "PM2.5": ("PM2.5", "mean"),
                "SO2": ("SO2", "mean"),
                "NO2": ("NO2", "mean"),
                "O3": ("O3", "mean"),
            },
            days_with_pm10=("PM10", lambda s: int(s.notna().sum())),
            days_missing_pm10=("PM10", lambda s: int(s.isna().sum())),
        )
        .sort_values("week_start")
        .reset_index(drop=True)
    )

    weekly["year"] = weekly["week_start"].dt.year
    weekly = weekly[
        [
            "week_start",
            "year",
            "PM10",
            "PM2.5",
            "SO2",
            "NO2",
            "O3",
            "days_with_pm10",
            "days_missing_pm10",
        ]
    ]
    return weekly


def build_weekly_airq(island: str, daily_dir: Path, processed_dir: Path) -> Path:
    code, daily_path, outdir = build_paths(island, daily_dir, processed_dir)

    if not daily_path.exists():
        raise FileNotFoundError(f"Daily CSV not found: {daily_path}")

    df = pd.read_csv(daily_path, parse_dates=["date"])
    validate_daily(df, daily_path)

    weekly = aggregate_weekly(df)

    start_year = int(df["year"].min())
    end_year = int(df["year"].max())
    outpath = outdir / f"weekly_{code}_{start_year}_{end_year}.parquet"
    weekly.to_parquet(outpath, index=False)

    print("Done.")
    print(f"Input: {daily_path}")
    print(f"Output: {outpath}")
    print(f"Rows: {len(weekly):,}")
    print(f"Range: {weekly['week_start'].min()} -> {weekly['week_start'].max()}")
    print("Null counts:")
    print(weekly[["PM10", "PM2.5", "SO2", "NO2", "O3"]].isna().sum())

    return outpath


def main() -> None:
    args = parse_args()
    build_weekly_airq(
        island=args.island,
        daily_dir=Path(args.daily_dir),
        processed_dir=Path(args.processed_dir),
    )


if __name__ == "__main__":
    main()
