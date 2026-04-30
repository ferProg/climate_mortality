from __future__ import annotations

import argparse
import logging
from pathlib import Path

import cdsapi
import pandas as pd
import xarray as xr


LOGGER = logging.getLogger(__name__)

DATASET_NAME = "cams-global-atmospheric-composition-forecasts"
EXPECTED_OBS_PER_DAY = 4  # 00, 06, 12, 18


ISLAND_CONFIG = {
    "tenerife": {
        "code": "tfe",
        "area": [29.0, -17.0, 27.5, -16.0],  # [north, west, south, east]
    },
    "gran_canaria": {
        "code": "gcan",
        "area": [28.5, -16.2, 27.5, -15.2],
    },
    "lanzarote": {
        "code": "lzt",
        "area": [29.5, -14.2, 28.7, -13.1],
    },
    "fuerteventura": {
        "code": "ftv",
        "area": [29.0, -14.6, 27.8, -13.5],
    },
    "la_palma": {
        "code": "lpa",
        "area": [29.1, -18.2, 28.4, -17.7],
    },
    "gomera": {
        "code": "gom",
        "area": [28.3, -17.45, 27.95, -17.05],
    },
    "hierro": {
        "code": "hie",
        "area": [27.95, -18.25, 27.55, -17.85],
    },
}


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download CAMS global PM10/PM2.5 and aggregate to daily/weekly."
    )
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--island", required=True, choices=sorted(ISLAND_CONFIG.keys()))
    parser.add_argument("--chunk-months", type=int, default=1, choices=[1, 2, 3, 6, 12])
    parser.add_argument("--also-csv", action="store_true")
    parser.add_argument("--keep-nc", action="store_true")
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip download and read the existing 6-hourly parquet from raw_dir.",
    )
    return parser.parse_args()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def get_project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def build_paths(project_root: Path, island: str, code: str, start: str, end: str) -> dict[str, Path]:
    raw_dir = project_root / "data" / "raw" / island / "air_quality"
    processed_dir = project_root / "data" / "processed" / island / "air_quality"

    ensure_dir(raw_dir)
    ensure_dir(processed_dir)

    return {
        "raw_dir": raw_dir,
        "processed_dir": processed_dir,
        "six_hourly": raw_dir / f"cams_pm_6hourly_{code}_{start}_{end}.parquet",
        "daily": processed_dir / f"cams_pm_daily_{code}_{start}_{end}.parquet",
        "weekly": processed_dir / f"cams_pm_weekly_{code}_{start}_{end}.parquet",
    }


def make_chunks(start: pd.Timestamp, end: pd.Timestamp, chunk_months: int) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    chunks: list[tuple[pd.Timestamp, pd.Timestamp]] = []
    current = start.normalize()

    while current <= end.normalize():
        next_start = (current + pd.DateOffset(months=chunk_months)).normalize()
        chunk_end = min(end.normalize(), next_start - pd.Timedelta(days=1))
        chunks.append((current, chunk_end))
        current = next_start

    return chunks


def download_cams_chunk(
    client: cdsapi.Client,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    area: list[float],
    target_path: Path,
) -> Path:
    request = {
        "variable": ["particulate_matter_10um", "particulate_matter_2.5um"],
        "date": [f"{start_date.date()}/{end_date.date()}"],
        "time": ["00:00", "06:00", "12:00", "18:00"],
        "type": ["analysis"],
        "format": "netcdf",
        "area": area,
    }

    LOGGER.info(
        "Downloading CAMS %s..%s -> %s",
        start_date.date(),
        end_date.date(),
        target_path.name,
    )
    client.retrieve(DATASET_NAME, request, str(target_path))
    return target_path


def nc_to_six_hourly_df(nc_path: Path) -> pd.DataFrame:
    ds = xr.open_dataset(nc_path)

    required_vars = {"pm10", "pm2p5"}
    missing = required_vars - set(ds.data_vars)
    if missing:
        raise ValueError(f"Missing vars in {nc_path.name}: {sorted(missing)}")

    df = (
        ds[["pm10", "pm2p5"]]
        .mean(dim=["latitude", "longitude"])
        .to_dataframe()
        .reset_index()
    )

    if "valid_time" not in df.columns:
        raise ValueError(f"'valid_time' not found in {nc_path.name}")

    df["valid_time"] = pd.to_datetime(df["valid_time"])

    df = df.drop(
        columns=[c for c in ["forecast_period", "forecast_reference_time"] if c in df.columns],
        errors="ignore",
    )

    df = df.rename(
        columns={
            "valid_time": "datetime",
            "pm10": "cams_pm10",
            "pm2p5": "cams_pm25",
        }
    )

    df = df[["datetime", "cams_pm10", "cams_pm25"]].copy()

        # CAMS global PM comes in kg/m3 -> convert to µg/m3
    df["cams_pm10"] = df["cams_pm10"] * 1_000_000_000
    df["cams_pm25"] = df["cams_pm25"] * 1_000_000_000
    
    df = df.sort_values("datetime").drop_duplicates(subset=["datetime"]).reset_index(drop=True)

    return df


def concat_frames(frames: list[pd.DataFrame]) -> pd.DataFrame:
    if not frames:
        raise RuntimeError("No CAMS dataframes produced.")

    df = pd.concat(frames, ignore_index=True)
    df = df.sort_values("datetime").drop_duplicates(subset=["datetime"]).reset_index(drop=True)
    return df


def build_daily(df_6h: pd.DataFrame) -> pd.DataFrame:
    df = df_6h.copy()
    df["date"] = df["datetime"].dt.floor("D")

    daily = (
        df.groupby("date", as_index=False)
        .agg(
            cams_pm10_mean_day=("cams_pm10", "mean"),
            cams_pm10_max_day=("cams_pm10", "max"),
            cams_pm10_min_day=("cams_pm10", "min"),
            cams_pm10_p90_day=("cams_pm10", lambda s: s.quantile(0.90)),
            cams_pm25_mean_day=("cams_pm25", "mean"),
            cams_pm25_max_day=("cams_pm25", "max"),
            cams_pm25_min_day=("cams_pm25", "min"),
            cams_pm25_p90_day=("cams_pm25", lambda s: s.quantile(0.90)),
            n_obs_day=("datetime", "size"),
        )
        .sort_values("date")
        .reset_index(drop=True)
    )

    daily["coverage__cams_pm_day"] = daily["n_obs_day"] / EXPECTED_OBS_PER_DAY
    daily["cams_pm10_high_day"] = daily["cams_pm10_p90_day"] >= daily["cams_pm10_p90_day"].quantile(0.90)
    daily["cams_pm25_high_day"] = daily["cams_pm25_p90_day"] >= daily["cams_pm25_p90_day"].quantile(0.90)

    return daily


def build_weekly(df_daily: pd.DataFrame) -> pd.DataFrame:
    df = df_daily.copy()
    df["week_start"] = df["date"] - pd.to_timedelta(df["date"].dt.weekday, unit="D")

    weekly = (
        df.groupby("week_start", as_index=False)
        .agg(
            cams_pm10_mean_week=("cams_pm10_mean_day", "mean"),
            cams_pm10_max_week=("cams_pm10_max_day", "max"),
            cams_pm10_min_week=("cams_pm10_min_day", "min"),
            cams_pm10_p90_week=("cams_pm10_p90_day", "max"),
            cams_pm25_mean_week=("cams_pm25_mean_day", "mean"),
            cams_pm25_max_week=("cams_pm25_max_day", "max"),
            cams_pm25_min_week=("cams_pm25_min_day", "min"),
            cams_pm25_p90_week=("cams_pm25_p90_day", "max"),
            n_days_week=("date", "size"),
            n_obs_week=("n_obs_day", "sum"),
            coverage__cams_pm_week=("coverage__cams_pm_day", "mean"),
            cams_pm10_high_days_week=("cams_pm10_high_day", "sum"),
            cams_pm25_high_days_week=("cams_pm25_high_day", "sum"),
        )
        .sort_values("week_start")
        .reset_index(drop=True)
    )

    weekly["cams_pm10_high_week"] = weekly["cams_pm10_p90_week"] >= weekly["cams_pm10_p90_week"].quantile(0.90)
    weekly["cams_pm25_high_week"] = weekly["cams_pm25_p90_week"] >= weekly["cams_pm25_p90_week"].quantile(0.90)

    return weekly


def save_table(df: pd.DataFrame, path: Path, also_csv: bool) -> None:
    ensure_dir(path.parent)
    df.to_parquet(path, index=False)
    LOGGER.info("Saved %s rows -> %s", len(df), path)

    if also_csv:
        csv_path = path.with_suffix(".csv")
        df.to_csv(csv_path, index=False)
        LOGGER.info("Saved CSV -> %s", csv_path)


def main() -> None:
    setup_logging()
    args = parse_args()

    start = pd.Timestamp(args.start)
    end = pd.Timestamp(args.end)

    if end < start:
        raise ValueError("--end must be >= --start")

    island_cfg = ISLAND_CONFIG[args.island]
    code = island_cfg["code"]
    area = island_cfg["area"]

    project_root = get_project_root()
    paths = build_paths(project_root, args.island, code, args.start, args.end)

    LOGGER.info(
        "START island=%s code=%s %s..%s",
        args.island,
        code,
        start.date(),
        end.date(),
    )
    LOGGER.info("Area bbox=%s", area)

    if args.skip_download:
        LOGGER.info("--skip-download set: reading existing 6-hourly parquet.")
        existing = paths["six_hourly"]
        if not existing.exists():
            raise FileNotFoundError(
                f"--skip-download requires the 6-hourly parquet to exist: {existing}"
            )
        df_6h = pd.read_parquet(existing)
        LOGGER.info("Loaded %s rows from %s", len(df_6h), existing)
    else:
        client = cdsapi.Client()
        chunks = make_chunks(start, end, args.chunk_months)
        LOGGER.info("Downloading %s chunk(s)", len(chunks))

        nc_paths: list[Path] = []
        frames_6h: list[pd.DataFrame] = []

        for chunk_start, chunk_end in chunks:
            nc_name = f"cams_pm_{code}_{chunk_start.date()}_{chunk_end.date()}.nc"
            nc_path = paths["raw_dir"] / nc_name

            download_cams_chunk(
                client=client,
                start_date=chunk_start,
                end_date=chunk_end,
                area=area,
                target_path=nc_path,
            )
            nc_paths.append(nc_path)

            df_chunk = nc_to_six_hourly_df(nc_path)
            frames_6h.append(df_chunk)

        df_6h = concat_frames(frames_6h)

        if not args.keep_nc:
            for path in nc_paths:
                try:
                    path.unlink(missing_ok=True)
                    LOGGER.info("Deleted temp NetCDF -> %s", path.name)
                except Exception as exc:
                    LOGGER.warning("Could not delete %s: %s", path.name, exc)

    df_daily = build_daily(df_6h)
    df_weekly = build_weekly(df_daily)

    save_table(df_6h, paths["six_hourly"], also_csv=args.also_csv)
    save_table(df_daily, paths["daily"], also_csv=args.also_csv)
    save_table(df_weekly, paths["weekly"], also_csv=args.also_csv)

    LOGGER.info(
        "DONE 6h=%s daily=%s weekly=%s",
        len(df_6h),
        len(df_daily),
        len(df_weekly),
    )


if __name__ == "__main__":
    main()