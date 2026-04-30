from __future__ import annotations

import argparse
import logging
from pathlib import Path

import cdsapi
import pandas as pd
import xarray as xr


LOGGER = logging.getLogger(__name__)

DATASET_NAME = "cams-global-reanalysis-eac4"
EXPECTED_OBS_PER_DAY = 4  # 00, 06, 12, 18

# EAC4 grid resolution is 0.75° — bounding boxes must span at least 0.75°
# in both dimensions. Areas below that threshold collapse to a single point.
# All boxes here are padded to at least 1.5° x 1.5°.
ISLAND_CONFIG = {
    "tenerife": {
        "code": "tfe",
        "area": [29.0, -17.0, 27.5, -16.0],  # [north, west, south, east]
    },
    "gran_canaria": {
        "code": "gcan",
        "area": [28.5, -16.5, 27.0, -15.0],
    },
    "lanzarote": {
        "code": "lzt",
        "area": [30.0, -14.5, 28.5, -13.0],
    },
    "fuerteventura": {
        "code": "ftv",
        "area": [29.5, -15.0, 28.0, -13.5],
    },
    "la_palma": {
        "code": "lpa",
        "area": [29.5, -18.5, 28.0, -17.0],
    },
    "gomera": {
        "code": "gom",
        "area": [29.0, -18.0, 27.5, -16.5],
    },
    "hierro": {
        "code": "hie",
        "area": [28.5, -18.5, 27.0, -17.0],
    },
}

# EAC4 variable names -> output column names (µg/m³ after unit conversion)
# EAC4 delivers all concentrations in kg/m³ -> multiply by 1e9 to get µg/m³
VARIABLES = {
    "particulate_matter_10um": ("pm10", "PM10"),
    "particulate_matter_2.5um": ("pm2p5", "PM2.5"),
    "sulphur_dioxide": ("so2", "SO2"),
    "nitrogen_dioxide": ("no2", "NO2"),
    "ozone": ("go3", "O3"),  # EAC4 uses 'go3' for ozone
}


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download CAMS EAC4 reanalysis (PM10, PM2.5, SO2, NO2, O3) "
            "and aggregate to daily/weekly parquet. "
            "Output column names match the Gobierno Canarias format "
            "so the master build picks them up without changes."
        )
    )
    parser.add_argument("--island", required=True, choices=sorted(ISLAND_CONFIG.keys()))
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    parser.add_argument(
        "--chunk-months",
        type=int,
        default=3,
        choices=[1, 2, 3, 6, 12],
        help="Months per download chunk (default 3 — safe for EAC4 queue limits)",
    )
    parser.add_argument("--also-csv", action="store_true")
    parser.add_argument("--keep-nc", action="store_true")
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip download and read existing 6-hourly parquet from raw dir.",
    )
    return parser.parse_args()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def get_project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def build_paths(
    project_root: Path, island: str, code: str, start: str, end: str
) -> dict[str, Path]:
    raw_dir = project_root / "data" / "raw" / island / "air_quality"
    processed_dir = project_root / "data" / "processed" / island / "air_quality"

    ensure_dir(raw_dir)
    ensure_dir(processed_dir)

    return {
        "raw_dir": raw_dir,
        "processed_dir": processed_dir,
        "six_hourly": raw_dir / f"eac4_6hourly_{code}_{start}_{end}.parquet",
        "daily": processed_dir / f"eac4_daily_{code}_{start}_{end}.parquet",
        "weekly": processed_dir / f"weekly_{code}_{start[:4]}_{end[:4]}_eac4.parquet",
    }


def make_chunks(
    start: pd.Timestamp, end: pd.Timestamp, chunk_months: int
) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    chunks: list[tuple[pd.Timestamp, pd.Timestamp]] = []
    current = start.normalize()

    while current <= end.normalize():
        next_start = (current + pd.DateOffset(months=chunk_months)).normalize()
        chunk_end = min(end.normalize(), next_start - pd.Timedelta(days=1))
        chunks.append((current, chunk_end))
        current = next_start

    return chunks


def download_eac4_chunk(
    client: cdsapi.Client,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    area: list[float],
    target_path: Path,
) -> Path:
    request = {
        "variable": list(VARIABLES.keys()),
        "date": f"{start_date.date()}/{end_date.date()}",
        "time": ["00:00", "06:00", "12:00", "18:00"],
        "format": "netcdf",
        "area": area,
    }

    LOGGER.info(
        "Downloading EAC4 %s..%s -> %s",
        start_date.date(),
        end_date.date(),
        target_path.name,
    )
    client.retrieve(DATASET_NAME, request, str(target_path))
    return target_path


def nc_to_six_hourly_df(nc_path: Path) -> pd.DataFrame:
    ds = xr.open_dataset(nc_path)

    # Build rename map: xarray var name -> output column name
    nc_var_names = {nc_name for _, (nc_name, _) in VARIABLES.items()}
    available = nc_var_names & set(ds.data_vars)
    missing = nc_var_names - available
    if missing:
        LOGGER.warning("Variables missing in %s: %s", nc_path.name, sorted(missing))

    rename_map: dict[str, str] = {}
    for _, (nc_name, col_name) in VARIABLES.items():
        if nc_name in ds.data_vars:
            rename_map[nc_name] = col_name

    df = (
        ds[list(rename_map.keys())]
        .mean(dim=["latitude", "longitude"])
        .to_dataframe()
        .reset_index()
    )

    # EAC4 time dimension is called 'time' (not 'valid_time')
    time_col = next((c for c in ["time", "valid_time"] if c in df.columns), None)
    if time_col is None:
        raise ValueError(f"No time column found in {nc_path.name}. Columns: {df.columns.tolist()}")

    df = df.rename(columns={time_col: "datetime"})
    df["datetime"] = pd.to_datetime(df["datetime"])

    df = df.drop(
        columns=[c for c in df.columns if c not in ["datetime"] + list(rename_map.keys())],
        errors="ignore",
    )
    df = df.rename(columns=rename_map)

    # Convert kg/m³ -> µg/m³
    for col in rename_map.values():
        if col in df.columns:
            df[col] = df[col] * 1_000_000_000

    output_cols = ["datetime"] + [col for col in rename_map.values() if col in df.columns]
    df = df[output_cols].copy()
    df = df.sort_values("datetime").drop_duplicates(subset=["datetime"]).reset_index(drop=True)

    return df


def concat_frames(frames: list[pd.DataFrame]) -> pd.DataFrame:
    if not frames:
        raise RuntimeError("No EAC4 dataframes produced.")
    df = pd.concat(frames, ignore_index=True)
    df = df.sort_values("datetime").drop_duplicates(subset=["datetime"]).reset_index(drop=True)
    return df


def build_daily(df_6h: pd.DataFrame) -> pd.DataFrame:
    df = df_6h.copy()
    df["date"] = df["datetime"].dt.floor("D")

    pollutant_cols = [c for c in df.columns if c not in ["datetime", "date"]]

    agg_dict: dict[str, tuple] = {"n_obs_day": ("datetime", "size")}
    for col in pollutant_cols:
        agg_dict[f"{col}_mean_day"] = (col, "mean")
        agg_dict[f"{col}_max_day"] = (col, "max")
        agg_dict[f"{col}_min_day"] = (col, "min")

    daily = (
        df.groupby("date", as_index=False)
        .agg(**agg_dict)
        .sort_values("date")
        .reset_index(drop=True)
    )

    daily["coverage_day"] = daily["n_obs_day"] / EXPECTED_OBS_PER_DAY
    return daily


def build_weekly(df_daily: pd.DataFrame) -> pd.DataFrame:
    df = df_daily.copy()
    df["week_start"] = df["date"] - pd.to_timedelta(df["date"].dt.weekday, unit="D")

    mean_cols = [c for c in df.columns if c.endswith("_mean_day")]
    max_cols = [c for c in df.columns if c.endswith("_max_day")]
    min_cols = [c for c in df.columns if c.endswith("_min_day")]

    agg_dict: dict[str, tuple] = {
        "n_days_week": ("date", "size"),
        "n_obs_week": ("n_obs_day", "sum"),
        "coverage_week": ("coverage_day", "mean"),
    }

    # Weekly mean of daily means -> becomes the main column (e.g. PM10, PM2.5)
    for col in mean_cols:
        base = col.replace("_mean_day", "")
        agg_dict[base] = (col, "mean")          # e.g. PM10, PM2.5, SO2, NO2, O3
        agg_dict[f"{base}_max_week"] = (col.replace("_mean_day", "_max_day"), "max")
        agg_dict[f"{base}_min_week"] = (col.replace("_mean_day", "_min_day"), "min")

    weekly = (
        df.groupby("week_start", as_index=False)
        .agg(**agg_dict)
        .sort_values("week_start")
        .reset_index(drop=True)
    )

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
        "START island=%s code=%s %s..%s dataset=%s",
        args.island, code, start.date(), end.date(), DATASET_NAME,
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
        LOGGER.info("Downloading %s chunk(s) of %s month(s) each", len(chunks), args.chunk_months)

        nc_paths: list[Path] = []
        frames_6h: list[pd.DataFrame] = []

        for chunk_start, chunk_end in chunks:
            nc_name = f"eac4_{code}_{chunk_start.date()}_{chunk_end.date()}.nc"
            nc_path = paths["raw_dir"] / nc_name

            download_eac4_chunk(
                client=client,
                start_date=chunk_start,
                end_date=chunk_end,
                area=area,
                target_path=nc_path,
            )
            nc_paths.append(nc_path)

            df_chunk = nc_to_six_hourly_df(nc_path)
            frames_6h.append(df_chunk)
            LOGGER.info("Chunk processed: %s rows", len(df_chunk))

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

    LOGGER.info(
        "Columns in weekly output: %s",
        [c for c in df_weekly.columns if c not in ["week_start", "n_days_week", "n_obs_week", "coverage_week"]],
    )

    save_table(df_6h, paths["six_hourly"], also_csv=args.also_csv)
    save_table(df_daily, paths["daily"], also_csv=args.also_csv)
    save_table(df_weekly, paths["weekly"], also_csv=args.also_csv)

    LOGGER.info(
        "DONE 6h=%s daily=%s weekly=%s",
        len(df_6h), len(df_daily), len(df_weekly),
    )


if __name__ == "__main__":
    main()
