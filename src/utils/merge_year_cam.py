from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from src.utils.constants import island_code


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Append CAMS weekly PM data for a target year into historical weekly air_quality parquet."
    )
    p.add_argument("--island", required=True, help="Island name, e.g. tenerife")
    p.add_argument("--start-date", required=True, help="YYYY-MM-DD for CAMS file start")
    p.add_argument("--end-date", required=True, help="YYYY-MM-DD for CAMS file end")
    p.add_argument(
        "--processed-dir",
        default="data/processed",
        help="Base processed directory (default: data/processed)",
    )
    p.add_argument(
        "--historical-file",
        default=None,
        help="Optional override path to historical weekly parquet",
    )
    p.add_argument(
        "--cams-file",
        default=None,
        help="Optional override path to CAMS weekly parquet",
    )
    p.add_argument(
        "--output-file",
        default=None,
        help="Optional override path for merged output parquet",
    )
    return p.parse_args()


def ensure_week_start(df: pd.DataFrame, source_name: str) -> pd.DataFrame:
    if "week_start" not in df.columns:
        raise ValueError(f"{source_name}: missing required column 'week_start'")

    out = df.copy()
    out["week_start"] = pd.to_datetime(out["week_start"], errors="raise")
    out = out.sort_values("week_start").reset_index(drop=True)
    return out


def build_default_paths(
    island: str,
    code: str,
    processed_dir: Path,
    start_date: str,
    end_date: str,
) -> tuple[Path, Path, Path]:
    island_dir = processed_dir / island / "air_quality"

    historical_fp = island_dir / f"weekly_{code}_2016_2024.parquet"
    cams_fp = island_dir / f"cams_pm_weekly_{code}_{start_date}_{end_date}.parquet"
    output_fp = island_dir / f"weekly_{code}_2016_2025.parquet"

    return historical_fp, cams_fp, output_fp


def map_cams_to_historical_schema(df_cams: pd.DataFrame) -> pd.DataFrame:
    """
    Map CAMS weekly schema to the historical weekly_tfe_2016_2024.parquet schema.

    Historical schema:
    ['week_start', 'year', 'PM10', 'PM2.5', 'SO2', 'NO2', 'O3', 'days_with_pm10', 'days_missing_pm10']

    CAMS source:
    ['week_start', 'cams_pm10_mean_week', ..., 'cams_pm25_mean_week', ...]
    """
    required = ["week_start", "cams_pm10_mean_week", "cams_pm25_mean_week"]
    missing = [c for c in required if c not in df_cams.columns]
    if missing:
        raise ValueError(f"CAMS weekly missing required columns: {missing}")

    out = pd.DataFrame(
        {
            "week_start": pd.to_datetime(df_cams["week_start"], errors="raise"),
            "year": pd.to_datetime(df_cams["week_start"], errors="raise").dt.year.astype("int32"),
            "PM10": pd.to_numeric(df_cams["cams_pm10_mean_week"], errors="coerce").astype("float64"),
            "PM2.5": pd.to_numeric(df_cams["cams_pm25_mean_week"], errors="coerce").astype("float64"),
            "SO2": pd.Series(float("nan"), index=df_cams.index, dtype="float64"),
            "NO2": pd.Series(float("nan"), index=df_cams.index, dtype="float64"),
            "O3": pd.Series(float("nan"), index=df_cams.index, dtype="float64"),
            "days_with_pm10": pd.Series(float("nan"), index=df_cams.index, dtype="float64"),
            "days_missing_pm10": pd.Series(float("nan"), index=df_cams.index, dtype="float64"),
        }
    )

    out = out.sort_values("week_start").reset_index(drop=True)
    return out


def main() -> None:
    args = parse_args()

    processed_dir = Path(args.processed_dir)
    code = island_code(args.island)

    hist_default, cams_default, out_default = build_default_paths(
        island=args.island,
        code=code,
        processed_dir=processed_dir,
        start_date=args.start_date,
        end_date=args.end_date,
    )

    historical_fp = Path(args.historical_file) if args.historical_file else hist_default
    cams_fp = Path(args.cams_file) if args.cams_file else cams_default
    output_fp = Path(args.output_file) if args.output_file else out_default

    print(f"HISTORICAL -> {historical_fp}")
    print(f"CAMS       -> {cams_fp}")
    print(f"OUTPUT     -> {output_fp}")

    if not historical_fp.exists():
        raise FileNotFoundError(f"Historical file not found: {historical_fp}")
    if not cams_fp.exists():
        raise FileNotFoundError(f"CAMS file not found: {cams_fp}")

    df_hist = pd.read_parquet(historical_fp)
    df_hist = ensure_week_start(df_hist, "historical")

    df_cams = pd.read_parquet(cams_fp)
    df_cams = ensure_week_start(df_cams, "cams_weekly")
    df_cams_mapped = map_cams_to_historical_schema(df_cams)

    expected_cols = [
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

    hist_missing = [c for c in expected_cols if c not in df_hist.columns]
    if hist_missing:
        raise ValueError(f"Historical parquet missing expected columns: {hist_missing}")

    df_hist = df_hist[expected_cols].copy()
    df_cams_mapped = df_cams_mapped[expected_cols].copy()

    merged = pd.concat([df_hist, df_cams_mapped], ignore_index=True)
    merged = merged.sort_values("week_start").drop_duplicates(subset=["week_start"], keep="last")
    merged = merged.reset_index(drop=True)

    output_fp.parent.mkdir(parents=True, exist_ok=True)
    merged.to_parquet(output_fp, index=False)

    print()
    print("Done.")
    print(f"Rows historical : {len(df_hist):,}")
    print(f"Rows CAMS mapped: {len(df_cams_mapped):,}")
    print(f"Rows merged     : {len(merged):,}")
    print(f"Range merged    : {merged['week_start'].min()} -> {merged['week_start'].max()}")
    print()
    print("Null counts:")
    print(merged.isna().sum())

    # Uncomment later if you want to remove the old historical parquet after validating the new one.
    historical_fp.unlink()

    #####SANEAMIENTO ############
    # python -c "import pandas as pd; p=r'data/processed/tenerife/air_quality/weekly_tfe_2016_2025.parquet'; df=pd.read_parquet(p); print('shape =', df.shape); print('range =', df['week_start'].min(), '->', df['week_start'].max()); print('dup week_start =', df['week_start'].duplicated().sum())"
    # python -c "import pandas as pd; p=r'data/processed/tenerife/air_quality/weekly_tfe_2016_2025.parquet'; df=pd.read_parquet(p); print(df.dtypes); print(); print(df.isna().sum())"
    # python -c "import pandas as pd; p=r'data/processed/tenerife/air_quality/weekly_tfe_2016_2025.parquet'; df=pd.read_parquet(p); df['week_start']=pd.to_datetime(df['week_start']); x=df[df['week_start']>=pd.Timestamp('2025-01-01')]; print(x[['week_start','year','PM10','PM2.5','SO2','NO2','O3']].head(10).to_string()); print(); print('rows_2025 =', len(x)); print('nulls_2025 ='); print(x[['PM10','PM2.5','SO2','NO2','O3','days_with_pm10','days_missing_pm10']].isna().sum())"
    '''Sí. Pasa el check:
        shape = (523, 9) → correcto
        dup week_start = 0 → bien
        PM10 y PM2.5 completos → bien
        2025 entra con valores razonables → bien
        SO2/NO2/O3 y days_* en 2025 como NaN → esperado, no bug'''

if __name__ == "__main__":
    main()