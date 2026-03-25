# Construye el dataset master semanal de una isla (o de todas) uniendo
# las fuentes procesadas del proyecto por week_start.
#
# Qué hace:
# 1) Localiza automáticamente los archivos de entrada más recientes para:
#    deaths, weather, visibility, air_quality, cap y Heliyon.
# 2) Valida que cada feed tenga week_start limpio, sin nulos ni duplicados.
# 3) Normaliza week_start y selecciona las columnas útiles de cada fuente.
# 4) Crea un calendario semanal fijo para el periodo de análisis.
# 5) Filtra deaths al rango del análisis y hace left-merge sucesivo con:
#    weather, visibility, air_quality, cap y Heliyon.
# 6) Añade metadatos de isla, código y año.
# 7) Valida que el master final tenga exactamente una fila por semana.
# 8) Guarda el parquet final en data/processed/<island>/master/.
#
# Nota:
# - El rango de análisis está fijado dentro del script.
# - CAP puede quedar con NaN al inicio si la fuente empieza más tarde.
# - Visibility se lee desde data/interim/<island>/visibility/step4_weekly/.

# Heliyon/calima is treated as a general Canarias-wide weekly feed,
# shared across all islands. It is read from:
# data/processed/calima/calima_general_weekly.parquet
# and merged by week_start into each island master dataset.


from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from src.utils.constants import island_code, ISLAND_CODES
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


def resolve_analysis_window(start_year: int, end_year: int) -> tuple[pd.Timestamp, pd.Timestamp]:
    if end_year < start_year:
        raise ValueError("--end-year must be >= --start-year")

    start_date = pd.Timestamp(f"{start_year}-01-01")
    end_date = pd.Timestamp(f"{end_year}-12-31")

    # normalizar a semanas Monday-start
    analysis_start = start_date - pd.to_timedelta(start_date.weekday(), unit="D")
    analysis_end = end_date - pd.to_timedelta(end_date.weekday(), unit="D")

    return analysis_start, analysis_end

def clip_to_analysis(df: pd.DataFrame, analysis_start: pd.Timestamp, analysis_end: pd.Timestamp) -> pd.DataFrame:
    return df[
        (df["week_start"] >= analysis_start) & (df["week_start"] <= analysis_end)
    ].copy()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build weekly master dataset for one island or all islands."
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--island",
        choices=sorted(ISLAND_CODES.keys()),
        help="Canonical island name, e.g. tenerife",
    )
    group.add_argument(
        "--all",
        action="store_true",
        help="Build master datasets for all islands",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        required=True, 
        help="Start year, e.g. 2016")
    parser.add_argument(
        "--end-year", 
        type=int, 
        required=True, 
        help="End year, e.g. 2025")

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

def find_all_matches(base_dir: Path, pattern: str, source_name: str) -> List[Path]:
    matches = sorted(base_dir.glob(pattern))
    if not matches:
        raise FileNotFoundError(
            f"{source_name}: no files matched pattern '{pattern}' under {base_dir}"
        )
    return matches


def read_concat_weekly(paths: List[Path], source_name: str) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []

    for path in paths:
        df = pd.read_parquet(path)
        if df.empty:
            continue
        df = ensure_week_start(df, f"{source_name}:{path.name}")
        frames.append(df)

    if not frames:
        raise ValueError(f"{source_name}: all matched files were empty")

    out = pd.concat(frames, ignore_index=True)
    out = out.sort_values("week_start").drop_duplicates(subset=["week_start"], keep="last")
    out = out.reset_index(drop=True)
    return out


def build_paths(island: str, processed_dir: Path, interim_dir: Path) -> Dict[str, object]:
    code = island_code(island)

    paths = {
        "deaths": find_all_matches(
            processed_dir / island / "deaths",
            f"deaths_weekly_{code}_*.parquet",
            "deaths",
        ),
        "weather": find_all_matches(
            processed_dir / island / "weather",
            f"weather_weekly_{code}_*.parquet",
            "weather",
        ),
        # Visibility now lives in interim, produced by step4 of the visibility pipeline.
        "visibility": find_all_matches(
            interim_dir / island / "visibility" / "step4_weekly",
            f"visibility_weekly_{code}_*.parquet",
            "visibility",
        ),
        "airq": find_all_matches(
            processed_dir / island / "air_quality",
            f"weekly_{code}_*.parquet",
            "air_quality",
        ),
        "cap": find_all_matches(
            processed_dir / island / "cap",
            f"cap_weekly_{code}_*.parquet",
            "cap",
        ),
        "heliyon": processed_dir / "calima" / "calima_general_weekly.parquet",
    }

    if not Path(paths["heliyon"]).exists():
        raise FileNotFoundError(f"Missing required input: {paths['heliyon']}")

    return paths

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
    wanted = ["week_start", "calima_canarias_dai_week", "calima_canarias_level_week"]
    missing = [c for c in wanted if c not in df.columns]
    if missing:
        raise ValueError(f"heliyon feed missing expected columns: {missing}")

    out = df[wanted].copy().rename(columns={
        "calima_canarias_dai_week": "calima_dai_flag",
        "calima_canarias_level_week": "calima_level_week",
    })

    out = calendar[["week_start"]].merge(out, on="week_start", how="left")
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

def validate_master(df: pd.DataFrame, analysis_start: pd.Timestamp, analysis_end: pd.Timestamp) -> None:
    if df["week_start"].duplicated().any():
        raise ValueError("Duplicate week_start rows found in master")

    if not df["week_start"].is_monotonic_increasing:
        raise ValueError("week_start is not sorted ascending")

    expected_weeks = pd.date_range(start=analysis_start, end=analysis_end, freq="W-MON")
    if len(df) != len(expected_weeks):
        raise ValueError(
            f"Unexpected number of weeks: got {len(df)}, expected {len(expected_weeks)} "
            f"for {analysis_start.date()}..{analysis_end.date()}"
        )
    

def build_master(
    island: str,
    processed_dir: Path,
    interim_dir: Path,
    analysis_start: pd.Timestamp,
    analysis_end: pd.Timestamp,
    start_year: int,
    end_year: int,
) -> Path:
    print(f"[build_master] {island} | {analysis_start.date()} .. {analysis_end.date()}")

    code = island_code(island)
    paths = build_paths(island=island, processed_dir=processed_dir, interim_dir=interim_dir)

    deaths = read_concat_weekly(paths["deaths"], "deaths")
    weather = read_concat_weekly(paths["weather"], "weather")
    visibility = read_concat_weekly(paths["visibility"], "visibility")
    airq = read_concat_weekly(paths["airq"], "airq")
    cap = read_concat_weekly(paths["cap"], "cap")

    deaths = clip_to_analysis(deaths, analysis_start, analysis_end)
    weather = clip_to_analysis(weather, analysis_start, analysis_end)
    visibility = clip_to_analysis(visibility, analysis_start, analysis_end)
    airq = clip_to_analysis(airq, analysis_start, analysis_end)
    cap = clip_to_analysis(cap, analysis_start, analysis_end)

    calendar = week_calendar(
        start=str(analysis_start.date()),
        end=str(analysis_end.date()),
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
    validate_master(master, analysis_start=analysis_start, analysis_end=analysis_end)

    outdir = processed_dir / island / "master"
    outdir.mkdir(parents=True, exist_ok=True)

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
            "calima_level_week",
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
    analysis_start, analysis_end = resolve_analysis_window(args.start_year, args.end_year)
    processed_dir = Path(args.processed_dir)
    interim_dir = Path(args.interim_dir)

    ######TEMPORAL#####
    print("ISLAND:", args.island)
    print("START YEAR:", args.start_year)
    print("END YEAR:", args.end_year)
    print("ANALYSIS START:", analysis_start)
    print("ANALYSIS END:", analysis_end)

    if args.all:
        outputs: List[Path] = []
        for island in ISLANDS:
            print(f"\n=== Building master for {island} ===")
            outputs.append(
                build_master(
                    island=island,
                    processed_dir=processed_dir,
                    interim_dir=interim_dir,
                    analysis_start=analysis_start,
                    analysis_end=analysis_end,
                    start_year=args.start_year,
                    end_year=args.end_year
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
            analysis_start=analysis_start,
            analysis_end=analysis_end,
            start_year=args.start_year,
            end_year=args.end_year
        )


if __name__ == "__main__":
    main()