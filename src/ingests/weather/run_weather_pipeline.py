"""
run_weather_pipeline.py
-----------------------
Wrapper that orchestrates the full weather ingest pipeline for one island:

  Step 1 — Main ingest
      Runs aemet_station_daily_to_weekly.py for the primary station.

  Step 2 — Gap detection
      Reads the weekly output and flags weeks with coverage < threshold
      (default: 4/7 ≈ 0.571, i.e. fewer than 4 days with data).
      Also detects calendar weeks that are entirely missing from the output.

  Step 3 — Gap fill (if gaps found)
      For each contiguous gap range, iterates through the island's fallback
      station list (from ISLAND_WEATHER_STATIONS in constants.py) and tries
      to download data from each alternate station until one succeeds.

  Step 4 — Merge
      Merges each successfully filled gap back into the main weekly parquet,
      replacing only the gap weeks and adding traceability columns
      (imputed_from_<station> flag + donor_station).

Usage example (from project root):
    python -m src.ingests.weather.run_weather_pipeline \
        --station C329B \
        --start 2016-01-01 \
        --end 2024-12-31 \
        --island gomera

Optional flags:
    --coverage-threshold   Float 0–1. Weeks below this are treated as gaps.
                           Default: 0.571 (4/7 days).
    --chunk-days           Days per AEMET API request chunk. Default: 60.
    --sleep                Seconds between API chunks. Default: 1.0.
    --format               parquet | csv. Default: parquet.
    --also-csv             Also write CSV copies alongside parquet.
    --dry-run              Detect and report gaps but do NOT attempt to fill them.
    --coverage-rule        any_temp | tmed_only. Passed to sub-scripts.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import logging
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from src.utils.constants import island_code, island_weather_stations
from src.utils.logging import setup_logging
from src.utils.text import safe_slug

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[3]
DATA_PROCESSED_ROOT = PROJECT_ROOT / "data" / "processed"
LOGS_ROOT = PROJECT_ROOT / "logs"

LOGGER = logging.getLogger(__name__)

# Sub-scripts (called via subprocess so they stay independent)
_SCRIPT_MAIN = "src.ingests.weather.aemet_station_daily_to_weekly"
_SCRIPT_GAP  = "src.ingests.weather.aemet_station_daily_to_weekly_gap"
_SCRIPT_MERGE = "src.ingests.weather.merge_weather_gap_into_main"

COVERAGE_THRESHOLD_DEFAULT = 4 / 7  # ~0.571


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Weather ingest pipeline: main ingest → gap detection → gap fill → merge"
    )
    p.add_argument("--station",  required=True, help="Primary AEMET station code, e.g. C329B")
    p.add_argument("--start",    required=True, help="YYYY-MM-DD")
    p.add_argument("--end",      required=True, help="YYYY-MM-DD")
    p.add_argument("--island",   required=True, help="Island slug, e.g. gomera, tenerife")
    p.add_argument(
        "--coverage-threshold",
        type=float,
        default=COVERAGE_THRESHOLD_DEFAULT,
        help=f"Weeks with coverage below this value are treated as gaps. Default: {COVERAGE_THRESHOLD_DEFAULT:.3f} (4/7)",
    )
    p.add_argument("--chunk-days",     type=int,   default=60,        help="Days per API chunk. Default: 60")
    p.add_argument("--sleep",          type=float, default=1.0,       help="Seconds between chunks. Default: 1.0")
    p.add_argument("--format",         choices=["parquet", "csv"],    default="parquet")
    p.add_argument("--also-csv",       action="store_true",           help="Also write CSV copies")
    p.add_argument("--coverage-rule",  choices=["any_temp", "tmed_only"], default="any_temp")
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Detect and print gaps but do NOT attempt to fill them.",
    )
    return p.parse_args()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _run(cmd: list[str], step_label: str) -> bool:
    """Run a subprocess command. Returns True on success, False on failure."""
    LOGGER.info("[%s] Running: %s", step_label, " ".join(cmd))
    result = subprocess.run(cmd, capture_output=False)
    if result.returncode != 0:
        LOGGER.error("[%s] FAILED (exit code %s)", step_label, result.returncode)
        return False
    LOGGER.info("[%s] OK", step_label)
    return True


def _weekly_path(island: str, start: str, end: str, fmt: str) -> Path:
    """Reconstruct the weekly processed path that aemet_station_daily_to_weekly.py writes."""
    code = island_code(safe_slug(island))
    ext = "parquet" if fmt == "parquet" else "csv"
    start_year = start[:4]
    end_year = end[:4]
    return DATA_PROCESSED_ROOT / safe_slug(island) / "weather" / f"weather_weekly_{code}_{start_year}_{end_year}.{ext}"


def _gap_path(island: str, station: str, gap_label: str, fmt: str) -> Path:
    """Reconstruct the gap weekly path that aemet_station_daily_to_weekly_gap.py writes."""
    code = island_code(safe_slug(island))
    station_slug = safe_slug(station).lower()
    ext = "parquet" if fmt == "parquet" else "csv"
    return DATA_PROCESSED_ROOT / safe_slug(island) / "weather" / f"weather_weekly_{code}_{station_slug}_{gap_label}.{ext}"


def detect_gaps(weekly_path: Path, start: str, end: str, threshold: float) -> list[tuple[date, date]]:
    """
    Returns a list of (gap_start, gap_end) date tuples representing contiguous
    gap ranges in the weekly series.

    A week is considered a gap if:
      - Its coverage column is below `threshold`, OR
      - The week_start is entirely missing from the file (calendar hole).
    """
    df = pd.read_parquet(weekly_path)
    df["week_start"] = pd.to_datetime(df["week_start"]).dt.tz_localize(None)

    # Build the full expected calendar of ISO-week Mondays in the range
    start_d = date.fromisoformat(start)
    end_d   = date.fromisoformat(end)

    # Move start to the Monday of its week
    monday_offset = start_d.weekday()
    first_monday  = start_d - timedelta(days=monday_offset)

    expected_weeks: list[date] = []
    cur = first_monday
    while cur <= end_d:
        expected_weeks.append(cur)
        cur += timedelta(weeks=1)

    present_weeks = set(df["week_start"].dt.date)

    gap_weeks: list[date] = []
    for wk in expected_weeks:
        if wk not in present_weeks:
            gap_weeks.append(wk)  # completely missing week
        else:
            row_coverage = df.loc[df["week_start"].dt.date == wk, "coverage"]
            if not row_coverage.empty and float(row_coverage.iloc[0]) < threshold:
                gap_weeks.append(wk)

    if not gap_weeks:
        return []

    # Group consecutive weeks into ranges
    ranges: list[tuple[date, date]] = []
    range_start = gap_weeks[0]
    prev = gap_weeks[0]

    for wk in gap_weeks[1:]:
        if (wk - prev).days == 7:
            prev = wk
        else:
            ranges.append((range_start, prev + timedelta(days=6)))
            range_start = wk
            prev = wk
    ranges.append((range_start, prev + timedelta(days=6)))

    return ranges


def fill_gap(
    gap_start: date,
    gap_end: date,
    island: str,
    primary_station: str,
    weekly_path: Path,
    fmt: str,
    chunk_days: int,
    sleep: float,
    coverage_rule: str,
    also_csv: bool,
) -> bool:
    """
    Tries each fallback station for the island in order.
    On first success: runs the gap script then the merge script.
    Returns True if gap was filled, False if all stations failed.
    """
    fallback_stations = [
        s for s in island_weather_stations(safe_slug(island))
        if s.upper() != primary_station.upper()
    ]

    if not fallback_stations:
        LOGGER.warning(
            "No fallback stations configured for island '%s'. Cannot fill gap %s → %s.",
            island, gap_start, gap_end,
        )
        return False

    gap_label = f"{gap_start.year}_gap"
    start_s   = gap_start.isoformat()
    end_s     = gap_end.isoformat()

    for alt_station in fallback_stations:
        LOGGER.info(
            "Trying alt station %s for gap %s → %s (island: %s)",
            alt_station, gap_start, gap_end, island,
        )

        # --- Step A: Download gap data from alternate station ---
        gap_cmd = [
            sys.executable, "-m", _SCRIPT_GAP,
            "--station",       alt_station,
            "--start",         start_s,
            "--end",           end_s,
            "--island",        island,
            "--gap-label",     gap_label,
            "--chunk-days",    str(chunk_days),
            "--sleep",         str(sleep),
            "--coverage-rule", coverage_rule,
            "--format",        fmt,
        ]
        if also_csv:
            gap_cmd.append("--also-csv")

        if not _run(gap_cmd, f"GAP-DOWNLOAD [{alt_station}]"):
            LOGGER.warning("Alt station %s failed for gap %s → %s. Trying next.", alt_station, gap_start, gap_end)
            continue

        gap_file = _gap_path(island, alt_station, gap_label, fmt)
        if not gap_file.exists():
            LOGGER.warning("Gap file not found after download: %s. Trying next station.", gap_file)
            continue

        # --- Step B: Merge gap into main weekly ---
        flag_col     = f"imputed_from_{alt_station.lower()}"
        donor_label  = alt_station.upper()

        merge_cmd = [
            sys.executable, "-m", _SCRIPT_MERGE,
            "--main",         str(weekly_path),
            "--gap",          str(gap_file),
            "--out",          str(weekly_path),   # overwrite main in place
            "--flag-col",     flag_col,
            "--donor-label",  donor_label,
        ]

        if not _run(merge_cmd, f"MERGE [{alt_station}]"):
            LOGGER.warning("Merge failed for alt station %s. Trying next.", alt_station)
            continue

        LOGGER.info(
            "Gap %s → %s filled successfully with station %s.",
            gap_start, gap_end, alt_station,
        )
        return True

    LOGGER.error(
        "All fallback stations exhausted for gap %s → %s on island '%s'. Gap remains unfilled.",
        gap_start, gap_end, island,
    )
    return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    args = parse_args()

    island_slug = safe_slug(args.island)
    logs_dir    = LOGS_ROOT / island_slug
    logs_dir.mkdir(parents=True, exist_ok=True)
    logfile = logs_dir / f"weather_pipeline_{island_slug}_{args.start}_{args.end}.log"
    setup_logging(logfile)

    LOGGER.info(
        "=== WEATHER PIPELINE START | island=%s station=%s %s → %s ===",
        island_slug, args.station, args.start, args.end,
    )

    # ------------------------------------------------------------------ #
    # STEP 1 — Main ingest                                                #
    # ------------------------------------------------------------------ #
    main_cmd = [
        sys.executable, "-m", _SCRIPT_MAIN,
        "--station",       args.station,
        "--start",         args.start,
        "--end",           args.end,
        "--island",        args.island,
        "--chunk-days",    str(args.chunk_days),
        "--sleep",         str(args.sleep),
        "--coverage-rule", args.coverage_rule,
        "--format",        args.format,
    ]
    if args.also_csv:
        main_cmd.append("--also-csv")

    if not _run(main_cmd, "MAIN-INGEST"):
        LOGGER.error("Main ingest failed. Aborting pipeline.")
        sys.exit(1)

    # ------------------------------------------------------------------ #
    # STEP 2 — Gap detection                                              #
    # ------------------------------------------------------------------ #
    weekly_path = _weekly_path(args.island, args.start, args.end, args.format)
    if not weekly_path.exists():
        LOGGER.error("Expected weekly output not found: %s", weekly_path)
        sys.exit(1)

    LOGGER.info("Reading weekly output: %s", weekly_path)
    gap_ranges = detect_gaps(weekly_path, args.start, args.end, args.coverage_threshold)

    if not gap_ranges:
        LOGGER.info("No gaps detected (all weeks above coverage threshold). Pipeline complete.")
        print("\n✓ No gaps found. Pipeline complete.")
        return

    print(f"\n⚠  {len(gap_ranges)} gap range(s) detected:")
    for i, (gs, ge) in enumerate(gap_ranges, 1):
        print(f"   [{i}] {gs} → {ge}")

    if args.dry_run:
        print("\n[dry-run] Gap fill skipped. Re-run without --dry-run to fill gaps.")
        return

    # ------------------------------------------------------------------ #
    # STEPS 3 & 4 — Gap fill + Merge                                      #
    # ------------------------------------------------------------------ #
    filled   = 0
    unfilled = 0

    for gap_start, gap_end in gap_ranges:
        ok = fill_gap(
            gap_start       = gap_start,
            gap_end         = gap_end,
            island          = args.island,
            primary_station = args.station,
            weekly_path     = weekly_path,
            fmt             = args.format,
            chunk_days      = args.chunk_days,
            sleep           = args.sleep,
            coverage_rule   = args.coverage_rule,
            also_csv        = args.also_csv,
        )
        if ok:
            filled += 1
        else:
            unfilled += 1

    # ------------------------------------------------------------------ #
    # Summary                                                             #
    # ------------------------------------------------------------------ #
    print(f"\n=== PIPELINE SUMMARY ===")
    print(f"  Gap ranges found   : {len(gap_ranges)}")
    print(f"  Filled             : {filled}")
    print(f"  Unfilled           : {unfilled}")
    print(f"  Weekly output      : {weekly_path}")
    print(f"  Log                : {logfile}")

    if unfilled > 0:
        print(f"\n⚠  {unfilled} gap(s) could not be filled. Check logs for details.")
        sys.exit(1)
    else:
        print("\n✓ Pipeline complete. All gaps filled.")


if __name__ == "__main__":
    main()
