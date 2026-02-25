# src/11_build_aemet_heat_alerts_from_cap.py
from __future__ import annotations

import re
from pathlib import Path
import pandas as pd


IN_FP = Path("data/raw/aemet_alerts_cap_parsed.csv")

OUT_DAILY = Path("data/interim/aemet_heat_alert_daily.csv")
OUT_WEEKLY = Path("data/interim/aemet_heat_alerts_weekly.csv")
LOG_FILE = Path("logs/11_build_aemet_heat_alerts_from_cap.txt")

CHUNK_SIZE = 250_000  # adjust if needed


def week_start_monday(day: pd.Timestamp) -> pd.Timestamp:
    """Return Monday week_start for a given day (00:00)."""
    day = pd.Timestamp(day).normalize()
    return (day - pd.to_timedelta(day.weekday(), unit="D")).normalize()


def severity_to_level(sev: str) -> int:
    """
    CAP severity → ordinal level
    Minor=1, Moderate=2, Severe=3, Extreme=4
    """
    if not isinstance(sev, str):
        return 0
    s = sev.strip().lower()
    return {
        "minor": 1,
        "moderate": 2,
        "severe": 3,
        "extreme": 4,
    }.get(s, 0)


def main() -> None:
    if not IN_FP.exists():
        raise FileNotFoundError(f"Missing input file: {IN_FP}")

    OUT_DAILY.parent.mkdir(parents=True, exist_ok=True)
    OUT_WEEKLY.parent.mkdir(parents=True, exist_ok=True)

    # Filters (based on what you observed in `event` and `areaDesc`)
    heat_pat = re.compile(r"(?:high-temperature|temperaturas m[aá]ximas)", re.IGNORECASE)
    tfe_pat = re.compile(r"(?:tenerife)", re.IGNORECASE)

    usecols = ["event", "areaDesc", "severity", "onset", "expires"]

    # Keep daily max level in a dict: day -> max_level
    # (memory-light and fast)
    day_level: dict[pd.Timestamp, int] = {}

    total_rows = 0
    kept_rows = 0

    for chunk in pd.read_csv(
        IN_FP,
        chunksize=CHUNK_SIZE,
        usecols=usecols,
        dtype={"event": "string", "areaDesc": "string", "severity": "string", "onset": "string", "expires": "string"},
        low_memory=False,
    ):
        total_rows += len(chunk)

        # Filter heat + Tenerife
        is_heat = chunk["event"].str.contains(heat_pat, na=False)
        is_tfe = chunk["areaDesc"].str.contains(tfe_pat, na=False)
        sub = chunk[is_heat & is_tfe].copy()
        if sub.empty:
            continue

        kept_rows += len(sub)

        # Parse onset/expires (they parse 1.0 in your diagnostics)
        sub["start_dt"] = pd.to_datetime(sub["onset"], errors="coerce", utc=True).dt.tz_convert(None)
        sub["end_dt"] = pd.to_datetime(sub["expires"], errors="coerce", utc=True).dt.tz_convert(None)
        sub = sub.dropna(subset=["start_dt", "end_dt"])
        if sub.empty:
            continue

        # Map severity to level
        sub["heat_level"] = sub["severity"].apply(severity_to_level)

        # Expand each alert to daily coverage
        for rs, re_, lvl in zip(sub["start_dt"], sub["end_dt"], sub["heat_level"]):
            if lvl <= 0:
                continue

            start_day = pd.Timestamp(rs.date())
            end_day = pd.Timestamp(re_.date())
            if end_day < start_day:
                continue

            # Safety cap to avoid accidental giant ranges
            span_days = (end_day - start_day).days
            if span_days > 30:
                continue

            for d in pd.date_range(start_day, end_day, freq="D"):
                prev = day_level.get(d)
                if prev is None or lvl > prev:
                    day_level[d] = lvl

    if not day_level:
        raise RuntimeError(
            "No heat alerts found. Check patterns for event/areaDesc or confirm file contents."
        )

    daily = (
        pd.DataFrame({"day": list(day_level.keys()), "heat_level_max_day": list(day_level.values())})
        .sort_values("day")
        .reset_index(drop=True)
    )

    daily["aemet_heat_alert_day"] = (daily["heat_level_max_day"] > 0).astype(int)
    daily["week_start"] = daily["day"].apply(week_start_monday)

    # Save daily
    daily.to_csv(OUT_DAILY, index=False)

    # Weekly aggregation (mirrors your calima weekly structure)
    weekly = daily.groupby("week_start").agg(
        aemet_heat_alert_days=("aemet_heat_alert_day", "sum"),
        heat_level_max_week=("heat_level_max_day", "max"),
        heat_days_lvl_ge2=("heat_level_max_day", lambda s: int((s >= 2).sum())),
        heat_days_lvl_ge3=("heat_level_max_day", lambda s: int((s >= 3).sum())),
        heat_days_lvl_ge4=("heat_level_max_day", lambda s: int((s >= 4).sum())),
        n_days=("day", "count"),
    ).reset_index()

    weekly["aemet_heat_alert_any"] = (weekly["aemet_heat_alert_days"] > 0).astype(int)
    weekly["heat_any_lvl_ge2"] = (weekly["heat_level_max_week"] >= 2).astype(int)
    weekly["heat_any_lvl_ge3"] = (weekly["heat_level_max_week"] >= 3).astype(int)
    weekly["heat_any_lvl_ge4"] = (weekly["heat_level_max_week"] >= 4).astype(int)
    weekly["coverage"] = weekly["n_days"] / 7.0

    # Save weekly
    weekly.to_csv(OUT_WEEKLY, index=False)

    print(f"Total rows scanned: {total_rows:,}")
    print(f"Rows kept (heat + Tenerife): {kept_rows:,}")
    print(f"Wrote {OUT_DAILY} (days={len(daily)})")
    print(f"Wrote {OUT_WEEKLY} (weeks={len(weekly)})")

    log_text = (
        "11_build_aemet_heat_alerts_from_cap.py\n"
        f"Total rows scanned: {total_rows:,}"
        f"Rows kept (heat + Tenerife): {kept_rows:,}"
        f"Wrote {OUT_DAILY} (days={len(daily)})"
        f"Wrote {OUT_WEEKLY} (weeks={len(weekly)})"
    )
    LOG_FILE.write_text(log_text, encoding="utf-8")
    print(log_text)

if __name__ == "__main__":
    main()