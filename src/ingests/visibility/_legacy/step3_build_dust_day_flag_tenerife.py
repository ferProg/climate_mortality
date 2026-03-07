# src/02_calima/step3_build_dust_day_flag.py

from __future__ import annotations

from pathlib import Path
import pandas as pd


IN_FP = Path("data/processed/isd_tfs_tfn_daily_13utc_wide_2016_2024.parquet")
OUT_DIR = Path("data/processed")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Paper thresholds
VIS_STRICT_LT_M = 9_999  # because 9999 encodes >=10km
RH_THRESHOLD_PCT = 70.0

# We’ll only trust “SYNOP 13 UTC” if the nearest obs is within this tolerance (minutes).
# This should match what you used in step2. If you change it there, change it here too.
TIME_TOL_MIN = 90


def main():
    if not IN_FP.exists():
        raise FileNotFoundError(f"Missing input: {IN_FP}")

    df = pd.read_parquet(IN_FP).sort_values("date_utc").reset_index(drop=True)

    # Safety: ensure expected columns exist
    required = [
        "date_utc",
        "vis_m_gcxo", "vis_m_gcts",
        "rh_pct_gcxo", "rh_pct_gcts",
        "minutes_from_13utc_gcxo", "minutes_from_13utc_gcts",
        "within_time_tolerance_gcxo", "within_time_tolerance_gcts",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Enforce time tolerance (just in case)
    within_gcxo = (df["minutes_from_13utc_gcxo"] <= TIME_TOL_MIN) & (df["within_time_tolerance_gcxo"] == True)
    within_gcts = (df["minutes_from_13utc_gcts"] <= TIME_TOL_MIN) & (df["within_time_tolerance_gcts"] == True)

    dust_like_gcxo = within_gcxo & (df["vis_m_gcxo"] < VIS_STRICT_LT_M) & (df["rh_pct_gcxo"] < RH_THRESHOLD_PCT)
    dust_like_gcts = within_gcts & (df["vis_m_gcts"] < VIS_STRICT_LT_M) & (df["rh_pct_gcts"] < RH_THRESHOLD_PCT)

    df["dust_like_gcxo"] = dust_like_gcxo.astype(int)
    df["dust_like_gcts"] = dust_like_gcts.astype(int)
    df["dust_day_tfe_tfs_tfn"] = (dust_like_gcxo & dust_like_gcts).astype(int)

    # Simple sanity checks / summaries
    df["year"] = pd.to_datetime(df["date_utc"]).dt.year

    print("\nDust-like days by station (count):")
    print(df[["dust_like_gcxo", "dust_like_gcts"]].sum())

    print("\nDust-days Tenerife (both stations) by year:")
    print(df.groupby("year")["dust_day_tfe_tfs_tfn"].sum().astype(int))

    print("\nOverall dust-day rate:")
    rate = df["dust_day_tfe_tfs_tfn"].mean()
    print(f"{rate:.4%}")

    # Save outputs
    out_wide = OUT_DIR / "dust_days_tfe_tfs_tfn_2016_2024.parquet"
    df.to_parquet(out_wide, index=False)
    print(f"\nSaved: {out_wide}")

    # Also save a minimal “long” daily table (clean + compact)
    out_min = OUT_DIR / "dust_days_tfe_tfs_tfn_2016_2024_min.csv"
    keep = [
        "date_utc",
        "dust_day_tfe_tfs_tfn",
        "dust_like_gcxo", "dust_like_gcts",
        "vis_m_gcxo", "vis_m_gcts",
        "rh_pct_gcxo", "rh_pct_gcts",
        "minutes_from_13utc_gcxo", "minutes_from_13utc_gcts",
    ]
    df[keep].to_csv(out_min, index=False)
    print(f"Saved: {out_min}")

    # Extra: quick view of the “dustiest” (lowest vis) days when dust_day is true
    dust = df[df["dust_day_tfe_tfs_tfn"] == 1].copy()
    if not dust.empty:
        dust["vis_min_m"] = dust[["vis_m_gcxo", "vis_m_gcts"]].min(axis=1)
        top = dust.sort_values("vis_min_m").head(10)[["date_utc", "vis_m_gcxo", "vis_m_gcts", "rh_pct_gcxo", "rh_pct_gcts", "vis_min_m"]]
        print("\nTop 10 lowest-visibility dust-days (both stations dust-like):")
        print(top.to_string(index=False))
    else:
        print("\nNo dust-days detected with current thresholds. (This would be surprising.)")


if __name__ == "__main__":
    main()
