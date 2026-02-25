# src/02_calima/step3_build_dust_day_flag_province.py
from __future__ import annotations
import pandas as pd
from src.calima.calima_config import (
    ROOT, AIRPORTS_SC, VIS_STRICT_LT_M, RH_LT_PCT, TIME_TOL_MIN, K_PROVINCE
)

IN_FP = ROOT / "data" / "processed" / "isd_daily_13utc_wide_sc_2016_2024.parquet"  # lo generaremos luego
OUT_DIR = ROOT / "data" / "processed"
OUT_DIR.mkdir(parents=True, exist_ok=True)

def main():
    df = pd.read_parquet(IN_FP).sort_values("date_utc").reset_index(drop=True)

    airports = [a for a, ids in AIRPORTS_SC.items() if ids[0] is not None]  # los que estén activos
    # (hoy serán GCXO/GCTS, luego añadimos los otros)

    # Compute dust_like per airport using time tolerance + vis + RH
    dust_cols = []
    for icao in airports:
        vis = f"vis_m_{icao.lower()}"
        rh  = f"rh_pct_{icao.lower()}"
        mins = f"minutes_from_13utc_{icao.lower()}"

        if not all(c in df.columns for c in [vis, rh, mins]):
            # aeropuerto no presente todavía en el wide
            continue

        dust_like = (
            (df[mins] <= TIME_TOL_MIN) &
            (df[vis] < VIS_STRICT_LT_M) &
            (df[rh] < RH_LT_PCT)
        )
        col = f"dust_like_{icao.lower()}"
        df[col] = dust_like.astype(int)
        dust_cols.append(col)

    # Provincial count + flag
    df["dust_like_count_sc"] = df[dust_cols].sum(axis=1) if dust_cols else 0
    df["dust_day_sc"] = (df["dust_like_count_sc"] >= K_PROVINCE).astype(int)

    out = OUT_DIR / "dust_days_sc_province_2016_2024.parquet"
    df.to_parquet(out, index=False)
    print("Saved:", out)
    print("Airports used:", dust_cols)
    print("Dust-day rate:", df["dust_day_sc"].mean())

if __name__ == "__main__":
    main()