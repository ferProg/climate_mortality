"""
QA validation for weather dataset ranges and physical logic.

Checks:
  - Temperature ranges (-50 to +60°C)
  - Pressure ranges (900 to 1050 hPa)
  - Logical: tmax > tmin
  - Wind/gust ranges (physical limits)
  - Humidity ranges (0-100%)
  - Precipitation (non-negative)
  
Output: CSV report with outliers flagged + counts
"""

import argparse
from pathlib import Path
import pandas as pd
import numpy as np


def find_column(df: pd.DataFrame, candidates: list) -> str | None:
    """Find first matching column from candidates."""
    for c in candidates:
        if c in df.columns:
            return c
    return None


def validate_weather_ranges(df: pd.DataFrame, island: str) -> tuple[dict, pd.DataFrame]:
    """
    Validate weather dataset ranges and physical logic.
    
    Args:
        df: Weather dataframe
        island: Island name for reporting
        
    Returns:
        qa_dict: Summary dict with counts of violations
        flags_df: DataFrame with flagged rows
    """
    
    df = df.copy()
    df["week_start"] = pd.to_datetime(df["week_start"], errors="coerce")
    
    qa_dict = {
        "island": island,
        "rows_total": int(len(df)),
        "week_start_nulls": int(df["week_start"].isna().sum()),
    }
    
    flags_rows = []
    
    # ========== 1) Temperature logic: tmax >= tmin ==========
    tmax_col = find_column(df, ["tmax_c_mean", "t_max", "tmax"])
    tmin_col = find_column(df, ["tmin_c_mean", "t_min", "tmin"])
    
    if tmax_col and tmin_col:
        bad_temp_logic = df[df[tmax_col] < df[tmin_col]]
        qa_dict["temp_logic_violations"] = int(len(bad_temp_logic))
        
        for idx, row in bad_temp_logic.iterrows():
            flags_rows.append({
                "week_start": row.get("week_start"),
                "variable": "temperature_logic",
                "value": f"{tmax_col}={row[tmax_col]}, {tmin_col}={row[tmin_col]}",
                "bounds": "tmax >= tmin",
                "status": "VIOLATION"
            })
    else:
        qa_dict["temp_logic_violations"] = -1  # Not checked
    
    # ========== 2) Temperature range: -50 to +60°C ==========
    temp_col = find_column(df, ["temp_c_mean", "temp_mean", "t_mean"])
    
    if temp_col:
        bad_temp_range = df[(df[temp_col] < -50) | (df[temp_col] > 60)]
        qa_dict["temp_range_outliers"] = int(len(bad_temp_range))
        
        for idx, row in bad_temp_range.iterrows():
            flags_rows.append({
                "week_start": row.get("week_start"),
                "variable": temp_col,
                "value": float(row[temp_col]),
                "bounds": "[-50, 60]",
                "status": "OUTLIER"
            })
    else:
        qa_dict["temp_range_outliers"] = -1
    
    # ========== 3) Pressure range: 900 to 1050 hPa ==========
    pressure_col = find_column(df, [
        "pressure_hpa_mean", "pressure_mean", "pres_mean", 
        "pmean", "mean_pressure", "pressure"
    ])
    
    if pressure_col:
        bad_pressure = df[(df[pressure_col] < 900) | (df[pressure_col] > 1050)]
        qa_dict["pressure_range_outliers"] = int(len(bad_pressure))
        
        for idx, row in bad_pressure.iterrows():
            flags_rows.append({
                "week_start": row.get("week_start"),
                "variable": pressure_col,
                "value": float(row[pressure_col]),
                "bounds": "[900, 1050]",
                "status": "OUTLIER"
            })
    else:
        qa_dict["pressure_range_outliers"] = -1
    
    # ========== 4) Wind (mean): 0 to 40 m/s ==========
    wind_col = find_column(df, ["wind_ms_mean", "wind_mean", "vv_mean", "wind"])
    
    if wind_col:
        bad_wind = df[(df[wind_col] < 0) | (df[wind_col] > 40)]
        qa_dict["wind_mean_outliers"] = int(len(bad_wind))
        
        for idx, row in bad_wind.iterrows():
            flags_rows.append({
                "week_start": row.get("week_start"),
                "variable": wind_col,
                "value": float(row[wind_col]),
                "bounds": "[0, 40]",
                "status": "OUTLIER"
            })
    else:
        qa_dict["wind_mean_outliers"] = -1
    
    # ========== 5) Wind gust (max): 0 to 80 m/s ==========
    gust_col = find_column(df, ["gust_max", "racha_max", "wind_gust_max", "wind_max"])
    
    if gust_col:
        bad_gust = df[(df[gust_col] < 0) | (df[gust_col] > 80)]
        qa_dict["wind_gust_outliers"] = int(len(bad_gust))
        
        for idx, row in bad_gust.iterrows():
            flags_rows.append({
                "week_start": row.get("week_start"),
                "variable": gust_col,
                "value": float(row[gust_col]),
                "bounds": "[0, 80]",
                "status": "OUTLIER"
            })
    else:
        qa_dict["wind_gust_outliers"] = -1
    
    # ========== 6) Humidity: 0 to 100% ==========
    humidity_col = find_column(df, ["rh_mean", "humidity_mean", "hr_mean", "humidity"])
    
    if humidity_col:
        bad_humidity = df[(df[humidity_col] < 0) | (df[humidity_col] > 100)]
        qa_dict["humidity_range_outliers"] = int(len(bad_humidity))
        
        for idx, row in bad_humidity.iterrows():
            flags_rows.append({
                "week_start": row.get("week_start"),
                "variable": humidity_col,
                "value": float(row[humidity_col]),
                "bounds": "[0, 100]",
                "status": "OUTLIER"
            })
    else:
        qa_dict["humidity_range_outliers"] = -1
    
    # ========== 7) Precipitation: >= 0 ==========
    precip_col = find_column(df, [
        "precip_sum", "precip_total", "prec_total", 
        "precipitation", "precip"
    ])
    
    if precip_col:
        bad_precip = df[df[precip_col] < 0]
        qa_dict["precip_negative"] = int(len(bad_precip))
        
        for idx, row in bad_precip.iterrows():
            flags_rows.append({
                "week_start": row.get("week_start"),
                "variable": precip_col,
                "value": float(row[precip_col]),
                "bounds": "[0, inf)",
                "status": "VIOLATION"
            })
    else:
        qa_dict["precip_negative"] = -1
    
    # ========== Build summary stats ==========
    qa_dict["total_violations"] = sum([
        v for k, v in qa_dict.items() 
        if k not in ["island", "rows_total", "week_start_nulls"] and isinstance(v, int) and v > 0
    ])
    
    flags_df = pd.DataFrame(flags_rows) if flags_rows else pd.DataFrame(
        columns=["week_start", "variable", "value", "bounds", "status"]
    )
    
    return qa_dict, flags_df


def main():
    ap = argparse.ArgumentParser(
        description="Validate weather dataset ranges and physical logic."
    )
    ap.add_argument("--master", required=True, help="Path to master parquet/csv with weather columns")
    ap.add_argument("--island", required=True, help="Island name for reporting, e.g., gcan")
    ap.add_argument("--outdir", default="reports/tables")
    args = ap.parse_args()
    
    fp_master = Path(args.master)
    outdir = Path(args.outdir) / "island" / args.island
    outdir.mkdir(parents=True, exist_ok=True)
    
    # Read
    if fp_master.suffix.lower() == ".parquet":
        df = pd.read_parquet(fp_master)
    else:
        df = pd.read_csv(fp_master)
    
    # Validate
    qa_dict, flags_df = validate_weather_ranges(df, args.island)
    
    # Write outputs
    qa_df = pd.DataFrame([qa_dict])
    qa_df.to_csv(outdir / f"qa_weather_ranges_{args.island}.csv", index=False)
    
    if not flags_df.empty:
        flags_df.to_csv(outdir / f"qa_weather_ranges_{args.island}_flags.csv", index=False)
    
    # Print summary
    print(f"Weather QA Summary ({args.island}):")
    print(f"  Total rows: {qa_dict['rows_total']}")
    print(f"  Temperature logic violations: {qa_dict.get('temp_logic_violations', 'N/A')}")
    print(f"  Temperature range outliers: {qa_dict.get('temp_range_outliers', 'N/A')}")
    print(f"  Pressure outliers: {qa_dict.get('pressure_range_outliers', 'N/A')}")
    print(f"  Wind outliers: {qa_dict.get('wind_mean_outliers', 'N/A')}")
    print(f"  Gust outliers: {qa_dict.get('wind_gust_outliers', 'N/A')}")
    print(f"  Humidity violations: {qa_dict.get('humidity_range_outliers', 'N/A')}")
    print(f"  Negative precip: {qa_dict.get('precip_negative', 'N/A')}")
    print(f"  Total violations: {qa_dict.get('total_violations', 'N/A')}")
    print(f"\nOutputs:")
    print(f"  QA: {(outdir / f'qa_weather_ranges_{args.island}.csv').resolve()}")
    if not flags_df.empty:
        print(f"  Flags: {(outdir / f'qa_weather_ranges_{args.island}_flags.csv').resolve()}")


if __name__ == "__main__":
    main()
