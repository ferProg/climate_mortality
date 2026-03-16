from pathlib import Path
import pandas as pd

fp = Path(r"data\processed\lanzarote\weather\weather_weekly_lzt_2016_2024.parquet")

df = pd.read_parquet(fp)

print("=" * 80)
print("FILE:", fp)
print("shape:", df.shape)
print("columns:", df.columns.tolist())
print("=" * 80)

# fecha
df["week_start"] = pd.to_datetime(df["week_start"], errors="coerce")

print("\nDATE QC")
print("week_start nulls:", df["week_start"].isna().sum())
print("duplicate week_start:", df["week_start"].duplicated().sum())
print("min/max:", df["week_start"].min(), "->", df["week_start"].max())

# nulls
print("\nNULL COUNTS")
print(df.isna().sum().sort_values(ascending=False).to_string())

# describe numericas
num_cols = df.select_dtypes(include="number").columns.tolist()
print("\nNUMERIC SUMMARY")
if num_cols:
    print(df[num_cols].describe().T.to_string())
else:
    print("No numeric columns found.")

def check_exists(cols):
    return all(c in df.columns for c in cols)

print("\nPHYSICAL QC")

# 1) tmax < tmin
if check_exists(["tmax_c_mean", "tmin_c_mean"]):
    bad = df[df["tmax_c_mean"] < df["tmin_c_mean"]]
    print(f"weeks with tmax_c_mean < tmin_c_mean: {len(bad)}")
    if len(bad):
        print(bad[["week_start", "tmin_c_mean", "tmax_c_mean"]].to_string(index=False))
else:
    print("Skipped tmax/tmin check (columns not found)")

# 2) temp_mean fuera de rango razonable
if "temp_c_mean" in df.columns:
    bad = df[(df["temp_c_mean"] < -5) | (df["temp_c_mean"] > 45)]
    print(f"weeks with temp_mean outside [-5, 45]: {len(bad)}")
    if len(bad):
        print(bad[["week_start", "temp_c_mean"]].to_string(index=False))
else:
    print("Skipped temp_mean range check")

# 3) pressure fuera de rango
pressure_candidates = [c for c in ["pressure_mean", "pres_mean", "pmean", "mean_pressure"] if c in df.columns]
if pressure_candidates:
    c = pressure_candidates[0]
    bad = df[(df[c] < 900) | (df[c] > 1100)]
    print(f"weeks with {c} outside [900, 1100]: {len(bad)}")
    if len(bad):
        print(bad[["week_start", c]].to_string(index=False))
else:
    print("Skipped pressure range check")

# 4) precip negativa
precip_candidates = [c for c in ["precip_sum", "pressure_hpa_mean", "prec_total", "precip_total"] if c in df.columns]
if precip_candidates:
    c = precip_candidates[0]
    bad = df[df[c] < 0]
    print(f"weeks with {c} < 0: {len(bad)}")
    if len(bad):
        print(bad[["week_start", c]].to_string(index=False))
else:
    print("Skipped precipitation check")

# 5) rachas/viento imposibles
wind_candidates = [c for c in ["wind_ms_mean", "wind_mean", "vv_mean"] if c in df.columns]
if wind_candidates:
    c = wind_candidates[0]
    bad = df[(df[c] < 0) | (df[c] > 40)]
    print(f"weeks with {c} outside [0, 40]: {len(bad)}")
    if len(bad):
        print(bad[["week_start", c]].to_string(index=False))
else:
    print("Skipped mean wind check")

gust_candidates = [c for c in ["gust_max", "racha_max", "wind_gust_max"] if c in df.columns]
if gust_candidates:
    c = gust_candidates[0]
    bad = df[(df[c] < 0) | (df[c] > 80)]
    print(f"weeks with {c} outside [0, 80]: {len(bad)}")
    if len(bad):
        print(bad[["week_start", c]].to_string(index=False))
else:
    print("Skipped gust check")

# 6) humedad fuera de rango
humidity_candidates = [c for c in ["rh_mean", "humidity_mean", "hr_mean"] if c in df.columns]
if humidity_candidates:
    c = humidity_candidates[0]
    bad = df[(df[c] < 0) | (df[c] > 100)]
    print(f"weeks with {c} outside [0, 100]: {len(bad)}")
    if len(bad):
        print(bad[["week_start", c]].to_string(index=False))
else:
    print("Skipped humidity check")

print("\nQC DONE")
