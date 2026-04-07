'''
Script para valorar los extremos y la incertidumbre que pudiera ocasionar los episodios pequeños por isla.

Para ejecutar ej.Gran Canaria:
python src/qa/extreme_week_audit.py `
  --island gcan `
  --master data/processed/gran_canaria/master/master_gcan_2015_2024.parquet `
  --xvar PM10 `
  --yvar deaths_week `
  --quantile 0.95 `
  --lag 2 `
  --seasonality woy `
  --start_date 2018-01-01 `
  --week_start_unit ms

  Output

Se guarda en:

reports/tables/island/gcan/extremes/extreme_audit_PM10_q95_lag2_woy_20180101_plus.csv

o todas las islas:

# Desde la raíz del proyecto: C:\dev\projects\heat_mortality_analysis

$root = "data\processed"

# Elige la variable a auditar
$xvar = "PM10"          # cambia a lo que quieras: PM2.5, tmax_c_mean, temp_c_mean, etc.
$yvar = "deaths_week"
$quantile = 0.95
$lag = 2
$seasonality = "woy"
$start_date = "2018-01-01"
$week_unit = "ms"

$jobs = @(
  @{ island="tfe";  master="$root\tenerife\master\master_tfe_2015_2024.parquet" },
  @{ island="gcan"; master="$root\gran_canaria\master\master_gcan_2015_2024.parquet" },
  @{ island="lzt";  master="$root\lanzarote\master\master_lzt_2015_2024.parquet" },
  @{ island="ftv";  master="$root\fuerteventura\master\master_ftv_2015_2024.parquet" },
  @{ island="lpa";  master="$root\la_palma\master\master_lpa_2015_2024.parquet" },
  @{ island="gom";  master="$root\gomera\master\master_gom_2015_2024.parquet" }
  # hie excluida
)

foreach ($j in $jobs) {
  Write-Host "`nRunning extreme audit for $($j.island)..." -ForegroundColor Cyan
  python src\qa\extreme_week_audit.py `
    --island $j.island `
    --master $j.master `
    --xvar $xvar `
    --yvar $yvar `
    --quantile $quantile `
    --lag $lag `
    --seasonality $seasonality `
    --start_date $start_date `
    --week_start_unit $week_unit
}

  '''

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def read_any(p: Path) -> pd.DataFrame:
    return pd.read_parquet(p) if p.suffix.lower() == ".parquet" else pd.read_csv(p)


def extreme_week_audit_one(
    df: pd.DataFrame,
    x_var: str,
    y_var: str,
    date_col: str = "week_start",
    quantile: float = 0.95,
    lag: int = 2,
    seasonality: str = "woy",
    start_date: str | None = None,
    B: int = 3000,
    seed: int = 42,
) -> dict:
    d = df[[date_col, x_var, y_var]].copy()
    d[date_col] = pd.to_datetime(d[date_col], errors="coerce")
    d = d.dropna(subset=[date_col, x_var, y_var]).sort_values(date_col)

    if start_date:
        d = d[d[date_col] >= pd.to_datetime(start_date)]

    n_total = int(len(d))
    if n_total == 0:
        return {"error": "No rows after filtering."}

    thr = float(d[x_var].quantile(quantile))
    d["is_extreme"] = (d[x_var] > thr).astype(int)

    # Episodes = consecutive extreme weeks
    start = (d["is_extreme"] == 1) & (d["is_extreme"].shift(1, fill_value=0) == 0)
    d["ep_id"] = start.cumsum()
    d.loc[d["is_extreme"] == 0, "ep_id"] = pd.NA

    eps = (
        d.dropna(subset=["ep_id"])
         .groupby("ep_id")
         .agg(weeks=(date_col, "size"))
    )
    n_episodes = int(len(eps))
    max_ep_weeks = int(eps["weeks"].max()) if n_episodes else 0
    n_ext_weeks = int(d["is_extreme"].sum())

    # Lagged exposure flag
    d["ext_lag"] = d["is_extreme"].shift(lag)
    d = d.dropna(subset=["ext_lag"])
    d["ext_lag"] = d["ext_lag"].astype(int)

    # Seasonality adjustment (anomalies) for outcome
    if seasonality == "woy":
        key = d[date_col].dt.isocalendar().week.astype(int)
    elif seasonality == "month":
        key = d[date_col].dt.month.astype(int)
    else:
        raise ValueError("seasonality must be 'woy' or 'month'")

    d["y_anom"] = d[y_var] - d.groupby(key)[y_var].transform("mean")

    g1 = d.loc[d["ext_lag"] == 1, "y_anom"].to_numpy()
    g0 = d.loc[d["ext_lag"] == 0, "y_anom"].to_numpy()

    # Point estimate
    d_mean = float(g1.mean() - g0.mean()) if (len(g1) and len(g0)) else np.nan

    # Bootstrap by weeks
    rng = np.random.default_rng(seed)
    boot = np.empty(B)
    for b in range(B):
        s1 = rng.choice(g1, size=len(g1), replace=True)
        s0 = rng.choice(g0, size=len(g0), replace=True)
        boot[b] = s1.mean() - s0.mean()
    ci_weeks = np.quantile(boot, [0.025, 0.975])

    # Bootstrap by episodes (more conservative)
    if n_episodes:
        base0 = d[d["is_extreme"] == 0].copy()
        ep_ids = eps.index.to_numpy()

        boot_ep = []
        for _ in range(B):
            sample_ids = rng.choice(ep_ids, size=len(ep_ids), replace=True)
            sample1 = pd.concat([d[d["ep_id"] == sid] for sid in sample_ids], ignore_index=True)

            dd = pd.concat([base0, sample1], ignore_index=True).sort_values(date_col).copy()
            dd["ext_lag"] = dd["is_extreme"].shift(lag)
            dd = dd.dropna(subset=["ext_lag"])
            dd["ext_lag"] = dd["ext_lag"].astype(int)

            if seasonality == "woy":
                k2 = dd[date_col].dt.isocalendar().week.astype(int)
            else:
                k2 = dd[date_col].dt.month.astype(int)

            dd["y_anom"] = dd[y_var] - dd.groupby(k2)[y_var].transform("mean")
            gg1 = dd.loc[dd["ext_lag"] == 1, "y_anom"].to_numpy()
            gg0 = dd.loc[dd["ext_lag"] == 0, "y_anom"].to_numpy()

            if len(gg1) < 5 or len(gg0) < 20:
                continue
            boot_ep.append(float(gg1.mean() - gg0.mean()))

        boot_ep = np.array(boot_ep)
        ci_eps = np.quantile(boot_ep, [0.025, 0.975]) if len(boot_ep) else np.array([np.nan, np.nan])
    else:
        ci_eps = np.array([np.nan, np.nan])

    return {
        "threshold_quantile": quantile,
        "threshold_value": thr,
        "n_total": n_total,
        "n_extreme_weeks": n_ext_weeks,
        "n_episodes": n_episodes,
        "max_episode_weeks": max_ep_weeks,
        "lag": lag,
        "seasonality": seasonality,
        "delta_mean_y_anom": d_mean,
        "ci95_week_lo": float(ci_weeks[0]),
        "ci95_week_hi": float(ci_weeks[1]),
        "ci95_episode_lo": float(ci_eps[0]),
        "ci95_episode_hi": float(ci_eps[1]),
        "B": B,
        "start_date": start_date or "",
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--island", required=True, help="e.g., gcan, tfe, lpa")
    ap.add_argument("--master", required=True, help="Path to island master parquet/csv")
    ap.add_argument("--xvar", required=True, help="Exposure variable, e.g. PM10")
    ap.add_argument("--yvar", default="deaths_week", help="Outcome variable")
    ap.add_argument("--quantile", type=float, default=0.95, help="Extreme threshold quantile, e.g. 0.95")
    ap.add_argument("--lag", type=int, default=2, help="Lag in weeks (0,1,2,...)")
    ap.add_argument("--seasonality", choices=["woy", "month"], default="woy")
    ap.add_argument("--start_date", default="2018-01-01", help="Filter start date (YYYY-MM-DD). Use '' for none.")
    ap.add_argument("--week_start_unit", choices=["ms", "s", "iso"], default="ms",
                    help="How to parse week_start in master: ms (epoch ms), s (epoch seconds), iso (already datetime/ISO).")
    ap.add_argument("--outdir", default="reports/tables", help="Base output dir")
    ap.add_argument("--B", type=int, default=3000)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    df = read_any(Path(args.master))

    # Parse week_start
    if "week_start" not in df.columns:
        raise KeyError("Master missing 'week_start'")

    if args.week_start_unit == "ms":
        df["week_start"] = pd.to_datetime(df["week_start"], unit="ms", errors="coerce")
    elif args.week_start_unit == "s":
        df["week_start"] = pd.to_datetime(df["week_start"], unit="s", errors="coerce")
    else:
        df["week_start"] = pd.to_datetime(df["week_start"], errors="coerce")

    start_date = args.start_date if args.start_date.strip() else None

    res = extreme_week_audit_one(
        df=df,
        x_var=args.xvar,
        y_var=args.yvar,
        quantile=args.quantile,
        lag=args.lag,
        seasonality=args.seasonality,
        start_date=start_date,
        B=args.B,
        seed=args.seed,
    )

    outdir = Path(args.outdir) / "island" / args.island / "extremes"
    outdir.mkdir(parents=True, exist_ok=True)

    qtag = f"q{int(args.quantile*100)}"
    fname = f"extreme_audit_{args.xvar}_{qtag}_lag{args.lag}_{args.seasonality}"
    if start_date:
        fname += f"_{start_date.replace('-','')}_plus"
    fname += ".csv"

    out_fp = outdir / fname
    pd.DataFrame([res]).to_csv(out_fp, index=False)

    print("Wrote:", out_fp.resolve())
    print("Summary:", {k: res[k] for k in [
        "threshold_value", "n_total", "n_extreme_weeks", "n_episodes", "max_episode_weeks",
        "delta_mean_y_anom", "ci95_week_lo", "ci95_week_hi", "ci95_episode_lo", "ci95_episode_hi"
    ]})


if __name__ == "__main__":
    main()