'''
Para esta variable, este script merge el master con calima_proxy y luego compara
-> hay que pasarles como argumentos donde se encuentra el calima_proxy y el master parquets
Lo Que Hace Este Script
No es una "validación que rechaza/acepta". Es un diagnóstico.
Lee: qa_calima_proxy_score_v2.py → genera reportes que te muestran:

¿Está el proxy completo? (missing_pct, missing_by_year, missing_by_month)
¿Qué rango tiene? (mean, min, max, p95, p99)
¿Correlaciona con lo que debería? (proxy_checks: cap_dust, PM10, low_vis, deaths)
¿Se mueve temporalmente como esperamos? (lead/lag correlations con deaths, ajustado por estacionalidad)

Luego tú miras los CSVs y decides: "Esto tiene sentido" o "Algo está roto."
'''

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def read_any(p: Path) -> pd.DataFrame:
    return pd.read_parquet(p) if p.suffix.lower() == ".parquet" else pd.read_csv(p)


def summarize_numeric(s: pd.Series) -> dict:
    s = s.dropna()
    if s.empty:
        return {"n": 0, "mean": np.nan, "median": np.nan, "p90": np.nan, "p95": np.nan, "p99": np.nan,
                "min": np.nan, "max": np.nan, "nunique": 0}
    return {
        "n": int(s.size),
        "mean": float(s.mean()),
        "median": float(s.median()),
        "p90": float(s.quantile(0.90)),
        "p95": float(s.quantile(0.95)),
        "p99": float(s.quantile(0.99)),
        "min": float(s.min()),
        "max": float(s.max()),
        "nunique": int(s.nunique()),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", required=True, help="Path to weekly master parquet/csv")
    ap.add_argument("--calima", required=True, help="Path to calima proxy weekly parquet/csv")
    ap.add_argument("--island", required=True, help="e.g., gcan, tfe")
    ap.add_argument("--var", default="calima_proxy_score")
    ap.add_argument("--outdir", default="reports/tables")
    args = ap.parse_args()

    fp_master = Path(args.master)
    fp_calima = Path(args.calima)
    outdir = Path(args.outdir) / "island" / args.island
    outdir.mkdir(parents=True, exist_ok=True)

    m = read_any(fp_master)
    c = read_any(fp_calima)

    var = args.var

    # Required columns
    if "week_start" not in m.columns:
        raise KeyError("Master missing required column: week_start")
    if "week_start" not in c.columns:
        raise KeyError("Calima proxy missing required column: week_start")
    if var not in c.columns:
        raise KeyError(f"Calima proxy missing required column: {var}")

    # Types
    m["week_start"] = pd.to_datetime(m["week_start"], unit="ms", errors="coerce")
    c["week_start"] = pd.to_datetime(c["week_start"], errors="coerce")

    # Keep only needed calima columns
    keep = ["week_start", var]
    if "calima_proxy_level_v2" in c.columns:
        keep.append("calima_proxy_level_v2")
    c = c[keep].copy()

    # Merge on week_start (master timeline)
    df = m.merge(c, on="week_start", how="left")

    # ---------- 1) Coverage + missing ----------
    qa = {}
    qa["rows_total"] = int(len(df))
    qa["date_min"] = str(df["week_start"].min())
    qa["date_max"] = str(df["week_start"].max())
    qa["missing_pct"] = float(df[var].isna().mean())

    df["year"] = df["week_start"].dt.year
    df["month"] = df["week_start"].dt.month

    miss_by_year = (
        df.groupby("year")[var]
          .apply(lambda s: float(s.isna().mean()))
          .reset_index(name="missing_pct")
    )
    miss_by_month = (
        df.groupby("month")[var]
          .apply(lambda s: float(s.isna().mean()))
          .reset_index(name="missing_pct")
    )

    # ---------- 2) Distribution ----------
    stats = summarize_numeric(df[var])

    # ---------- 3) Convergent validity vs proxies (if present) ----------
    proxy_rows = []
    proxies = [
        "cap_dust_yellow_plus_week",
        "pm10_p95_flag",
        "PM10",
        "low_vis_any_week",
        "low_vis_flag",
        "deaths_week",
    ]

    for p in proxies:
        if p not in df.columns:
            continue

        tmp = df[["week_start", var, p]].dropna()
        if tmp.empty:
            continue

        # binary-ish proxies
        if p in ["cap_dust_yellow_plus_week", "pm10_p95_flag", "low_vis_any_week", "low_vis_flag"]:
            grp = tmp.groupby(p)[var].agg(["count", "mean", "median"]).reset_index()
            grp.insert(0, "proxy", p)
            proxy_rows.append(grp)
        else:
            corr = float(tmp[var].corr(tmp[p]))
            proxy_rows.append(pd.DataFrame([{"proxy": p, "corr": corr, "n": len(tmp)}]))

    proxy_report = pd.concat(proxy_rows, ignore_index=True) if proxy_rows else pd.DataFrame()

        # ---------- 4) Temporal sanity: lead/lag correlations with deaths ----------
    lag_report = []
    lag_report_anom = []

    if "deaths_week" in df.columns:
        tmp = df[["week_start", var, "deaths_week"]].dropna().sort_values("week_start").copy()

        # ---- raw lead/lag corr ----
        for k in [0, 1, 2]:
            tmp[f"{var}_lag{k}"] = tmp[var].shift(k)
            lag_report.append({
                "shift": f"lag{k}",
                "corr_with_deaths": float(tmp["deaths_week"].corr(tmp[f"{var}_lag{k}"]))
            })

        for k in [1, 2]:
            tmp[f"{var}_lead{k}"] = tmp[var].shift(-k)
            lag_report.append({
                "shift": f"lead{k}",
                "corr_with_deaths": float(tmp["deaths_week"].corr(tmp[f"{var}_lead{k}"]))
            })

        # ---- woy anomalies lead/lag corr ----
        # woy = ISO week number (1-52/53). We de-seasonalize by subtracting mean within woy.
        woy = tmp["week_start"].dt.isocalendar().week.astype(int)

        tmp["deaths_anom_woy"] = tmp["deaths_week"] - tmp.groupby(woy)["deaths_week"].transform("mean")
        tmp[f"{var}_anom_woy"] = tmp[var] - tmp.groupby(woy)[var].transform("mean")

        for k in [0, 1, 2]:
            tmp[f"{var}_anom_lag{k}"] = tmp[f"{var}_anom_woy"].shift(k)
            lag_report_anom.append({
                "shift": f"lag{k}",
                "corr_with_deaths_anom_woy": float(tmp["deaths_anom_woy"].corr(tmp[f"{var}_anom_lag{k}"]))
            })

        for k in [1, 2]:
            tmp[f"{var}_anom_lead{k}"] = tmp[f"{var}_anom_woy"].shift(-k)
            lag_report_anom.append({
                "shift": f"lead{k}",
                "corr_with_deaths_anom_woy": float(tmp["deaths_anom_woy"].corr(tmp[f"{var}_anom_lead{k}"]))
            })

    lag_report = pd.DataFrame(lag_report)
    lag_report_anom = pd.DataFrame(lag_report_anom)

    # ---------- write outputs ----------
    qa_row = {**qa, **{f"stat_{k}": v for k, v in stats.items()}}
    qa_df = pd.DataFrame([qa_row])

    qa_df.to_csv(outdir / f"qa_{var}_{args.island}.csv", index=False)
    miss_by_year.to_csv(outdir / f"qa_{var}_{args.island}_missing_by_year.csv", index=False)
    miss_by_month.to_csv(outdir / f"qa_{var}_{args.island}_missing_by_month.csv", index=False)
    proxy_report.to_csv(outdir / f"qa_{var}_{args.island}_proxy_checks.csv", index=False)
    if not lag_report.empty:
        lag_report.to_csv(outdir / f"qa_{var}_{args.island}_leadlag_corr.csv", index=False)

    if not lag_report_anom.empty:
        lag_report_anom.to_csv(outdir / f"qa_{var}_{args.island}_leadlag_corr_anom_woy.csv", index=False)

    print("Wrote QA tables to:", outdir.resolve())
    print("Main QA file:", (outdir / f"qa_{var}_{args.island}.csv").name)


if __name__ == "__main__":
    main()