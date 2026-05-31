# audit_master_regional.py
#
# Audita los masters regionales:
#   - master_regional_2004_2025.parquet
#   - master_regional_2009_2025.parquet
#
# Checks:
#   1. Shape, rango, columnas
#   2. Duplicados y semanas faltantes
#   3. Nulls por columna
#   4. Distribución calima_level y calima_score
#   5. Consistencia deaths_week (rango, outliers, tendencia)
#   6. Variables físicas: rango y outliers
#   7. Flag proxy_reliable
#
# Uso: python -m src.qa.audit_master_regional

from pathlib import Path
import pandas as pd
import numpy as np

REGIONAL_DIR = Path("data/processed/regional")

MASTERS = {
    "2004_2025": REGIONAL_DIR / "master_regional_2004_2025.parquet",
    "2009_2025": REGIONAL_DIR / "master_regional_2009_2025.parquet",
}

PHYS_VARS = [
    "temp_c_mean", "tmax_c_mean", "tmin_c_mean",
    "humidity_mean", "pressure_hpa_mean", "wind_ms_mean",
    "PM10", "PM2.5", "vis_min_m_week",
]

EXPECTED_RANGES = {
    "temp_c_mean":      (10, 35),
    "tmax_c_mean":      (10, 45),
    "tmin_c_mean":      (5,  30),
    "humidity_mean":    (30, 95),
    "pressure_hpa_mean":(990, 1030),
    "wind_ms_mean":     (0,  20),
    "PM10":             (0,  500),
    "PM2.5":            (0,  200),
    "vis_min_m_week":   (0,  35000),
    "deaths_week":      (100, 600),
}


def sep(title: str = "") -> None:
    print(f"\n{'─'*55}")
    if title:
        print(f"  {title}")
        print(f"{'─'*55}")


def audit(name: str, path: Path) -> None:
    print(f"\n{'='*55}")
    print(f"  MASTER REGIONAL — {name}")
    print(f"{'='*55}")

    if not path.exists():
        print(f"  ❌ FICHERO NO ENCONTRADO: {path}")
        return

    df = pd.read_parquet(path)
    df["week_start"] = pd.to_datetime(df["week_start"])

    # 1. Shape y rango
    sep("1. Shape y rango")
    print(f"  Filas     : {len(df):,}")
    print(f"  Columnas  : {len(df.columns)}")
    print(f"  Rango     : {df['week_start'].min().date()} → {df['week_start'].max().date()}")
    print(f"  Columnas  : {list(df.columns)}")

    # 2. Duplicados y semanas faltantes
    sep("2. Integridad temporal")
    dups = df["week_start"].duplicated().sum()
    print(f"  Duplicados     : {dups}  {'✅' if dups == 0 else '❌'}")

    expected = pd.date_range(df["week_start"].min(), df["week_start"].max(), freq="W-MON")
    missing  = expected.difference(df["week_start"])
    print(f"  Semanas esperadas : {len(expected)}")
    print(f"  Semanas presentes : {len(df)}")
    print(f"  Semanas faltantes : {len(missing)}  {'✅' if len(missing) == 0 else '❌ ' + str(missing.date.tolist()[:5])}")

    # 3. Nulls
    sep("3. Nulls")
    nulls = df.isnull().sum()
    nulls = nulls[nulls > 0]
    if nulls.empty:
        print("  Sin nulls ✅")
    else:
        for col, n in nulls.items():
            pct = 100 * n / len(df)
            flag = "❌" if pct > 5 else "⚠️"
            print(f"  {flag} {col:30s}: {n:4d} ({pct:.1f}%)")

    # 4. Calima
    sep("4. Proxy de calima")
    print(f"  Score — min: {df['calima_score'].min():.4f}  max: {df['calima_score'].max():.4f}  "
          f"mean: {df['calima_score'].mean():.4f}")
    print(f"\n  Distribución calima_level:")
    vc = df["calima_level"].value_counts().sort_index()
    for lvl, n in vc.items():
        pct = 100 * n / len(df)
        print(f"    {str(lvl):12s}: {n:4d} semanas ({pct:.1f}%)")

    # Monotonicity check: tasa de calima_label no disponible en master,
    # pero verificamos que el score medio sube por nivel
    print(f"\n  Score medio por nivel:")
    for lvl in ["no_calima", "possible", "probable", "intense"]:
        sub = df[df["calima_level"] == lvl]["calima_score"]
        if len(sub):
            print(f"    {lvl:12s}: mean={sub.mean():.4f}  min={sub.min():.4f}  max={sub.max():.4f}")

    # 5. Deaths
    sep("5. Muertes semanales")
    d = df["deaths_week"].dropna()
    lo, hi = EXPECTED_RANGES["deaths_week"]
    out_of_range = ((d < lo) | (d > hi)).sum()
    print(f"  Media   : {d.mean():.1f}")
    print(f"  Mediana : {d.median():.1f}")
    print(f"  Min     : {d.min():.0f}  Max: {d.max():.0f}")
    print(f"  Rango esperado [{lo}, {hi}]: {out_of_range} semanas fuera  {'✅' if out_of_range == 0 else '⚠️'}")
    print(f"  Total   : {d.sum():,.0f}")

    # Tendencia anual
    df["year"] = df["week_start"].dt.year
    annual = df.groupby("year")["deaths_week"].mean().round(1)
    print(f"\n  Media anual de muertes/semana:")
    for yr, val in annual.items():
        print(f"    {yr}: {val:.1f}")

    # 6. Variables físicas
    sep("6. Variables físicas — rangos")
    for var in PHYS_VARS:
        if var not in df.columns:
            print(f"  ⚠️  {var}: NO DISPONIBLE")
            continue
        s = df[var].dropna()
        lo, hi = EXPECTED_RANGES.get(var, (-np.inf, np.inf))
        out = ((s < lo) | (s > hi)).sum()
        flag = "✅" if out == 0 else "⚠️"
        print(f"  {flag} {var:25s}: min={s.min():.1f}  p50={s.median():.1f}  "
              f"max={s.max():.1f}  out_of_range={out}")

    # 7. Proxy reliable
    sep("7. Flag proxy_reliable")
    vc2 = df["proxy_reliable"].value_counts().sort_index()
    for val, n in vc2.items():
        label = "fiable" if val == 1 else "no fiable (pre-2009)"
        print(f"  {val} ({label}): {n} semanas")

    print(f"\n  ✅ Audit completo — {name}\n")


def main() -> None:
    print("=== audit_master_regional ===")
    for name, path in MASTERS.items():
        audit(name, path)


if __name__ == "__main__":
    main()
