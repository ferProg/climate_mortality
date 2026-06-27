from pathlib import Path
import pandas as pd


# ============================================================
# CONFIGURACIÓN
# ============================================================

PROJECT_ROOT = Path(r"C:\Users\fdora\RA_Career\Projects\climate_mortality")

INPUT_FILE = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "regional"
    / "master_regional_byisland_2009_2025.parquet"
)

OUTPUT_FILE = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "regional"
    / "master_regional_byisland_2009_2025.csv"
)

COLUMNS_V1 = [
    # Identificación temporal y geográfica
    "week_start",
    "year",
    "island",
    "island_code",

    # Mortalidad
    "deaths_week",

    # Calidad / cobertura básica
    "n_days",
    "coverage",

    # Temperatura y clima
    "temp_c_mean",
    "tmax_c_mean",
    "tmax_c_max",
    "tmin_c_mean",
    "tmin_c_min",
    "humidity_mean",
    "wind_ms_mean",
    "prec_sum",

    # Contaminación
    "PM10",
    "PM2.5",
    "NO2",
    "O3",

    # Calima proxy V1
    "calima_proxy_score",
    "calima_proxy_level",
]


# ============================================================
# PROCESO
# ============================================================

def main() -> None:
    print("=" * 80)
    print("EXPORT POWER BI V1 CSV WITH CALIMA PROXY")
    print("=" * 80)

    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"No existe el archivo de entrada: {INPUT_FILE}")

    print("\nLeyendo parquet:")
    print(INPUT_FILE)

    df = pd.read_parquet(INPUT_FILE)

    missing_columns = [col for col in COLUMNS_V1 if col not in df.columns]

    if missing_columns:
        raise ValueError(
            "Faltan columnas esperadas en el dataset:\n"
            + "\n".join(missing_columns)
        )

    df_v1 = df[COLUMNS_V1].copy()

    # Asegurar tipo fecha
    df_v1["week_start"] = pd.to_datetime(df_v1["week_start"], errors="coerce")

    # Orden recomendado para Power BI
    df_v1 = df_v1.sort_values(["island", "week_start"]).reset_index(drop=True)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    df_v1.to_csv(
        OUTPUT_FILE,
        index=False,
        encoding="utf-8-sig"
    )

    print("\nCSV V1 creado correctamente:")
    print(OUTPUT_FILE)

    print("\nFilas:")
    print(len(df_v1))

    print("\nColumnas exportadas:")
    for col in df_v1.columns:
        print(f"- {col}")

    print("\nNulls por columna en CSV V1:")
    print(df_v1.isna().sum().sort_values(ascending=False))

    print("\nResumen de fechas:")
    print(f"Fecha mínima: {df_v1['week_start'].min()}")
    print(f"Fecha máxima: {df_v1['week_start'].max()}")

    print("\nFilas por isla:")
    print(df_v1["island"].value_counts().sort_index())

    print("\nDistribución calima_proxy_level por isla:")
    print(
        df_v1
        .groupby(["island", "calima_proxy_level"], dropna=False)
        .size()
        .unstack(fill_value=0)
    )

    print("\nResumen calima_proxy_score:")
    print(df_v1["calima_proxy_score"].describe())

    print("\nProceso completado.")


if __name__ == "__main__":
    main()