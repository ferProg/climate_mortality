from pathlib import Path
import pandas as pd


# ============================================================
# CONFIGURACIÓN
# ============================================================

PROJECT_ROOT = Path(r"C:\Users\fdora\RA_Career\Projects\climate_mortality")

INPUT_BASE_DIR = PROJECT_ROOT / "data" / "processed"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed" / "regional"
OUTPUT_FILE = OUTPUT_DIR / "master_regional_byisland_2009_2025.parquet"

START_DATE = pd.Timestamp("2009-01-01")

ISLANDS = [
    "tenerife",
    "gran_canaria",
    "lanzarote",
    "fuerteventura",
    "la_palma",
    "gomera",
    "hierro",
]

ISLAND_CODES = {
    "tenerife": "tfe",
    "gran_canaria": "gcan",
    "lanzarote": "lzt",
    "fuerteventura": "ftv",
    "la_palma": "lpa",
    "gomera": "gom",
    "hierro": "hie",
}

# Proxy source mapping:
# - Hierro uses Gomera calima proxy by project decision.
CALIMA_PROXY_SOURCES = {
    "tenerife": {"folder": "tenerife", "code": "tfe"},
    "gran_canaria": {"folder": "gran_canaria", "code": "gcan"},
    "lanzarote": {"folder": "lanzarote", "code": "lzt"},
    "fuerteventura": {"folder": "fuerteventura", "code": "ftv"},
    "la_palma": {"folder": "la_palma", "code": "lpa"},
    "gomera": {"folder": "gomera", "code": "gom"},
    "hierro": {"folder": "gomera", "code": "gom"},
}

CALIMA_PROXY_COLUMNS = [
    "week_start",
    "calima_proxy_score",
    "calima_proxy_level",
]


# ============================================================
# FUNCIONES AUXILIARES
# ============================================================

def find_date_column(df: pd.DataFrame) -> str:
    """
    Detecta automáticamente una columna de fecha.
    Ajusta esta lista si tus datasets usan otro nombre.
    """
    possible_date_columns = [
        "fecha",
        "date",
        "week",
        "week_start",
        "start_date",
        "time",
        "period",
    ]

    for col in possible_date_columns:
        if col in df.columns:
            return col

    raise ValueError(
        "No se encontró columna de fecha. "
        f"Columnas disponibles: {list(df.columns)}"
    )


def read_filter_island_dataset(island: str, code: str) -> pd.DataFrame:
    """
    Lee el parquet maestro de una isla, filtra desde 2009-01-01
    y añade columnas de identificación de isla.
    """
    input_file = (
        INPUT_BASE_DIR
        / island
        / "master"
        / f"master_{code}_2004_2025.parquet"
    )

    if not input_file.exists():
        raise FileNotFoundError(f"No existe el archivo esperado: {input_file}")

    print(f"\nLeyendo master: {input_file}")

    df = pd.read_parquet(input_file)

    date_col = find_date_column(df)
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

    rows_before = len(df)

    df = df[df[date_col] >= START_DATE].copy()

    rows_after = len(df)

    df["island"] = island
    df["island_code"] = code

    print(f"Columna de fecha detectada: {date_col}")
    print(f"Filas master antes del filtro: {rows_before}")
    print(f"Filas master después del filtro desde {START_DATE.date()}: {rows_after}")

    return df


def read_filter_calima_proxy(island: str) -> pd.DataFrame:
    """
    Lee el calima proxy correspondiente a una isla, filtra desde 2009-01-01
    y etiqueta el proxy con la isla objetivo.

    Caso especial:
    - hierro usa el proxy de gomera, pero se etiqueta como island='hierro'
      para poder hacer merge con el master regional.
    """
    if island not in CALIMA_PROXY_SOURCES:
        raise KeyError(f"No hay configuración de calima proxy para la isla: {island}")

    proxy_cfg = CALIMA_PROXY_SOURCES[island]
    source_folder = proxy_cfg["folder"]
    source_code = proxy_cfg["code"]

    proxy_file = (
        INPUT_BASE_DIR
        / source_folder
        / "calima"
        / f"calima_proxy_weekly_{source_code}_2004_2025.parquet"
    )

    if not proxy_file.exists():
        raise FileNotFoundError(f"No existe el archivo de calima proxy esperado: {proxy_file}")

    print(f"\nLeyendo calima proxy para {island}: {proxy_file}")

    df_proxy = pd.read_parquet(proxy_file)

    missing_columns = [
        col for col in CALIMA_PROXY_COLUMNS
        if col not in df_proxy.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Faltan columnas en el proxy de {island}: {missing_columns}. "
            f"Columnas disponibles: {list(df_proxy.columns)}"
        )

    df_proxy = df_proxy[CALIMA_PROXY_COLUMNS].copy()

    df_proxy["week_start"] = pd.to_datetime(df_proxy["week_start"], errors="coerce")

    rows_before = len(df_proxy)

    df_proxy = df_proxy[df_proxy["week_start"] >= START_DATE].copy()

    rows_after = len(df_proxy)

    # Etiquetamos con la isla objetivo, no necesariamente con la isla fuente.
    # Ejemplo: hierro usa archivo de gomera, pero island='hierro'.
    df_proxy["island"] = island

    print(f"Fuente proxy: {source_folder} / {source_code}")
    print(f"Filas proxy antes del filtro: {rows_before}")
    print(f"Filas proxy después del filtro desde {START_DATE.date()}: {rows_after}")
    print(f"Fecha mínima proxy: {df_proxy['week_start'].min()}")
    print(f"Fecha máxima proxy: {df_proxy['week_start'].max()}")

    return df_proxy


def merge_master_with_calima(df_master: pd.DataFrame, df_proxy: pd.DataFrame, island: str) -> pd.DataFrame:
    """
    Une el master insular con el calima proxy por week_start + island.
    """
    if "week_start" not in df_master.columns:
        date_col = find_date_column(df_master)
        if date_col != "week_start":
            df_master = df_master.rename(columns={date_col: "week_start"})

    df_master["week_start"] = pd.to_datetime(df_master["week_start"], errors="coerce")
    df_proxy["week_start"] = pd.to_datetime(df_proxy["week_start"], errors="coerce")

    rows_before = len(df_master)

    df_merged = df_master.merge(
        df_proxy,
        on=["week_start", "island"],
        how="left",
        validate="one_to_one",
    )

    rows_after = len(df_merged)

    if rows_before != rows_after:
        raise ValueError(
            f"El merge cambió el número de filas para {island}: "
            f"antes={rows_before}, después={rows_after}"
        )

    missing_score = df_merged["calima_proxy_score"].isna().sum()
    missing_level = df_merged["calima_proxy_level"].isna().sum()

    print(f"\nMerge master + calima proxy para {island}: OK")
    print(f"Filas tras merge: {rows_after}")
    print(f"Nulls calima_proxy_score: {missing_score}")
    print(f"Nulls calima_proxy_level: {missing_level}")

    return df_merged


# ============================================================
# PROCESO PRINCIPAL
# ============================================================

def main() -> None:
    print("=" * 80)
    print("BUILD MASTER REGIONAL BY ISLAND + CALIMA PROXY")
    print("=" * 80)

    island_dfs = []

    for island in ISLANDS:
        print("\n" + "-" * 80)
        print(f"Procesando isla: {island}")
        print("-" * 80)

        code = ISLAND_CODES[island]

        df_island = read_filter_island_dataset(island, code)
        df_proxy = read_filter_calima_proxy(island)

        df_island_merged = merge_master_with_calima(
            df_master=df_island,
            df_proxy=df_proxy,
            island=island,
        )

        island_dfs.append(df_island_merged)

    print("\nConcatenando datasets insulares...")

    master_regional = pd.concat(
        island_dfs,
        ignore_index=True,
        sort=False,
    )

    master_regional = master_regional.sort_values(
        ["island", "week_start"]
    ).reset_index(drop=True)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    master_regional.to_parquet(
        OUTPUT_FILE,
        index=False,
    )

    print("\n" + "=" * 80)
    print("DATASET REGIONAL CREADO")
    print("=" * 80)

    print("\nArchivo guardado en:")
    print(OUTPUT_FILE)

    print("\nColumnas del dataset final:")
    for col in master_regional.columns:
        print(f"- {col}")

    print("\nNúmero total de filas:")
    print(len(master_regional))

    print("\nFilas por isla:")
    print(master_regional["island"].value_counts().sort_index())

    print("\nResumen calima_proxy_level por isla:")
    if "calima_proxy_level" in master_regional.columns:
        print(
            master_regional
            .groupby(["island", "calima_proxy_level"], dropna=False)
            .size()
            .unstack(fill_value=0)
        )

    print("\nNulls por columna:")
    nulls = master_regional.isna().sum().sort_values(ascending=False)
    print(nulls)

    print("\nResumen de fechas:")
    try:
        date_col = find_date_column(master_regional)
        print(f"Columna de fecha: {date_col}")
        print(f"Fecha mínima: {master_regional[date_col].min()}")
        print(f"Fecha máxima: {master_regional[date_col].max()}")
    except ValueError as error:
        print(f"No se pudo calcular resumen de fechas: {error}")

    print("\nChequeo específico de calima proxy:")
    print(f"Nulls calima_proxy_score: {master_regional['calima_proxy_score'].isna().sum()}")
    print(f"Nulls calima_proxy_level: {master_regional['calima_proxy_level'].isna().sum()}")

    print("\nProceso completado correctamente.")


if __name__ == "__main__":
    main()