# Heat, Calima and Weekly Mortality — Santa Cruz de Tenerife (2016–2025)

This project studies how **weather (AEMET)** and **Saharan dust / particulate pollution (PM10/PM2.5)** relate to **weekly mortality**.

It is designed as a reproducible pipeline: **scripts build datasets**, **notebooks analyze them**, and **reports** store the publication-ready outputs.

---

## Research questions

1) **Heat / weather vs mortality (weekly)**  
   - Are hotter weeks associated with higher mortality?
   - Do effects appear with delays (lags)?

2) **Calima / PM episodes vs mortality (weekly)**  
   - Do dust episodes (PM peaks, coarse fraction) show association with mortality?

3) **Combined model**  
   - When controlling for weather + seasonality, does calima add explanatory power?

---

## Data sources

- **AEMET OpenData**: daily climatological values for station **C429I (Tenerife Sur airport)**  
  Variables include: tmed, tmax, tmin, precipitation, humidity, pressure, wind, etc.

- **INE**: weekly deaths (EDeS) filtered to **province 38: Santa Cruz de Tenerife**, period encoded as `YYYYSMWW`.

- **Canary Islands Air Quality Network**: hourly air quality tables (annual workbooks), station used here for PM series:
  - **Mercado Central (Gran Canaria)** as a **regional proxy** for dust episodes, with PM10 and PM2.5 available for **2016–2024** (validated yearly files).

> Note: PM data currently covers **2016–2024** (validated annual packages).  
> Mortality + AEMET weather cover **2016–2025**.

---

## Project structure
heat_mortality_tenerife/
├── data/
│ ├── raw/ # downloads as-is (do not edit)
│ ├── interim/ # partially cleaned (daily, standardized)
│ └── processed/ # final weekly datasets used for EDA/modeling
├── logs/ # download + sanity checks (row counts, coverage)
├── src/ # reproducible pipeline scripts
├── notebooks/ # exploration & figures (reads processed/ only)
├── reports/
│ ├── figures/ # plots for publication
│ └── tables/ # summary tables
├── README.md
└── requirements.txt


---

## Outputs (what this repo produces)

### Core weekly datasets
- `data/processed/tfe_deaths_weather_weekly_2016_2025_C429I.csv`  
  Weekly mortality merged with weekly weather.

- `data/processed/calima_pm_daily_2016_2024_mercado_central.csv`  
  Daily PM aggregates + episode flags.

- `data/processed/calima_pm_weekly_2016_2024_mercado_central_episodes.csv`  
  Weekly PM features including:
  - `calima_days_peak80` (main calima metric)
  - `calima_days_peak100`
  - `calima_days_coarse20`, `calima_days_coarse40max`
  - `pm10_max_week`, `coarse_max_week`, etc.

### Final merged dataset (produced later)
- `data/processed/tfe_deaths_weather_calima_weekly_2016_2024.csv`  
  Weekly mortality + weather + calima/PM features.

---

## Reproducible pipeline (scripts)

Scripts are numbered; run in order. Each script writes to `data/*` and logs checks to `logs/`.

### `src/01_ingest_aemet.py`
Downloads AEMET daily climatological values for station `C429I` (Tenerife Sur) for 2016–2025.

**Produces:**
- `data/raw/aemet_C429I_daily_2016_2025.csv`

### `src/01_ingest_ine.py`
Loads INE EDeS weekly deaths for Santa Cruz de Tenerife and converts `YYYYSMWW → week_start (Monday)`.

**Produces:**
- `data/raw/ine_deaths_sc_weekly_2016_2025.csv`

### `src/02_clean_standardize.py`
Standardizes numeric formats, dates, column names, and removes obviously non-data rows.

**Produces:**
- `data/interim/aemet_C429I_daily_clean.csv`
- `data/interim/ine_deaths_weekly_clean.csv`

### `src/03_aggregate_weekly.py`
Aggregates daily → weekly for AEMET and computes weekly coverage.

**Produces:**
- `data/processed/aemet_C429I_weekly_2016_2025.csv`

### `src/04_merge_weather_mortality.py`
Merges INE weekly deaths with AEMET weekly weather.

**Produces:**
- `data/processed/tfe_deaths_weather_weekly_2016_2025_C429I.csv`

### `src/01_ingest_air_quality.py`
Reads annual air-quality workbooks and extracts the station sheet (Mercado Central). Standardizes hourly PM10/PM2.5.

**Produces:**
- `data/interim/pm_hourly_mercado_central_2016_2024.csv`

### `src/03_aggregate_pm_daily_weekly.py`
Builds daily and weekly PM features + calima episode flags.

**Produces:**
- `data/processed/calima_pm_daily_2016_2024_mercado_central.csv`
- `data/processed/calima_pm_weekly_2016_2024_mercado_central_episodes.csv`

### `src/05_merge_all.py` (later)
Merges weekly mortality+weather with weekly calima PM features (2016–2024 overlap).

**Produces:**
- `data/processed/tfe_deaths_weather_calima_weekly_2016_2024.csv`

---

## Notebooks (analysis only)

Notebooks read from `data/processed/` and write figures/tables to `reports/`.

- `notebooks/01_eda_climate_vs_mortality.ipynb`
- `notebooks/02_eda_calima_vs_mortality.ipynb`
- `notebooks/03_eda_merged.ipynb`

Rule: **No downloading or core cleaning in notebooks.**

---

## How to run

### 1) Create environment
'''
python -m venv .venv
# Windows
.venv\Scripts\activate
pip install -r requirements.txt
python src/01_ingest_aemet.py
python src/01_ingest_ine.py
python src/02_clean_standardize.py
python src/03_aggregate_weekly.py
python src/04_merge_weather_mortality.py

python src/01_ingest_air_quality.py
python src/03_aggregate_pm_daily_weekly.py

python src/05_merge_all.py
'''

### Notes on methodology (current decisions)

Weekly alignment: weeks start on Monday (week_start) for consistent merges.

Calima main metric: daily peak episodes aggregated to weekly:

calima_days_peak80 = number of days/week with PM10_max_day ≥ 80 µg/m³ (main).

Also kept as secondary:

WHO / EU / ICA thresholds based on PM10 daily mean.

Coarse fraction proxies (PM10 − PM2.5) to better isolate dust vs urban fine aerosols.

## Contact / license

This is an open research-style project. If you reuse the pipeline or results, cite the data sources and this repository.
