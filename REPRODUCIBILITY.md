# REPRODUCIBILITY.md

> **Project:** climate_mortality  
> **Last updated:** 2026-05-12  
> **Purpose:** Enable independent reproduction of the full pipeline and regression analysis from source data.

---

## Data Sources

| Source | Variables | URL |
|---|---|---|
| INE | Weekly mortality by island | https://www.ine.es/dynt3/inebase/index.htm?padre=6480&capsel=6480 |
| AEMET OpenData | Temperature, humidity, wind, pressure | https://opendata.aemet.es/opendata/api |
| AEMET CAP Alerts | Official dust/heat alerts (from March 2018) | https://opendata.aemet.es/opendata/api/avisos_cap/archivo/fechaini/{fechaini}/fechafin/{fechafin} |
| Gobierno de Canarias | PM10, PM2.5, SO2, NO2 (2016–2024) | https://www3.gobiernodecanarias.org/medioambiente/calidaddelaire/datosHistoricosForm.do |
| Copernicus CAMS | PM10, PM2.5 (2025 only) | https://ads.atmosphere.copernicus.eu |
| NOAA ISD | Airport visibility observations | https://www.ncei.noaa.gov/products/land-based-station/integrated-surface-database |
| Heliyon / DAI | External calima dust index (2016–2022) | https://www.cell.com/heliyon/fulltext/S2405-8440(24)07293-1 |

> **Note:** Raw data files are not included in this repository. The `/src` scripts document how each source dataset was ingested.

---

## Environment Setup

**Python version:** 3.10+

**Install dependencies:**
```bash
pip install -r requirements.txt
```

**requirements.txt:**
```

pandas==2.3.3
numpy==2.4.1
pyarrow==23.0.1
matplotlib==3.10.8
matplotlib-inline==0.2.1
seaborn==0.13.2
statsmodels==0.14.6
scipy==1.17.0
scikit-learn==1.8.0
openpyxl==3.1.5
requests==2.32.5
requests-cache==1.3.0
```

**API keys required:**
```powershell
# AEMET API key (required for weather + CAP ingestion)
$env:AEMET_API_KEY="<your_key>"
```

AEMET API keys are free — register at https://opendata.aemet.es/

---

## Step-by-Step Execution

The pipeline runs in 7 sequential steps. Steps 1–5 are independent and can run in parallel. Steps 6–7 require all prior steps to be complete.

Replace `<island>` with one of: `tenerife`, `gran_canaria`, `lanzarote`, `la_palma`, `gomera`, `fuerteventura`.

### Step 1 — Mortality (INE)
```powershell
python src\ingests\deaths\build_deaths_weekly_by_island.py <island_code> --start_year 2016 --end_year 2025 --data "./data"
```
Output: `data/processed/<island>/deaths/deaths_weekly_<code>_2016_2025.parquet`

### Step 2 — Weather (AEMET)
```powershell
$env:AEMET_API_KEY="<your_key>"
python -m src.ingests.weather.run_weather_pipeline --station <station> --start 2016-01-01 --end 2025-12-31 --island <island>
```
Output: `data/processed/<island>/weather/weather_weekly_<code>_2016_2025.parquet`

Station codes per island:
| Island | Station |
|---|---|
| Tenerife | C429I |
| Gran Canaria | C649I |
| Lanzarote | C029O |
| Fuerteventura | C249I |
| La Palma | C139E |
| Gomera | C329B |

### Step 3 — Air Quality
```powershell
# Step 3a — build daily
python -m src.ingests.airq.build_airq_daily --island <island_code> --start-year 2016 --end-year 2024 --root "C:\data\Air_Polution_GC_2015_2025_raw\Datos2016_2025" --outdir "data\interim\air_q"

# Step 3b — aggregate to weekly
python -m src.ingests.airq.build_weekly_airq_island --island <island> --daily-dir "data\interim\air_q" --processed-dir "data\processed"
```
Output: `data/processed/<island>/air_quality/weekly_<code>_2016_2025.parquet`

> **Note:** 2025 air quality data sourced from Copernicus — see `src/ingests/airq/build_cams_2025_airq_excel.py`.

### Step 4 — Visibility (NOAA)
```powershell
python -m src.ingests.visibility.run_island_pipeline --isla <island> --start_date 2016-01-01 --end_date 2025-12-31
```
Output: `data/interim/<island>/visibility/step4_weekly/visibility_weekly_<code>_2016_2025.parquet`

> **Tenerife note:** Use station `GCTS` (TFS, south airport) to avoid fog noise from TFN (north).

### Step 5 — CAP Alerts (AEMET)
```powershell
# Step 5a — download (only if year is missing from interim)
python -m src.ingests.cap.aemet_cap_alerts_ingest_chunks_generic --start 2016-01-01 --end 2025-12-31 --step-days 7 --sleep 0.5 --resume

# Step 5b — extract by island
python -m src.ingests.cap.extract_canarias_avisos_by_island --isla <island> --data-dir "data\interim\cap\canarias" --start 2016-01-01 --end 2025-12-31 --save-alerts
```
Output: `data/processed/<island>/cap/cap_weekly_<code>_2016_2025.parquet`

> **Note:** CAP alerts only available from March 2018 onwards.

### Step 6 — Master Dataset
```powershell
python -m src.master.build_master_all_islands --island <island> --start-year 2016 --end-year 2025
```
Output: `data/processed/<island>/master/master_<code>_2016_2025.parquet`

### Step 7 — Calima Proxy v2
```powershell
python src/master/calima_per_island/build_calima_proxy_v2.py --island <island> --start-year 2016 --end-year 2025
```
Output: `data/processed/<island>/calima/calima_proxy_weekly_<code>_2016_2025.parquet`

---

## Regression Analysis

Once the pipeline is complete for Tenerife and Gran Canaria:

1. Open `CCAA/regression/regression_tfe_gc_modeling.ipynb`
2. Verify data path: `data/processed/provinces/regression_tfe_gc_2016_2025.parquet`
3. Run all cells in order (kernel: Python 3.10+)

The notebook covers:
- Data load and audit
- Feature engineering (lags, seasonality dummies, calima encoding)
- EDA (scatter, boxplots, correlation matrix)
- Simple regression (Model 0)
- Multiple regression Models 1–3
- Autocorrelation check (Durbin-Watson)
- Residual diagnostics

---

## Expected Outputs

| Output | Location |
|---|---|
| Master dataset (TFE+GC) | `data/processed/provinces/regression_tfe_gc_2016_2025.parquet` |
| Regression features dataset | `data/processed/provinces/regression_tfe_gc_features_2016_2025.parquet` |
| EDA figures | `reports/ccaa/figures/eda_regression_tfe_gc.png` |
| Diagnostics M1 | `reports/ccaa/figures/diagnostics_model1_tfe_gc.png` |
| Diagnostics M3 | `reports/ccaa/figures/diagnostics_model3_tfe_gc.png` |

---

## Estimated Runtime

| Step | Estimated time |
|---|---|
| Steps 1–5 (per island) | 5–15 min (API-dependent) |
| Step 6 — Master build | < 1 min |
| Step 7 — Calima proxy | < 1 min |
| Regression notebook | 2–5 min |

> API steps (AEMET weather, CAP) are the bottleneck — runtime depends on AEMET server response.

---

## Known Limitations

- Raw data files are not included in the repository — must be downloaded from original sources
- Air quality pipeline lacks a single orchestrator script — steps must be run manually (see Step 3)
- CAP alerts unavailable before March 2018 — proxy relies on CAMS + visibility for 2016–2017
- DAI (Heliyon dust index) only available up to March 2022
- Copernicus 2025 air quality ingestion steps are partially documented — see `src/ingests/airq/`
