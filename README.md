# Heat & Weekly Mortality — Santa Cruz de Tenerife (2016–2025)

This project studies the relationship between **weekly mortality (INE)** and **weather (AEMET)** in the province of **Santa Cruz de Tenerife (Canary Islands, Spain)**.  
The repository is organized as a reproducible workflow: **scripts → datasets**, **notebooks → analysis**, **reports → figures/tables**.

> Status: the repository is not public yet. **Datasets (CSV/JSON)** are intentionally **not versioned/published** (see *Data & privacy / Git*).




## Research questions

### 1) Weather / heat vs weekly mortality (2016–2025)
- Do hotter weeks associate with higher weekly mortality?
- Is the relationship linear, or concentrated in extremes (tail risk)?
- Are there short lags (0–2 weeks)?

### 2) Dust / calima vs mortality (planned)
- Dust/PM and official alerts (CAP) will be analyzed in a separate notebook once the weather-only block is finalized.


## Data sources

- **INE (EDeS)**: weekly mortality, filtered to province **38 (Santa Cruz de Tenerife)**.
- **AEMET**: meteorological variables aggregated to weekly level (station **C429I – Tenerife Sur**), including:
  - `tmed_mean`, `tmax_mean`, `tmax_max`, `tmin_mean`, `tmin_min`
  - `prec_sum`, `hr_mean`, `presmax_mean`, `presmin_mean`
  - `wind_mean`, `gust_max`, `sun_sum`

*(BDRC / AEMET Dust THREDDS access requested; pending.)*


## Project structure

```
heat_mortality_analysis/
├── data/
│   ├── raw/
│   ├── interim/
│   └── processed/
├── logs/
├── notebooks/
├── reports/
│   ├── figures/
│   │   └── eda01/
│   └── tables/
│       └── eda01/
└── src/
    └── utils/
```


## Outputs

### Notebook 01 (weather vs mortality)
- Figures: `reports/figures/eda01/`
- Tables: `reports/tables/eda01/`

### Local datasets (not versioned)
- `data/raw/tfe_deaths_weather_weekly_2016_2025_C429I.csv`  
  Weekly deaths + weekly weather (2016–2025).


## Reproducible pipeline (scripts)

**Convention:** numbered scripts under `src/`.  
Scripts were renumbered following a single linear pipeline (“Option A”) to avoid confusing duplicates (e.g., multiple `01_...` files).

- `src/01_ingest_ine.py`  
  Ingests INE weekly deaths and standardizes `week_start`.

- `src/06_aggregate_weather_weekly.py`  
  Aggregates AEMET weather variables to weekly level and computes coverage.

- `src/08_merge_deaths_weather.py`  
  Merges weekly deaths and weekly weather into a single table.

*(Dust-related scripts exist but are out of scope for Notebook 01 and will be used in Notebook 02.)*
- `src/03_ingest_aemet_cap_alerts.py`
- `src/04_ingest_air_quality.py`
- `src/05_build_cap_weekly_flags.py`
- `src/07_aggregate_pm_daily_weekly.py`
- `src/09_build_master_weekly_2018_2024.py`


## Notebooks

- `notebooks/01_eda_climate_vs_mortality.ipynb`  
  **Weather-only EDA (2016–2025)**:
  - Seasonality adjustment via weekly baseline (`deaths_excess`)
  - Rolling smoothing (3-week) for episode visualization
  - QA filtering of physically implausible weather weeks
  - Tail analysis (p95/p99 “heat weeks”) to capture non-linear effects

- `notebooks/02_eda_calima_vs_mortality.ipynb` *(planned)*  
  Dust/PM + CAP alerts vs mortality, including lags and interaction with heat.

> Rule of thumb: notebooks **read** prepared datasets and **write** outputs to `reports/`. Core ingestion/processing happens in `src/`.


## Data quality notes

During EDA01, **14/521 weeks (~2.7%)** in the 2016–2025 deaths+weather weekly dataset were flagged as **physically implausible** (e.g., `tmax_mean < tmin_mean`, pressure outside 900–1100 hPa, extreme gust values).  
For analysis, we created a `df_clean` excluding those weeks and recomputed seasonality baselines.

**Pending:** fix the root cause in the weekly aggregation/merge scripts so the dataset is clean by construction (not just cleaned in-notebook).


## Key findings so far (EDA01)

- Linear correlation between `deaths_excess` and weekly temperature is near zero, suggesting a **non-linear** relationship.
- Extreme heat weeks show higher mean excess mortality (illustrative, not causal):
  - `tmax_mean ≥ p95` → mean excess ~ **+7.7 deaths/week** (n≈26)
  - `tmax_mean ≥ p99` → mean excess ~ **+22.3 deaths/week** (n≈6)  
  *(Interpret with caution due to small sample sizes at p99.)*
- March 2020 peaks appear inconsistent with heat (likely non-climatic shocks), reinforcing the need to control for structural events in later modeling.


## How to run (local)

1) Create/activate your environment and install dependencies.
2) Run the pipeline scripts for ingestion/aggregation/merge.
3) Open notebooks to reproduce EDA and write outputs to `reports/`.

*(Exact commands will be finalized once dependency pinning and run order are fully stabilized.)*


## Data & privacy / Git

- This repo intentionally **does not publish datasets**:
  - `data/raw/`, `data/interim/`, `data/processed/`
  - `*.csv` (globally ignored)
- Recommended publishable artifacts: **figures** (PNG) and non-sensitive summary tables.

> Note: `.gitignore` prevents tracking of new files, but files committed earlier must be removed from tracking with `git rm --cached`.


## Next steps

1) Implement QA rules in the processing pipeline (notebook-independent).
2) Build Notebook 02 (dust/PM/CAP) with the same QA → seasonality → tail/lag approach.
3) Compare contributions of heat vs dust and assess confounding/interaction.

## Notes to include:
## What is “calima” (Saharan dust) in the Canary Islands?

In meteorological terms, *calima* refers to the suspension of very small, non-aqueous particles (aerosols) in the air that produces a hazy atmosphere and reduces horizontal visibility. In the Canary Islands, the term is commonly used for **Saharan mineral dust intrusions** transported from North Africa toward the archipelago under specific synoptic patterns, with impacts on aviation, air quality, and health. :contentReference[oaicite:0]{index=0}

### Observable/measureable signatures

Because “calima” is fundamentally an aerosol/dust phenomenon, the most defensible operational indicators are:

- **Surface particulate matter (PM):** Saharan dust intrusions typically produce noticeable increases in **PM10** (often the most diagnostic) and sometimes **PM2.5**. These variables are widely used in air-quality monitoring and dust-event characterization. :contentReference[oaicite:1]{index=1}  
- **Visibility reduction:** A key hallmark of haze/dust is reduced horizontal visibility. Visibility records (e.g., airport observations) can be used to identify and characterize dust events, including long historical reconstructions. :contentReference[oaicite:2]{index=2}  
- **Column aerosol / dust products (optional but strong):** Reanalysis/forecast products (e.g., CAMS) provide aerosol optical depth (AOD) and dust fields that can support detection and severity grading, especially when surface PM coverage is incomplete. :contentReference[oaicite:3]{index=3}

### Operational definition used in this project (weekly scale)

We define a **weekly calima indicator** as a week in which mineral dust intrusion is likely, based primarily on **PM10/PM2.5 statistics** (e.g., weekly mean, percentiles, maxima, or persistence of daily exceedances). Meteorological variables (temperature, humidity, pressure, wind) are used as *supporting context* and for sensitivity checks, but not as the core detector because they are not specific to dust.

We then validate this operational definition against official AEMET/CAP dust alerts (when available) to quantify agreement and identify systematic mismatches.

# Data Product: Calima/Dust Exposure for Mortality Analysis (SC Tenerife Province)
## 1) Purpose

Create a reproducible, auditable exposure dataset that quantifies Saharan dust (“calima”) conditions on a weekly basis for the Province of Santa Cruz de Tenerife, aligned with weekly mortality data.

## 2) Primary use case

Join (merge) with weekly provincial deaths to study associations between dust exposure and mortality.

Provide a transparent exposure proxy that can later be validated/enriched with CAP alerts and PM (PM10/PM2.5).

## 3) Scope

Geography: Province of Santa Cruz de Tenerife (Tenerife, La Palma, La Gomera, El Hierro).

Period: 2016–2024 (extendable to 2025+ and backward later).

Time basis: UTC, using 13:00 UTC daily observation to match the reference methodology.

## 4) Grain (levels of data)

We ship three related datasets:

** A) Daily station-level (long) **

Grain: (date_utc, airport_station)

Source: NOAA ISD

Fields: vis_m, temp_c, dewpoint_c, rh_pct, minutes_from_13utc, within_time_tolerance

** B) Daily provincial-wide (wide) **

Grain: date_utc

One row per day with columns per station: vis_m_gcxo, rh_pct_gcxo, …

** C) Weekly provincial exposure (final product) **

Grain: week_start (Monday)

Output for analysis/merges with mortality.

** 5) Reference methodology (baseline definition) ** 

Baseline dust detection is based on the paper’s operational rule:

A dust-like observation at an aerodrome occurs when:

horizontal visibility indicates reduced conditions (VIS < 10 km), and

relative humidity is low (RH < 70%) to exclude fog/mist effects,
evaluated at 13:00 UTC.

Important encoding rule (NOAA/aviation data):

visibility == 9999 is treated as ≥ 10 km, not “9,999 m”.

Therefore we implement “VIS < 10 km” as vis_m < 9999 (strict).

Provincial daily dust condition (we will define explicitly):

dust_day_sc = (count(dust_like_airports) >= K) where baseline K=2 (paper-consistent).

** 6) Core output variables (weekly product) **

Final weekly dataset includes:

week_start (datetime, Monday)

dust_any_week_sc (0/1): at least one dust-day that week

dust_days_week_sc (0–7): number of dust-days that week

dust_level_week_sc (ordinal): bins from dust-days (initially 0, 1, 2–3, 4–7)

dust_vis_min_week_m_sc: min daily visibility (during dust-days)

dust_vis_mean_week_m_sc: mean daily visibility (during dust-days)

days_observed_week_sc (coverage): how many days are present in the source

** 7) Quality & coverage rules **

Time alignment: choose the observation closest to 13:00 UTC per station/day.

within_time_tolerance = minutes_from_13utc <= 90 (baseline).

If a day is outside tolerance for a station, that station is treated as missing for that day (does not count toward K).

Week completeness:

week_complete = (days_observed_week_sc == 7) used for “main analysis” vs sensitivity.

** 8) Validation strategy (planned, not blocking) **

We will validate the exposure proxy in layers:

Internal coherence: distributions, seasonality, extremes, station agreement.

Official alerts (AEMET CAP dust): agreement at weekly level.

PM measurements (PM10/PM2.5): dust weeks should show higher PM10, especially coarse fraction.

** 9) Versioning **

Dataset version tagged by:

stations included (GCXO, GCTS, GCLA, GCGM, GCHI),

thresholds (VIS, RH, tolerance, K),

period coverage.

Any change to thresholds increments a version string (e.g., v0.1.0).

