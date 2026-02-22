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

