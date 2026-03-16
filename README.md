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

| Island        | Weather station used                        | Visibility station used                     | Rationale                                                                                                                                                                                                                                                               | Confidence     |
| ------------- | ------------------------------------------- | ------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------- |
| Tenerife      | **C429I – Tenerife Sur Aeropuerto**         | **Likely C429I – Tenerife Sur Aeropuerto**  | This is the only station clearly fixed in the project context. AEMET identifies C429I as Tenerife Sur Aeropuerto. Airport stations are also a natural choice for visibility because aeronautical observations are standardised and operationally reliable. ([AEMET][1]) | **High**       |
| Gran Canaria  | **Likely C649I – Gran Canaria Aeropuerto**  | **Likely C649I – Gran Canaria Aeropuerto**  | AEMET identifies C649I as Gran Canaria Aeropuerto. This fits the likely project logic of using airport stations for visibility and possibly also for the core weather series when a stable, official station was preferred. ([AEMET][2])                                | **Medium**     |
| Lanzarote     | **Likely C029O – Lanzarote Aeropuerto**     | **Likely C029O – Lanzarote Aeropuerto**     | AEMET lists C029O as Lanzarote Aeropuerto. If the same airport-based logic was applied consistently across islands, this is the most plausible station for both weather and visibility. ([AEMET][3])                                                                    | **Low–Medium** |
| Fuerteventura | **Likely C249I – Fuerteventura Aeropuerto** | **Likely C249I – Fuerteventura Aeropuerto** | AEMET lists C249I as Fuerteventura Aeropuerto. This is a strong candidate under an airport-first observational rule, especially for visibility. ([AEMET][4])                                                                                                            | **Low–Medium** |
| La Palma      | **Likely C139E – La Palma Aeropuerto**      | **Likely C139E – La Palma Aeropuerto**      | AEMET lists C139E as La Palma Aeropuerto. It is an official long-running station and a plausible candidate if the same island-level rule was used. ([AEMET][5])                                                                                                         | **Low–Medium** |
| La Gomera     | **Likely C329B – La Gomera Aeropuerto** **C449O SanSebastian?**  **a veces aparece como 6000C en listados históricos**   | **Likely C329B – La Gomera Aeropuerto**     | Multiple AEMET climate-monitoring documents identify La Gomera Aeropuerto with code C329B. I did not recover a clean “valores climatológicos” landing page in search, but the code-station match appears repeatedly in official AEMET material.                         |                |

[1]: https://www.aemet.es/es/serviciosclimaticos/datosclimatologicos/valoresclimatologicos?k=&l=C429I&utm_source=chatgpt.com "Valores climatológicos normales: Tenerife Sur Aeropuerto"
[2]: https://www.aemet.es/es/serviciosclimaticos/datosclimatologicos/valoresclimatologicos?k=&l=C649I&utm_source=chatgpt.com "Valores climatológicos normales: Gran Canaria Aeropuerto"
[3]: https://www.aemet.es/es/serviciosclimaticos/datosclimatologicos/valoresclimatologicos?k=coo&l=C029O&utm_source=chatgpt.com "Valores climatológicos normales: Lanzarote Aeropuerto"
[4]: https://www.aemet.es/es/serviciosclimaticos/datosclimatologicos/valoresclimatologicos?k=coo&l=C249I&utm_source=chatgpt.com "Valores climatológicos normales: Fuerteventura Aeropuerto"
[5]: https://www.aemet.es/es/serviciosclimaticos/datosclimatologicos/valoresclimatologicos?l=C139E&utm_source=chatgpt.com "Valores climatológicos normales: La Palma Aeropuerto"

La estación del aeropuerto de La Gomera (C329B) tiene, como mínimo, una presencia consolidada en la red climatológica de AEMET desde hace años y probablemente cobertura útil que se solapa con el periodo 1981–2010; no he podido confirmar la fecha exacta de inicio de la serie.

Weekly visibility calendarization for La Gomera.
The original weekly visibility parquet contained 460 weeks instead of the expected 471 for the analysis window (2015-12-28 to 2024-12-30). All missing weeks were concentrated in spring 2020 (2020-03-30 to 2020-06-08). To maintain alignment with deaths, weather, and other weekly layers, the series was calendarized by inserting the missing weeks as rows with NaN values. These rows were explicitly flagged using visibility_missing_gap_2020 = 1. No synthetic visibility values were imputed.

La Gomera data-recovery notes (spring 2020 gap).
Both the weekly weather and weekly visibility series for La Gomera contained an 11-week gap between 2020-03-30 and 2020-06-08. To preserve weekly alignment across the island master dataset, two different recovery strategies were applied. For weather, the missing weeks were patched using an auxiliary AEMET station from the same island (C329Z, San Sebastián de La Gomera), and the affected rows were explicitly flagged (imputed_from_c329z = 1, donor_station = "C329Z"). For visibility, no synthetic values were imputed; instead, the weekly series was calendarized so that the missing weeks were inserted as explicit rows with NaN values and flagged as visibility_missing_gap_2020 = 1. This preserves a complete weekly timeline while keeping patched and missing observations fully traceable.

Air quality ingestion for La Gomera completed successfully for 2016–2024 with full daily date coverage. PM10 showed near-complete availability and is suitable for downstream weekly aggregation. SO2, NO2 and O3 also showed broadly usable coverage. PM2.5 availability was extremely limited and should not be treated as a robust analytical variable for La Gomera.

La Gomera deaths series calendarization.
The weekly deaths series for La Gomera contained 18 missing weeks within the analysis period, not as explicit null values but as absent weekly rows in the source dataset. To preserve temporal alignment with the other weekly layers in the island master dataset, the deaths series was calendarized to a complete weekly sequence. Missing weeks were inserted as explicit rows with NaN values in deaths_week and flagged as deaths_missing_week = 1. No mortality values were imputed.
Calima proxy validation (La Gomera).

The weekly calima proxy for La Gomera shows a strong monotonic relationship with weekly PM10 levels, supporting its usefulness as an operational dust-intensity proxy. Mean PM10 rises from 33.4 in no_calima weeks to 49.7 in possible, 111.2 in probable, and 200.2 in intense weeks. CAP dust yellow-or-higher alerts are also more concentrated in the higher proxy categories, although CAP coverage remains partial for the earlier part of the series. The pressure component is less cleanly ordered across categories, suggesting that pressure contributes signal but should not be overinterpreted in isolation. Overall, the proxy appears suitable for exploratory analysis, while remaining a derived indicator rather than a direct observation of dust conditions.

Hierro puede quedar demasiado débil para una EDA comparable a Tenerife / Gran Canaria / La Palma / Gomera.

El Hierro deaths coverage.
After fixing the weekly deaths extraction pipeline to preserve calendar continuity, the El Hierro deaths series still shows 61 missing weeks within the 2015–2024 analysis window. These are not pipeline artefacts: they reflect weeks where the underlying INE source leaves the base weekly deaths value blank. As a result, El Hierro should be treated as a high-missingness island and is not a strong candidate for the main comparative EDA.

The Lanzarote weekly deaths series includes 7 flagged missing weeks, all occurring before the modern analytical window. These gaps do not materially affect the main study period and the island remains usable for downstream ingestion and merge steps.
The Lanzarote station-level daily meteorological series (C029O) was successfully aggregated to weekly resolution for 2016–2024. The cleaned weekly output contains 471 unique weeks (2015-12-28 to 2024-12-30), with no duplicated weekly timestamps. The boundary weeks extend slightly beyond the nominal start/end dates because weekly aggregation is Monday-anchored.
Lanzarote weather ingestion passed QC. The weekly dataset is structurally clean (471 weeks, no duplicates, no nulls) and shows no obvious physical inconsistencies in the main meteorological variables.
Lanzarote weekly weather ingestion passed QC. The dataset contains 471 unique weeks with no duplicated timestamps and no missing values in the aggregated variables. Physical checks found no obvious inconsistencies. Coverage is high overall (mean coverage = 0.992); 19 weeks have coverage below 1.0, but most of these retain 6 of 7 daily observations, while the first and last weeks are partially truncated by the analysis boundaries.

The Lanzarote weekly visibility dataset passed initial QC. The file contains 471 unique weekly observations (2015-12-28 to 2024-12-30), with no duplicated timestamps and no missing values in the derived weekly variables. The visibility indicators show substantial week-to-week variation, suggesting that the series contains usable signal rather than being structurally flat or sparse.
Lanzarote air-quality ingestion completed successfully and produced a weekly island-level file with 471 observations (2015-12-28 to 2024-12-30). PM10, SO2 and O3 show complete weekly coverage, whereas PM2.5 and NO2 contain missing weekly values (43 and 17 weeks, respectively). The dataset remains usable for downstream analysis, but pollutant-specific coverage should be considered when defining analytic subsets.
Lanzarote weekly air-quality ingestion passed QC. The dataset contains 471 unique weekly observations with no duplicated timestamps. PM10, SO2 and O3 have complete weekly coverage, while PM2.5 and NO2 show pollutant-specific missingness (43 and 17 weeks, respectively). The missingness is not fully random: PM2.5 has a long gap from mid-2019 to early 2020, and NO2 also shows a shorter block of missing weeks over late 2019 to early 2020. As a result, Lanzarote remains usable for downstream analysis, but PM2.5 and NO2 should be treated as partial-coverage covariates rather than core universally available series.
Lanzarote CAP weekly ingestion passed QC. The resulting file contains 342 unique weekly observations with no missing values in the derived CAP variables. Coverage within the available period is high (mean weekly coverage = 0.994), but the series begins on 2018-06-18, so CAP can only be used in the post-2018 analysis window rather than across the full 2016–2024 period.
The Lanzarote master weekly dataset was successfully built with 471 observations (2015-12-28 to 2024-12-30). Core variables such as deaths, PM10, weather, pressure, and visibility merged cleanly with no missing values. Remaining missingness is consistent with known source limitations: PM2.5 and NO2 show pollutant-specific gaps, CAP variables are unavailable before mid-2018, and DAI-derived calima indicators remain partially unavailable. Overall, Lanzarote is retained as a usable island-level master dataset.
Lanzarote calima proxy generation completed successfully. The weekly proxy shows a balanced distribution across categories (no_calima = 199, possible = 181, probable = 56, intense = 35), indicating that the classification is operationally usable for downstream EDA and comparative analysis.


Fuerteventura weekly deaths ingestion passed QC for the modern analysis window. Although 77 historical weeks are flagged as missing across the full 1974–2026 series, no missing weeks occur within 2016–2024. The island is therefore retained for downstream analysis, with historical pre-2016 incompleteness documented as a legacy coverage limitation rather than a current-period blocker.
Fuerteventura weekly weather ingestion passed QC. The dataset contains 471 unique weekly observations with no duplicated timestamps, no missing values, and no obvious physical inconsistencies in the aggregated meteorological variables. Coverage is high overall (mean coverage = 0.989); 25 weeks have partial coverage, but most retain 6 of 7 daily observations, with only a small number of more truncated weeks and the expected boundary truncation at the start and end of the series.
Fuerteventura visibility ingestion passed QC. The weekly dataset is structurally clean and shows meaningful variation in low-visibility indicators across the study period.

Fuerteventura weekly air-quality ingestion passed QC, but pollutant coverage is uneven. PM10 and O3 are complete at weekly level, whereas PM2.5, SO2 and NO2 show structured periods of missingness rather than isolated random gaps. In particular, major missing blocks occur from late 2019 into 2020 for PM2.5 and SO2, and from late 2020 into mid-2021 for NO2. The island remains usable for downstream analysis, but PM2.5, SO2 and NO2 should be treated as partial-availability covariates rather than core uniformly complete series.
Fuerteventura CAP ingestion passed QC. The weekly CAP file contains 342 unique observations from 2018-06-18 to 2024-12-30, with no duplicated timestamps. As with other islands, CAP is only available for the later analysis window and cannot be used across the full 2016–2024 period.
Fuerteventura master build passed QC. The island is analytically usable, with missingness patterns aligned with known source-level limitations rather than merge failure.
The Fuerteventura weekly calima proxy (v2) was successfully built from the island-level master dataset. The resulting file contains 471 weekly observations and shows a non-degenerate distribution across both proxy scores and categorical levels, indicating that the proxy captures meaningful weekly variation rather than collapsing into a near-constant series.


La Gomera weekly deaths ingestion passed only with a substantial coverage caveat. Although the full 1974–2026 series contains 138 flagged missing weeks, the key issue is that 18 missing weeks remain within the core 2016–2024 analysis window. These gaps are mostly dispersed rather than forming one single catastrophic block, so the island remains usable for descriptive analysis, but its deaths series is materially less complete than those of Lanzarote or Fuerteventura.
For La Gomera, the airport station was not retained as the main meteorological source because its coverage deteriorated after the COVID period. The island-level weekly weather series was therefore rebuilt using station C329Z for the full analysis window to preserve temporal consistency and avoid a hybrid patched series.
For La Gomera, station C329Z was used as the primary meteorological source, but a complete missing block from late 2023 to early 2024 required supplemental weekly data from the airport station (C329B). The final weather series therefore prioritizes temporal continuity by using C329Z as the base record and filling the modern missing block with airport-derived weekly observations.
La Gomera weekly visibility data are retained for analysis, with a moderate completeness caveat. The dataset contains 471 unique weekly observations with no duplicated timestamps, but 11 weeks have missing visibility-derived values. This gap is explicitly tracked by the visibility_missing_gap_2020 flag, so the missingness is known rather than silent. Overall, the series remains usable for descriptive EDA, though less complete than the cleaner visibility series available for some other islands.
CAP data for La Gomera are structurally clean and usable, but only from 2018-06-18 onward.
La Gomera weekly air-quality dataset is usable only with an important pollutant-specific caveat. PM10 is complete across the full weekly series and remains suitable as the main air-quality exposure variable, while SO2, NO2 and O3 are nearly complete. However, PM2.5 is effectively unavailable for most of the study window (433 missing weeks out of 471), so it should not be treated as a generally usable weekly covariate for island-level analysis.

