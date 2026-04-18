# Environmental Factors and Mortality: Heat and Dust Analysis

## Overview

This project explores the relationship between weekly mortality, heat, and dust exposure using official public datasets.

It is designed as a structured, reproducible analytical workflow rather than a single exploratory notebook.

The Canary Islands are used as a case study due to their frequent Saharan dust intrusions ("calima"), strong seasonal patterns, and relatively well-defined environmental conditions, which make exposure patterns easier to observe at an island level.



## Why this project matters

Extreme heat and dust events are recurrent in the Canary Islands and may affect health outcomes through multiple pathways, including thermal stress, reduced visibility, and degraded air quality.


## Research question

Do weeks with higher heat or stronger calima-related conditions tend to show higher mortality than baseline weeks?

Secondary questions include:
- whether these relationships appear mainly in extremes rather than average weeks
- whether the signal differs across islands
- whether dust and heat overlap or confound each other

## Data sources

This project integrates multiple official and research-relevant sources:

- **INE** — weekly mortality data  
  https://www.ine.es/dynt3/inebase/index.htm?padre=6480&capsel=6480

- **AEMET OpenData** — meteorological observations  
  https://opendata.aemet.es/opendata/api

- **AEMET CAP alerts** — official alert data retrieved via the AEMET OpenData API  
  Main API: https://opendata.aemet.es/  
  Documentation: https://opendata.aemet.es/opendata/documentation/swagger-ui.html  
  Endpoint used in this project:  
  https://opendata.aemet.es/opendata/api/avisos_cap/archivo/fechaini/{fechaini}/fechafin/{fechafin}`

- **Canary Islands Air Quality Network (Gobierno de Canarias)** — validated station-level air quality data (including PM10 / PM2.5) downloaded from the historical data portal  
  https://www3.gobiernodecanarias.org/medioambiente/calidaddelaire/datosHistoricosForm.do

- **NOAA NCEI Integrated Surface Database (Global Hourly / station files)** — airport visibility observations used as a dust-proxy support source  
  [ADD LINK: https://www.ncei.noaa.gov/products/land-based-station/integrated-surface-database]

## Acknowledgements

Calima-related datasets and methodological references used in this project are partially based on resources provided in the following Heliyon publication:

- https://www.cell.com/heliyon/fulltext/S2405-8440(24)07293-1

The original authors are acknowledged for making these data available.  
Please refer to the publication for full methodology and attribution.

## Background and motivation

Calima events are a distinctive and recurrent feature of Canary Islands climate and public life. They can coincide with high temperatures, reduced visibility, and poor air quality, making them relevant for environmental-health analysis.

This repository was built as a structured analytical project to investigate whether these conditions are associated with changes in weekly mortality patterns across islands, while keeping the limits of observational data explicit.

## Related literature

This project is informed by existing environmental-health research on dust exposure, heat, air pollution, and mortality, including work relevant to Saharan dust episodes and the Canary Islands context.


## Repository structure

- `/src` → ingestion, aggregation, QA, and master dataset scripts  
- `/islands` → island-level notebooks and island-specific README files  
- `/reports` → generated figures and tables  
- `/data` → local-only raw/interim/processed datasets (not published)

## Island analyses

Each island folder contains a dedicated notebook and a short README summarising data coverage, variables, findings, and caveats.

Current island-level analyses:

- Tenerife → `islands/tenerife/`
- Gran Canaria → `islands/gran_canaria/`
- La Palma → `islands/la_palma/`
- Gomera → `islands/gomera/`
- Lanzarote → `islands/lanzarote/`
- Fuerteventura → `islands/fuerteventura/`

## Analytical approach

At a high level, the workflow is:

1. ingest official source data
2. clean and validate each source
3. aggregate to weekly resolution
4. build island-level merged datasets
5. run QA checks and exploratory analysis
6. generate figures and summary tables

The emphasis is on transparency, structured QA, and cautious interpretation rather than overstated claims.

The repository combines data ingestion, weekly aggregation, quality checks, island-level exploratory analysis, and generated figures/tables. The current focus is descriptive and analytical rather than causal: the goal is to identify patterns worth understanding while being explicit about uncertainty, missingness, and confounding.

## Initial findings

Early exploratory results suggest that:

- Higher mortality tends to coincide with stronger dust-related conditions
- CAP yellow dust-alert weeks often coincide with higher average deaths
- PM10 and dust-level indicators show positive associations with mortality in several island-level analyses
- simple linear temperature relationships can be weak or misleading, likely due to seasonality and non-linearity

These findings are descriptive, not causal.

## EDA Status (updated 2026-04-16)

| Island | Master built | EDA done | Key findings |
|---|---|---|---|
| Tenerife | ✅ `master_tfe_2016_2025.parquet` (522 semanas, 49 cols) | ✅ Apr 13 | Δ muertes calima intensa vs no_calima: **+18.72/week**. Corr muertes/tmax anomalía: 0.054 (señal débil). Outputs: 6 tablas + 3 figs en `reports/islands/`. |
| Gran Canaria | ✅ | ✅ Apr 14 | Lag0 r=0.197. ANOVA F=15.57, p<0.001, η²=0.0827. Δ intense vs no_calima: **+12.88/week**. Δ intense vs possible: +21.17. Patrón no-monotónico. |
| Lanzarote | ⏳ Pendiente | ⏳ Pendiente | — |
| Fuerteventura | ✅ | ✅ Apr 16 | Seasonality mensual + trimestral documentada. **Lag2 = efecto principal** (calima → mortalidad con 2 semanas de retraso). Year-over-year documentado. Narrativa + executive summary completados. Notebook: `eda_seasonality_ftv.ipynb`. |
| La Palma | ⏳ Pendiente | ⏳ Pendiente | — |
| Gomera | ⏳ Pendiente | ⏳ Pendiente | — |

**Note (Apr 13):** Master datasets desaparecidos → pipeline reconstruido completo antes de iniciar EDA. Pipeline ahora estable y reproducible end-to-end.

## Limitations

- the project currently works at **weekly** resolution, which limits temporal precision
- strong seasonality can confound simple associations
- CAP alerts are only usable from 2018 onward in the current workflow
- some island-level variables have important gaps or uneven coverage
- DAI-based dust coverage is unavailable after March 2022 in the current project workflow
- island-specific data quality and variable availability differ, so comparisons must be interpreted carefully

## Reproducibility and data availability

Raw, interim, and processed datasets are intentionally not included in the public repository.

The repo is designed to publish:
- source code
- island-level analysis notebooks
- selected generated figures and summary tables

The scripts in `/src` document how the analytical datasets were built from source materials.

## How to navigate this repository

Start from the island-level analyses:

- Tenerife → `islands/tenerife/`
- Gran Canaria → `islands/gran_canaria/`

Each island contains a dedicated notebook and summary.

## Next steps

- extend cross-island comparison
- strengthen interpretation around confounding, missingness, and data quality

## License

This project is released under the MIT License.
