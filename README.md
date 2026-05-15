# Environmental Factors and Mortality: Heat and Dust Analysis

## Overview

This project explores the relationship between weekly mortality, heat, and dust exposure using official public datasets.

It is designed as a structured, reproducible analytical workflow rather than a single exploratory notebook.

The Canary Islands are used as a case study due to their frequent Saharan dust intrusions ("calima"), strong seasonal patterns, and relatively well-defined environmental conditions, which make exposure patterns easier to observe at an island level.

## Quick Navigation

| Document | Description |
|---|---|
| [FINDINGS.md](FINDINGS.md) | Executive summary — regression results, effect sizes, diagnostics |
| [REPRODUCIBILITY.md](REPRODUCIBILITY.md) | How to reproduce the full pipeline from source data |
| [VALIDATION.md](VALIDATION.md) | Calima proxy validation — AUC, CAP alignment, QA results |
| `regression_tfe_gc_modeling.ipynb` | Full regression notebook (TFE + GC) |
| `islands/tenerife/` | Island-level EDA — Tenerife |
| `islands/gran_canaria/` | Island-level EDA — Gran Canaria |



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

## Findings (updated 2026-04-29)

Island-level analysis using Calima Proxy v2 (normalised score [0–1], incorporating visibility, PM10, PM2.5, humidity, and temperature anomaly; AUC 0.886 vs CAP+DAI validation) across all six islands, 2016–2025.

**Island-level results (EDA Calima-Mortality v2):**

| Island | Δ deaths/week (intense calima) | ANOVA p | η² | Conclusion |
|--------|-------------------------------|---------|-----|------------|
| Tenerife | +17.19 | <0.001 | 0.054 | Strong signal |
| Gran Canaria | +17.97 | <0.001 | 0.058 | Strong signal |
| Lanzarote | +2.16 | 0.043 | 0.016 | Marginal signal |
| La Palma | +1.12 | 0.617 | 0.003 | No signal |
| Gomera | — | — | — | Analysis discontinued |
| Fuerteventura | — | — | — | Analysis discontinued |

**Population-adjusted comparison (δ deaths per 100k inhabitants):**

| Island | δ/100k |
|--------|--------|
| Gran Canaria | +2.101 |
| Tenerife | +1.868 |
| Lanzarote | +1.394 |
| La Palma | +1.350 |
| Gomera | +0.269 |
| Fuerteventura | +0.194 |

Convergence of +1.35–2.10 per 100k across the four largest islands suggests a genuine per-capita mechanism independent of population size.

**Seasonality (Tenerife + Gran Canaria):** The calima-mortality association is not confounded by seasonal patterns. Both a same-week effect (lag0, r=0.221) and a delayed two-week effect (lag2, r=0.192–0.205) are present, consistent with a dual mechanism of acute exacerbation and delayed inflammatory response.

**Provincial-level analysis (Phase 2, completed 2026-04-29):**

Calima Proxy v2 extended to provincial scale with consistent methodology:

| Province | Δ deaths/week (intense) | η² | Lag0 | Lag2 | Status |
|----------|------------------------|-----|------|------|--------|
| SC Tenerife (TFE + La Palma + Gomera) | +17.88 | **0.0563** | 0.056 | 0.041 | ✅ Phase 2 complete |
| Las Palmas (GC + Lanzarote + FTV) | — | — | — | — | ⏳ Phase 3 in progress |

**Key finding:** Provincial η² (0.0563) ≈ insular η² (TFE 0.0541, GC 0.0058) → signal strengthens or maintains (NOT diluted) with aggregation. Confirms genuine per-capita mechanism across scales.

All findings are descriptive and associational, not causal.

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

## Phases (updated 2026-04-29)

**Phase 1 — Island-level EDA:** ✅ Complete (Apr 26)
- 6 islands, Calima Proxy v2 (AUC 0.886), consistent methodology across islands
- Strong signal: Tenerife (η²=0.054), Gran Canaria (η²=0.058); marginal/no signal: smaller islands

**Phase 2 — Provincial-level EDA:** ✅ Complete (Apr 29)
- SC Tenerife provincial (TFE + La Palma + Gomera): η²=0.0563 (NOT diluted vs insular)
- Calima Proxy v2 extended to provincial scale with population-weighted aggregation
- Methodology blocker resolved: proxy now includes continuous score + categorical levels (consistent with island-level)

**Phase 3 — Las Palmas provincial:** ⏳ In progress
- Master generated (Apr 28), EDA template validated (Apr 29)
- Ready to replicate SC Tenerife template

**Phase 4–5:** ⏳ Pending
- CCAA-level EDA (expected May 2)
- Synthesis + regression specification (expected May 2)

| Master | Filas | Deaths nulls | Calima nulls | Intense episodes |
|--------|-------|--------------|--------------|-----------------|
| `master_provincial_sc_tenerife_2016_2025.parquet` | 522 | 0 | 0 | 49 |
| `master_provincial_las_palmas_2016_2025.parquet` | 522 | 0 | 0 | 46 |
| `master_ccaa_canarias_2016_2025.parquet` | 522 | 0 | 0 | 58 (any) / 37 (both) |

CCAA master includes 4 calima columns: `calima_sct`, `calima_lp`, `calima_any`, `calima_both`.

Deaths/100k ranges: SC Tenerife 10.56–23.55 · Las Palmas 9.53–21.28 · CCAA 10.49–21.67.

Scripts: `src/ingests/provinces/` + `src/master/ccaa/`

## Current status and next steps

**🟢 PROJECT PHASE: CLOSURE AND PUBLICATION (as of May 7, 2026)**

Analytical work is **complete**:
- ✅ Island-level EDA (Phase 1): all 6 islands analyzed
- ✅ Provincial-level EDA (Phase 2): SC Tenerife η² = 0.0563
- ✅ CCAA-level EDA (Phase 4): η² multinivel table complete  
- ✅ Synthesis + regression decision (Phase 5): island-level regression specified (TFE + GC)
- ✅ Feature engineering (May 5): calima_level categories, lags, seasonality dummies
- ✅ Data verification (May 7): CSV lock complete, codebook complete

**Regresión modeling** (simple + multiple + diagnostics + model comparison) was completed on **May 5** in `regression_tfe_gc_modeling.ipynb`.

**Regression validation (May 14, 2026):**
- Model 3 (lag predictor) confirmed for both TFE and GC
- Autocorrelation fix: DW 0.79→2.30 (Tenerife), 0.93→2.36 (Gran Canaria) ✅
- Lag-1 mortality: significant predictor (β>0, p<0.05) for both islands
- Calima ordinal effect confirmed: β=+2.93 (TFE, p<0.05), β=+1.77 (GC, p<0.05)
- R² ≈ 0.464 (TFE), 0.486 (GC) with autocorrelation control
- Model diagnostics: Shapiro-Wilk W>0.99, DW≥2.30, AIC/BIC compared
- **Alert closed:** DW autocorrelation issue fully resolved

**Next steps for publication:**
1. Review project structure and documentation completeness
2. Compile write-up / analytical summary
3. Organize figures and tables for presentation
4. Finalize repository structure for public release
5. Peer review / final verification before publication

## License

This project is released under the MIT License.