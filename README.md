# Environmental Factors and Mortality: Heat and Dust Analysis

## Overview

This project investigates whether weeks with stronger Saharan dust (calima) conditions are associated with higher all-cause mortality across the Canary Islands, using official mortality, meteorological, and air quality data for the period 2009–2025.

The Canary Islands are used as a case study due to their frequent calima intrusions, strong seasonal patterns, and relatively well-defined environmental conditions, which make exposure patterns easier to observe at island, provincial, and regional scales.

The repository is designed as a structured, reproducible analytical workflow: ingestion → cleaning → validation → weekly aggregation → regression modeling across four geographic scales.

## Quick Navigation

| Document | Description |
|---|---|
| [FINDINGS.md](FINDINGS.md) | Full results — regression coefficients, effect sizes, diagnostics, all scales |
| [REPRODUCIBILITY.md](REPRODUCIBILITY.md) | How to reproduce the full pipeline from source data |
| [VALIDATION.md](VALIDATION.md) | Calima proxy v4 validation — AUC, CAP alignment, QA |
| `CCAA/regression/regression_regional.ipynb` | Regional regression notebook (Canarias CCAA) |
| `CCAA/regression/regression_tfe_gc_modeling.ipynb` | Island-level regression notebook (TFE + GC) |
| `reports/final/FIGURES_INDEX.md` | Portfolio figures index with narrative order |

## Abstract

Calima events — Saharan dust intrusions — are a recurrent feature of Canary Islands climate. This study investigates whether weeks with stronger calima conditions are associated with higher all-cause mortality across the six main islands, using official mortality (INE), meteorological (AEMET), and air quality data for the period 2009–2025.

A composite calima proxy (v4, **AUC = 0.932**) was constructed via logistic regression from PM10, PM2.5, and visibility, calibrated against DAI (Heliyon) and CAP (AEMET) ground truth. Island-level analysis shows excess mortality of +17–18 deaths/week during intense calima episodes in Tenerife and Gran Canaria (η² ≈ 0.054–0.058, p < 0.001), with consistent per-capita effects across islands (+1.4–2.1 per 100,000). Multiple regression controlling for temperature and mortality autocorrelation confirms the calima effect independently at every geographic scale (island, province, CCAA). At regional scale: **β = +3.51 deaths/week per calima level** (p = 0.001, R² = 0.746). Demographic normalization (deaths/100k) confirms the signal is not explained by population growth (+7.1%, 2009–2025): **β = +0.18/100k/week** (p < 0.001, R² = 0.715). All findings are observational.

## Key Findings

**Multi-scale calima effect (2009–2025):**

| Scale | Model | β calima | p-value | R² | DW | n |
|---|---|---|---|---|---|---|
| Island — Tenerife | OLS lag | +2.93 (ordinal) | <0.001 | 0.464 | 2.303 | 522 |
| Island — Gran Canaria | OLS lag | +1.77 (ordinal) | <0.001 | 0.486 | 2.355 | 522 |
| Province — SC Tenerife | FD HC3 | +7.48 (score) | 0.014 | 0.024 | 2.98 | 886 |
| Province — Las Palmas | FD HC3 | +2.50 (score) | 0.429 ❌ | 0.018 | 2.89 | 886 |
| Regional — Canarias (v4) | OLS HC3 | +3.51 (ordinal) | 0.001 | 0.746 | 2.550 | 886 |
| Regional — normalized | OLS HC3 | +0.18 /100k/week | <0.001 | 0.715 | 2.512 | 886 |

⚠️ β magnitudes not directly comparable across scales (different outcome variables and proxy encodings).

**Population-adjusted effect by island (intense vs no_calima, EDA):**

| Island | δ/100k |
|--------|--------|
| Gran Canaria | +2.101 |
| Tenerife | +1.868 |
| Lanzarote | +1.394 |
| La Palma | +1.350 |
| Gomera | +0.269 |
| Fuerteventura | +0.194 |

Convergence of +1.35–2.10/100k across the four largest islands suggests a genuine per-capita mechanism independent of population size.

![Cross-island calima-mortality effect (normalized)](reports/islands/figures/cross_island/cross_island_delta_normalized.png)
*Per-capita calima-related mortality change across the six main Canary Islands.*

## Data Sources

- **INE** — weekly all-cause mortality data  
  https://www.ine.es/dynt3/inebase/index.htm?padre=6480&capsel=6480

- **AEMET OpenData** — meteorological observations  
  https://opendata.aemet.es/opendata/api

- **AEMET CAP alerts** — official dust/heat alert data  
  https://opendata.aemet.es/opendata/api/avisos_cap/archivo/fechaini/{fechaini}/fechafin/{fechafin}

- **Canary Islands Air Quality Network (Gobierno de Canarias)** — PM10/PM2.5 station data  
  https://www3.gobiernodecanarias.org/medioambiente/calidaddelaire/datosHistoricosForm.do

- **NOAA NCEI Integrated Surface Database** — airport visibility observations  
  https://www.ncei.noaa.gov/products/land-based-station/integrated-surface-database

- **ISTAC Padrón Municipal** — official annual population (Canarias 2009–2025)  
  https://www.gobiernodecanarias.org/istac/

## Acknowledgements

Calima-related datasets and methodological references are partially based on:

> Heliyon (2024): https://www.cell.com/heliyon/fulltext/S2405-8440(24)07293-1

The original authors are acknowledged for making these data available.

## Related Literature

- **Domínguez-Rodríguez et al. (2020):** 86% of in-hospital heart failure deaths in Tenerife occurred during Saharan dust episodes with PM10 > 50 µg/m³ (OR = 2.79, adjusted). This project extends that finding to all-cause weekly mortality at population level.

- **Díaz et al. (2017):** PM10 during Saharan dust intrusions associates with daily mortality across Spanish regions — an effect absent on non-dust days.

- **Pérez et al. (2012):** Cardiovascular mortality effects during Saharan dust days roughly double those on non-dust days (Barcelona, case-crossover). The lag0 + lag2 structure observed here is consistent.

**Positioning:** Existing studies use daily resolution, cause-specific mortality, and direct PM measurements. This project contributes a weekly all-cause analysis at island, provincial, and regional scale (2009–2025), with a validated composite proxy (AUC = 0.932) and explicit autocorrelation control — not previously applied to this geographic context.

## Repository Structure

```
climate_mortality/
├── src/               → pipeline scripts (ingestion, aggregation, QA, master build)
├── CCAA/              → CCAA-level analysis and regression notebooks
├── islands/           → island-level EDA notebooks and READMEs
├── provinces/         → provincial regression notebooks
├── reports/
│   ├── final/         → portfolio figures (fig01–fig08) + FIGURES_INDEX.md
│   ├── ccaa/          → CCAA figures and calibration plots
│   ├── islands/       → island and cross-island figures
│   └── provinces/     → provincial figures
├── data/              → local only (not published — reproduce via pipeline)
├── FINDINGS.md        → full regression results and diagnostics
├── REPRODUCIBILITY.md → pipeline reproduction guide
└── VALIDATION.md      → proxy validation results
```

## Analysis Phases

| Phase | Description | Status |
|---|---|---|
| 1 | Island-level EDA — 6 islands, proxy v2 (AUC 0.886) | ✅ Complete |
| 2 | Provincial EDA — SC Tenerife + Las Palmas | ✅ Complete |
| 3 | CCAA-level EDA | ✅ Complete |
| 4 | Island regression — TFE + GC, Model 3 with lag (DW 2.30–2.36) | ✅ Complete |
| 5 | Provincial regression — P2 first-difference HC3 | ✅ Complete |
| 6 | Proxy v4 calibration — logistic regression, AUC 0.932 | ✅ Complete |
| 7 | Regional master + regression — 6 islands aggregated, R²=0.746 | ✅ Complete |
| 8 | Demographic normalization — deaths/100k, ISTAC Padrón 2009–2025 | ✅ Complete |
| 9 | Las Palmas lag analysis — lag0/lag1/lag2 all non-significant; null result confirmed | ✅ Complete |

## Limitations

- Weekly resolution limits temporal precision relative to daily studies
- Autocorrelation addressed via `deaths_lag1` predictor (DW improved from ~0.8 to 2.30–2.55)
- Las Palmas province shows no significant signal at any lag (0–2 weeks) at provincial scale — null result robust across all specifications; signal concentrated in Gran Canaria island (β=+1.77, p<0.001)
- CAP alerts available only from 2018; proxy calibration uses DAI + CAP combined
- Some island-level variables have coverage gaps (Gomera PM10 45% nulls, interpolated)
- All findings are observational; causal inference requires further study

## Reproducibility

Raw, interim, and processed datasets are not included. The scripts in `/src` document how all analytical datasets were built from the source materials listed above. See [REPRODUCIBILITY.md](REPRODUCIBILITY.md) for the full pipeline walkthrough.

## License

This project is released under the MIT License.
