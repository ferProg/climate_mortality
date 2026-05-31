# Figures Index — climate_mortality portfolio

**Project:** Calima (Saharan dust) effect on weekly mortality in the Canary Islands  
**Period:** 2009–2025 | **Model:** OLS HC3 with lag | **Proxy:** v4 (AUC = 0.932)

---

## Portfolio figures (`figures/`)

| File | Title | Use |
|---|---|---|
| `fig01_pipeline.svg` | Data pipeline architecture | LinkedIn / README — shows end-to-end pipeline from raw sources to regression |
| `fig02_proxy_calibration_roc.png` | Proxy v4 ROC curve (AUC = 0.932) | Methods section — classifier performance vs DAI + CAP ground truth |
| `fig03_deaths_timeseries_regional.png` | Weekly deaths time series — Canarias 2009–2025 | Context / intro — establishes scale and trend of outcome variable |
| `fig04_mortality_rate_100k.png` | Deaths per 100,000 inhabitants (normalized) | Methods — shows demographic-controlled variable; justifies normalization |
| `fig05_boxplot_calima_deaths_regional.png` | Deaths by calima level — regional boxplot | Key result — visualizes the calima–mortality association directly |
| `fig06_cross_island_delta_normalized.png` | Cross-island Δ deaths normalized | Results — per-capita effect consistent across islands (+1.4–2.1/100k) |
| `fig07_diagnostics_model_final.png` | Model M3b diagnostics (residuals, Q-Q, DW) | Methods/diagnostics — confirms OLS assumptions met, DW = 2.51 |
| `fig08_multiscale_comparison.png` | Calima effect across scales (island → province → CCAA) | Summary — shows signal at every geographic level |

**Narrative order for LinkedIn post:** fig01 → fig02 → fig03 → fig05 → fig06 → fig08

---

## Appendix (`appendix/islands/`)

Exploratory figures for smaller islands — available for supplementary material but not in primary portfolio.

| Folder | Island | Figures |
|---|---|---|
| `gomera/` | La Gomera | EDA, seasonality, calima distribution, lag analysis |
| `lanzarote/` | Lanzarote | EDA, seasonality, calima distribution, lag analysis |
| `fuerteventura/` | Fuerteventura | EDA, seasonality, calima distribution, lag analysis |
| `la_palma/` | La Palma | EDA, seasonality, calima distribution, lag analysis |
| `tenerife_eda_legacy/` | Tenerife (early EDA) | Legacy exploratory figures from proxy v2 phase |

---

*Last updated: 2026-05-31*
