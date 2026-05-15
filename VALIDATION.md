# VALIDATION.md

> **Project:** climate_mortality  
> **Last updated:** 2026-05-12  
> **Purpose:** QA checklist, calima proxy validation, known limitations, and analytical caveats.

---

## QA Checklist

### Master Dataset

| Check | Expected | Status |
|---|---|---|
| No null `week_start` | 0 nulls | ✅ |
| No duplicate weeks per island | 0 duplicates | ✅ |
| Full date range 2016–2025 | 522 weeks | ✅ |
| `deaths_week` nulls | Only starter week (2015-12-28) | ✅ — dropped in regression prep |
| PM10 nulls | Isolated gaps (Copernicus coverage) | ✅ — imputed with island median |
| PM2.5 nulls | Same pattern as PM10 | ✅ — imputed with island median |
| `low_vis_any_days_week` nulls | Isolated gaps | ✅ — imputed with island median |
| Calima proxy nulls | 0 | ✅ |
| Data types | `week_start` as timestamp, numeric columns as float | ✅ |

### Calima Proxy v2

| Check | Result |
|---|---|
| Score range | 0.0 – 4.0 (normalized) |
| Level distribution covers all 4 categories | ✅ confirmed across all islands |
| Correlation score vs PM10 (numeric) | Positive, significant ✅ |
| Score higher when CAP dust alert present | ✅ confirmed |
| AUC vs CAP+DAI validation (2018–2022) | **0.886** |
| Sensitivity | 0.741 |
| Specificity | 0.877 |
| Accuracy | 0.860 |

> Validation period: March 2018 – March 2022 (CAP + DAI overlap window).

### Regression Dataset (TFE + GC)

| Check | Result |
|---|---|
| Shape | 1044 rows × ~30 columns (522 weeks × 2 islands) |
| No nulls after imputation | ✅ |
| `calima_ordinal` encoding | no_calima=0, possible=1, probable=2, intense=3 ✅ |
| Lag features (`calima_lag1`, `calima_lag2`, `deaths_lag1`) | Introduced 2 nulls per island (first rows) — dropped before modeling ✅ |
| Quarter dummies (Q_1–Q_4) | Q_4 as reference category ✅ |

---

## Known Limitations

### Temporal resolution
- Analysis is at **weekly** resolution — intra-week mortality patterns are not captured.
- Acute same-day effects of calima are averaged across the week, potentially attenuating the signal.

### CAP alerts coverage
- CAP dust alerts only available from **March 2018** onwards.
- 2016–2017 calima classification relies on CAMS + visibility only — no official alert validation available for this period.

### DAI coverage
- Heliyon DAI (external dust index used in proxy validation) only available up to **March 2022**.
- Post-2022 proxy performance is not independently validated.

### Air quality data
- 2016–2024: Gobierno de Canarias station data (validated, manual download).
- 2025: Copernicus CAMS reanalysis (modeled, not station-measured) — methodological discontinuity.
- Some islands have PM10 gaps (52 null weeks for Hierro due to missing CAMS source file — documented incident).

### Calima proxy
- Proxy v2 is a composite synthetic indicator, not a direct physical measurement.
- Weights (PM10 ×1.0, visibility ×0.5, humidity ×0.25, tmax_anomaly ×0.5) were calibrated against CAP+DAI — not independently validated on a held-out period.
- Island-specific thresholds (percentile-based) mean the same `intense` label may correspond to different absolute PM10 values across islands.

### Regression model
- OLS assumes linear relationships — non-linear calima effects are not modeled.
- Model explains ~46–49% of variance (R² Model 3) — substantial unexplained variation remains.
- `deaths_lag1` captures general mortality inertia, not calima-specific delayed effects.
- Calima proxy is ordinal (0–3), not continuous — effect sizes between levels may not be uniform.

---

## Analytical Caveats

### Observational design
All findings are **associational, not causal**. The analysis cannot establish that calima causes excess mortality — only that weeks classified as intense calima tend to coincide with higher mortality after controlling for temperature and seasonality.

### Confounding
Temperature and seasonality are the dominant confounders and are controlled in Model 3. However, other potential confounders (healthcare access, population age structure changes over time, influenza seasons, COVID-19 periods) are not explicitly modeled.

### COVID-19 period
The dataset includes 2020–2022, which coincides with the COVID-19 pandemic. Weekly mortality during this period may reflect pandemic effects not captured by the model. No explicit COVID flag is included.

### Sample variance
- Tenerife and Gran Canaria: n=522 weeks each — sufficient for regression.
- Smaller islands (Lanzarote, La Palma, Gomera, Fuerteventura): lower death counts per week → higher variance → reduced statistical power. Regression was not extended to these islands.

### Seasonality
The calima-mortality association is not confounded by seasonal patterns (verified via lag analysis and controlled in Model 3 via quarter dummies). However, calima events are more frequent in winter and spring, which partially overlaps with the high-mortality season.

### Scalability
The project was designed island-first rather than top-down from the autonomous community (CCAA) level — a known structural limitation accepted due to time constraints. Provincial and CCAA-level analyses aggregate island results but do not re-run the full pipeline at higher resolution.
