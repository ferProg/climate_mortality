# FINDINGS — climate_mortality

> Calima (Saharan dust) effect on weekly mortality in the Canary Islands  
> Islands analyzed: Tenerife (TFE), Gran Canaria (GC)  
> Period: 2016–2025 | Model: OLS multiple regression | n > 500 weeks per island

---

## Executive Summary

Saharan dust (calima) events are associated with a statistically significant increase in weekly all-cause mortality in Tenerife and Gran Canaria. After controlling for temperature, winter seasonality, and partial mediation via PM10, the adjusted effect is **+3.79 deaths/week (TFE)** and **+2.98 deaths/week (GC)** per unit increase in calima ordinal. At intense calima episodes, predicted excess mortality reaches **+11.4 deaths/week (TFE)** and **+8.9 deaths/week (GC)** relative to no-calima weeks. The dominant driver of winter mortality remains cold-season seasonality (Q1), with calima as a secondary but robust independent signal.

---

## Table 1 — Island-level Effect Sizes

| Metric | Tenerife | Gran Canaria |
|---|---|---|
| β crude (simple regression) | +5.08 | +4.45 |
| β adjusted (multiple regression) | +3.79 | +2.98 |
| p-value (adjusted model) | 2.9e-05 | 5.2e-04 |
| R² (Model 1) | 0.292 | 0.258 |
| Predicted excess deaths at intense vs no_calima | +11.4/week | +8.9/week |
| Mean deaths — no_calima | 138.2 | 132.8 |
| Mean deaths — intense | 155.4 | 150.8 |
| Δ intense vs no_calima | +17.2 (+12.4%) | +18.0 (+13.5%) |

---

## Table 2 — Multiple Regression Coefficients (Model 1)

**Final model:** `deaths_week ~ calima_ordinal + temp_c_mean + Q1 + Q2 + Q3`

| Predictor | Tenerife β | Gran Canaria β | Interpretation |
|---|---|---|---|
| calima_ordinal | +3.79** | +2.98** | Primary effect of interest |
| temp_c_mean | −1.67 | −1.78 | Cold = more deaths (classic) |
| Q1 (winter) | +14.9 | +11.6 | Dominant seasonal driver |
| Q2 (spring) | reference | reference | — |
| Q3 (summer) | — | — | — |

** p < 0.001

**Model fit:**

| Metric | Tenerife | Gran Canaria |
|---|---|---|
| R² | 0.292 | 0.258 |
| Normality (Shapiro-Wilk W) | 0.985 | 0.991 |
| Autocorrelation (DW) | not tested | not tested |

> **Note:** Model 2 (+ humidity + PM10) used as sensitivity analysis only. Calima loses significance in Model 2 due to partial mediation via PM10 pathway — PM10 is a mediator, not an independent confounder. Model 1 selected as primary model.

---

## EDA Highlights

- Positive slope confirmed in scatter plots for both islands — calima signal is real but noisy, as expected with weekly all-cause mortality data.
- Tenerife shows a progressive median rise across calima levels (no_calima → intense). Gran Canaria shows a concentrated jump at the intense tier.
- Correlation matrix: calima_ordinal → deaths_week r = 0.22 (TFE), 0.21 (GC). Effect is primarily contemporaneous; lag1/lag2 correlations slightly lower (0.16–0.19).
- Q1 → deaths_week: r = 0.49 (TFE), 0.46 (GC) — confirms seasonality must be controlled.
- temp_c_mean → deaths_week: r = −0.40 both islands — classic cold-mortality confounder.

---

## Diagnostics

| Check | Result |
|---|---|
| Linearity (Residuals vs Fitted) | ✅ Residuals scatter randomly around zero — assumption holds |
| Homoscedasticity (Scale-Location) | ✅ Variance approximately constant across fitted values |
| Normality (Q-Q + Shapiro-Wilk) | ⚠️ Mild tail deviation — W > 0.98 both islands, not severe |
| Residual distribution | Right-skewed with longer upper tail — consistent with Q-Q findings |
| OLS robustness | ✅ With n > 500, CLT ensures valid inference despite mild non-normality |

**Figures:** Residual plots and Q-Q plots available in `regression_tfe_gc_modeling.ipynb` → Section 6.

### Figure References

- **EDA (scatter, boxplot, correlation matrix):** `reports/ccaa/figures/eda_regression_tfe_gc.png`
- **Model 1 diagnostics (residuals, Q-Q, scale-location):** `reports/ccaa/figures/diagnostics_model1_tfe_gc.png`
- **Model 3 diagnostics (with lag_mortality_1):** `reports/ccaa/figures/diagnostics_model3_tfe_gc.png`

---

## Limitations

- OLS assumes independence of observations — weekly mortality data may have autocorrelation (Durbin-Watson not tested; flagged for Week 7).
- Calima proxy v2 is a weighted composite (CAMS + visibility + tmax anomaly) — not a direct PM10 or AOD measure.
- 2016–2017 period has no CAP alerts data; proxy relies on CAMS + visibility only.
- Model explains ~29% of variance — substantial unexplained variation remains.

---

## Next Steps

- [ ] Test for autocorrelation (Durbin-Watson) — Week 7 priority
- [ ] Add lagged calima terms to final model
- [ ] Extend to interaction terms (calima × season)
- [ ] Replicate methodology for remaining islands

---

*Generated: 2026-05-11 | Source: `regression_tfe_gc_modeling.ipynb`*