# Gran Canaria — Island EDA Summary (2016–2025)
**Climate Mortality Project | RA Career**

---

## Coverage

| Field | Value |
|---|---|
| Island | Gran Canaria (`gc`) |
| Input file | `data/processed/gran_canaria/master/master_gc_2016_2025.parquet` |
| Weekly observations | 523 weeks |
| Date range | 2015-12-28 → 2025-12-29 |
| Unit | Weekly all-cause mortality |
| `calima_proxy_score` missing | 0.0% |
| `calima_proxy_level` missing | 0.0% |

---

## Key Findings

### Calima proxy distribution
| Level | Weeks |
|---|---|
| no_calima | 126 |
| possible | 279 |
| probable | 78 |
| intense | 40 |

### Mortality by calima level
| Level | Mean deaths/week | Δ vs no_calima |
|---|---|---|
| no_calima | 140.81 | — |
| possible | ~135 | — |
| probable | ~137 | — |
| intense | 151.57 | **+10.77 (+7.6%)** |

Pattern: `possible` and `probable` remain near baseline. The mortality jump is concentrated at `intense`.

### Temperature signal
- `corr(deaths, tmax_c_mean)` = −0.365 (seasonally confounded)
- `corr(deaths, Tmax anomaly)` = 0.082 (weak)
- Δ deaths heat p95 vs baseline = +0.99 deaths/week (small)

### Regression model (Model 3 — GC)
| Parameter | Value |
|---|---|
| Model | deaths_week ~ calima_ordinal + temp_c_mean + Q_1 + Q_2 + Q_3 + deaths_lag1 |
| β calima_ordinal | +1.77 (p < 0.05) ✅ |
| β deaths_lag1 | +0.56 (p < 0.05) ✅ |
| R² | 0.486 |
| Adj. R² | 0.480 |
| AIC | 4304.58 |
| DW | 2.36 ✅ |

Calima signal survives autocorrelation control. deaths_lag1 is the dominant predictor.

---

## Caveats

- **Non-monotonic gradient:** `possible` and `probable` do not show a clean step-up above baseline — the mortality elevation is concentrated at `intense`. Claims about a linear calima dose-response are not supported.
- **Seasonality:** `intense` weeks are concentrated in winter months; seasonal confounding remains a concern despite quarterly controls.
- **CAP alerts:** only usable from 2018 onward.
- **DAI data:** not available after March 2022.
- **Normality:** Shapiro-Wilk rejects normality (W=0.9915, p=4.56e-03) but W > 0.99; CLT applies at n > 500.
- **PM10 as mediator:** PM10 is a pathway of the calima effect, not an independent confounder. Model 2 (with PM10) shows partial mediation, not confounding.
- **Causal interpretation:** all results are descriptive/associative.

---

## Output Files

**Tables** (`reports/islands/tables/gran_canaria/`):
- QA tables, descriptive summaries, heat and calima comparison tables

**Figures** (`reports/islands/figures/gran_canaria/`):
- Time-series, scatter plots, boxplots by calima level

---

## Session Log

| Date | Action | Status |
|---|---|---|
| 2026-04-xx | Island EDA completed | ✅ |
| 2026-05-14 | PUB-D: findings, caveats, regression results added | ✅ |
