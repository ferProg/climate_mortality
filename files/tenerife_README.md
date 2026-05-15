# Tenerife — Island EDA Summary (2016–2025)
**Climate Mortality Project | RA Career**

---

## Coverage

| Field | Value |
|---|---|
| Island | Tenerife (`tfe`) |
| Input file | `data/processed/tenerife/master/master_tfe_2016_2025.parquet` |
| Weekly observations | 523 weeks |
| Date range | 2016-01-04 → 2025-12-29 |
| Unit | Weekly all-cause mortality |
| `calima_proxy_score` missing | 0.0% |
| `calima_proxy_level` missing | 0.0% |

---

## Key Findings

### Calima proxy distribution
| Level | Weeks | % |
|---|---|---|
| no_calima | ~313 | 59.8% |
| possible | — | — |
| probable | ~35 | 6.7% |
| intense | ~40 | 7.6% |

### Mortality by calima level
| Level | Mean deaths/week | Δ vs no_calima |
|---|---|---|
| no_calima | 138.2 | — |
| possible | 139.6 | +1.4 |
| probable | 146.3 | +8.1 |
| intense | 155.4 | **+17.2 (+12.4%)** |

### Temperature signal
- `corr(deaths, tmax_c_mean)` = negative (seasonal confounding)
- `corr(deaths, Tmax anomaly)` = weak positive
- Extreme heat (p95) shows only modest crude mortality difference

### Regression model (Model 3 — TFE)
| Parameter | Value |
|---|---|
| Model | deaths_week ~ calima_ordinal + temp_c_mean + Q_1 + Q_2 + Q_3 + deaths_lag1 |
| β calima_ordinal | +2.93 (p < 0.05) ✅ |
| β deaths_lag1 | +0.49 (p < 0.05) ✅ |
| R² | 0.464 |
| Adj. R² | 0.458 |
| AIC | 4402.61 |
| DW | 2.30 ✅ |

Calima signal survives autocorrelation control. deaths_lag1 is the dominant predictor (strong mortality inertia).

---

## Caveats

- **Seasonality:** `intense` weeks concentrated in winter (Dec–Feb) — calima mortality gradient may partially reflect seasonal confounding. Quarterly dummies included in regression to control for this.
- **CAP alerts:** only usable from 2018 onward.
- **DAI data:** not available after March 2022; DAI-based comparisons are historical only.
- **Calima proxy validation:** proxy is constructed from PM10, humidity, visibility, and pressure. Agreement with CAP/PM10 reflects expected internal consistency, not independent validation.
- **Normality:** Shapiro-Wilk rejects normality (W=0.9908, p=2.49e-03) but W > 0.99 indicates mild tail deviation only. With n > 500, OLS estimates remain valid by CLT.
- **Causal interpretation:** all results are descriptive/associative. The project does not claim causal effect of calima on mortality.

---

## Output Files

**Tables** (`reports/islands/tables/tenerife/`):
- `tfe_qa_checks.csv`
- `tfe_missing_top50.csv`
- `desc_core_tfe.csv`
- `heat_p95_deaths_tfe.csv`
- `calima_proxy_level_v_deaths_tfe.csv`
- `calima_proxy_audit_missing_tfe.csv`
- `calima_proxy_level_counts_tfe.csv`
- `interaction_heat_p95_x_calima_intense_tfe.csv`

**Figures** (`reports/islands/figures/tenerife/`):
- `tfe_eda01_weekly_deaths_timeseries.png`
- `tfe_eda01_weekly_tmax_timeseries.png`
- `tfe_eda01_deaths_vs_absolute_tmax_scatter.png`
- `tfe_eda01_deaths_vs_temperature_anomaly_scatter.png`
- `tfe_eda01_calima_proxy_level_distribution.png`

---

## Session Log

| Date | Action | Status |
|---|---|---|
| 2026-04-xx | Island EDA completed | ✅ |
| 2026-05-14 | PUB-D: findings, caveats, regression results added | ✅ |
