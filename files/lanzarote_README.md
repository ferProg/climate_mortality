# Lanzarote — Island EDA Summary (2016–2025)
**Climate Mortality Project | RA Career**

---

## Coverage

| Field | Value |
|---|---|
| Island | Lanzarote (`lzt`) |
| Input file | `data/processed/lanzarote/master/master_lzt_2016_2025.parquet` |
| Weekly observations | 523 weeks |
| Deaths coverage | 523 non-missing weeks |
| Mean weekly deaths | 15.92 |
| Median weekly deaths | 15 |
| `calima_proxy_score` missing | 0.0% |
| `calima_proxy_level` missing | 0.0% |

---

## Key Findings

### Calima proxy distribution
| Level | Weeks |
|---|---|
| no_calima | 249 |
| possible | 181 |
| probable | 56 |
| intense | 37 |

### Mortality by calima level
| Level | Mean deaths/week | Δ vs no_calima |
|---|---|---|
| no_calima | 16.33 | — |
| possible | 15.02 | −1.31 |
| probable | 15.18 | −1.15 |
| intense | 18.68 | **+2.34** |

Pattern: no monotonic gradient across intermediate levels. The mortality elevation is specific to `intense` weeks. `possible` and `probable` fall slightly below baseline.

### Temperature signal
- `corr(deaths, tmax_c_mean)` = −0.212
- `corr(deaths, Tmax anomaly)` = 0.057
- p95 threshold: 31.36°C; Δ deaths heat p95 = −0.53/week (no positive signal)

### Heat × calima interaction
- Only 2 weeks with both `heat_p95 = 1` and `calima_intense = 1` — not interpretable.

---

## Caveats

- **Non-monotonic gradient:** unlike TFE/GC, intermediate levels (`possible`, `probable`) do not show elevation — the signal is specific to `intense`. Any claim of dose-response must be qualified.
- **No regression model run** at island level for Lanzarote — findings are descriptive EDA only.
- **CAP alerts:** only usable from 2018 onward.
- **DAI data:** not available after March 2022; `calima_dai_flag` shows inconsistent non-binary values — not used for interpretation.
- **DAI source note:** Heliyon DAI dataset is regional (stored under `tfe` label) — applies to all islands equally, not island-specific.
- **`calima_level_week` missingness:** 37.9% missing — not used in main analysis; proxy used instead.
- **Seasonality:** intense calima weeks concentrated in winter — confounding cannot be excluded without model controls.
- **Causal interpretation:** all results are descriptive/associative.

---

## Output Files

**Tables** (`reports/islands/tables/lanzarote/`):
- QA checks, missingness summary, descriptive stats, calima distribution, deaths by calima level, interaction table

**Figures** (`reports/islands/figures/lanzarote/`):
- Time-series, scatter plots, boxplots, calima proxy by month

---

## Session Log

| Date | Action | Status | Notes |
|---|---|---|---|
| 2026-04-22 | EDA completed | ✅ | Proxy validation vs CAP/DAI added (Section 1.5); signal confirmed in `intense` only |
| 2026-05-14 | PUB-D: structured findings + caveats added | ✅ | |
