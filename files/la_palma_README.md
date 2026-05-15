# La Palma — Island EDA Summary (2016–2025)
**Climate Mortality Project | RA Career**

---

## Coverage

| Field | Value |
|---|---|
| Island | La Palma (`lpa`) |
| Input file | `data/processed/la_palma/master/master_lpa_2016_2025.parquet` |
| Weekly observations | 523 weeks |
| Deaths coverage | 523 non-missing weeks |
| Mean weekly deaths | 15.82 |
| Median weekly deaths | 15 |
| `calima_proxy_score` missing | 0.0% |
| `calima_proxy_level` missing | 0.0% |

---

## Key Findings

### Calima proxy distribution
| Level | Weeks |
|---|---|
| no_calima | 329 |
| possible | 147 |
| probable | 27 |
| intense | 20 |

### Mortality by calima level
| Level | Mean deaths/week | Δ vs no_calima |
|---|---|---|
| no_calima | 15.71 | — |
| possible | 15.77 | +0.06 |
| probable | 16.37 | +0.66 |
| intense | 17.40 | **+1.69** |

Pattern: modest but directionally consistent gradient. Δ at `intense` is the largest but based on only 20 weeks (small n).

### Temperature signal
- `corr(deaths, tmax_c_mean)` = −0.169
- `corr(deaths, Tmax anomaly)` = 0.024 (essentially null)
- p95 threshold: 27.07°C; Δ deaths heat p95 = +1.58/week (small positive signal)

### Heat × calima interaction
- No observed overlap between `heat_p95 = 1` and `calima_intense = 1` — not interpretable.

---

## Caveats

- **Small n in intense category:** only 20 `intense` weeks — results are statistically fragile. Δ=+1.69 with p=0.086 (not significant at 0.05).
- **No regression model run** at island level — findings are descriptive EDA only.
- **Seasonality:** `intense` weeks concentrated in Jan, Feb, Dec — mortality elevation may partly reflect seasonal confounding.
- **CAP alerts:** only usable from 2018 onward.
- **DAI data:** not available after March 2022; `calima_dai_flag` inconsistent — not used.
- **`rh_min_pct_week` missingness:** 64.8% missing — excluded from analysis.
- **Pipeline note:** 2025 visibility digest was generated and masters rebuilt prior to EDA (pipeline fix). Data considered complete.
- **Causal interpretation:** all results are descriptive/associative.

---

## Output Files

**Tables** (`reports/islands/tables/la_palma/`):
- QA checks, missingness summary, descriptive stats, calima distribution, deaths by calima level, interaction table

**Figures** (`reports/islands/figures/la_palma/`):
- Time-series, scatter plots, boxplots, calima proxy by month

---

## Session Log

| Date | Action | Status | Notes |
|---|---|---|---|
| 2026-04-24 | EDA completed | ✅ | Pipeline fix: visibility 2025 generated, master + proxy rebuilt. Δ=+1.78, p=0.086, n=20 intense. |
| 2026-05-14 | PUB-D: structured findings + caveats added | ✅ | |
