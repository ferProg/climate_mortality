# Fuerteventura — Island EDA Summary (2016–2025)
**Climate Mortality Project | RA Career**

---

## Coverage

| Field | Value |
|---|---|
| Island | Fuerteventura (`fve`) |
| Input file | `data/processed/fuerteventura/master/master_fve_2016_2025.parquet` |
| Weekly observations | 523 weeks |
| `calima_proxy_score` missing | 0.0% |
| `calima_proxy_level` missing | 0.0% |

---

## Key Findings

### Mortality by calima level
| Level | Mean deaths/week | Δ vs no_calima |
|---|---|---|
| no_calima | 10.57 | — |
| possible | 9.61 | −0.96 |
| probable | 9.08 | −1.49 |
| intense | 10.18 | **−0.40** |

Pattern: **no positive mortality signal** at any calima level. `intense` weeks are slightly below baseline. This is the clearest null result in the dataset.

### Temperature signal
- `corr(deaths, tmax_c_mean)` = −0.107
- `corr(deaths, Tmax anomaly)` = 0.074 (weak)
- p90 = 27.99°C; p95 = 28.99°C
- Δ deaths heat p95 = +0.25/week (negligible)

### Heat × calima interaction
- Only 3 weeks with both `heat_p95 = 1` and `calima_intense = 1` — not interpretable.

---

## Caveats

- **Null calima signal:** Fuerteventura does not show a crude mortality increase during intense calima weeks. This does not prove absence of effect — weekly aggregation may dilute short-lived events and the island has a smaller population than TFE/GC. The null result is documented transparently.
- **Seasonal confounding investigation:** Δ=−0.11 in earlier analysis was confirmed as a seasonal artefact — intense calima weeks concentrated in winter but no excess within individual winter months. Resolved and documented (2026-04-24).
- **No regression model run** at island level — findings are descriptive EDA only.
- **CAP alerts:** only usable from 2018 onward.
- **DAI data:** not available after March 2022.
- **Pipeline note:** 2025 visibility digest generated and masters rebuilt prior to EDA (pipeline fix). Data considered complete.
- **Causal interpretation:** all results are descriptive/associative.

---

## Output Files

**Tables** (`reports/islands/tables/fuerteventura/`):
- QA tables, descriptive summaries, heat and calima comparison tables, interaction table

**Figures** (`reports/islands/figures/fuerteventura/`):
- Time-series, scatter plots, boxplots by calima level

---

## Session Log

| Date | Action | Status | Notes |
|---|---|---|---|
| 2026-04-24 | EDA completed | ✅ | Seasonal artefact (Δ=−0.11) investigated and resolved. Null signal confirmed. |
| 2026-05-14 | PUB-D: structured findings + caveats added | ✅ | |
