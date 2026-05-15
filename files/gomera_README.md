# La Gomera — Island EDA Summary (2016–2025)
**Climate Mortality Project | RA Career**

---

## Coverage

| Field | Value |
|---|---|
| Island | La Gomera (`gmr`) |
| Input file | `data/processed/gomera/master/master_gmr_2016_2025.parquet` |
| Weekly observations | 523 weeks |
| Deaths coverage | 505 non-missing weeks (18 missing) |
| Mean weekly deaths | 4.09 |
| Median weekly deaths | 4 |
| `calima_proxy_score` missing | 0.0% |
| `calima_proxy_level` missing | 0.0% |

---

## Key Findings

### Calima proxy distribution
| Level | Weeks |
|---|---|
| no_calima | 363 |
| possible | 103 |
| probable | 27 |
| intense | 30 |

### Mortality by calima level
| Level | Mean deaths/week | Δ vs no_calima |
|---|---|---|
| no_calima | 4.04 | — |
| possible | 4.25 | +0.21 |
| probable | 4.00 | −0.04 |
| intense | 4.23 | **+0.19** |

Pattern: no meaningful signal. Differences are within noise range given mean deaths ≈ 4/week.

### Temperature signal
- `corr(deaths, tmax_c_mean)` = −0.044 (near zero)
- `corr(deaths, Tmax anomaly)` = −0.042 (near zero)
- Δ deaths heat p95 = +0.18/week (negligible)

### Heat × calima interaction
- Only 1 week with both `heat_p95 = 1` and `calima_intense = 1` — not interpretable.

---

## Caveats

### 🟡 ALERT: Pressure anomaly
`pres_p75` for La Gomera = **994.61 hPa**, which is approximately 20 hPa below the range observed in other Canary Islands (~1014–1017 hPa). This anomaly is documented but its cause has not been fully investigated. Possible explanations include station altitude differences, sensor calibration issues, or data ingestion errors. Pressure-derived features for La Gomera should be treated with caution and are not used as primary analysis variables.

### Low-signal island
- La Gomera's mean weekly deaths (~4) are inherently noisy at this aggregation level. With counts this low, week-to-week variation is dominated by stochastic fluctuation rather than environmental signal.
- Δ=+0.19 deaths/week at `intense` is not a meaningful finding given this baseline.

### Other caveats
- **Deaths coverage:** 505/523 weeks (18 missing) — slightly lower than other islands.
- **No regression model run** at island level — findings are descriptive EDA only.
- **PM2.5:** very limited coverage — excluded from analysis.
- **CAP alerts:** only usable from 2018 onward; partial missingness.
- **DAI data:** incomplete; `calima_dai_flag` not used.
- **Causal interpretation:** all results are descriptive/associative.

---

## Output Files

**Tables** (`reports/islands/tables/gomera/`):
- QA checks, missingness summary, descriptive stats, calima distribution, deaths by calima level, interaction table

**Figures** (`reports/islands/figures/gomera/`):
- Time-series, scatter plots, boxplots

---

## Session Log

| Date | Action | Status | Notes |
|---|---|---|---|
| 2026-04-xx | Island EDA completed | ✅ | Low-signal island confirmed |
| 2026-05-14 | PUB-D: structured findings + pressure anomaly caveat added | ✅ | `pres_p75=994.61 hPa` documented |
