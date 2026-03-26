## Exploratory Data Analysis (Fuerteventura)

This notebook reviews the weekly master dataset for **Fuerteventura (2016–2025)** and explores the relationship between mortality, heat, and calima-related indicators.

### Scope

The analysis focuses on:

- weekly all-cause deaths
- weekly meteorological conditions
- extreme heat thresholds based on weekly Tmax
- calima proxy intensity levels derived from the merged environmental signals
- simple exploratory interaction between extreme heat and intense calima

### QA / audit checks

Basic audit checks passed:

- required columns present
- `week_start` parsed correctly
- no duplicate weekly records
- no negative weekly death counts

Main missingness patterns are consistent with known source limitations:

- **CAP alerts are only usable from 2018 onwards**
- **DAI flag coverage is not available after March 2022**
- some weather and air-quality variables have partial missingness in earlier periods

For the main calima analysis, the combined proxy used in this notebook has:

- **0.0% missingness** for `calima_proxy_score`
- **0.0% missingness** for `calima_proxy_level`

### Main descriptive findings

Weekly deaths in Fuerteventura are much lower in absolute level than in Tenerife or Gran Canaria, which is expected given the island’s smaller population base.

Temperature shows the expected seasonal pattern. As in the other islands, **absolute temperature should not be interpreted causally on its own**, because it is strongly affected by seasonality.

### Heat and mortality

Using empirical weekly thresholds from `tmax_c_mean`:

- **p90 = 27.99°C**
- **p95 = 28.99°C**

When comparing weeks above the **p95** threshold against the rest:

- mean deaths in baseline weeks = **9.90**
- mean deaths in heat p95 weeks = **10.15**
- **Δ deaths (heat p95 vs baseline) = +0.25 deaths/week**

This suggests only a **small crude difference** in weekly mortality for the hottest weeks.

Also:

- `corr(deaths, tmax_c_mean) = -0.107`
- `corr(deaths, Tmax anomaly) = 0.0742`

This pattern is consistent with the idea that **absolute temperature is seasonally confounded**, while the anomaly-based association is slightly more informative but still weak at this stage.

### Calima proxy and mortality

The calima proxy distribution is complete and internally consistent, with the following ordered categories:

- `no_calima`
- `possible`
- `probable`
- `intense`

Weekly mean deaths by calima proxy level:

- **no_calima**: 10.57
- **possible**: 9.61
- **probable**: 9.08
- **intense**: 10.18

Key contrast:

- **Δ deaths (calima intense vs baseline) = -0.40 deaths/week**

So, unlike Tenerife and some earlier island-level results, **Fuerteventura does not show a crude increase in weekly mortality during intense calima weeks** in this EDA.

That does **not** prove the absence of any effect. It only means that, with this weekly aggregation and this proxy, the simple descriptive comparison does **not** reveal a positive mortality signal.

### Heat × calima interaction

A simple 2×2 exploratory table was built using:

- `heat_p95`
- `calima_intense`

The cell with both extreme heat and intense calima contains **very few observations (n = 3)**, so this interaction should be treated as **descriptive only** and not as a robust finding.

### Interpretation

For Fuerteventura, the current weekly EDA suggests:

- **very modest crude heat signal**
- **no clear crude excess mortality in intense calima weeks**
- strong need to interpret results cautiously because of:
  - seasonality
  - small island counts
  - limited sample size in extreme subsets
  - weekly aggregation, which can dilute short-lived environmental effects

### Limitations

- This is **EDA**, not causal inference.
- Weekly aggregation may smooth short-term exposure effects.
- CAP coverage is only usable from 2018 onward.
- DAI coverage is incomplete after March 2022.
- Small counts in some subgroup analyses reduce stability.

### Files generated

This notebook exports:

- QA tables
- descriptive summary tables
- heat threshold comparison tables
- calima proxy distribution and level tables
- interaction tables
- time-series and comparison figures

These outputs are intended to support a transparent, island-by-island comparison across the Canary Islands.