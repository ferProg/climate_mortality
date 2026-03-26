## Gran Canaria: weekly audit and exploratory analysis (2016–2025)

This island-level audit uses the weekly master dataset for **Gran Canaria** covering **2016–2025**. The objective is descriptive: to assess data coverage, inspect basic mortality–climate relationships, and quantify how weekly deaths vary across heat and calima conditions.

### Dataset and coverage

- Weekly master dataset loaded successfully for **523 weeks**
- Date range in the notebook: **2015-12-28 to 2025-12-29**
- Core QA checks passed for:
  - weekly uniqueness
  - required fields (`week_start`, `deaths_week`)
  - date parsing
- `calima_proxy_score` and `calima_proxy_level` had **0.00% missingness** in the weekly dataset used in the EDA

### Main descriptive findings

#### Temperature and mortality

Absolute temperature showed a **moderate negative correlation** with weekly deaths:

- `corr(deaths_week, tmax_c_mean) = -0.365`

This is not interpreted as a protective effect of heat. At weekly scale, absolute temperature is still strongly entangled with **seasonality**, so this pattern is likely confounded by colder-season mortality.

To reduce that problem, a simple temperature anomaly variable was created using the month-of-year mean as reference. The anomaly relationship with deaths was much weaker:

- `corr(deaths_week, Tmax anomaly) = 0.0822`

This suggests that **relative heat** may be more informative than absolute weekly temperature, but the signal remains weak in this descriptive pass.

#### Extreme heat weeks

Extreme heat was defined empirically using the distribution of `tmax_c_mean`:

- `p90 = 28.19 °C`
- `p95 = 29.30 °C`

Comparing **p95 heat weeks** against the rest of the sample:

- **Δ deaths (heat p95 vs baseline) = +0.99 deaths/week**

This indicates only a **small descriptive difference** in weekly mortality under this threshold-based heat definition.

#### Calima proxy and mortality

The calima proxy was merged at weekly level and classified into four ordered categories:

- `no_calima`: **126 weeks**
- `possible`: **279 weeks**
- `probable`: **78 weeks**
- `intense`: **40 weeks**

Comparing weekly deaths by calima level showed a clearer mortality gradient than the heat p95 split. In particular:

- Mean deaths in `intense` weeks: **151.57**
- Mean deaths in `no_calima` weeks: **140.81**
- **Δ deaths (calima intense vs baseline) = +10.77 deaths/week**

This is the strongest descriptive signal in the Gran Canaria EDA: **weeks classified as intense calima show noticeably higher average mortality than baseline no-calima weeks**.

#### Heat × calima interaction

A simple `heat_p95 × calima_intense` 2×2 table was produced for exploratory purposes. This is useful as a first pass, but the cell sizes are too limited for strong conclusions without further modeling or seasonal adjustment.

### Interpretation

At this stage, the Gran Canaria island EDA suggests:

1. **Absolute weekly temperature is not a reliable standalone mortality signal**, likely because of seasonal confounding.
2. **Temperature anomaly is more conceptually useful**, but the observed association is weak in the current descriptive setup.
3. **Calima intensity shows a stronger descriptive association with weekly mortality than the heat p95 split**.
4. These results are **exploratory and non-causal**. They should be interpreted as pattern detection, not as evidence of direct causal effect.

### Important limitations

- The analysis is performed at **weekly** level, which limits temporal precision.
- **Seasonality and confounding** remain major concerns.
- CAP alert coverage is only considered reliable from **2018 onward**.
- DAI-derived coverage is not available after **March 2022**.
- Some derived variables are useful as exploratory proxies but still require validation before being treated as robust exposure definitions.

### Saved outputs

The notebook saves descriptive tables and figures under:

- `reports/islands/tables/gran_canaria/`
- `reports/islands/figures/gran_canaria/`

These outputs include QA tables, descriptive summaries, heat and calima comparison tables, and core exploratory plots.