# La Palma

## Overview

This notebook explores the weekly mortality–climate dataset for **La Palma** over **2016–2025**, using the island master table that combines mortality, weather, air quality, CAP alerts, and derived calima proxy variables.

The purpose of this EDA is descriptive: to validate the weekly dataset, summarise coverage, and inspect whether weekly mortality shows any visible relationship with heat or calima-related conditions.

---

## Dataset and coverage

- **Weekly observations:** 523 weeks in the master table
- **Deaths coverage:** 523 non-missing weeks
- **Mean weekly deaths:** 15.82
- **Median weekly deaths:** 15
- **QA checks:** passed for required columns, datetime parsing, duplicate week keys, and non-negative deaths

### Main missingness caveats

Some variables have relevant missingness and should be interpreted cautiously:

- `rh_min_pct_week`: 64.8% missing
- `calima_level_week`: 37.9% missing
- `calima_dai_flag`: 37.9% missing
- CAP variables: around 24.7% missing
- Several weather and air-quality variables have smaller but non-trivial gaps

However, the main derived calima proxy used in this EDA has full coverage:

- **`calima_proxy_score` missing:** 0.0%
- **`calima_proxy_level` missing:** 0.0%

### Important data note

The raw `calima_dai_flag` field shows inconsistent non-binary values in this master and is **not used as a main interpretive variable** in this EDA. The descriptive analysis relies instead on the derived calima proxy (`calima_proxy_score` / `calima_proxy_level`), which appears internally consistent and complete.

---

## Mortality and temperature

The notebook includes:

- weekly deaths time series
- weekly mean Tmax time series
- deaths vs absolute weekly Tmax scatter
- deaths vs Tmax anomaly scatter
- heat threshold comparison using empirical weekly p90 / p95 thresholds

### Main heat results

- **corr(deaths, `tmax_c_mean`) = -0.1685**
- **corr(deaths, Tmax anomaly) = 0.0242**

The anomaly correlation is essentially null. The absolute Tmax correlation is weak and negative, which may reflect seasonal structure rather than a direct protective effect of temperature.

Using the empirical weekly **p95** threshold for `tmax_c_mean`:

- **p95 threshold:** 27.07°C
- **Non-p95 weeks:** mean deaths = 15.75
- **p95 weeks:** mean deaths = 17.33
- **Δ deaths (heat p95 vs baseline) = +1.58 deaths/week**

This suggests a modest descriptive increase in mean weekly mortality during the hottest weeks.

---

## Calima proxy and mortality

The notebook audits the proxy distribution and compares mortality by calima intensity level.

### Calima proxy level distribution

- **no_calima:** 329 weeks
- **possible:** 147 weeks
- **probable:** 27 weeks
- **intense:** 20 weeks

### Calima proxy score distribution

The score distribution is concentrated at lower values:

- **0.0:** 264 weeks
- **0.5:** 65 weeks
- **1.0:** 111 weeks
- **1.5:** 36 weeks
- **2.0+:** relatively infrequent

This indicates that strong calima weeks exist, but they are a minority of the full weekly series.

### Weekly deaths by calima proxy level

- **no_calima:** mean = 15.71
- **possible:** mean = 15.77
- **probable:** mean = 16.37
- **intense:** mean = 17.40

### Main calima result

- **Δ deaths (calima intense vs baseline) = +1.69 deaths/week**

This is a modest but visible increase in mean weekly mortality for weeks classified as **intense** by the calima proxy.

The pattern is not dramatic, but compared with baseline it is directionally consistent with a higher-mortality profile under more intense calima conditions.

---

## Seasonality note on calima proxy

The monthly distribution shows that **intense** calima proxy weeks are concentrated mainly in:

- **January**
- **February**
- **December**

with very few or none across much of late spring and summer.

This matters because any mortality difference could still be partly shaped by broader seasonal structure rather than calima alone.

---

## Interaction: heat × calima

The notebook also checks the interaction between:

- `heat_p95`
- `calima_intense`

This is useful as an exploratory step, but in La Palma there is **no observed overlap** between:

- `heat_p95 = 1`
- `calima_intense = 1`

So the interaction table is informative as a coverage check, but **not interpretable as evidence of combined extreme heat + intense calima effects** in this island dataset.

---

## Interpretation

For La Palma, the descriptive signal is **modest but not absent**.

The main pattern is:

- no clear mortality relationship with temperature anomalies
- a small increase in mean deaths in p95 heat weeks
- a somewhat larger increase in mean deaths in **intense** calima proxy weeks compared with baseline
- limited sample sizes in the highest calima categories

Taken together, this makes La Palma more suggestive than a very low-signal island, but still far from strong evidence.

---

## Limitations

Key limitations for La Palma:

- descriptive EDA cannot establish causality
- weekly aggregation may hide short, acute event effects
- small sample sizes in higher-intensity calima categories
- no observed overlap between p95 heat weeks and intense calima weeks
- partial missingness in CAP, weather, and air-quality variables
- `calima_dai_flag` appears inconsistent in this master and is not used for interpretation

---

## Output files

The notebook exports the main tables and figures to the island report folders, including:

- QA checks
- missingness summary
- core descriptive statistics
- heat threshold mortality table
- calima proxy score and level distributions
- deaths by calima proxy level
- calima proxy by month
- heat × calima interaction table
- time series, scatterplots, and boxplots

---

## Bottom line

La Palma shows a **moderate descriptive signal** rather than a null one.

At weekly island level:

- extreme heat weeks show somewhat higher mortality than baseline
- intense calima proxy weeks also show higher mean mortality than baseline
- but the signal remains modest and should be interpreted cautiously, especially given seasonality and aggregation limits

Overall, La Palma supports a **careful, non-causal** narrative of elevated mortality during more intense calima conditions, while still requiring stronger modelling or adjustment before any firmer claim.