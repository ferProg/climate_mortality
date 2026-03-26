# La Gomera

## Overview

This notebook explores the weekly mortality–climate dataset for **La Gomera** over **2016–2025**, using the island master table that combines mortality, weather, air quality, CAP alerts, and derived calima proxy variables.

The purpose of this EDA is descriptive: to check data quality, summarise coverage, and inspect whether weekly mortality shows any visible relationship with heat or calima-related conditions.

---

## Dataset and coverage

- **Weekly observations:** 523 weeks in the master table
- **Deaths coverage:** 505 non-missing weeks
- **Mean weekly deaths:** 4.09
- **Median weekly deaths:** 4
- **QA checks:** passed for required columns, datetime parsing, duplicate week keys, and non-negative deaths

### Main missingness caveats

Some variables have relevant missingness and should be interpreted cautiously:

- `PM2.5` has very limited coverage
- CAP variables have partial missingness
- `calima_dai_flag` is incomplete
- Several visibility-related fields are also partially missing

However, the derived calima proxy used in the main descriptive comparisons has full coverage in this master:

- **`calima_proxy_score` missing:** 0.0%
- **`calima_proxy_level` missing:** 0.0%

---

## Mortality and temperature

The notebook includes:

- weekly deaths time series
- weekly mean Tmax time series
- deaths vs absolute weekly Tmax scatter
- deaths vs Tmax anomaly scatter
- heat threshold comparison using empirical weekly p90 / p95 thresholds

### Main heat results

- **corr(deaths, `tmax_c_mean`) = -0.0437**
- **corr(deaths, Tmax anomaly) = -0.0421**

These correlations are effectively near zero in descriptive terms.

Using the empirical weekly **p95** threshold for `tmax_c_mean`:

- **Δ deaths (heat p95 vs baseline) = +0.18 deaths/week**

This suggests only a very small difference in mean weekly mortality between extreme-heat weeks and the rest.

---

## Calima proxy and mortality

The notebook audits the proxy distribution and compares mortality by calima intensity level.

### Calima proxy level distribution

- **no_calima:** 363 weeks
- **possible:** 103 weeks
- **probable:** 27 weeks
- **intense:** 30 weeks

### Weekly deaths by calima proxy level

- **no_calima:** mean = 4.04
- **possible:** mean = 4.25
- **probable:** mean = 4.00
- **intense:** mean = 4.23

### Main calima result

- **Δ deaths (calima intense vs baseline) = +0.19 deaths/week**

This is a small absolute increase. Unlike islands where the proxy shows clearer separation, in La Gomera the mortality differences across calima levels are modest and should be treated carefully.

---

## Interaction: heat × calima

The notebook also checks the interaction between:

- `heat_p95`
- `calima_intense`

This is useful as an exploratory step, but in La Gomera the overlap is extremely sparse:

- `heat_p95 = 1` and `calima_intense = 1` appears in **only 1 week**

So this interaction table should be treated as **not interpretable** beyond a basic coverage check.

---

## Interpretation

For La Gomera, the EDA does **not** show a strong descriptive signal linking weekly mortality with either heat or calima proxy intensity.

The main pattern is:

- temperature correlations are essentially null
- p95 heat weeks show only a very small increase in mean deaths
- intense calima weeks also show only a very small increase over baseline
- the island’s low weekly death counts make the series inherently noisy

This does **not** mean there is no relationship. It means that, at weekly island level and with the current coverage, the descriptive signal is weak and any claim should remain cautious.

---

## Limitations

Key limitations for La Gomera:

- small island population and very low weekly death counts
- small sample sizes in higher-intensity calima categories
- sparse overlap for heat × intense calima interaction
- missingness in CAP, PM2.5, DAI, and visibility-related fields
- weekly aggregation may hide short, acute event effects
- descriptive EDA cannot establish causality

---

## Output files

The notebook exports the main tables and figures to the island report folders, including:

- QA checks
- missingness summary
- core descriptive statistics
- heat threshold mortality table
- calima proxy score and level distributions
- deaths by calima proxy level
- heat × calima interaction table
- time series and boxplot/scatter figures

---

## Bottom line

La Gomera is best interpreted as a **low-signal island** in this descriptive phase.

The notebook is still useful because it:

- confirms that the master dataset is usable
- documents variable coverage and limitations
- shows that any mortality–heat or mortality–calima relationship is weak at this aggregation level
- prevents overclaiming where the signal is limited