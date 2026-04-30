# Lanzarote

## Overview

This notebook explores the weekly mortality–climate dataset for **Lanzarote** over **2016–2025**, using the island master table that combines mortality, weather, air quality, CAP alerts, and derived calima proxy variables.

The purpose of this EDA is descriptive: to validate the weekly dataset, summarise coverage, and inspect whether weekly mortality shows any visible relationship with heat or calima-related conditions.

---

## Dataset and coverage

- **Weekly observations:** 523 weeks in the master table
- **Deaths coverage:** 523 non-missing weeks
- **Mean weekly deaths:** 15.92
- **Median weekly deaths:** 15
- **QA checks:** passed for required columns, datetime parsing, duplicate week keys, and non-negative deaths

### Main missingness caveats

Some variables have relevant missingness and should be interpreted cautiously:

- `calima_dai_flag`: 37.9% missing
- `calima_level_week`: 37.9% missing
- CAP variables: around 24.7% missing
- several weather and air-quality variables also have partial missingness

However, the main derived calima proxy used in this EDA has full coverage:

- **`calima_proxy_score` missing:** 0.0%
- **`calima_proxy_level` missing:** 0.0%

### Important data note

The raw `calima_dai_flag` field shows inconsistent non-binary values in this master and is **not used as a main interpretive variable** in this EDA. The main descriptive comparisons rely instead on the derived calima proxy (`calima_proxy_score` / `calima_proxy_level`), which appears complete and internally consistent.

---

## Mortality and temperature

The notebook includes:

- weekly deaths time series
- weekly mean Tmax time series
- deaths vs absolute weekly Tmax scatter
- deaths vs Tmax anomaly scatter
- heat threshold comparison using empirical weekly p90 / p95 thresholds

### Main heat results

- **corr(deaths, `tmax_c_mean`) = -0.2121**
- **corr(deaths, Tmax anomaly) = 0.0571**

These results do **not** suggest a clear positive descriptive relationship between weekly mortality and heat in Lanzarote.

Using the empirical weekly **p95** threshold for `tmax_c_mean`:

- **p95 threshold:** 31.36°C
- **Non-p95 weeks:** mean deaths = 15.94
- **p95 weeks:** mean deaths = 15.42
- **Δ deaths (heat p95 vs baseline) = -0.53 deaths/week**

At this weekly island level, the hottest weeks do not show elevated mortality relative to the rest of the series.

---

## Calima proxy and mortality

The notebook audits the proxy distribution and compares mortality by calima intensity level.

### Calima proxy score distribution

The score distribution is concentrated in lower and mid-range values:

- **0.0:** 205 weeks
- **0.5:** 44 weeks
- **1.0:** 142 weeks
- **1.5:** 39 weeks
- **2.0:** 39 weeks
- higher values occur less frequently

### Calima proxy level distribution

- **no_calima:** 249 weeks
- **possible:** 181 weeks
- **probable:** 56 weeks
- **intense:** 37 weeks

### Weekly deaths by calima proxy level

- **no_calima:** mean = 16.33
- **possible:** mean = 15.02
- **probable:** mean = 15.18
- **intense:** mean = 18.68

### Main calima result

- **Δ deaths (calima intense vs baseline) = +2.34 deaths/week**

This is the clearest descriptive result in the Lanzarote notebook.

The pattern is not a smooth linear gradient across all calima levels, because `possible` and `probable` do not sit above the baseline. Instead, the main separation appears specifically in the **intense** category, which shows clearly higher mean weekly mortality than `no_calima`.

---

## Seasonality note on calima proxy

The monthly distribution suggests that intense calima proxy weeks are not evenly spread across the calendar year.

This matters because part of the observed mortality difference may still reflect broader seasonal structure rather than calima alone.

---

## Interaction: heat × calima

The notebook also checks the interaction between:

- `heat_p95`
- `calima_intense`

The overlap exists, but it is extremely sparse:

- `heat_p95 = 1` and `calima_intense = 1` appears in **2 weeks**

The mean weekly mortality in that cell is high (**25.0 deaths/week**), but the sample is far too small for robust interpretation. This interaction table should therefore be treated as **exploratory only**.

---

## Interpretation

For Lanzarote, the descriptive signal is more visible for **intense calima** than for heat.

The main pattern is:

- no clear positive mortality signal for weekly heat
- near-null anomaly relationship
- slightly lower mortality in p95 heat weeks than baseline
- clearly higher mean mortality in **intense** calima proxy weeks
- no clean monotonic gradient across intermediate calima levels

So the most defensible descriptive summary is that **intense calima weeks stand out**, while heat does not.

---

## Limitations

Key limitations for Lanzarote:

- descriptive EDA cannot establish causality
- weekly aggregation may hide short, acute event effects
- sample sizes become smaller in high-intensity calima categories
- overlap between p95 heat and intense calima is too sparse for interpretation
- partial missingness remains in CAP, weather, and air-quality variables
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

## Session Log

### 2026-04-22 — EDA Lanzarote ✅ Completado

- Carga y validación del master (`master_lzt_2016_2025.parquet`) — shape, columnas, rango temporal confirmados
- Diagnóstico de nulos en `calima_level_week` y `calima_dai_flag` (198 nulos, 2022–2025) — identificado como limitación de la fuente Heliyon, no bug de pipeline
- Confirmado que el proxy NO usa CAP ni DAI — solo PM10, humidity, visibility y pressure
- Fuente DAI (Heliyon) identificada como regional para todas las islas, almacenada bajo nombre `tfe` (limitación documentada)
- **Sección 1.5 añadida al notebook:** validación proxy vs CAP y proxy vs DAI con % de acuerdo asimétrico en ambas direcciones
- Hallazgo clave: señal descriptiva de mortalidad concentrada en `intense`, sin gradiente limpio en niveles intermedios — claim mantenible con matices documentados

**Pendientes:**
- Ejecutar notebook completo end-to-end para verificar integración de la sección 1.5
- Replicar validación proxy vs CAP/DAI para otras islas

---

## Bottom line

Lanzarote behaves as a **calima-led descriptive signal** rather than a heat-led one.

At weekly island level:

- heat does not show elevated mortality
- intense calima weeks show clearly higher mean mortality than baseline
- intermediate calima levels do not show a simple gradient
- the combined heat × calima extreme cell is too sparse to support conclusions

Overall, Lanzarote supports a **careful, non-causal** narrative in which the strongest descriptive mortality difference appears during **intense calima proxy weeks**, not during the hottest weeks.s