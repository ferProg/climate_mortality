# Tenerife Weekly Audit + EDA (2016–2025)

## Scope
This notebook reviews the weekly Tenerife master dataset for 2016–2025 and focuses on descriptive patterns linking mortality, temperature, and dust-related conditions. It is a notebook-level audit rather than a causal study.

## Dataset used
- **Island:** Tenerife (`tfe`)
- **Input file:** `data/processed/tenerife/master/master_tfe_2016_2025.parquet`
- **Temporal range:** 2016–2025
- **Unit of analysis:** weekly observations

## Purpose
The notebook has three goals:
1. confirm that the Tenerife weekly master dataset loads correctly and passes basic QA;
2. document core descriptive patterns in mortality, temperature, and calima-related variables;
3. produce saved tables and figures for later write-up and comparison with other islands.

## Coverage notes
- CAP alerts are only considered usable from **2018 onward**.
- DAI data are not available after **March 2022**, so DAI-based comparisons should be treated as partial and historical only.

## Main sections
1. **Setup and data load**  
   Path setup, helpers, and loading of the Tenerife weekly master dataset.

2. **QA / audit**  
   Basic checks for weekly uniqueness, required columns, and missingness.

3. **Core descriptive statistics**  
   Summary statistics for mortality, weather, air quality, and alert variables.

4. **Time-series overview**  
   Weekly deaths and weekly maximum temperature through time.

5. **Temperature and mortality**  
   - absolute weekly Tmax vs deaths;
   - temperature anomaly vs deaths;
   - extreme heat weeks (`p95`) vs baseline weeks.

6. **Calima and mortality**  
   - `calima_proxy` coverage audit;
   - proxy level distribution;
   - score distribution and score-to-level mapping;
   - deaths by proxy level;
   - monthly distribution of proxy levels.

7. **Heat × calima interaction**  
   A simple descriptive 2×2 cross-tab using `heat_p95` and `calima_intense`.

## Current descriptive takeaways
### Temperature
- Absolute weekly Tmax remains negatively correlated with weekly mortality, which is consistent with seasonal confounding rather than a simple protective interpretation.
- Temperature anomalies show only a weak positive linear correlation with mortality, so anomaly-based temperature alone does not explain much of the week-to-week mortality variation.

### Calima proxy
- `calima_proxy` has full weekly coverage in the Tenerife 2016–2025 dataset.
- Most weeks are classified as `no_calima`, while `probable` and `intense` weeks are relatively uncommon.
- Weeks classified as `intense` show clearly higher mean weekly mortality than the other proxy levels.
- `possible` is very close to `no_calima`, while `probable` shows a modest increase.
- `intense` weeks are concentrated mainly in winter months, so seasonal confounding remains a live concern.

## Interpretation limits
This notebook is descriptive. The outputs should not be read as causal evidence that calima or heat independently increase mortality. In particular:
- weekly aggregation may dilute short-lived effects;
- mortality is driven by multiple overlapping processes;
- strong seasonal structure can affect both exposure variables and deaths;
- agreement between `calima_proxy` and CAP/PM10 is expected because the proxy is derived from dust-related inputs and should be treated as internal consistency, not independent validation.

## Saved outputs
### Tables
- `tfe_qa_checks.csv`
- `tfe_missing_top50.csv`
- `desc_core_tfe.csv`
- `heat_p95_deaths_tfe.csv`
- `calima_proxy_audit_missing_tfe.csv`
- `calima_proxy_level_counts_tfe.csv`
- `calima_proxy_level_v_deaths_tfe.csv`
- `interaction_heat_p95_x_calima_intense_tfe.csv`

### Figures
- `tfe_eda01_weekly_deaths_timeseries.png`
- `tfe_eda01_weekly_tmax_timeseries.png`
- `tfe_eda01_deaths_vs_absolute_tmax_scatter.png`
- `tfe_eda01_deaths_vs_temperature_anomaly_scatter.png`
- `tfe_eda01_calima_proxy_level_distribution.png`

## Recommended next step
The next analytical step should not be adding more descriptive plots at random. The sensible continuation is to move into seasonality-aware comparisons, such as excess mortality framing, month-controlled comparisons, or simple regression models that test whether dust-related variables still matter after temporal adjustment.


In Tenerife (2016–2025), weeks classified as intense by calima_proxy show noticeably higher mean weekly mortality than the other proxy levels. Possible remains very close to no_calima, while probable shows a modest increase.

calima_proxy_score was heavily concentrated in the lower range, with 59.3% of weeks classified as no_calima (scores 0.0–0.5). Higher-intensity proxy weeks were relatively uncommon: 6.7% were classified as probable and 7.6% as intense.

Intense proxy weeks were not evenly distributed across the year and were concentrated mainly in winter months, especially December to February. This suggests that part of the observed mortality increase may still be confounded by seasonal structure.

Since calima_proxy is constructed from dust-related indicators including PM10 and CAP-derived signals, agreement with those components should be interpreted as expected internal consistency rather than independent validation.
Its practical value in the EDA is not that it reproduces its source inputs, but that higher proxy levels appear to correspond to higher weekly mortality, especially in intense weeks.

## Additional proxy diagnostics

`calima_proxy_score` was heavily concentrated in the lower range, with most weeks classified in the `no_calima` band. Higher-intensity proxy weeks were relatively uncommon, which is consistent with calima being an episodic rather than persistent condition.

Weeks classified as `intense` showed the highest mean weekly mortality, while `possible` remained very close to `no_calima` and `probable` showed only a modest increase. A boxplot of weekly deaths by proxy level was added to check whether this pattern reflected a broader shift in the distribution rather than only a few extreme weeks.

The monthly distribution of `calima_proxy_level` also showed that `intense` weeks were concentrated mainly in winter months, especially December to February. This reinforces the need to interpret the mortality gradient cautiously, since part of the observed increase may still reflect seasonal confounding.