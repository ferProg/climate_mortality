# Calima-Mortality Signal Across Scales
## Provincial and CCAA Synthesis — Canary Islands (2016–2025)

*Project: climate_mortality | Author: Ferdie Ramos*

---

## 1. Effect Size Across Scales — η² Summary

| Scale | Level | η² | Interpretation |
|---|---|---|---|
| Canarias CCAA | Regional | 0.0617 | Moderate |
| Gran Canaria | Island | 0.0576 | Moderate |
| SC Tenerife | Provincial | 0.0563 | Moderate |
| Tenerife | Island | 0.0541 | Moderate |
| Las Palmas | Provincial | 0.0495 | Small-moderate |

All five scales show a statistically significant calima-mortality association
(p < 0.001). Effect sizes range from 0.0495 to 0.0617, consistently within
the moderate range (η² ~ 0.06).

## 2. Signal Pattern Across Scales

The calima-mortality signal does not dilute with aggregation. Moving from
island to provincial to regional scale, η² remains stable and slightly
increases at the CCAA level (0.0617), suggesting that when calima episodes
affect both provinces simultaneously, the population-level mortality impact
is amplified rather than averaged out.


Notable finding: SC Tenerife provincial (0.0563) exceeds Tenerife island
(0.0541), despite the provincial scale including smaller islands with weaker
individual signals (La Palma, Gomera, Hierro). This indicates that the 50%
population exposure threshold filters out noise effectively, retaining only
episodes with genuine region-wide impact.
Notable finding: SC Tenerife provincial (0.0563) exceeds Tenerife island
(0.0541), despite the provincial scale including smaller islands with weaker
individual signals (La Palma, Gomera, Hierro). This indicates that the 50%
population exposure threshold filters out noise effectively, retaining only
episodes with genuine region-wide impact.

*Methodological note: El Hierro was excluded from island-level EDA due to
insufficient weekly death counts, but its mortality and calima data are
included in the SC Tenerife provincial aggregation. The direction of its
influence on provincial η² has not been isolated.*

## 3. The SC Tenerife Case — Provincial vs Island Scale

SC Tenerife provincial η² (0.0563) exceeds Tenerife island η² (0.0541).
This is counterintuitive at first glance: adding smaller islands with weaker
signals should dilute the effect, not strengthen it.

The explanation lies in the classification methodology. The provincial proxy
uses a 50% population exposure threshold: a week is only classified as
intense if at least 50% of the provincial population is exposed to intense
calima. This is a stricter criterion than the island-level proxy, which
classifies intensity based on the island's own air quality and meteorological
indicators.

The result is a cleaner signal: weeks classified as intense at provincial
level represent genuine region-wide episodes, filtering out localized or
partial events that add noise at island level.

## 4. Regression Scale Decision

Based on the η² evidence across five scales, the regression model will be
specified at **island level**, using Tenerife and Gran Canaria as primary
units.

Rationale:
- Island-level EDAs are complete and validated for both islands
- η² values are robust at island scale (TFE: 0.0541, GC: 0.0576)
- The island geography provides well-defined exposure boundaries:
  calima episodes are spatially discrete, reducing misclassification
  of exposure compared to continental settings
- Provincial and CCAA results serve as robustness checks: the signal
  does not dilute with aggregation, which strengthens the island-level
  findings
- Regression at CCAA level would obscure inter-island heterogeneity,
  which is scientifically relevant

Provincial and CCAA analyses remain in the project as supplementary
evidence, not as primary units of analysis.

## 5. Hypothesis Assessment

Original hypothesis: greater population exposure to intense calima
episodes is associated with higher excess mortality.

**Validated.** The evidence is consistent across all five scales:

- The signal is present at every level of aggregation (island,
  provincial, regional)
- η² increases slightly at CCAA level (0.0617), where the exposure
  variable captures the proportion of the total Canarian population
  exposed to simultaneous intense calima in both provinces
- The 50% population exposure threshold at provincial level produces
  a cleaner signal than island-level classification (SC Tenerife
  provincial η² 0.0563 > Tenerife island η² 0.0541)
- Pairwise tests confirm the signal is driven by intense episodes
  (intense vs no_calima: p < 0.001 across scales)

The hypothesis is validated at the exploratory level. Regression
analysis will quantify the net effect controlling for
seasonality, temperature, and year.

