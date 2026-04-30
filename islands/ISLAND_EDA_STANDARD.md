# Island EDA Standard — climate_mortality

> **Created:** 2026-04-24  
> **Purpose:** Define the standard structure for all island EDA notebooks and READMEs, and document the route to bring all islands to the same level.  
> **Reference island:** Lanzarote (most complete as of Apr 2026)

---

## 1. Standard notebook structure

Every `eda_calima_mortality_[island].ipynb` must follow this section order:

| Section | Title | Status required |
|---------|-------|-----------------|
| Header cell | Title + objective + key variables + section list | ✅ All islands |
| **1** | Load Data | ✅ All islands |
| **1.5** | Proxy validation vs CAP and DAI | ✅ If CAP data exists for island |
| **2** | Lags (lag0, lag1, lag2) | ✅ All islands |
| **3** | Group by proxy category | ✅ All islands |
| **4** | ANOVA + effect sizes (Δ deaths) | ✅ All islands |
| **4.1** | Pairwise comparisons (t-tests) | ✅ All islands |
| **5** | Visualizations | ✅ All islands |
| **6** | Summary (auto-generated table + markdown interpretation) | ✅ All islands |
| **7** | Anomalies & Deep Dives | ✅ All islands (even if empty placeholder) |

**Rule:** Section 7 must exist in every notebook — even if it only contains a placeholder header and "No anomalies identified". This keeps the structure consistent and signals that the section was checked.

---

## 2. Section 7 — Anomalies & Deep Dives (template)

This section handles unexpected findings in-place, without creating separate notebooks.

### How it works
- Each anomaly gets a numbered subsection: 7.1, 7.2, etc.
- Each subsection is self-contained and follows the same 4-part structure
- New anomalies are appended here — never in a new notebook
- If no anomaly exists, the section stays as an empty placeholder

### Template for each subsection

```markdown
### 7.X — [Short anomaly name]

**Finding:** [What was observed — one sentence, with numbers]
**Hypothesis:** [2–3 possible explanations, ordered by plausibility]
**Investigation:** [Code cells below]
**Conclusion:** [What the investigation found — what's ruled out, what remains open]
**Status:** ⏳ Under investigation | ✅ Resolved | ❓ Inconclusive
```

### Example (Fuerteventura 7.1)

```markdown
### 7.1 — Paradoxical calima-mortality relationship

**Finding:** Δ deaths (intense vs no_calima) = −0.40/week — unlike all other islands,
intense calima weeks in Fuerteventura show slightly FEWER deaths than baseline.

**Hypothesis:**
1. Seasonality confounding: intense calima concentrated in winter → winter already has higher
   baseline mortality → possible collinearity masking calima effect
2. Survivor effect: Fuerteventura has oldest population exposed to frequent calima → 
   chronic adaptation or selection of hardier survivors
3. Data artefact: small absolute counts (n=~475 deaths/year) → Δ−0.40 may be noise
4. Spatial exposure: wind patterns in FTV may mean intense proxy ≠ intense human exposure

**Investigation:** [code cells]
**Conclusion:** TBD
**Status:** ⏳ Under investigation
```

---

## 3. Standard README structure

Every island `README.md` follows the Lanzarote format:

```markdown
# [Island Name]

## Overview
[2–3 sentences: dataset, purpose, dates]

## Dataset and coverage
- Weekly observations, deaths coverage, mean/median deaths
- Main missingness caveats table
- calima_proxy completeness note

## Mortality and temperature
[correlations, p95 threshold, Δ deaths heat]

## Calima proxy and mortality
[level distribution, deaths by level, main Δ finding]

## Seasonality note on calima proxy
[monthly distribution of intense weeks]

## Interaction: heat × calima
[2×2 table result + caveat on cell sizes]

## Interpretation
[4–5 bullet points: what the signal says and doesn't say]

## Limitations
[standard list + island-specific caveats]

## Output files
[tables + figures generated]

## Session Log
[one entry per session that touched this island]
| Date | Action | Status | Notes |
|------|--------|--------|-------|

## Bottom line
[3–4 sentences: honest summary for non-technical reader]
```

---

## 4. Route to bring all islands to the same level

### Current gaps per island

| Island | Section 7 | Session Log in README | Format aligned |
|--------|-----------|----------------------|----------------|
| Tenerife | ❌ Missing | ❌ Missing | ❌ Old format |
| Gran Canaria | ❌ Missing | ❌ Missing | ⚠️ Partial |
| Fuerteventura | ❌ Missing | ❌ Missing | ⚠️ Partial |
| Lanzarote | ❌ Missing | ✅ Present | ✅ Reference |
| La Palma | ❌ Missing | ❌ Missing | ✅ New format |
| Gomera | ❌ Missing | ❌ Missing | ✅ New format |

### Action per island (ordered by priority)

**Priority 1 — Fuerteventura** (has active anomaly to investigate)
- [ ] Add Section 7.1: paradoxical calima-mortality relationship
- [ ] Run investigation: seasonality stratification, monthly control, small-n check
- [ ] Add Session Log to README

**Priority 2 — Lanzarote** (reference island, mostly complete)
- [ ] Add Section 7 placeholder (no anomaly identified yet)
- [ ] Verify notebook runs end-to-end

**Priority 3 — La Palma** (notebook exists, README complete)
- [ ] Execute all notebook cells and verify outputs match README
- [ ] Add Section 7 placeholder
- [ ] Add Session Log to README

**Priority 4 — Gran Canaria** (non-monotonic pattern = potential anomaly)
- [ ] Add Section 7.1: non-monotonic pattern (possible < no_calima)
- [ ] Add Session Log to README

**Priority 5 — Gomera** (low signal, small population)
- [ ] Add Section 7 placeholder with note: low-signal island, no anomaly identified
- [ ] Add Session Log to README

**Priority 6 — Tenerife** (different format, needs reformatting)
- [ ] Reformat README to standard structure
- [ ] Add Section 7 placeholder
- [ ] Add Session Log to README

---

## 5. Known anomalies by island

| Island | Anomaly | Section | Status |
|--------|---------|---------|--------|
| Fuerteventura | Δ−0.40/week: fewer deaths during intense calima | 7.1 | ⏳ Pending |
| Gran Canaria | Non-monotonic: `possible` < `no_calima` baseline | 7.1 | ⏳ Pending |
| Lanzarote | No clean gradient in intermediate levels (proxy-only signal at intense) | — | Documented in README, no deep dive needed yet |
| La Palma | No heat×calima overlap | — | Documented, no investigation needed |

---

## 6. Session start phrase for Chat

When opening Chat to work on a specific island EDA, paste this:

> "Vamos a trabajar en el EDA de [isla]. El notebook está en `islands/[isla]/eda_calima_mortality_[code].ipynb`. Lee el README en `islands/[isla]/README.md` para ver el estado actual y los hallazgos documentados. La estructura estándar está en `islands/ISLAND_EDA_STANDARD.md`. Hoy vamos a: [acción específica]."

---

*Última actualización: 2026-04-24*
