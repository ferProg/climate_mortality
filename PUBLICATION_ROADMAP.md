# Climate Mortality Project — Publication Roadmap

**Status:** Closure & Publication Phase (initiated May 7, 2026)  
**Analytical work:** ✅ Complete (Phases 1–5)  
**Next:** Prepare for public release

---

## 📦 PUBLICATION BLOCKLIST

**Estimate:** 7–8 hours total work (divided into 1h blocks over 7–10 days)

### FASE 1: Critical Path (Must do for "publication-ready" status)

| Block | Task | Deliverable | Est. Time | Status |
|-------|------|-------------|-----------|--------|
| **PUB-1** | Extract regression results from notebook | `/reports/regression/model_summary.csv` + regression findings | 1h | ⏳ Pendiente |
| **PUB-2** | Create `/reports/FINDINGS.md` (executive summary) | FINDINGS.md with key tables + methodology | 1h | ⏳ Pendiente |
| **PUB-3** | Update main README.md (quick nav + structure) | Cleaner README with section links | 1h | ⏳ Pendiente |
| **PUB-4** | Create `/REPRODUCIBILITY.md` (how to regenerate) | Step-by-step reproduction guide | 1h | ⏳ Pendiente |
| **PUB-5** | Create `/VALIDATION.md` (QA + caveats) | Checklist + known limitations documented | 0.75h | ⏳ Pendiente |

**Subtotal CRITICAL: 4.75 hours**

### FASE 2: Repository Cleanup (Important but not blocking)

| Block | Task | Deliverable | Est. Time | Status |
|-------|------|-------------|-----------|--------|
| **PUB-6** | Reorganize `/notebooks/` directory | Move `legacy/` + `eda_playground/` to `.ignored/` or document | 0.5h | ⏳ Pendiente |
| **PUB-7** | Audit & document `/src/` scripts | Add docstrings + src/README.md | 1h | ⏳ Pendiente |
| **PUB-8** | Verify island READMEs completeness | Update 6 island README.md files if needed | 1h | ⏳ Pendiente |
| **PUB-9** | Create `/reports/regression/` structure | Move regression outputs + curate tables | 0.75h | ⏳ Pendiente |

**Subtotal CLEANUP: 3.25 hours**

### FASE 3: Final Verification (Quick polish)

| Block | Task | Deliverable | Est. Time | Status |
|-------|------|-------------|-----------|--------|
| **PUB-10** | Pre-publication checklist | Sign-off on all 10 items → mark ready for release | 0.5h | ⏳ Pendiente |

**Subtotal VERIFICATION: 0.5 hours**

---

## 🗓️ SUGGESTED SCHEDULE

### **Week 6 (May 7–9) — MINIMAL**
- ⏳ Focus on formal evaluation (SQL, Power BI, Estadística)
- 🟢 **Optional micro-task if time:** PUB-1 (30 min) — just extract regression table from notebook as reference

### **Week 7 (May 12–16) — PRIMARY PUBLICATION PUSH**

**Monday May 12:**
- **PUB-1** (1h): Extract regression results → model_summary.csv
- **PUB-2** (1h): Write FINDINGS.md with tables

**Tuesday May 13:**
- **PUB-3** (1h): Update README with navigation
- **PUB-4** (1h): Write REPRODUCIBILITY.md

**Wednesday May 14:**
- **PUB-5** (0.75h): Create VALIDATION.md
- **PUB-6** (0.5h): Clean up /notebooks/

**Thursday May 15:**
- **PUB-7** (1h): Audit /src/ scripts
- **PUB-8** (1h): Verify island READMEs

**Thursday May 14:** ✅ COMPLETED
- **PUB-D** (2h): Island READMEs + requirements.txt update ✅
  - 6 island READMEs actualizados (Tenerife, Gran Canaria, Lanzarote, La Palma, Gomera, Fuerteventura)
  - Coverage + Key Findings + Temperature/Heat signals + Caveats documented
  - Gomera pressure anomaly (pres_p75 = 994.61 hPa) flagged with 🟡 ALERT
  - Regression models (TFE/GC) with DW diagnostics included
  - requirements.txt estructura clara (sin versiones pinned — to be completed)
  - src/README.md omitido (REPRODUCIBILITY.md covers execution order)

**Friday May 16:**
- **PUB-9** (0.75h): Curate /reports/regression/
- **PUB-10** (0.5h): Final checklist

**Total Week 7: ~8 hours (1h/day + 1h flex)**

---

## 🎯 INTEGRATION WITH RA CAREER PROGRAM

These blocks will be added to Week 7 calendar as a **parallel track** to main SQL/Excel/Power BI learning:

| Week 7 Bloque | Time | Slot | Owner |
|---------------|------|------|-------|
| climate_mortality: PUB-1 | 1h | PM-3 (Mon) | Ferdie |
| climate_mortality: PUB-2 | 1h | PM-3 (Tue) | Ferdie |
| climate_mortality: PUB-3 | 1h | AM-2 (Wed) | Ferdie |
| climate_mortality: PUB-4 | 1h | PM-3 (Wed) | Ferdie |
| climate_mortality: PUB-5 | 0.75h | AM-2 (Thu) | Ferdie |
| climate_mortality: PUB-6 | 0.5h | PM-3 (Thu) | Ferdie |
| climate_mortality: PUB-7 | 1h | AM-2 (Fri) | Ferdie |
| climate_mortality: PUB-8 | 1h | PM-3 (Fri) | Ferdie |
| climate_mortality: PUB-9 | 0.75h | AM-2 (Fri) | Ferdie |
| climate_mortality: PUB-10 | 0.5h | Flex/Buffer | Ferdie |

**Note:** These are low-intensity "housekeeping" blocks that don't require learning new concepts — just organization + writing. Can be done in parallel with main skills work.

---

## 📝 BLOCK DESCRIPTIONS (For Cowork integration)

### PUB-1: Extract Regression Results (1h)
- Open `CCAA/regression/regression_tfe_gc_modeling.ipynb`
- Extract tables: simple model (coefs, SE, p-values) + multiple model
- Extract: model comparison (R², AIC, DW, residual plots)
- Save as structured CSVs in `/reports/regression/`
- Output: `model_summary.csv`, `diagnostics_summary.csv`

### PUB-2: Write FINDINGS.md (1h)
- Create `/reports/FINDINGS.md`
- Sections: motivation (1 para) | methods (1 para) | key findings (table) | regression results (table) | caveats (bullets)
- Include: Δ deaths by island, p-values, η², regression coefs (simple + multiple), model comparison
- Reference: findings already in README + regression notebook

### PUB-3: Update README Navigation (1h)
- Add "Quick Navigation" table of contents near top
- Restructure main README: add section links (Analysis | Reproducibility | Data | Code)
- Simplify "Findings" section → reference to FINDINGS.md for full results
- Verify all links work

### PUB-4: Write REPRODUCIBILITY.md (1h)
- Create `/REPRODUCIBILITY.md`
- Step-by-step: what data to download (INE, AEMET, Aire Canarias links)
- Order of execution: which scripts to run (src/ingests → src/master → notebooks)
- Expected outputs at each step
- Estimated runtime for full regeneration

### PUB-5: Create VALIDATION.md (0.75h)
- Create `/VALIDATION.md`
- QA checklist: nulls ✅ | ranges ✅ | types ✅ | features ✅ (May 7 audit)
- Known limitations: weekly resolution, CAP 2018+, DAI gaps after 2022, island-specific quality variance
- Caveats: observational (no causality), confounding possible, seasonality controlled by dummies
- Transparency statement: what was verified vs what remains exploratory

### PUB-6: Reorganize Notebooks (0.5h)
- Review `notebooks/legacy/`, `notebooks/eda_playground/`, `regression_playground.ipynb`
- Move non-final work to `.ignored_local/` or mark as "deprecated" in README
- Keep: `island_eda_template.ipynb`, final regression notebook (if public release includes it)
- Document: what's deprecated and why

### PUB-7: Audit /src/ Scripts (1h)
- Review each script in `src/data/`, `src/ingests/`, `src/master/`, `src/qa/`, `src/utils/`
- Add/verify docstrings (what does each script do? what are inputs/outputs?)
- Create `src/README.md` with execution order
- Verify no hardcoded paths or credentials

### PUB-8: Verify Island READMEs (1h)
- Open each of 6 island READMEs
- Check: data coverage (n weeks, years) | key findings (Δ deaths, p, η²) | seasonality | caveats documented
- Update any stale content
- Verify cross-references to main README + notebooks

### PUB-9: Curate /reports/regression/ (0.75h)
- Create subdirectory structure: `/reports/regression/` with: `figures/`, `tables/`
- Move diagnostic plots from `CCAA/figures/` to `reports/regression/figures/`
- Create index: `reports/regression/README.md` explaining outputs
- Verify all regression outputs are referenced in FINDINGS.md

### PUB-10: Final Checklist (0.5h)
- Verify 10-point checklist (see below)
- Sign-off: project is "publication-ready"
- Mark in `climate_mortality/README.md`: "Status: Ready for release (May 16, 2026)"

---

## ✅ PUBLICATION-READY CHECKLIST

- [ ] README navigates clearly to analysis (quick nav works)
- [ ] Hallazgos principales visibles sin entrar en code (FINDINGS.md completo)
- [ ] Reproducibilidad documentada paso a paso (REPRODUCIBILITY.md)
- [ ] Validación transparente (VALIDATION.md + QA checklist)
- [ ] No datos raw públicos (gitignore respetado, solo processed/)
- [ ] License visible (MIT)
- [ ] Ningún email / info personal en notebooks
- [ ] Figuras nombradas y referenciadas en reportes
- [ ] Links a fuentes de datos funcionan
- [ ] Scripts `/src/` documentados + orden claro

---

## 📌 NEXT STEPS FOR TODAY (May 7)

- ✅ This roadmap created
- ✅ Blocks defined + estimated times
- ⏳ **For Cowork:** Integrate into Week 7 calendar (optional: add as "parallel track" PM slots)
- ⏳ **Decision:** Execute PUB-1 this week if <30 min extra time, or start fresh Monday May 12

**Recommendation:** Close Week 6 evaluation tomorrow (Sat 9 May). Start publication blocks Monday (May 12) in Week 7 roadmap.

---

**Last updated:** 2026-05-07  
**Owner:** Ferdie  
**Status:** Planning phase → ready to execute Week 7
