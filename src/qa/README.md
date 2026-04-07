# QA Module: Data Validation Pipeline

## Overview

QA scripts validate data integrity at different pipeline stages. They report problems without auto-filtering—you decide what to do based on severity.

---

## Scripts & Execution Order

### **Stage 1: Post-Ingestion** (after each feed is parsed)

#### 1.1 `qa_deaths_structure.py`
**What:** Validates deaths dataset for nulls, duplicates, missing weeks, and flag columns.

**When:** After ingesting deaths feed (post-cleaning).

**Command:**
```powershell
python src/qa/qa_deaths_structure.py `
  --master "data/processed/<island>/deaths/deaths_weekly_<code>_DATES.parquet" `
  --island "<code>" `
  --outdir "reports/tables"
```

**Outputs:**
- `qa_deaths_structure_<island>.csv` — Summary (nulls, duplicates, missing count)
- `qa_deaths_structure_<island>_flags.csv` — Flagged rows (if any problems found)

**Decision Logic:**
```
✅ If nulls < 1% AND duplicates = 0 AND missing_weeks < 2%
   → OK to proceed

⚠️ If nulls 1-5% OR missing_weeks 2-10%
   → INVESTIGATE (seasonal gap? parsing issue?)

❌ If nulls > 5% OR missing_weeks > 10%
   → STOP, check raw data source
```

---

#### 1.2 `qa_weather_ranges.py`
**What:** Validates weather dataset for physical impossibilities (temp, pressure, wind, humidity ranges) and logical violations (tmax >= tmin).

**When:** After ingesting weather feed (post-cleaning).

**Command:**
```powershell
python src/qa/qa_weather_ranges.py `
  --master "data/processed/<island>/weather/weather_weekly_<code>_YEARS.parquet" `
  --island "<code>" `
  --outdir "reports/tables"
```

**Outputs:**
- `qa_weather_ranges_<island>.csv` — Summary (violation counts per check)
- `qa_weather_ranges_<island>_flags.csv` — Flagged rows with violation details

**Ranges Checked:**
| Variable | Min | Max | Notes |
|----------|-----|-----|-------|
| Temperature | -50°C | +60°C | Canary Islands extremes |
| Pressure | 900 hPa | 1050 hPa | Sea-level range |
| Wind (mean) | 0 m/s | 40 m/s | Normal weather |
| Wind (gust) | 0 m/s | 80 m/s | Storm-level |
| Humidity | 0% | 100% | Physical limits |
| Precipitation | ≥ 0 mm | No max | No negative rain |

**Logic Checks:**
- `tmax >= tmin` (max temp ≥ min temp)

**Decision Logic:**
```
✅ If all checks = 0 violations
   → OK to proceed

⚠️ If 1-5 violations total (likely isolated sensor errors)
   → INVESTIGATE those rows, likely safe to exclude

❌ If > 5 violations or logic violations > 0
   → INVESTIGATE data source or sensor setup
```

---

#### 1.3 `qa_aemet_format_validation.py`
**What:** Validates that CSV→Parquet conversion preserves data integrity (max diffs per column).

**When:** When re-converting weather/weather_daily feeds from CSV→Parquet.

**Command:**
```powershell
python src/qa/qa_aemet_format_validation.py \
  --csv "data/raw/<island>/weather_daily.csv" \
  --parquet "data/processed/<island>/weather/weather_daily_<code>.parquet"
```

**Outputs:**
- Print summary of max differences per numeric column

**Decision Logic:**
```
✅ If all diffs ≈ 0 (rounding errors only)
   → OK, conversion is clean

❌ If any diff > 0.1 for same variable
   → STOP, conversion has data loss or corruption
```

---

### **Stage 2: Post-Merge** (after combining all feeds into master)

#### 2.1 `extreme_week_audit.py`
**What:** Audits outliers and extreme episodes in target variable (deaths_week).

**When:** After merge, before QA on proxy.

**Command:**
```powershell
python src/qa/extreme_week_audit.py `
  --master "data/processed/<island>/master/master_<code>_YEARS.parquet" `
  --island "<code>" `
  --variable "deaths_week" `
  --outdir "reports/tables"
```

**Outputs:**
- `extreme_week_audit_deaths_week_<island>.csv` — Summary (threshold, n_extreme, n_episodes, max_episode_duration)

**Metrics:**
- `n_extreme_weeks` — Count of weeks exceeding p95/p99
- `n_episodes` — Clusters of consecutive extreme weeks
- `max_episode_weeks` — Duration of longest cluster
- `delta_mean_y_anom` — Avg difference (extreme vs. normal)

**Decision Logic:**
```
✅ If n_extreme_weeks < 5% AND max_episode_weeks < 4
   → Isolated anomalies, likely investigable

⚠️ If n_extreme_weeks 5-10% OR max_episode_weeks 4-8
   → Check if there's a seasonal pattern or policy change

❌ If n_extreme_weeks > 15% OR max_episode_weeks > 20
   → Data quality issue, investigate raw source
```

---

### **Stage 3: Post-Feature Engineering** (after building proxy)

#### 3.1 `qa_calima_proxy_score.py`
**What:** Validates that calima proxy correlates with its source variables (PM10, visibility, dust flags) and target (deaths).

**When:** After `build_calima_proxy_weekly.py` generates proxy scores.

**Command:**
```powershell
python src/qa/qa_calima_proxy_score.py `
  --master "data/processed/<island>/master/master_<code>_YEARS.parquet" `
  --calima "data/processed/<island>/calima/calima_proxy_weekly_<code>_YEARS.parquet" `
  --island "<code>" `
  --outdir "reports/tables"
```

**Outputs:**
- `qa_calima_proxy_score_<island>.csv` — Proxy distribution (mean, p95, p99, missing %)
- `qa_calima_proxy_score_<island>_missing_by_year.csv` — Coverage by year
- `qa_calima_proxy_score_<island>_missing_by_month.csv` — Coverage by month
- `qa_calima_proxy_score_<island>_proxy_checks.csv` — **CRITICAL**: Correlations with PM10, dust flags, visibility, deaths
- `qa_calima_proxy_score_<island>_leadlag_corr.csv` — Lead/lag correlations with deaths
- `qa_calima_proxy_score_<island>_leadlag_corr_anom_woy.csv` — Deseasonalized lead/lag

**Critical Validation (proxy_checks.csv):**
```
Expected:
  - When cap_dust_yellow_plus_week=1 (dust detected), proxy_score should be visibly higher
  - PM10 correlation with proxy_score > 0.4 (strong positive)
  - low_vis_any_week=1 should correlate with higher proxy_score
  - deaths_week correlation with proxy >= 0 (any positive or weak)

If observed:
  - PM10 corr < 0.2 → PROBLEM: Proxy not measuring what it should
  - cap_dust pattern inverted → PROBLEM: Formula is broken
  - deaths_week corr << 0 (negative) → NOT a problem of proxy, but hypothesis question

Decision Logic:
  ✅ If PM10 corr > 0.4 AND cap_dust pattern expected AND coverage > 80%
     → Proxy is valid, proceed to EDA
  
  ❌ If PM10 corr < 0.2 OR cap_dust pattern broken
     → Proxy formula needs review/fix before analysis
```

---

## Full Pipeline Diagram

```
Raw Sources (deaths, weather, air quality, visibility, dust alerts)
  ↓
Ingestion (parse + type conversion)
  ↓
  ├─ qa_deaths_structure.py
  ├─ qa_weather_ranges.py
  └─ qa_aemet_format_validation.py (if reconverting format)
  ↓
Cleaning (remove nulls, fix obvious errors)
  ↓
Merge (4 feeds → 1 master, by week_start)
  ↓
extreme_week_audit.py (audit target variable)
  ↓
build_calima_proxy_weekly.py (synthesize proxy from PM10 + humidity + visibility + pressure)
  ↓
qa_calima_proxy_score.py (validate proxy makes sense)
  ↓
✅ SAFE TO EDA (if all QA passed)
  ↓
Analysis + Visualization
```

---

## Decision Framework

### Green Path: Proceed to EDA
```
Deaths structure: nulls < 1%, duplicates = 0, missing < 2%
Weather ranges: violations = 0 (or < 3 isolated)
Extreme weeks: < 5% of dataset, max episode < 4 weeks
Proxy checks: PM10 corr > 0.4, cap_dust pattern expected, coverage > 80%
Lead/lag: Some positive correlation at lag0-2

→ Data is trustworthy, proceed to EDA
```

### Yellow Path: Investigate, Then Proceed
```
Deaths structure: nulls 1-5% (seasonal gap suspected)
Weather ranges: 3-5 violations (likely sensor errors, localized)
Extreme weeks: 5-10% (possible seasonal or event-driven)
Proxy checks: PM10 corr 0.3-0.4 (weaker but acceptable)

→ Investigate root causes, exclude/flag problematic weeks, then EDA
```

### Red Path: Stop & Review
```
Deaths structure: nulls > 5% OR missing > 10%
Weather ranges: > 10 violations OR logic violations
Extreme weeks: > 15% OR max episode > 20 weeks
Proxy checks: PM10 corr < 0.2 OR cap_dust pattern broken

→ DO NOT PROCEED. Investigate data source, raw files, collection methodology.
   Contact data owner if needed.
```

---

## Loop: Automated QA for All Islands

```powershell
$islands = @("gcan", "tfe", "gomerab", "lanzarote", "lpa", "gomera", "hierro")
$master_path = "data/processed/<island>/master/master_<code>_YEARS.parquet"
$calima_path = "data/processed/<island>/calima/calima_proxy_weekly_<code>_YEARS.parquet"

foreach ($island in $islands) {
    Write-Host "======== QA for $island ========"
    
    # Post-merge audits
    python src/qa/extreme_week_audit.py --master $master_path --island $island --outdir "reports/tables"
    
    # Post-proxy validation
    python src/qa/qa_calima_proxy_score.py --master $master_path --calima $calima_path --island $island --outdir "reports/tables"
    
    Write-Host "Done: $island"
}

# Aggregate reports (optional, for review dashboard)
# python src/qa/aggregate_qa_reports.py --indir "reports/tables" --outdir "reports/qa_summary"
```

---

## Notes

- **QA does NOT auto-filter.** It reports. You decide.
- **QA is mandatory.** Skipping QA = publishing garbage.
- **QA time:** ~30 sec per island for all checks.
- **QA outputs:** Review CSVs in `reports/tables/island/<ISLAND>/` after each run.

---

**Last Updated:** 2026-04-07  
**Next Review:** After each pipeline update