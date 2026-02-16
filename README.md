# Heat & Mortality in the Canary Islands — Exploratory Analysis

This project explores the relationship between ambient temperature and mortality in the Canary Islands using self-collected data on climate and deaths over the last few years. The goal is to understand how episodes of high temperature relate to changes in the number of deaths, and to build a basic foundation for estimating potential impacts as temperatures continue to rise.

## Objectives

- Clean and align daily (or weekly) time series of:
  - climate variables (e.g. daily max/mean temperature),
  - total deaths in the Canary Islands.
- Explore seasonal patterns in temperature and mortality separately.
- Analyze how mortality changes during hot periods compared to typical conditions.
- Test simple hypotheses about heat–mortality relationships (e.g. thresholds, lags).
- Document limitations and outline what would be needed for more robust modelling.

## Data

- **Climate data:** self-collected records for the Canary Islands (JSON).
- **Mortality data:** self-collected number of deaths (exported to CSV from Apple Numbers).

Raw data files are stored under `data/raw/` and are not shared publicly unless sources and permissions allow it. The focus of this repository is on the analysis workflow and methodology.
