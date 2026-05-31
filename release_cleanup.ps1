# release_cleanup.ps1
# Ejecutar desde la raiz del proyecto:
#   cd C:\Users\fdora\RA_Career\Projects\climate_mortality
#   .\release_cleanup.ps1
#
# Qué hace:
# 1) Elimina del tracking (git rm --cached) los archivos legacy/internos
# 2) Elimina físicamente esos directorios
# 3) Añade al tracking los nuevos archivos públicos
# 4) Hace commit de todos los cambios

# --- 1. ELIMINAR DEL TRACKING ---
Write-Host "`n[1/4] Removing legacy files from git tracking..." -ForegroundColor Yellow

git rm -r --cached `
    notebooks/legacy/ `
    notebooks/eda_playground/ `
    notebooks/regression_playground.ipynb `
    notebooks/reports/ `
    notebooks/30_years_interval/ `
    run_visibility_all_islands_2004_2015.ps1 `
    run_weather_all_islands.ps1 `
    2>$null

# data/processed/tfe (legacy air quality dir)
git rm -r --cached data/processed/tfe/ 2>$null

Write-Host "Done." -ForegroundColor Green

# --- 2. ELIMINAR FÍSICAMENTE ---
Write-Host "`n[2/4] Deleting legacy directories from disk..." -ForegroundColor Yellow

$toDelete = @(
    "notebooks\legacy",
    "notebooks\eda_playground",
    "notebooks\reports",
    "notebooks\30_years_interval",
    "data\processed\tfe"
)

foreach ($dir in $toDelete) {
    if (Test-Path $dir) {
        Remove-Item -Recurse -Force $dir
        Write-Host "  Deleted: $dir"
    }
}

if (Test-Path "notebooks\regression_playground.ipynb") {
    Remove-Item -Force "notebooks\regression_playground.ipynb"
    Write-Host "  Deleted: notebooks\regression_playground.ipynb"
}

Write-Host "Done." -ForegroundColor Green

# --- 3. AÑADIR NUEVOS ARCHIVOS ---
Write-Host "`n[3/4] Adding new public files to tracking..." -ForegroundColor Yellow

git add `
    .gitignore `
    FINDINGS.md `
    README.md `
    VALIDATION.md `
    CCAA/regression/regression_regional.ipynb `
    src/master/build_master_regional.py `
    src/master/build_calibration_dataset.py `
    src/master/provinces/ `
    src/qa/audit_airq.py `
    src/qa/audit_deaths.py `
    src/qa/audit_master_regional.py `
    src/qa/audit_masters.py `
    src/qa/audit_visibility.py `
    src/ingests/airq/00_build_pollutants_2000_2025.py `
    src/ingests/airq/build_pollutants_2000_2025.py `
    reports/ccaa/figures/calibration_coefficients.png `
    reports/ccaa/figures/calibration_distributions.png `
    reports/ccaa/figures/calibration_label_timeline.png `
    reports/ccaa/figures/calibration_roc.png `
    reports/ccaa/figures/calibration_score_distribution.png `
    reports/ccaa/figures/regression_regional/ `
    reports/final/

Write-Host "Done." -ForegroundColor Green

# --- 4. COMMIT ---
Write-Host "`n[4/4] Committing..." -ForegroundColor Yellow

git commit -m "release: clean repo for public publication

- Remove legacy/exploratory notebooks (notebooks/legacy, eda_playground, 30_years_interval)
- Remove internal figures (notebooks/reports/)
- Remove deleted ps1 scripts
- Remove legacy data/processed/tfe/
- Add regional regression notebook and src scripts
- Add proxy v4 calibration figures and reports/final/ portfolio figures
- Update .gitignore for clean public release"

Write-Host "`nRelease cleanup complete." -ForegroundColor Green
git log --oneline -3
