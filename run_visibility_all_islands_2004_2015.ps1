# run_visibility_all_islands_2004_2015.ps1
# Downloads visibility data from NOAA ISD for all 7 islands (2004-2015)

$islands = @(
    "gomera",
    "tenerife",
    "gran_canaria",
    "lanzarote",
    "fuerteventura",
    "la_palma",
    "hierro"
)

foreach ($island in $islands) {
    Write-Host "`n=== Processing $island ===" -ForegroundColor Cyan
    python -m src.ingests.visibility.run_island_pipeline `
        --isla $island `
        --start_date 2004-01-01 `
        --end_date 2015-12-31

    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR: $island failed (exit code $LASTEXITCODE)" -ForegroundColor Red
    } else {
        Write-Host "OK: $island complete" -ForegroundColor Green
    }
}

Write-Host "`n=== ALL ISLANDS DONE ===" -ForegroundColor Cyan