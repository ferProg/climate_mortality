# run_weather_all_islands.ps1
# Runs run_weather_pipeline.py for all 7 Canary Islands (2004-01-01 to 2025-12-31)

$env:AEMET_API_KEY = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJmcmFtb3MuZGF0YUBnbWFpbC5jb20iLCJqdGkiOiJlYzYxODM1Ni04OTAyLTRjMzgtODZiZi0yMWJiNjgzMjg3MmQiLCJpc3MiOiJBRU1FVCIsImlhdCI6MTc3NDM1OTE3MiwidXNlcklkIjoiZWM2MTgzNTYtODkwMi00YzM4LTg2YmYtMjFiYjY4MzI4NzJkIiwicm9sZSI6IiJ9.XnPrUYPagv5lweXVd9_LLdb0F3EOwb3wlWLkLcL7p3g"


$islands = @(
    @{ station = "C329B"; island = "gomera" },
    @{ station = "C429I"; island = "tenerife" },
    @{ station = "C649I"; island = "gran_canaria" },
    @{ station = "C029O"; island = "lanzarote" },
    @{ station = "C249I"; island = "fuerteventura" },
    @{ station = "C139E"; island = "la_palma" },
    @{ station = "C929I"; island = "hierro" }
)

foreach ($entry in $islands) {
    Write-Host "`n=== Processing $($entry.island) ===" -ForegroundColor Cyan
    python -m src.ingests.weather.run_weather_pipeline `
        --station $entry.station `
        --start 2004-01-01 `
        --end 2015-12-31 `
        --island $entry.island

    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR: $($entry.island) failed (exit code $LASTEXITCODE)" -ForegroundColor Red
    } else {
        Write-Host "OK: $($entry.island) complete" -ForegroundColor Green
    }
}

Write-Host "`n=== ALL ISLANDS DONE ===" -ForegroundColor Cyan