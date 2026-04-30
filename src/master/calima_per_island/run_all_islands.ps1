# run_all_islands.ps1
# Ejecuta build_calima_proxy_v2.py para todas las islas
# CLI -> cd C:\Users\fdora\RA_Career\Projects\climate_mortality
#   .\src\master\calima_per_island\run_all_islands.ps1

$islands = @("tenerife","fuerteventura", "lanzarote", "gran_canaria", "la_palma", "gomera")

foreach ($island in $islands) {
    Write-Host "`n========================================" -ForegroundColor Green
    Write-Host "Processing: $island" -ForegroundColor Green
    Write-Host "========================================`n" -ForegroundColor Green
    
    python src/master/calima_per_island/build_calima_proxy_v2.py --island $island
    
    if ($LASTEXITCODE -ne 0) {
        Write-Host "✗ Error processing $island" -ForegroundColor Red
        exit 1
    }
}

Write-Host "`n✓ All islands processed successfully!" -ForegroundColor Green