param(
    [string]$PythonExe = "python",
    [string]$ProjectRoot = "C:\dev\projects\climate_mortality",
    [string]$DataDir = "data\interim\cap\canarias",
    [string]$StartDate = "2016-01-01",
    [string]$EndDate = "2025-12-31"
)

$ErrorActionPreference = "Stop"

$islands = @(
    "gran_canaria",
    "lanzarote",
    "fuerteventura",
    "la_palma",
    "gomera"
)

Set-Location $ProjectRoot

Write-Host "=== CAP extract by island ===" -ForegroundColor Yellow
Write-Host "ProjectRoot : $ProjectRoot"
Write-Host "DataDir     : $DataDir"
Write-Host "StartDate   : $StartDate"
Write-Host "EndDate     : $EndDate"
Write-Host "Islands     : $($islands -join ', ')"
Write-Host ""

foreach ($island in $islands) {
    Write-Host ("=" * 80) -ForegroundColor DarkGray
    Write-Host "Procesando isla: $island" -ForegroundColor Cyan

    & $PythonExe -m src.ingests.cap.extract_canarias_avisos_by_island `
        --isla $island `
        --data-dir $DataDir `
        --start $StartDate `
        --end $EndDate `
        --save-alerts

    if ($LASTEXITCODE -ne 0) {
        throw "Falló CAP extract para isla: $island"
    }

    Write-Host "OK -> $island" -ForegroundColor Green
}

Write-Host ""
Write-Host "Proceso CAP completado." -ForegroundColor Green