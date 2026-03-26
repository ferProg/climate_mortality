<# para usar este archivo
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
cd C:\dev\projects\climate_mortality\src\ingests
.\run_cams_2025_airq_excel_batch.ps1 `
  -PythonExe "python" `
  -ScriptPath ".\build_cams_2025_airq_excel.py" `
  -WorkbookPath "C:\data\Air_Polution_GC_2015_2025_raw\Datos2016_2025\Datos2025\Datos 2025.xlsx" `
  -StartDate "2025-01-01" `
  -EndDate "2025-12-31" `
  -ReplaceSheet
#>
param(
    [string]$PythonExe = "python",
    [string]$ScriptPath = ".\build_cams_2025_airq_excel.py",
    [string]$WorkbookPath = "C:\data\Air_Polution_GC_2015_2025_raw\Datos2016_2025\Datos2025\Datos 2025.xlsx",
    [string]$StartDate = "2025-01-01",
    [string]$EndDate = "2025-12-31",
    [string]$RawDir = ".\data\raw\cams_temp",
    [int]$ChunkMonths = 1,
    [switch]$KeepNc,
    [switch]$ReplaceSheet
)

$ErrorActionPreference = "Stop"

$islands = @(
    "gran_canaria",
    "lanzarote",
    "fuerteventura",
    "la_palma",
    "gomera"
)

Write-Host "=== CAMS 2025 -> Excel por isla ===" -ForegroundColor Cyan
Write-Host "PythonExe   : $PythonExe"
Write-Host "ScriptPath  : $ScriptPath"
Write-Host "WorkbookPath: $WorkbookPath"
Write-Host "StartDate   : $StartDate"
Write-Host "EndDate     : $EndDate"
Write-Host "RawDir      : $RawDir"
Write-Host "ChunkMonths : $ChunkMonths"
Write-Host "Islas       : $($islands -join ', ')"
Write-Host ""

if (-not (Test-Path $ScriptPath)) {
    throw "No se encontró el script Python: $ScriptPath"
}

$workbookDir = Split-Path -Parent $WorkbookPath
if (-not (Test-Path $workbookDir)) {
    New-Item -ItemType Directory -Force -Path $workbookDir | Out-Null
}

foreach ($island in $islands) {
    Write-Host ""
    Write-Host ("=" * 70) -ForegroundColor DarkGray
    Write-Host "Procesando isla: $island" -ForegroundColor Yellow

    $args = @(
        $ScriptPath,
        "--island", $island,
        "--start", $StartDate,
        "--end", $EndDate,
        "--workbook", $WorkbookPath,
        "--raw-dir", $RawDir,
        "--chunk-months", "$ChunkMonths"
    )

    if ($ReplaceSheet) {
        $args += "--replace-sheet"
    }

    if ($KeepNc) {
        $args += "--keep-nc"
    }

    & $PythonExe @args

    if ($LASTEXITCODE -ne 0) {
        throw "Falló la ejecución para la isla: $island"
    }

    Write-Host "OK -> $island" -ForegroundColor Green
}

Write-Host ""
Write-Host "Proceso completado." -ForegroundColor Green
Write-Host "Workbook actualizado en: $WorkbookPath" -ForegroundColor Green
