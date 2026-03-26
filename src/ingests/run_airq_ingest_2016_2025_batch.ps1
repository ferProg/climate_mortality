<# para usar este archivo: pwsh
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
cd C:\dev\projects\climate_mortality\src\ingests

.\run_airq_ingest_2016_2025_batch.ps1 `
  -PythonExe "python" `
  -ProjectRoot "C:\dev\projects\climate_mortality" `
  -AirqRoot "C:\data\Air_Polution_GC_2015_2025_raw\Datos2016_2025" `
  -DailyOutDir "data\interim\air_q" `
  -ProcessedDir "data\processed" `
  -StartYear 2016 `
  -EndYear 2025
#>
param(
    [string]$PythonExe = "python",
    [string]$ProjectRoot = "C:\dev\projects\climate_mortality",
    [string]$AirqRoot = "C:\data\Air_Polution_GC_2015_2025_raw\Datos2016_2025",
    [string]$DailyOutDir = "data\interim\air_q",
    [string]$ProcessedDir = "data\processed",
    [int]$StartYear = 2016,
    [int]$EndYear = 2025,
    [switch]$UsePatchedDailyScript,
    [string]$PatchedDailyScriptPath = ".\build_airq_daily_patched.py"
)

$ErrorActionPreference = "Stop"

$islands = @(
    @{ Canonical = "gran_canaria"; Code = "gcan" },
    @{ Canonical = "lanzarote"; Code = "lzt" },
    @{ Canonical = "fuerteventura"; Code = "ftv" },
    @{ Canonical = "la_palma"; Code = "lpa" },
    @{ Canonical = "gomera"; Code = "gom" }
)

function Invoke-Step {
    param(
        [string]$Title,
        [scriptblock]$Action
    )
    Write-Host ""
    Write-Host ("=" * 80) -ForegroundColor DarkGray
    Write-Host $Title -ForegroundColor Cyan
    & $Action
    if ($LASTEXITCODE -ne 0) {
        throw "Falló el paso: $Title"
    }
}

Set-Location $ProjectRoot

Write-Host "=== AIR QUALITY INGEST 2016-2025 ===" -ForegroundColor Yellow
Write-Host "ProjectRoot           : $ProjectRoot"
Write-Host "AirqRoot              : $AirqRoot"
Write-Host "DailyOutDir           : $DailyOutDir"
Write-Host "ProcessedDir          : $ProcessedDir"
Write-Host "StartYear             : $StartYear"
Write-Host "EndYear               : $EndYear"
Write-Host "Islands               : $($islands.Canonical -join ', ')"
Write-Host "UsePatchedDailyScript : $UsePatchedDailyScript"
Write-Host ""

foreach ($item in $islands) {
    $island = $item.Canonical
    $code = $item.Code

    Write-Host ""
    Write-Host ("#" * 80) -ForegroundColor DarkYellow
    Write-Host "ISLAND: $island ($code)" -ForegroundColor Yellow

    if ($UsePatchedDailyScript) {
        Invoke-Step -Title "Daily build [$island] using patched script" -Action {
            & $PythonExe $PatchedDailyScriptPath `
                --island $code `
                --start-year $StartYear `
                --end-year $EndYear `
                --root $AirqRoot `
                --outdir $DailyOutDir
        }
    }
    else {
        Invoke-Step -Title "Daily build [$island] using module" -Action {
            & $PythonExe -m src.ingests.airq.build_airq_daily `
                --island $code `
                --start-year $StartYear `
                --end-year $EndYear `
                --root $AirqRoot `
                --outdir $DailyOutDir
        }
    }

    Invoke-Step -Title "Daily sanity [$island]" -Action {
        & $PythonExe -c @"
import pandas as pd
from pathlib import Path
fp = Path(r"$DailyOutDir") / "daily_$code.csv"
df = pd.read_csv(fp)
df["date"] = pd.to_datetime(df["date"], errors="coerce")
print("file:", fp)
print("shape:", df.shape)
print("min/max:", df["date"].min(), df["date"].max())
print("dup dates:", int(df["date"].duplicated().sum()))
print("null dates:", int(df["date"].isna().sum()))
print(df[["PM10","PM2.5","SO2","NO2","O3","station"]].notna().sum())
print(df.head(3))
print(df.tail(3))
"@
    }

    Invoke-Step -Title "Weekly build [$island]" -Action {
        & $PythonExe -m src.ingests.airq.build_weekly_airq_island `
            --island $island `
            --daily-dir $DailyOutDir `
            --processed-dir $ProcessedDir
    }

    Invoke-Step -Title "Weekly sanity [$island]" -Action {
        & $PythonExe -c @"
import pandas as pd
from pathlib import Path
fp = max(Path(r"$ProcessedDir\$island\air_quality").glob("*.parquet"), key=lambda p: p.stat().st_mtime)
df = pd.read_parquet(fp)
df["week_start"] = pd.to_datetime(df["week_start"], errors="coerce")
print("file:", fp)
print("shape:", df.shape)
print("min/max:", df["week_start"].min(), df["week_start"].max())
print("dup weeks:", int(df["week_start"].duplicated().sum()))
print("null weeks:", int(df["week_start"].isna().sum()))
print(df.head(3))
print(df.tail(3))
"@
    }
}

Write-Host ""
Write-Host "Proceso completado para todas las islas objetivo." -ForegroundColor Green
