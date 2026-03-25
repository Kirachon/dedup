param(
    [string]$Output = "build/bin/beneficiary-app.exe"
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$root = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $root

$outDir = Split-Path -Parent $Output
if ($outDir -and -not (Test-Path $outDir)) {
    New-Item -ItemType Directory -Path $outDir -Force | Out-Null
}

Write-Host "Building $Output ..."
go build -o $Output ./cmd/beneficiary-app
Write-Host "Build completed."
