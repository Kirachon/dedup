$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$root = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $root

Write-Host "Starting beneficiary app scaffold..."
go run ./cmd/beneficiary-app
