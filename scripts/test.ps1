$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$root = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $root

Write-Host "Running tests..."
go test ./...
