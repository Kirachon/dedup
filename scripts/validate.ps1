$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$root = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $root

$goFiles = @(
    "cmd/beneficiary-app/main.go",
    "internal/app/bootstrap.go",
    "internal/app/run.go",
    "internal/config/config.go"
)

Write-Host "Formatting Go files..."
gofmt -w $goFiles

Write-Host "Running tests (readonly module mode)..."
go test -mod=readonly ./...

Write-Host "Validation completed."
