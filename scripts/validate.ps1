$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$root = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $root

$goFiles = @(
    "cmd/beneficiary-app/main.go",
    "internal/app/bootstrap.go",
    "internal/app/run.go",
    "internal/config/config.go",
    "internal/service/backup_service.go",
    "internal/ui/app.go",
    "internal/ui/import_screen.go",
    "internal/workflowsmoke/smoke_test.go"
)

Write-Host "Formatting Go files..."
gofmt -w $goFiles

Write-Host "Running tests (readonly module mode with Windows lock fallback)..."
& (Join-Path $PSScriptRoot "test.ps1") -AllowKnownWindowsExeLockFallback
if ($LASTEXITCODE -ne 0) {
    throw "Validation test step failed."
}

Write-Host "Validation completed."
