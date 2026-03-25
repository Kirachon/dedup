$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$root = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $root

Write-Host "Running workflow smoke test..."
go test ./internal/workflowsmoke -run TestWorkflowSmoke -count=1 -v
