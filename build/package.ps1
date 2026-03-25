$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$root = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $root

Write-Host "Wave 1 packaging placeholder."
Write-Host "Use scripts/build.ps1 to produce the executable."
Write-Host "Installer/signing steps are intentionally deferred to later waves."
