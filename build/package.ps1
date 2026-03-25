param(
    [string]$Version = "0.1.0-dev",
    [string]$OutputRoot = "build/releases",
    [string]$BinaryPath = "build/bin/beneficiary-app.exe",
    [string]$NoticesPath = "THIRD_PARTY_NOTICES.md",
    [switch]$SkipBuild,
    [string]$GoCache = "build/.gocache",
    [string]$TempRoot = "build/.gotmp",
    [switch]$CleanOutput
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$root = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $root
$psgcCsvName = "lib_geo_map_2025_202603251312.csv"
$templateCsvName = "beneficiary_import_template.csv"
$psgcCsvSource = Join-Path $root $psgcCsvName
$templateCsvSource = Join-Path $root $templateCsvName

function Resolve-AbsolutePath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$PathValue
    )
    if ([System.IO.Path]::IsPathRooted($PathValue)) {
        return [System.IO.Path]::GetFullPath($PathValue)
    }
    return [System.IO.Path]::GetFullPath((Join-Path $root $PathValue))
}

$sourceCommit = "unknown"
try {
    $gitCommit = & git rev-parse --short=12 HEAD
    if ($LASTEXITCODE -eq 0) {
        $sourceCommit = ($gitCommit | Out-String).Trim()
    }
} catch {
    $sourceCommit = "unknown"
}

$goVersion = "unknown"
try {
    $versionOutput = & go version
    if ($LASTEXITCODE -eq 0) {
        $goVersion = ($versionOutput | Out-String).Trim()
    }
} catch {
    $goVersion = "unknown"
}

$binaryAbsolutePath = Resolve-AbsolutePath -PathValue $BinaryPath
$noticesAbsolutePath = Resolve-AbsolutePath -PathValue $NoticesPath
$outputRootPath = Resolve-AbsolutePath -PathValue $OutputRoot

if (-not $SkipBuild) {
    & (Join-Path $root "scripts/build.ps1") -Output $binaryAbsolutePath -GoCache $GoCache -TempRoot $TempRoot -CleanTemp
    if ($LASTEXITCODE -ne 0) {
        throw "Build step failed."
    }
}

if (-not (Test-Path $binaryAbsolutePath)) {
    throw "Binary not found at $binaryAbsolutePath"
}
if (-not (Test-Path $noticesAbsolutePath)) {
    throw "Third-party notices file not found at $noticesAbsolutePath"
}

$packageName = "offline-beneficiary-tool-win64-$Version"
$releaseDir = Join-Path $outputRootPath $packageName
if ($CleanOutput -and (Test-Path $releaseDir)) {
    Remove-Item -Path $releaseDir -Recurse -Force
}
if (Test-Path $releaseDir) {
    Remove-Item -Path $releaseDir -Recurse -Force
}
New-Item -ItemType Directory -Path $releaseDir -Force | Out-Null

$binaryReleasePath = Join-Path $releaseDir "beneficiary-app.exe"
$noticesReleasePath = Join-Path $releaseDir "THIRD_PARTY_NOTICES.md"
$psgcCsvReleasePath = Join-Path $releaseDir $psgcCsvName
$templateCsvReleasePath = Join-Path $releaseDir $templateCsvName
Copy-Item -Path $binaryAbsolutePath -Destination $binaryReleasePath -Force
Copy-Item -Path $noticesAbsolutePath -Destination $noticesReleasePath -Force
if (Test-Path $psgcCsvSource) {
    Copy-Item -Path $psgcCsvSource -Destination $psgcCsvReleasePath -Force
} else {
    throw "PSGC CSV not found at $psgcCsvSource"
}
if (Test-Path $templateCsvSource) {
    Copy-Item -Path $templateCsvSource -Destination $templateCsvReleasePath -Force
} else {
    throw "Import template CSV not found at $templateCsvSource"
}

$manifestPath = Join-Path $releaseDir "manifest.json"
$shaSumsPath = Join-Path $releaseDir "checksums.sha256"

$filesForManifest = Get-ChildItem -File -Path $releaseDir |
    Where-Object { $_.Name -ne "checksums.sha256" -and $_.Name -ne "manifest.json" } |
    Sort-Object Name

$manifestFiles = @()
foreach ($file in $filesForManifest) {
    $hash = (Get-FileHash -Path $file.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
    $manifestFiles += [ordered]@{
        path = $file.Name
        size_bytes = $file.Length
        sha256 = $hash
    }
}

$manifest = [ordered]@{
    schema_version = "release-manifest-v1"
    package_name = $packageName
    version = $Version
    platform = "windows-amd64"
    source_commit = $sourceCommit
    go_version = $goVersion
    files = $manifestFiles
}
$manifestJson = $manifest | ConvertTo-Json -Depth 10
[System.IO.File]::WriteAllText($manifestPath, $manifestJson, (New-Object System.Text.UTF8Encoding($false)))

$hashTargets = Get-ChildItem -File -Path $releaseDir |
    Where-Object { $_.Name -ne "checksums.sha256" } |
    Sort-Object Name

$hashLines = foreach ($file in $hashTargets) {
    $hash = (Get-FileHash -Path $file.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
    "$hash  $($file.Name)"
}
Set-Content -Path $shaSumsPath -Value $hashLines -Encoding ascii

Write-Host "Packaging completed."
Write-Host "Release directory: $releaseDir"
Write-Host "Manifest: $manifestPath"
Write-Host "Checksums: $shaSumsPath"
