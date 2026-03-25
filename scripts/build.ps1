param(
    [string]$Output = "build/bin/beneficiary-app.exe",
    [string]$GoCache = "build/.gocache",
    [string]$TempRoot = "build/.gotmp",
    [switch]$CleanTemp
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$root = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $root

if ([System.IO.Path]::IsPathRooted($Output)) {
    $outputPath = [System.IO.Path]::GetFullPath($Output)
} else {
    $outputPath = [System.IO.Path]::GetFullPath((Join-Path $root $Output))
}

$outDir = Split-Path -Parent $outputPath
if ($outDir -and -not (Test-Path $outDir)) {
    New-Item -ItemType Directory -Path $outDir -Force | Out-Null
}

if ([System.IO.Path]::IsPathRooted($GoCache)) {
    $goCachePath = [System.IO.Path]::GetFullPath($GoCache)
} else {
    $goCachePath = [System.IO.Path]::GetFullPath((Join-Path $root $GoCache))
}
if ([System.IO.Path]::IsPathRooted($TempRoot)) {
    $tempRootPath = [System.IO.Path]::GetFullPath($TempRoot)
} else {
    $tempRootPath = [System.IO.Path]::GetFullPath((Join-Path $root $TempRoot))
}

if (-not (Test-Path $goCachePath)) {
    New-Item -ItemType Directory -Path $goCachePath -Force | Out-Null
}
if (-not (Test-Path $tempRootPath)) {
    New-Item -ItemType Directory -Path $tempRootPath -Force | Out-Null
}

$buildTempPath = Join-Path $tempRootPath ("go-build-" + [guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $buildTempPath -Force | Out-Null

$previousGoCache = $env:GOCACHE
$previousGoTmp = $env:GOTMPDIR
$env:GOCACHE = $goCachePath
$env:GOTMPDIR = $buildTempPath

$goArgs = @(
    "build",
    "-trimpath",
    "-buildvcs=false",
    "-mod=readonly",
    "-o", $outputPath,
    "./cmd/beneficiary-app"
)

try {
    Write-Host "Building $outputPath ..."
    Write-Host "go $($goArgs -join ' ')"
    & go @goArgs
    if ($LASTEXITCODE -ne 0) {
        throw "go build failed with exit code $LASTEXITCODE"
    }
    Write-Host "Build completed."
    Write-Host "Output: $outputPath"
} finally {
    if ($null -ne $previousGoCache) {
        $env:GOCACHE = $previousGoCache
    } else {
        Remove-Item Env:GOCACHE -ErrorAction SilentlyContinue
    }
    if ($null -ne $previousGoTmp) {
        $env:GOTMPDIR = $previousGoTmp
    } else {
        Remove-Item Env:GOTMPDIR -ErrorAction SilentlyContinue
    }

    if ($CleanTemp -and (Test-Path $buildTempPath)) {
        Remove-Item -Path $buildTempPath -Recurse -Force -ErrorAction SilentlyContinue
    }
}
