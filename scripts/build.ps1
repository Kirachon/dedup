param(
    [string]$Output = "build/bin/beneficiary-app.exe",
    [string]$GoCache = "build/.gocache",
    [string]$TempRoot = "build/.gotmp",
    [switch]$CleanTemp,
    [int]$MaxAttempts = 3,
    [int]$RetryDelaySeconds = 2
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
$PSNativeCommandUseErrorActionPreference = $false

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
if ($MaxAttempts -lt 1) {
    throw "MaxAttempts must be >= 1."
}
if ($RetryDelaySeconds -lt 0) {
    throw "RetryDelaySeconds must be >= 0."
}

if (-not (Test-Path $goCachePath)) {
    New-Item -ItemType Directory -Path $goCachePath -Force | Out-Null
}
if (-not (Test-Path $tempRootPath)) {
    New-Item -ItemType Directory -Path $tempRootPath -Force | Out-Null
}

$previousGoCache = $env:GOCACHE
$previousGoTmp = $env:GOTMPDIR

$goArgs = @(
    "build",
    "-trimpath",
    "-buildvcs=false",
    "-mod=readonly",
    "-o", $outputPath,
    "./cmd/beneficiary-app"
)

function Invoke-GoProcessCapture {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Arguments
    )

    $stdoutPath = [System.IO.Path]::GetTempFileName()
    $stderrPath = [System.IO.Path]::GetTempFileName()
    try {
        $process = Start-Process -FilePath "go" -ArgumentList $Arguments -NoNewWindow -Wait -PassThru -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath
        $stdoutText = ""
        $stderrText = ""
        if (Test-Path $stdoutPath) {
            $stdoutText = Get-Content -Path $stdoutPath -Raw
        }
        if (Test-Path $stderrPath) {
            $stderrText = Get-Content -Path $stderrPath -Raw
        }

        if (-not [string]::IsNullOrWhiteSpace($stdoutText)) {
            $stdoutText.TrimEnd("`r", "`n").Split("`n") | ForEach-Object { Write-Host $_.TrimEnd("`r") }
        }
        if (-not [string]::IsNullOrWhiteSpace($stderrText)) {
            $stderrText.TrimEnd("`r", "`n").Split("`n") | ForEach-Object { Write-Host $_.TrimEnd("`r") }
        }

        return @{
            ExitCode = $process.ExitCode
            Output   = (($stdoutText + [Environment]::NewLine + $stderrText).Trim())
        }
    } finally {
        Remove-Item -Path $stdoutPath -ErrorAction SilentlyContinue
        Remove-Item -Path $stderrPath -ErrorAction SilentlyContinue
    }
}

function Test-IsKnownWindowsLinkLock {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Text
    )

    if ([string]::IsNullOrWhiteSpace($Text)) {
        return $false
    }

    return (
        $Text -match "a\.out\.exe:\s+Access is denied\." -or
        $Text -match "go:\s+unlinkat .*a\.out\.exe:\s+Access is denied\."
    )
}

try {
    $env:GOCACHE = $goCachePath
    $succeeded = $false

    for ($attempt = 1; $attempt -le $MaxAttempts; $attempt++) {
        $buildTempPath = Join-Path $tempRootPath ("go-build-" + [guid]::NewGuid().ToString("N"))
        New-Item -ItemType Directory -Path $buildTempPath -Force | Out-Null
        $env:GOTMPDIR = $buildTempPath

        try {
            Write-Host "Building $outputPath ... (attempt $attempt/$MaxAttempts)"
            Write-Host "go $($goArgs -join ' ')"

            $result = Invoke-GoProcessCapture -Arguments $goArgs
            $exitCode = $result.ExitCode
            $outputText = $result.Output

            if ($exitCode -eq 0) {
                $succeeded = $true
                break
            }

            $knownLock = Test-IsKnownWindowsLinkLock -Text $outputText
            if ($knownLock -and $attempt -lt $MaxAttempts) {
                Write-Warning "Known Windows linker temp executable lock detected. Retrying with a fresh temp directory."
                if ($RetryDelaySeconds -gt 0) {
                    Start-Sleep -Seconds $RetryDelaySeconds
                }
                continue
            }

            if ($knownLock) {
                throw "go build failed due Windows temp executable lock after $MaxAttempts attempts. Consider allowing go.exe/toolchain in endpoint protection or using a trusted temp path."
            }

            throw "go build failed with exit code $exitCode"
        } finally {
            if ($CleanTemp -and (Test-Path $buildTempPath)) {
                Remove-Item -Path $buildTempPath -Recurse -Force -ErrorAction SilentlyContinue
            }
        }
    }

    if (-not $succeeded) {
        throw "go build did not complete successfully."
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
}
