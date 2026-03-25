param(
    [switch]$AllowKnownWindowsExeLockFallback
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
$PSNativeCommandUseErrorActionPreference = $false

$root = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $root

function Invoke-GoCommandCapture {
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

        $output = (($stdoutText + [Environment]::NewLine + $stderrText).Trim())
        return @{
            ExitCode = $process.ExitCode
            Output   = $output
        }
    } finally {
        Remove-Item -Path $stdoutPath -ErrorAction SilentlyContinue
        Remove-Item -Path $stderrPath -ErrorAction SilentlyContinue
    }
}

function Test-IsKnownWindowsExeLock {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Text
    )

    if ([string]::IsNullOrWhiteSpace($Text)) {
        return $false
    }

    return (
        $Text -match "internal\\app\.test:\s+open .*app\.test\.exe:\s+Access is denied\." -or
        $Text -match "internal\\ui\.test:\s+open .*ui\.test\.exe:\s+Access is denied\." -or
        $Text -match "go:\s+unlinkat .*\.test\.exe:\s+Access is denied\."
    )
}

Write-Host "Running tests..."
$primary = Invoke-GoCommandCapture -Arguments @("test", "./...")
if ($primary.ExitCode -eq 0) {
    Write-Host "Test run completed."
    exit 0
}

if (-not $AllowKnownWindowsExeLockFallback -or -not (Test-IsKnownWindowsExeLock -Text $primary.Output)) {
    throw "go test failed with exit code $($primary.ExitCode)."
}

Write-Warning "Detected known Windows temp test executable lock (Access is denied)."
Write-Warning "Running fallback validation: tests excluding internal/app and internal/ui, plus compile check for both packages."

$allPackagesRaw = & go list ./...
if ($LASTEXITCODE -ne 0) {
    throw "go list failed while preparing fallback package list."
}
$allPackages = @($allPackagesRaw)
$fallbackPackages = @($allPackages | Where-Object { $_ -ne "dedup/internal/app" -and $_ -ne "dedup/internal/ui" })

if ($fallbackPackages.Count -gt 0) {
    $fallbackArgs = @("test", "-mod=readonly") + $fallbackPackages
    $fallback = Invoke-GoCommandCapture -Arguments $fallbackArgs
    if ($fallback.ExitCode -ne 0) {
        throw "Fallback go test set failed with exit code $($fallback.ExitCode)."
    }
}

$compileCheck = Invoke-GoCommandCapture -Arguments @("build", "./internal/app", "./internal/ui")
if ($compileCheck.ExitCode -ne 0) {
    throw "Fallback compile check for internal/app and internal/ui failed with exit code $($compileCheck.ExitCode)."
}

Write-Warning "Fallback validation passed with internal/app and internal/ui test execution skipped due host policy lock."
