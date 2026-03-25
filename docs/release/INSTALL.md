# Install and Verification (Windows, Offline)

This runbook installs the portable release on a clean Windows workstation without network access.

## 1) Prerequisites

1. Windows 10 or Windows 11
2. Local folder write access for the release payload
3. Release folder containing:
   - `beneficiary-app.exe`
   - `manifest.json`
   - `checksums.sha256`
   - `THIRD_PARTY_NOTICES.md`

## 2) Copy the Release Payload

1. Copy the entire packaged release folder to the target machine (for example via USB).
2. Keep all files in one folder. Do not rename or remove manifest/checksum files.

## 3) Verify Integrity

Open PowerShell in the release folder and run:

```powershell
Get-Content .\checksums.sha256 | ForEach-Object {
    if ($_ -match '^([0-9a-fA-F]{64})\s+\*?(.+)$') {
        $expected = $Matches[1].ToLower()
        $fileName = $Matches[2]
        if (-not (Test-Path $fileName)) {
            Write-Error "Missing file: $fileName"
            return
        }
        $actual = (Get-FileHash -Path $fileName -Algorithm SHA256).Hash.ToLower()
        if ($actual -ne $expected) {
            Write-Error "Checksum mismatch: $fileName"
        } else {
            Write-Host "OK $fileName"
        }
    }
}
```

Also confirm `THIRD_PARTY_NOTICES.md` is present and readable.

## 4) First Launch Validation

1. Start `beneficiary-app.exe`.
2. Confirm the app opens with no internet dependency.
3. Confirm initial database bootstrap completes.
4. Confirm default database path is created unless overridden:
   - `%APPDATA%\beneficiary-app\beneficiary.db`
5. Confirm PSGC ingest status appears in the UI shell/dashboard.

## 5) Rollback/Recovery Expectations

1. Keep the previous release folder until validation completes.
2. Keep at least one DB snapshot before replacing an existing deployment.
3. If rollout fails:
   - Stop the new binary.
   - Restore previous binary folder.
   - Restore prior DB snapshot using the app backup/restore workflow.

