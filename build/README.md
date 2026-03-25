# Build Surface

This directory contains Windows-first build and packaging helpers for the offline beneficiary tool.

## Scripts

1. `scripts/build.ps1`
Builds `cmd/beneficiary-app` to `build/bin/beneficiary-app.exe` by default.
Uses reproducibility-friendly flags:
`-trimpath`, `-buildvcs=false`, and `-mod=readonly`.
Also supports isolated Go temp/cache paths via `-GoCache` and `-TempRoot`.

2. `build/package.ps1`
Builds (unless `-SkipBuild` is used), then assembles a portable release folder under `build/releases/`.
Release payload includes:
- `beneficiary-app.exe`
- `THIRD_PARTY_NOTICES.md`
- `manifest.json`
- `checksums.sha256`

Installer and signing steps are intentionally deferred to a later wave.

## Usage

Build executable:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/build.ps1
```

Create a release package:

```powershell
powershell -ExecutionPolicy Bypass -File build/package.ps1 -Version 0.1.0
```

Package without rebuilding (requires an existing binary at `build/bin/beneficiary-app.exe`):

```powershell
powershell -ExecutionPolicy Bypass -File build/package.ps1 -Version 0.1.0 -SkipBuild
```

## Artifact Layout Example

`build/releases/offline-beneficiary-tool-win64-0.1.0/`
- `beneficiary-app.exe`
- `THIRD_PARTY_NOTICES.md`
- `manifest.json`
- `checksums.sha256`

`manifest.json` stores file metadata (path, size, sha256) together with source
commit and Go toolchain information, while `checksums.sha256` provides an
operator-friendly integrity check list.
