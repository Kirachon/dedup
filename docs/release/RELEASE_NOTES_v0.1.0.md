# Release Notes v0.1.0

This release packages the offline beneficiary tool as a portable Windows build with local SQLite storage and PSGC ingestion.

## Highlights

- Fyne UI shell with Dashboard, Encoding, Dedup, Import, Export, History, Settings, and Backup tabs
- PSGC CSV ingestion into the local database for cascading location lookup
- Import preview/commit/resume workflow with manifest-validated exchange packages
- Deterministic dedup engine and review workflow
- Backup and restore flow with manifest and checksum verification
- Portable release packaging with bundled third-party notices and integrity manifests

## Validation

- `scripts/build.ps1` passed after Windows NOD32 exclusions were added
- `scripts/test.ps1` passed
- `scripts/validate.ps1` passed
- Package artifact created at `build/releases/offline-beneficiary-tool-win64-0.1.0/`

## Artifact

- `beneficiary-app.exe`
- `manifest.json`
- `checksums.sha256`
- `THIRD_PARTY_NOTICES.md`
