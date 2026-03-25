# Release Notes v0.2.0

This release adds PSGC-backed location normalization reuse for messy CSV imports
and existing beneficiary backfill, while keeping the app fully offline.

## Highlights

- Pure-Go location normalization using the Cleanlist PSGC parity rules
- Public import template with split birthdate fields and PSGC-backed location cleanup
- Import preview/commit/resume tokens that carry normalization metadata for deterministic resumes
- Backfill flow for existing beneficiaries with dry-run and apply modes
- Regression coverage showing raw location label noise does not affect duplicate precheck or dedup
- Release and setup docs updated for the new import and backfill workflow

## Validation

- `scripts/build.ps1 -CleanTemp` passed
- `scripts/test.ps1` passed
- `scripts/validate.ps1` passed
- `scripts/smoke.ps1` passed

## Artifact

- `beneficiary-app.exe`
- `lib_geo_map_2025_202603251312.csv`
- `beneficiary_import_template.csv`
- `THIRD_PARTY_NOTICES.md`
- `manifest.json`
- `checksums.sha256`
