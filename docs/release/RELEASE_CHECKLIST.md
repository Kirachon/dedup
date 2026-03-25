# Release Checklist

Use this checklist before promoting a portable release of the Offline Beneficiary Tool.

## Package Integrity

- [ ] Build completed with `scripts/build.ps1`
- [ ] Package completed with `build/package.ps1`
- [ ] Release folder contains:
  - [ ] `beneficiary-app.exe`
  - [ ] `manifest.json`
  - [ ] `checksums.sha256`
  - [ ] `THIRD_PARTY_NOTICES.md`
- [ ] `checksums.sha256` matches the file hashes in the release folder
- [ ] `manifest.json` includes the expected package name, source commit, and Go version

## Operator Readiness

- [ ] `THIRD_PARTY_NOTICES.md` is readable and bundled with the release
- [ ] Release folder is copied intact to the target Windows machine
- [ ] The target machine can launch `beneficiary-app.exe` without network access
- [ ] The app opens its database and shows PSGC/bootstrap status in the shell
- [ ] A backup snapshot exists before replacing any live deployment

## Rollback Readiness

- [ ] Previous release folder is preserved until the new deployment is validated
- [ ] A rollback copy of the database snapshot is available
- [ ] Operators know how to restore the previous binary and database if validation fails

If any box is unchecked, do not promote the release.
