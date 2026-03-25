# Offline Beneficiary Tool Release Guide

This directory contains operator-facing release instructions for the portable Windows build of the Offline Beneficiary Tool.

## Release Artifact Expectations

The packaging output should contain one portable release folder with these minimum files:

1. `beneficiary-app.exe`
2. `manifest.json`
3. `checksums.sha256`
4. `THIRD_PARTY_NOTICES.md`

If the packaging script uses a different folder name, keep the same required file set.

## Integrity Requirements

Before deployment, validate:

1. `manifest.json` exists and describes the shipped files.
2. `checksums.sha256` contains SHA-256 entries for payload files.
3. `Get-FileHash` values match the declared checksums.
4. `THIRD_PARTY_NOTICES.md` is bundled in the same release folder.

## Operator Docs in This Folder

1. `INSTALL.md` - clean-machine install and first-run verification
2. `RELEASE_CHECKLIST.md` - release gate checklist for deployment readiness

