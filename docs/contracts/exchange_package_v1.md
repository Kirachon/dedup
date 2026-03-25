# exchange_package_v1 (FROZEN)

- Status: Frozen for wave handoff
- Version: `v1`
- Last Updated: `2026-03-25`
- Change policy: Structural file-layout changes require `v2`.

## Purpose
Defines open exchange package format for offline data movement between installations.

## Package Type
- Container: ZIP archive
- Encoding: UTF-8 for text payloads
- Integrity algorithm: `sha256`

## Required Files
1. `manifest.json`
2. `beneficiaries.csv`
3. `checksums.txt`
4. `export_meta.json`

## Optional Files
1. `README.txt`
2. `attachments/*` (not interpreted by core importer in `v1`)

## manifest.json Minimum Schema
1. `spec_version` (must equal `v1`)
2. `package_id` (UUID-like unique string)
3. `created_at_utc` (ISO-8601 UTC)
4. `source_lgu_name`
5. `source_system_name`
6. `rows_declared` (integer >= 0)
7. `checksum_algorithm` (must be `sha256`)

## checksums.txt Format
One line per file:
`<sha256_hex><two_spaces><relative_file_path>`

Example:
`2f1c...  beneficiaries.csv`

## beneficiaries.csv Minimum Columns
1. `generated_id`
2. `last_name`
3. `first_name`
4. `middle_name`
5. `extension_name`
6. `sex`
7. `birthdate_iso`
8. `region_code`
9. `province_code`
10. `city_code`
11. `barangay_code`
12. `contact_no`

## Import Validation Requirements
1. Required files must exist exactly once.
2. Manifest `spec_version` must be supported.
3. Declared checksums must match computed checksums.
4. Required CSV headers must match contract.
5. File path traversal entries (e.g., `../`) are forbidden.
6. Validation failure aborts import before any commit.

