# Offline Beneficiary Encoding and Deduplication Tool Plan (Open-Source Only)

## Project Goal

Build a fully offline, open-source Windows desktop beneficiary encoding and deduplication application for LGUs using Go, Fyne, and SQLite.

The system must support:

- beneficiary encoding, editing, filtering, and soft deletion
- deduplication review and decision handling
- import from CSV or open exchange packages
- export of cleaned records to Excel-compatible CSV
- PSGC-based cascading location dropdowns
- persistent audit/history tracking
- automatic SQLite database creation on first run
- responsive GUI during long-running operations
- packaging as a professional Windows executable suitable for municipal deployment

All components, dependencies, and packaging tools must be open-source.

---

## Open-Source Stack

- **Language:** Go
- **GUI:** Fyne
- **Embedded Database:** SQLite
- **SQLite Driver:** `modernc.org/sqlite` (preferred for simpler Windows builds)
- **File Handling:** Go standard library for CSV, ZIP, JSON, hashing, and filesystem operations

### Open-Source Policy

- lock all dependency versions in `go.mod`
- maintain a `THIRD_PARTY_NOTICES.md`
- do not use closed-source UI controls, proprietary database engines, or Office COM automation
- keep import/export formats open and documented

---

## Key Design Improvements

### 1. Replace “import from another .exe”
A desktop executable is not a reliable data source.

Supported import types should be:
- CSV import
- open exchange package import (ZIP containing CSV + manifest JSON + checksums)

### 2. Use soft delete instead of hard delete
For government records, delete should usually mean:
- mark record as `DELETED`
- keep audit trail
- exclude deleted records from export and active views

### 3. Separate visible ID from the true primary key
Keep:
- `internal_uuid` = true primary key
- `generated_id` = user-facing immutable ID

Use a dedicated sequence table to generate reliable running numbers.

### 4. Avoid full O(n²) dedup comparison
Use a candidate blocking stage first, then apply the exact weighted scoring formula only to candidate pairs.

### 5. Store normalized fields
Store normalized values for faster search and duplicate checks:
- `norm_last_name`
- `norm_first_name`
- `norm_middle_name`
- `norm_extension_name`

### 6. Add auditability
Every destructive action and dedup decision must be logged.

### 7. Make LGU profile configurable
Settings should include:
- LGU display name
- LGU code prefix used in visible IDs
- office logo
- default city/municipality profile

---

## Recommended Architecture

```text
cmd/beneficiary-app/main.go
internal/app/
internal/config/
internal/db/
internal/db/migrations/
internal/model/
internal/repository/
internal/service/
internal/dedup/
internal/dedup/metrics/
internal/importer/
internal/exporter/
internal/psgc/
internal/ui/
internal/ui/pages/
internal/ui/components/
internal/ui/dialogs/
internal/jobs/
internal/audit/
assets/
build/
scripts/
```

### Layer Responsibilities

- `ui`: Fyne windows, forms, dialogs, tables, navigation
- `service`: business logic and orchestration
- `repository`: database queries and persistence
- `db`: connection, migrations, schema bootstrap
- `dedup`: normalization, candidate generation, scoring, decisions
- `importer/exporter`: CSV and exchange package logic
- `psgc`: PSGC parsing and cascading lookup services
- `audit`: user action logging
- `jobs`: background worker execution and progress state

---

## First-Run Behavior

On first launch, the app should:

1. create application data directory
2. create SQLite database
3. run migrations/schema creation
4. load app settings defaults
5. load PSGC data from the provided CSV file
6. create required indexes
7. open a first-run setup screen for LGU profile

### First-Run Setup Fields

- LGU name
- LGU code prefix for IDs, example `CEBUCITY`
- default region
- default province
- default city/municipality
- office logo path
- operator name

---

## PSGC CSV Integration Note

You will provide the **PSGC CSV file** that will serve as the source of truth for geographic data.

The application should:

- accept the provided PSGC CSV during setup or deployment
- parse the CSV on first run or during initial setup
- store the PSGC records inside the local SQLite database for faster access and better offline performance
- use the stored PSGC tables as the live source for cascading dropdowns and filtering

### Recommended PSGC Storage Tables

- `psgc_regions`
- `psgc_provinces`
- `psgc_cities`
- `psgc_barangays`

### PSGC Behavior

- Region -> Province
- Province -> City/Municipality
- City/Municipality -> Barangay

Both PSGC codes and display names should be stored in the database.

This approach is preferred because storing PSGC data in SQLite provides:
- faster local lookups
- simpler cascading dropdown queries
- better filtering performance
- no need to re-parse the CSV on every app start

---

## Database Design

### Core Tables

#### beneficiaries
- `internal_uuid` TEXT PRIMARY KEY
- `generated_id` TEXT UNIQUE NOT NULL
- `last_name`
- `first_name`
- `middle_name`
- `extension_name`
- `norm_last_name`
- `norm_first_name`
- `norm_middle_name`
- `norm_extension_name`
- `region_code`
- `region_name`
- `province_code`
- `province_name`
- `city_code`
- `city_name`
- `barangay_code`
- `barangay_name`
- `contact_no`
- `contact_no_norm`
- `birth_month`
- `birth_day`
- `birth_year`
- `birthdate_iso`
- `sex`
- `record_status` (`ACTIVE`, `RETAINED`, `DELETED`)
- `dedup_status` (`CLEAR`, `POSSIBLE_DUPLICATE`, `RESOLVED`)
- `source_type` (`LOCAL`, `IMPORT`)
- `source_reference`
- `created_at`
- `updated_at`
- `deleted_at` nullable

#### id_sequences
- `sequence_key` TEXT PRIMARY KEY
- `last_value` INTEGER NOT NULL

#### dedup_runs
- `run_id` TEXT PRIMARY KEY
- `started_at`
- `completed_at`
- `status`
- `total_candidates`
- `total_matches`
- `notes`

#### dedup_matches
- `match_id` TEXT PRIMARY KEY
- `run_id`
- `record_a_uuid`
- `record_b_uuid`
- `pair_key` UNIQUE
- `first_name_score`
- `middle_name_score`
- `last_name_score`
- `extension_name_score`
- `total_score`
- `birthdate_compare`
- `barangay_compare`
- `decision_status`
- `created_at`

#### dedup_decisions
- `decision_id` TEXT PRIMARY KEY
- `pair_key` UNIQUE
- `record_a_uuid`
- `record_b_uuid`
- `decision`
- `resolved_by`
- `resolved_at`
- `notes`

#### app_settings
- `setting_key`
- `setting_value`

#### import_logs
- `import_id`
- `file_name`
- `file_hash`
- `import_type`
- `rows_read`
- `rows_inserted`
- `rows_skipped`
- `rows_failed`
- `started_at`
- `completed_at`
- `remarks`

#### export_logs
- `export_id`
- `file_name`
- `export_type`
- `rows_exported`
- `created_at`

#### audit_logs
- `audit_id`
- `entity_type`
- `entity_id`
- `action`
- `performed_by`
- `details_json`
- `created_at`

### Important Indexes

Create indexes for:
- `beneficiaries(generated_id)`
- `beneficiaries(norm_last_name, norm_first_name)`
- `beneficiaries(city_code, barangay_code)`
- `beneficiaries(record_status, dedup_status)`
- `beneficiaries(birth_year)`
- `dedup_matches(pair_key)`
- `dedup_matches(decision_status)`

---

## Visible ID Generation

Use:
- stable LGU prefix from settings
- numeric counter from `id_sequences`

Format:
- `CEBUCITY-000001`
- `CEBUCITY-000002`

Rules:
- visible ID is immutable after creation
- sequence increment happens inside a transaction
- imported records may preserve original visible IDs if no collision exists
- if collision exists, assign a new local visible ID and keep the source ID in logs

---

## Encoding Module

### Main Screen
- paginated table
- search box
- city/barangay filters
- “Add Beneficiary” button
- row actions: View, Edit, Delete

### Table Columns
- ID
- Last Name
- First Name
- Middle Name
- Extension Name
- Region
- Province
- City/Municipality
- Barangay
- Birthdate
- Sex
- Contact No
- Record Status
- Actions

### Validation
Required fields:
- `last_name`
- `first_name`
- `region`
- `province`
- `city_municipality`
- `barangay`
- `sex`

Additional validation:
- trim leading/trailing spaces
- collapse repeated internal spaces
- normalize case consistently
- validate real calendar dates
- contact number optional but normalized if provided
- prevent exact duplicate creation unless user confirms

### Duplicate Prevention During Save
Before save, check for existing records with the same normalized:
- last name
- first name
- middle name
- extension name
- birthdate
- sex
- barangay or city

If an exact-match candidate exists, show:
- existing record summary
- “Cancel Save”
- “Save Anyway”

### Delete Behavior
Use soft delete:
- set `record_status = DELETED`
- set `deleted_at`
- log audit event

---

## Deduplication Module

### Required Weighted Formula
```text
total_score =
(first_name_score * 0.49) +
(middle_name_score * 0.05) +
(last_name_score * 0.45) +
(extension_name_score * 0.01)
```

Threshold:
- possible duplicate if `total_score >= 90`

### Required Algorithms
- First name: Jaro-Winkler = 49%
- Middle name: Levenshtein = 5%
- Last name: Levenshtein = 45%
- Extension name: Hamming Distance = 1%

### Normalization Before Matching
- trim spaces
- collapse multiple spaces
- normalize case
- optionally remove punctuation
- treat null and empty consistently

### Score Normalization
All component scores should return values from `0..100`.

Suggested behavior:
- Jaro-Winkler -> normalize to `0..100`
- Levenshtein similarity -> `100 * (1 - distance / maxLen)`
- safe Hamming similarity -> pad shorter string before comparison

### Candidate Blocking
Do not compare every record against every other record directly.

Use blocking keys such as:
- same city + first letter of last name
- same city + first letter of first name
- same barangay + birth year
- same surname prefix + same first-name prefix

### Pair Key
Use a canonical key:
- smaller UUID first
- larger UUID second

Example:
- `uuidA|uuidB`

### Result Table
- Record A ID
- Record A Full Name
- Record B ID
- Record B Full Name
- First Name Similarity
- Middle Name Similarity
- Last Name Similarity
- Extension Name Similarity
- Total Weighted Similarity
- Birthdate Comparison
- Barangay Comparison
- Dedup Status
- Actions

### Dedup Actions
- Retain Record A
- Retain Record B
- Retain Both
- Delete Record A
- Delete Record B
- Mark as Different Persons

### Rules
- no automatic deletion
- all decisions stored in `dedup_decisions`
- resolved pairs do not reappear unless decisions are reset
- allow re-run against the latest database contents

### Background Execution
- run dedup in goroutine/job worker
- keep UI responsive
- show progress overlay
- batch-write results to SQLite

---

## Import Module

### Supported Import Types
- CSV import
- open exchange package import

### Exchange Package Format
```text
exchange_package.zip
 ├── manifest.json
 ├── beneficiaries.csv
 ├── checksums.txt
 └── export_meta.json
```

### Import Rules
- validate headers
- preview row count and errors
- normalize fields
- generate new internal UUIDs locally
- preserve visible IDs only if safe
- insert in transactions
- record inserted/skipped/failed counts
- imported data appears immediately in Encoding and Dedup modules

### Required Protections
- reject malformed headers
- validate required fields
- trim and normalize names
- detect exact duplicate rows before insert
- prevent UUID collisions
- preserve provenance using source metadata

### Import History Screen
Show:
- file name
- import type
- timestamp
- rows inserted
- rows skipped
- rows failed
- operator

---

## Export Module

### Required CSV Export
Export only final records with status:
- `ACTIVE`
- `RETAINED`

Exclude:
- `DELETED`
- unresolved duplicates if policy requires exclusion

### Export Fields
- id
- last_name
- first_name
- middle_name
- extension_name
- region
- province
- city_municipality
- barangay
- contact_no
- month_mm
- day_dd
- year_yyyy
- sex

### CSV Compatibility
- Excel-compatible
- UTF-8 with BOM recommended
- proper quoting
- save dialog
- success/error notification
- export logging

### Optional Export Type
- Exchange Package Export

---

## UI / UX Plan

### Layout
- white background
- top header with system title and LGU profile
- left sidebar
- main content panel
- cards for dashboard metrics
- tables for list pages
- modal dialogs for forms
- confirmation dialogs for destructive actions
- toast/dialog notifications
- progress overlay for import and dedup

### Sidebar
- Dashboard
- Encoding
- Deduplication
- Import
- Export
- Settings
- History
- Backup/Restore

### Dashboard Cards
- Total beneficiaries encoded
- Total active records
- Total possible duplicates
- Total deleted records
- Total retained records
- Last import date
- Last export date

---

## Reliability and Data Safety

### Scope
Document the app as:
- offline
- single-user
- single-workstation
- not intended for simultaneous multi-user editing on a shared network database

### Backups
Built-in backup should:
- create timestamped backup copies
- allow restore
- confirm before overwrite

### Audit Trail
Track:
- beneficiary created
- beneficiary edited
- beneficiary deleted
- import executed
- export executed
- dedup run executed
- dedup decision made
- dedup reset performed

### Error Handling
- never swallow errors
- log technical errors locally
- show user-friendly error messages
- isolate import failures where possible

---

## Settings Module

Recommended settings:
- LGU display name
- LGU code prefix
- office address
- city/municipality profile
- logo file path
- default export folder
- operator name
- duplicate-check sensitivity options
- backup folder
- advanced DB mode settings
- reset dedup decisions button

---

## Build and Deployment

### Build Target
- Windows desktop executable
- offline deployment
- no separate database server
- no external runtime dependencies beyond packaged app files

### Build Steps
1. `go mod tidy`
2. `go build`
3. `fyne package -os windows -icon assets/app.png`

### Distribution Options
- zipped portable distribution
- open-source installer package

### First-Run Database Behavior
Document clearly that:
- the SQLite database file is created automatically on first run
- schema is created automatically
- PSGC data is loaded automatically from the provided PSGC CSV
- settings are initialized automatically

### Recommended Deliverables
- `beneficiary-app.exe`
- application assets
- optional installer
- user guide
- admin guide
- sample import CSV template
- exchange package specification
- third-party license notices

---

## Final Scope Summary

### Encoding
- add
- view
- edit
- soft-delete
- search
- paginate
- filter

### Deduplication
- background-run dedup
- candidate blocking
- exact weighted score
- persistent decisions
- reset decisions

### Import
- CSV import
- exchange package import
- preview
- validation
- logs

### Export
- final CSV export
- optional exchange package export
- logs

### PSGC
- initial load from provided PSGC CSV
- storage into SQLite library tables
- cascading dropdowns
- fast local lookup and filtering

### Operations
- settings
- dashboard
- backup/restore
- audit trail
- import/export history

---

## Final Requirement Statement

Build a fully offline, open-source Windows desktop beneficiary encoding and deduplication application for LGUs using Go, Fyne, and SQLite. The system must support beneficiary encoding, editing, filtering, deduplication review, import from CSV or open exchange packages, export of cleaned records to Excel-compatible CSV, PSGC-based cascading location dropdowns, and persistent audit/history tracking. The application must use SQLite as an embedded local database created automatically on first run, must keep the GUI responsive during long-running operations, and must package as a professional Windows executable suitable for municipal deployment. The PSGC CSV will be provided separately and should be imported and stored in the database for faster local access and better offline performance.
