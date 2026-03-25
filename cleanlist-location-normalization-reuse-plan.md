# Plan: Cleanlist Location Normalization Reuse for Batch Dedup

## Summary
- Port Cleanlist’s hierarchical PSGC location matcher into the Go app as an internal batch-normalization service, then run it before beneficiary creation and dedup.
- Use the Rust repo only as the behavior oracle and fixture source; do not make it a shipped runtime dependency.
- Normalize region, province, city/municipality, and barangay as one atomic chain, then feed resolved PSGC codes/names into importer, duplicate precheck, and dedup blocking.
- Preserve traceability with shadow-mode reporting plus a small normalization lineage ledger so ambiguous or mismatched rows are reviewable instead of silently rewritten.

## Public Interfaces
- `LocationNormalizer.NormalizeChain(ctx, rawChain) -> NormalizedLocation`
- `NormalizedLocation` carries raw inputs, resolved PSGC codes/names, `confidence`, `needs_review`, `match_source`, and `normalization_version`
- `ImportToken` / `PreviewReport` / `ImportLog` include `normalization_version`
- `LocationNormalizationRun` and `LocationNormalizationItem` persist lineage and review status
- `BackfillService.NormalizeExistingBeneficiaries(ctx, filter) -> BackfillReport`

## Scope Lock
- In scope: import-time normalization, existing-row backfill, lineage/audit persistence, dedup/precheck consumption of resolved PSGC fields, smoke/docs updates
- Out of scope: Tauri/UI reuse, Rust runtime sidecar, network geocoding, dedup scoring changes, beneficiary table schema changes
- Data decision: keep `beneficiaries` unchanged in v1; add only normalization lineage tables and import metadata

## Prerequisites
- Access to the current Go app repo and the Cleanlist Rust repo as the behavior oracle
- PSGC source CSV already bundled and available in the Go app
- Windows build/test environment for offline smoke validation
- Existing import, beneficiary service, and dedup test suites available for regression coverage

## Dependency Graph
```text
T1 -> T2 -> T3 -> T4 -> T6 -> T7
          \-> T5 -/
```

## Tasks

### T1: Lock the Oracle Contract
- **depends_on**: []
- **location**: `D:\GitProjects\cleanlist_pansamantalaan\cleanlist_rust\crates\cleanlist_core\src\{hierarchical_index.rs,location_matcher.rs,pipeline.rs,fuzzy_matcher.rs}`
- **description**: Extract and freeze the exact Cleanlist behavior for PSGC matching: exact-match fast path, fuzzy fallback, confidence thresholding, tie handling, punctuation preservation, and atomic full-chain resolution. Produce a parity matrix and golden fixture set before any Go implementation starts.
- **validation**: Parity matrix approved; fixture set covers exact, fuzzy, ambiguous, parent-mismatch, and punctuation-sensitive cases.
- **status**: Not Completed
- **log**: -
- **files edited/created**: -

### T2: Add a Pure-Go Location Normalizer
- **depends_on**: [T1]
- **location**: `internal/locationnorm/`, `internal/importer/importer.go`
- **description**: Implement a reusable `LocationNormalizer` package that resolves a raw region/province/city/barangay chain against PSGC and returns resolved codes/names, confidence, raw inputs, and review flags. Use exact PSGC code/name matching first, then hierarchical fuzzy matching if needed.
- **validation**: Unit tests pass for exact, fuzzy, ambiguous, and chain-mismatch inputs; no beneficiary schema changes are required to compile.
- **status**: Completed
- **log**: Added pure-Go `LocationNormalizer` + reusable PSGC `Catalog` with exact-first matching, Jaro-Winkler fuzzy fallback (`0.95` cutoff), tie suppression, parent-child validation, and atomic full-chain apply decisions. Added deterministic model contracts and unit tests for exact, fuzzy, ambiguous tie, punctuation-preserving, and mismatch/review behavior.
- **files edited/created**: `internal/locationnorm/normalizer.go`, `internal/locationnorm/normalizer_test.go`, `internal/model/location_normalization.go`

### T3: Add Lineage Persistence and Shadow Mode
- **depends_on**: [T2]
- **location**: `internal/db/migrations/`, `internal/repository/`, `internal/service/`
- **description**: Add a small normalization ledger with `location_normalization_runs` and `location_normalization_items`, and write shadow-mode reporting so the app can compare current exact-match behavior against the new normalizer before the write path is enabled. Persist row number, source reference, raw chain, resolved chain, confidence, match source, status, and `normalization_version`.
- **validation**: Ledger writes and reads work; shadow-mode reports show diffs without mutating beneficiaries; normalization version survives preview/checkpoint flows.
- **status**: Completed
- **log**: Added `0002` normalization ledger migration, repository CRUD/list support for runs/items, and shadow-mode comparison reporting that flags drift without mutating beneficiary rows. Added repository round-trip tests and migration assertions; normalization version persists in run/item rows and shadow reports.
- **files edited/created**: `internal/db/migrations/0002_location_normalization.up.sql`, `internal/db/migrations/0002_location_normalization.down.sql`, `internal/db/migrator_test.go`, `internal/repository/location_normalization.go`, `internal/repository/repository.go`, `internal/repository/repository_test.go`, `internal/locationnorm/shadow.go`, `internal/locationnorm/shadow_test.go`

### T4: Wire Normalization into Import Preview and Commit
- **depends_on**: [T2, T3]
- **location**: `internal/importer/importer.go`, `internal/importer/importer_test.go`
- **description**: Replace the importer’s current location resolution path with the normalizer. Normalize rows before `BeneficiaryDraft` creation, keep a fast exact-match path, quarantine ambiguous or low-confidence chains into review reporting, and add normalization version/hash to preview/checkpoint metadata so resume stays deterministic.
- **validation**: Dirty CSV preview/commit/resume tests pass; ambiguous rows are flagged instead of silently auto-fixed; resume behavior is stable across reruns.
- **status**: Completed
- **log**: Wired importer preview/commit/resume to `locationnorm` with normalization version/hash checkpoint metadata, review-required quarantine for ambiguous chains, and lineage persistence during import runs. Kept legacy package compatibility intact and updated importer tests for metadata continuity, quarantine behavior, and resume determinism.
- **files edited/created**: `internal/importer/importer.go`, `internal/importer/importer_test.go`

### T5: Add Backfill for Existing Beneficiaries
- **depends_on**: [T2, T3]
- **location**: `internal/service/`, `internal/repository/`, `internal/workflowsmoke/`
- **description**: Add a maintenance/backfill pass that reprocesses already-persisted beneficiaries with vague or wrong location fields, records lineage, and only applies changes when the full chain resolves uniquely and parent-child consistency passes. Keep partial single-field fixes disallowed.
- **validation**: Dry-run and apply tests prove existing rows can be normalized safely; partial chain updates are rejected; backfill emits lineage records.
- **status**: Completed
- **log**: Added `LocationBackfillService` with dry-run/apply modes, PSGC catalog loading, transactional lineage persistence, and atomic full-chain updates only. Added service tests and smoke coverage proving dry-run safety, partial-chain rejection, canonical chain restoration, and lineage emission.
- **files edited/created**: `internal/service/location_backfill_service.go`, `internal/service/location_backfill_service_test.go`, `internal/workflowsmoke/location_backfill_smoke_test.go`

### T6: Keep Dedup and Duplicate Precheck on Normalized PSGC Fields
- **depends_on**: [T4, T5]
- **location**: `internal/service/beneficiary_service.go`, `internal/dedup/engine.go`
- **description**: Confirm that duplicate precheck and dedup consume only resolved PSGC codes/names. Do not change dedup scoring in v1. If any consumer still reads raw location text, redirect it to normalized fields and recompute affected dedup results after normalization commits.
- **validation**: Existing dedup/precheck tests remain stable for clean inputs; imported dirty-location rows resolve to the same candidate grouping once normalized.
- **status**: Completed
- **log**: Verified that duplicate precheck and dedup already consume canonical PSGC fields, then added regression tests proving raw location-label noise does not affect duplicate precheck or dedup blocking/matching. No production scoring changes were needed.
- **files edited/created**: `internal/service/beneficiary_service_test.go`, `internal/dedup/engine_test.go`

### T7: Tests, Smoke, Docs, and Final Readiness Gate
- **depends_on**: [T6]
- **location**: `internal/workflowsmoke/`, `README.md`, `build/README.md`
- **description**: Add parity tests, atomic-chain tests, import/backfill smoke coverage, and updated docs that explain the import-time normalization flow. Finish with an offline Windows smoke run using a messy-location CSV and verify the Rust repo is behavior oracle only.
- **validation**: Full Go test suite, workflow smoke suite, and offline build/validate checks pass; docs match the shipped behavior.
- **status**: Completed
- **log**: Updated the beginner/setup and build docs for PSGC-backed import normalization, messy-location handling, backfill dry-run/apply flow, and the Rust oracle-only role. Extended workflow smoke coverage to import messy location names and assert canonical PSGC fields before downstream dedup/backfill/export checks continue.
- **files edited/created**: `README.md`, `build/README.md`, `internal/workflowsmoke/smoke_test.go`

## Test Plan
- Golden parity tests copied from the Rust oracle for exact, fuzzy, ambiguous, and punctuation-sensitive PSGC values
- Atomic chain tests proving no partial parent-child updates and no single-field drift
- Import preview/commit/resume tests proving dirty location rows normalize correctly, ambiguous rows are quarantined/reported, and resume remains deterministic
- Backfill tests proving existing beneficiaries can be normalized safely and lineage is recorded
- Dedup regression tests proving resolved location fields do not change clean-input behavior and improve grouping for messy inputs
- End-to-end smoke test with messy locations through import, duplicate precheck, dedup, backfill, and export

## Final Readiness Gate
- The Rust repo is used only as the parity oracle and fixture source
- Shadow-mode diffs are acceptable and show no partial chain drift
- Dirty-location import and backfill smoke tests pass on Windows offline
- Dedup/precheck still behaves correctly for clean inputs after normalization is enabled
- Offline build, test, and validate checks pass before rollout

## Assumptions
- The Go beneficiary model already has enough PSGC code/name fields and split birthdate fields, so no beneficiary table migration is needed in v1
- The only new persistence in v1 is the normalization lineage ledger plus preview/checkpoint metadata
- Auto-fix applies only when the full chain resolves to one PSGC path and parent-child consistency passes; otherwise the row goes to review or is skipped
- Normalization must remain atomic across region -> province -> city -> barangay; no partial field rewriting
- The Rust repo is the behavior oracle only, not a shipped runtime dependency
