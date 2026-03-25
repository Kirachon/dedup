# Plan: Offline Beneficiary Tool Swarm Execution (with PSGC CSV Ingestion)

### Summary
- Build the offline LGU desktop system using Go + Fyne + SQLite, using `lib_geo_map_2025_202603251312.csv` as the PSGC source loaded into local DB tables for fast cascading lookups.
- Use a contract-first sequence so multiple agents can execute in parallel without behavior drift.
- Final plan includes subagent-reviewed dependency fixes for recovery integrity, deterministic dedup, data protection, and release evidence gates.
- Wave 5 follow-through landed: service-layer beneficiary workflow and deterministic dedup engine are now implemented, which unblocks the import, decision, and backup/restore waves.
- Wave 6 follow-through landed: import preview/commit/resume, dedup decision workflow, and backup/restore snapshots are now implemented, which unblocks the export and data-protection waves.

### Scope Lock
- In scope: offline single-workstation app; beneficiary CRUD with soft delete; dedup run/review; CSV + exchange import; CSV export; PSGC DB ingestion; audit/history; backup/restore; Windows packaging.
- Out of scope: multi-user collaboration, cloud APIs, proprietary dependencies, automatic hard-delete dedup.
- File surfaces: `cmd/`, `internal/*`, `build/`, `scripts/`, `assets/`, `docs/contracts/`, `docs/release/`.

### Public Interfaces / Types To Freeze Early
- `DedupEngine.Run(ctx, request) -> runResult` with deterministic ordering and scoring behavior.
- `ImportService.Preview(source) -> previewReport` and `ImportService.Commit(token) -> importResult` with idempotency/replay protection.
- `PSGCIngestor.Load(csvPath) -> ingestReport` with schema validation and fail-closed behavior on drift.
- `JobManager` state machine with `queued/running/cancel_requested/cancelled/succeeded/failed/recoverable`.
- `BackupService.CreateSnapshot()` / `BackupService.Restore(dryRun|apply)` with active-job quiesce requirement.

### Dependency Graph
```text
T1 -> T2 -> T3 -> T4
T3 -> T6 -> T7
T3,T4 -> T5
T2,T3,T4 -> T8
T6,T7,T8 -> T9
T6,T8 -> T10 -> T11
T7,T11 -> T12
T3,T4,T7,T8 -> T13
T6,T7,T12 -> T14
T5,T7,T8,T9,T11,T12,T13,T14 -> T15 -> T16
T5,T9,T10,T11,T12,T13,T15,T16 -> T17
```

### Tasks

### T1: Freeze Contracts
- **depends_on**: `[]`
- **location**: `docs/contracts/`
- **description**: Freeze `data_invariants_v1`, `migration_safety_v1`, `dedup_contract_v1`, `import_contract_v1`, `jobstate_v1`, `exchange_package_v1`.
- **validation**: Contract checklist approved; all tasks reference frozen contracts.
- **status**: Completed
- **log**: Frozen six contract docs to unblock later waves; kept them additive and versioned for v1.
- **files edited/created**: `docs/contracts/data_invariants_v1.md`, `docs/contracts/migration_safety_v1.md`, `docs/contracts/dedup_contract_v1.md`, `docs/contracts/import_contract_v1.md`, `docs/contracts/jobstate_v1.md`, `docs/contracts/exchange_package_v1.md`

### T2: Bootstrap Project and Tooling
- **depends_on**: `[T1]`
- **location**: `cmd/`, `internal/`, `build/`, `scripts/`
- **description**: Scaffold modules, pin dependencies, add OSS notice flow, baseline test/build scripts.
- **validation**: `go test ./...` scaffold pass; dependency pin and notice artifacts present.
- **status**: Completed
- **log**: Created the Go/Fyne/SQLite scaffold, Windows helper scripts, package metadata, and dependency lock.
- **files edited/created**: `go.mod`, `go.sum`, `cmd/beneficiary-app/main.go`, `internal/config/config.go`, `internal/app/bootstrap.go`, `internal/app/run.go`, `build/README.md`, `build/package.ps1`, `scripts/build.ps1`, `scripts/test.ps1`, `scripts/run.ps1`, `scripts/validate.ps1`, `assets/README.md`, `THIRD_PARTY_NOTICES.md`

### T3: Implement Schema and Migrations
- **depends_on**: `[T1, T2]`
- **location**: `internal/db/`, `internal/db/migrations/`
- **description**: Create all core and PSGC tables, indexes, and invariants including soft-delete-safe uniqueness rules.
- **validation**: Migration up/down on temp DB passes; invariant tests pass.
- **status**: Completed
- **log**: Added versioned SQLite schema migrations, core tables, indexes, migration journal, and migration tests.
- **files edited/created**: `internal/db/bootstrap.go`, `internal/db/sqlite.go`, `internal/db/migrator.go`, `internal/db/writer_guard.go`, `internal/db/migrator_test.go`, `internal/db/sqlite_policy_test.go`, `internal/db/writer_guard_test.go`, `internal/db/migrations/0001_core_schema.up.sql`, `internal/db/migrations/0001_core_schema.down.sql`

### T4: Enforce SQLite Runtime Policy
- **depends_on**: `[T3]`
- **location**: `internal/db/`, `internal/jobs/`
- **description**: Apply WAL, busy timeout, and single-writer guard strategy.
- **validation**: Lock-contention and transaction stress tests pass.
- **status**: Completed
- **log**: Enforced WAL, foreign keys, busy timeout, and a deterministic single-writer guard for later long-running jobs.
- **files edited/created**: `internal/db/sqlite.go`, `internal/db/writer_guard.go`, `internal/db/sqlite_policy_test.go`, `internal/db/writer_guard_test.go`

### T5: PSGC Ingestion + Drift Guard
- **depends_on**: `[T3, T4]`
- **location**: `internal/psgc/`, `internal/service/`
- **description**: Load `lib_geo_map_2025_202603251312.csv`, validate required headers and hierarchy integrity, ingest canonical region/province/city/barangay records.
- **validation**: Header/integrity checks pass; row counts reconcile; cascade query tests pass; drifted CSV fails closed with no partial commit.
- **status**: Completed
- **log**: Added checksum-aware PSGC ingest with fail-closed drift handling, bootstrap auto-ingest, and UI status reporting for the local CSV.
- **files edited/created**: `internal/config/config.go`, `internal/app/bootstrap.go`, `internal/app/bootstrap_test.go`, `internal/app/run.go`, `internal/psgc/ingest.go`, `internal/psgc/ingest_test.go`

### T6: Repository and Model Layer
- **depends_on**: `[T3, T4]`
- **location**: `internal/model/`, `internal/repository/`
- **description**: Implement models/repos for beneficiaries, settings, audit, dedup runs/matches/decisions, import/export logs.
- **validation**: Repository CRUD + transaction + rollback tests pass.
- **status**: Completed
- **log**: Normalized the repository/model contract to pointer-aware optional fields, stable enums, PSGC lookups, and round-trip tests.
- **files edited/created**: `internal/model/models.go`, `internal/repository/helpers.go`, `internal/repository/beneficiary.go`, `internal/repository/dedup.go`, `internal/repository/logs.go`, `internal/repository/metadata.go`, `internal/repository/psgc.go`, `internal/repository/repository_test.go`

### T7: Encoding Service Core
- **depends_on**: `[T6]`
- **location**: `internal/service/`
- **description**: Add normalization/validation, immutable visible ID generation, soft delete semantics, exact duplicate precheck prompt contract.
- **validation**: Status transition, collision handling, and duplicate precheck tests pass.
- **status**: Completed
- **log**: Added a dedicated beneficiary service with deterministic normalization/validation, transactional visible-ID allocation, collision provenance tracking, soft-delete stamping, and duplicate precheck prompts.
- **files edited/created**: `internal/service/beneficiary_service.go`, `internal/service/beneficiary_service_test.go`

### T8: Background Job Runtime and Journal
- **depends_on**: `[T2, T3, T4]`
- **location**: `internal/jobs/`
- **description**: Implement durable job state transitions and resume/recover logic for long operations.
- **validation**: Crash/restart recovery scenarios pass with consistent jobstate.
- **status**: Completed
- **log**: Implemented durable job state transitions, queued/running/cancel/recover states, and startup recovery handling.
- **files edited/created**: `internal/jobs/manager.go`, `internal/jobs/manager_test.go`

### T9: Import Pipeline (Preview/Commit/Resume)
- **depends_on**: `[T6, T7, T8]`
- **location**: `internal/importer/`, `internal/service/`
- **description**: Implement CSV and exchange-package preview/commit, idempotency keys, resume checkpoints, provenance logging.
- **validation**: Interrupted import resume test passes; repeated import remains idempotent.
- **status**: Completed
- **log**: Added the importer service with CSV and exchange-package preview/commit/resume support, manifest/checksum validation, PSGC-backed row projection, source-reference provenance, checkpointed partial runs, and idempotent repeat handling.
- **files edited/created**: `internal/importer/importer.go`, `internal/importer/importer_test.go`

### T10: Deterministic Dedup Engine
- **depends_on**: `[T6, T8]`
- **location**: `internal/dedup/`, `internal/service/`
- **description**: Implement candidate blocking plus required weighted formula and algorithms with stable tie-breaks.
- **validation**: Golden determinism tests pass across repeated runs; threshold behavior verified.
- **status**: Completed
- **log**: Added a pure deterministic dedup engine with blocking, weighted scoring, compare-state outputs, and stable ordering across reordered input.
- **files edited/created**: `internal/dedup/engine.go`, `internal/dedup/engine_test.go`

### T11: Dedup Decision Workflow
- **depends_on**: `[T7, T10]`
- **location**: `internal/service/`, `internal/audit/`
- **description**: Implement retain/delete-as-soft-delete/different-person decisions, reversible reset/recompute behavior, and full audit lineage.
- **validation**: Decision replay/reset tests pass.
- **status**: Completed
- **log**: Added a service-only dedup decision workflow with apply/reset paths, soft-delete restore on reset, in-place recompute lineage, and immutable audit-log evidence.
- **files edited/created**: `internal/service/dedup_decision_service.go`, `internal/service/dedup_decision_service_test.go`

### T12: Export Pipeline and Safety
- **depends_on**: `[T7, T11]`
- **location**: `internal/exporter/`
- **description**: Export Excel-compatible CSV (BOM, quoting), enforce record policy filters, and protect against formula injection (`=`, `+`, `-`, `@`).
- **validation**: Row accounting invariants pass; malicious fixture export opens safely.
- **status**: Not Completed
- **log**:
- **files edited/created**:

### T13: Backup/Restore Atomic Workflow
- **depends_on**: `[T3, T4, T7, T8]`
- **location**: `internal/service/`, `internal/ui/dialogs/`
- **description**: Implement snapshot/checksum restore flow with active-job quiesce, dry-run validation, typed confirmation, and audit logs.
- **validation**: Restore drills pass for idle and active-job scenarios; no partial replay corruption.
- **status**: Completed
- **log**: Added a service-only backup/restore workflow with timestamped snapshots, manifest/checksum validation, typed confirmation, active-job blocking, rollback copy handling, and audit evidence.
- **files edited/created**: `internal/service/backup_service.go`, `internal/service/backup_service_test.go`

### T14: Data Protection Controls
- **depends_on**: `[T6, T7, T12]`
- **location**: `internal/service/`, `internal/audit/`, `internal/exporter/`
- **description**: Enforce PII-safe logs, audit redaction policy, export allowlist, and local directory permission checks.
- **validation**: Security checklist and leakage regression tests pass.
- **status**: Not Completed
- **log**:
- **files edited/created**:

### T15: Fyne UI Integration
- **depends_on**: `[T5, T7, T8, T9, T11, T12, T13, T14]`
- **location**: `internal/ui/`
- **description**: Build Dashboard/Encoding/Dedup/Import/Export/Settings/History/Backup screens using thread-safe UI update patterns (`fyne.Do`/binding).
- **validation**: UAT flows pass; race and responsiveness checks pass.
- **status**: Not Completed
- **log**:
- **files edited/created**:

### T16: Windows Packaging and Artifact Integrity
- **depends_on**: `[T15]`
- **location**: `build/`, `scripts/`, `assets/`, `docs/release/`
- **description**: Produce executable/package artifacts with reproducible metadata, hashes, OSS notices, and deployment guides.
- **validation**: Clean-machine offline install/first-run passes; artifact manifest checks pass.
- **status**: Not Completed
- **log**:
- **files edited/created**:

### T17: Final Readiness Gate
- **depends_on**: `[T5, T9, T10, T11, T12, T13, T15, T16]`
- **location**: `docs/release/`
- **description**: Run evidence-based go/no-go gate including rollback ownership and stabilization plan.
- **validation**: No open P0/P1; rollback rehearsal evidence, determinism report, PSGC ingest report, export safety report, and artifact hash manifest all linked.
- **status**: Not Completed
- **log**:
- **files edited/created**:

### Parallel Execution Groups

| Wave | Tasks | Can Start When |
|------|-------|----------------|
| 1 | T1 | Immediately |
| 2 | T2 | T1 complete |
| 3 | T3 | T2 complete |
| 4 | T4, T6 | T3 complete |
| 5 | T5, T7, T8 | Their dependencies complete |
| 6 | T9, T10, T13 | Their dependencies complete |
| 7 | T11, T12, T14 | Their dependencies complete |
| 8 | T15 | T5,T7,T8,T9,T11,T12,T13,T14 complete |
| 9 | T16 | T15 complete |
| 10 | T17 | Final dependency set complete |

### Test Plan and Evidence Gate
- Unit tests: normalization, similarity scoring, blocking keys, ID sequencing, status transitions.
- Integration tests: first-run bootstrap, PSGC ingest from provided CSV, import preview/commit/resume, dedup run/decision/reset, export safety.
- Reliability tests: migration failure restore, backup/restore crash matrix, lock contention, race checks, UI responsiveness on large datasets.
- Release evidence bundle: rollback drill receipt, determinism report, PSGC integrity report, export injection safety report, artifact hash manifest.

### Assumptions and Defaults
- Single-user, single-workstation, fully offline operation.
- PSGC canonical mapping uses the provided dataset and enforces strict schema validation before ingest.
- Dedup never performs automatic hard delete; all destructive outcomes are operator-mediated and auditable.
- Default export excludes `DELETED` records and applies unresolved-duplicate policy defined in frozen contract.
