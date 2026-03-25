# migration_safety_v1 (FROZEN)

- Status: Frozen for wave handoff
- Version: `v1`
- Last Updated: `2026-03-25`
- Change policy: Additive clarifications only; migration behavior changes require `v2`.

## Objectives
Protect local LGU data during schema evolution and prevent partial/corrupt upgrades.

## Safety Rules
1. Migrations are ordered, deterministic, and identified by unique monotonically increasing version IDs.
2. Each migration must record apply status in a migration journal table (name can vary, semantics cannot).
3. Schema changes run inside a transaction when SQLite allows transactional DDL for that operation.
4. Non-transactional statements (if unavoidable) must be isolated and journaled explicitly.
5. Migration run is fail-closed: on error, stop immediately and return non-zero status.
6. Startup must not proceed to business services if required migrations are not fully applied.
7. Destructive schema operations are prohibited in `v1` baseline migrations unless explicitly approved by release gate.
8. Existing beneficiary rows must remain recoverable after failed upgrade attempts.

## Backup and Restore Safety
1. Before applying a migration batch, create or confirm a recent local snapshot of the SQLite file.
2. On migration failure, restore procedure must be documented and executable offline.
3. Recovery commands must not require network resources.

## Integrity Verification
1. Post-migration check must verify:
- expected table existence,
- expected critical columns existence,
- expected critical indexes existence.
2. If verification fails, startup exits with clear operator-safe message.

## Compatibility Promise
1. `v1` migration contract promises forward-only additive evolution for upcoming waves.
2. Breaking changes require explicit contract version bump and data conversion plan.

