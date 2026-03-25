# data_invariants_v1 (FROZEN)

- Status: Frozen for wave handoff
- Version: `v1`
- Last Updated: `2026-03-25`
- Change policy: Backward-compatible clarifications only; behavioral changes require `v2`.

## Scope
Defines non-negotiable data rules for beneficiary records, identifiers, statuses, timestamps, and normalized fields.

## Core Invariants
1. `internal_uuid` is the immutable technical primary key for every beneficiary row.
2. `generated_id` is the immutable operator-facing ID and must be unique across non-deleted and deleted rows.
3. `record_status` enum is strictly: `ACTIVE | RETAINED | DELETED`.
4. `dedup_status` enum is strictly: `CLEAR | POSSIBLE_DUPLICATE | RESOLVED`.
5. Soft delete only:
- delete operations set `record_status = DELETED` and set `deleted_at`.
- rows are not physically removed by normal workflows.
6. Normalized name fields (`norm_last_name`, `norm_first_name`, `norm_middle_name`, `norm_extension_name`) must be produced deterministically from source fields.
7. Geographic linkage fields (`region_code`, `province_code`, `city_code`, `barangay_code`) must be code-safe strings; empty code values are invalid once a record is persisted as valid.
8. `created_at` is write-once; `updated_at` is updated on mutation; all timestamps stored in UTC ISO-8601.
9. Contact numbers are optional, but if present a normalized companion value must be maintained.
10. All imported rows must preserve provenance (`source_type`, `source_reference`) even after normalization.

## Normalization Contract
1. Trim leading and trailing spaces.
2. Collapse repeated internal whitespace to a single space.
3. Convert to a deterministic case profile for matching (implementation detail may vary by layer, output must be stable).
4. Treat null and empty string equivalently in matching paths.

## ID Safety Contract
1. `generated_id` is never edited after create.
2. Sequence allocation must be transactional.
3. If an imported `generated_id` collides, the system assigns a new local ID and logs source value as provenance.

## Export Visibility Contract
1. `DELETED` rows are excluded from default export.
2. Unresolved duplicates are policy-gated and not assumed exportable by default.

