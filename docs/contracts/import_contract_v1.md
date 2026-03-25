# import_contract_v1 (FROZEN)

- Status: Frozen for wave handoff
- Version: `v1`
- Last Updated: `2026-03-25`
- Change policy: Additive fields only; workflow semantics changes require `v2`.

## Purpose
Defines preview/commit import behavior for CSV and exchange package sources with idempotency and resumability.

## Source Types
Allowed `source_type` values:
1. `CSV`
2. `EXCHANGE_PACKAGE`

## API Surface (Service-Level Contract)
1. `Preview(source) -> preview_report`
2. `Commit(token, idempotency_key) -> import_result`
3. `Resume(checkpoint_token) -> import_result`

## Preview Contract
`preview_report` minimum fields:
1. `preview_token`
2. `source_type`
3. `row_count_total`
4. `row_count_valid`
5. `row_count_invalid`
6. `header_validation_passed` (boolean)
7. `sample_errors[]` (bounded list)
8. `generated_at_utc`

## Commit Contract
`Commit` requirements:
1. Requires valid `preview_token`.
2. Requires caller-provided `idempotency_key`.
3. Must be idempotent for same `(source_hash, idempotency_key)`.
4. Must preserve source provenance metadata per inserted row.
5. Must report inserted/skipped/failed counts with deterministic accounting.

`import_result` minimum fields:
1. `import_id`
2. `status` (`SUCCEEDED | FAILED | PARTIAL | RESUMED`)
3. `rows_read`
4. `rows_inserted`
5. `rows_skipped`
6. `rows_failed`
7. `checkpoint_token` (optional)
8. `completed_at_utc` (nullable when resumable)

## Validation Rules
1. Required headers must exactly match contracted schema for that source type.
2. Required business fields must be present and non-empty after normalization.
3. UUID collisions are disallowed.
4. Exact duplicate row detection must be explicit and counted as skipped/failed by policy.
5. Import must fail-closed on malformed payloads or checksum mismatch.

