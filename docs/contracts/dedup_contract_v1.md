# dedup_contract_v1 (FROZEN)

- Status: Frozen for wave handoff
- Version: `v1`
- Last Updated: `2026-03-25`
- Change policy: Backward-compatible additions only; scoring semantics changes require `v2`.

## Purpose
Defines deterministic dedup run inputs, scoring, ordering, and decision persistence expectations.

## Run Request Contract
Minimum request fields:
1. `run_id` (caller-provided or generated, unique per execution)
2. `initiated_by` (operator identity)
3. `threshold` (default `90.0`, allowed range `0..100`)
4. `include_deleted` (default `false`)
5. `resume_from_checkpoint` (optional token)

## Candidate and Match Contract
Each scored pair must include:
1. `record_a_uuid`
2. `record_b_uuid`
3. canonical `pair_key = min(uuidA, uuidB) + "|" + max(uuidA, uuidB)`
4. `first_name_score` (`0..100`)
5. `middle_name_score` (`0..100`)
6. `last_name_score` (`0..100`)
7. `extension_name_score` (`0..100`)
8. `total_score` (`0..100`)
9. `birthdate_compare` (boolean or typed compare state)
10. `barangay_compare` (boolean or typed compare state)

## Required Weighted Formula
`total_score = (first_name_score * 0.49) + (middle_name_score * 0.05) + (last_name_score * 0.45) + (extension_name_score * 0.01)`

## Determinism Rules
1. Same dataset + same config + same threshold => identical candidate set and total ordering.
2. Tie-break order must be stable by:
- descending `total_score`,
- ascending `pair_key`.
3. Resolved pairs stay hidden on rerun unless decision reset is explicitly requested.

## Decision Contract
Allowed decisions:
1. `RETAIN_A`
2. `RETAIN_B`
3. `RETAIN_BOTH`
4. `DELETE_A_SOFT`
5. `DELETE_B_SOFT`
6. `DIFFERENT_PERSONS`

Rules:
1. No automatic hard delete actions.
2. Every decision is auditable with actor and timestamp.
3. Decision replay/reset must be supported without orphaning prior audit lineage.

