# jobstate_v1 (FROZEN)

- Status: Frozen for wave handoff
- Version: `v1`
- Last Updated: `2026-03-25`
- Change policy: New states require version bump.

## Purpose
Defines durable state machine for long-running jobs (dedup, import, export, maintenance).

## States
1. `queued`
2. `running`
3. `cancel_requested`
4. `cancelled`
5. `succeeded`
6. `failed`
7. `recoverable`

## Allowed Transitions
1. `queued -> running`
2. `running -> succeeded`
3. `running -> failed`
4. `running -> cancel_requested`
5. `cancel_requested -> cancelled`
6. `running -> recoverable`
7. `recoverable -> running`
8. `recoverable -> failed`
9. `queued -> cancelled` (pre-execution cancellation only)

All other transitions are invalid and must be rejected.

## Persistence Contract
Each state update records:
1. `job_id`
2. `state`
3. `updated_at_utc`
4. `attempt`
5. `progress_percent` (`0..100`, nullable for unknown)
6. `message` (operator-safe text)
7. `error_code` (nullable)

## Recovery Rules
1. Startup recovery scans for jobs in `running` and `cancel_requested`.
2. Incomplete jobs transition to `recoverable` unless terminal proof exists.
3. Resume path requires explicit operator or scheduler action.

