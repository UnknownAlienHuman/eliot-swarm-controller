# Persistent schedule registry

Schedules are a controller-owned registry of typed Operation admissions. The
first slice supports only `check_run`; configuration cannot supply an
arbitrary method name, model prompt, shell command, or cron expression. A
scheduled check uses the existing immutable Attempt, source snapshot, exact
Task revision, and check-profile revision.

## Configuration

Add entries under `[[schedules]]` in the controller TOML:

```toml
[[schedules]]
schedule_id = "nightly-candidate-check"
enabled = true
anchor_ms = 1790899200000
period_ms = 86400000

[schedules.action]
kind = "check_run"
attempt_id = "01930000-0000-7000-8000-000000000001"
expected_task_revision = 4
candidate_ref = "01930000-0000-7000-8000-000000000002"
profile_id = "strict"
profile_revision = "v1"
```

`anchor_ms` is a Unix epoch millisecond value. Omitting `period_ms` makes the
entry one-shot at its anchor. An interval must be positive and remains anchored
to that wall-clock instant; the controller does not infer a period from the
start time. Schedule IDs are unique lowercase identifiers. The registry is
bounded to 64 retained identities. Once an ID has been used, changing its
anchor, period, or action is an identity error; define a new ID for new
semantics. `enabled` may be toggled to pause and resume the same schedule.

The configured Attempt, Task revision, candidate artifact, and check profile
revision are pinned. Missing or stale inputs are reported as a schedule
failure; they are never updated automatically. A new Task/Attempt or check
profile requires an explicit configuration change under a new schedule ID.

## Slot identity and catch-up

For an interval, the due slot is
`floor((wall_now_ms - anchor_ms) / period_ms)` when `wall_now_ms >= anchor_ms`.
The one-shot has slot `0`. Each `(schedule_id, slot)` maps to one stable
SHA-256 `client_request_id` under the dedicated internal scheduler principal.
The ordinary Store request-receipt path therefore returns the original receipt
for a repeated slot.

The scheduler considers only the latest due slot. It never replays each missed
interval after sleep or restart. It uses wall-clock time to select slots and a
monotonic timer only to wait. A bounded clock recheck lets it observe forward
wall-clock jumps; every catch-up pass reads Store state again before admission.

Admission and the schedule cursor share one immediate Store transaction. The
transaction checks `new_work`, confirms the Attempt remains unreleased and the
Task is still open at the configured revision, validates the candidate and
profile, applies the typed `check.run`, and records the resulting Operation
receipt and schedule state. Rejected Operations retain their normal request
receipt. Structural target/configuration failures are fingerprinted; the same
inputs do not create repeated Operations on later ticks. A changed input
fingerprint permits a fresh relevance check.

When `new_work` is disabled or an entry is disabled, due state may be observed
but no Operation is admitted. Resume computes the current latest due slot, so
there is at most one catch-up admission. An already queued, running, or
outcome-unknown scheduled CheckRun blocks a newer slot until normal Store
reconciliation establishes its disposition. Unknown effects are never replayed
by the schedule loop.

## Persistence and status

Schedule cursors and definition digests live in one versioned
`meta.schedule_registry:v1` value, bounded to 64 schedule identities. The
registry does not add a table or database. Definition digests cover the stable
schedule ID, anchor, period, and typed action; the enable switch is excluded.
Retained IDs cannot be repurposed for different actions. Existing Operation
rows remain the durable receipts and outcomes.

`host.status.schedules.items` reports each configured schedule's ID, enabled
state, action kind, anchor and period, next due time, last considered and
observed slots, last receipt/work Operation and state, last outcome, and last
failure. Reading status is read-only and never advances a cursor or admits
work.

The scheduler is an in-process host loop under a dedicated internal principal.
It is not a GM, observer, runtime module, second broker, or model timer. The
normal CheckRun supervisor retains responsibility for worker launch,
readback, and completion.
