# R44. ScriptRun start authorization: one immutable decision, no `go.json`/`deny.json` race

**Status:** implementation handoff. Production script registry, workers and retained runs are unchanged on this branch.

**Evidence baseline:** `40591a295af94b1541ec2ba30afe8e3247701a71` (`main`, 2026-10-08).

## 1. Result

A ScriptRun worker starts only after observing one immutable, identity-bound start decision:

```text
Pending
  ├─ Allow
  └─ Deny { error_code }
```

Exactly one decision can be published for one `(run_id, operation_id, token)`. A late competing decision is a conflict and cannot override the first. The worker validates that single record before spawning the script.

This slice does not rewrite the script registry, effect system or finite execution path. R35/#61 owns execution/capture/cleanup after start.

## 2. Audit correction: `script.revise` authority is not the current P0

The earlier allegation “`script.revise` lacks `gm::require_authority`, so an Operator can revise another owner's script” is misleading for the current source:

- `require_script_authority` refreshes the principal and allows only Manager or local Operator;
- `gm::require_authority` explicitly treats the local Operator as GM authority;
- `revise_script` rejects a Manager whose client ID differs from the registered owner;
- `require_script_scope` permits cross-owner access only to current GM/local Operator.

Therefore the direct revision-authority bypass is **refuted** on this baseline. Do not add a second script IAM helper merely to call a function whose Operator result is identical.

A separate product decision could narrow local Operator powers, but that is not a bug fix inferred from this audit.

## 3. Confirmed start-gate race

Current host-side functions:

```text
allow(work):
    if deny.json exists → conflict
    write_once(go.json)

deny(work):
    write_once(deny.json)
```

The sequence is not atomic:

```text
allow checks deny.json: absent
→ deny writes deny.json
→ allow writes go.json
→ both files exist
```

Current worker loop checks `go.json` before `deny.json`. In the race above it validates `go.json`, exits the loop and executes the script despite the later/competing denial.

`deny_unstarted` has the same check-then-write shape: it checks `go.json`, writes `deny.json`, then checks `go.json` again. The second check can report the race, but it cannot retract an already observed allow decision or prevent a worker that checks `go.json` first from starting.

This is an authority race, not merely a filesystem cleanup issue.

## 4. One decision record

Replace the two decision files with one closed record, for example:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case", deny_unknown_fields)]
enum ScriptStartDecision {
    Allow {
        schema_version: u16,
        run_id: String,
        operation_id: String,
        token: String,
        decided_at_ms: i64,
    },
    Deny {
        schema_version: u16,
        run_id: String,
        operation_id: String,
        token: String,
        error_code: String,
        decided_at_ms: i64,
    },
}
```

The exact storage name may be `start-decision.json`. There is one file, one schema and one reader.

Validation:

- schema version exact;
- run/Operation/token exact and nonempty;
- decision time nonnegative;
- deny code uses the existing bounded safe error-code grammar;
- no additional JSON fields;
- file is private regular non-symlink/reparse;
- content size bounded.

Do not store arbitrary messages or credentials in the decision.

## 5. Atomic publication

Use a create-new publication owned by the script start-decision component:

```text
construct exact canonical bytes
→ create_new(start-decision.json)
→ private permissions
→ write_all + file sync
→ parent directory sync where supported
→ exact readback
```

If the file already exists:

- exact same identity and exact same decision → idempotent success;
- exact identity but different decision → `SCRIPT_START_DECISION_CONFLICT`;
- malformed/different identity → damaged/conflict, never overwrite.

Do not implement “last writer wins”, rename-over-existing or deletion/recreation. The first durable decision is final.

Consume the durable file primitive from R41/#65 when available; do not create a third private-file helper in `scripts/runner.rs`.

## 6. Store/host wiring

### Allow path

`runner::allow` becomes `publish_start_decision(Allow)` and returns the retained decision/readback. The Store/host caller must persist/admit its own transition before publishing Allow as it does today; publication is still outside SQLite.

After publication, re-read the exact decision. If another Deny already won, return conflict and do not claim the worker was allowed.

### Deny paths

`deny`, `deny_unstarted` and any pre-start reconciliation publish the same `Deny` record. They do not write a sibling file.

A denial attempted after Allow returns a typed conflict/“already allowed” result. The Store then classifies the run by exact retained decision and process evidence; it does not pretend the denial took effect.

### Worker path

The worker waits for `start-decision.json` only:

```text
missing → wait until bounded gate deadline
Allow exact → continue to spawn
Deny exact → finish_without_spawn with retained code
malformed/mismatch → finish_without_spawn SCRIPT_START_DECISION_INVALID
```

No ordering between two filenames remains.

The standalone `swarm-script-worker` and legacy runner consume the same reader during migration. R35 later removes the legacy executable path; R44 must not leave two start-decision formats for new runs.

## 7. Crash and unknown boundaries

- crash before decision publication → no decision; worker waits until bounded gate timeout and publishes pre-start failure;
- crash after file sync but before caller receives success → exact retry reads the same decision;
- SQLite admission rollback before Allow publication → Allow must not be attempted;
- Allow published, caller response lost → readback discovers Allow; never publish Deny as compensation;
- decision file present but Store state uncertain → reconcile exact Store Operation/run plus exact decision, no script replay;
- worker starts only after validated Allow; direct existence of a file name is not enough.

## 8. Existing script authority to keep

Preserve current separations:

- registered Manager owns their script catalog;
- current GM/local Operator has cross-owner administrative authority;
- active revision and Task/Attempt scope are revalidated before direct/automatic run;
- automatic ScriptRun retains exact automation owner/source cause;
- controller effects remain closed and independently authorized;
- worker receives no Store handle or Manager credential.

Do not mix these policies into the file CAS.

## 9. Migration and deletion

### Existing in-flight runs

Before switching writers, define a one-time reader policy:

- an old run with only valid `go.json` → interpret as historical Allow;
- only valid `deny.json` → historical Deny;
- both files → `SCRIPT_START_DECISION_AMBIGUOUS`, never execute;
- neither → pending.

New runs write only `start-decision.json`. Do not maintain dual writers.

After all pre-migration runs are terminal and no retained live run references the old format, delete:

- `go.json` writer and normal reader;
- `deny.json` writer and normal reader;
- check-before-write race logic;
- second post-deny existence check;
- tests that encode filename priority instead of decision identity.

Historical read compatibility may remain in a narrow versioned decoder until the migration condition is proven.

## 10. Donor use

No external dependency is needed.

Useful mechanisms:

- Bazel Action/Command identity: exact immutable input identity before execution;
- Petri: outstanding command/event is one state transition, not two competing mutable flags;
- ELIOT Operations: caller-owned intent and exact readback.

Do not import another workflow engine or file-lock protocol.

## 11. Exact fixtures

- `script_allow_and_deny_race_has_one_winner`
- `script_worker_never_prefers_allow_over_retained_deny`
- `script_worker_never_prefers_deny_over_retained_allow`
- `script_exact_allow_retry_is_idempotent`
- `script_changed_decision_retry_conflicts`
- `script_decision_identity_mismatch_never_starts`
- `script_crash_after_decision_sync_recovers_same_decision`
- `script_no_decision_hits_bounded_prestart_timeout`
- `script_legacy_both_go_and_deny_is_ambiguous_not_executed`
- `script_store_rollback_creates_no_allow_decision`
- `script_allow_lost_reply_does_not_replay_script`

Race fixtures must use real concurrent create-new attempts against one temporary run directory, not a sequential mock.

## 12. Ownership and order

- R35/#61 owns process execution, output capture, timeout, cleanup-pending and removal of duplicate legacy executor.
- R41/#65 owns durable private-file primitives.
- R44 owns only pre-execution start authority and migration from `go.json`/`deny.json`.
- automation/script registry domain owners retain their existing authorization rules.

Recommended order:

```text
R41 durable create/readback
→ R44 start decision CAS in both current callers
→ R35 standalone executor migration and legacy deletion
```

One manager owns `scripts/runner.rs` and shared script protocol. Other PRs rebase instead of adding a local start gate.

## 13. Gate after connected code

```sh
cargo fmt --all -- --check
cargo clippy --locked -p swarm-process -p swarm-script-worker -p swarm-kernel-host --lib --bins -- -D warnings
```

Then the exact race/public Store-to-worker fixtures above. Broad native/script qualification remains the final project phase.
