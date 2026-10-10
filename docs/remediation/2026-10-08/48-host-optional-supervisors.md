# R48. Optional host supervisors: per-scope isolation, bounded lifecycle and exact receipt closure

**Status:** implementation handoff. Production bus/scheduler supervisors and Store receipts are unchanged on this branch.

**Evidence baseline:** `40591a295af94b1541ec2ba30afe8e3247701a71` (`main`, 2026-10-08).

## 1. Result

A damaged optional scope cannot stop supervision for unrelated scopes; every admitted optional-worker launch reaches exactly one durable closure; shutdown/start waits are bounded and never drop the last live owner handle.

```text
bounded demand snapshot
→ isolate/construct one exact scope slot
→ durable launch receipt before process effect
→ exact child/family identity
→ readiness/owner receipt before work
→ running/readback
→ bounded stop and exact departure
→ close the same launch receipt once
```

No new supervisor framework, scheduler or process registry is added. Reuse R01/R35 process-family and finite-completion seams.

## 2. Confirmed HIGH failures

### 2.1 One bad bus scope exits the whole coordinator

`host_bus_supervisor::run` builds missing slots with:

```rust
slots.entry(scope).or_insert(Slot::new(...)?);
```

`Slot::new` is scope-local filesystem/state construction, but `?` escapes the top-level supervisor loop. One malformed/unavailable scope path therefore terminates supervision for every healthy bus scope.

The neighboring `reconcile_slot` path already distinguishes Store-wide failures from local scope failures. Slot construction must use the same containment rule.

### 2.2 Missing bus owner receipt never reaches a recovery decision

After a verified child starts, the bus slot waits for its owner receipt. At ten seconds it records:

```text
BUS_SERVICE_OWNER_RECEIPT_MISSING
state = unknown
```

but keeps the child and repeats the same state forever. There is no bounded start deadline, stop escalation, exact family-departure reconciliation or safe replacement condition.

The result is a permanently wedged scope until host restart/manual intervention.

### 2.3 Scheduler launch receipt abandoned on identity branches

`begin_automation_scheduler_worker` durably admits `(scope, launch_id)` before spawn.

Spawn failure calls `abandon_automation_scheduler_worker`, and missing stdout calls `abandon_exited_automation_scheduler_worker`. But two branches omit all receipt closure:

- `child.id() == None`;
- spawned identity / owner-group identity failure.

Both kill/wait best-effort and return while the Store receipt remains held. Later scheduler runs are blocked by a launch that has no terminal fact.

### 2.4 Optional-worker stop waits forever

`stop_worker` and `stop_all` signal the worker and then execute unbounded `task.await`. A worker task blocked in child/process cleanup can hang host shutdown forever. The host has no separate proof that work stopped, remains live or became unknown.

## 3. Scope-local failure containment

Replace direct `Slot::new(...)?` insertion with an explicit constructor outcome:

```rust
enum SlotConstruction {
    Ready(Slot),
    Isolated { scope: BusServiceScope, code: SafeErrorCode },
}
```

A smaller `Result<Slot>` plus local match is sufficient; do not expose this enum publicly if it adds no clarity.

Rules:

- Store/read/commit failure that invalidates the global snapshot remains a top-level hard error/retry path;
- unsafe/corrupt/unavailable state for one scope records a bounded scoped diagnostic and leaves other slots running;
- a damaged scope is retried only after the existing bounded backoff or changed demand/state evidence;
- no second child is started while a prior child or owner receipt may still exist;
- slot construction failure does not delete foreign/unknown state.

The per-pass result must report `progressed | idle | degraded` truthfully to the host diagnostic layer; degraded is not success and is not whole-coordinator death.

## 4. Bus startup deadline and owner evidence

Separate three facts:

```text
child spawned and image verified
owner receipt published and validated
owned process family departed
```

Add a trusted startup deadline from exact child spawn/identity capture, not from coordinator process start.

Before deadline:

- retain child handle and identity;
- project `starting`;
- accept exact owner receipt when it appears.

At deadline with no owner receipt:

1. retain a typed `owner_receipt_missing` start failure;
2. request graceful stop through the existing owned process channel if available;
3. wait through R35's bounded family-departure seam;
4. escalate to exact family kill only after the grace deadline;
5. prove departure before clearing child/slot ownership;
6. if departure cannot be proven, retain `owner_identity_or_departure_unknown`, keep replacement blocked and surface attention;
7. never infer “no effect” solely from a missing receipt unless the reviewed helper protocol proves receipt publication precedes every possible work effect.

Do not loop forever in `unknown`; do not replace a possibly-live dispatcher.

## 5. One launch-receipt guard for the scheduler worker

After `begin_automation_scheduler_worker`, represent the still-open launch explicitly:

```rust
struct PendingSchedulerLaunch {
    scope: SchedulerScope,
    launch_id: String,
    state: Pending | RunningIdentityKnown | Closed,
}
```

This is a local control object, not a second Store record. It may expose explicit async closure methods; do not rely on `Drop` for async Store writes.

Every return path after admission must choose exactly one closure:

```text
spawn not attempted / spawn failed
  → abandon before effect

child existed, exact identity known, then exited before ready
  → abandon_exited with exact identity/departure

ready and running
  → finish/activate existing receipt

identity or departure uncertain
  → retain outcome_unknown/identity_unknown against same launch_id
```

For `child.id() == None`, do not merely kill/wait and return. Close the exact receipt according to proven process facts.

For identity-capture or group-identity failure:

- keep the child handle;
- use bounded stop/family departure;
- retain unknown rather than dropping the receipt if identity/departure cannot be proved;
- never start a replacement under a new receipt while the old process may live.

Add a debug/assertion or exhaustive state check that a successful function return cannot leave `PendingSchedulerLaunch` open.

## 6. Bounded optional-worker stop

R48 consumes R35's finite process/task completion seam; it does not write another timeout loop.

For each optional worker task record:

```text
stop requested
→ task completion before grace deadline
→ otherwise explicit cancel/kill request to owned child path
→ bounded family departure/readback
→ dormant | cleanup_pending | observation_unknown
```

`JoinHandle::abort` alone is not process departure proof. A task owning a child must return an exact cleanup state before its handle is discarded.

Host shutdown may finish with a truthful aggregate error/attention if an optional worker remains cleanup-pending; it must not block forever or report dormant.

Preserve the first Store/kernel failure if diagnostic/status persistence also fails.

## 7. Shared primitives and non-import boundaries

Reuse:

- R01/#27 exact process identity and long-lived owner stop-and-wait;
- R35/#61 bounded direct-exit/family-departure/capture completion;
- existing Store scheduler launch receipts;
- existing bus slot owner readback and scoped status records;
- existing restart/backoff budget.

Do not import an actor runtime, create a generic supervisor DSL, move optional workers into a second process registry or add another database.

The bus and scheduler remain separate domain owners; only the narrow lifecycle/result vocabulary is shared after two connected callers exist.

## 8. Simplification and deletion

After migration, delete:

- top-level `?` from scope-local `Slot::new` insertion;
- infinite `BUS_SERVICE_OWNER_RECEIPT_MISSING` status-only loop;
- scheduler branches that return with an admitted receipt and no closure;
- unbounded `task.await` in `stop_worker`/`stop_all`;
- duplicate local kill/wait loops replaced by R35;
- success/dormant projections without exact task/process completion.

Keep immutable Store receipts and exact process evidence.

## 9. Exact fixtures

### Bus isolation

- `corrupt_bus_scope_does_not_stop_healthy_scope`
- `slot_construction_store_failure_remains_global_hard_error`
- `isolated_scope_retries_only_after_backoff_or_changed_evidence`

### Bus owner receipt

- `bus_child_without_owner_receipt_is_bounded_and_departed_before_retry`
- `bus_missing_receipt_with_unproven_departure_blocks_replacement`
- `late_exact_owner_receipt_before_deadline_transitions_once`
- `stale_owner_receipt_from_prior_child_is_rejected`

### Scheduler receipt closure

- `scheduler_child_without_pid_closes_exact_launch_receipt`
- `scheduler_identity_capture_failure_retains_truthful_terminal_or_unknown_receipt`
- `scheduler_group_identity_failure_does_not_start_replacement_before_departure`
- `scheduler_ready_path_closes_same_launch_id_once`
- `lost_store_reply_reads_back_same_launch_without_respawn`

### Shutdown

- `optional_worker_task_ignoring_stop_does_not_hang_host_shutdown`
- `cleanup_pending_worker_is_not_reported_dormant`
- `secondary_status_failure_does_not_mask_primary_stop_error`

Use public host/supervisor seams with controlled child fixtures. Tests must assert process-family state and Store receipt state, not merely returned error codes.

## 10. Ownership and order

R48 owns:

- optional bus per-scope lifecycle containment;
- scheduler/legacy optional-worker launch receipt closure;
- bounded optional-worker stop aggregation.

R01/#27 owns generic long-lived module identity and module reapers. R35/#61 owns reusable finite completion. R12/#38 owns scheduler due-source semantics and pacing, not the worker process receipt. R34/#60 owns automation fact isolation, not host process supervision.

Files overlap with R01/R35 shared lifecycle seams. Implement after those private APIs stabilize or in one serialized manager worktree; do not create local substitute helpers to avoid rebasing.

Recommended order:

```text
1. consume R35 bounded completion primitive
2. close every scheduler launch receipt branch
3. bound stop_worker/stop_all
4. isolate bus Slot::new failures
5. add bus owner-receipt startup deadline and departure reconciliation
6. delete duplicate/unbounded paths
7. public host fixtures
```

## 11. Gate

After connected code:

```sh
cargo fmt --all -- --check
cargo clippy --locked \
  -p swarm-process \
  -p swarm-supervisor \
  -p swarm-kernel-host \
  --lib --bins -- -D warnings
```

Then the exact host/process fixtures above. Broad native/load qualification remains later.
