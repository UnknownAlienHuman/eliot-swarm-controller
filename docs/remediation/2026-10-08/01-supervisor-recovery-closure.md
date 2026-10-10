# R01 companion. Supervisor recovery closure: prior-owner handoff, replaceable terminal state and bounded reapers

**Status:** implementation handoff. Production supervisor/process code is unchanged on this branch.

**Evidence baseline:** `40591a295af94b1541ec2ba30afe8e3247701a71` (`main`, 2026-10-08).

## 1. Result

One module scope has one recoverable owner chain:

```text
verified prior owner checkpoint
→ exact family departure
→ explicit handoff from prior checkpoint to current boot
→ current owner/worker receipts validated in either arrival order
→ authenticated hello
→ running
→ bounded stop and exact departure
→ replaceable terminal state
```

A stale prior receipt is neither deleted before it ceases to be needed nor mistaken for the current helper. A finished/closed old descriptor cannot block a valid replacement forever. Detached reapers never own a process indefinitely without a deadline and retained status.

R01 remains the single owner of long-lived module lifecycle; R35 supplies neutral bounded process completion.

## 2. Confirmed prior-owner race

### Current writers

The module-owner helper deliberately supports restart with an existing `owner.json`:

1. read prior owner receipt;
2. prove its process group departed;
3. retain the checkpoint as recovery evidence;
4. publish the new owner receipt over the same path;
5. spawn/publish worker evidence.

### Current supervisor reader

Before spawning the new helper, `clear_prior_helper_result` removes:

```text
launch-result.json
worker.json
```

but retains `owner.json`.

Immediately after spawn, `monitor_owner_helper` scans `owner.json`. If it reads the verified **prior** receipt before the new helper replaces it, it compares that prior process identity with the newly spawned helper identity and returns:

```text
MODULE_OWNER_IDENTITY_MISMATCH
```

The retained checkpoint is valid historical evidence, but the current monitor has no handoff state distinguishing prior from current.

## 3. Exact prior→current handoff

Carry the already validated prior-owner receipt/identity from `wait_for_prior_owner_to_depart` into the current launch/monitor path.

Use an explicit private state:

```text
PriorOwnerAbsent
PriorOwnerDeparted { exact receipt digest / identity }
CurrentOwnerValidated { exact current receipt }
```

Monitor rules:

- no prior checkpoint: first valid owner receipt must identify current helper;
- prior departed checkpoint: the exact prior bytes/identity may remain temporarily and are ignored only as the known prior checkpoint;
- any different third receipt is corruption;
- current owner receipt must identify the exact spawned helper and current boot plan;
- after current receipt validation, a regression to prior/different bytes is corruption;
- worker receipt may arrive before owner; retain raw worker bytes and validate them after current owner arrives;
- repeat the receipt scan after exact helper exit to close publication races.

Do not delete `owner.json` before spawn merely to avoid the race. It is the helper's recovery checkpoint and may be the only proof of a prior live/unknown group.

After current owner publication is validated and durable, historical cleanup may occur only under a named retention policy; it is not required for this correctness slice.

## 4. Descriptor replacement currently depends on incidental lifecycle labels

`can_replace_descriptor` requires all of:

- no demand;
- no readback/unknown Operations;
- lifecycle in `WaitingForDemand | Completed | Isolated`;
- runner absent/finished.

A finished runner in `ProcessExited` or `WaitingForKernel` is rejected. The descriptor-mismatch demand path calls `can_replace_descriptor` **before** `existing.demand`, so it does not restart the old service to move it into an accepted label. The new descriptor receives `MODULE_UPDATE_DEFERRED` repeatedly with no state transition that can make replacement possible.

The replacement safety predicate must be based on authoritative obligations, not a small allowlist of presentation states.

## 5. One replaceability verdict

Add a private exhaustive verdict derived under the service locks/readback:

```text
Replaceable
ActiveOwner
DemandHeld
ReadbackRequired
UnknownOperations
RunnerActive
OwnerIdentityUnknown
CleanupPending
StateCorrupt
```

`Replaceable` requires:

- zero demand;
- no live owner/worker identity;
- exact family departure for every retained owner;
- no readback-required or unresolved Operations;
- no cleanup-pending/identity-unknown state;
- runner absent or completed;
- retained state/checkpoint belongs to this exact scope and is readable.

`ProcessExited`, `WaitingForKernel`, `Completed`, `Isolated` and `WaitingForDemand` are not individually authoritative. A state may be Replaceable only when the facts above prove it.

On replacement:

1. retain final old descriptor/departure evidence;
2. clear only restart budget/state owned by the old descriptor after exact readback;
3. create the new fingerprinted slot;
4. no second live helper;
5. no reuse of old boot/worker receipt as current evidence.

## 6. Unbounded reapers

`host_module_supervisor` moves failed children into detached reaper tasks.

Both paths currently wait without a process deadline:

```rust
reap_unidentified_supervisor_child: child.wait().await
reap_owned_supervisor_child: child.wait().await
```

The owned path writes uncertainty first, but a child that never exits occupies a detached task forever. The unidentified path has even less authority: it has a Child handle but no verified birth/image identity, and waiting forever does not make it safe. Meanwhile higher-level logic can later start another supervisor if it cannot see that detached reaper as the active owner.

Required repair:

- retain every reaper in an explicit registry keyed by exact launch/scope identity, not a dropped JoinHandle;
- request graceful EOF/stop once;
- bounded direct-child wait;
- if exact identity/family is known, escalate through R35/R01 process-family stop;
- if identity is unknown, do not broad-kill by PID/name; retain `identity_unknown`, block replacement and surface attention;
- record confirmed departure or cleanup-pending against the exact same launch;
- one reaper per child; no duplicate supervisor until departure/recovery decision;
- shutdown drains reapers with a bound and truthful residual status.

A timeout is not departure proof. `JoinHandle::abort` is not child cleanup.

## 7. Module-owner lock held by unbounded child/family wait

`swarm-process::module_owner` holds the module state lock while it executes:

```text
child.wait()
→ while !group.children_empty() { sleep }
→ drop group
→ drop lock
```

There is no stop deadline or escalation. A child/descendant that ignores normal shutdown keeps `module.lock` forever; replacement and recovery cannot acquire the scope.

Connect the helper to the same explicit stop-and-wait contract:

- parent/supervisor requests stop through a defined owner channel/EOF;
- owner waits a bounded graceful interval;
- exact owned adapter process family is terminated after deadline;
- family departure is read back;
- only then release the state lock;
- external/shared native services are not in this owned process family and are never killed;
- if departure cannot be proved, retain lock/owner uncertainty and do not report a clean stop.

Do not add a silence-based kill. The stop request and deadlines are explicit configuration/contract values with bounded defaults owned by the host, not model-generated task limits.

## 8. Same-boot status publication

Preserve the previously verified invariant:

```text
ProcessRunning / Ready
only after Store accepted exact module.hello
and current worker birth/image remains live
```

Any I/O between reading boot/generation and status publication must recheck:

- same service slot pointer;
- same descriptor fingerprint;
- same binding generation;
- same boot;
- same exact worker identity.

A stale hello/liveness result from a replaced slot cannot mutate current status. A polling error does not demote a previously verified Running state into a fresh start/replacement permission.

## 9. Exact fixtures

### Prior owner

- `prior_departed_owner_receipt_may_exist_until_current_owner_publication`
- `prior_owner_is_never_accepted_as_current_helper`
- `worker_receipt_before_current_owner_converges_after_owner_arrives`
- `current_owner_before_worker_converges_once`
- `third_owner_receipt_during_handoff_is_corruption`
- `helper_exit_receipt_rescan_closes_publication_race`

### Descriptor replacement

- `finished_process_exited_service_with_no_obligations_is_replaceable`
- `waiting_for_kernel_without_owner_or_obligations_is_replaceable`
- `cleanup_pending_or_identity_unknown_blocks_replacement`
- `changed_descriptor_does_not_need_old_demand_to_relabel_state`
- `replacement_never_starts_second_helper_before_departure`

### Reapers/lock

- `owned_supervisor_reaper_is_bounded_and_registered`
- `unidentified_reaper_blocks_duplicate_without_broad_kill`
- `module_owner_stop_kills_exact_owned_family_after_grace`
- `module_owner_lock_released_only_after_confirmed_departure`
- `external_native_service_survives_adapter_owner_stop`
- `host_shutdown_reports_cleanup_pending_instead_of_hanging`

### Ready invariant

- process/receipt before hello → no Ready;
- exact current hello → one Ready;
- stale boot or departed worker → reject/no Ready;
- old status task after replacement → no mutation.

Tests must assert exact child/family and slot state, not only error strings.

## 10. Simplification and deletion

After migration, delete:

- implicit prior/current owner discrimination by immediate byte comparison;
- lifecycle-state allowlist as descriptor replacement authority;
- detached untracked reaper tasks;
- unbounded module-owner child/group loops;
- stale status publications without same-slot/boot guard;
- any recovery branch that removes checkpoint evidence before replacement safety is proven.

Keep one process identity constructor, one stop/departure seam and one replaceability verdict.

## 11. Ownership and order

R01/#27 owns this long-lived lifecycle closure.

R35/#61 supplies neutral bounded process-family completion. R39/#63 supplies crash-repairable state marker acquisition. R48/#71 owns optional bus/scheduler supervisors. R49/#72 owns finite Zed batches. R02/#28 consumes the lifecycle seam for OpenCode NativeOwner.

Shared `swarm-process`/`swarm-supervisor` files require one manager/worktree. Recommended order:

```text
R35 finite completion primitive
→ R39 marker primitive
→ prior/current owner handoff
→ same-boot status guard
→ replaceability verdict
→ registered bounded reapers
→ module-owner stop/lock release
→ delete old loops/allowlists
```

## 12. Gate

After connected code:

```sh
cargo fmt --all -- --check
cargo clippy --locked \
  -p swarm-supervisor \
  -p swarm-process \
  -p swarm-kernel-host \
  --lib --bins -- -D warnings
```

Then Windows and Unix lifecycle fixtures above. Broad native qualification remains later.
