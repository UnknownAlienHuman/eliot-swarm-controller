# R49. Zed batch lifecycle: retain the child until exact terminal or family departure

**Status:** implementation handoff. Production Zed runtime, Store outcomes and native processes are unchanged on this branch.

**Evidence baseline:** `40591a295af94b1541ec2ba30afe8e3247701a71` (`main`, 2026-10-08).

## 1. Result

A one-shot Zed `eval-cli` run has one recoverable lifecycle:

```text
durable exact intent
→ exact child/process-family owner
→ bounded direct-exit and family-departure observation
→ complete/partial capture and native result
→ durable terminal or outcome-unknown receipt
→ Store outcome
→ evidence release only by later policy
```

An observation error never drops the live child. A timeout never reports cleanup success without departure. A crash cannot leave a torn final-looking control file. Recovery never reruns the prompt.

No persistent Zed session, steer, goal or second scheduler is introduced. Zed remains a one-shot batch runtime.

## 2. Confirmed failure chain

### 2.1 `try_wait` error drops ownership of a possibly-live process

`runtime/zed.rs::wait_bounded` currently does:

```rust
if let Some(status) = child.try_wait()? {
    return Ok((status, false));
}
```

An OS observation error returns `Err` immediately. `run_batch_inner` then unwinds and drops `std::process::Child`; dropping that handle does not terminate the process.

The run already has:

- a unique output directory;
- `intent.json`;
- stdout/stderr files;
- a possibly running child and descendants.

The Store wrapper catches the error, sees the run directory, writes an outcome-unknown diagnostic and may persist a receipt. This is better than leaving the Operation queued, but it has lost the only process handle. The native effect can keep running outside controller ownership.

### 2.2 The immutable run marker correctly prevents replay, but makes the orphan permanent

Every new run first performs:

```rust
create_dir(output_root/run_id)
```

and refuses to overwrite an existing directory. Recovery reads a completed receipt when one exists; otherwise it records `prior_run_marker_without_terminal` / another unknown diagnostic.

That no-replay rule is correct. Combined with the lost child handle, however, it means:

```text
orphan continues or exits later
→ controller cannot stop or prove departure
→ same prompt cannot be replayed
→ same run directory cannot be reused
→ result remains unknown even if native files appear after the handle was lost
```

The fix must restore ownership/readback, not delete the marker or rerun the command.

### 2.3 Host timeout cleanup is not a complete process-family proof

Zed creates a separate process group on every Unix platform:

```rust
command.process_group(0);
```

but `terminate` sends the group signal only on Linux. Other Unix systems kill only the direct child. A descendant can retain work/output handles after the controller reports `host_terminated=true`.

`child.wait()` proves only direct-child exit. It does not prove the process group is empty.

### 2.4 Control-file creation is durable only after a complete write

`write_json_new` opens the final pathname with `create_new`, writes canonical JSON in place, syncs the file and syncs the parent directory on Unix.

If the process crashes after final-path creation but before the complete write/sync, recovery sees the immutable filename containing zero/prefix bytes. `read_json_bounded` then rejects it. Because the run directory is immutable, the torn record cannot be repaired or replaced safely by the current path.

The create-new/no-overwrite invariant is useful, but publication must be temp/write/sync/rename/parent-sync or an equivalent exact finalization primitive.

### 2.5 Command retrieval failures are silently swallowed

`drive_zed_binding` performs:

```rust
self.run(next).await.ok()
    .and_then(|value| serde_json::from_value::<RuntimeCommand>(...).ok())
```

A Store error and an invalid retained command both become “no command”; the worker sleeps and retries without a durable diagnostic. If `next` already advanced an Operation to `sending`, malformed command decoding can strand it while the worker appears healthy.

This is a separate input-side lifecycle hole in the same sole Zed owner and must be fixed in the vertical slice, not by adding a watchdog.

## 3. Reuse boundaries

Consume, do not duplicate:

- R35/#61 finite child completion vocabulary and bounded direct-exit/family-departure/capture drain;
- `swarm-process` process-group/Windows Job ownership and exact departure readers;
- R41/#65 narrow durable private-file publication primitives once extracted;
- existing `BatchIntent`, `BatchReceipt`, run-directory identity and no-replay recovery;
- existing Store outcome/readback path.

Zed must not depend on an adapter crate. Shared file/process primitives live at the minimum neutral owner only after connected callers exist.

No PTY is needed for this batch executor. `portable-pty` does not prove process-family departure.

## 4. One private batch owner

Introduce one private owner object around the already spawned child and exact run:

```rust
struct ZedBatchOwner {
    child: Child,
    family: ProcessFamilyIdentity,
    run_dir: PathBuf,
    intent: BatchIntent,
    state: Spawned | DirectExited | CleanupPending | Departed,
}
```

Use existing concrete process types/names; this shape is descriptive, not a requirement to add a broad public abstraction.

Rules:

1. Capture exact child/family identity immediately after spawn and before treating the run as launched.
2. If identity capture is uncertain, retain the child handle, enter cleanup/readback, and never start another run for that identity.
3. `try_wait` error becomes `DirectExit::Unknown`; it does not return while ownership is still live.
4. On execution deadline or observation uncertainty, request termination once and start a separate cleanup deadline.
5. At cleanup deadline, return a truthful `cleanup_pending`/`observation_unknown` receipt while preserving enough exact owner identity for later reconciliation. Do not report family released.
6. Exact family departure is required before the owner is discarded.
7. Never execute the original prompt twice.

If the current in-process architecture cannot retain the `Child` across Store calls, move the finite run into the existing bounded worker/owner pattern rather than detaching it. Do not use a background thread with no Store identity.

## 5. Execution and cleanup deadlines

Keep the native `--timeout` and host execution deadline, but distinguish:

```text
execution_deadline
termination_grace_deadline
capture_drain_deadline
```

At host execution deadline:

- mark timeout intent;
- terminate the exact family, not a PID-name match;
- wait only through termination grace;
- preserve partial output and capture completeness;
- if the family remains/unobserved, return cleanup-pending and keep ownership for reconciliation.

On Unix, use the neutral process-family primitive on every supported Unix target. Do not maintain Linux-only group cleanup beside a direct-child fallback that claims equivalent completion.

On Windows, use the existing Job semantics rather than `Child::kill` as the family proof.

## 6. Durable control-file publication

Use a narrow exact publication operation:

```text
open unique temp in exact run directory
→ private permissions
→ write bounded canonical bytes
→ sync temp
→ rename without accepting a different final value
→ sync parent directory
→ exact readback
```

For create-new immutable records, handle an existing final path as:

- exact same validated bytes/identity → idempotent;
- different or damaged bytes → conflict/corruption;
- never overwrite.

Apply it to intent and receipt/control records. Native output files remain owned by the native contract and are validated separately.

Recovery may salvage only a provably torn final suffix where the selected primitive explicitly supports it. It must not reinterpret arbitrary partial JSON as an intent.

## 7. Store-side command handling

Replace `.ok()` swallowing with exhaustive dispositions.

### Store `next` failure

- Store/journal failure closes or degrades the worker admission path and is surfaced to host status/attention;
- no new native effect starts;
- preserve the primary error if diagnostic recording also fails.

### RuntimeCommand decode failure

Where the raw response still exposes the exact Operation ID and binding identity, settle that exact command as rejected/not-started with a closed schema diagnostic.

Where identity cannot be trusted, stop admitting work for the binding and require Store readback. Do not silently sleep or guess an Operation.

### `process_zed_command` error

Every command taken from `next` must end in one of:

```text
terminal outcome retained
outcome_unknown retained with exact run/owner evidence
worker admission closed because command identity is corrupt
```

A logged error alone is not completion.

## 8. Receipt and result semantics

Extend the private receipt only as needed to represent truthful completion:

```text
direct_exit: observed | unknown
family_departure: confirmed | cleanup_pending | observation_unknown
capture: complete | incomplete
host_termination_requested: bool
```

Do not fabricate zero bytes on capture failure. Retain actual prefix bytes, total observed bytes when available, truncation, I/O error and completeness separately.

A terminal Applied/Rejected result requires the existing native result/exit consistency plus exact completed capture and the method-specific effect evidence. Cleanup-pending remains nonterminal/unknown.

Late native files discovered after an outcome-unknown run are read only after exact run/intent validation and, where required, family departure. They may settle the same Operation once; they never start another command.

## 9. Exact fixtures

### Process ownership

- `zed_try_wait_error_retains_child_until_family_departure`
- `zed_timeout_kills_entire_process_family_on_supported_unix`
- `zed_timeout_kills_entire_job_on_windows`
- `zed_cleanup_deadline_returns_pending_without_claiming_release`
- `zed_late_family_departure_settles_same_run_once`

Use a controlled child with a descendant that holds stdout/stderr or ignores ordinary termination.

### No replay and recovery

- `zed_existing_run_marker_never_replays_prompt`
- `zed_unknown_run_with_late_receipt_is_reconciled_without_respawn`
- `zed_changed_intent_for_existing_run_conflicts`
- `zed_orphan_identity_unknown_blocks_replacement`

Count actual child executions, not only Operation rows.

### Durable files

- `zed_intent_publish_survives_crash_windows`
- `zed_zero_byte_or_prefix_final_record_is_corrupt_not_fresh`
- `zed_same_intent_publication_is_idempotent`
- `zed_different_existing_intent_never_overwrites`

### Command intake

- `zed_store_next_failure_is_visible_and_starts_no_child`
- `zed_malformed_retained_command_cannot_disappear_as_idle`
- `zed_each_taken_command_has_terminal_unknown_or_closed_admission`

### Evidence

- `zed_capture_failure_preserves_partial_bytes`
- `zed_direct_exit_without_family_departure_is_not_terminal_cleanup`
- `zed_result_readback_binds_exact_intent_route_prompt_and_snapshot`

Tests must enter through the real binding worker/Store seam for lifecycle claims. Helper tests supplement but do not replace them.

## 10. Simplification and deletion

After migration, delete:

- direct `child.try_wait()?` escape while a child may live;
- Linux-only group kill plus supposedly equivalent direct-child cleanup on other Unix;
- final-path in-place JSON publication;
- `.await.ok()` and decode `.ok()` in the Zed command loop;
- status-only error paths that leave a taken command without retained disposition;
- any cleanup-success flag derived from direct-child exit alone.

Keep the immutable run identity and no-replay rule.

## 11. Ownership and ordering

R49 owns the Zed vertical slice:

- command intake;
- one-shot child/family lifecycle;
- control-file durability;
- exact batch receipt/recovery.

R35/#61 owns neutral finite-process completion. R41/#65 owns neutral durable-file primitives. R48/#71 owns host optional supervisors, not Zed runs. R01/#27 owns long-lived module lifecycle.

Implement after R35/R41 private APIs stabilize, or serialize one manager through the shared files. Do not create Zed-local copies just to avoid a rebase.

Recommended order:

```text
1. connect neutral durable create/replace primitive
2. connect finite process-family completion
3. retain Zed child/family owner across uncertainty
4. migrate timeout and capture
5. make command intake exhaustive
6. migrate receipts/recovery
7. delete old direct loops and swallowed errors
8. run public-path fixtures
```

## 12. Gate

After connected code:

```sh
cargo fmt --all -- --check
cargo clippy --locked \
  -p swarm-process \
  -p swarm-kernel-host \
  --lib --bins -- -D warnings
```

Then exact Windows and Unix process fixtures. Live `eval-cli` qualification remains a later phase; current source explicitly says the installed runtime is not yet live-qualified.
