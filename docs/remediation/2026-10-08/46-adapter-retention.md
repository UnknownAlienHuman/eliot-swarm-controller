# R46. OpenCode and Claude retained state: delete only fully acknowledged, unreferenced operation journals

**Status:** implementation handoff. Production journals, native sessions and Store facts are unchanged on this branch.

**Evidence baseline:** `40591a295af94b1541ec2ba30afe8e3247701a71` (`main`, 2026-10-08).

## 1. Result

OpenCode and Claude startup/recovery cost becomes proportional to unresolved work, not the lifetime number of Operations on a binding.

```text
durable intent
→ native effect / exact outcome
→ Store acknowledgement
→ all local result pages acknowledged
→ no unresolved journal references this Operation
→ required compact root/boot checkpoint persisted
→ durable journal removal
```

No unresolved intent, unknown outcome, unacknowledged result page or current recovery anchor is deleted. There is no age-based LRU and no “old enough means safe” rule.

R41/#65 supplies torn-tail recovery and durable create/replace/remove primitives. R46 supplies the semantic deletion predicate and connected callers.

## 2. Confirmed lifetime-growth paths

### 2.1 OpenCode

The OpenCode adapter keeps one JSONL file per Operation under `operations/` and separate outbox items. `recover_outbox` and hello/root recovery enumerate every retained operation file.

An operation journal can contain:

```text
intent
outcome
outcome acknowledgement
result page
result acknowledgement
```

After both acknowledgements are durable, the file is still retained and rescanned on every startup/hello. No terminal removal path exists.

`native_root_for_hello` also scans all outbox and operation files because operation outcomes can serve as root identity evidence when a reply was lost. Therefore terminal cleanup cannot simply delete every acknowledged file without first preserving the exact current root checkpoint.

### 2.2 Claude

Claude stores one `.jsonl` file per Operation directly in its journal directory. `pending_outcomes`, `unresolved`, `has_task_dispatch_for_boot` and restart recovery enumerate the entire directory.

A fully acknowledged outcome remains forever. The same historical file is later re-read merely to prove that one `task.dispatch` occurred under a boot.

### 2.3 Existing per-record bounds do not bound lifetime state

Both adapters cap an individual record/file, but neither caps the number of fully acknowledged operations retained. This is not a single-record OOM; it is unbounded directory count and O(total history) restart/readback cost.

The fix is safe terminal reclamation, not lowering the per-record cap or adding a global LRU.

## 3. Do not build a generic retention framework

Keep adapter-specific journal decoders and identity rules. Share only:

- R41 durable file removal/directory sync;
- one small common accounting struct if both live callers use it;
- exact bounded directory iteration helper only if it deletes duplicate code.

Do not add:

- another database;
- background GC service;
- age-based global cleanup;
- generic `Journal<T>`;
- compaction that rewrites unresolved history;
- a retention policy language.

## 4. Closed retention states

Each adapter computes a private state for one validated operation file:

```rust
enum LocalOperationRetention {
    Unresolved,
    DeliveryPending,
    RecoveryAnchor,
    ReferencedByUnresolvedOperation,
    Releasable,
    Damaged,
}
```

This is not persisted as a second authority. It is derived from the current validated journal plus compact checkpoint and unresolved-reference set.

### Unresolved

Any of:

- durable intent without terminal outcome;
- outcome is `Unknown` and its Store resolution is not acknowledged/final;
- adapter-specific deferred interaction/session state still names the Operation.

Never delete.

### DeliveryPending

Any of:

- saved outcome not acknowledged by Store;
- saved result page not acknowledged;
- durable outbox item still exists;
- acknowledgement exists but does not match exact saved digest.

Never delete.

### RecoveryAnchor

The file is currently required to reconstruct:

- OpenCode native root for hello because the compact root checkpoint is absent/incomplete;
- Claude current boot's exact task-dispatch marker;
- another explicitly documented adapter recovery identity.

Persist and read back the compact anchor before changing this state to Releasable.

### ReferencedByUnresolvedOperation

An unresolved operation names this Operation as:

- reconcile target;
- result input/status target;
- assistant-result parent/input target;
- exact predecessor required by current adapter recovery.

Never delete until the referencing operation is terminal and acknowledged.

### Releasable

All terminal payloads/pages are acknowledged, no unresolved reference remains, and every required root/boot checkpoint is durable.

### Damaged

Any identity/digest/state conflict. Never auto-delete; report adapter state damage and retain bytes.

## 5. OpenCode implementation

### 5.1 One connected operation inventory

After R41 unifies the operation-file loader, add one bounded inventory pass that returns only validated summaries needed for retention:

```rust
struct OpenCodeJournalSummary {
    operation_id: String,
    method: String,
    native_root_id: Option<String>,
    outcome_kind: Option<EffectOutcome>,
    outcome_saved: bool,
    outcome_acknowledged: bool,
    result_saved: bool,
    result_acknowledged: bool,
    referenced_operation_ids: Vec<String>,
}
```

Do not clone raw prompt/result bodies into the inventory.

Build the unresolved-reference set from validated unresolved files first. Then classify terminal candidates.

### 5.2 Root checkpoint before deletion

If a releasable file carries the only exact native-root evidence for the binding generation:

1. validate root against deterministic binding/scope identity;
2. persist the existing compact root checkpoint through the R41 durable replace/append seam;
3. read it back through `checkpoint_root`;
4. only then remove the operation file.

Do not infer root from a filename or most-recent timestamp.

### 5.3 Result/reference ordering

An acknowledged outcome is not enough when an unresolved result/reconcile Operation still references the original dispatch/send. The reverse-reference set is checked before deletion.

When both outcome and result page exist, both must be acknowledged. Absence of a result page is acceptable only when no unresolved/future-current local contract requires one; method-specific logic must make that explicit rather than treating “file absent” as universal completion.

### 5.4 Cleanup call sites

Run bounded cleanup:

- after exact outcome acknowledgement;
- after exact result-page acknowledgement;
- once after startup recovery/torn-tail repair;
- after a referencing unresolved Operation becomes acknowledged.

A cleanup failure does not change the acknowledged Store outcome and does not replay native work. It sets a degraded retention diagnostic and is retried on a later bounded cleanup pass.

## 6. Claude implementation

### 6.1 Compact boot dispatch marker

`has_task_dispatch_for_boot` currently scans every journal file. Replace historical-file retention with one compact derived marker, for example:

```rust
struct BootDispatchMarker {
    schema_version: u16,
    boot_id: String,
    operation_id: String,
    receipt_digest: String,
    acknowledged_outcome_digest: String,
}
```

Before deleting an acknowledged `task.dispatch` journal needed by the current boot:

1. validate exact receipt/method/intent/boot ID;
2. persist the marker durably;
3. read it back and compare all fields;
4. make `has_task_dispatch_for_boot` consult this marker plus unresolved files only.

The marker prevents duplicate first dispatch; it is not an Operation result or general history index.

A new boot may replace the marker only after exact boot identity changes under the same binding-generation owner rules.

### 6.2 Terminal deletion

A Claude journal is Releasable only when:

- outcome exists;
- exact outcome acknowledgement exists;
- no unresolved interaction/deferred callback/recovery record names it;
- any current-boot dispatch anchor was compacted first.

Deletion uses R41 durable remove. A missing file after a confirmed prior delete is not corruption; a missing unresolved file remains a recovery gap.

## 7. Accounting and bounded passes

Expose one compact adapter-state projection, not one row per deleted Operation:

```text
unresolved_operation_count
unresolved_bytes
pending_delivery_count
pending_delivery_bytes
recovery_anchor_count
releasable_count_seen
reclaimed_operation_count
reclaimed_bytes
retention_degraded
last_retention_error_code?
```

Rules:

- no raw prompt/result/error text;
- counts use saturating checked arithmetic and signal overflow/partial coverage;
- one pass examines a fixed number of directory entries and carries a stable cursor when more remain;
- immediate post-ack cleanup may target the exact one file without scanning the directory;
- cursor identity is filename/digest order under one locked state owner, not mtime.

Do not block every new command on a full cleanup scan.

## 8. Concurrency and crash boundaries

The module owner already gives one process owner per state directory. Still guard file-level races:

- open/scan/delete the exact regular file under the adapter's journal mutex/single-threaded command loop;
- require the file identity/length/digest state observed by the classifier still matches before delete;
- durable remove may fail after unlink but before directory sync; retry treats exact absence plus no unresolved index/outbox reference as already removed;
- crash before checkpoint publication leaves source journal intact;
- crash after checkpoint publication but before journal removal yields harmless duplicate evidence;
- crash after journal removal cannot lose current root/boot identity because checkpoint readback preceded removal.

## 9. Command adapter boundary

Do **not** delete Command run directories in R46.

Command directories contain native stdout/stderr and run receipts used by later `agent.result` operations. Outcome acknowledgement alone does not prove those bytes are no longer needed. The exact release condition requires a Store-to-adapter evidence-release contract or proof that a result artifact/page covering the capture was committed.

R47 owns that separate source/implementation card. R46 may reuse its accounting shape later but must not apply OpenCode/Claude deletion rules to Command.

## 10. Simplification/deletion

After connected migration delete:

- full-history `has_task_dispatch_for_boot` scan;
- hello/root dependence on fully acknowledged old operation files;
- startup scanning of acknowledged terminal files after bounded cleanup converges;
- duplicate operation-directory walkers replaced by one validated inventory path per adapter;
- any age-only cleanup prototype;
- any retained terminal journal kept solely “just in case” after its explicit compact anchor exists.

Do not remove historical Store Operations or artifacts.

## 11. Donor mechanisms

- CCCC: startup/read cost should follow unread/unresolved tail, not total retained history;
- Bazel CAS/action cache: compact identity/digest evidence can replace repeated reading of full completed input history;
- Emdash: delete mirror/evidence only after host/current truth is durably observed;
- R41 internal journal work: exact durable file operations and torn-tail classification.

Do not copy CCCC's file ledger or add a second durable engine.

## 12. Exact fixtures

OpenCode:

- `opencode_acknowledged_unreferenced_operation_is_reclaimed`
- `opencode_unacknowledged_outcome_is_never_reclaimed`
- `opencode_result_target_reference_retains_source_operation`
- `opencode_root_checkpoint_is_durable_before_source_delete`
- `opencode_startup_scan_cost_tracks_unresolved_tail`
- `opencode_cleanup_failure_never_replays_native_effect`

Claude:

- `claude_acknowledged_operation_is_reclaimed`
- `claude_current_boot_dispatch_compacts_before_delete`
- `claude_unresolved_operation_survives_cleanup`
- `claude_boot_marker_prevents_duplicate_dispatch_after_restart`
- `claude_damaged_terminal_file_is_retained_not_deleted`

Shared:

- `retention_pass_resumes_from_stable_cursor`
- `retention_crash_after_checkpoint_before_delete_is_idempotent`
- `retention_crash_after_delete_never_loses_recovery_anchor`
- `retention_projection_contains_no_raw_payload`

Use real directories and public adapter recovery/startup paths. A unit test that only returns `Releasable` is insufficient.

## 13. Ownership and order

- R41/#65: durable file operations and torn-tail recovery.
- R02/#28: OpenCode native effect/recovery semantics.
- R16/#42: Claude session/permission semantics.
- R46: terminal journal reference analysis, compact anchors and reclamation.
- R47: Command capture/evidence release.

Recommended order:

```text
R41 file semantics
→ OpenCode cleanup with current R02 decoder
→ Claude cleanup with current R16 decoder
→ delete duplicate full-history scans
```

One manager owns shared file primitives. Adapter writers rebase and keep domain classifiers local.

## 14. Gate after connected code

```sh
cargo fmt --all -- --check
cargo clippy --locked -p swarm-process -p swarm-adapter-opencode -p swarm-adapter-claude --lib --bins -- -D warnings
```

Then exact startup/locality fixtures. Paid model calls are not required for this retention slice.
