# R41. Adapter journals: salvage only a torn tail, durable directory updates, one record interpretation

**Status:** implementation handoff. Production adapter state is unchanged on this branch.

**Evidence baseline:** `40591a295af94b1541ec2ba30afe8e3247701a71` (`main`, 2026-10-08).

## 1. Result

OpenCode and Claude adapter journals recover after a crash during the final append without inventing an outcome, replaying a native effect or accepting interior corruption.

```text
open exact private journal
→ scan complete bounded records
→ validate every complete record through the adapter's current decoder
→ classify EOF
   complete                         → use history
   incomplete final byte sequence   → retain evidence + truncate to last valid boundary
   complete invalid/interior damage → fail closed, no truncation
→ fsync repaired file and directory
→ rebuild exact pending outbox from durable records
```

File creation, replacement, append acknowledgement and removal use one small set of durable filesystem primitives. This task does not build a generic workflow engine or another Store.

## 2. Confirmed current failures

### 2.1 OpenCode: one torn or zero-byte operation file blocks all recovery

`swarm-adapter-opencode::journal` stores one JSONL file per Operation. Both `load` and `load_by_path` require every read record to end in `\n`; an incomplete final line is a hard `ADAPTER_JOURNAL` error.

`Journal::open` does not scan/salvage operation journals. Immediately afterward `recover_outbox` enumerates every operation file and calls `load_by_path`. Therefore one crash during the final append, or one zero-byte file left after creation, aborts adapter startup and prevents every unrelated pending outcome/result from being requeued.

The same strict decoder is also used by `native_root_for_hello`, so fixing only `recover_outbox` leaves hello identity discovery bricked.

### 2.2 Claude has the same failure shape

`swarm-adapter-claude::journal::OperationJournal::get/read_path` reads JSONL and rejects a record when:

```text
line > MAX_RECORD_BYTES
OR final byte != newline
OR JSON/identity/digest is invalid
```

`pending_outcomes`, `unresolved`, boot lookup and restart recovery enumerate every `.jsonl` file. One torn tail prevents unrelated operations from being recovered.

This is the same storage failure class with a different domain record type. Do not fix it with two unrelated policies.

### 2.3 New-file durability differs from append durability

Both adapters append with:

```text
OpenOptions::append
write_all
file.sync_all
```

For the first record they call `swarm_process::write_private_new`, which syncs the file but does not sync the parent directory. A power loss may therefore remove the directory entry after code has treated the intent/outcome as durable.

Outbox removal similarly deletes an acknowledged file without a parent-directory sync. The host effect may already be committed; reappearance after crash must be handled deliberately, not by filesystem luck.

### 2.4 Current readers mix two questions

The existing read loops simultaneously:

- frame lines;
- decide whether EOF means a torn tail;
- deserialize domain records;
- validate identity/digest/state order;
- build history.

This makes tail recovery tempting to implement as “ignore the last error”, which would also hide a complete but invalid final record. The framing verdict must be explicit and narrower.

## 3. Reuse existing code before creating abstractions

### 3.1 Durable publication sequence

The Command adapter already contains the useful sequence:

```text
private create-new temp
write_all
file.sync_all
atomic same-directory rename
Unix parent-directory sync_all
```

Adapt that small platform sequence into the existing `swarm-process` private-file owner. Do not make adapter A depend on adapter B.

### 3.2 Existing line scanners and decoders

Keep each adapter's current domain decoder and identity/digest checks. The new framing pass should return complete record byte ranges to those decoders; it must not introduce a second interpretation of `JournalRecord`.

### 3.3 No adapter SDK yet

R41 shares only filesystem/framing primitives. Do not extract an `AdapterRuntime`, `Journal<T>`, generic effect state machine or new crate in this slice. A common adapter SDK belongs after two fully connected adapters expose a stable identical contract and can delete more code than the abstraction adds.

## 4. Narrow durable file primitives

Extend the existing platform owner (`swarm-process`) with small functions; names may differ, responsibilities may not:

```rust
pub fn sync_parent_directory(path: &Path) -> Result<()>;

pub fn write_private_new_durable(path: &Path, bytes: &[u8]) -> Result<()>;

pub fn replace_private_durable(path: &Path, bytes: &[u8]) -> Result<()>;

pub fn remove_private_durable(path: &Path) -> Result<bool>;
```

Rules:

- regular private file only; no symlink/reparse traversal;
- create-new never overwrites;
- replacement is same-directory temp + exact atomic replace;
- file data is synced before publication;
- Unix parent directory is synced after create/rename/remove;
- Windows uses the existing write-through atomic replacement primitive where available; do not claim directory fsync equivalence that the implementation cannot prove;
- errors remain errors; no best-effort success for authority/effect records.

If extending `write_private_new` itself is safe for all current callers, do that and delete the duplicate name. Otherwise migrate the two journal callers and name the deletion condition for the old primitive. Do not keep two indefinite “durable” APIs.

## 5. Explicit JSONL scan verdict

Add one small bounded scanner near the journal code or in the narrow file primitive owner only after both callers use it:

```rust
enum JsonlTail {
    Complete,
    Torn { valid_bytes: u64, tail_digest: String, tail_bytes: u64 },
}

struct JsonlScan {
    complete_ranges: Vec<Range<u64>>, // or callback-driven streaming equivalent
    tail: JsonlTail,
}
```

Prefer callback/streaming ranges so total journal bytes are not duplicated in memory.

Classification:

1. newline-terminated record within byte bound → pass exact bytes to current domain decoder;
2. EOF after zero bytes → Complete;
3. EOF with nonempty non-newline suffix → Torn **only after every previous complete record validated**;
4. newline-terminated invalid JSON/domain record → Corrupt, no repair;
5. oversized complete or incomplete record → Corrupt/over-limit, no repair;
6. invalid record in the middle → Corrupt, no repair.

A zero-byte operation file is repairable only when its filename/parent scope is valid and no durable record/effect can have been inferred from it. Delete it durably or truncate/classify it as empty according to the caller's file-creation protocol; do not fabricate an intent.

## 6. Repair algorithm

OpenCode and Claude use the same sequence:

1. open the exact regular file read/write without following links;
2. scan and validate complete prefix;
3. if Complete, build history normally;
4. if Torn:
   - retain bounded diagnostic evidence (`file identity`, valid prefix length, tail digest/length), never raw secret/prompt bytes;
   - acquire/retain the adapter's single-owner state lock;
   - re-stat the opened file and require the same length/identity observed by the scan;
   - truncate to `valid_bytes`;
   - `sync_all` file and parent directory;
   - rescan once and require Complete;
5. rebuild pending outcome/result files only from validated retained records;
6. never repeat the original native input/effect because a tail was repaired.

If file identity/length changes between scan and repair, return conflict and retry from a fresh open; do not truncate a moving target.

## 7. Wire all current readers to one verdict

### OpenCode

Use one operation-file loader for:

- `load`;
- `load_by_path`;
- `recover_outbox`;
- `native_root_for_hello`;
- `native_root_checkpoint_for_hello` where relevant.

Do not keep a strict hello scanner beside a salvaging outbox scanner.

### Claude

Use one operation-file loader for:

- `get`;
- `read_path`;
- `pending_outcomes`;
- `unresolved`;
- boot/task-dispatch lookup;
- restart unknown-outcome creation.

A repaired tail must produce the same `OperationState` as the validated prefix did before the crash.

## 8. Acknowledged outbox removal

After host acknowledgement:

```text
append acknowledgement record + sync
→ remove exact pending outbox file
→ sync outbox directory
```

If removal or directory sync fails, return an error and retain/reconstruct the pending item. A later duplicate host delivery is permitted only because Store idempotency validates the same operation/payload; document that exact boundary. Do not report the local outbox empty when deletion durability is unknown.

## 9. Retention boundary

R41 repairs crash consistency only. It does not solve unlimited journal/history growth.

However, implementation must expose exact accounting needed by a later retention slice:

```text
operation file count
journal bytes
outbox count/bytes
oldest unacknowledged item
last successful compaction/repair
```

Do not add compaction in this PR unless it can preserve every unresolved intent/outcome/result and delete the replaced scan path in the same connected slice.

## 10. Simplification/deletion

After both callers switch, delete:

- duplicate tail/line EOF interpretation in `load` versus `load_by_path`;
- any branch that treats incomplete final bytes as whole-journal corruption;
- any “ignore last parse error” compatibility fallback;
- adapter-local create/replace/remove durability copies replaced by `swarm-process` primitives;
- OpenCode and Claude startup paths that rescan the same file through a second decoder.

Historical complete records remain readable. No migration rewrites valid journal history.

## 11. Exact fixtures

Shared filesystem fixtures:

- `durable_create_survives_reopen_with_parent_sync`
- `durable_replace_never_exposes_partial_bytes`
- `durable_remove_reports_directory_sync_failure`
- `jsonl_torn_final_record_truncates_to_valid_prefix`
- `jsonl_complete_invalid_final_record_is_not_salvaged`
- `jsonl_interior_corruption_is_not_salvaged`
- `jsonl_oversized_tail_is_not_salvaged`
- `jsonl_changed_during_repair_is_not_truncated`

OpenCode public/adapter fixtures:

- `opencode_torn_outcome_tail_recovers_unrelated_outbox`
- `opencode_zero_byte_operation_does_not_brick_hello_scan`
- `opencode_repair_never_replays_native_input`
- `opencode_load_and_load_by_path_have_one_verdict`

Claude fixtures:

- `claude_torn_ack_tail_requeues_exact_saved_outcome`
- `claude_torn_tail_does_not_hide_complete_invalid_record`
- `claude_recovery_unknown_keeps_exact_receipt_identity`

Use real temporary files and the public adapter startup/recovery path where practical, not helper-only byte arrays.

## 12. Ownership and order

- R02/#28 owns OpenCode effect semantics, IPC reuse and native-owner lifetime. It rebases onto this file primitive/tail verdict and deletes local duplicate handling.
- R16/#42 owns Claude permission/session interactions. R41 changes only durable operation record recovery.
- Command journal is a source-reviewed internal donor; its behavior is not changed unless the shared primitive removes a duplicate safely.
- Observer segment salvage/retention is a separate task because its record/rotation semantics differ from per-operation journals.

Recommended order:

```text
swarm-process durable create/replace/remove
→ OpenCode connected loader + recovery caller
→ Claude connected loader + recovery caller
→ delete duplicate scanners/helpers
```

One manager owns `swarm-process` shared files. OpenCode and Claude writers rebase; they do not each add another platform helper.

## 13. Gate after connected code

```sh
cargo fmt --all -- --check
cargo clippy --locked -p swarm-process -p swarm-adapter-opencode -p swarm-adapter-claude --lib --bins -- -D warnings
```

Broad native/model qualification remains the final product phase. This branch currently changes one Markdown file only.
