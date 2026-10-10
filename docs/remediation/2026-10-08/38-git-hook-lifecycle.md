# R38. Git hook lifecycle: resumable install/revoke, bounded readback, one local state machine

**Status:** implementation handoff. This branch changes documentation only; production hook files, Store rows and credentials are unchanged.

**Evidence baseline:** `40591a295af94b1541ec2ba30afe8e3247701a71` (`main`, 2026-10-08). Before implementation, compare the current branch and main; do not reproduce fixes that have already landed.

## 1. Result

`swarm hook setup`, local install readback and `swarm hook install revoke` use one crash-resumable local installation record. A failure between manifest publication, wrapper replacement, Store revocation and local cleanup never bricks the repository and never causes an unrelated user hook to be overwritten.

```text
caller-owned setup identity + private credential
→ Store HookSource identity
→ durable local install intent
→ exact wrapper/backup effect
→ readback
→ installed state

Store source revoked
→ durable local revocation intent
→ exact restore/remove effect
→ readback
→ cleanup local source files
```

The existing Store `hook.source.setup`, `hook.source.revoke` and `hook.emit` contracts remain. No new Store table, public method, hook daemon, workflow engine or generic filesystem transaction layer is added.

## 2. Current confirmed failure chain

### 2.1 Install manifest is written before the wrapper but claims a completed installation

`hooks/git.rs::apply_post_commit` currently:

1. preserves the previous extensionless hook;
2. builds an `InstallManifest` with no lifecycle phase;
3. writes `install.json` with `create_new`;
4. writes the wrapper temporary file;
5. atomically replaces `.git/hooks/post-commit`;
6. performs readback.

If step 4–6 fails, `install.json` remains. On retry `readback_post_commit` classifies the source as `modified`; `apply_post_commit` returns `HOOK_INSTALL_STATE_CONFLICT` before it can resume.

The manifest is not wrong because it exists before the effect; it is wrong because its schema cannot distinguish an admitted intent from an installed state.

### 2.2 Current compensation cannot reliably repair the local state

`main.rs::hook_setup` calls `rollback_hook_setup` after a local install/readback failure. It revokes the Store source and then calls `revoke_post_commit`.

`revoke_post_commit` returns early for `state == "modified"`: it removes the credential but does not remove or complete the install manifest/backup. Therefore the same repository can remain permanently non-installable even though Store authority was revoked.

This is a cross-domain compensation attempt without a durable local transition state. A larger rollback function is not the fix.

### 2.3 Revoke is not resumable

The current revoke sequence:

```text
restore/remove hook target
→ remove credential
→ remove manifest
→ remove source directory
```

A crash after the target was restored but before the metadata was removed causes the next readback to classify the target as `modified`, because it no longer matches the ELIOT wrapper. The early `modified` path then refuses to finish cleanup.

### 2.4 Backup verification is fail-open and not actually bounded

`readback_post_commit` uses:

```rust
fs::read(path).ok().map(|bytes| digest(bytes) == expected)
```

For an expected backup, a read error becomes `backup_matches = None`. The installation is considered installed whenever `backup_matches != Some(false)`, so an unreadable required backup can be treated as valid.

`read_bounded` and `read_optional_regular_file` check metadata length and then call `fs::read`. A growing/replaced regular file can exceed the checked bound during the actual read. The bound protects the pre-read metadata, not allocated/read bytes.

### 2.5 A normal `post-commit.exe` is incorrectly subjected to the script-retention limit

The extensionless hook must be retained byte-for-byte because ELIOT may restore or chain it. The sibling `post-commit.exe` is never copied; ELIOT only chains the original path and records a digest. Applying the one-MiB retained-script cap to this executable rejects legitimate binaries unnecessarily.

The executable should be hashed as a stable opened regular-file snapshot, not loaded into memory or copied into ELIOT storage.

### 2.6 Git process timeout does not bound pipe-reader completion

`run_git` has a four-second child deadline, but after the direct child exits it performs unbounded joins of stdout/stderr reader threads. A descendant inheriting either pipe can hold EOF forever.

R38 must consume the bounded finite-process/capture primitive delivered by R35/#61. It must not create a third private capture implementation in `hooks/git.rs`.

## 3. Keep the current responsibility split

| Concern | Existing owner | R38 action |
|---|---|---|
| HookSource identity, client credential hash, project/registration authority, immutable commit facts | `store/hooks.rs` | Keep. Do not move local filesystem state into Store JSON or a new table. |
| Pending pre-IPC request identity and private source credential | `hooks/git.rs::prepare_source_credential` + pending descriptors | Keep as pre-Store intent. Clarify when those descriptors are deleted. |
| Local `.git/hooks` wrapper, preserved user hook and credential file | `hooks/git.rs` | Replace the current unphased manifest with one resumable v2 state machine. |
| Finite Git child/process/capture deadlines | R35/#61, `swarm-process` seam | Reuse after rebase. No local thread/join loop. |
| Setup/revoke orchestration and CLI output | `main.rs::{hook_setup,hook_install}` | Route through one local reconcile function; delete ad-hoc compensation branches. |
| `git.post_commit` event delivery | existing wrapper + `hook_emit_with_retry` + Store `hooks::emit` | Preserve. R38 does not redesign event delivery or automation intake. |

## 4. One private v2 installation record

Keep `install.json`; do not add a second transition file. Replace new writes of schema v1 with one private schema v2:

```rust
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum HookInstallPhase {
    Installing,
    Installed,
    Revoking,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InstallManifestV2 {
    schema_version: u32, // exact 2
    phase: HookInstallPhase,
    source_id: String,
    project_id: String,
    setup_request_digest: String,
    event: String,
    repository_path: String,
    git_executable: String,
    git_dir: String,
    hook_path: String,
    wrapper_sha256: String,
    previous_hook_path: Option<String>,
    previous_hook_sha256: Option<String>,
    previous_hook_mode: Option<u32>,
    previous_hook_was_active: bool,
    chained_hook_path: Option<String>,
    chained_hook_sha256: Option<String>,
    credential_path: String,
    installed_at_ms: Option<i64>,
}
```

Do not export this type from `hooks::contract` or `swarm-contracts`. It has one local filesystem consumer family.

### 4.1 Historical schema v1

Parse v1 through a separate retained decoder:

```text
valid v1 + exact wrapper/backup → LegacyInstalled
valid v1 + mismatch             → Modified/Damaged
```

New setup never writes v1. On explicit revoke, a valid v1 installation is atomically converted to v2 `Revoking` before the hook target is changed. Do not rewrite v1 merely because it was read.

No runtime union of two active writers is allowed.

### 4.2 One authoritative local loader

Add one private function used by apply, readback and revoke:

```rust
fn load_local_installation(
    plan_or_scope: &ValidatedHookPaths,
    source_id: &str,
) -> Result<LocalHookState>;
```

Suggested closed result:

```text
Absent
Installing(InstallManifestV2, LocalReadbackEvidence)
Installed(InstallManifestV2, LocalReadbackEvidence)
Revoking(InstallManifestV2, LocalReadbackEvidence)
LegacyInstalled(InstallManifestV1, LocalReadbackEvidence)
Modified(LocalReadbackEvidence)
```

The loader performs path/scope validation, bounded reads, wrapper digest, required backup digest and credential presence exactly once. `apply_post_commit`, `readback_post_commit` and `revoke_post_commit` must not reconstruct those predicates independently.

## 5. Durable file helpers: adapt existing code, do not import another adapter

### 5.1 Bounded regular-file read

Replace metadata-check-then-`fs::read` with one private helper:

```rust
fn read_regular_bounded(path: &Path, maximum: usize, code: &'static str) -> Result<Vec<u8>>;
```

Implementation shape already exists in `swarm-adapter-command::journal`:

```text
symlink_metadata: regular, non-symlink
File::open
take(maximum + 1)
read_to_end
reject len > maximum
```

Adapt the small sequence; do not add a dependency from kernel-host to an adapter. After a second core caller exists, a manager may move the helper to the narrow platform/process owner in a separate connected refactor.

Use it for:

- install manifest;
- pending descriptors;
- credential JSON;
- retained extensionless hook;
- wrapper readback.

A required backup read error is an error/damaged state. It is never represented as `None`.

### 5.2 Streamed digest for an existing binary

Add a separate private helper:

```rust
fn digest_regular_snapshot(path: &Path) -> Result<FileDigest>;
```

It:

- opens a non-symlink regular file;
- captures the opened file length;
- hashes exactly that many bytes with a fixed buffer;
- rejects short read or an additional byte;
- returns digest + byte length.

Use it for `post-commit.exe`, which is chained by path and not copied. Do not allocate the complete binary and do not apply the retained-script byte cap to it.

### 5.3 Durable JSON create/replace

Reuse the current local ingredients and the Command journal sequence:

```text
private temp create
write all
file.sync_all
atomic same-volume rename/MoveFileEx WRITE_THROUGH
Unix parent-directory sync
```

Add one private `write_manifest_new` and one private `replace_manifest`; do not create a generic filesystem transaction crate.

Every manifest phase transition is durable before the next external filesystem effect. Cleanup removals sync the parent directory on platforms where this is supported.

## 6. Setup algorithm

`hook_setup` keeps its current caller-owned request/source credential identity and Store setup call.

### 6.1 Before local install intent

The following definitive failures may still revoke/discard because no local wrapper intent exists:

- invalid Store response shape;
- Store source/project/repository identity mismatch;
- credential/source mismatch;
- stale preview before any v2 manifest was retained.

Reuse the current exact Store revoke and matching pending-descriptor cleanup.

### 6.2 Begin or resume local installation

Replace `apply_post_commit` with an idempotent function, for example:

```rust
pub fn reconcile_post_commit_install(
    plan: &HookInstallPlan,
    prepared: &PreparedHookSource,
    source: &HookSourceRecordOrProjection,
    swarm_executable: &Path,
) -> Result<HookInstallReadback>;
```

Ordered behavior:

1. re-run `preview_post_commit` and require the exact captured repository/Git/hook target/prior hook identity;
2. build exact wrapper bytes and manifest body;
3. if state is Absent:
   - preserve the extensionless prior hook with create-new/exact verify;
   - durably write v2 `Installing`;
4. if state is Installing:
   - require exact setup/source/plan/credential/wrapper identity;
   - continue, never create a new source/request/credential;
5. if target is the exact prior state, atomically install the wrapper;
6. if target already equals the wrapper, continue;
7. any unrelated target bytes → `HOOK_INSTALL_MODIFIED`; do not overwrite;
8. perform exact wrapper + backup readback;
9. atomically replace manifest phase with `Installed` and set `installed_at_ms`;
10. only after Installed is durable, call `complete_source_setup` to remove retry descriptors;
11. return installed readback.

### 6.3 Failure after intent

After `Installing` exists, do **not** automatically revoke Store authority and attempt multi-domain rollback.

Return a typed resumable error such as:

```text
HOOK_SETUP_RESUME_REQUIRED
source_id
client_request_id
local phase
```

The same `swarm hook setup` request resumes. Explicit `hook install revoke` performs revocation.

This is simpler and safer than a compensating transaction across SQLite and `.git` that has no atomic commit boundary.

## 7. Revoke algorithm

The CLI order remains:

```text
Store hook.source.revoke
→ local revoke reconcile
```

A revoked Store source makes any surviving wrapper inert before local restoration begins.

Replace `revoke_post_commit` with an idempotent reconcile:

1. load exact local state;
2. Absent: remove matching pending credential/descriptors if any; return absent/restored;
3. Installing/Installed/LegacyInstalled:
   - atomically retain/upgrade v2 phase `Revoking` before target modification;
4. Revoking: resume;
5. determine target state:
   - exact ELIOT wrapper → restore/remove;
   - exact prior extensionless hook already present → continue;
   - no prior hook and target already absent → continue;
   - anything else → `modified`, retain Revoking state and do not overwrite;
6. restore prior hook via same-directory temp + sync + atomic replace, or remove the ELIOT wrapper when no prior hook existed;
7. read back exact restored/absent target;
8. remove credential and matching pending descriptors;
9. remove preserved backup;
10. remove manifest last;
11. best-effort remove the now-empty source directory; a leftover empty directory is not installed authority.

A crash at any numbered point re-enters the same Revoking phase and continues. No step assumes that the previous function invocation completed.

## 8. Public readback states

Keep the current `HookInstallReadback` endpoint; extend/clarify its `state` vocabulary rather than adding a method:

```text
absent
installing
installed
revoking
modified
```

Rules:

- `installed` only when phase is Installed and wrapper + every required backup digest match;
- `installing`/`revoking` are explicit incomplete local states, never projected as installed;
- unreadable required evidence returns an error/damaged status, not `backup_matches=None` success;
- `credential_file_present` is observation, not authorization; Store source status remains separate;
- readback never mutates or attempts cleanup.

`hook_setup` and `hook_install revoke` are the only mutating local reconcilers.

## 9. Bounded Git child execution

R35/#61 owns the reusable finite-process/capture primitive. After that seam lands, replace the local `run_git` implementation with it.

Required options for the hook caller:

```text
stdin null
bounded stdout bytes
bounded stderr bytes
execution deadline
termination grace deadline
capture-drain deadline
exact process-family ownership
```

The result must distinguish:

```text
exit status
output limit
execution timeout
cleanup pending / family not departed
capture incomplete
```

Do not retain the current reader-thread joins as a fallback. Do not use PTY for Git readback.

R38 must rebase onto R35 for this final slice; parallel implementations of finite process capture are rejected.

## 10. Tests

### 10.1 Local state-machine tests

Use a temporary Git repository and real files. Do not mock only the final projection.

- `hook_install_crash_after_installing_manifest_resumes`
- `hook_install_crash_after_wrapper_before_phase_commit_resumes`
- `hook_install_same_request_is_idempotent`
- `hook_install_changed_plan_conflicts_without_overwrite`
- `hook_revoke_crash_after_restore_before_cleanup_resumes`
- `hook_revoke_crash_after_credential_removal_resumes`
- `hook_revoke_user_modified_target_is_never_overwritten`
- `hook_unreadable_required_backup_is_not_installed`
- `hook_growing_manifest_exceeds_read_bound`
- `hook_large_post_commit_exe_is_stream_hashed_not_copied`
- `hook_legacy_v1_installation_revokes_through_v2_intent`

Structure installation/revocation into small private phase functions so tests can stop after a phase and call the public reconcile again. Do not add production fault-injection flags.

### 10.2 Public orchestration tests

- Store source committed + local install fails → same request resumes; source/request identity unchanged;
- setup response lost → retry uses the same pending credential and source;
- explicit revoke first disables Store source, then restores local hook;
- local cleanup failure leaves Revoking readback and a retry completes it;
- no duplicate HookSource client or setup Operation;
- no `git.post_commit` event accepted after Store revocation.

### 10.3 Finite Git command tests after R35

- direct Git child exits while descendant holds stdout: bounded cleanup-pending/error, no hanging join;
- output cap is explicit, not clean empty output;
- timeout kills exact family and readback proves departure;
- no visible Windows console.

## 11. Deletion list

After all callers switch, delete:

- schema-v1 manifest writer;
- `apply_post_commit` one-shot/conflict implementation;
- non-resumable `revoke_post_commit` implementation;
- post-intent `rollback_hook_setup` compensation path;
- `fs::read(...).ok()` backup verification;
- metadata-check-then-unbounded-read helpers;
- local `run_git` reader-thread/join implementation after R35 helper adoption;
- duplicate wrapper/backup/path validation outside `load_local_installation`.

Retain:

- historical schema-v1 decoder until no installed v1 source exists;
- exact Store HookSource records and observations;
- private credential and setup request identity;
- user hook preservation and refusal to overwrite modified bytes.

## 12. Donors and exact boundaries

### Emdash

Take the lifecycle rule:

```text
durable intent/tombstone
→ idempotent host effect
→ observed host truth
→ remove tombstone
```

Here the v2 manifest phase is the local intent/tombstone; exact wrapper/backup readback is host truth. Do not import Emdash desktop database, workspace daemon or UI.

### Command journal

Adapt its temp/write/file-sync/rename/Unix-directory-sync sequence. Do not make kernel-host depend on `swarm-adapter-command`; do not copy its immutable no-clobber semantics where hook replacement requires an explicit phase transition.

### Goose validated input

Use one captured `HookInstallPlan`/validated local state through the effect. Do not repeatedly reinterpret raw user paths and do not import Goose scheduler.

### Ractor/systemd field semantics

Cleanup completion and readiness are explicit states. Timeout does not mean cleanup succeeded. No actor/service-manager dependency is added.

## 13. Ownership and dependencies

- R35/#61 lands bounded finite-child/capture semantics first; R38 consumes that seam for Git subprocesses.
- Store hook source authority remains in `store/hooks.rs`; R38 does not change generic automation intake.
- R34/#60 may later quarantine malformed hook facts, but it does not repair local Git installation.
- R33/R36/R37/#59 owns donor registry/playbook, not this production state machine.
- One manager/worktree. No parallel writer in `hooks/git.rs` or the hook sections of `main.rs`.

## 14. Minimal gate

After the connected code and deletion are complete:

```sh
cargo fmt --all -- --check
cargo clippy --locked -p swarm-process -p swarm-kernel-host -p swarm-cli --lib --bins -- -D warnings
```

Then the exact local-repository and process-family tests above. Broad workspace/native/model qualification remains the final phase.

## 15. Non-goals

- new hook type or veto-capable pre-commit hook;
- a generic filesystem transaction framework;
- a second Store table/ledger;
- automatic overwrite of a user-modified hook;
- auto-retry of `git.post_commit` under a new identity;
- global Git config or `core.hooksPath` mutation;
- loading the complete `post-commit.exe` into memory;
- preserving two active manifest writers;
- claiming readback success when required evidence is unreadable;
- broad cleanup of repository files outside the exact source directory and hook target.
