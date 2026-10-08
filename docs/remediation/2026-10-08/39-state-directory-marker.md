# R39. State-directory marker: one crash-repairable locked marker for host and module owners

**Status:** implementation handoff. This branch changes one Markdown file only; production state directories and lock files are unchanged.

**Evidence baseline:** `40591a295af94b1541ec2ba30afe8e3247701a71` (`main`, 2026-10-08). Re-read the current implementation before coding and remove work that has already landed.

## 1. Result

Host `host.lock` and module `module.lock` are acquired through one small `swarm-process` primitive. Fresh initialization cannot leave a zero-byte or partial marker that permanently turns an otherwise empty dedicated state directory into `FOREIGN_STATE_DIRECTORY`.

```text
explicit dedicated directory
→ open/create exact marker
→ acquire OS lock
→ validate exact bytes
→ repair only an interrupted prefix in a marker-only directory
→ retain File handle for owner lifetime
```

No state-directory registry, database, daemon, migration or actor is added.

## 2. Current duplicated defect

Three paths currently implement essentially the same algorithm independently:

1. `crates/swarm-kernel-host/src/platform/mod.rs::DataRoot::acquire` (`host.lock`);
2. `crates/swarm-process/src/module_owner.rs::run_module_with_resolver` (`module.lock`);
3. legacy `crates/swarm-kernel-host/src/runtime/owner.rs::run` (`module.lock`).

They all:

```text
create directory
check empty / marker exists
OpenOptions create(empty)
acquire file lock
read marker
if empty && directory was empty: write marker in place
else require exact marker
```

Crash window:

```text
create marker file
→ process dies before write_all/sync_all
```

Next start observes:

```text
directory is not empty
marker file exists
marker contents are empty/partial
empty-directory flag is false
→ FOREIGN_STATE_DIRECTORY forever
```

The same occurs after a short/partial marker write. Manual deletion is the only recovery, even though the directory contains no state other than the interrupted marker.

This is not a malicious-user boundary. Both current comments already define the marker as coordination for an explicitly selected dedicated directory. Repair must nevertheless remain exact and must never adopt a directory containing other state.

## 3. One narrow primitive

Add `crates/swarm-process/src/state_marker.rs` and export only the following small boundary:

```rust
pub struct LockedStateMarker {
    file: std::fs::File,
}

impl LockedStateMarker {
    pub fn file(&self) -> &std::fs::File;
    pub fn into_file(self) -> std::fs::File;
}

#[derive(Debug)]
pub enum StateMarkerError {
    Busy(std::io::Error),
    ForeignDirectory,
    InvalidMarker,
    Io(std::io::Error),
}

pub fn acquire_state_marker(
    canonical_directory: &Path,
    file_name: &str,
    expected_marker: &[u8],
) -> std::result::Result<LockedStateMarker, StateMarkerError>;
```

The exact public names may follow existing crate style, but the semantics must remain this small. Do not add callbacks, generic lifecycle phases, JSON, Task IDs or restart policy to this primitive.

The caller maps neutral failures to its current public codes:

| Neutral result | DataRoot | Module owner |
|---|---|---|
| `Busy` | `HOST_ALREADY_RUNNING` | `MODULE_OWNER_ACTIVE` |
| `ForeignDirectory` / `InvalidMarker` | `FOREIGN_STATE_DIRECTORY` | `FOREIGN_STATE_DIRECTORY` |
| `Io` | existing typed I/O conversion | existing typed I/O conversion |

Thus R39 does not change external error vocabulary.

## 4. Exact algorithm

### 4.1 Validate caller-owned inputs

Before filesystem mutation:

- directory path is absolute/canonical and already created by the caller;
- `file_name` is one nonempty normal component with no separator, `.` or `..`;
- marker bytes are nonempty, at most 128 bytes, valid UTF-8 only if a caller needs text; the primitive itself compares bytes;
- marker path must be inside the selected directory.

Do not canonicalize a nonexistent marker into another path and do not follow a marker symlink/reparse point.

### 4.2 Initial directory classification

Read directory entries before create:

```text
marker absent + zero entries      → fresh candidate
marker absent + any entry         → ForeignDirectory
marker exists                     → open existing candidate
marker symlink/non-regular        → InvalidMarker
```

Do not treat a hidden temp, owner receipt, database or arbitrary file as an empty state directory.

### 4.3 Create/open under race

For a fresh candidate:

1. open marker with `read + write + create_new` and private file mode where supported;
2. if create returns AlreadyExists, reopen only the exact marker path and continue validation;
3. never use `create(true)`/overwrite;
4. apply private file permissions;
5. acquire the OS file lock immediately.

For an existing candidate:

1. reject symlink/reparse/non-regular marker;
2. open read/write without truncate/create;
3. acquire the OS lock.

Only after the lock is acquired may repair or exact validation occur. If another live process owns the marker, return `Busy`; do not inspect/repair its in-progress bytes.

### 4.4 Exact bytes or safe interrupted-prefix repair

Read at most `expected_marker.len() + 1` bytes from the locked file.

Cases:

| Stored bytes | Directory after lock | Result |
|---|---|---|
| exact expected marker | any valid existing state contents | success |
| empty or strict prefix of expected | marker is the only directory entry | repair exact marker, sync, success |
| empty/partial prefix | any other entry exists | invalid/foreign, no repair |
| extra byte, non-prefix or different marker | any | `InvalidMarker`, no write |

Repair:

```text
seek(0)
set_len(0)
write_all(expected)
file.sync_all()
Unix: sync parent directory
seek(0)
read back exact expected bytes
```

A short write interrupted again remains a prefix and is repairable on the next acquisition only when the marker is still the sole entry.

Do not accept arbitrary zero-byte markers in a nonempty existing state directory.

### 4.5 Ownership lifetime

Return the locked File through `LockedStateMarker`; dropping it releases the OS lock. The primitive does not spawn, stop or inspect processes.

DataRoot and module owner structs retain this value for the same lifetime they currently retain the raw `File`.

## 5. Caller migration

### 5.1 Host DataRoot

`DataRoot::acquire` keeps:

- `create_dir_all`;
- canonical data directory;
- private directory permissions;
- current `HOST_ALREADY_RUNNING` and foreign-directory errors;
- retained lock for Store lifetime.

Replace only the duplicated empty/create/read/write/verify block with `acquire_state_marker(path, "host.lock", b"ELIOT_SWARM_STATE_V1\n")`.

Do not regenerate `operator.json`, repair an existing database or infer ownership from `swarm.db`.

### 5.2 Generic module owner

`swarm_process::module_owner::run_module_with_resolver` calls:

```text
acquire_state_marker(dir, "module.lock", MARKER)
```

before reading `owner.json`, checkpoints or worker evidence.

All existing owner/departure/gap rules remain. R39 does not authorize checkpoint adoption or replacement.

### 5.3 Legacy kernel-host runtime owner

This path must not keep a third marker implementation.

Preferred integration:

- if R01 removes/replaces it before R39 rebase, delete the duplicate and do nothing;
- otherwise call the same `swarm-process` primitive temporarily;
- do not create a kernel-host wrapper that reimplements the algorithm.

The legacy executor’s eventual deletion remains R01/R14 ownership.

## 6. Private-file durability boundary

R39 does **not** turn `write_private_new` into a broad filesystem transaction API.

However the marker helper must sync the marker bytes and, on Unix, the selected directory after first creation/repair. This is a few private lines beside the primitive.

A later connected PR may strengthen `swarm_process::write_private_new` for all callers, but only after auditing those callers and adding a real durability test. Do not silently broaden R39 into every credential/receipt writer.

## 7. What to reuse

### Existing ELIOT code

- current OS file lock API (`File::try_lock`);
- `swarm_process::private_permissions`;
- current retained File ownership in DataRoot/module owner;
- Artifact/Command-journal discipline: create-new, exact readback, file sync, directory sync;
- current `reject_link_components`/canonical state-dir checks in module owner.

### Donor semantics

- systemd: ownership/readiness is explicit and retained for service lifetime; a marker is not process liveness itself;
- Emdash: mirror/intent is not host truth; exact host resource readback decides completion;
- Petri: stale generation/owner cannot publish into replacement state.

No donor dependency or copied service-manager code is needed.

## 8. Tests

Place shared primitive tests in `swarm-process`; caller tests verify error mapping.

### 8.1 Primitive tests

- `fresh_state_marker_is_written_locked_and_read_back`
- `zero_byte_marker_only_directory_is_repaired`
- `partial_expected_prefix_only_directory_is_repaired`
- `random_or_long_marker_is_rejected_without_write`
- `empty_marker_with_other_state_is_not_repaired`
- `symlink_marker_is_rejected`
- `second_owner_gets_busy_while_first_lock_is_held`
- `second_owner_succeeds_after_first_drop`
- `concurrent_fresh_acquire_creates_one_exact_marker`
- `marker_creation_and_repair_sync_parent_directory_on_unix`

The concurrency test must use two real file handles/threads or processes, not call the helper sequentially and label it a race.

### 8.2 Public caller tests

- DataRoot fresh directory succeeds and leaves exact marker;
- DataRoot recovers sole zero-byte/partial marker;
- DataRoot refuses the same marker when `swarm.db` or another state file is already present;
- module owner recovers interrupted sole marker but never adopts checkpoint/owner evidence without the existing exact checks;
- busy module marker maps to `MODULE_OWNER_ACTIVE`;
- busy host marker maps to `HOST_ALREADY_RUNNING`.

### 8.3 Mutation tests

The test suite must fail if:

- prefix check becomes arbitrary-empty acceptance;
- directory-only condition is removed;
- lock acquisition moves after repair;
- symlinks are followed;
- exact marker comparison accepts trailing bytes;
- the retained lock is dropped before caller lifetime.

## 9. Deletion list

After migration, delete the duplicated marker logic from:

- `platform::DataRoot::acquire`;
- `swarm_process::module_owner::run_module_with_resolver`;
- legacy `runtime::owner::run` if still present.

Also delete duplicate marker constants where the caller can pass a local constant directly. Keep public domain-specific error mapping and owner/departure logic.

The final change should be net-negative or close to neutral across the three callers despite adding focused tests.

## 10. Boundaries with other PRs

- R01/#27 owns module owner/worker identity, readiness, restart and legacy-owner deletion. R39 owns only marker acquisition/repair.
- R35/#61 owns process completion/capture, not state markers.
- R38/#62 owns Git hook filesystem lifecycle and uses a different phased manifest.
- R14/#40 owns frontend/module dependency direction; do not move marker semantics into MCP/CLI.

One manager/worktree. Coordinate rebases with R01 because both may touch `swarm-process::module_owner.rs`.

## 11. Minimal gate

After connected code:

```sh
cargo fmt --all -- --check
cargo clippy --locked -p swarm-process -p swarm-kernel-host --lib --bins -- -D warnings
```

Run only the named marker/DataRoot/module-owner tests in the implementation phase; broad workspace/native qualification remains final.

## 12. Non-goals

- repairing a nonempty foreign state directory;
- adopting unknown owner/checkpoint evidence;
- PID-based ownership;
- a state-directory database or registry;
- automatic restart;
- deleting arbitrary temp/state files;
- changing operator credential policy;
- making marker contents a schema for runtime state;
- another process supervisor or actor framework;
- weakening exact symlink/reparse checks.
