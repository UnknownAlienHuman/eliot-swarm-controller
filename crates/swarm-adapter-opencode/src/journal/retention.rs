// Queue entries and reverse references are volatile projections of validated
// operation journals. Persisted state contains traversal progress and a
// bounded removal-durability uncertainty bit; neither supplies receipt, ACK,
// reference, or deletion authority.
use super::{
    Error, HISTORY_LIMIT, Journal, Result, RetainedOperation, RetentionSummary, StateIdentity,
    inspect_retained_operation, operation_key, read_limited, read_operation_history_bytes,
    replace_private_durable, summarize_retained_operation, write_private_new,
};
use serde::{Deserialize, Serialize};
use sha2::Digest;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    ops::Bound::{Excluded, Unbounded},
    path::{Path, PathBuf},
    sync::{MutexGuard, atomic::Ordering},
};
use swarm_process::{RemovalDurability, RemovalOutcome, remove_private_acknowledged};

const MAX_RETENTION_STEPS: usize = 32;
const RETENTION_CURSOR_FILE: &str = "retention-cursor.json";
const RETENTION_CURSOR_VERSION: u32 = 3;
pub(super) const ADDRESS_CHECKPOINT_DIR: &str = "native-checkpoint-addresses";
pub(super) const ADDRESS_MEMO_VERSION: u32 = 1;
const ADDRESS_MEMO_LIMIT: usize = HISTORY_LIMIT * 4;
const RETENTION_CURSOR_LIMIT: usize = 1024;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct RetentionCursor {
    version: u32,
    binding_id: String,
    generation: i64,
    native_scope_key: String,
    route_sha256: String,
    cursor: Option<String>,
    #[serde(default)]
    removal_durability_unknown: Option<bool>,
    /// Bounded startup-inventory position in the same file: the last
    /// operation key fully classified this cycle, or None when the cycle
    /// is complete. Traversal-only like `cursor`; never deletion authority.
    #[serde(default)]
    startup_cursor: Option<String>,
    /// Read only to migrate v2 cursors. New cursors omit this field.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    unresolved_keys: Vec<String>,
    #[serde(default)]
    address_count: u64,
    #[serde(default)]
    address_digest: String,
    #[serde(default)]
    legacy_reclamation_pending: bool,
    /// The address memos have passed one complete legacy convergence.
    #[serde(default)]
    startup_complete: bool,
}

#[derive(Default)]
struct LoadedRetentionCursor {
    cursor: Option<String>,
    removal_durability_unknown: bool,
    startup_cursor: Option<String>,
    address_count: u64,
    address_digest: String,
    legacy_reclamation_pending: bool,
    startup_complete: bool,
}

/// Address-only metadata for one operation that is unresolved, referenced,
/// newly appended, or awaiting safe reclamation. It contains no outcome/ACK
/// authority; the operation journal remains authoritative.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct OperationAddressMemo {
    version: u32,
    binding_id: String,
    generation: i64,
    native_scope_key: String,
    route_sha256: String,
    operation_id: String,
    operation_key: String,
    references: Vec<String>,
    pub(super) native_root_ids: Vec<String>,
}

impl OperationAddressMemo {
    pub(super) fn new(
        identity: &StateIdentity,
        operation_id: String,
        operation_key: String,
        references: Vec<String>,
        native_root_ids: Vec<String>,
    ) -> Self {
        Self {
            version: ADDRESS_MEMO_VERSION,
            binding_id: identity.binding_id.clone(),
            generation: identity.generation,
            native_scope_key: identity.native_scope_key.clone(),
            route_sha256: identity.route_sha256.clone(),
            operation_id,
            operation_key,
            references,
            native_root_ids,
        }
    }
}

#[derive(Clone)]
struct IndexedOperation {
    retained: RetainedOperation,
}

pub(super) struct RetentionIndex {
    cursor_path: PathBuf,
    address_dir: PathBuf,
    identity: StateIdentity,
    cursor: Option<String>,
    entries: BTreeMap<String, IndexedOperation>,
    operation_keys: BTreeMap<String, String>,
    reverse_references: BTreeMap<String, BTreeSet<String>>,
    queue: BTreeSet<String>,
    ready: bool,
    references_unknown: bool,
    removal_durability_unknown: bool,
    startup_cursor: Option<String>,
    /// True until one accumulation cycle has classified every listed
    /// operation file. Deletion still requires a complete, damage-free
    /// index; an incomplete scan only holds, never authorises removal.
    scan_incomplete: bool,
    /// Durable mirror of `unresolved_keys`: the steady-state read list.
    /// Derived from per-operation address memos, never stored as a key vector
    /// in the traversal cursor.
    durable_unresolved: BTreeSet<String>,
    addresses: BTreeMap<String, OperationAddressMemo>,
    address_digest: [u8; 32],
    /// A bounded legacy name snapshot is reused across all body windows in
    /// one in-process convergence.
    startup_names: Option<Vec<String>>,
    startup_invalid_names: usize,
    classified_names: BTreeSet<String>,
    steady_seen: BTreeSet<String>,
    /// Safe legacy candidates are rediscovered in one legacy convergence
    /// after a crash until the bounded reclamation queue drains.
    legacy_reclamation_pending: bool,
    /// Distinguishes the completed legacy census on this handle from a fresh
    /// handle that still needs to rebuild its volatile reclamation queue.
    legacy_scan_complete_this_handle: bool,
    startup_complete: bool,
}

impl RetentionIndex {
    pub(super) fn open(root: &Path, address_dir: &Path, identity: &StateIdentity) -> Self {
        let cursor_path = root.join(RETENTION_CURSOR_FILE);
        let saved = read_cursor(&cursor_path, identity);
        let (addresses, address_digest, addresses_valid) =
            read_address_memos(address_dir, identity);
        let address_count = addresses.len() as u64;
        let saved_address_state_matches = saved.address_count == address_count
            && saved.address_digest == digest_hex(&address_digest);
        let startup_complete =
            saved.startup_complete && addresses_valid && saved_address_state_matches;
        let mut durable_unresolved = BTreeSet::new();
        for (key, memo) in &addresses {
            durable_unresolved.insert(key.clone());
            for reference in &memo.references {
                durable_unresolved.insert(operation_key(reference));
            }
        }
        Self {
            cursor_path,
            address_dir: address_dir.to_path_buf(),
            identity: identity.clone(),
            cursor: saved.cursor,
            entries: BTreeMap::new(),
            operation_keys: BTreeMap::new(),
            reverse_references: BTreeMap::new(),
            queue: BTreeSet::new(),
            ready: false,
            references_unknown: !addresses_valid,
            removal_durability_unknown: saved.removal_durability_unknown,
            startup_cursor: if addresses_valid && saved_address_state_matches {
                saved.startup_cursor
            } else {
                None
            },
            scan_incomplete: true,
            durable_unresolved,
            addresses,
            address_digest,
            startup_names: None,
            startup_invalid_names: 0,
            classified_names: BTreeSet::new(),
            steady_seen: BTreeSet::new(),
            legacy_reclamation_pending: saved.legacy_reclamation_pending,
            legacy_scan_complete_this_handle: false,
            startup_complete,
        }
    }

    fn begin_recovery(&mut self) {
        // A caller may request another bounded window while a legacy scan or
        // steady sparse pass is in progress. Preserve its accumulated index.
        if self.ready && self.scan_incomplete {
            return;
        }
        self.entries.clear();
        self.operation_keys.clear();
        self.reverse_references.clear();
        self.queue.clear();
        self.ready = false;
        self.references_unknown = false;
        // A fresh recovery scan must prove coverage again before deletion.
        self.scan_incomplete = true;
        self.legacy_scan_complete_this_handle = false;
        self.classified_names.clear();
        self.steady_seen.clear();
        if self.startup_complete && !self.legacy_reclamation_pending {
            self.startup_names = None;
        }
    }

    fn register(&mut self, retained: RetainedOperation) {
        let operation_id = retained.operation_id.clone();
        let key = operation_key(&operation_id);
        let mut affected = BTreeSet::from([operation_id.clone()]);

        if let Some(previous) = self.entries.remove(&key) {
            if previous.retained.operation_id != operation_id {
                self.references_unknown = true;
                self.entries.insert(key, previous);
                return;
            }
            self.queue.remove(&key);
            for target_id in &previous.retained.references {
                affected.insert(target_id.clone());
                self.remove_reference(target_id, &operation_id);
            }
        }

        if let Some(previous_key) = self
            .operation_keys
            .insert(operation_id.clone(), key.clone())
            && previous_key != key
        {
            self.references_unknown = true;
            self.operation_keys.insert(operation_id, previous_key);
            return;
        }

        for target_id in &retained.references {
            affected.insert(target_id.clone());
            self.reverse_references
                .entry(target_id.clone())
                .or_default()
                .insert(retained.operation_id.clone());
        }
        self.entries
            .insert(key.clone(), IndexedOperation { retained });
        affected.insert(operation_id);
        for operation_id in affected {
            self.sync_queue(&operation_id);
        }
    }

    fn remove_reference(&mut self, target_id: &str, source_id: &str) {
        let remove_target = if let Some(sources) = self.reverse_references.get_mut(target_id) {
            sources.remove(source_id);
            sources.is_empty()
        } else {
            false
        };
        if remove_target {
            self.reverse_references.remove(target_id);
        }
    }

    fn sync_queue(&mut self, operation_id: &str) {
        let Some(key) = self.operation_keys.get(operation_id).cloned() else {
            return;
        };
        // Keep blocked terminal records indexed, but do not keep a referenced
        // record in the reclaim queue. When its final reference is removed,
        // the source update/removal path calls sync_queue again.
        let eligible = self
            .entries
            .get(&key)
            .is_some_and(|entry| entry.retained.releasable && !entry.retained.result_source)
            && !self.has_references(operation_id);
        if eligible {
            self.queue.insert(key);
        } else {
            self.queue.remove(&key);
        }
    }

    fn has_references(&self, operation_id: &str) -> bool {
        self.reverse_references
            .get(operation_id)
            .is_some_and(|sources| !sources.is_empty())
    }

    fn next_candidate(&self, visited: &BTreeSet<String>) -> Option<String> {
        if let Some(cursor) = self.cursor.as_ref()
            && let Some(key) = self
                .queue
                .range((Excluded(cursor.clone()), Unbounded))
                .find(|key| !visited.contains(*key))
        {
            return Some(key.clone());
        }
        self.queue
            .iter()
            .find(|key| !visited.contains(*key))
            .cloned()
    }

    fn remove_operation(&mut self, key: &str) {
        self.queue.remove(key);
        if let Some(entry) = self.entries.remove(key) {
            let operation_id = entry.retained.operation_id;
            self.operation_keys.remove(&operation_id);
            for target_id in entry.retained.references {
                self.remove_reference(&target_id, &operation_id);
                self.sync_queue(&target_id);
            }
        }
    }

    fn cursor_record(&self) -> RetentionCursor {
        RetentionCursor {
            version: RETENTION_CURSOR_VERSION,
            binding_id: self.identity.binding_id.clone(),
            generation: self.identity.generation,
            native_scope_key: self.identity.native_scope_key.clone(),
            route_sha256: self.identity.route_sha256.clone(),
            cursor: self.cursor.clone(),
            removal_durability_unknown: Some(self.removal_durability_unknown),
            startup_cursor: self.startup_cursor.clone(),
            unresolved_keys: Vec::new(),
            address_count: self.addresses.len() as u64,
            address_digest: digest_hex(&self.address_digest),
            legacy_reclamation_pending: self.legacy_reclamation_pending,
            startup_complete: self.startup_complete,
        }
    }

    fn persist_cursor(&self) -> Result<()> {
        let bytes = serde_json::to_vec(&self.cursor_record())?;
        if bytes.len() > RETENTION_CURSOR_LIMIT {
            return Err(Error::new(
                "ADAPTER_RETENTION_STATE",
                "fixed retention cursor exceeds its byte limit",
            ));
        }
        if self.cursor_path.exists() {
            super::ensure_regular(&self.cursor_path, "retention cursor")?;
            replace_private_durable(&self.cursor_path, &bytes)?;
        } else {
            write_private_new(&self.cursor_path, &bytes)?;
        }
        super::ensure_regular(&self.cursor_path, "retention cursor")?;
        if read_limited(&self.cursor_path, RETENTION_CURSOR_LIMIT)? != bytes {
            return Err(Error::new(
                "ADAPTER_RETENTION_STATE",
                "retention cursor failed exact durable readback",
            ));
        }
        Ok(())
    }

    fn persist_address_memo(&mut self, memo: OperationAddressMemo) -> Result<()> {
        if memo.version != ADDRESS_MEMO_VERSION
            || memo.binding_id != self.identity.binding_id
            || memo.generation != self.identity.generation
            || memo.native_scope_key != self.identity.native_scope_key
            || memo.route_sha256 != self.identity.route_sha256
            || !valid_operation_key(&memo.operation_key)
            || memo.operation_key != operation_key(&memo.operation_id)
            || memo.references.iter().any(|id| id.is_empty())
        {
            return Err(Error::new(
                "ADAPTER_RETENTION_STATE",
                "operation address memo identity is invalid",
            ));
        }
        let bytes = serde_json::to_vec(&memo)?;
        if bytes.len() > ADDRESS_MEMO_LIMIT {
            return Err(Error::new(
                "ADAPTER_RETENTION_STATE",
                "operation address memo exceeds its per-operation byte limit",
            ));
        }
        let path = self
            .address_dir
            .join(format!("{}.json", memo.operation_key));
        if path.exists() {
            super::ensure_regular(&path, "operation address memo")?;
            replace_private_durable(&path, &bytes)?;
        } else {
            write_private_new(&path, &bytes)?;
        }
        super::ensure_regular(&path, "operation address memo")?;
        if read_limited(&path, ADDRESS_MEMO_LIMIT)? != bytes {
            return Err(Error::new(
                "ADAPTER_RETENTION_STATE",
                "operation address memo failed exact durable readback",
            ));
        }
        let key = memo.operation_key.clone();
        self.address_digest =
            replace_address_fingerprint(self.address_digest, self.addresses.get(&key), &memo)?;
        self.addresses.insert(key.clone(), memo.clone());
        self.durable_unresolved.insert(key);
        for reference in &memo.references {
            self.durable_unresolved.insert(operation_key(reference));
        }
        Ok(())
    }

    fn ensure_address_closure(&mut self, memo: OperationAddressMemo) -> Result<()> {
        // The source record is the write-ahead authority for its outgoing
        // addresses. Persist it first, then materialize target address slots
        // so the closure survives if the source later resolves before delete.
        self.persist_address_memo(memo.clone())?;
        for reference in &memo.references {
            let target_key = operation_key(reference);
            if self.addresses.contains_key(&target_key) {
                continue;
            }
            self.persist_address_memo(OperationAddressMemo {
                version: ADDRESS_MEMO_VERSION,
                binding_id: self.identity.binding_id.clone(),
                generation: self.identity.generation,
                native_scope_key: self.identity.native_scope_key.clone(),
                route_sha256: self.identity.route_sha256.clone(),
                operation_id: reference.clone(),
                operation_key: target_key,
                references: Vec::new(),
                native_root_ids: Vec::new(),
            })?;
        }
        Ok(())
    }

    fn recompute_durable_unresolved(&mut self) {
        self.durable_unresolved.clear();
        for (key, memo) in &self.addresses {
            self.durable_unresolved.insert(key.clone());
            for reference in &memo.references {
                self.durable_unresolved.insert(operation_key(reference));
            }
        }
    }

    fn remove_address_memo(&mut self, key: &str) -> Result<()> {
        if !self.addresses.contains_key(key) {
            return Ok(());
        }
        let path = self.address_dir.join(format!("{key}.json"));
        let outcome = remove_private_acknowledged(&path)?;
        if outcome.removed || outcome.durability == RemovalDurability::Confirmed {
            self.addresses.remove(key);
            // Recompute from the remaining records after a confirmed namespace
            // removal. (Unknown durability is reported separately by the
            // operation journal deletion path.)
            self.address_digest = address_set_fingerprint(&self.addresses)?;
            self.recompute_durable_unresolved();
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn cursor_for_test(&self) -> Option<&str> {
        self.cursor.as_deref()
    }

    #[cfg(test)]
    pub(super) fn removal_durability_unknown_for_test(&self) -> bool {
        self.removal_durability_unknown
    }
    #[cfg(test)]
    pub(super) fn startup_cursor_for_test(&self) -> Option<&str> {
        self.startup_cursor.as_deref()
    }
}

fn read_cursor(path: &Path, identity: &StateIdentity) -> LoadedRetentionCursor {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return LoadedRetentionCursor::default();
        }
        Err(_) => return untrusted_cursor(),
        Ok(_) => {}
    }
    if super::ensure_regular(path, "retention cursor").is_err() {
        return untrusted_cursor();
    }
    let Ok(bytes) = read_limited(path, RETENTION_CURSOR_LIMIT) else {
        return untrusted_cursor();
    };
    let Ok(saved) = serde_json::from_slice::<RetentionCursor>(&bytes) else {
        return untrusted_cursor();
    };
    if saved.binding_id != identity.binding_id
        || saved.generation != identity.generation
        || saved.native_scope_key != identity.native_scope_key
        || saved.route_sha256 != identity.route_sha256
        || saved
            .cursor
            .as_deref()
            .is_some_and(|key| !valid_operation_key(key))
    {
        return untrusted_cursor();
    }
    match (saved.version, saved.removal_durability_unknown) {
        // Versions 1 and 2 have no per-operation address checkpoint. Keep the
        // deletion cursor, but rebuild startup coverage before trusting them.
        (1 | 2, _) => LoadedRetentionCursor {
            cursor: saved.cursor,
            removal_durability_unknown: saved.removal_durability_unknown.unwrap_or(true),
            startup_cursor: None,
            address_count: 0,
            address_digest: String::new(),
            legacy_reclamation_pending: true,
            startup_complete: false,
        },
        (RETENTION_CURSOR_VERSION, Some(removal_durability_unknown)) => LoadedRetentionCursor {
            cursor: saved.cursor,
            removal_durability_unknown,
            startup_cursor: saved.startup_cursor.filter(|key| valid_operation_key(key)),
            address_count: saved.address_count,
            address_digest: saved.address_digest.clone(),
            legacy_reclamation_pending: saved.legacy_reclamation_pending,
            startup_complete: saved.startup_complete
                && saved.unresolved_keys.is_empty()
                && valid_digest(&saved.address_digest),
        },
        // An existing malformed, incomplete, or future cursor must never be
        // interpreted as a clean durability disposition.
        _ => untrusted_cursor(),
    }
}

fn untrusted_cursor() -> LoadedRetentionCursor {
    LoadedRetentionCursor {
        cursor: None,
        removal_durability_unknown: true,
        startup_cursor: None,
        address_count: 0,
        address_digest: String::new(),
        legacy_reclamation_pending: true,
        startup_complete: false,
    }
}

fn valid_operation_key(key: &str) -> bool {
    key.len() == 64 && key.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn digest_hex(digest: &[u8; 32]) -> String {
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn memo_fingerprint(memo: &OperationAddressMemo) -> Result<[u8; 32]> {
    let bytes = serde_json::to_vec(memo)?;
    let mut hasher = sha2::Sha256::new();
    hasher.update(b"eliot-opencode-address-memo-v1\0");
    hasher.update(bytes);
    Ok(hasher.finalize().into())
}

fn xor_digest(mut digest: [u8; 32], fingerprint: [u8; 32]) -> [u8; 32] {
    for (left, right) in digest.iter_mut().zip(fingerprint) {
        *left ^= right;
    }
    digest
}

fn replace_address_fingerprint(
    digest: [u8; 32],
    previous: Option<&OperationAddressMemo>,
    next: &OperationAddressMemo,
) -> Result<[u8; 32]> {
    let digest = match previous {
        Some(previous) => xor_digest(digest, memo_fingerprint(previous)?),
        None => digest,
    };
    Ok(xor_digest(digest, memo_fingerprint(next)?))
}

fn address_set_fingerprint(addresses: &BTreeMap<String, OperationAddressMemo>) -> Result<[u8; 32]> {
    let mut digest = [0; 32];
    for memo in addresses.values() {
        digest = xor_digest(digest, memo_fingerprint(memo)?);
    }
    Ok(digest)
}

fn read_address_memos(
    address_dir: &Path,
    identity: &StateIdentity,
) -> (BTreeMap<String, OperationAddressMemo>, [u8; 32], bool) {
    let mut addresses = BTreeMap::new();
    let mut digest = [0; 32];
    let entries = match fs::read_dir(address_dir) {
        Ok(entries) => entries,
        Err(_) => return (addresses, digest, false),
    };
    let mut valid = true;
    for entry in entries {
        let Ok(entry) = entry else {
            valid = false;
            continue;
        };
        let path = entry.path();
        let Some(key) = path.file_stem().and_then(|value| value.to_str()) else {
            valid = false;
            continue;
        };
        if !valid_operation_key(key)
            || path.extension().and_then(|value| value.to_str()) != Some("json")
            || super::ensure_regular(&path, "operation address memo").is_err()
        {
            valid = false;
            continue;
        }
        let Ok(bytes) = read_limited(&path, ADDRESS_MEMO_LIMIT) else {
            valid = false;
            continue;
        };
        let Ok(memo) = serde_json::from_slice::<OperationAddressMemo>(&bytes) else {
            valid = false;
            continue;
        };
        if memo.version != ADDRESS_MEMO_VERSION
            || memo.binding_id != identity.binding_id
            || memo.generation != identity.generation
            || memo.native_scope_key != identity.native_scope_key
            || memo.route_sha256 != identity.route_sha256
            || memo.operation_key != key
            || operation_key(&memo.operation_id) != key
        {
            valid = false;
            continue;
        }
        match memo_fingerprint(&memo) {
            Ok(fingerprint) => digest = xor_digest(digest, fingerprint),
            Err(_) => {
                valid = false;
                continue;
            }
        }
        addresses.insert(key.to_owned(), memo);
    }
    (addresses, digest, valid)
}

fn lock_index(journal: &Journal) -> Result<MutexGuard<'_, RetentionIndex>> {
    journal
        .retention
        .lock()
        .map_err(|_| Error::new("ADAPTER_RETENTION_STATE", "retention index is unavailable"))
}

pub(super) fn begin_recovery(journal: &Journal) -> Result<()> {
    lock_index(journal)?.begin_recovery();
    Ok(())
}

pub(super) fn register_operation(journal: &Journal, retained: RetainedOperation) -> Result<()> {
    lock_index(journal)?.register(retained);
    Ok(())
}

pub(super) fn refresh_operation_address(
    journal: &Journal,
    memo: OperationAddressMemo,
) -> Result<()> {
    let mut index = lock_index(journal)?;
    index.ensure_address_closure(memo)?;
    index.persist_cursor()?;
    Ok(())
}

pub(super) fn mark_damaged(journal: &Journal) -> Result<()> {
    lock_index(journal)?.references_unknown = true;
    Ok(())
}

pub(super) fn finish_recovery(journal: &Journal) -> Result<()> {
    lock_index(journal)?.ready = true;
    Ok(())
}
/// Bounded startup-inventory progress in the shared cursor file: the last
/// classified operation key and whether the current accumulation still
/// misses listed files. One lock per call; callers persist once per pass.
/// Selection is cyclic after the persisted position and skips already
/// registered keys, so repeated bounded calls partition the listing with
/// no overlap and the pending tail resumes across restarts.
pub(super) fn startup_window(
    journal: &Journal,
    names: &[String],
    budget: usize,
) -> Result<Vec<String>> {
    let index = lock_index(journal)?;
    Ok(select_startup_window(&index, names, budget))
}

pub(super) fn remember_startup_names(
    journal: &Journal,
    names: Vec<String>,
    invalid: usize,
) -> Result<()> {
    let mut index = lock_index(journal)?;
    if index.startup_names.is_none() {
        index.startup_names = Some(names);
        index.startup_invalid_names = invalid;
        index.startup_complete = false;
        // A validated cursor resumes the sorted legacy listing after restart.
        // Appends during an incomplete scan invalidate it before their journal
        // frame is written.
        index.classified_names.clear();
        if invalid > 0 {
            index.references_unknown = true;
        }
    }
    Ok(())
}

pub(super) fn cached_startup_names(journal: &Journal) -> Result<Option<(Vec<String>, usize)>> {
    let index = lock_index(journal)?;
    Ok(index
        .startup_names
        .clone()
        .map(|names| (names, index.startup_invalid_names)))
}

pub(super) fn mark_startup_classified(journal: &Journal, key: &str) -> Result<()> {
    lock_index(journal)?.classified_names.insert(key.to_owned());
    Ok(())
}

fn select_startup_window(index: &RetentionIndex, names: &[String], budget: usize) -> Vec<String> {
    let start = match index.startup_cursor.as_deref() {
        None => 0,
        Some(cursor) => names.partition_point(|name| name.as_str() <= cursor),
    };
    names[start..]
        .iter()
        .chain(names[..start].iter())
        .filter(|name| !index.classified_names.contains(*name))
        .take(budget.max(1))
        .cloned()
        .collect()
}

pub(super) fn scan_covered(journal: &Journal, names: &[String]) -> Result<bool> {
    let index = lock_index(journal)?;
    Ok(names.iter().all(|name| {
        index
            .startup_cursor
            .as_deref()
            .is_some_and(|cursor| name.as_str() <= cursor)
            || index.classified_names.contains(name)
    }))
}

pub(super) fn store_startup_progress(
    journal: &Journal,
    cursor: Option<String>,
    complete: bool,
    legacy: bool,
) -> Result<()> {
    let mut index = lock_index(journal)?;
    index.startup_cursor = cursor;
    if complete {
        if index.references_unknown {
            index.scan_incomplete = true;
            index.legacy_scan_complete_this_handle = legacy;
            index.persist_cursor()?;
            return Err(Error::new(
                "ADAPTER_STARTUP_RECOVERY_INCOMPLETE",
                "startup inventory contains an unclassified operation address",
            ));
        }
        // Publish sparse per-operation addresses before the fixed cursor can
        // claim that the legacy inventory is complete.
        snapshot_attention(&mut index);
        persist_attention_memos(&mut index)?;
        index.startup_complete = true;
        index.legacy_reclamation_pending = index
            .queue
            .iter()
            .any(|key| !index.durable_unresolved.contains(key));
        if legacy {
            index.startup_names = None;
            index.classified_names.clear();
            index.legacy_scan_complete_this_handle = true;
            // A legacy census may have overlapped a write-ahead address memo
            // that was appended after its name snapshot. Keep recovery open
            // until the existing sparse closure has read every such address.
            index.scan_incomplete = !index
                .durable_unresolved
                .iter()
                .all(|key| index.steady_seen.contains(key));
        } else {
            index.scan_incomplete = !index
                .durable_unresolved
                .iter()
                .all(|key| index.steady_seen.contains(key));
        }
    } else {
        index.scan_incomplete = true;
    }
    index.persist_cursor()?;
    Ok(())
}

/// Fold every entry needing attention into the durable set: entries that
/// are not releasable plus targets referenced by other entries. Stale
/// keys (resolved and drained) leave only through reclaim deletion or an
/// explicit missing-file drop, so the set stays proportional to current
/// work rather than lifetime history.
fn snapshot_attention(index: &mut RetentionIndex) {
    let mut attention = Vec::new();
    let mut references = Vec::new();
    for (key, entry) in &index.entries {
        let referenced = index
            .reverse_references
            .get(&entry.retained.operation_id)
            .is_some_and(|sources| !sources.is_empty());
        if !entry.retained.releasable
            || entry.retained.result_source
            || entry.retained.pending_delivery
            || referenced
        {
            attention.push(key.clone());
            references.extend(entry.retained.references.iter().cloned());
        }
    }
    for key in attention {
        index.durable_unresolved.insert(key);
    }
    for reference in references {
        index.durable_unresolved.insert(operation_key(&reference));
    }
}

fn persist_attention_memos(index: &mut RetentionIndex) -> Result<()> {
    let keys: Vec<String> = index.durable_unresolved.iter().cloned().collect();
    for key in keys {
        let Some(entry) = index.entries.get(&key).cloned() else {
            continue;
        };
        let retained = entry.retained;
        let memo = OperationAddressMemo {
            version: ADDRESS_MEMO_VERSION,
            binding_id: index.identity.binding_id.clone(),
            generation: index.identity.generation,
            native_scope_key: index.identity.native_scope_key.clone(),
            route_sha256: index.identity.route_sha256.clone(),
            operation_id: retained.operation_id,
            operation_key: key,
            references: retained.references,
            native_root_ids: retained.native_root_ids,
        };
        index.ensure_address_closure(memo)?;
    }
    Ok(())
}

pub(super) fn operation_address_memos(journal: &Journal) -> Result<Vec<OperationAddressMemo>> {
    Ok(lock_index(journal)?.addresses.values().cloned().collect())
}

pub(super) fn durable_startup_complete(journal: &Journal) -> Result<bool> {
    Ok(lock_index(journal)?.startup_complete)
}

pub(super) fn legacy_reclamation_pending(journal: &Journal) -> Result<bool> {
    Ok(lock_index(journal)?.legacy_reclamation_pending)
}

pub(super) fn legacy_scan_complete_this_handle(journal: &Journal) -> Result<bool> {
    Ok(lock_index(journal)?.legacy_scan_complete_this_handle)
}

pub(super) fn startup_inventory_required(journal: &Journal) -> Result<bool> {
    let index = lock_index(journal)?;
    Ok(!index.startup_complete
        || (index.legacy_reclamation_pending && !index.legacy_scan_complete_this_handle))
}

pub(super) fn steady_window_keys(journal: &Journal, budget: usize) -> Result<Vec<String>> {
    let index = lock_index(journal)?;
    Ok(index
        .durable_unresolved
        .iter()
        .filter(|key| !index.steady_seen.contains(*key))
        .take(budget.max(1))
        .cloned()
        .collect())
}

pub(super) fn mark_steady_visited(journal: &Journal, key: &str) -> Result<()> {
    lock_index(journal)?.steady_seen.insert(key.to_owned());
    Ok(())
}

pub(super) fn steady_coverage_complete(journal: &Journal) -> Result<bool> {
    let index = lock_index(journal)?;
    Ok(index
        .durable_unresolved
        .iter()
        .all(|key| index.steady_seen.contains(key)))
}

pub(super) fn startup_has_unknown_addresses(journal: &Journal) -> Result<bool> {
    Ok(lock_index(journal)?.references_unknown)
}

/// Persist the operation's own address and outgoing reference targets before
/// its journal bytes are appended. A crash can leave phantoms, never a live
/// operation without an address memo.
pub(super) fn note_operation_append(journal: &Journal, memo: OperationAddressMemo) -> Result<()> {
    let mut index = lock_index(journal)?;
    index.ensure_address_closure(memo)?;
    // The per-operation memo is the write-ahead address for this append.
    // Keep the legacy traversal cursor and cached name snapshot intact; after
    // the census, the existing durable closure reads this exact key. Rewinding
    // the whole inventory here lets steady command traffic starve convergence.
    if !index.startup_complete {
        index.scan_incomplete = true;
    }
    index.persist_cursor()?;
    Ok(())
}

/// Drop a durable key whose file is gone with no pending delivery.
/// Missing live work always holds elsewhere; this only forgets phantoms.
pub(super) fn drop_durable_key(journal: &Journal, key: &str) -> Result<()> {
    let mut index = lock_index(journal)?;
    let incoming_reference = index.addresses.values().any(|memo| {
        memo.references
            .iter()
            .any(|reference| operation_key(reference) == key)
    });
    if incoming_reference {
        index.references_unknown = true;
        index.scan_incomplete = true;
        return Ok(());
    }
    if index.addresses.contains_key(key) {
        index.remove_address_memo(key)?;
        index.persist_cursor()?;
    }
    Ok(())
}

pub(super) fn scan_incomplete(journal: &Journal) -> Result<bool> {
    Ok(lock_index(journal)?.scan_incomplete)
}

pub(super) fn refresh_operation(journal: &Journal, operation_id: &str) {
    if refresh_operation_inner(journal, operation_id).is_err() {
        let _ = mark_damaged(journal);
    }
}

fn refresh_operation_inner(journal: &Journal, operation_id: &str) -> Result<()> {
    let path = journal.operation_path(operation_id);
    super::ensure_regular(&path, "operation journal")?;
    let observed_bytes = read_limited(&path, HISTORY_LIMIT)?;
    let history = read_operation_history_bytes(&observed_bytes, Some(operation_id))?;
    let pending_delivery = journal.operation_has_pending_delivery(operation_id)?;
    let retained = summarize_retained_operation(
        path,
        &observed_bytes,
        &history,
        &journal.identity.binding_id,
        journal.identity.generation,
        &journal.identity.native_scope_key,
        pending_delivery,
    )?;
    register_operation(journal, retained.clone())?;
    refresh_operation_address(
        journal,
        OperationAddressMemo {
            version: ADDRESS_MEMO_VERSION,
            binding_id: journal.identity.binding_id.clone(),
            generation: journal.identity.generation,
            native_scope_key: journal.identity.native_scope_key.clone(),
            route_sha256: journal.identity.route_sha256.clone(),
            operation_id: retained.operation_id.clone(),
            operation_key: operation_key(&retained.operation_id),
            references: retained.references,
            native_root_ids: retained.native_root_ids,
        },
    )
}
/// Extend an incomplete startup accumulation by one bounded window.
/// Register-only: classification mirrors the recovery scan, but pending
/// deliveries are left for the recovery window's turn and found unknowns
/// are only counted. Entries that cannot be releasable (unacked, torn,
/// damaged) stay retained either way, so deferring publication can only
/// delay delivery, never authorise deletion. Caller holds the index lock;
/// no second lock is taken here.
fn extend_startup_scan(journal: &Journal, index: &mut RetentionIndex, budget: usize) -> Result<()> {
    if index.startup_names.is_none() {
        let (names, invalid) = journal.list_operation_names()?;
        journal
            .scan_listed
            .fetch_add(names.len() as u64 + invalid as u64, Ordering::Relaxed);
        if invalid > 0 {
            index.references_unknown = true;
        }
        index.startup_invalid_names = invalid;
        index.startup_names = Some(names);
        index.startup_complete = false;
        index.startup_cursor = None;
        index.classified_names.clear();
    }
    let names = index.startup_names.clone().unwrap_or_default();
    let window = select_startup_window(index, &names, budget);
    for name in &window {
        index.classified_names.insert(name.clone());
        let path = journal.operations.join(format!("{name}.jsonl"));
        super::ensure_regular(&path, "operation journal")?;
        let observed_bytes = read_limited(&path, HISTORY_LIMIT)?;
        journal.scan_bodies.fetch_add(1, Ordering::Relaxed);
        journal
            .scan_bytes
            .fetch_add(observed_bytes.len() as u64, Ordering::Relaxed);
        let history = match read_operation_history_bytes(&observed_bytes, None) {
            Ok(history) => history,
            Err(error) if error.code == "ADAPTER_JOURNAL" => {
                index.references_unknown = true;
                index.startup_cursor = Some(name.clone());
                continue;
            }
            Err(error) => return Err(error),
        };
        let known_operation_id = history
            .intent
            .as_ref()
            .map(|intent| intent.operation_id.as_str())
            .or_else(|| {
                history
                    .outcome
                    .as_ref()
                    .and_then(|outcome| outcome["operation_id"].as_str())
            })
            .or_else(|| {
                history
                    .result_params
                    .as_ref()
                    .and_then(|params| params["operation_id"].as_str())
            });
        let pending_delivery = match known_operation_id {
            Some(operation_id) => match journal.operation_has_pending_delivery(operation_id) {
                Ok(pending) => pending,
                Err(_) => {
                    index.references_unknown = true;
                    true
                }
            },
            None => {
                index.references_unknown = true;
                true
            }
        };
        match summarize_retained_operation(
            path,
            &observed_bytes,
            &history,
            &journal.identity.binding_id,
            journal.identity.generation,
            &journal.identity.native_scope_key,
            pending_delivery,
        ) {
            Ok(retained) => {
                journal.check_native_root_identity(&retained)?;
                index.register(retained);
            }
            Err(_) => index.references_unknown = true,
        }
        if history.recovery_unknown() || history.torn_tail_evidence.is_some() {
            journal.scan_unresolved.fetch_add(1, Ordering::Relaxed);
        }
        index.startup_cursor = Some(name.clone());
    }
    if names
        .iter()
        .all(|name| index.classified_names.contains(name))
    {
        if index.references_unknown {
            index.scan_incomplete = true;
            index.persist_cursor()?;
            return Err(Error::new(
                "ADAPTER_STARTUP_RECOVERY_INCOMPLETE",
                "legacy startup inventory contains damaged operation state",
            ));
        }
        snapshot_attention(index);
        persist_attention_memos(index)?;
        index.startup_complete = true;
        index.legacy_reclamation_pending = index
            .queue
            .iter()
            .any(|key| !index.durable_unresolved.contains(key));
        index.scan_incomplete = false;
        index.startup_names = None;
        index.classified_names.clear();
    } else {
        index.scan_incomplete = true;
    }
    index.persist_cursor()?;
    Ok(())
}

pub(super) fn reclaim_acknowledged(
    journal: &Journal,
    binding_id: &str,
    generation: i64,
    scope: &str,
) -> Result<RetentionSummary> {
    reclaim_acknowledged_with_remover(
        journal,
        binding_id,
        generation,
        scope,
        remove_private_acknowledged,
    )
}

pub(super) fn reclaim_acknowledged_with_remover(
    journal: &Journal,
    binding_id: &str,
    generation: i64,
    scope: &str,
    remove: impl Fn(&Path) -> Result<RemovalOutcome>,
) -> Result<RetentionSummary> {
    if binding_id != journal.identity.binding_id
        || generation != journal.identity.generation
        || scope != journal.identity.native_scope_key
    {
        return Err(Error::new(
            "ADAPTER_STATE_IDENTITY_MISMATCH",
            "retention request differs from the journal owner identity",
        ));
    }

    let mut index = lock_index(journal)?;
    if !index.ready {
        return Err(Error::new(
            "ADAPTER_RETENTION_STATE",
            "retention index has not completed the existing recovery scan",
        ));
    }
    if index.references_unknown {
        return Ok(RetentionSummary {
            durability_unknown: index.removal_durability_unknown,
            ..RetentionSummary::default()
        });
    }
    // Incomplete startup accumulation holds deletion while it extends the
    // index by one more bounded window. The queue stays drained=false so
    // the command loop retries; nothing is deleted without full coverage.
    if index.scan_incomplete {
        extend_startup_scan(
            journal,
            &mut index,
            crate::journal::MAX_STARTUP_SCAN_ENTRIES,
        )?;
        if index.scan_incomplete {
            return Ok(RetentionSummary {
                durability_unknown: index.removal_durability_unknown,
                ..RetentionSummary::default()
            });
        }
    }
    index.persist_cursor()?;

    let initial_work = index.queue.len();
    let step_limit = initial_work.min(MAX_RETENTION_STEPS);
    let mut visited = BTreeSet::new();
    let mut summary = RetentionSummary::default();
    while summary.examined < step_limit {
        let Some(key) = index.next_candidate(&visited) else {
            break;
        };
        visited.insert(key.clone());
        summary.examined += 1;

        let Some(indexed) = index.entries.get(&key).cloned() else {
            index.queue.remove(&key);
            index.cursor = Some(key);
            index.persist_cursor()?;
            continue;
        };
        let pending_delivery =
            journal.operation_has_pending_delivery(&indexed.retained.operation_id)?;
        let operation = inspect_retained_operation(
            indexed.retained.path.clone(),
            binding_id,
            generation,
            scope,
            pending_delivery,
        )?;
        if operation.operation_id != indexed.retained.operation_id
            || operation.observed_sha256 != indexed.retained.observed_sha256
            || operation_key(&operation.operation_id) != key
            || operation.path.parent() != Some(journal.operations.as_path())
        {
            return Err(Error::new(
                "ADAPTER_JOURNAL",
                "operation journal changed during retention classification",
            ));
        }

        if !operation.releasable
            || operation.result_source
            || pending_delivery
            || index.has_references(&operation.operation_id)
        {
            index.register(operation);
            index.cursor = Some(key);
            index.persist_cursor()?;
            continue;
        }

        journal.checkpoint_operation_root(
            operation.native_root_id.as_deref(),
            operation.native_scope_key.as_deref(),
            binding_id,
            generation,
            scope,
        )?;
        let current_bytes = read_limited(&operation.path, HISTORY_LIMIT)?;
        if format!("{:x}", sha2::Sha256::digest(current_bytes.as_slice()))
            != operation.observed_sha256
        {
            return Err(Error::new(
                "ADAPTER_JOURNAL",
                "operation journal changed during retention classification",
            ));
        }
        // Publish uncertainty before unlinking. A crash after namespace
        // removal but before its returned disposition is recorded can then
        // only leave a conservative Unknown marker, never a false confirmation.
        let previous_durability_unknown = index.removal_durability_unknown;
        index.removal_durability_unknown = true;
        index.persist_cursor()?;
        let outcome = match remove(&operation.path) {
            Ok(outcome) => outcome,
            Err(remove_error) => {
                if let Err(restore_error) = replace_private_durable(&operation.path, &current_bytes)
                {
                    return Err(Error::new(
                        "ADAPTER_JOURNAL_RESTORE",
                        format!(
                            "journal removal failed ({}) and exact restoration failed ({})",
                            remove_error.code, restore_error.code
                        ),
                    ));
                }
                index.removal_durability_unknown = previous_durability_unknown;
                if let Err(cursor_error) = index.persist_cursor() {
                    return Err(Error::new(
                        "ADAPTER_RETENTION_STATE",
                        format!(
                            "journal removal failed ({}) and uncertainty rollback could not be persisted ({})",
                            remove_error.code, cursor_error.code
                        ),
                    ));
                }
                return Err(remove_error);
            }
        };
        if outcome.removed {
            summary.namespace_removed += 1;
        }
        index.removal_durability_unknown =
            previous_durability_unknown || outcome.durability == RemovalDurability::Unknown;
        index.remove_operation(&key);
        index.remove_address_memo(&key)?;
        index.cursor = Some(key);
        index.persist_cursor()?;
    }
    summary.queue_drained = index.queue.is_empty();
    summary.durability_unknown = index.removal_durability_unknown;
    index.legacy_reclamation_pending = index
        .queue
        .iter()
        .any(|key| !index.durable_unresolved.contains(key));
    index.persist_cursor()?;
    Ok(summary)
}
