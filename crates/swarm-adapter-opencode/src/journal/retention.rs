// Queue entries and reverse references are volatile projections of validated
// operation journals. The only persisted value is a traversal cursor; it never
// supplies receipt, ACK, reference, or deletion authority.
use super::{
    Error, HISTORY_LIMIT, Journal, Result, RetainedOperation, RetentionSummary, StateIdentity,
    inspect_retained_operation, operation_key, read_limited, read_operation_history_bytes,
    remove_private_durable, replace_private_durable, summarize_retained_operation,
    write_private_new,
};
use serde::{Deserialize, Serialize};
use sha2::Digest;
use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Bound::{Excluded, Unbounded},
    path::{Path, PathBuf},
    sync::MutexGuard,
};

const MAX_RETENTION_STEPS: usize = 32;
const RETENTION_CURSOR_FILE: &str = "retention-cursor.json";
const RETENTION_CURSOR_VERSION: u32 = 1;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct RetentionCursor {
    version: u32,
    binding_id: String,
    generation: i64,
    native_scope_key: String,
    route_sha256: String,
    cursor: Option<String>,
}

#[derive(Clone)]
struct IndexedOperation {
    retained: RetainedOperation,
}

pub(super) struct RetentionIndex {
    cursor_path: PathBuf,
    identity: StateIdentity,
    cursor: Option<String>,
    entries: BTreeMap<String, IndexedOperation>,
    operation_keys: BTreeMap<String, String>,
    reverse_references: BTreeMap<String, BTreeSet<String>>,
    queue: BTreeSet<String>,
    ready: bool,
    references_unknown: bool,
}

impl RetentionIndex {
    pub(super) fn open(root: &Path, identity: &StateIdentity) -> Self {
        let cursor_path = root.join(RETENTION_CURSOR_FILE);
        let cursor = read_cursor(&cursor_path, identity);
        Self {
            cursor_path,
            identity: identity.clone(),
            cursor,
            entries: BTreeMap::new(),
            operation_keys: BTreeMap::new(),
            reverse_references: BTreeMap::new(),
            queue: BTreeSet::new(),
            ready: false,
            references_unknown: false,
        }
    }

    fn begin_recovery(&mut self) {
        self.entries.clear();
        self.operation_keys.clear();
        self.reverse_references.clear();
        self.queue.clear();
        self.ready = false;
        self.references_unknown = false;
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
        // Keep blocked terminal candidates indexed. A pass advances past an
        // exact outbox/reference deferral without authorizing its deletion.
        let eligible = self
            .entries
            .get(&key)
            .is_some_and(|entry| entry.retained.releasable && !entry.retained.result_source);
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
        }
    }

    fn persist_cursor(&self) -> Result<()> {
        let bytes = serde_json::to_vec(&self.cursor_record())?;
        if self.cursor_path.exists() {
            super::ensure_regular(&self.cursor_path, "retention cursor")?;
            replace_private_durable(&self.cursor_path, &bytes)?;
        } else {
            write_private_new(&self.cursor_path, &bytes)?;
        }
        super::ensure_regular(&self.cursor_path, "retention cursor")?;
        if read_limited(&self.cursor_path, 1024)? != bytes {
            return Err(Error::new(
                "ADAPTER_RETENTION_STATE",
                "retention cursor failed exact durable readback",
            ));
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn cursor_for_test(&self) -> Option<&str> {
        self.cursor.as_deref()
    }
}

fn read_cursor(path: &Path, identity: &StateIdentity) -> Option<String> {
    if !path.exists() || super::ensure_regular(path, "retention cursor").is_err() {
        return None;
    }
    let bytes = read_limited(path, 1024).ok()?;
    let saved: RetentionCursor = serde_json::from_slice(&bytes).ok()?;
    if saved.version != RETENTION_CURSOR_VERSION
        || saved.binding_id != identity.binding_id
        || saved.generation != identity.generation
        || saved.native_scope_key != identity.native_scope_key
        || saved.route_sha256 != identity.route_sha256
        || saved
            .cursor
            .as_deref()
            .is_some_and(|key| !valid_operation_key(key))
    {
        return None;
    }
    saved.cursor
}

fn valid_operation_key(key: &str) -> bool {
    key.len() == 64 && key.bytes().all(|byte| byte.is_ascii_hexdigit())
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

pub(super) fn mark_damaged(journal: &Journal) -> Result<()> {
    lock_index(journal)?.references_unknown = true;
    Ok(())
}

pub(super) fn finish_recovery(journal: &Journal) -> Result<()> {
    lock_index(journal)?.ready = true;
    Ok(())
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
    register_operation(journal, retained)
}

pub(super) fn reclaim_acknowledged(
    journal: &Journal,
    binding_id: &str,
    generation: i64,
    scope: &str,
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
            reclaimed: 0,
            examined: 0,
            complete: false,
        });
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
        match remove_private_durable(&operation.path) {
            Ok(true) => summary.reclaimed += 1,
            Ok(false) => {
                return Err(Error::new(
                    "ADAPTER_JOURNAL",
                    "acknowledged operation journal disappeared during retention",
                ));
            }
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
                return Err(remove_error);
            }
        }
        index.remove_operation(&key);
        index.cursor = Some(key);
        index.persist_cursor()?;
    }
    summary.complete = index.queue.is_empty();
    Ok(summary)
}
