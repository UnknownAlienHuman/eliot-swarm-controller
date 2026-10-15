//! Small durable adapter receipt journal. It is scoped below the existing
//! module owner directory and never stores prompts, credentials, or SDK text.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Mutex,
};
use swarm_contracts::{
    error::{Error, Result},
    runtime::{ModuleReceiptIdentity, RuntimeOutcome},
};
use swarm_process::{
    JsonlScanVerdict, private_permissions, remove_private_acknowledged, replace_private_durable,
    scan_jsonl, write_private_new,
};

const MAX_RECORD_BYTES: usize = 1_048_576;
const MAX_STATE_BYTES: u64 = 16 * 1024 * 1024;
const MAX_RETENTION_METADATA_BYTES: usize = 1_048_576;
const MAX_CURSOR_STEPS: usize = 32;

const RETENTION_DIRECTORY: &str = "retention";
const RETENTION_STATE_FILE: &str = "state.json";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct StateIdentity {
    version: u32,
    binding_id: String,
    generation: i64,
    native_scope_key: String,
    route_sha256: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct JournalRecord {
    version: u32,
    operation_id: String,
    kind: String,
    #[serde(default)]
    receipt: Option<ModuleReceiptIdentity>,
    #[serde(default)]
    method: Option<String>,
    #[serde(default)]
    intent: Option<Value>,
    #[serde(default)]
    outcome: Option<Value>,
    #[serde(default)]
    digest: Option<String>,
    #[serde(default)]
    outbox_sequence: Option<u64>,
}

#[derive(Debug, Clone, Default)]
pub struct OperationState {
    /// Stable filename key used to locate this journal, including when a
    /// torn first frame does not reveal the operation ID.
    pub operation_key: String,
    pub operation_id: String,
    pub receipt: Option<ModuleReceiptIdentity>,
    pub method: Option<String>,
    pub intent: Option<Value>,
    pub outcome: Option<Value>,
    pub outcome_sha256: Option<String>,
    pub acknowledged_sha256: Option<String>,
    pub outbox_sequence: Option<u64>,
    pub compacted: bool,
    /// Evidence for an incomplete final frame. The validated prefix is
    /// retained, and callers must not replay or append to this journal.
    pub torn_tail_evidence: Option<TornTailEvidence>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TornTailEvidence {
    pub valid_prefix_bytes: usize,
    pub tail_sha256: String,
    pub tail_bytes: usize,
}

impl OperationState {
    /// A durable effect marker without a saved terminal outcome is unknown.
    pub fn recovery_unknown(&self) -> bool {
        self.receipt.is_some() && self.outcome.is_none()
    }

    /// A torn journal cannot authorize another native effect attempt.
    pub fn native_replay_permitted(&self) -> bool {
        self.receipt.is_none() && self.outcome.is_none() && self.torn_tail_evidence.is_none()
    }
}

fn is_current_boot_unknown(state: &OperationState, current_boot_id: Option<&str>) -> bool {
    state.recovery_unknown()
        && current_boot_id.is_some_and(|boot_id| {
            state
                .intent
                .as_ref()
                .and_then(|intent| intent["bridge_boot_id"].as_str())
                == Some(boot_id)
        })
}

pub struct OperationJournal {
    root: PathBuf,
    retention_root: PathBuf,
    startup_lock: Mutex<()>,
    startup_names: Mutex<Option<Vec<String>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct RetentionState {
    version: u32,
    next_outbox_sequence: u64,
    outbox_cursor_sequence: u64,
    next_cleanup_sequence: u64,
    cleanup_cursor_sequence: u64,
    migration_complete: bool,
    #[serde(default)]
    startup_complete: bool,
    #[serde(default)]
    startup_cursor: Option<String>,
    counters: RetentionCounters,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct RetentionCounters {
    examined: u64,
    compacted: u64,
    deferred_unknown: u64,
    deferred_unacknowledged: u64,
    deferred_accepted: u64,
    deferred_reference: u64,
    deferred_damage: u64,
    degraded: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum WorkPhase {
    Pending,
    Deferred,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct WorkPointer {
    version: u32,
    sequence: u64,
    operation_id: String,
    operation_key: String,
    phase: WorkPhase,
    outcome_sha256: Option<String>,
    acknowledged_sha256: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct WorkItem {
    version: u32,
    sequence: u64,
    operation_id: String,
    operation_key: String,
    outcome_sha256: Option<String>,
    acknowledged_sha256: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct OperationTombstone {
    version: u32,
    operation_id: String,
    operation_key: String,
    receipt: ModuleReceiptIdentity,
    method: String,
    intent: Value,
    outcome: Value,
    outcome_sha256: String,
    acknowledged_sha256: String,
    #[serde(default)]
    outbox_sequence: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct BootDispatchMarker {
    version: u32,
    operation_id: String,
    operation_key: String,
    binding_id: String,
    binding_generation: i64,
    input_sha256: String,
    bridge_boot_id: String,
    native_scope_key: String,
    native_root_id: Option<String>,
    native_input_id: Option<String>,
    native_payload_sha256: Option<String>,
    native_payload_bytes: Option<u64>,
    outcome_sha256: Option<String>,
    acknowledged_sha256: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct OperationReference {
    version: u32,
    target_operation_id: String,
    reference_operation_id: String,
}

impl OperationJournal {
    pub fn open(
        root: &Path,
        binding_id: &str,
        generation: i64,
        native_scope_key: &str,
        route_sha256: &str,
    ) -> Result<Self> {
        if !root.is_absolute() {
            return Err(Error::new(
                "ADAPTER_STATE",
                "state directory must be absolute",
            ));
        }
        fs::create_dir_all(root)
            .map_err(|_| Error::new("ADAPTER_STATE", "state directory cannot be created"))?;
        ensure_directory(root)?;
        private_permissions(root, true)?;
        let expected = StateIdentity {
            version: 1,
            binding_id: binding_id.to_owned(),
            generation,
            native_scope_key: native_scope_key.to_owned(),
            route_sha256: route_sha256.to_owned(),
        };
        let identity_path = root.join("state-identity.json");
        if identity_path.exists() {
            ensure_regular(&identity_path)?;
            let bytes = read_limited(&identity_path, MAX_RECORD_BYTES)?;
            let saved: StateIdentity = serde_json::from_slice(&bytes)
                .map_err(|_| Error::new("ADAPTER_STATE", "state identity is invalid"))?;
            if saved != expected {
                return Err(Error::new(
                    "ADAPTER_STATE_IDENTITY_MISMATCH",
                    "state directory belongs to another binding generation or native scope",
                ));
            }
        } else {
            let bytes = serde_json::to_vec(&expected)?;
            write_private_new(&identity_path, &bytes)
                .map_err(|_| Error::new("ADAPTER_STATE", "state identity cannot be created"))?;
        }
        let retention_root = ensure_child_directory(root, RETENTION_DIRECTORY)?;
        for name in [
            "outbox",
            "outbox/by-operation",
            "outbox/items",
            "cleanup",
            "cleanup/by-operation",
            "cleanup/items",
            "operations",
            "boots",
            "references",
        ] {
            ensure_relative_directory(&retention_root, name)?;
        }
        let journal = Self {
            root: root.to_owned(),
            retention_root,
            startup_lock: Mutex::new(()),
            startup_names: Mutex::new(None),
        };
        journal.ensure_retention_state()?;
        journal.prepare_startup_recovery()?;
        Ok(journal)
    }

    pub fn get(&self, operation_id: &str) -> Result<Option<OperationState>> {
        let path = self.path(operation_id)?;
        if !path.exists() {
            let tombstone_path = self.tombstone_path(operation_id)?;
            if !tombstone_path.exists() {
                return Ok(None);
            }
            let tombstone = self.read_tombstone_record(&tombstone_path, operation_id)?;
            return Ok(Some(state_from_tombstone(tombstone)));
        }
        ensure_regular(&path)?;
        let state = self.read_path(&path, Some(operation_id))?;
        let tombstone_path = self.tombstone_path(operation_id)?;
        if tombstone_path.exists() {
            let tombstone = self.read_tombstone_record(&tombstone_path, operation_id)?;
            if state.receipt.as_ref() != Some(&tombstone.receipt)
                || state.method.as_ref() != Some(&tombstone.method)
                || state.intent.as_ref().map(compact_intent).as_ref() != Some(&tombstone.intent)
                || state.outcome.as_ref().map(compact_outcome).as_ref() != Some(&tombstone.outcome)
                || state.outcome_sha256.as_deref() != Some(tombstone.outcome_sha256.as_str())
                || state.acknowledged_sha256.as_deref()
                    != Some(tombstone.acknowledged_sha256.as_str())
            {
                self.note_deferred_damage()?;
                return Err(Error::new(
                    "ADAPTER_RETENTION_MARKER_MISMATCH",
                    "compacted operation marker differs from its retained journal",
                ));
            }
        }
        if state.receipt.is_none() && state.torn_tail_evidence.is_none() {
            return Ok(None);
        }
        Ok(Some(state))
    }

    pub fn write_intent(
        &self,
        operation_id: &str,
        receipt: &ModuleReceiptIdentity,
        method: &str,
        intent: &Value,
    ) -> Result<()> {
        let _startup_guard = self.startup_lock.lock().map_err(|_| {
            Error::new(
                "ADAPTER_RETENTION_STATE",
                "startup journal recovery lock is unavailable",
            )
        })?;
        if let Some(previous) = self.get(operation_id)?
            && !previous.native_replay_permitted()
        {
            if let Some(tail) = previous.torn_tail_evidence.as_ref() {
                return Err(Error::new(
                    "ADAPTER_JOURNAL_RECOVERY_UNKNOWN",
                    format!(
                        "torn operation journal {}: valid_prefix_bytes={}, tail_bytes={}, tail_sha256={}",
                        previous.operation_key,
                        tail.valid_prefix_bytes,
                        tail.tail_bytes,
                        tail.tail_sha256
                    ),
                ));
            }
            if previous.recovery_unknown() {
                return Err(Error::new(
                    "ADAPTER_JOURNAL_RECOVERY_UNKNOWN",
                    "operation has a durable effect marker without a saved terminal outcome",
                ));
            }
            return Err(Error::new(
                "ADAPTER_INTENT_EXISTS",
                "operation already has a durable effect marker",
            ));
        }
        let record = JournalRecord {
            version: 1,
            operation_id: operation_id.to_owned(),
            kind: "intent".to_owned(),
            receipt: Some(receipt.clone()),
            method: Some(method.to_owned()),
            intent: Some(intent.clone()),
            outcome: None,
            digest: None,
            outbox_sequence: None,
        };
        // The startup cursor is traversal-only. A live append is already tied
        // to the exact Store Operation ID and the next boot reopens a completed
        // census; do not rewind an incomplete legacy walk on every new command.
        self.register_references_for_intent(operation_id, intent)?;
        if method == "task.dispatch" {
            self.write_boot_dispatch_marker(operation_id, receipt, intent, None, None)?;
        }
        self.append(operation_id, &record)
    }

    /// Re-read and retain an exact existing marker while recovering a result
    /// outcome. This path is deliberately separate from `write_intent`, whose
    /// rejection of existing markers prevents ordinary command handling from
    /// treating a duplicate as permission to repeat a native effect.
    pub fn retain_exact_intent_for_recovery(
        &self,
        operation_id: &str,
        receipt: &ModuleReceiptIdentity,
        method: &str,
        intent: &Value,
    ) -> Result<()> {
        let state = self.get(operation_id)?.ok_or_else(|| {
            Error::new(
                "ADAPTER_INTENT_MISSING",
                "result recovery has no retained operation intent",
            )
        })?;
        if state.torn_tail_evidence.is_some() {
            return Err(Error::new(
                "ADAPTER_JOURNAL_RECOVERY_UNKNOWN",
                "result recovery cannot extend a torn operation journal",
            ));
        }
        if state.receipt.as_ref() != Some(receipt)
            || state.method.as_deref() != Some(method)
            || state.intent.as_ref() != Some(intent)
        {
            return Err(Error::new(
                "ADAPTER_INTENT_CONFLICT",
                "result recovery differs from the exact retained operation marker",
            ));
        }
        Ok(())
    }

    pub fn save_outcome(&self, operation_id: &str, outcome: &RuntimeOutcome) -> Result<()> {
        self.save_outcome_value(operation_id, &serde_json::to_value(outcome)?)
    }

    pub fn save_outcome_value(&self, operation_id: &str, outcome: &Value) -> Result<()> {
        let current = self.get(operation_id)?.ok_or_else(|| {
            Error::new(
                "ADAPTER_INTENT_MISSING",
                "outcome has no durable effect marker",
            )
        })?;
        if outcome["operation_id"] != operation_id {
            return Err(Error::new(
                "ADAPTER_OUTCOME",
                "outcome names another operation",
            ));
        }
        if current.compacted {
            if current.outcome_sha256.as_deref() == Some(digest_json(outcome)?.as_str())
                && current.acknowledged_sha256 == current.outcome_sha256
            {
                return Ok(());
            }
            return Err(Error::new(
                "ADAPTER_OUTCOME_CONFLICT",
                "compacted operation cannot accept a replacement outcome",
            ));
        }
        if let Some(saved) = current.outcome.as_ref() {
            if saved == outcome {
                if current.acknowledged_sha256 != current.outcome_sha256 {
                    self.ensure_outbox_entry(operation_id, current.outcome_sha256.clone())?;
                }
                return Ok(());
            }
            return Err(Error::new(
                "ADAPTER_OUTCOME_CONFLICT",
                "operation outcome cannot be replaced",
            ));
        }
        let digest = digest_json(outcome)?;
        let sequence = self.ensure_outbox_entry(operation_id, Some(digest.clone()))?;
        let record = JournalRecord {
            version: 1,
            operation_id: operation_id.to_owned(),
            kind: "outcome".to_owned(),
            receipt: None,
            method: None,
            intent: None,
            outcome: Some(outcome.clone()),
            digest: Some(digest),
            outbox_sequence: Some(sequence),
        };
        self.append(operation_id, &record)
    }

    pub fn acknowledge(&self, operation_id: &str, outcome: &Value) -> Result<()> {
        let digest = digest_json(outcome)?;
        let mut current = self
            .get(operation_id)?
            .ok_or_else(|| Error::new("ADAPTER_OUTBOX", "acknowledged operation is missing"))?;
        if current.compacted {
            return if current.acknowledged_sha256.as_deref() == Some(digest.as_str()) {
                Ok(())
            } else {
                Err(Error::new(
                    "ADAPTER_OUTBOX",
                    "host acknowledgement differs from the compacted operation digest",
                ))
            };
        }
        if current.outcome.as_ref() != Some(outcome)
            || current.outcome_sha256.as_deref() != Some(digest.as_str())
        {
            return Err(Error::new(
                "ADAPTER_OUTBOX",
                "host acknowledgement differs from the exact saved outcome",
            ));
        }
        if current.torn_tail_evidence.is_some() {
            if !self.repair_torn_acknowledgement(operation_id, outcome)? {
                return Err(torn_ack_recovery_unknown());
            }
            current = self.get(operation_id)?.ok_or_else(|| {
                Error::new(
                    "ADAPTER_OUTBOX",
                    "repaired operation acknowledgement disappeared",
                )
            })?;
        }
        self.enqueue_cleanup(operation_id, &digest)?;
        self.ensure_outbox_entry(operation_id, Some(digest.clone()))?;
        if current.acknowledged_sha256.as_deref() == Some(digest.as_str()) {
            self.finalize_acknowledged(operation_id, &digest)?;
            return Ok(());
        }
        let record = JournalRecord {
            version: 1,
            operation_id: operation_id.to_owned(),
            kind: "acknowledged".to_owned(),
            receipt: None,
            method: None,
            intent: None,
            outcome: None,
            digest: Some(digest.clone()),
            outbox_sequence: None,
        };
        self.append(operation_id, &record)?;
        let verified = self
            .get(operation_id)?
            .ok_or_else(|| Error::new("ADAPTER_OUTBOX", "acknowledged operation disappeared"))?;
        if verified.acknowledged_sha256.as_deref() != Some(digest.as_str()) {
            self.note_deferred_damage()?;
            return Err(Error::new(
                "ADAPTER_OUTBOX",
                "durable host acknowledgement failed exact readback",
            ));
        }
        self.finalize_acknowledged(operation_id, &digest)
    }

    fn repair_torn_acknowledgement(&self, operation_id: &str, outcome: &Value) -> Result<bool> {
        let digest = digest_json(outcome)?;
        let path = self.path(operation_id)?;
        ensure_regular(&path).map_err(|_| torn_ack_recovery_unknown())?;
        let mut file = OpenOptions::new()
            .read(true)
            .open(&path)
            .map_err(|_| torn_ack_recovery_unknown())?;
        let bytes = read_opened_file_for_repair(&path, &mut file)?;
        let opened_state = read_operation_state_bytes(&bytes, &path, Some(operation_id))
            .map_err(|_| torn_ack_recovery_unknown())?;
        let Some(evidence) = opened_state.torn_tail_evidence.as_ref() else {
            return Ok(false);
        };
        let acknowledged_sha256 = opened_state.acknowledged_sha256.clone();
        if opened_state.operation_id != operation_id
            || opened_state.outcome.as_ref() != Some(outcome)
            || opened_state.outcome_sha256.as_deref() != Some(digest.as_str())
            || acknowledged_sha256
                .as_deref()
                .is_some_and(|saved| saved != digest)
        {
            return Err(torn_ack_recovery_unknown());
        }

        let prefix_bytes = evidence.valid_prefix_bytes;
        let Some(tail_end) = prefix_bytes.checked_add(evidence.tail_bytes) else {
            return Err(torn_ack_recovery_unknown());
        };
        if evidence.tail_bytes == 0 || tail_end != bytes.len() || prefix_bytes >= bytes.len() {
            return Err(torn_ack_recovery_unknown());
        }
        let repaired_bytes = bytes[..prefix_bytes].to_vec();
        let opened_metadata = file.metadata().map_err(|_| torn_ack_recovery_unknown())?;
        if repaired_bytes.len() as u64 > MAX_STATE_BYTES
            || opened_metadata.len() != bytes.len() as u64
            || !opened_file_matches_path(&file, &path)
        {
            return Err(torn_ack_recovery_unknown());
        }
        // Close the validated reader before Windows write-through replacement;
        // the adapter's single-owner scope remains retained.
        drop(file);
        replace_private_durable(&path, &repaired_bytes).map_err(|_| torn_ack_recovery_unknown())?;

        ensure_regular(&path).map_err(|_| torn_ack_recovery_unknown())?;
        let mut repaired_file = OpenOptions::new()
            .read(true)
            .open(&path)
            .map_err(|_| torn_ack_recovery_unknown())?;
        let verified_bytes = read_opened_file_for_repair(&path, &mut repaired_file)?;
        let verified_state = read_operation_state_bytes(&verified_bytes, &path, Some(operation_id))
            .map_err(|_| torn_ack_recovery_unknown())?;
        if verified_bytes != repaired_bytes
            || verified_state.torn_tail_evidence.is_some()
            || verified_state.operation_id != operation_id
            || verified_state.outcome.as_ref() != Some(outcome)
            || verified_state.outcome_sha256.as_deref() != Some(digest.as_str())
            || verified_state.acknowledged_sha256 != acknowledged_sha256
        {
            return Err(torn_ack_recovery_unknown());
        }
        Ok(true)
    }

    pub fn pending_outcomes(&self) -> Result<Vec<OperationState>> {
        self.process_cleanup_batch(MAX_CURSOR_STEPS)?;
        let mut pending = Vec::new();
        for _ in 0..MAX_CURSOR_STEPS {
            let state = self.read_retention_state()?;
            if state.outbox_cursor_sequence >= state.next_outbox_sequence {
                break;
            }
            let sequence = state.outbox_cursor_sequence;
            let entry_path = self.outbox_item_path(sequence);
            if !entry_path.exists() {
                self.advance_outbox_cursor_past(sequence)?;
                continue;
            }
            let entry = match self.read_work_item(&entry_path) {
                Ok(entry) if entry.sequence == sequence => entry,
                Ok(_) | Err(_) => {
                    self.note_deferred_damage()?;
                    self.advance_outbox_cursor_past(sequence)?;
                    continue;
                }
            };
            let mut operation = match self.get(&entry.operation_id) {
                Ok(Some(operation)) if operation.operation_key == entry.operation_key => operation,
                Ok(Some(_)) => {
                    self.note_deferred_damage()?;
                    self.defer_outbox_item(&entry, None)?;
                    self.advance_outbox_cursor_past(sequence)?;
                    continue;
                }
                Ok(None) | Err(_) => {
                    self.note_deferred_damage()?;
                    self.defer_outbox_item(&entry, None)?;
                    self.advance_outbox_cursor_past(sequence)?;
                    continue;
                }
            };
            if operation.compacted {
                if operation.outcome_sha256 == entry.outcome_sha256
                    && operation.acknowledged_sha256 == entry.outcome_sha256
                {
                    self.remove_outbox_item(&entry)?;
                } else {
                    self.note_deferred_damage()?;
                    self.defer_outbox_item(&entry, None)?;
                    self.advance_outbox_cursor_past(sequence)?;
                }
                continue;
            }
            let Some(outcome) = operation.outcome.as_ref() else {
                self.note_deferred_unknown()?;
                self.defer_outbox_item(&entry, None)?;
                self.advance_outbox_cursor_past(sequence)?;
                continue;
            };
            if operation.outcome_sha256 != entry.outcome_sha256
                || outcome["operation_id"] != operation.operation_id
            {
                self.note_deferred_damage()?;
                self.defer_outbox_item(&entry, None)?;
                self.advance_outbox_cursor_past(sequence)?;
                continue;
            }
            if operation.torn_tail_evidence.is_some() {
                match self.repair_torn_acknowledgement(&operation.operation_id, outcome) {
                    Ok(true) => {
                        operation = match self.get(&entry.operation_id)? {
                            Some(operation) if operation.operation_key == entry.operation_key => {
                                operation
                            }
                            _ => {
                                self.note_deferred_damage()?;
                                self.defer_outbox_item(&entry, None)?;
                                self.advance_outbox_cursor_past(sequence)?;
                                continue;
                            }
                        };
                    }
                    Ok(false) => {
                        self.note_deferred_damage()?;
                        self.defer_outbox_item(&entry, None)?;
                        self.advance_outbox_cursor_past(sequence)?;
                        continue;
                    }
                    Err(error) if error.code == "ADAPTER_JOURNAL_RECOVERY_UNKNOWN" => {
                        self.note_deferred_damage()?;
                        self.defer_outbox_item(&entry, None)?;
                        self.advance_outbox_cursor_past(sequence)?;
                        continue;
                    }
                    Err(error) => return Err(error),
                }
            }
            let outcome = operation.outcome.as_ref().ok_or_else(|| {
                Error::new(
                    "ADAPTER_OUTBOX",
                    "pending outcome disappeared during recovery",
                )
            })?;
            if operation.acknowledged_sha256 == operation.outcome_sha256 {
                match outcome["outcome"].as_str() {
                    Some("unknown") => self.note_deferred_unknown()?,
                    Some("accepted")
                        if matches!(
                            outcome["details"]["completion_condition"].as_str(),
                            Some("unknown" | "deferred")
                        ) =>
                    {
                        self.note_deferred_accepted()?
                    }
                    Some("accepted") => self.note_deferred_accepted()?,
                    Some("applied" | "rejected") => {
                        if self.has_references(&operation.operation_id)? {
                            self.note_deferred_reference()?;
                        } else {
                            self.note_deferred_damage()?;
                        }
                    }
                    _ => self.note_deferred_damage()?,
                }
                self.defer_outbox_item(&entry, operation.acknowledged_sha256.as_deref())?;
                self.advance_outbox_cursor_past(sequence)?;
                continue;
            }
            pending.push(operation);
            break;
        }
        Ok(pending)
    }

    pub fn unresolved(&self) -> Result<Vec<OperationState>> {
        let mut pending = Vec::new();
        for entry in fs::read_dir(&self.root)
            .map_err(|_| Error::new("ADAPTER_JOURNAL", "operation directory cannot be listed"))?
        {
            let entry = entry
                .map_err(|_| Error::new("ADAPTER_JOURNAL", "operation entry cannot be read"))?;
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("jsonl") {
                continue;
            }
            ensure_regular(&path)?;
            let Some(name) = path.file_stem().and_then(|value| value.to_str()) else {
                self.note_deferred_damage()?;
                continue;
            };
            let state = match self.read_path(&path, None) {
                Ok(state) => state,
                Err(error) if is_item_local_operation_damage(&error) => {
                    self.note_deferred_damage()?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            if !state.operation_id.is_empty() && hex_digest(state.operation_id.as_bytes()) != name {
                self.note_deferred_damage()?;
                continue;
            }
            if (state.receipt.is_some() && state.outcome.is_none())
                || state.torn_tail_evidence.is_some()
            {
                pending.push(state);
            }
        }
        Ok(pending)
    }

    pub fn has_terminal_shutdown(&self) -> Result<bool> {
        Ok(!self.terminal_shutdown_states(false)?.is_empty())
    }

    /// Restore exact outcome delivery after the ordinary bounded cursor has
    /// passed damaged/deferred work. This never submits native input.
    pub fn prepare_terminal_shutdown_drain(&self) -> Result<()> {
        for mut state in self.terminal_shutdown_states(true)? {
            let outcome = state
                .outcome
                .clone()
                .ok_or_else(torn_ack_recovery_unknown)?;
            if state.torn_tail_evidence.is_some() {
                if !self.repair_torn_acknowledgement(&state.operation_id, &outcome)? {
                    return Err(torn_ack_recovery_unknown());
                }
                state = self
                    .get(&state.operation_id)?
                    .ok_or_else(torn_ack_recovery_unknown)?;
            }
            if state.acknowledged_sha256 != state.outcome_sha256 {
                self.save_outcome_value(&state.operation_id, &outcome)?;
            }
        }
        Ok(())
    }

    pub fn require_terminal_shutdown_acknowledged(&self) -> Result<()> {
        let terminal = self.terminal_shutdown_states(true)?;
        if terminal.is_empty() {
            return Err(Error::new(
                "ADAPTER_SHUTDOWN_ACK_PENDING",
                "previously selected shutdown evidence is unavailable",
            ));
        }
        for state in terminal {
            if state.torn_tail_evidence.is_some()
                || state.outcome_sha256.is_none()
                || state.acknowledged_sha256 != state.outcome_sha256
            {
                return Err(Error::new(
                    "ADAPTER_SHUTDOWN_ACK_PENDING",
                    "retired transport retains an unacknowledged shutdown outcome",
                ));
            }
        }
        Ok(())
    }

    fn terminal_shutdown_states(
        &self,
        require_readable_drain: bool,
    ) -> Result<Vec<OperationState>> {
        let mut terminal = Vec::new();
        for entry in fs::read_dir(&self.root)
            .map_err(|_| Error::new("ADAPTER_JOURNAL", "operation directory cannot be listed"))?
        {
            let entry = entry
                .map_err(|_| Error::new("ADAPTER_JOURNAL", "operation entry cannot be read"))?;
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("jsonl") {
                continue;
            }
            ensure_regular(&path)?;
            let state = match self.read_path(&path, None) {
                Ok(state) => state,
                Err(error) if is_item_local_operation_damage(&error) => {
                    self.note_deferred_damage()?;
                    if require_readable_drain {
                        return Err(error);
                    }
                    continue;
                }
                Err(error) => return Err(error),
            };
            if state.method.as_deref() != Some("agent.shutdown") {
                continue;
            }
            if state.outcome.as_ref().is_some_and(|outcome| {
                matches!(outcome["outcome"].as_str(), Some("applied" | "unknown"))
            }) {
                terminal.push(state);
            }
        }
        Ok(terminal)
    }
    pub fn has_task_dispatch_for_boot(&self, boot_id: &str) -> Result<bool> {
        let path = self.boot_marker_path(boot_id);
        if !path.exists() {
            return Ok(false);
        }
        let marker = self.read_boot_marker(&path)?;
        if marker.bridge_boot_id != boot_id {
            self.note_deferred_damage()?;
            return Err(Error::new(
                "ADAPTER_RETENTION_BOOT_MARKER",
                "boot dispatch marker differs from its exact key",
            ));
        }
        Ok(true)
    }

    pub fn recover_uncertain_with_code(&self, diagnostic_code: &str) -> Result<()> {
        let diagnostic_code = safe_diagnostic_code(diagnostic_code);
        for state in self.unresolved()? {
            self.recover_uncertain_state(&state, diagnostic_code)?;
        }
        Ok(())
    }

    pub fn startup_recovery_complete(&self) -> Result<bool> {
        Ok(self.read_retention_state()?.startup_complete)
    }

    /// Build the existing in-memory sorted name snapshot without holding the
    /// startup-write lock. Owned startup runs this metadata walk on a blocking
    /// worker; operation bodies are still classified in bounded service-loop
    /// windows.
    pub fn prepare_startup_inventory(&self) -> Result<()> {
        if self.read_retention_state()?.startup_complete {
            return Ok(());
        }
        {
            let cached = self.startup_names.lock().map_err(|_| {
                Error::new(
                    "ADAPTER_RETENTION_STATE",
                    "startup inventory cache is unavailable",
                )
            })?;
            if cached.is_some() {
                return Ok(());
            }
        }
        let names = self.list_operation_names()?;
        let mut cached = self.startup_names.lock().map_err(|_| {
            Error::new(
                "ADAPTER_RETENTION_STATE",
                "startup inventory cache is unavailable",
            )
        })?;
        if cached.is_none() {
            *cached = Some(names);
        }
        Ok(())
    }

    pub fn startup_inventory_ready(&self) -> Result<bool> {
        if self.startup_recovery_complete()? {
            return Ok(true);
        }
        Ok(self
            .startup_names
            .lock()
            .map_err(|_| {
                Error::new(
                    "ADAPTER_RETENTION_STATE",
                    "startup inventory cache is unavailable",
                )
            })?
            .is_some())
    }

    /// Service-loop variant: never enumerate the full directory inline. A
    /// cold exact operation read remains available while the name worker runs.
    pub fn try_recover_startup_window_for_boot(
        &self,
        diagnostic_code: &str,
        current_boot_id: &str,
    ) -> Result<Option<usize>> {
        if self.startup_recovery_complete()? {
            return Ok(Some(0));
        }
        if !self.startup_inventory_ready()? {
            return Ok(None);
        }
        self.recover_startup_window_inner(diagnostic_code, Some(current_boot_id))
            .map(Some)
    }

    /// Classify one bounded legacy window and persist its last key once. A
    /// crash may revisit at most 32 operations; migration writes are idempotent
    /// and effect markers are never replayed.
    #[cfg(test)]
    pub fn recover_startup_window(&self, diagnostic_code: &str) -> Result<usize> {
        self.prepare_startup_inventory()?;
        self.recover_startup_window_inner(diagnostic_code, None)
    }

    #[cfg(test)]
    pub fn recover_startup_window_for_boot(
        &self,
        diagnostic_code: &str,
        current_boot_id: &str,
    ) -> Result<usize> {
        self.prepare_startup_inventory()?;
        self.recover_startup_window_inner(diagnostic_code, Some(current_boot_id))
    }

    fn recover_startup_window_inner(
        &self,
        diagnostic_code: &str,
        current_boot_id: Option<&str>,
    ) -> Result<usize> {
        let _startup_guard = self.startup_lock.lock().map_err(|_| {
            Error::new(
                "ADAPTER_RETENTION_STATE",
                "startup journal recovery lock is unavailable",
            )
        })?;
        let diagnostic_code = safe_diagnostic_code(diagnostic_code);
        let state = self.read_retention_state()?;
        if state.startup_complete {
            return Ok(0);
        }
        let names = self
            .startup_names
            .lock()
            .map_err(|_| {
                Error::new(
                    "ADAPTER_RETENTION_STATE",
                    "startup inventory cache is unavailable",
                )
            })?
            .clone()
            .ok_or_else(|| {
                Error::new(
                    "ADAPTER_STARTUP_INVENTORY_PENDING",
                    "legacy operation names have not been prepared",
                )
            })?;
        let start = state.startup_cursor.as_deref().map_or(0, |cursor| {
            names.partition_point(|name| name.as_str() <= cursor)
        });
        let end = start.saturating_add(MAX_CURSOR_STEPS).min(names.len());
        for name in &names[start..end] {
            if state.migration_complete {
                self.recover_legacy_operation(name, diagnostic_code, current_boot_id)?;
            } else {
                self.migrate_legacy_operation(name, diagnostic_code, current_boot_id)?;
            }
        }
        let mut progress = self.read_retention_state()?;
        if end == names.len() {
            progress.migration_complete = true;
            progress.startup_complete = true;
            progress.startup_cursor = None;
            *self.startup_names.lock().map_err(|_| {
                Error::new(
                    "ADAPTER_RETENTION_STATE",
                    "startup inventory cache is unavailable",
                )
            })? = None;
        } else {
            progress.startup_cursor = names.get(end - 1).cloned();
            progress.startup_complete = false;
        }
        self.write_retention_state(&progress)?;
        Ok(end - start)
    }

    pub fn retention_status(&self) -> Result<Value> {
        let state = self.read_retention_state()?;
        Ok(serde_json::json!({
            "schema_version":state.version,
            "degraded":state.counters.degraded,
            "outbox_cursor":state.outbox_cursor_sequence,
            "cleanup_cursor":state.cleanup_cursor_sequence,
            "counters":{
                "examined":state.counters.examined,
                "compacted":state.counters.compacted,
                "deferred_unknown":state.counters.deferred_unknown,
                "deferred_unacknowledged":state.counters.deferred_unacknowledged,
                "deferred_accepted":state.counters.deferred_accepted,
                "deferred_reference":state.counters.deferred_reference,
                "deferred_damage":state.counters.deferred_damage
            }
        }))
    }

    fn ensure_retention_state(&self) -> Result<()> {
        let path = self.retention_state_path();
        if path.exists() {
            self.read_retention_state()?;
            return Ok(());
        }
        let state = RetentionState {
            version: 1,
            next_outbox_sequence: 0,
            outbox_cursor_sequence: 0,
            next_cleanup_sequence: 0,
            cleanup_cursor_sequence: 0,
            migration_complete: false,
            startup_complete: false,
            startup_cursor: None,
            counters: RetentionCounters::default(),
        };
        let bytes = serde_json::to_vec(&state)?;
        write_private_new(&path, &bytes).map_err(|_| {
            Error::new(
                "ADAPTER_RETENTION_STATE",
                "retention cursor cannot be initialized",
            )
        })?;
        let readback = self.read_retention_state()?;
        if readback != state {
            return Err(Error::new(
                "ADAPTER_RETENTION_STATE",
                "retention cursor failed exact initialization readback",
            ));
        }
        Ok(())
    }

    fn prepare_startup_recovery(&self) -> Result<()> {
        let mut state = self.read_retention_state()?;
        if state.startup_complete {
            state.startup_complete = false;
            state.startup_cursor = None;
            self.write_retention_state(&state)?;
        }
        Ok(())
    }

    fn list_operation_names(&self) -> Result<Vec<String>> {
        let entries = fs::read_dir(&self.root).map_err(|_| {
            Error::new(
                "ADAPTER_STARTUP_RECOVERY_INCOMPLETE",
                "journal directory cannot be listed",
            )
        })?;
        let mut names = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|_| {
                Error::new(
                    "ADAPTER_STARTUP_RECOVERY_INCOMPLETE",
                    "journal directory entry cannot be read",
                )
            })?;
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("jsonl") {
                continue;
            }
            let name = path
                .file_stem()
                .and_then(|value| value.to_str())
                .ok_or_else(|| {
                    Error::new(
                        "ADAPTER_STARTUP_RECOVERY_INCOMPLETE",
                        "operation journal has no stable UTF-8 address",
                    )
                })?;
            names.push(name.to_owned());
        }
        names.sort();
        Ok(names)
    }

    fn recover_legacy_operation(
        &self,
        name: &str,
        diagnostic_code: &str,
        current_boot_id: Option<&str>,
    ) -> Result<()> {
        let path = self.root.join(format!("{name}.jsonl"));
        let state = match ensure_regular(&path).and_then(|()| self.read_path(&path, None)) {
            Ok(state) => state,
            Err(_) => {
                self.note_deferred_damage()?;
                return Ok(());
            }
        };
        if state.operation_id.is_empty() || hex_digest(state.operation_id.as_bytes()) != name {
            self.note_deferred_damage()?;
            return Ok(());
        }
        if state.torn_tail_evidence.is_some() {
            self.note_deferred_damage()?;
        }
        if is_current_boot_unknown(&state, current_boot_id) {
            return Ok(());
        }
        self.recover_uncertain_state(&state, diagnostic_code)
    }

    fn migrate_legacy_operation(
        &self,
        name: &str,
        diagnostic_code: &str,
        current_boot_id: Option<&str>,
    ) -> Result<()> {
        let path = self.root.join(format!("{name}.jsonl"));
        let state = match ensure_regular(&path).and_then(|()| self.read_path(&path, None)) {
            Ok(state) => state,
            Err(_) => {
                self.note_deferred_damage()?;
                return Ok(());
            }
        };
        if state.operation_id.is_empty() || hex_digest(state.operation_id.as_bytes()) != name {
            self.note_deferred_damage()?;
            return Ok(());
        }
        self.increment_examined()?;
        if let Some(intent) = state.intent.as_ref() {
            if state.method.as_deref() == Some("task.dispatch") {
                let outcome_digest = state.outcome_sha256.as_deref();
                let acknowledged_digest = state.acknowledged_sha256.as_deref();
                if self
                    .write_boot_dispatch_marker(
                        &state.operation_id,
                        state.receipt.as_ref().ok_or_else(|| {
                            Error::new(
                                "ADAPTER_RETENTION_STATE",
                                "task dispatch has no exact receipt",
                            )
                        })?,
                        intent,
                        outcome_digest,
                        acknowledged_digest,
                    )
                    .is_err()
                {
                    self.note_deferred_damage()?;
                }
            }
            if self
                .register_references_for_intent(&state.operation_id, intent)
                .is_err()
            {
                self.note_deferred_damage()?;
            }
        }
        if let (Some(outcome_digest), Some(outcome)) =
            (state.outcome_sha256.as_deref(), state.outcome.as_ref())
        {
            let outbox_sequence =
                self.ensure_outbox_entry(&state.operation_id, Some(outcome_digest.to_owned()))?;
            if state.acknowledged_sha256.as_deref() == Some(outcome_digest) {
                self.enqueue_cleanup(&state.operation_id, outcome_digest)?;
                let item_path = self.outbox_item_path(outbox_sequence);
                let item = self.read_work_item(&item_path)?;
                if item.sequence != outbox_sequence
                    || item.operation_id != state.operation_id
                    || item.outcome_sha256.as_deref() != Some(outcome_digest)
                {
                    self.note_deferred_damage()?;
                } else {
                    self.defer_outbox_item(&item, Some(outcome_digest))?;
                }
            }
            if outcome["outcome"] == "unknown" || outcome["outcome"] == "accepted" {
                self.note_outcome_class(outcome)?;
            }
        } else if state.torn_tail_evidence.is_some() {
            // Do not append to a torn frame; the original bytes remain the
            // authoritative custody evidence for this operation.
            self.note_deferred_damage()?;
        }
        if is_current_boot_unknown(&state, current_boot_id) {
            return Ok(());
        }
        self.recover_uncertain_state(&state, diagnostic_code)
    }

    fn recover_uncertain_state(&self, state: &OperationState, diagnostic_code: &str) -> Result<()> {
        if state.torn_tail_evidence.is_some() || !state.recovery_unknown() {
            return Ok(());
        }
        let receipt = state
            .receipt
            .as_ref()
            .ok_or_else(|| Error::new("ADAPTER_JOURNAL", "unresolved operation has no receipt"))?;
        let scope = state
            .intent
            .as_ref()
            .and_then(|intent| {
                intent["native_scope_key"]
                    .as_str()
                    .or_else(|| intent["native"]["native_scope_key"].as_str())
            })
            .map(ToOwned::to_owned);
        let native = state.intent.as_ref().map(|intent| &intent["native"]);
        let outcome = RuntimeOutcome {
            operation_id: state.operation_id.clone(),
            outcome: swarm_contracts::runtime::EffectOutcome::Unknown,
            native_scope_key: scope,
            native_root_id: state
                .intent
                .as_ref()
                .and_then(|intent| intent["native_root_id"].as_str())
                .map(ToOwned::to_owned),
            turn_id: None,
            native_input_id: native
                .and_then(|intent| intent["user_message_uuid"].as_str())
                .map(ToOwned::to_owned),
            details: serde_json::json!({
                "diagnostic_code":diagnostic_code,
                "completion_condition":"unknown",
                "replay_permitted":false,
                "module_receipt":receipt
            }),
        };
        self.save_outcome(&state.operation_id, &outcome)
    }

    fn read_retention_state(&self) -> Result<RetentionState> {
        let path = self.retention_state_path();
        ensure_regular(&path).map_err(|_| {
            Error::new(
                "ADAPTER_RETENTION_STATE",
                "retention cursor is not a bounded regular file",
            )
        })?;
        let bytes = read_limited(&path, MAX_RETENTION_METADATA_BYTES)?;
        let state: RetentionState = serde_json::from_slice(&bytes)
            .map_err(|_| Error::new("ADAPTER_RETENTION_STATE", "retention cursor is malformed"))?;
        if state.version != 1
            || state.outbox_cursor_sequence > state.next_outbox_sequence
            || state.cleanup_cursor_sequence > state.next_cleanup_sequence
        {
            return Err(Error::new(
                "ADAPTER_RETENTION_STATE",
                "retention cursor identity or sequence is invalid",
            ));
        }
        Ok(state)
    }

    fn write_retention_state(&self, state: &RetentionState) -> Result<()> {
        let bytes = serde_json::to_vec(state)?;
        if bytes.len() > MAX_RETENTION_METADATA_BYTES {
            self.note_deferred_damage()?;
            return Err(Error::new(
                "ADAPTER_RETENTION_STATE",
                "retention counters exceed their size boundary",
            ));
        }
        replace_private_durable(&self.retention_state_path(), &bytes).map_err(|_| {
            Error::new(
                "ADAPTER_RETENTION_STATE",
                "retention cursor cannot be durably updated",
            )
        })?;
        let readback = self.read_retention_state()?;
        if &readback != state {
            return Err(Error::new(
                "ADAPTER_RETENTION_STATE",
                "retention cursor failed exact durable readback",
            ));
        }
        Ok(())
    }

    fn ensure_outbox_entry(
        &self,
        operation_id: &str,
        outcome_sha256: Option<String>,
    ) -> Result<u64> {
        let operation_key = hex_digest(operation_id.as_bytes());
        let pointer_path = self.outbox_pointer_path(&operation_key);
        let mut pointer = if pointer_path.exists() {
            self.read_pointer(&pointer_path)?
        } else {
            let mut state = self.read_retention_state()?;
            let sequence = state.next_outbox_sequence;
            state.next_outbox_sequence = sequence
                .checked_add(1)
                .ok_or_else(|| Error::new("ADAPTER_OUTBOX", "outbox sequence is exhausted"))?;
            self.write_retention_state(&state)?;
            let pointer = WorkPointer {
                version: 1,
                sequence,
                operation_id: operation_id.to_owned(),
                operation_key: operation_key.clone(),
                phase: WorkPhase::Pending,
                outcome_sha256: outcome_sha256.clone(),
                acknowledged_sha256: None,
            };
            self.write_pointer(&pointer_path, &pointer, true)?;
            pointer
        };
        if pointer.version != 1
            || pointer.operation_id != operation_id
            || pointer.operation_key != operation_key
        {
            self.note_deferred_damage()?;
            return Err(Error::new(
                "ADAPTER_OUTBOX",
                "outbox pointer differs from its exact operation identity",
            ));
        }
        if let Some(digest) = outcome_sha256 {
            if pointer
                .outcome_sha256
                .as_deref()
                .is_some_and(|saved| saved != digest)
            {
                self.note_deferred_damage()?;
                return Err(Error::new(
                    "ADAPTER_OUTBOX",
                    "outbox pointer digest conflicts with the saved outcome",
                ));
            }
            pointer.outcome_sha256 = Some(digest);
        }
        let retention = self.read_retention_state()?;
        if (pointer.phase == WorkPhase::Deferred
            || pointer.sequence < retention.outbox_cursor_sequence)
            && pointer.acknowledged_sha256.is_none()
        {
            let mut state = self.read_retention_state()?;
            let sequence = state.next_outbox_sequence;
            state.next_outbox_sequence = sequence
                .checked_add(1)
                .ok_or_else(|| Error::new("ADAPTER_OUTBOX", "outbox sequence is exhausted"))?;
            self.write_retention_state(&state)?;
            pointer.sequence = sequence;
            pointer.phase = WorkPhase::Pending;
        }
        self.ensure_work_item("outbox", &pointer)?;
        self.write_pointer(&pointer_path, &pointer, false)?;
        Ok(pointer.sequence)
    }

    fn enqueue_cleanup(&self, operation_id: &str, acknowledged_sha256: &str) -> Result<u64> {
        let operation_key = hex_digest(operation_id.as_bytes());
        let pointer_path = self.cleanup_pointer_path(&operation_key);
        let mut pointer = if pointer_path.exists() {
            self.read_pointer(&pointer_path)?
        } else {
            let mut state = self.read_retention_state()?;
            let sequence = state.next_cleanup_sequence;
            state.next_cleanup_sequence = sequence.checked_add(1).ok_or_else(|| {
                Error::new("ADAPTER_RETENTION_STATE", "cleanup sequence is exhausted")
            })?;
            self.write_retention_state(&state)?;
            let pointer = WorkPointer {
                version: 1,
                sequence,
                operation_id: operation_id.to_owned(),
                operation_key: operation_key.clone(),
                phase: WorkPhase::Pending,
                outcome_sha256: Some(acknowledged_sha256.to_owned()),
                acknowledged_sha256: Some(acknowledged_sha256.to_owned()),
            };
            self.write_pointer(&pointer_path, &pointer, true)?;
            pointer
        };
        if pointer.version != 1
            || pointer.operation_id != operation_id
            || pointer.operation_key != operation_key
            || pointer.outcome_sha256.as_deref() != Some(acknowledged_sha256)
            || pointer
                .acknowledged_sha256
                .as_deref()
                .is_some_and(|saved| saved != acknowledged_sha256)
        {
            self.note_deferred_damage()?;
            return Err(Error::new(
                "ADAPTER_RETENTION_STATE",
                "cleanup pointer differs from its exact acknowledged operation",
            ));
        }
        pointer.acknowledged_sha256 = Some(acknowledged_sha256.to_owned());
        if pointer.phase == WorkPhase::Deferred {
            let mut state = self.read_retention_state()?;
            let sequence = state.next_cleanup_sequence;
            state.next_cleanup_sequence = sequence.checked_add(1).ok_or_else(|| {
                Error::new("ADAPTER_RETENTION_STATE", "cleanup sequence is exhausted")
            })?;
            self.write_retention_state(&state)?;
            pointer.sequence = sequence;
            pointer.phase = WorkPhase::Pending;
        }
        self.ensure_work_item("cleanup", &pointer)?;
        self.write_pointer(&pointer_path, &pointer, false)?;
        Ok(pointer.sequence)
    }

    fn ensure_work_item(&self, queue: &str, pointer: &WorkPointer) -> Result<()> {
        let path = self.work_item_path(queue, pointer.sequence);
        let item = WorkItem {
            version: 1,
            sequence: pointer.sequence,
            operation_id: pointer.operation_id.clone(),
            operation_key: pointer.operation_key.clone(),
            outcome_sha256: pointer.outcome_sha256.clone(),
            acknowledged_sha256: pointer.acknowledged_sha256.clone(),
        };
        if path.exists() {
            let existing = self.read_work_item(&path)?;
            if existing.sequence != item.sequence
                || existing.operation_id != item.operation_id
                || existing.operation_key != item.operation_key
                || (existing.outcome_sha256.is_some()
                    && existing.outcome_sha256 != item.outcome_sha256)
                || (existing.acknowledged_sha256.is_some()
                    && existing.acknowledged_sha256 != item.acknowledged_sha256)
            {
                self.note_deferred_damage()?;
                return Err(Error::new(
                    "ADAPTER_RETENTION_STATE",
                    "durable work item conflicts with its exact operation",
                ));
            }
            if existing != item {
                self.write_work_item(&path, &item, false)?;
            }
            return Ok(());
        }
        self.write_work_item(&path, &item, true)
    }

    fn read_work_item(&self, path: &Path) -> Result<WorkItem> {
        ensure_regular(path).map_err(|_| {
            Error::new(
                "ADAPTER_RETENTION_STATE",
                "retention work item is not a regular file",
            )
        })?;
        let bytes = read_limited(path, MAX_RETENTION_METADATA_BYTES)?;
        let item: WorkItem = serde_json::from_slice(&bytes).map_err(|_| {
            Error::new(
                "ADAPTER_RETENTION_STATE",
                "retention work item is malformed",
            )
        })?;
        if item.version != 1
            || !valid_operation_id(&item.operation_id)
            || item.operation_key != hex_digest(item.operation_id.as_bytes())
            || item
                .outcome_sha256
                .as_deref()
                .is_some_and(|digest| !valid_sha256(digest))
            || item.acknowledged_sha256.as_deref().is_some_and(|digest| {
                !valid_sha256(digest) || item.outcome_sha256.as_deref() != Some(digest)
            })
        {
            return Err(Error::new(
                "ADAPTER_RETENTION_STATE",
                "retention work item identity is invalid",
            ));
        }
        Ok(item)
    }

    fn write_work_item(&self, path: &Path, item: &WorkItem, create: bool) -> Result<()> {
        let bytes = serde_json::to_vec(item)?;
        let write = if create {
            write_private_new(path, &bytes)
        } else {
            replace_private_durable(path, &bytes)
        };
        write.map_err(|_| {
            Error::new(
                "ADAPTER_RETENTION_STATE",
                "retention work item cannot be saved",
            )
        })?;
        if self.read_work_item(path)? != *item {
            return Err(Error::new(
                "ADAPTER_RETENTION_STATE",
                "retention work item failed exact durable readback",
            ));
        }
        Ok(())
    }

    fn read_pointer(&self, path: &Path) -> Result<WorkPointer> {
        ensure_regular(path).map_err(|_| {
            Error::new(
                "ADAPTER_RETENTION_STATE",
                "retention pointer is not a regular file",
            )
        })?;
        let bytes = read_limited(path, MAX_RETENTION_METADATA_BYTES)?;
        let pointer: WorkPointer = serde_json::from_slice(&bytes)
            .map_err(|_| Error::new("ADAPTER_RETENTION_STATE", "retention pointer is malformed"))?;
        if pointer.version != 1
            || !valid_operation_id(&pointer.operation_id)
            || pointer.operation_key != hex_digest(pointer.operation_id.as_bytes())
            || pointer
                .outcome_sha256
                .as_deref()
                .is_some_and(|digest| !valid_sha256(digest))
            || pointer
                .acknowledged_sha256
                .as_deref()
                .is_some_and(|digest| {
                    !valid_sha256(digest) || pointer.outcome_sha256.as_deref() != Some(digest)
                })
        {
            return Err(Error::new(
                "ADAPTER_RETENTION_STATE",
                "retention pointer identity is invalid",
            ));
        }
        Ok(pointer)
    }

    fn write_pointer(&self, path: &Path, pointer: &WorkPointer, create: bool) -> Result<()> {
        let bytes = serde_json::to_vec(pointer)?;
        let write = if create {
            write_private_new(path, &bytes)
        } else {
            replace_private_durable(path, &bytes)
        };
        write.map_err(|_| {
            Error::new(
                "ADAPTER_RETENTION_STATE",
                "retention pointer cannot be saved",
            )
        })?;
        if self.read_pointer(path)? != *pointer {
            return Err(Error::new(
                "ADAPTER_RETENTION_STATE",
                "retention pointer failed exact durable readback",
            ));
        }
        Ok(())
    }

    fn remove_outbox_item(&self, item: &WorkItem) -> Result<()> {
        let operation = match self.get(&item.operation_id) {
            Ok(Some(operation)) => operation,
            Ok(None) | Err(_) => {
                self.note_deferred_damage()?;
                self.advance_outbox_cursor_past(item.sequence)?;
                return Ok(());
            }
        };
        if !operation.compacted
            || operation.operation_key != item.operation_key
            || operation.outcome_sha256 != item.outcome_sha256
            || operation.acknowledged_sha256 != item.outcome_sha256
        {
            self.note_deferred_damage()?;
            self.advance_outbox_cursor_past(item.sequence)?;
            return Ok(());
        }
        let pointer_path = self.outbox_pointer_path(&item.operation_key);
        if pointer_path.exists() {
            let pointer = match self.read_pointer(&pointer_path) {
                Ok(pointer) => pointer,
                Err(_) => {
                    self.note_deferred_damage()?;
                    self.advance_outbox_cursor_past(item.sequence)?;
                    return Ok(());
                }
            };
            if pointer.operation_id != item.operation_id
                || pointer.operation_key != item.operation_key
                || pointer.sequence != item.sequence
                || pointer.outcome_sha256 != item.outcome_sha256
                || pointer
                    .acknowledged_sha256
                    .as_ref()
                    .is_some_and(|digest| Some(digest) != item.outcome_sha256.as_ref())
            {
                self.note_deferred_damage()?;
                self.advance_outbox_cursor_past(item.sequence)?;
                return Ok(());
            }
            remove_exact_file(&pointer_path)?;
        }
        let entry_path = self.outbox_item_path(item.sequence);
        if entry_path.exists() {
            let existing = match self.read_work_item(&entry_path) {
                Ok(existing) => existing,
                Err(_) => {
                    self.note_deferred_damage()?;
                    self.advance_outbox_cursor_past(item.sequence)?;
                    return Ok(());
                }
            };
            if existing != *item {
                self.note_deferred_damage()?;
                self.advance_outbox_cursor_past(item.sequence)?;
                return Ok(());
            }
            remove_exact_file(&entry_path)?;
        }
        self.advance_outbox_cursor()
    }

    fn defer_outbox_item(&self, item: &WorkItem, acknowledged_sha256: Option<&str>) -> Result<()> {
        let pointer_path = self.outbox_pointer_path(&item.operation_key);
        if !pointer_path.exists() {
            return Ok(());
        }
        let mut pointer = match self.read_pointer(&pointer_path) {
            Ok(pointer) => pointer,
            Err(_) => return self.note_deferred_damage(),
        };
        if pointer.sequence != item.sequence
            || pointer.operation_id != item.operation_id
            || pointer.operation_key != item.operation_key
            || pointer.outcome_sha256 != item.outcome_sha256
        {
            return self.note_deferred_damage();
        }
        if acknowledged_sha256.is_some_and(|digest| {
            item.outcome_sha256.as_deref() != Some(digest)
                || pointer
                    .acknowledged_sha256
                    .as_deref()
                    .is_some_and(|saved| saved != digest)
        }) {
            return self.note_deferred_damage();
        }
        let mut updated_item = item.clone();
        if let Some(digest) = acknowledged_sha256 {
            updated_item.acknowledged_sha256 = Some(digest.to_owned());
        }
        if updated_item != *item {
            self.write_work_item(&self.outbox_item_path(item.sequence), &updated_item, false)?;
        }
        let pointer_changed = pointer.phase != WorkPhase::Deferred
            || acknowledged_sha256
                .is_some_and(|digest| pointer.acknowledged_sha256.as_deref() != Some(digest));
        if pointer_changed {
            pointer.phase = WorkPhase::Deferred;
            if let Some(digest) = acknowledged_sha256 {
                pointer.acknowledged_sha256 = Some(digest.to_owned());
            }
            self.write_pointer(&pointer_path, &pointer, false)?;
        }
        Ok(())
    }

    fn advance_outbox_cursor_past(&self, sequence: u64) -> Result<()> {
        let mut state = self.read_retention_state()?;
        if state.outbox_cursor_sequence == sequence {
            state.outbox_cursor_sequence = sequence.saturating_add(1);
            self.write_retention_state(&state)?;
        }
        Ok(())
    }

    fn advance_outbox_cursor(&self) -> Result<()> {
        let mut state = self.read_retention_state()?;
        let mut steps = 0usize;
        while state.outbox_cursor_sequence < state.next_outbox_sequence
            && steps < MAX_CURSOR_STEPS
            && !self.outbox_item_path(state.outbox_cursor_sequence).exists()
        {
            state.outbox_cursor_sequence = state.outbox_cursor_sequence.saturating_add(1);
            steps = steps.saturating_add(1);
        }
        self.write_retention_state(&state)
    }

    fn process_cleanup_batch(&self, maximum_steps: usize) -> Result<()> {
        for _ in 0..maximum_steps.min(MAX_CURSOR_STEPS) {
            let state = self.read_retention_state()?;
            if state.cleanup_cursor_sequence >= state.next_cleanup_sequence {
                break;
            }
            let sequence = state.cleanup_cursor_sequence;
            let item_path = self.cleanup_item_path(sequence);
            if !item_path.exists() {
                self.advance_cleanup_cursor_past(sequence)?;
                continue;
            }
            let item = match self.read_work_item(&item_path) {
                Ok(item) if item.sequence == sequence && item.acknowledged_sha256.is_some() => item,
                Ok(_) | Err(_) => {
                    self.note_deferred_damage()?;
                    self.advance_cleanup_cursor_past(sequence)?;
                    continue;
                }
            };
            let pointer_path = self.cleanup_pointer_path(&item.operation_key);
            if !pointer_path.exists() {
                if self.compacted_ack_matches(&item)? {
                    self.remove_cleanup_item(&item)?;
                } else {
                    self.note_deferred_damage()?;
                }
                self.advance_cleanup_cursor_past(sequence)?;
                continue;
            }
            let pointer = match self.read_pointer(&pointer_path) {
                Ok(pointer) => pointer,
                Err(_) => {
                    self.note_deferred_damage()?;
                    self.advance_cleanup_cursor_past(sequence)?;
                    continue;
                }
            };
            if pointer.operation_id != item.operation_id
                || pointer.operation_key != item.operation_key
                || pointer.acknowledged_sha256 != item.acknowledged_sha256
            {
                self.note_deferred_damage()?;
                self.advance_cleanup_cursor_past(sequence)?;
                continue;
            }
            if pointer.sequence != sequence {
                if pointer.sequence < sequence {
                    self.note_deferred_damage()?;
                }
                self.advance_cleanup_cursor_past(sequence)?;
                continue;
            }
            let previously_compacted = self
                .get(&item.operation_id)?
                .is_some_and(|operation| operation.compacted);
            match self.attempt_cleanup(&item.operation_id, item.acknowledged_sha256.as_deref())? {
                true => {
                    self.remove_cleanup_item(&item)?;
                    self.remove_outbox_for_operation(
                        &item.operation_id,
                        item.acknowledged_sha256.as_deref().ok_or_else(|| {
                            Error::new(
                                "ADAPTER_RETENTION_STATE",
                                "cleanup acknowledgement is missing",
                            )
                        })?,
                    )?;
                    if !previously_compacted {
                        self.increment_compacted()?;
                    }
                }
                false => {
                    let mut deferred = pointer;
                    deferred.phase = WorkPhase::Deferred;
                    self.write_pointer(&pointer_path, &deferred, false)?;
                }
            }
            self.advance_cleanup_cursor_past(sequence)?;
        }
        Ok(())
    }

    fn advance_cleanup_cursor_past(&self, sequence: u64) -> Result<()> {
        let mut state = self.read_retention_state()?;
        if state.cleanup_cursor_sequence == sequence {
            state.cleanup_cursor_sequence = sequence.saturating_add(1);
            self.write_retention_state(&state)?;
        }
        Ok(())
    }

    fn compacted_ack_matches(&self, item: &WorkItem) -> Result<bool> {
        let Some(expected) = item.acknowledged_sha256.as_deref() else {
            return Ok(false);
        };
        let Some(operation) = self.get(&item.operation_id)? else {
            return Ok(false);
        };
        Ok(operation.compacted
            && operation.operation_key == item.operation_key
            && operation.outcome_sha256.as_deref() == Some(expected)
            && operation.acknowledged_sha256.as_deref() == Some(expected))
    }

    fn remove_cleanup_item(&self, item: &WorkItem) -> Result<()> {
        if !self.compacted_ack_matches(item)? {
            self.note_deferred_damage()?;
            return Ok(());
        }
        let pointer_path = self.cleanup_pointer_path(&item.operation_key);
        if pointer_path.exists() {
            let pointer = match self.read_pointer(&pointer_path) {
                Ok(pointer) => pointer,
                Err(_) => {
                    self.note_deferred_damage()?;
                    return Ok(());
                }
            };
            if pointer.operation_id != item.operation_id
                || pointer.operation_key != item.operation_key
                || pointer.acknowledged_sha256 != item.acknowledged_sha256
            {
                self.note_deferred_damage()?;
                return Ok(());
            }
            if pointer.sequence == item.sequence {
                remove_exact_file(&pointer_path)?;
            }
        }
        let item_path = self.cleanup_item_path(item.sequence);
        if item_path.exists() {
            match self.read_work_item(&item_path) {
                Ok(saved) if saved == *item => remove_exact_file(&item_path)?,
                _ => self.note_deferred_damage()?,
            }
        }
        Ok(())
    }

    fn remove_outbox_for_operation(
        &self,
        operation_id: &str,
        acknowledged_sha256: &str,
    ) -> Result<()> {
        let Some(saved) = self.get(operation_id)? else {
            self.note_deferred_damage()?;
            return Ok(());
        };
        if !saved.compacted
            || saved.outcome_sha256.as_deref() != Some(acknowledged_sha256)
            || saved.acknowledged_sha256.as_deref() != Some(acknowledged_sha256)
        {
            self.note_deferred_damage()?;
            return Ok(());
        }
        let operation_key = hex_digest(operation_id.as_bytes());
        let pointer_path = self.outbox_pointer_path(&operation_key);
        if !pointer_path.exists() {
            return Ok(());
        }
        let pointer = match self.read_pointer(&pointer_path) {
            Ok(pointer) => pointer,
            Err(_) => {
                self.note_deferred_damage()?;
                return Ok(());
            }
        };
        if pointer.operation_id != operation_id
            || pointer.operation_key != operation_key
            || pointer.outcome_sha256.as_deref() != Some(acknowledged_sha256)
            || pointer
                .acknowledged_sha256
                .as_deref()
                .is_some_and(|digest| digest != acknowledged_sha256)
        {
            self.note_deferred_damage()?;
            return Ok(());
        }
        let item_path = self.outbox_item_path(pointer.sequence);
        if !item_path.exists() {
            remove_exact_file(&pointer_path)?;
            self.advance_outbox_cursor()?;
            return Ok(());
        }
        let item = match self.read_work_item(&item_path) {
            Ok(item)
                if item.sequence == pointer.sequence
                    && item.operation_id == operation_id
                    && item.operation_key == operation_key
                    && item.outcome_sha256.as_deref() == Some(acknowledged_sha256) =>
            {
                item
            }
            _ => {
                self.note_deferred_damage()?;
                return Ok(());
            }
        };
        self.remove_outbox_item(&item)
    }

    fn finalize_acknowledged(&self, operation_id: &str, acknowledged_sha256: &str) -> Result<()> {
        let state = self.get(operation_id)?.ok_or_else(|| {
            Error::new(
                "ADAPTER_RETENTION_STATE",
                "acknowledged cleanup target is missing",
            )
        })?;
        if state.compacted {
            if state.acknowledged_sha256.as_deref() != Some(acknowledged_sha256)
                || state.outcome_sha256.as_deref() != Some(acknowledged_sha256)
            {
                self.note_deferred_damage()?;
                return Err(Error::new(
                    "ADAPTER_RETENTION_STATE",
                    "compacted cleanup target differs from its exact acknowledgement",
                ));
            }
            self.remove_outbox_for_operation(operation_id, acknowledged_sha256)?;
            return Ok(());
        }
        if state.acknowledged_sha256.as_deref() != Some(acknowledged_sha256)
            || state.outcome_sha256.as_deref() != Some(acknowledged_sha256)
        {
            self.note_deferred_damage()?;
            return Err(Error::new(
                "ADAPTER_RETENTION_STATE",
                "cleanup requires the exact durable host acknowledgement",
            ));
        }
        self.release_references_for(operation_id, &state)?;
        let sequence = self.enqueue_cleanup(operation_id, acknowledged_sha256)?;
        let cleanup_pointer = self.cleanup_pointer_path(&hex_digest(operation_id.as_bytes()));
        let pointer = self.read_pointer(&cleanup_pointer)?;
        if pointer.sequence != sequence
            || pointer.operation_id != operation_id
            || pointer.acknowledged_sha256.as_deref() != Some(acknowledged_sha256)
        {
            return Err(Error::new(
                "ADAPTER_RETENTION_STATE",
                "cleanup cursor points to another exact operation",
            ));
        }
        let item_path = self.cleanup_item_path(sequence);
        let item = self.read_work_item(&item_path)?;
        if item.sequence != sequence
            || item.operation_id != operation_id
            || item.operation_key != hex_digest(operation_id.as_bytes())
            || item.acknowledged_sha256.as_deref() != Some(acknowledged_sha256)
        {
            self.note_deferred_damage()?;
            return Err(Error::new(
                "ADAPTER_RETENTION_STATE",
                "cleanup work item differs from its exact acknowledged operation",
            ));
        }
        match self.attempt_cleanup(operation_id, Some(acknowledged_sha256))? {
            true => {
                self.remove_cleanup_item(&item)?;
                self.remove_outbox_for_operation(operation_id, acknowledged_sha256)?;
                if !state.compacted {
                    self.increment_compacted()?;
                }
            }
            false => {
                let mut deferred = pointer;
                deferred.phase = WorkPhase::Deferred;
                self.write_pointer(&cleanup_pointer, &deferred, false)?;
            }
        }
        Ok(())
    }

    fn attempt_cleanup(
        &self,
        operation_id: &str,
        expected_acknowledged_sha256: Option<&str>,
    ) -> Result<bool> {
        let Some(expected_acknowledged_sha256) = expected_acknowledged_sha256 else {
            self.note_deferred_unacknowledged()?;
            return Ok(false);
        };
        let Some(state) = self.get(operation_id)? else {
            self.note_deferred_damage()?;
            return Ok(false);
        };
        // This one terminal record is the binding's durable retirement
        // marker. Removing it would let a later boot reopen native work.
        if state.method.as_deref() == Some("agent.shutdown")
            && state.outcome.as_ref().is_some_and(|outcome| {
                matches!(outcome["outcome"].as_str(), Some("applied" | "unknown"))
            })
        {
            return Ok(false);
        }
        if state.compacted {
            if state.acknowledged_sha256.as_deref() != Some(expected_acknowledged_sha256) {
                self.note_deferred_damage()?;
                return Ok(false);
            }
            return Ok(true);
        }
        if state.torn_tail_evidence.is_some() {
            self.note_deferred_damage()?;
            return Ok(false);
        }
        let Some(outcome) = state.outcome.as_ref() else {
            self.note_deferred_unacknowledged()?;
            return Ok(false);
        };
        let Some(outcome_digest) = state.outcome_sha256.as_deref() else {
            self.note_deferred_damage()?;
            return Ok(false);
        };
        if state.acknowledged_sha256.as_deref() != Some(outcome_digest)
            || expected_acknowledged_sha256 != outcome_digest
        {
            self.note_deferred_unacknowledged()?;
            return Ok(false);
        }
        match outcome["outcome"].as_str() {
            Some("unknown") => {
                self.note_deferred_unknown()?;
                return Ok(false);
            }
            Some("accepted") => {
                self.note_deferred_accepted()?;
                return Ok(false);
            }
            Some("applied" | "rejected") => {}
            _ => {
                self.note_deferred_damage()?;
                return Ok(false);
            }
        }
        if matches!(
            outcome["details"]["completion_condition"].as_str(),
            Some("unknown" | "deferred")
        ) {
            self.note_deferred_accepted()?;
            return Ok(false);
        }
        if self.has_references(operation_id)? {
            self.note_deferred_reference()?;
            return Ok(false);
        }
        if state.method.as_deref() == Some("task.dispatch") {
            let (Some(receipt), Some(intent)) = (state.receipt.as_ref(), state.intent.as_ref())
            else {
                self.note_deferred_damage()?;
                return Ok(false);
            };
            self.write_boot_dispatch_marker(
                operation_id,
                receipt,
                intent,
                Some(outcome_digest),
                Some(outcome_digest),
            )?;
        }
        let Some(receipt) = state.receipt.as_ref() else {
            self.note_deferred_damage()?;
            return Ok(false);
        };
        let Some(method) = state.method.as_ref() else {
            self.note_deferred_damage()?;
            return Ok(false);
        };
        let Some(intent) = state.intent.as_ref() else {
            self.note_deferred_damage()?;
            return Ok(false);
        };
        let tombstone = OperationTombstone {
            version: 1,
            operation_id: operation_id.to_owned(),
            operation_key: hex_digest(operation_id.as_bytes()),
            receipt: receipt.clone(),
            method: method.clone(),
            intent: compact_intent(intent),
            outcome: compact_outcome(outcome),
            outcome_sha256: outcome_digest.to_owned(),
            acknowledged_sha256: outcome_digest.to_owned(),
            outbox_sequence: state.outbox_sequence,
        };
        self.write_tombstone(&tombstone)?;
        let path = self.path(operation_id)?;
        if path.exists() {
            ensure_regular(&path)?;
            remove_exact_file(&path)?;
        }
        Ok(true)
    }

    fn register_references_for_intent(&self, operation_id: &str, intent: &Value) -> Result<()> {
        for target_id in reference_targets(operation_id, intent)? {
            let target_key = hex_digest(target_id.as_bytes());
            let reference_key = hex_digest(operation_id.as_bytes());
            let directory = ensure_child_directory(&self.references_directory(), &target_key)?;
            let path = directory.join(format!("{reference_key}.json"));
            let reference = OperationReference {
                version: 1,
                target_operation_id: target_id.clone(),
                reference_operation_id: operation_id.to_owned(),
            };
            let bytes = serde_json::to_vec(&reference)?;
            if path.exists() {
                let saved: OperationReference = self.read_json(&path)?;
                if saved != reference {
                    self.note_deferred_damage()?;
                    return Err(Error::new(
                        "ADAPTER_RETENTION_REFERENCE",
                        "operation reference differs from its exact target identity",
                    ));
                }
            } else {
                write_private_new(&path, &bytes).map_err(|_| {
                    Error::new(
                        "ADAPTER_RETENTION_REFERENCE",
                        "operation reference cannot be durably recorded",
                    )
                })?;
                if self.read_json::<OperationReference>(&path)? != reference {
                    self.note_deferred_damage()?;
                    return Err(Error::new(
                        "ADAPTER_RETENTION_REFERENCE",
                        "operation reference failed exact durable readback",
                    ));
                }
            }
        }
        Ok(())
    }

    fn release_references_for(&self, operation_id: &str, state: &OperationState) -> Result<()> {
        let Some(outcome) = state.outcome.as_ref() else {
            return Ok(());
        };
        if !matches!(outcome["outcome"].as_str(), Some("applied" | "rejected"))
            || state.acknowledged_sha256 != state.outcome_sha256
        {
            return Ok(());
        }
        let Some(intent) = state.intent.as_ref() else {
            return Ok(());
        };
        for target_id in reference_targets(operation_id, intent)? {
            let target_key = hex_digest(target_id.as_bytes());
            let reference_key = hex_digest(operation_id.as_bytes());
            let path = self
                .references_directory()
                .join(target_key)
                .join(format!("{reference_key}.json"));
            if !path.exists() {
                continue;
            }
            let saved: OperationReference = self.read_json(&path)?;
            if saved.target_operation_id != target_id
                || saved.reference_operation_id != operation_id
            {
                self.note_deferred_damage()?;
                continue;
            }
            remove_exact_file(&path)?;
            if let Some(target) = self.get(&target_id)?
                && let Some(digest) = target.acknowledged_sha256.as_deref()
                && target.outcome_sha256.as_deref() == Some(digest)
            {
                self.enqueue_cleanup(&target_id, digest)?;
            }
        }
        Ok(())
    }

    fn has_references(&self, operation_id: &str) -> Result<bool> {
        let directory = self
            .references_directory()
            .join(hex_digest(operation_id.as_bytes()));
        match fs::symlink_metadata(&directory) {
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
            Ok(_) => {
                self.note_deferred_damage()?;
                return Ok(true);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(_) => {
                self.note_deferred_damage()?;
                return Ok(true);
            }
        }
        let mut entries = fs::read_dir(&directory).map_err(|_| {
            Error::new(
                "ADAPTER_RETENTION_REFERENCE",
                "reference directory cannot be read",
            )
        })?;
        match entries.next() {
            Some(Ok(entry)) => {
                let path = entry.path();
                let file_type = entry.file_type();
                if !file_type.is_ok_and(|kind| kind.is_file() && !kind.is_symlink()) {
                    self.note_deferred_damage()?;
                    return Ok(true);
                }
                let reference = match self.read_json::<OperationReference>(&path) {
                    Ok(reference) => reference,
                    Err(_) => {
                        self.note_deferred_damage()?;
                        return Ok(true);
                    }
                };
                let filename_key = path.file_stem().and_then(|value| value.to_str());
                if reference.version != 1
                    || reference.target_operation_id != operation_id
                    || filename_key
                        != Some(hex_digest(reference.reference_operation_id.as_bytes()).as_str())
                {
                    self.note_deferred_damage()?;
                }
                Ok(true)
            }
            Some(Err(_)) => {
                self.note_deferred_damage()?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    fn write_boot_dispatch_marker(
        &self,
        operation_id: &str,
        receipt: &ModuleReceiptIdentity,
        intent: &Value,
        outcome_sha256: Option<&str>,
        acknowledged_sha256: Option<&str>,
    ) -> Result<()> {
        let boot_id = intent["bridge_boot_id"]
            .as_str()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                Error::new(
                    "ADAPTER_RETENTION_BOOT_MARKER",
                    "dispatch boot identity is missing",
                )
            })?;
        let native = &intent["native"];
        let scope = native["native_scope_key"]
            .as_str()
            .or_else(|| intent["native_scope_key"].as_str())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                Error::new(
                    "ADAPTER_RETENTION_BOOT_MARKER",
                    "dispatch scope identity is missing",
                )
            })?;
        let marker = BootDispatchMarker {
            version: 1,
            operation_id: operation_id.to_owned(),
            operation_key: hex_digest(operation_id.as_bytes()),
            binding_id: receipt.binding_id.clone(),
            binding_generation: receipt.binding_generation,
            input_sha256: receipt.input_sha256.clone(),
            bridge_boot_id: boot_id.to_owned(),
            native_scope_key: scope.to_owned(),
            native_root_id: native["native_root_id"].as_str().map(ToOwned::to_owned),
            native_input_id: native["user_message_uuid"].as_str().map(ToOwned::to_owned),
            native_payload_sha256: native["native_payload_sha256"]
                .as_str()
                .map(ToOwned::to_owned),
            native_payload_bytes: native["native_payload_bytes"].as_u64(),
            outcome_sha256: outcome_sha256.map(ToOwned::to_owned),
            acknowledged_sha256: acknowledged_sha256.map(ToOwned::to_owned),
        };
        let path = self.boot_marker_path(boot_id);
        if path.exists() {
            let existing = self.read_boot_marker(&path)?;
            if existing.operation_id != marker.operation_id
                || existing.operation_key != marker.operation_key
                || existing.binding_id != marker.binding_id
                || existing.binding_generation != marker.binding_generation
                || existing.input_sha256 != marker.input_sha256
                || existing.bridge_boot_id != marker.bridge_boot_id
                || existing.native_scope_key != marker.native_scope_key
                || existing.native_root_id != marker.native_root_id
                || existing.native_input_id != marker.native_input_id
                || existing.native_payload_sha256 != marker.native_payload_sha256
                || existing.native_payload_bytes != marker.native_payload_bytes
            {
                self.note_deferred_damage()?;
                return Err(Error::new(
                    "ADAPTER_RETENTION_BOOT_MARKER",
                    "boot dispatch marker conflicts with an existing exact dispatch",
                ));
            }
            if existing.acknowledged_sha256.is_some()
                && existing.acknowledged_sha256 != marker.acknowledged_sha256
            {
                return Err(Error::new(
                    "ADAPTER_RETENTION_BOOT_MARKER",
                    "boot dispatch acknowledgement digest cannot be replaced",
                ));
            }
            if existing == marker {
                return Ok(());
            }
            self.write_json_replace(&path, &marker)?;
        } else {
            self.write_json_new(&path, &marker)?;
        }
        let readback = self.read_boot_marker(&path)?;
        if readback != marker {
            self.note_deferred_damage()?;
            return Err(Error::new(
                "ADAPTER_RETENTION_BOOT_MARKER",
                "boot dispatch marker failed exact durable readback",
            ));
        }
        Ok(())
    }

    fn read_boot_marker(&self, path: &Path) -> Result<BootDispatchMarker> {
        let marker: BootDispatchMarker = self.read_json(path)?;
        if marker.version != 1
            || !valid_operation_id(&marker.operation_id)
            || marker.operation_key != hex_digest(marker.operation_id.as_bytes())
            || marker.binding_id.trim().is_empty()
            || marker.binding_generation < 1
            || !valid_sha256(&marker.input_sha256)
            || marker.bridge_boot_id.trim().is_empty()
            || marker.native_scope_key.trim().is_empty()
            || marker
                .native_payload_sha256
                .as_deref()
                .is_some_and(|digest| !valid_sha256(digest))
            || marker.native_root_id.as_deref().is_some_and(str::is_empty)
            || marker.native_input_id.as_deref().is_some_and(str::is_empty)
            || marker.native_payload_sha256.is_some() != marker.native_payload_bytes.is_some()
            || marker
                .outcome_sha256
                .as_deref()
                .is_some_and(|digest| !valid_sha256(digest))
            || marker
                .acknowledged_sha256
                .as_deref()
                .is_some_and(|digest| !valid_sha256(digest))
            || marker.acknowledged_sha256.is_some()
                && marker.acknowledged_sha256 != marker.outcome_sha256
            || path.file_stem().and_then(|value| value.to_str())
                != Some(hex_digest(marker.bridge_boot_id.as_bytes()).as_str())
        {
            return Err(Error::new(
                "ADAPTER_RETENTION_BOOT_MARKER",
                "boot dispatch marker has invalid exact identities or acknowledgement",
            ));
        }
        Ok(marker)
    }

    fn write_tombstone(&self, tombstone: &OperationTombstone) -> Result<()> {
        let path = self.tombstone_path(&tombstone.operation_id)?;
        if path.exists() {
            let saved = self.read_tombstone_record(&path, &tombstone.operation_id)?;
            if saved != *tombstone {
                self.note_deferred_damage()?;
                return Err(Error::new(
                    "ADAPTER_RETENTION_MARKER_MISMATCH",
                    "operation tombstone conflicts with the exact acknowledged record",
                ));
            }
            return Ok(());
        }
        self.write_json_new(&path, tombstone)?;
        let saved = self.read_tombstone_record(&path, &tombstone.operation_id)?;
        if saved != *tombstone {
            self.note_deferred_damage()?;
            return Err(Error::new(
                "ADAPTER_RETENTION_MARKER_MISMATCH",
                "operation tombstone failed exact durable readback",
            ));
        }
        Ok(())
    }

    fn read_tombstone_record(
        &self,
        path: &Path,
        expected_operation_id: &str,
    ) -> Result<OperationTombstone> {
        let tombstone: OperationTombstone = self.read_json(path)?;
        if tombstone.version != 1
            || tombstone.operation_id != expected_operation_id
            || tombstone.operation_key != hex_digest(expected_operation_id.as_bytes())
            || tombstone.receipt.operation_id != expected_operation_id
            || !valid_sha256(&tombstone.receipt.input_sha256)
            || !valid_sha256(&tombstone.outcome_sha256)
            || tombstone.outcome["operation_id"] != expected_operation_id
            || tombstone.acknowledged_sha256 != tombstone.outcome_sha256
            || tombstone.outcome_sha256.is_empty()
            || !matches!(
                tombstone.outcome["outcome"].as_str(),
                Some("applied" | "rejected")
            )
            || matches!(
                tombstone.outcome["details"]["completion_condition"].as_str(),
                Some("unknown" | "deferred")
            )
            || tombstone.method.trim().is_empty()
            || tombstone
                .outbox_sequence
                .is_some_and(|sequence| sequence == u64::MAX)
        {
            self.note_deferred_damage()?;
            return Err(Error::new(
                "ADAPTER_RETENTION_MARKER_MISMATCH",
                "operation tombstone identity or acknowledgement is invalid",
            ));
        }
        tombstone.receipt.validate().map_err(|_| {
            Error::new(
                "ADAPTER_RETENTION_MARKER_MISMATCH",
                "compacted receipt is invalid",
            )
        })?;
        Ok(tombstone)
    }

    fn cleanup_item_path(&self, sequence: u64) -> PathBuf {
        self.retention_root
            .join("cleanup")
            .join("items")
            .join(format!("{sequence:020}.json"))
    }

    fn outbox_item_path(&self, sequence: u64) -> PathBuf {
        self.retention_root
            .join("outbox")
            .join("items")
            .join(format!("{sequence:020}.json"))
    }

    fn work_item_path(&self, queue: &str, sequence: u64) -> PathBuf {
        self.retention_root
            .join(queue)
            .join("items")
            .join(format!("{sequence:020}.json"))
    }

    fn outbox_pointer_path(&self, operation_key: &str) -> PathBuf {
        self.retention_root
            .join("outbox")
            .join("by-operation")
            .join(format!("{operation_key}.json"))
    }

    fn cleanup_pointer_path(&self, operation_key: &str) -> PathBuf {
        self.retention_root
            .join("cleanup")
            .join("by-operation")
            .join(format!("{operation_key}.json"))
    }

    fn tombstone_path(&self, operation_id: &str) -> Result<PathBuf> {
        Ok(self
            .retention_root
            .join("operations")
            .join(format!("{}.json", hex_digest(operation_id.as_bytes()))))
    }

    fn boot_marker_path(&self, boot_id: &str) -> PathBuf {
        self.retention_root
            .join("boots")
            .join(format!("{}.json", hex_digest(boot_id.as_bytes())))
    }

    fn references_directory(&self) -> PathBuf {
        self.retention_root.join("references")
    }

    fn retention_state_path(&self) -> PathBuf {
        self.retention_root.join(RETENTION_STATE_FILE)
    }

    fn read_json<T: for<'de> Deserialize<'de>>(&self, path: &Path) -> Result<T> {
        ensure_regular(path).map_err(|_| {
            Error::new(
                "ADAPTER_RETENTION_STATE",
                "retention evidence is not a regular file",
            )
        })?;
        let bytes = read_limited(path, MAX_RETENTION_METADATA_BYTES)?;
        serde_json::from_slice(&bytes)
            .map_err(|_| Error::new("ADAPTER_RETENTION_STATE", "retention evidence is malformed"))
    }

    fn write_json_new<T: Serialize>(&self, path: &Path, value: &T) -> Result<()> {
        let bytes = serde_json::to_vec(value)?;
        if bytes.len() > MAX_RETENTION_METADATA_BYTES {
            return Err(Error::new(
                "ADAPTER_RETENTION_STATE",
                "retention evidence exceeds its size boundary",
            ));
        }
        write_private_new(path, &bytes).map_err(|_| {
            Error::new(
                "ADAPTER_RETENTION_STATE",
                "retention evidence cannot be durably created",
            )
        })
    }

    fn write_json_replace<T: Serialize>(&self, path: &Path, value: &T) -> Result<()> {
        let bytes = serde_json::to_vec(value)?;
        if bytes.len() > MAX_RETENTION_METADATA_BYTES {
            return Err(Error::new(
                "ADAPTER_RETENTION_STATE",
                "retention evidence exceeds its size boundary",
            ));
        }
        replace_private_durable(path, &bytes).map_err(|_| {
            Error::new(
                "ADAPTER_RETENTION_STATE",
                "retention evidence cannot be durably replaced",
            )
        })
    }

    fn note_deferred_unknown(&self) -> Result<()> {
        self.update_counters(|counters| {
            counters.deferred_unknown = counters.deferred_unknown.saturating_add(1)
        })
    }

    fn note_deferred_unacknowledged(&self) -> Result<()> {
        self.update_counters(|counters| {
            counters.deferred_unacknowledged = counters.deferred_unacknowledged.saturating_add(1)
        })
    }

    fn note_deferred_accepted(&self) -> Result<()> {
        self.update_counters(|counters| {
            counters.deferred_accepted = counters.deferred_accepted.saturating_add(1)
        })
    }

    fn note_deferred_reference(&self) -> Result<()> {
        self.update_counters(|counters| {
            counters.deferred_reference = counters.deferred_reference.saturating_add(1)
        })
    }

    fn note_deferred_damage(&self) -> Result<()> {
        self.update_counters(|counters| {
            counters.deferred_damage = counters.deferred_damage.saturating_add(1)
        })
    }

    fn note_outcome_class(&self, outcome: &Value) -> Result<()> {
        match outcome["outcome"].as_str() {
            Some("unknown") => self.note_deferred_unknown(),
            Some("accepted") => self.note_deferred_accepted(),
            _ => self.note_deferred_damage(),
        }
    }

    fn increment_examined(&self) -> Result<()> {
        self.update_counters(|counters| counters.examined = counters.examined.saturating_add(1))
    }

    fn increment_compacted(&self) -> Result<()> {
        self.update_counters(|counters| counters.compacted = counters.compacted.saturating_add(1))
    }

    fn update_counters(&self, update: impl FnOnce(&mut RetentionCounters)) -> Result<()> {
        let mut state = self.read_retention_state()?;
        update(&mut state.counters);
        if state.counters.deferred_unknown > 0
            || state.counters.deferred_unacknowledged > 0
            || state.counters.deferred_accepted > 0
            || state.counters.deferred_reference > 0
            || state.counters.deferred_damage > 0
        {
            state.counters.degraded = true;
        }
        self.write_retention_state(&state)
    }

    fn append(&self, operation_id: &str, record: &JournalRecord) -> Result<()> {
        let path = self.path(operation_id)?;
        let state = self.get(operation_id)?;
        if let Some(tail) = state
            .as_ref()
            .and_then(|state| state.torn_tail_evidence.as_ref())
        {
            return Err(Error::new(
                "ADAPTER_JOURNAL_RECOVERY_UNKNOWN",
                format!(
                    "operation journal append withheld: valid_prefix_bytes={}, tail_bytes={}, tail_sha256={}",
                    tail.valid_prefix_bytes, tail.tail_bytes, tail.tail_sha256
                ),
            ));
        }

        let mut frame = serde_json::to_vec(record)?;
        if frame.len().saturating_add(1) > MAX_RECORD_BYTES {
            return Err(Error::new(
                "ADAPTER_JOURNAL",
                "operation record exceeds its size boundary",
            ));
        }
        frame.push(b'\n');

        if !path.exists() {
            if record.kind != "intent" {
                return Err(Error::new(
                    "ADAPTER_INTENT_MISSING",
                    "the first durable operation record must be an intent",
                ));
            }
            return write_private_new(&path, &frame).map_err(|_| {
                Error::new(
                    "ADAPTER_JOURNAL",
                    "operation intent cannot be durably created",
                )
            });
        }

        ensure_regular(&path)?;
        let current_len = fs::metadata(&path)
            .map_err(|_| Error::new("ADAPTER_JOURNAL", "operation record cannot be inspected"))?
            .len();
        if current_len.saturating_add(frame.len() as u64) > MAX_STATE_BYTES {
            return Err(Error::new(
                "ADAPTER_JOURNAL",
                "operation history exceeds its total size boundary",
            ));
        }
        if current_len == 0 && record.kind != "intent" {
            return Err(Error::new(
                "ADAPTER_INTENT_MISSING",
                "the first durable operation record must be an intent",
            ));
        }

        let mut file = OpenOptions::new()
            .append(true)
            .open(&path)
            .map_err(|_| Error::new("ADAPTER_JOURNAL", "operation record cannot be appended"))?;
        private_permissions(&path, false)?;
        file.write_all(&frame)
            .and_then(|_| file.sync_all())
            .map_err(|_| {
                Error::new(
                    "ADAPTER_JOURNAL",
                    "operation record could not be durably saved",
                )
            })
    }

    fn path(&self, operation_id: &str) -> Result<PathBuf> {
        if operation_id.trim().is_empty()
            || operation_id.len() > 256
            || operation_id.bytes().any(|byte| byte.is_ascii_control())
        {
            return Err(Error::new(
                "ADAPTER_JOURNAL",
                "operation identity is invalid",
            ));
        }
        Ok(self
            .root
            .join(format!("{}.jsonl", hex_digest(operation_id.as_bytes()))))
    }

    fn read_path(
        &self,
        path: &Path,
        expected_operation_id: Option<&str>,
    ) -> Result<OperationState> {
        let mut file = File::open(path)
            .map_err(|_| Error::new("ADAPTER_JOURNAL", "operation record cannot be read"))?;
        let mut bytes = Vec::new();
        Read::by_ref(&mut file)
            .take(MAX_STATE_BYTES.saturating_add(1))
            .read_to_end(&mut bytes)
            .map_err(|_| Error::new("ADAPTER_JOURNAL", "operation record read failed"))?;
        if bytes.len() as u64 > MAX_STATE_BYTES {
            return Err(Error::new(
                "ADAPTER_JOURNAL",
                "operation record must be a bounded regular file",
            ));
        }
        drop(file);
        let state = read_operation_state_bytes(&bytes, path, expected_operation_id)?;
        if state.method.as_deref() != Some("agent.reply") {
            return Ok(state);
        }
        let Some(mut safe_intent) = state.intent.clone() else {
            return Ok(state);
        };
        if !discard_question_reply_payload(&mut safe_intent) {
            return Ok(state);
        }
        let torn_tail_migration_unknown = || {
            Error::new(
                "ADAPTER_JOURNAL_RECOVERY_UNKNOWN",
                "torn operation journal changed during legacy reply privacy migration",
            )
        };
        let (valid_prefix_bytes, torn_suffix) = match state.torn_tail_evidence.as_ref() {
            Some(evidence) => {
                let Some(tail_end) = evidence.valid_prefix_bytes.checked_add(evidence.tail_bytes)
                else {
                    return Err(torn_tail_migration_unknown());
                };
                if evidence.tail_bytes == 0
                    || evidence.valid_prefix_bytes > bytes.len()
                    || tail_end != bytes.len()
                {
                    return Err(torn_tail_migration_unknown());
                }
                let suffix = &bytes[evidence.valid_prefix_bytes..];
                if hex_digest(suffix) != evidence.tail_sha256 {
                    return Err(torn_tail_migration_unknown());
                }
                (evidence.valid_prefix_bytes, Some(suffix))
            }
            None => (bytes.len(), None),
        };
        let mut rewritten_bytes =
            rewrite_question_reply_frames(&bytes[..valid_prefix_bytes], &safe_intent)?.ok_or_else(
                || {
                    Error::new(
                        "ADAPTER_JOURNAL",
                        "legacy question reply disappeared during privacy migration",
                    )
                },
            )?;
        if let Some(suffix) = torn_suffix {
            rewritten_bytes.extend_from_slice(suffix);
        }
        ensure_regular(path)?;
        let mut current_file = OpenOptions::new()
            .read(true)
            .open(path)
            .map_err(|_| Error::new("ADAPTER_JOURNAL", "operation record cannot be read"))?;
        let current_bytes = if torn_suffix.is_some() {
            read_opened_file_for_repair(path, &mut current_file)?
        } else {
            let mut current_bytes = Vec::new();
            Read::by_ref(&mut current_file)
                .take(MAX_STATE_BYTES.saturating_add(1))
                .read_to_end(&mut current_bytes)
                .map_err(|_| Error::new("ADAPTER_JOURNAL", "operation record read failed"))?;
            current_bytes
        };
        if current_bytes != bytes {
            if torn_suffix.is_some() {
                return Err(torn_tail_migration_unknown());
            }
            return Err(Error::new(
                "ADAPTER_JOURNAL",
                "operation record changed during privacy migration",
            ));
        }
        drop(current_file);
        replace_private_durable(path, &rewritten_bytes).map_err(|_| {
            Error::new(
                "ADAPTER_JOURNAL",
                "legacy question reply privacy migration cannot be durably published",
            )
        })?;

        let mut readback_file = OpenOptions::new()
            .read(true)
            .open(path)
            .map_err(|_| Error::new("ADAPTER_JOURNAL", "operation record cannot be read"))?;
        let readback_bytes = if torn_suffix.is_some() {
            read_opened_file_for_repair(path, &mut readback_file)?
        } else {
            let mut readback_bytes = Vec::new();
            Read::by_ref(&mut readback_file)
                .take(MAX_STATE_BYTES.saturating_add(1))
                .read_to_end(&mut readback_bytes)
                .map_err(|_| Error::new("ADAPTER_JOURNAL", "operation record read failed"))?;
            readback_bytes
        };
        if readback_bytes != rewritten_bytes || readback_bytes.len() as u64 > MAX_STATE_BYTES {
            return Err(Error::new(
                "ADAPTER_JOURNAL",
                "legacy question reply privacy migration readback differs from its durable bytes",
            ));
        }
        drop(readback_file);
        let migrated = read_operation_state_bytes(&readback_bytes, path, expected_operation_id)?;
        let torn_suffix_preserved = match (
            state.torn_tail_evidence.as_ref(),
            migrated.torn_tail_evidence.as_ref(),
            torn_suffix,
        ) {
            (None, None, None) => true,
            (Some(before), Some(after), Some(suffix)) => {
                after.tail_bytes == before.tail_bytes
                    && after.tail_sha256 == before.tail_sha256
                    && after.tail_bytes == suffix.len()
                    && after.valid_prefix_bytes.checked_add(after.tail_bytes)
                        == Some(readback_bytes.len())
                    && hex_digest(suffix) == after.tail_sha256
                    && readback_bytes.get(after.valid_prefix_bytes..) == Some(suffix)
            }
            _ => false,
        };
        if migrated.intent.as_ref() != Some(&safe_intent)
            || state.operation_key != migrated.operation_key
            || state.operation_id != migrated.operation_id
            || state.receipt != migrated.receipt
            || state.method != migrated.method
            || state.outcome != migrated.outcome
            || state.outcome_sha256 != migrated.outcome_sha256
            || state.acknowledged_sha256 != migrated.acknowledged_sha256
            || state.outbox_sequence != migrated.outbox_sequence
            || state.compacted != migrated.compacted
            || !torn_suffix_preserved
            || state.recovery_unknown() != migrated.recovery_unknown()
            || state.native_replay_permitted() != migrated.native_replay_permitted()
        {
            return Err(Error::new(
                "ADAPTER_JOURNAL",
                "legacy question reply privacy migration changed operation custody",
            ));
        }
        Ok(migrated)
    }
}

pub(crate) fn is_item_local_operation_damage(error: &Error) -> bool {
    match error.code.as_str() {
        "ADAPTER_JOURNAL" => {
            matches!(
                error.message.as_str(),
                "operation identity is invalid"
                    | "operation record must be a bounded regular file"
                    | "operation record is invalid"
                    | "operation record identity changed"
                    | "intent receipt is missing"
                    | "intent method is missing"
                    | "intent marker is missing"
                    | "intent receipt is invalid"
                    | "operation intent conflicts with its saved identity"
                    | "saved outcome is missing"
                    | "outcome lacks its exact saved intent"
                    | "saved outcome digest or identity changed"
                    | "saved outcome changed its durable outbox sequence"
                    | "acknowledgement digest is missing"
                    | "acknowledgement differs from the exact saved outcome"
                    | "unknown operation record kind"
            ) || error
                .message
                .starts_with("operation record damaged at byte ")
        }
        "ADAPTER_RETENTION_MARKER_MISMATCH" => true,
        "ADAPTER_RETENTION_STATE" => error.message == "retention evidence is malformed",
        _ => false,
    }
}

fn read_operation_state_bytes(
    bytes: &[u8],
    path: &Path,
    expected_operation_id: Option<&str>,
) -> Result<OperationState> {
    let mut state = OperationState {
        operation_key: path
            .file_stem()
            .and_then(|value| value.to_str())
            .ok_or_else(|| Error::new("ADAPTER_JOURNAL", "operation filename is invalid"))?
            .to_owned(),
        operation_id: expected_operation_id.unwrap_or_default().to_owned(),
        ..OperationState::default()
    };
    // File I/O is complete before this pure, in-memory domain decoder runs;
    // no Store call can be misclassified as damaged journal data.
    let verdict = scan_jsonl(bytes, MAX_RECORD_BYTES, |_, frame| {
        let record: JournalRecord = serde_json::from_slice(&frame[..frame.len() - 1])
            .map_err(|_| Error::new("ADAPTER_JOURNAL", "operation record is invalid"))?;
        apply_record(&mut state, record)
    })?;
    match verdict {
        JsonlScanVerdict::Complete { .. } => {}
        JsonlScanVerdict::TornLastFrame {
            valid_bytes,
            tail_digest,
            tail_bytes,
            ..
        } => {
            state.torn_tail_evidence = Some(TornTailEvidence {
                valid_prefix_bytes: valid_bytes,
                tail_sha256: tail_digest,
                tail_bytes,
            });
        }
        JsonlScanVerdict::Damaged {
            valid_prefix_bytes,
            reason,
        } => {
            return Err(Error::new(
                "ADAPTER_JOURNAL",
                format!("operation record damaged at byte {valid_prefix_bytes}: {reason:?}"),
            ));
        }
    }
    Ok(state)
}

fn discard_question_reply_payload(intent: &mut Value) -> bool {
    let Some(native) = intent.get_mut("native").and_then(Value::as_object_mut) else {
        return false;
    };
    let Some(reply) = native.get("reply") else {
        return false;
    };
    let is_question_reply = native.get("interaction_kind").and_then(Value::as_str)
        == Some("question")
        || reply["type"].as_str() == Some("question")
        || (reply["updated_input"]["questions"].is_array()
            && reply["updated_input"]["answers"].is_object());
    let has_historical_digest = native
        .get("reply_sha256")
        .and_then(Value::as_str)
        .is_some_and(valid_sha256);
    is_question_reply && has_historical_digest && native.remove("reply").is_some()
}

fn rewrite_question_reply_frames(bytes: &[u8], safe_intent: &Value) -> Result<Option<Vec<u8>>> {
    let mut rewritten = Vec::with_capacity(bytes.len());
    let mut changed = false;
    for frame in bytes.split_inclusive(|byte| *byte == b'\n') {
        if !frame.ends_with(b"\n") {
            return Ok(None);
        }
        let mut record: JournalRecord = serde_json::from_slice(&frame[..frame.len() - 1])?;
        let is_legacy_question_intent = record.kind == "intent"
            && record.method.as_deref() == Some("agent.reply")
            && record
                .intent
                .as_mut()
                .is_some_and(discard_question_reply_payload);
        if is_legacy_question_intent {
            record.intent = Some(safe_intent.clone());
            rewritten.extend_from_slice(&serde_json::to_vec(&record)?);
            rewritten.push(b'\n');
            changed = true;
        } else {
            rewritten.extend_from_slice(frame);
        }
    }
    Ok(changed.then_some(rewritten))
}

fn apply_record(state: &mut OperationState, record: JournalRecord) -> Result<()> {
    if record.version != 1
        || (!state.operation_id.is_empty() && record.operation_id != state.operation_id)
    {
        return Err(Error::new(
            "ADAPTER_JOURNAL",
            "operation record identity changed",
        ));
    }
    state.operation_id = record.operation_id;
    match record.kind.as_str() {
        "intent" => {
            let receipt = record
                .receipt
                .ok_or_else(|| Error::new("ADAPTER_JOURNAL", "intent receipt is missing"))?;
            let method = record
                .method
                .ok_or_else(|| Error::new("ADAPTER_JOURNAL", "intent method is missing"))?;
            let intent = record
                .intent
                .ok_or_else(|| Error::new("ADAPTER_JOURNAL", "intent marker is missing"))?;
            receipt
                .validate()
                .map_err(|_| Error::new("ADAPTER_JOURNAL", "intent receipt is invalid"))?;
            if receipt.operation_id != state.operation_id
                || state.receipt.as_ref().is_some_and(|old| old != &receipt)
                || state.method.as_ref().is_some_and(|old| old != &method)
                || state.intent.as_ref().is_some_and(|old| old != &intent)
            {
                return Err(Error::new(
                    "ADAPTER_JOURNAL",
                    "operation intent conflicts with its saved identity",
                ));
            }
            state.receipt = Some(receipt);
            state.method = Some(method);
            state.intent = Some(intent);
        }
        "outcome" => {
            let outcome = record
                .outcome
                .ok_or_else(|| Error::new("ADAPTER_JOURNAL", "saved outcome is missing"))?;
            if state.receipt.is_none() || outcome["operation_id"] != state.operation_id {
                return Err(Error::new(
                    "ADAPTER_JOURNAL",
                    "outcome lacks its exact saved intent",
                ));
            }
            let digest = digest_json(&outcome)?;
            if record.digest.as_deref() != Some(digest.as_str())
                || state.outcome.as_ref().is_some_and(|old| old != &outcome)
            {
                return Err(Error::new(
                    "ADAPTER_JOURNAL",
                    "saved outcome digest or identity changed",
                ));
            }
            state.outcome = Some(outcome);
            state.outcome_sha256 = Some(digest);
            if let Some(sequence) = record.outbox_sequence {
                if state
                    .outbox_sequence
                    .is_some_and(|previous| previous != sequence)
                {
                    return Err(Error::new(
                        "ADAPTER_JOURNAL",
                        "saved outcome changed its durable outbox sequence",
                    ));
                }
                state.outbox_sequence = Some(sequence);
            }
        }
        "acknowledged" => {
            let digest = record.digest.ok_or_else(|| {
                Error::new("ADAPTER_JOURNAL", "acknowledgement digest is missing")
            })?;
            if state.outcome_sha256.as_deref() != Some(digest.as_str()) {
                return Err(Error::new(
                    "ADAPTER_JOURNAL",
                    "acknowledgement differs from the exact saved outcome",
                ));
            }
            state.acknowledged_sha256 = Some(digest);
        }
        _ => {
            return Err(Error::new(
                "ADAPTER_JOURNAL",
                "unknown operation record kind",
            ));
        }
    }
    Ok(())
}

fn state_from_tombstone(tombstone: OperationTombstone) -> OperationState {
    OperationState {
        operation_key: tombstone.operation_key,
        operation_id: tombstone.operation_id,
        receipt: Some(tombstone.receipt),
        method: Some(tombstone.method),
        intent: Some(tombstone.intent),
        outcome: Some(tombstone.outcome),
        outcome_sha256: Some(tombstone.outcome_sha256),
        acknowledged_sha256: Some(tombstone.acknowledged_sha256),
        outbox_sequence: tombstone.outbox_sequence,
        compacted: true,
        torn_tail_evidence: None,
    }
}

fn compact_intent(intent: &Value) -> Value {
    let mut compact = serde_json::Map::new();
    for key in [
        "method",
        "binding_id",
        "generation",
        "input_sha256",
        "bridge_boot_id",
        "native_scope_key",
        "native_root_id",
        "replay_permitted",
    ] {
        if let Some(value) = intent.get(key) {
            compact.insert(key.to_owned(), value.clone());
        }
    }
    let mut native = serde_json::Map::new();
    for key in [
        "native_scope_key",
        "native_root_id",
        "user_message_uuid",
        "prompt_sha256",
        "prompt_bytes",
        "task_snapshot_sha256",
        "native_payload_sha256",
        "native_payload_bytes",
        "native_transport_sha256",
        "native_transport_bytes",
        "task_prompt_sha256",
        "task_prompt_bytes",
        "task_prompt_contract_revision",
        "initial_dispatch",
        "interaction_request_id",
        "interaction_kind",
        "tool_name",
        "request_sha256",
        "reply_sha256",
        "result_target_operation_id",
        "reconcile_target_operation_id",
    ] {
        if let Some(value) = intent.get("native").and_then(|value| value.get(key)) {
            native.insert(key.to_owned(), value.clone());
        }
    }
    if let Some(admission) = intent
        .get("native")
        .and_then(|value| value.get("dispatch_admission"))
        .and_then(Value::as_object)
    {
        let mut compact_admission = serde_json::Map::new();
        for key in [
            "native_payload_sha256",
            "native_payload_bytes",
            "task_prompt_sha256",
            "task_prompt_bytes",
            "task_prompt_contract_revision",
        ] {
            if let Some(value) = admission.get(key) {
                compact_admission.insert(key.to_owned(), value.clone());
            }
        }
        if !compact_admission.is_empty() {
            native.insert(
                "dispatch_admission".to_owned(),
                Value::Object(compact_admission),
            );
        }
    }
    if !native.is_empty() {
        compact.insert("native".to_owned(), Value::Object(native));
    }
    Value::Object(compact)
}

fn compact_outcome(outcome: &Value) -> Value {
    let mut compact = serde_json::Map::new();
    for key in [
        "operation_id",
        "outcome",
        "native_scope_key",
        "native_root_id",
        "turn_id",
        "native_input_id",
    ] {
        if let Some(value) = outcome.get(key) {
            compact.insert(key.to_owned(), value.clone());
        }
    }
    let mut details = serde_json::Map::new();
    for key in [
        "completion_condition",
        "diagnostic_code",
        "replay_permitted",
        "module_receipt",
        "interaction_request_id",
        "interaction_kind",
        "request_sha256",
        "reply_sha256",
        "callback_reply_acknowledged",
        "target_operation_id",
        "target_module_receipt",
        "artifact_ref",
    ] {
        if let Some(value) = outcome.get("details").and_then(|details| details.get(key)) {
            details.insert(key.to_owned(), value.clone());
        }
    }
    if !details.is_empty() {
        compact.insert("details".to_owned(), Value::Object(details));
    }
    Value::Object(compact)
}

fn reference_targets(operation_id: &str, intent: &Value) -> Result<Vec<String>> {
    let Some(native) = intent.get("native").and_then(Value::as_object) else {
        return Ok(Vec::new());
    };
    let mut targets = Vec::new();
    for field in [
        "result_target_operation_id",
        "reconcile_target_operation_id",
    ] {
        let Some(value) = native.get(field) else {
            continue;
        };
        let target = value.as_str().filter(|target| {
            !target.trim().is_empty()
                && target.len() <= 256
                && !target.bytes().any(|byte| byte.is_ascii_control())
        });
        let Some(target) = target else {
            return Err(Error::new(
                "ADAPTER_RETENTION_REFERENCE",
                "native operation reference has an invalid exact target identity",
            ));
        };
        if target == operation_id {
            return Err(Error::new(
                "ADAPTER_RETENTION_REFERENCE",
                "operation cannot retain itself as a target reference",
            ));
        }
        if !targets.iter().any(|existing| existing == target) {
            targets.push(target.to_owned());
        }
    }
    Ok(targets)
}

fn ensure_child_directory(parent: &Path, child: &str) -> Result<PathBuf> {
    ensure_directory(parent)?;
    let relative = Path::new(child);
    if relative.as_os_str().is_empty()
        || relative
            .components()
            .any(|component| !matches!(component, std::path::Component::Normal(_)))
    {
        return Err(Error::new(
            "ADAPTER_STATE",
            "state subdirectory identity is invalid",
        ));
    }
    let path = parent.join(relative);
    fs::create_dir_all(&path)
        .map_err(|_| Error::new("ADAPTER_STATE", "state subdirectory cannot be created"))?;
    ensure_directory(&path)?;
    private_permissions(&path, true)?;
    Ok(path)
}

fn ensure_relative_directory(root: &Path, relative: &str) -> Result<()> {
    let mut current = root.to_owned();
    for component in Path::new(relative).components() {
        let std::path::Component::Normal(name) = component else {
            return Err(Error::new(
                "ADAPTER_STATE",
                "state subdirectory path is invalid",
            ));
        };
        let child = current.join(name);
        match fs::symlink_metadata(&child) {
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
            Ok(_) => {
                return Err(Error::new(
                    "ADAPTER_STATE",
                    "state subdirectory must be a real directory",
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir(&child).map_err(|_| {
                    Error::new("ADAPTER_STATE", "state subdirectory cannot be created")
                })?;
            }
            Err(_) => {
                return Err(Error::new(
                    "ADAPTER_STATE",
                    "state subdirectory cannot be inspected",
                ));
            }
        }
        ensure_directory(&child)?;
        private_permissions(&child, true)?;
        current = child;
    }
    Ok(())
}

fn remove_exact_file(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {}
        Ok(_) => {
            return Err(Error::new(
                "ADAPTER_RETENTION_STATE",
                "retention cleanup target is not an exact regular file",
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => {
            return Err(Error::new(
                "ADAPTER_RETENTION_STATE",
                "retention cleanup target cannot be inspected",
            ));
        }
    }
    let removal = remove_private_acknowledged(path).map_err(|_| {
        Error::new(
            "ADAPTER_RETENTION_STATE",
            "retention target cannot be removed",
        )
    })?;
    if removal.durability == swarm_process::RemovalDurability::Unknown {
        eprintln!(
            "Claude adapter cleanup durability is unknown; retained ACK/readback state remains authoritative"
        );
    }
    Ok(())
}

fn valid_operation_id(value: &str) -> bool {
    !value.trim().is_empty()
        && value.len() <= 256
        && !value.bytes().any(|byte| byte.is_ascii_control())
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn ensure_directory(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|_| Error::new("ADAPTER_STATE", "state directory cannot be inspected"))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(Error::new(
            "ADAPTER_STATE",
            "state directory must be a real directory",
        ));
    }
    Ok(())
}

fn ensure_regular(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|_| Error::new("ADAPTER_JOURNAL", "operation record cannot be inspected"))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > MAX_STATE_BYTES
    {
        return Err(Error::new(
            "ADAPTER_JOURNAL",
            "operation record must be a bounded regular file",
        ));
    }
    Ok(())
}

fn torn_ack_recovery_unknown() -> Error {
    Error::new(
        "ADAPTER_JOURNAL_RECOVERY_UNKNOWN",
        "torn journal suffix could not be safely removed from the exact saved outcome prefix",
    )
}

fn read_opened_file_for_repair(path: &Path, file: &mut File) -> Result<Vec<u8>> {
    if !opened_file_matches_path(file, path) {
        return Err(torn_ack_recovery_unknown());
    }
    let initial_metadata = file.metadata().map_err(|_| torn_ack_recovery_unknown())?;
    if initial_metadata.len() > MAX_STATE_BYTES {
        return Err(torn_ack_recovery_unknown());
    }

    let mut bytes = Vec::new();
    Read::by_ref(file)
        .take(MAX_STATE_BYTES.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|_| torn_ack_recovery_unknown())?;
    if bytes.len() as u64 > MAX_STATE_BYTES || initial_metadata.len() != bytes.len() as u64 {
        return Err(torn_ack_recovery_unknown());
    }
    if !opened_file_matches_path(file, path) {
        return Err(torn_ack_recovery_unknown());
    }

    file.seek(SeekFrom::Start(0))
        .map_err(|_| torn_ack_recovery_unknown())?;
    let mut confirmed_bytes = Vec::new();
    Read::by_ref(file)
        .take(MAX_STATE_BYTES.saturating_add(1))
        .read_to_end(&mut confirmed_bytes)
        .map_err(|_| torn_ack_recovery_unknown())?;
    let final_metadata = file.metadata().map_err(|_| torn_ack_recovery_unknown())?;
    if confirmed_bytes != bytes
        || confirmed_bytes.len() as u64 > MAX_STATE_BYTES
        || final_metadata.len() != bytes.len() as u64
        || !opened_file_matches_path(file, path)
    {
        return Err(torn_ack_recovery_unknown());
    }
    Ok(bytes)
}

fn opened_file_matches_path(file: &File, path: &Path) -> bool {
    if ensure_regular(path).is_err() {
        return false;
    }
    swarm_process::opened_regular_file_matches_path(file, path).unwrap_or(false)
}

fn read_limited(path: &Path, maximum: usize) -> Result<Vec<u8>> {
    let file = File::open(path)
        .map_err(|_| Error::new("ADAPTER_STATE", "state identity cannot be read"))?;
    let mut bytes = Vec::new();
    file.take((maximum + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| Error::new("ADAPTER_STATE", "state identity read failed"))?;
    if bytes.len() > maximum {
        return Err(Error::new(
            "ADAPTER_STATE",
            "state identity exceeds its boundary",
        ));
    }
    Ok(bytes)
}

pub fn digest_json(value: &Value) -> Result<String> {
    let bytes = serde_json::to_vec(value)?;
    Ok(hex_digest(&bytes))
}

pub fn digest_bytes(bytes: &[u8]) -> String {
    hex_digest(bytes)
}

fn hex_digest(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn safe_diagnostic_code(value: &str) -> &str {
    if !value.is_empty()
        && value.len() <= 64
        && value.as_bytes()[0].is_ascii_uppercase()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        value
    } else {
        "ADAPTER_UNCERTAIN_AFTER_EFFECT_MARKER"
    }
}

#[cfg(test)]
mod recovery_tests {
    use super::*;
    use serde_json::json;
    use std::io::Write;

    #[test]
    fn shutdown_drain_recovers_past_damaged_outbox_and_requires_exact_ack() {
        let root =
            std::env::temp_dir().join(format!("claude-shutdown-drain-{}", uuid::Uuid::new_v4()));
        let journal =
            OperationJournal::open(&root, "shutdown-binding", 1, "scope", &"b".repeat(64)).unwrap();
        let receipt: ModuleReceiptIdentity = serde_json::from_value(json!({
            "schema_version":1,"module_id":"eliot.claude",
            "artifact":{"artifact_id":crate::ARTIFACT_ID,"version":crate::ARTIFACT_VERSION},
            "protocol":{"major":1,"minor":0},"binding_id":"shutdown-binding",
            "binding_generation":1,"operation_id":"shutdown-operation","input_sha256":"a".repeat(64)
        }))
        .unwrap();
        journal
            .write_intent(
                "shutdown-operation",
                &receipt,
                "agent.shutdown",
                &json!({"owned_scope":"owned_sdk_transport"}),
            )
            .unwrap();
        let outcome = json!({"operation_id":"shutdown-operation","outcome":"applied","details":{"completion_condition":"owned_resource_released","native_replay":false}});
        journal
            .save_outcome_value("shutdown-operation", &outcome)
            .unwrap();
        let old_sequence = journal
            .get("shutdown-operation")
            .unwrap()
            .unwrap()
            .outbox_sequence
            .unwrap();
        let damaged_item = journal.outbox_item_path(old_sequence);
        std::fs::write(&damaged_item, b"{").unwrap();
        assert!(journal.pending_outcomes().unwrap().is_empty());
        assert!(journal.has_terminal_shutdown().unwrap());
        assert_eq!(
            journal
                .require_terminal_shutdown_acknowledged()
                .unwrap_err()
                .code,
            "ADAPTER_SHUTDOWN_ACK_PENDING"
        );
        journal.prepare_terminal_shutdown_drain().unwrap();
        let pending = journal.pending_outcomes().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].outcome.as_ref(), Some(&outcome));
        assert_eq!(pending[0].operation_id, "shutdown-operation");
        assert_eq!(std::fs::read(&damaged_item).unwrap(), b"{");
        journal.acknowledge("shutdown-operation", &outcome).unwrap();
        journal.require_terminal_shutdown_acknowledged().unwrap();
        journal.process_cleanup_batch(MAX_CURSOR_STEPS).unwrap();
        assert!(journal.path("shutdown-operation").unwrap().is_file());
        drop(journal);
        let reopened =
            OperationJournal::open(&root, "shutdown-binding", 1, "scope", &"b".repeat(64)).unwrap();
        assert!(reopened.has_terminal_shutdown().unwrap());
        reopened.require_terminal_shutdown_acknowledged().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn shutdown_drain_cannot_succeed_without_retained_shutdown_evidence() {
        let root =
            std::env::temp_dir().join(format!("claude-shutdown-missing-{}", uuid::Uuid::new_v4()));
        let journal =
            OperationJournal::open(&root, "shutdown-binding", 1, "scope", &"b".repeat(64)).unwrap();
        assert_eq!(
            journal
                .require_terminal_shutdown_acknowledged()
                .unwrap_err()
                .code,
            "ADAPTER_SHUTDOWN_ACK_PENDING"
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn lost_result_ack_recovery_retains_exact_intent_and_never_allows_native_replay() {
        let root = std::env::temp_dir().join(format!(
            "swarm-claude-result-recovery-{}",
            uuid::Uuid::new_v4()
        ));
        let receipt: ModuleReceiptIdentity = serde_json::from_value(json!({
            "schema_version":1,"module_id":"eliot.claude",
            "artifact":{"artifact_id":crate::ARTIFACT_ID,"version":crate::ARTIFACT_VERSION},
            "protocol":{"major":1,"minor":0},"binding_id":"binding-result",
            "binding_generation":1,"operation_id":"result-operation","input_sha256":"a".repeat(64)
        }))
        .unwrap();
        receipt.validate().unwrap();
        let intent = json!({"kind":"result","target_operation_id":"dispatch-operation"});
        let journal =
            OperationJournal::open(&root, "binding-result", 1, "fixture-scope", &"b".repeat(64))
                .unwrap();
        journal
            .write_intent("result-operation", &receipt, "agent.result", &intent)
            .unwrap();
        let retained = std::fs::read(journal.path("result-operation").unwrap()).unwrap();
        drop(journal);
        let journal =
            OperationJournal::open(&root, "binding-result", 1, "fixture-scope", &"b".repeat(64))
                .unwrap();
        journal
            .retain_exact_intent_for_recovery("result-operation", &receipt, "agent.result", &intent)
            .unwrap();
        assert_eq!(
            std::fs::read(journal.path("result-operation").unwrap()).unwrap(),
            retained
        );
        let changed = json!({"kind":"result","target_operation_id":"foreign-dispatch"});
        assert_eq!(
            journal
                .retain_exact_intent_for_recovery(
                    "result-operation",
                    &receipt,
                    "agent.result",
                    &changed
                )
                .unwrap_err()
                .code,
            "ADAPTER_INTENT_CONFLICT"
        );
        assert_eq!(
            journal
                .write_intent("result-operation", &receipt, "agent.result", &intent)
                .unwrap_err()
                .code,
            "ADAPTER_JOURNAL_RECOVERY_UNKNOWN"
        );
        let unknown = json!({"operation_id":"result-operation","outcome":"unknown","details":{"reason":"RESULT_ACK_LOST"}});
        journal
            .save_outcome_value("result-operation", &unknown)
            .unwrap();
        journal
            .save_outcome_value("result-operation", &unknown)
            .unwrap();
        let state = journal.get("result-operation").unwrap().unwrap();
        assert!(!state.native_replay_permitted());
        assert_eq!(state.intent.as_ref(), Some(&intent));
        assert_eq!(journal.pending_outcomes().unwrap().len(), 1);
        let different = json!({"operation_id":"result-operation","outcome":"completed"});
        assert_eq!(
            journal
                .save_outcome_value("result-operation", &different)
                .unwrap_err()
                .code,
            "ADAPTER_OUTCOME_CONFLICT"
        );
        journal.acknowledge("result-operation", &unknown).unwrap();
        assert!(journal.pending_outcomes().unwrap().is_empty());
        drop(journal);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn legacy_question_reply_migration_scrubs_payload_and_preserves_effect_evidence() {
        let root = std::env::temp_dir().join(format!(
            "swarm-claude-legacy-question-intent-{}",
            uuid::Uuid::new_v4()
        ));
        let make_receipt = |operation_id: &str| -> ModuleReceiptIdentity {
            let receipt: ModuleReceiptIdentity = serde_json::from_value(json!({
                "schema_version":1,"module_id":"eliot.claude",
                "artifact":{"artifact_id":crate::ARTIFACT_ID,"version":crate::ARTIFACT_VERSION},
                "protocol":{"major":1,"minor":0},"binding_id":"binding-legacy-question",
                "binding_generation":1,"operation_id":operation_id,"input_sha256":"d".repeat(64)
            }))
            .unwrap();
            receipt.validate().unwrap();
            receipt
        };
        let applied_outcome = json!({
            "operation_id":"legacy-question-applied",
            "outcome":"applied",
            "details":{"completion_condition":"applied","replay_permitted":false}
        });
        let raw_question = "ghp_0123456789abcdef0123456789abcdef0123";
        let raw_answer = "legacy-question-answer-private-marker";
        let questions = json!([{
            "question":raw_question,
            "header":"Choice",
            "options":[
                {"label":"Keep","description":"Keep the current value"},
                {"label":"Skip","description":"Leave it unchanged"}
            ],
            "multiSelect":false
        }]);
        let mut answers = serde_json::Map::new();
        answers.insert(raw_question.to_owned(), json!(raw_answer));
        let reply = json!({
            "type":"question",
            "updated_input":{
                "questions":questions,
                "answers":Value::Object(answers)
            }
        });
        let reply_sha256 = digest_json(&reply).unwrap();
        let applied_reply_intent = json!({
            "bridge_boot_id":"legacy-boot",
            "native_scope_key":"fixture-scope",
            "native_root_id":"fixture-root",
            "native":{
                "interaction_request_id":"legacy-question-applied-request",
                "interaction_kind":"question",
                "tool_name":"AskUserQuestion",
                "request_sha256":"b".repeat(64),
                "input_sha256":"c".repeat(64),
                "reply_sha256":reply_sha256,
                "reply":reply,
                "native_scope_key":"fixture-scope",
                "native_root_id":"fixture-root"
            }
        });
        let unknown_id = "legacy-question-unknown";
        let mut unknown_intent = applied_reply_intent.clone();
        unknown_intent["native"]["interaction_request_id"] =
            json!("legacy-question-unknown-request");

        let journal = OperationJournal::open(
            &root,
            "binding-legacy-question",
            1,
            "fixture-scope",
            &"e".repeat(64),
        )
        .unwrap();
        let mut expected = Vec::new();
        for (operation_id, intent, outcome, acknowledged) in [
            (
                "legacy-question-applied",
                applied_reply_intent,
                Some(applied_outcome.clone()),
                true,
            ),
            (unknown_id, unknown_intent, None, false),
        ] {
            let receipt = make_receipt(operation_id);
            let intent_record = JournalRecord {
                version: 1,
                operation_id: operation_id.to_owned(),
                kind: "intent".to_owned(),
                receipt: Some(receipt.clone()),
                method: Some("agent.reply".to_owned()),
                intent: Some(intent.clone()),
                outcome: None,
                digest: None,
                outbox_sequence: None,
            };
            let mut bytes = serde_json::to_vec(&intent_record).unwrap();
            bytes.push(b'\n');
            let outcome_sha256 = outcome
                .as_ref()
                .map(|outcome| digest_json(outcome).unwrap());
            if let Some(outcome) = outcome.as_ref() {
                let digest = outcome_sha256.clone().unwrap();
                let outcome_record = JournalRecord {
                    version: 1,
                    operation_id: operation_id.to_owned(),
                    kind: "outcome".to_owned(),
                    receipt: None,
                    method: None,
                    intent: None,
                    outcome: Some(outcome.clone()),
                    digest: Some(digest.clone()),
                    outbox_sequence: None,
                };
                bytes.extend(serde_json::to_vec(&outcome_record).unwrap());
                bytes.push(b'\n');
                if acknowledged {
                    let acknowledgement = JournalRecord {
                        version: 1,
                        operation_id: operation_id.to_owned(),
                        kind: "acknowledged".to_owned(),
                        receipt: None,
                        method: None,
                        intent: None,
                        outcome: None,
                        digest: Some(digest),
                        outbox_sequence: None,
                    };
                    bytes.extend(serde_json::to_vec(&acknowledgement).unwrap());
                    bytes.push(b'\n');
                }
            }
            assert_eq!(
                String::from_utf8_lossy(&bytes)
                    .matches(raw_question)
                    .count(),
                2
            );
            write_private_new(&journal.path(operation_id).unwrap(), &bytes).unwrap();
            expected.push((operation_id, receipt, outcome_sha256, intent));
        }
        drop(journal);

        let journal = OperationJournal::open(
            &root,
            "binding-legacy-question",
            1,
            "fixture-scope",
            &"e".repeat(64),
        )
        .unwrap();
        assert_eq!(
            journal
                .recover_startup_window("ADAPTER_RESTART_AFTER_EFFECT_MARKER")
                .unwrap(),
            2
        );
        assert!(journal.startup_recovery_complete().unwrap());

        for (operation_id, receipt, old_outcome_sha256, old_intent) in expected {
            let path = journal.path(operation_id).unwrap();
            let persisted = std::fs::read_to_string(path).unwrap();
            assert!(!persisted.contains(raw_question));
            assert!(!persisted.contains(raw_answer));
            let state = journal.get(operation_id).unwrap().unwrap();
            assert_eq!(state.receipt.as_ref(), Some(&receipt));
            assert!(!state.native_replay_permitted());
            let native = state.intent.as_ref().unwrap().get("native").unwrap();
            assert!(native.get("reply").is_none());
            assert_eq!(
                native["request_sha256"],
                old_intent["native"]["request_sha256"]
            );
            assert_eq!(native["reply_sha256"], old_intent["native"]["reply_sha256"]);

            if operation_id == "legacy-question-applied" {
                let outcome_sha256 = old_outcome_sha256.unwrap();
                assert_eq!(state.outcome, Some(applied_outcome.clone()));
                assert_eq!(
                    state.outcome_sha256.as_deref(),
                    Some(outcome_sha256.as_str())
                );
                assert_eq!(
                    state.acknowledged_sha256.as_deref(),
                    Some(outcome_sha256.as_str())
                );
                journal.acknowledge(operation_id, &applied_outcome).unwrap();
            } else {
                let outcome = state.outcome.as_ref().unwrap();
                assert_eq!(outcome["operation_id"], operation_id);
                assert_eq!(outcome["outcome"], "unknown");
                assert_eq!(outcome["details"]["replay_permitted"], false);
                assert_eq!(state.acknowledged_sha256, None);
                assert_eq!(
                    state.outcome_sha256.as_deref(),
                    Some(digest_json(outcome).unwrap().as_str())
                );
            }

            let retry = journal
                .write_intent(operation_id, &receipt, "agent.reply", &old_intent)
                .expect_err("migrated question intent cannot authorize a fresh effect");
            assert_eq!(retry.code, "ADAPTER_INTENT_EXISTS");
        }
        drop(journal);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn legacy_question_reply_migration_preserves_torn_tail_and_outcome_identity() {
        let root = std::env::temp_dir().join(format!(
            "swarm-claude-legacy-reply-torn-tail-{}",
            uuid::Uuid::new_v4()
        ));
        let make_receipt = |operation_id: &str| -> ModuleReceiptIdentity {
            let receipt: ModuleReceiptIdentity = serde_json::from_value(json!({
                "schema_version":1,"module_id":"eliot.claude",
                "artifact":{"artifact_id":crate::ARTIFACT_ID,"version":crate::ARTIFACT_VERSION},
                "protocol":{"major":1,"minor":0},"binding_id":"binding-legacy-torn",
                "binding_generation":1,"operation_id":operation_id,"input_sha256":"f".repeat(64)
            }))
            .unwrap();
            receipt.validate().unwrap();
            receipt
        };
        let raw_question = "legacy-private-question-key";
        let raw_answer = "legacy-private-answer-value";
        let questions = json!([{
            "question":raw_question,
            "header":"Choice",
            "options":[
                {"label":"Keep","description":"Keep the current value"},
                {"label":"Skip","description":"Leave it unchanged"}
            ],
            "multiSelect":false
        }]);
        let mut answers = serde_json::Map::new();
        answers.insert(raw_question.to_owned(), json!(raw_answer));
        let reply = json!({
            "type":"question",
            "updated_input":{"questions":questions,"answers":Value::Object(answers)}
        });
        let reply_sha256 = digest_json(&reply).unwrap();
        let applied_outcome = json!({
            "operation_id":"legacy-torn-applied",
            "outcome":"applied",
            "details":{"completion_condition":"applied","replay_permitted":false}
        });
        let torn_suffix = b"{\"incomplete_tail\":";
        let journal = OperationJournal::open(
            &root,
            "binding-legacy-torn",
            1,
            "fixture-scope",
            &"e".repeat(64),
        )
        .unwrap();
        for (operation_id, with_applied_ack) in [
            ("legacy-torn-unknown", false),
            ("legacy-torn-applied", true),
        ] {
            let receipt = make_receipt(operation_id);
            let intent = json!({
                "bridge_boot_id":"legacy-boot",
                "native_scope_key":"fixture-scope",
                "native_root_id":"fixture-root",
                "native":{
                    "interaction_request_id":format!("{}-request", operation_id),
                    "interaction_kind":"question",
                    "tool_name":"AskUserQuestion",
                    "request_sha256":"b".repeat(64),
                    "input_sha256":"c".repeat(64),
                    "reply_sha256":reply_sha256,
                    "reply":reply,
                    "native_scope_key":"fixture-scope",
                    "native_root_id":"fixture-root"
                }
            });
            let intent_record = JournalRecord {
                version: 1,
                operation_id: operation_id.to_owned(),
                kind: "intent".to_owned(),
                receipt: Some(receipt),
                method: Some("agent.reply".to_owned()),
                intent: Some(intent),
                outcome: None,
                digest: None,
                outbox_sequence: None,
            };
            let mut bytes = serde_json::to_vec(&intent_record).unwrap();
            bytes.push(b'\n');
            let outcome = with_applied_ack.then(|| {
                let mut outcome = applied_outcome.clone();
                outcome["operation_id"] = json!(operation_id);
                outcome
            });
            if let Some(outcome) = outcome.as_ref() {
                let outcome_digest = digest_json(outcome).unwrap();
                bytes.extend(
                    serde_json::to_vec(&JournalRecord {
                        version: 1,
                        operation_id: operation_id.to_owned(),
                        kind: "outcome".to_owned(),
                        receipt: None,
                        method: None,
                        intent: None,
                        outcome: Some(outcome.clone()),
                        digest: Some(outcome_digest.clone()),
                        outbox_sequence: None,
                    })
                    .unwrap(),
                );
                bytes.push(b'\n');
                bytes.extend(
                    serde_json::to_vec(&JournalRecord {
                        version: 1,
                        operation_id: operation_id.to_owned(),
                        kind: "acknowledged".to_owned(),
                        receipt: None,
                        method: None,
                        intent: None,
                        outcome: None,
                        digest: Some(outcome_digest),
                        outbox_sequence: None,
                    })
                    .unwrap(),
                );
                bytes.push(b'\n');
            }
            bytes.extend_from_slice(torn_suffix);
            assert!(String::from_utf8_lossy(&bytes).contains(raw_question));
            assert!(String::from_utf8_lossy(&bytes).contains(raw_answer));
            write_private_new(&journal.path(operation_id).unwrap(), &bytes).unwrap();
        }
        drop(journal);

        let journal = OperationJournal::open(
            &root,
            "binding-legacy-torn",
            1,
            "fixture-scope",
            &"e".repeat(64),
        )
        .unwrap();
        for operation_id in ["legacy-torn-unknown", "legacy-torn-applied"] {
            let state = journal.get(operation_id).unwrap().unwrap();
            let evidence = state
                .torn_tail_evidence
                .as_ref()
                .expect("torn suffix remains explicit");
            let path = journal.path(operation_id).unwrap();
            let persisted = std::fs::read(&path).unwrap();
            assert_eq!(state.operation_id, operation_id);
            assert_eq!(state.method.as_deref(), Some("agent.reply"));
            assert_eq!(evidence.tail_bytes, torn_suffix.len());
            assert_eq!(evidence.tail_sha256, hex_digest(torn_suffix));
            assert_eq!(
                evidence.valid_prefix_bytes + evidence.tail_bytes,
                persisted.len()
            );
            assert_eq!(&persisted[evidence.valid_prefix_bytes..], torn_suffix);
            let prefix = String::from_utf8_lossy(&persisted[..evidence.valid_prefix_bytes]);
            assert!(!prefix.contains(raw_question));
            assert!(!prefix.contains(raw_answer));
            let persisted_text = String::from_utf8_lossy(&persisted);
            assert!(!persisted_text.contains(raw_question));
            assert!(!persisted_text.contains(raw_answer));
            let native = state.intent.as_ref().unwrap().get("native").unwrap();
            assert!(native.get("reply").is_none());
            assert_eq!(native["reply_sha256"], reply_sha256);
            assert!(!state.native_replay_permitted());

            if operation_id == "legacy-torn-unknown" {
                assert!(state.recovery_unknown());
                assert!(state.outcome.is_none());
                assert!(state.acknowledged_sha256.is_none());
                assert_eq!(
                    journal
                        .write_intent(
                            operation_id,
                            &make_receipt(operation_id),
                            "agent.reply",
                            &json!({}),
                        )
                        .unwrap_err()
                        .code,
                    "ADAPTER_JOURNAL_RECOVERY_UNKNOWN"
                );
            } else {
                let outcome = state.outcome.as_ref().unwrap();
                let expected_outcome = json!({
                    "operation_id":operation_id,
                    "outcome":"applied",
                    "details":{"completion_condition":"applied","replay_permitted":false}
                });
                let outcome_digest = digest_json(&expected_outcome).unwrap();
                assert_eq!(outcome, &expected_outcome);
                assert_eq!(
                    state.outcome_sha256.as_deref(),
                    Some(outcome_digest.as_str())
                );
                assert_eq!(
                    state.acknowledged_sha256.as_deref(),
                    Some(outcome_digest.as_str())
                );
                assert!(!state.recovery_unknown());
            }
        }
        assert!(!journal.startup_recovery_complete().unwrap());
        drop(journal);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn legacy_migration_is_bounded_resumable_and_append_safe() {
        let root = std::env::temp_dir().join(format!(
            "swarm-claude-bounded-migration-{}",
            uuid::Uuid::new_v4()
        ));
        let make_receipt = |operation_id: &str| -> ModuleReceiptIdentity {
            let receipt: ModuleReceiptIdentity = serde_json::from_value(json!({
                "schema_version":1,"module_id":"eliot.claude",
                "artifact":{"artifact_id":crate::ARTIFACT_ID,"version":crate::ARTIFACT_VERSION},
                "protocol":{"major":1,"minor":0},"binding_id":"binding-legacy-window",
                "binding_generation":1,"operation_id":operation_id,"input_sha256":"d".repeat(64)
            }))
            .unwrap();
            receipt.validate().unwrap();
            receipt
        };
        let open = || {
            OperationJournal::open(
                &root,
                "binding-legacy-window",
                1,
                "fixture-scope",
                &"e".repeat(64),
            )
            .unwrap()
        };
        let journal = open();
        for index in 0..64 {
            let operation_id = format!("legacy-window-{index:03}");
            let receipt = make_receipt(&operation_id);
            journal
                .write_intent(
                    &operation_id,
                    &receipt,
                    "agent.reply",
                    &json!({"kind":"legacy"}),
                )
                .unwrap();
        }
        let torn_id = "legacy-window-torn";
        let torn_receipt = make_receipt(torn_id);
        journal
            .write_intent(
                torn_id,
                &torn_receipt,
                "agent.reply",
                &json!({"kind":"legacy"}),
            )
            .unwrap();
        let torn_path = journal.path(torn_id).unwrap();
        std::fs::OpenOptions::new()
            .append(true)
            .open(&torn_path)
            .unwrap()
            .write_all(b"{")
            .unwrap();
        let torn_bytes = std::fs::read(&torn_path).unwrap();

        // The service-loop variant never lists names inline. A requested
        // operation remains directly readable while cold inventory is absent.
        assert_eq!(
            journal
                .try_recover_startup_window_for_boot(
                    "ADAPTER_RESTART_AFTER_EFFECT_MARKER",
                    "boot-current"
                )
                .unwrap(),
            None
        );
        let targeted = journal.get("legacy-window-063").unwrap().unwrap();
        assert!(targeted.recovery_unknown());
        assert!(!targeted.native_replay_permitted());

        assert_eq!(
            journal
                .recover_startup_window("ADAPTER_RESTART_AFTER_EFFECT_MARKER")
                .unwrap(),
            MAX_CURSOR_STEPS
        );
        let saved_cursor = journal.read_retention_state().unwrap().startup_cursor;
        assert!(saved_cursor.is_some());
        assert!(!journal.startup_recovery_complete().unwrap());
        drop(journal);

        let journal = open();
        assert_eq!(
            journal.read_retention_state().unwrap().startup_cursor,
            saved_cursor
        );
        assert_eq!(
            journal
                .recover_startup_window("ADAPTER_RESTART_AFTER_EFFECT_MARKER")
                .unwrap(),
            MAX_CURSOR_STEPS
        );
        assert_ne!(
            journal.read_retention_state().unwrap().startup_cursor,
            saved_cursor
        );

        let appended_id = "legacy-window-appended-after-restart";
        let appended_receipt = make_receipt(appended_id);
        let cursor_before_append = journal.read_retention_state().unwrap().startup_cursor;
        journal
            .write_intent(
                appended_id,
                &appended_receipt,
                "agent.reply",
                &json!({"kind":"appended","bridge_boot_id":"boot-current"}),
            )
            .unwrap();
        assert_eq!(
            journal.read_retention_state().unwrap().startup_cursor,
            cursor_before_append
        );
        assert_eq!(
            journal
                .recover_startup_window_for_boot(
                    "ADAPTER_RESTART_AFTER_EFFECT_MARKER",
                    "boot-current"
                )
                .unwrap(),
            1
        );
        assert!(journal.startup_recovery_complete().unwrap());
        let appended_live = journal.get(appended_id).unwrap().unwrap();
        assert!(appended_live.recovery_unknown());
        assert!(appended_live.outcome.is_none());
        assert!(!appended_live.native_replay_permitted());
        drop(journal);

        // A restart makes the exact saved operation eligible for recovery;
        // direct ID lookup preserves custody even before its scan window.
        let journal = open();
        let appended_after_restart = journal.get(appended_id).unwrap().unwrap();
        assert!(appended_after_restart.recovery_unknown());
        assert!(!appended_after_restart.native_replay_permitted());
        assert_eq!(
            journal
                .recover_startup_window_for_boot("ADAPTER_RESTART_AFTER_EFFECT_MARKER", "boot-next")
                .unwrap(),
            MAX_CURSOR_STEPS
        );
        assert_eq!(
            journal
                .recover_startup_window_for_boot("ADAPTER_RESTART_AFTER_EFFECT_MARKER", "boot-next")
                .unwrap(),
            MAX_CURSOR_STEPS
        );
        assert_eq!(
            journal
                .recover_startup_window_for_boot("ADAPTER_RESTART_AFTER_EFFECT_MARKER", "boot-next")
                .unwrap(),
            2
        );
        assert!(journal.startup_recovery_complete().unwrap());

        for index in 0..64 {
            let operation_id = format!("legacy-window-{index:03}");
            let state = journal.get(&operation_id).unwrap().unwrap();
            assert_eq!(state.outcome.as_ref().unwrap()["outcome"], "unknown");
            assert!(!state.native_replay_permitted());
        }
        let appended = journal.get(appended_id).unwrap().unwrap();
        assert_eq!(appended.outcome.as_ref().unwrap()["outcome"], "unknown");
        assert!(!appended.native_replay_permitted());
        let torn = journal.get(torn_id).unwrap().unwrap();
        assert!(torn.torn_tail_evidence.is_some());
        assert!(!torn.native_replay_permitted());
        assert_eq!(std::fs::read(&torn_path).unwrap(), torn_bytes);

        // A new live intent does not rewind completed traversal. The next boot
        // resets the per-boot scan and recovers the exact operation in windows.
        let runtime_id = "legacy-window-appended-after-completion";
        let runtime_receipt = make_receipt(runtime_id);
        journal
            .write_intent(
                runtime_id,
                &runtime_receipt,
                "agent.reply",
                &json!({"kind":"runtime-append"}),
            )
            .unwrap();
        assert!(journal.startup_recovery_complete().unwrap());
        assert_eq!(journal.read_retention_state().unwrap().startup_cursor, None);
        drop(journal);

        let journal = open();
        assert_eq!(
            journal
                .recover_startup_window("ADAPTER_RESTART_AFTER_EFFECT_MARKER")
                .unwrap(),
            MAX_CURSOR_STEPS
        );
        assert_eq!(
            journal
                .recover_startup_window("ADAPTER_RESTART_AFTER_EFFECT_MARKER")
                .unwrap(),
            MAX_CURSOR_STEPS
        );
        assert_eq!(
            journal
                .recover_startup_window("ADAPTER_RESTART_AFTER_EFFECT_MARKER")
                .unwrap(),
            3
        );
        assert!(journal.startup_recovery_complete().unwrap());
        let runtime = journal.get(runtime_id).unwrap().unwrap();
        assert_eq!(runtime.outcome.as_ref().unwrap()["outcome"], "unknown");
        assert!(!runtime.native_replay_permitted());

        drop(journal);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn claude_current_boot_dispatch_compacts_before_delete_and_reads_anchor_directly() {
        let root =
            std::env::temp_dir().join(format!("swarm-claude-boot-anchor-{}", uuid::Uuid::new_v4()));
        let boot_id = "boot-compact-anchor-current";
        let operation_id = "dispatch-last-releasable-journal";
        let receipt: ModuleReceiptIdentity = serde_json::from_value(json!({
            "schema_version":1,"module_id":"eliot.claude",
            "artifact":{"artifact_id":crate::ARTIFACT_ID,"version":crate::ARTIFACT_VERSION},
            "protocol":{"major":1,"minor":0},"binding_id":"binding-boot-anchor",
            "binding_generation":1,"operation_id":operation_id,"input_sha256":"f".repeat(64)
        }))
        .unwrap();
        receipt.validate().unwrap();
        let intent = json!({
            "bridge_boot_id":boot_id,
            "native":{"native_scope_key":"fixture-scope","native_root_id":"root-current"}
        });
        let outcome = json!({
            "operation_id":operation_id,"outcome":"applied",
            "details":{"completion_condition":"completed"}
        });
        let open = || {
            OperationJournal::open(
                &root,
                "binding-boot-anchor",
                1,
                "fixture-scope",
                &"f".repeat(64),
            )
            .unwrap()
        };

        let journal = open();
        journal
            .write_intent(operation_id, &receipt, "task.dispatch", &intent)
            .unwrap();
        journal.save_outcome_value(operation_id, &outcome).unwrap();
        journal.acknowledge(operation_id, &outcome).unwrap();

        let operation_path = journal.path(operation_id).unwrap();
        assert!(
            !operation_path.exists(),
            "the releasable journal is deleted"
        );
        let compacted = journal.get(operation_id).unwrap().unwrap();
        assert!(compacted.compacted);
        assert_eq!(compacted.acknowledged_sha256, compacted.outcome_sha256);
        let marker_path = journal.boot_marker_path(boot_id);
        let marker = journal.read_boot_marker(&marker_path).unwrap();
        assert_eq!(marker.bridge_boot_id, boot_id);
        assert_eq!(marker.operation_id, operation_id);
        assert_eq!(marker.acknowledged_sha256, marker.outcome_sha256);
        assert!(journal.has_task_dispatch_for_boot(boot_id).unwrap());
        drop(journal);

        let reopened = open();
        assert!(
            !reopened.startup_recovery_complete().unwrap(),
            "the boot anchor remains readable before legacy recovery completes"
        );
        assert!(reopened.has_task_dispatch_for_boot(boot_id).unwrap());
        let compacted_after_restart = reopened.get(operation_id).unwrap().unwrap();
        assert!(compacted_after_restart.compacted);
        assert!(!compacted_after_restart.native_replay_permitted());
        assert!(!operation_path.exists());

        drop(reopened);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn pending_outcomes_discards_torn_suffix_without_ack_then_advances_past_corruption() {
        let root = std::env::temp_dir().join(format!(
            "swarm-claude-torn-ack-recovery-{}",
            uuid::Uuid::new_v4()
        ));
        let make_receipt = |operation_id: &str| -> ModuleReceiptIdentity {
            let receipt: ModuleReceiptIdentity = serde_json::from_value(json!({
                "schema_version":1,"module_id":"eliot.claude",
                "artifact":{"artifact_id":crate::ARTIFACT_ID,"version":crate::ARTIFACT_VERSION},
                "protocol":{"major":1,"minor":0},"binding_id":"binding-torn-ack",
                "binding_generation":1,"operation_id":operation_id,"input_sha256":"c".repeat(64)
            }))
            .unwrap();
            receipt.validate().unwrap();
            receipt
        };
        let acknowledged = json!({
            "operation_id":"acknowledged-operation","outcome":"unknown",
            "details":{"reason":"FIXTURE_ACK_RECOVERY"}
        });
        let unproved = json!({
            "operation_id":"unproved-operation","outcome":"unknown",
            "details":{"reason":"FIXTURE_ACK_RECOVERY"}
        });
        let unrelated = json!({
            "operation_id":"unrelated-operation","outcome":"unknown",
            "details":{"reason":"FIXTURE_ACK_RECOVERY"}
        });
        let journal = OperationJournal::open(
            &root,
            "binding-torn-ack",
            1,
            "fixture-scope",
            &"c".repeat(64),
        )
        .unwrap();
        for (operation_id, outcome) in [
            ("acknowledged-operation", &acknowledged),
            ("unproved-operation", &unproved),
            ("unrelated-operation", &unrelated),
        ] {
            let receipt = make_receipt(operation_id);
            let intent = json!({"kind":"result","target_operation_id":"dispatch-operation"});
            journal
                .write_intent(operation_id, &receipt, "agent.result", &intent)
                .unwrap();
            journal.save_outcome_value(operation_id, outcome).unwrap();
        }

        let acknowledged_path = journal.path("acknowledged-operation").unwrap();
        let acknowledged_prefix = std::fs::read(&acknowledged_path).unwrap();
        std::fs::OpenOptions::new()
            .append(true)
            .open(&acknowledged_path)
            .unwrap()
            .write_all(b"{")
            .unwrap();

        let unproved_path = journal.path("unproved-operation").unwrap();
        std::fs::OpenOptions::new()
            .append(true)
            .open(&unproved_path)
            .unwrap()
            .write_all(b"{not-json}\n")
            .unwrap();
        let unproved_corrupt_bytes = std::fs::read(&unproved_path).unwrap();
        drop(journal);

        let journal = OperationJournal::open(
            &root,
            "binding-torn-ack",
            1,
            "fixture-scope",
            &"c".repeat(64),
        )
        .unwrap();
        assert_eq!(
            journal
                .recover_startup_window("ADAPTER_RESTART_AFTER_EFFECT_MARKER")
                .unwrap(),
            3
        );
        assert!(journal.startup_recovery_complete().unwrap());
        let pending = journal.pending_outcomes().unwrap();
        assert_eq!(
            pending
                .iter()
                .map(|operation| operation.operation_id.as_str())
                .collect::<Vec<_>>(),
            vec!["acknowledged-operation"]
        );
        assert_eq!(
            std::fs::read(&acknowledged_path).unwrap(),
            acknowledged_prefix
        );
        let acknowledged_state = journal.get("acknowledged-operation").unwrap().unwrap();
        assert!(acknowledged_state.torn_tail_evidence.is_none());
        assert!(acknowledged_state.acknowledged_sha256.is_none());
        assert!(!acknowledged_state.native_replay_permitted());

        let host_response = json!({"recorded":true});
        assert_eq!(host_response["recorded"], true);
        journal
            .acknowledge("acknowledged-operation", &acknowledged)
            .unwrap();
        let digest = digest_json(&acknowledged).unwrap();
        let ack_record = JournalRecord {
            version: 1,
            operation_id: "acknowledged-operation".to_owned(),
            kind: "acknowledged".to_owned(),
            receipt: None,
            method: None,
            intent: None,
            outcome: None,
            digest: Some(digest.clone()),
            outbox_sequence: None,
        };
        let mut expected_acknowledged_bytes = acknowledged_prefix;
        expected_acknowledged_bytes.extend_from_slice(&serde_json::to_vec(&ack_record).unwrap());
        expected_acknowledged_bytes.push(b'\n');
        assert_eq!(
            std::fs::read(&acknowledged_path).unwrap(),
            expected_acknowledged_bytes
        );
        let acknowledged_state = journal.get("acknowledged-operation").unwrap().unwrap();
        assert_eq!(
            acknowledged_state.acknowledged_sha256.as_deref(),
            Some(digest.as_str())
        );
        assert!(!acknowledged_state.native_replay_permitted());

        let pending = journal.pending_outcomes().unwrap();
        assert_eq!(
            pending
                .iter()
                .map(|operation| operation.operation_id.as_str())
                .collect::<Vec<_>>(),
            vec!["unrelated-operation"]
        );
        assert_eq!(
            std::fs::read(&unproved_path).unwrap(),
            unproved_corrupt_bytes
        );
        assert_eq!(
            journal.get("unproved-operation").unwrap_err().code,
            "ADAPTER_JOURNAL"
        );

        drop(journal);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[cfg(test)]
mod journal65_parity_tests {
    use super::*;
    use serde_json::json;
    use std::io::Write;

    const PARITY_BINDING: &str = "parity-binding";
    const PARITY_SCOPE: &str = "parity-scope";

    fn parity_receipt(operation_id: &str) -> ModuleReceiptIdentity {
        serde_json::from_value(json!({
            "schema_version": 1,
            "module_id": "eliot.claude",
            "artifact": {"artifact_id": crate::ARTIFACT_ID, "version": crate::ARTIFACT_VERSION},
            "protocol": {"major": 1, "minor": 0},
            "binding_id": PARITY_BINDING,
            "binding_generation": 1,
            "operation_id": operation_id,
            "input_sha256": "c".repeat(64)
        }))
        .unwrap()
    }

    fn parity_intent() -> Value {
        json!({"kind": "result", "target_operation_id": "dispatch-operation"})
    }

    fn parity_outcome(operation_id: &str) -> Value {
        json!({"operation_id": operation_id, "outcome": "unknown", "details": {"reason": "PARITY_FIXTURE"}})
    }

    fn open_parity_journal(root: &Path) -> OperationJournal {
        OperationJournal::open(root, PARITY_BINDING, 1, PARITY_SCOPE, &"b".repeat(64)).unwrap()
    }

    fn fresh_root(prefix: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "swarm-claude-journal65-{}-{}",
            prefix,
            uuid::Uuid::new_v4()
        ))
    }

    #[test]
    fn get_and_read_path_agree_on_complete_torn_and_interior_damage() {
        let root = fresh_root("parity");
        let journal = open_parity_journal(&root);
        let operation_id = "parity-operation";
        let receipt = parity_receipt(operation_id);
        let intent = parity_intent();
        journal
            .write_intent(operation_id, &receipt, "agent.result", &intent)
            .unwrap();
        let outcome = parity_outcome(operation_id);
        journal.save_outcome_value(operation_id, &outcome).unwrap();
        let path = journal.path(operation_id).unwrap();
        let complete_bytes = std::fs::read(&path).unwrap();

        let via_get = journal.get(operation_id).unwrap().expect("state via get");
        let via_path = journal.read_path(&path, Some(operation_id)).unwrap();
        assert!(via_get.torn_tail_evidence.is_none());
        assert!(via_path.torn_tail_evidence.is_none());
        assert_eq!(via_get.operation_id, via_path.operation_id);
        assert_eq!(via_get.receipt, via_path.receipt);
        assert_eq!(via_get.method, via_path.method);
        assert_eq!(via_get.intent, via_path.intent);
        assert_eq!(via_get.outcome, via_path.outcome);
        assert_eq!(via_get.outcome, Some(outcome.clone()));
        assert_eq!(via_get.outcome_sha256, via_path.outcome_sha256);
        assert_eq!(via_get.acknowledged_sha256, via_path.acknowledged_sha256);
        assert_eq!(format!("{:?}", via_get), format!("{:?}", via_path));
        assert_eq!(std::fs::read(&path).unwrap(), complete_bytes);

        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"{")
            .unwrap();
        let torn_bytes = std::fs::read(&path).unwrap();
        let via_get = journal
            .get(operation_id)
            .unwrap()
            .expect("torn state via get");
        let via_path = journal.read_path(&path, Some(operation_id)).unwrap();
        let get_tail = via_get
            .torn_tail_evidence
            .clone()
            .expect("get reports torn tail");
        let path_tail = via_path
            .torn_tail_evidence
            .clone()
            .expect("read_path reports torn tail");
        assert_eq!(get_tail, path_tail);
        assert!(!via_get.native_replay_permitted());
        assert!(!via_path.native_replay_permitted());
        assert_eq!(via_get.outcome, via_path.outcome);
        assert_eq!(format!("{:?}", via_get), format!("{:?}", via_path));
        assert_eq!(std::fs::read(&path).unwrap(), torn_bytes);

        let newline = complete_bytes
            .iter()
            .position(|byte| *byte == b'\n')
            .expect("complete intent line")
            + 1;
        let mut damaged = Vec::new();
        damaged.extend_from_slice(&complete_bytes[..newline]);
        damaged.extend_from_slice(b"{not-json}\n");
        damaged.extend_from_slice(&complete_bytes[newline..]);
        std::fs::write(&path, &damaged).unwrap();
        let get_err = journal.get(operation_id).unwrap_err();
        let path_err = journal.read_path(&path, Some(operation_id)).unwrap_err();
        assert_eq!(get_err.code, "ADAPTER_JOURNAL");
        assert_eq!(path_err.code, "ADAPTER_JOURNAL");
        assert_eq!(get_err.message, path_err.message);
        assert!(get_err.message.contains("damaged at byte"));
        assert_eq!(std::fs::read(&path).unwrap(), damaged);

        drop(journal);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn repair_refuses_file_changed_after_scan_and_keeps_new_bytes() {
        let root = fresh_root("changed");
        let journal = open_parity_journal(&root);
        let operation_id = "parity-changed-operation";
        let receipt = parity_receipt(operation_id);
        let intent = parity_intent();
        journal
            .write_intent(operation_id, &receipt, "agent.result", &intent)
            .unwrap();
        let outcome = parity_outcome(operation_id);
        journal.save_outcome_value(operation_id, &outcome).unwrap();
        let path = journal.path(operation_id).unwrap();
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"{")
            .unwrap();
        let scanned = journal
            .get(operation_id)
            .unwrap()
            .expect("torn tail scanned");
        let evidence = scanned
            .torn_tail_evidence
            .clone()
            .expect("torn tail scanned");
        let torn_bytes = std::fs::read(&path).unwrap();
        assert_eq!(
            torn_bytes.len(),
            evidence.valid_prefix_bytes + evidence.tail_bytes
        );

        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"{not-json}\n")
            .unwrap();
        let changed_bytes = std::fs::read(&path).unwrap();
        assert!(changed_bytes.len() > torn_bytes.len());
        assert!(changed_bytes.starts_with(&torn_bytes));

        let err = journal
            .repair_torn_acknowledgement(operation_id, &outcome)
            .unwrap_err();
        assert_eq!(err.code, "ADAPTER_JOURNAL_RECOVERY_UNKNOWN");
        assert_eq!(std::fs::read(&path).unwrap(), changed_bytes);

        let rescan = journal.get(operation_id).unwrap_err();
        assert_eq!(rescan.code, "ADAPTER_JOURNAL");
        assert!(rescan.message.contains("damaged at byte"));

        drop(journal);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn c_oversized_complete_and_torn_frames_are_damage() {
        let root = fresh_root("oversized");
        let journal = open_parity_journal(&root);
        let operation_id = "parity-oversized-operation";
        let path = journal.path(operation_id).unwrap();

        let mut oversized_complete = vec![b'x'; MAX_RECORD_BYTES];
        oversized_complete.push(b'\n');
        std::fs::write(&path, &oversized_complete).unwrap();
        let complete_by_id = journal.get(operation_id).unwrap_err();
        let complete_by_path = journal.read_path(&path, Some(operation_id)).unwrap_err();
        let expected_complete = format!(
            "operation record damaged at byte 0: OversizedCompleteRecord {{ record_bytes: {}, max_record_bytes: {} }}",
            MAX_RECORD_BYTES + 1,
            MAX_RECORD_BYTES
        );
        assert_eq!(complete_by_id.code, "ADAPTER_JOURNAL");
        assert_eq!(complete_by_id.message, expected_complete);
        assert_eq!(complete_by_path.code, "ADAPTER_JOURNAL");
        assert_eq!(complete_by_path.message, expected_complete);
        assert_eq!(std::fs::read(&path).unwrap(), oversized_complete);

        let oversized_torn = vec![b'x'; MAX_RECORD_BYTES + 1];
        std::fs::write(&path, &oversized_torn).unwrap();
        let torn_by_id = journal.get(operation_id).unwrap_err();
        let torn_by_path = journal.read_path(&path, Some(operation_id)).unwrap_err();
        let expected_torn = format!(
            "operation record damaged at byte 0: OversizedTornTail {{ tail_bytes: {}, max_record_bytes: {} }}",
            MAX_RECORD_BYTES + 1,
            MAX_RECORD_BYTES
        );
        assert_eq!(torn_by_id.code, "ADAPTER_JOURNAL");
        assert_eq!(torn_by_id.message, expected_torn);
        assert_eq!(torn_by_path.code, "ADAPTER_JOURNAL");
        assert_eq!(torn_by_path.message, expected_torn);
        assert_eq!(std::fs::read(&path).unwrap(), oversized_torn);

        drop(journal);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn d_zero_byte_journal_is_complete_empty_state() {
        let root = fresh_root("zero-byte");
        let journal = open_parity_journal(&root);
        let operation_id = "parity-zero-byte-operation";
        let path = journal.path(operation_id).unwrap();
        std::fs::write(&path, b"").unwrap();

        let by_path = journal.read_path(&path, Some(operation_id)).unwrap();
        assert!(journal.get(operation_id).unwrap().is_none());
        assert_eq!(by_path.operation_id, operation_id);
        assert!(by_path.receipt.is_none());
        assert!(by_path.torn_tail_evidence.is_none());
        assert_eq!(std::fs::read(&path).unwrap(), b"");

        drop(journal);
        std::fs::remove_dir_all(root).unwrap();
    }
}
