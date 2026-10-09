//! Small durable adapter receipt journal. It is scoped below the existing
//! module owner directory and never stores prompts, credentials, or SDK text.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};
use swarm_contracts::{
    error::{Error, Result},
    runtime::{ModuleReceiptIdentity, RuntimeOutcome},
};
use swarm_process::{
    JsonlScanVerdict, private_permissions, remove_private_durable, replace_private_durable,
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

pub struct OperationJournal {
    root: PathBuf,
    retention_root: PathBuf,
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
        };
        journal.ensure_retention_state()?;
        journal.migrate_existing_records()?;
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
        self.register_references_for_intent(operation_id, intent)?;
        if method == "task.dispatch" {
            self.write_boot_dispatch_marker(operation_id, receipt, intent, None, None)?;
        }
        self.append(operation_id, &record)
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
        let current = self
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
            let operation = match self.get(&entry.operation_id) {
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
            let name = path
                .file_stem()
                .and_then(|value| value.to_str())
                .ok_or_else(|| Error::new("ADAPTER_JOURNAL", "operation filename is invalid"))?;
            let state = self.read_path(&path, None)?;
            if !state.operation_id.is_empty() && hex_digest(state.operation_id.as_bytes()) != name {
                return Err(Error::new(
                    "ADAPTER_JOURNAL",
                    "operation filename does not match its identity",
                ));
            }
            if (state.receipt.is_some() && state.outcome.is_none())
                || state.torn_tail_evidence.is_some()
            {
                pending.push(state);
            }
        }
        Ok(pending)
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

    pub fn recover_uncertain(&self) -> Result<()> {
        self.recover_uncertain_with_code("ADAPTER_RESTART_AFTER_EFFECT_MARKER")
    }

    pub fn recover_uncertain_with_code(&self, diagnostic_code: &str) -> Result<()> {
        let diagnostic_code = safe_diagnostic_code(diagnostic_code);
        for state in self.unresolved()? {
            if state.torn_tail_evidence.is_some() {
                // The valid prefix and tail digest remain observable through
                // `unresolved`; appending an unknown outcome would attach it
                // to the torn frame and destroy the original evidence.
                continue;
            }
            let receipt = state.receipt.as_ref().ok_or_else(|| {
                Error::new("ADAPTER_JOURNAL", "unresolved operation has no receipt")
            })?;
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
            self.save_outcome(&state.operation_id, &outcome)?;
        }
        Ok(())
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

    fn migrate_existing_records(&self) -> Result<()> {
        if self.read_retention_state()?.migration_complete {
            return Ok(());
        }
        let entries = fs::read_dir(&self.root).map_err(|_| {
            Error::new(
                "ADAPTER_RETENTION_STATE",
                "journal directory cannot be listed",
            )
        })?;
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(_) => {
                    self.note_deferred_damage()?;
                    continue;
                }
            };
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("jsonl") {
                continue;
            }
            let name = path
                .file_stem()
                .and_then(|value| value.to_str())
                .unwrap_or_default()
                .to_owned();
            let state = match ensure_regular(&path).and_then(|()| self.read_path(&path, None)) {
                Ok(state) => state,
                Err(_) => {
                    self.note_deferred_damage()?;
                    continue;
                }
            };
            if state.operation_id.is_empty() || hex_digest(state.operation_id.as_bytes()) != name {
                self.note_deferred_damage()?;
                continue;
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
                } else {
                    // The exact unacknowledged outcome remains available to the host outbox.
                }
                if outcome["outcome"] == "unknown" || outcome["outcome"] == "accepted" {
                    self.note_outcome_class(outcome)?;
                }
            } else if state.torn_tail_evidence.is_some() {
                self.note_deferred_damage()?;
            }
        }
        let mut state = self.read_retention_state()?;
        state.migration_complete = true;
        self.write_retention_state(&state)
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
        if pointer.phase == WorkPhase::Deferred && pointer.acknowledged_sha256.is_none() {
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
        file.read_to_end(&mut bytes)
            .map_err(|_| Error::new("ADAPTER_JOURNAL", "operation record read failed"))?;
        let mut state = OperationState {
            operation_key: path
                .file_stem()
                .and_then(|value| value.to_str())
                .ok_or_else(|| Error::new("ADAPTER_JOURNAL", "operation filename is invalid"))?
                .to_owned(),
            operation_id: expected_operation_id.unwrap_or_default().to_owned(),
            ..OperationState::default()
        };
        // File I/O is complete before this pure, in-memory domain decoder
        // runs; no Store call can be misclassified as damaged journal data.
        let verdict = scan_jsonl(&bytes, MAX_RECORD_BYTES, |_, frame| {
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
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(_) => {
            return Err(Error::new(
                "ADAPTER_RETENTION_STATE",
                "retention cleanup target cannot be inspected",
            ));
        }
    }
    remove_private_durable(path).map(|_| ()).map_err(|_| {
        Error::new(
            "ADAPTER_RETENTION_STATE",
            "retention target cannot be removed",
        )
    })
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
