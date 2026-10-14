mod retention;
#[cfg(test)]
mod retention_tests;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::HashSet,
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
use swarm_contracts::{
    error::{Error, Result},
    runtime::{EffectOutcome, ModuleReceiptIdentity, RuntimeOutcome, TaskDispatchAdmissionReceipt},
};
use swarm_process::{
    JsonlScanVerdict, RemovalOutcome, private_permissions, remove_private_acknowledged,
    replace_private_durable, scan_jsonl, write_private_new,
};

const RECORD_LIMIT: usize = 1_048_576;
const HISTORY_LIMIT: usize = 8 * RECORD_LIMIT;
const OUTBOX_BATCH: usize = 32;
const RETENTION_ACK_INTERVAL: usize = 32;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct OperationIntent {
    pub version: u32,
    pub operation_id: String,
    /// Store-validated identity for this exact original Operation request.
    pub module_receipt: ModuleReceiptIdentity,
    pub method: String,
    pub binding_id: String,
    pub generation: i64,
    pub native_scope_key: String,
    pub native_root_id: Option<String>,
    pub native_input_id: Option<String>,
    pub prompt_sha256: Option<String>,
    pub prompt_bytes: Option<u64>,
    #[serde(default)]
    pub reconcile_target_operation_id: Option<String>,
    /// Exact admitted result selector and target receipt used for the
    /// readback-only input-status page. Missing on older journal records.
    #[serde(default)]
    pub result_input_status: Option<ResultInputStatusIntent>,
    /// Exact normalized assistant result selector and its parent link. Missing
    /// on older journal records; a result page is never inferred from a
    /// timeline position.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_assistant: Option<ResultAssistantIntent>,
    /// Exact normalized dispatch admission persisted before the native POST.
    /// Missing on legacy records; a normalized descriptor fails closed if the
    /// record cannot prove this pre-effect marker.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dispatch_admission: Option<TaskDispatchAdmissionReceipt>,
    pub route_sha256: String,
    pub model: Value,
    pub marker: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResultInputStatusIntent {
    pub input_operation_id: String,
    pub native_session_id: String,
    pub native_input_id: String,
    pub target_module_receipt: swarm_contracts::runtime::ModuleReceiptIdentity,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResultAssistantIntent {
    pub input_operation_id: String,
    pub native_session_id: String,
    pub native_input_id: String,
    pub assistant_message_id: String,
    pub assistant_parent_id: String,
    pub target_input_sha256: String,
    pub selector_sha256: String,
    pub target_module_receipt: swarm_contracts::runtime::ModuleReceiptIdentity,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct JournalRecord {
    version: u32,
    operation_id: String,
    kind: String,
    #[serde(default)]
    intent: Option<OperationIntent>,
    #[serde(default)]
    outcome: Option<Value>,
    #[serde(default)]
    outcome_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    result_params: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    result_sha256: Option<String>,
}

#[derive(Debug, Default)]
pub struct OperationHistory {
    pub intent: Option<OperationIntent>,
    pub outcome: Option<Value>,
    pub outcome_sha256: Option<String>,
    pub acknowledged_sha256: Option<String>,
    pub outcome_revisions: Vec<OutcomeRevision>,
    pub result_params: Option<Value>,
    pub result_sha256: Option<String>,
    pub result_acknowledged_sha256: Option<String>,
    /// Evidence for an incomplete final frame. The retained prefix remains
    /// available, but the journal must not be appended to or replayed.
    pub torn_tail_evidence: Option<TornTailEvidence>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutcomeRevision {
    pub outcome: Value,
    pub sha256: String,
    pub acknowledged: bool,
}

#[derive(Clone, Copy)]
enum SavedAcknowledgement {
    Outcome,
    ResultPage,
}

impl SavedAcknowledgement {
    fn fields(self, history: &OperationHistory) -> (Option<&Value>, Option<&str>, Option<&str>) {
        match self {
            Self::Outcome => (
                history.outcome.as_ref(),
                history.outcome_sha256.as_deref(),
                history.acknowledged_sha256.as_deref(),
            ),
            Self::ResultPage => (
                history.result_params.as_ref(),
                history.result_sha256.as_deref(),
                history.result_acknowledged_sha256.as_deref(),
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TornTailEvidence {
    pub valid_prefix_bytes: usize,
    pub tail_sha256: String,
    pub tail_bytes: usize,
}

/// Startup recovery state for the controller to route without replaying an
/// effect. `operation_id` is absent only when the torn bytes contain the first
/// record and no complete record identifies the operation yet.
#[derive(Debug)]
pub struct OperationRecoveryState {
    pub operation_key: String,
    pub operation_id: Option<String>,
    pub history: OperationHistory,
}

#[derive(Debug, Default)]
pub struct RetentionSummary {
    /// Files removed from the namespace during this pass. Durability is
    /// reported separately because removal may be unconfirmed.
    pub namespace_removed: usize,
    pub examined: usize,
    /// Whether no queued namespace candidates remain to be examined.
    pub queue_drained: bool,
    /// Persistent conservative disposition for any unconfirmed removal.
    pub durability_unknown: bool,
}

#[derive(Clone)]
struct RetainedOperation {
    operation_id: String,
    path: PathBuf,
    observed_sha256: String,
    releasable: bool,
    result_source: bool,
    references: Vec<String>,
    native_root_id: Option<String>,
    native_scope_key: Option<String>,
}

fn inspect_retained_operation(
    path: PathBuf,
    binding_id: &str,
    generation: i64,
    scope: &str,
    pending_delivery: bool,
) -> Result<RetainedOperation> {
    ensure_regular(&path, "operation journal")?;
    let observed_bytes = read_limited(&path, HISTORY_LIMIT)?;
    let history = read_operation_history_bytes(&observed_bytes, None)?;
    summarize_retained_operation(
        path,
        &observed_bytes,
        &history,
        binding_id,
        generation,
        scope,
        pending_delivery,
    )
}

fn summarize_retained_operation(
    path: PathBuf,
    observed_bytes: &[u8],
    history: &OperationHistory,
    binding_id: &str,
    generation: i64,
    scope: &str,
    pending_delivery: bool,
) -> Result<RetainedOperation> {
    let name = path
        .file_stem()
        .and_then(|value| value.to_str())
        .ok_or_else(|| Error::new("ADAPTER_JOURNAL", "operation filename is invalid"))?;
    let operation_id = history
        .intent
        .as_ref()
        .map(|intent| intent.operation_id.clone())
        .or_else(|| {
            history
                .outcome
                .as_ref()
                .and_then(|outcome| outcome["operation_id"].as_str())
                .map(ToOwned::to_owned)
        })
        .or_else(|| {
            history
                .result_params
                .as_ref()
                .and_then(|params| params["operation_id"].as_str())
                .map(ToOwned::to_owned)
        })
        .ok_or_else(|| {
            Error::new(
                "ADAPTER_JOURNAL",
                "operation journal has no recoverable identity",
            )
        })?;
    if name != operation_key(&operation_id) {
        return Err(Error::new(
            "ADAPTER_JOURNAL",
            "operation filename does not match its identity",
        ));
    }
    if let Some(intent) = history.intent.as_ref()
        && (intent.binding_id != binding_id
            || intent.generation != generation
            || intent.native_scope_key != scope)
    {
        return Err(Error::new(
            "ADAPTER_STATE_IDENTITY_MISMATCH",
            "operation journal belongs to another binding generation or native scope",
        ));
    }
    let intent_root = history
        .intent
        .as_ref()
        .and_then(|intent| intent.native_root_id.as_deref());
    let outcome_root = history
        .outcome
        .as_ref()
        .and_then(|outcome| outcome["native_root_id"].as_str());
    if intent_root.is_some() && outcome_root.is_some() && intent_root != outcome_root {
        return Err(Error::new(
            "NATIVE_IDENTITY_MISMATCH",
            "operation intent and outcome name different native roots",
        ));
    }
    let intent_scope = history
        .intent
        .as_ref()
        .map(|intent| intent.native_scope_key.as_str());
    let outcome_scope = history
        .outcome
        .as_ref()
        .and_then(|outcome| outcome["native_scope_key"].as_str());
    if intent_scope.is_some() && outcome_scope.is_some() && intent_scope != outcome_scope {
        return Err(Error::new(
            "NATIVE_IDENTITY_MISMATCH",
            "operation intent and outcome name different native scopes",
        ));
    }

    let mut references = Vec::new();
    if (history_is_unresolved(history) || pending_delivery)
        && let Some(intent) = history.intent.as_ref()
    {
        if let Some(target) = intent.reconcile_target_operation_id.as_ref() {
            references.push(target.clone());
        }
        if let Some(result) = intent.result_input_status.as_ref() {
            references.push(result.input_operation_id.clone());
        }
        if let Some(result) = intent.result_assistant.as_ref() {
            references.push(result.input_operation_id.clone());
        }
    }
    if history_is_unresolved(history) || pending_delivery {
        references.extend(history.outcome_revisions.iter().filter_map(|revision| {
            revision.outcome["details"]["reconcile_operation_id"]
                .as_str()
                .map(ToOwned::to_owned)
        }));
    }
    let mut result_source = false;
    if let Some(intent) = history.intent.as_ref()
        && matches!(
            intent.method.as_str(),
            "task.dispatch" | "agent.send" | "native.opencode.loop_step"
        )
    {
        let outcome_input_id = history
            .outcome
            .as_ref()
            .and_then(|outcome| outcome["native_input_id"].as_str());
        if let Some(input_id) = intent.native_input_id.as_deref() {
            let expected_input_id = crate::native::input_id(&intent.operation_id);
            if intent.native_root_id.is_none()
                || input_id != expected_input_id.as_str()
                || history.outcome.is_some() && outcome_input_id != Some(input_id)
            {
                return Err(Error::new(
                    "NATIVE_IDENTITY_MISMATCH",
                    "saved input source differs from its exact Operation, root, or outcome identity",
                ));
            }
            result_source = true;
        } else if outcome_input_id.is_some() {
            return Err(Error::new(
                "NATIVE_IDENTITY_MISMATCH",
                "input outcome has no matching saved source identity",
            ));
        }
    }

    Ok(RetainedOperation {
        operation_id,
        path,
        observed_sha256: format!("{:x}", Sha256::digest(observed_bytes)),
        releasable: history_is_releasable(history),
        result_source,
        references,
        native_root_id: outcome_root.or(intent_root).map(ToOwned::to_owned),
        native_scope_key: outcome_scope.or(intent_scope).map(ToOwned::to_owned),
    })
}

impl OperationHistory {
    /// An effect intent without terminal evidence is unknown unless Store has
    /// acknowledged the exact result page for an `agent.result` Operation.
    pub fn recovery_unknown(&self) -> bool {
        self.intent.is_some() && self.outcome.is_none() && !has_acknowledged_result_page(self)
    }

    /// A torn journal cannot authorize another native effect attempt.
    pub fn native_replay_permitted(&self) -> bool {
        self.intent.is_none()
            && self.outcome.is_none()
            && self.result_params.is_none()
            && self.torn_tail_evidence.is_none()
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingItem {
    pub kind: String,
    pub key: String,
    pub payload: Value,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RootCheckpoint {
    version: u32,
    binding_id: String,
    generation: i64,
    native_scope_key: String,
    native_root_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct StateIdentity {
    version: u32,
    binding_id: String,
    generation: i64,
    native_scope_key: String,
    route_sha256: String,
}

pub struct Journal {
    identity: StateIdentity,
    operations: PathBuf,
    outbox: PathBuf,
    checkpoints: PathBuf,
    retention: Mutex<retention::RetentionIndex>,
    acknowledgements_since_retention: AtomicUsize,
}

impl Journal {
    pub fn open(
        root: &Path,
        binding_id: &str,
        generation: i64,
        scope: &str,
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
        ensure_directory(root, "state directory")?;
        private_permissions(root, true)?;
        let identity_path = root.join("state-identity.json");
        let identity = StateIdentity {
            version: 1,
            binding_id: binding_id.into(),
            generation,
            native_scope_key: scope.into(),
            route_sha256: route_sha256.into(),
        };
        if identity_path.exists() {
            ensure_regular(&identity_path, "state identity")?;
            let bytes = read_limited(&identity_path, RECORD_LIMIT)?;
            let saved: StateIdentity = serde_json::from_slice(&bytes)
                .map_err(|_| Error::new("ADAPTER_STATE", "state identity is invalid"))?;
            if saved != identity {
                return Err(Error::new(
                    "ADAPTER_STATE_IDENTITY_MISMATCH",
                    "state directory belongs to another binding generation or native service scope",
                ));
            }
        } else {
            for existing in [
                "operations",
                "outbox",
                "native-checkpoints.jsonl",
                "retention-cursor.json",
            ] {
                if root.join(existing).exists() {
                    return Err(Error::new(
                        "ADAPTER_STATE_IDENTITY_MISSING",
                        "existing adapter state has no binding identity; refusing to adopt it",
                    ));
                }
            }
            let bytes = serde_json::to_vec(&identity)?;
            write_private_new(&identity_path, &bytes)?;
        }
        let operations = root.join("operations");
        let outbox = root.join("outbox");
        fs::create_dir_all(&operations).map_err(|_| {
            Error::new(
                "ADAPTER_STATE",
                "operation journal directory cannot be created",
            )
        })?;
        fs::create_dir_all(&outbox)
            .map_err(|_| Error::new("ADAPTER_STATE", "outbox directory cannot be created"))?;
        ensure_directory(&operations, "operation journal directory")?;
        ensure_directory(&outbox, "outbox directory")?;
        private_permissions(&operations, true)?;
        private_permissions(&outbox, true)?;
        let checkpoints = root.join("native-checkpoints.jsonl");
        if !checkpoints.exists() {
            write_private_new(&checkpoints, &[])?;
        } else {
            ensure_regular(&checkpoints, "native checkpoint")?;
        }
        let retention = retention::RetentionIndex::open(root, &identity);
        Ok(Self {
            identity,
            operations,
            outbox,
            checkpoints,
            retention: Mutex::new(retention),
            acknowledgements_since_retention: AtomicUsize::new(0),
        })
    }

    pub fn load(&self, operation_id: &str) -> Result<OperationHistory> {
        let path = self.operation_path(operation_id);
        if !path.exists() {
            return Ok(OperationHistory::default());
        }
        read_operation_history(&path, Some(operation_id))
    }

    pub fn write_intent(&self, intent: &OperationIntent) -> Result<()> {
        let previous = self.load(&intent.operation_id)?;
        if !previous.native_replay_permitted() {
            if let Some(tail) = previous.torn_tail_evidence.as_ref() {
                return Err(Error::new(
                    "ADAPTER_JOURNAL_RECOVERY_UNKNOWN",
                    format!(
                        "torn operation journal: valid_prefix_bytes={}, tail_bytes={}, tail_sha256={}",
                        tail.valid_prefix_bytes, tail.tail_bytes, tail.tail_sha256
                    ),
                ));
            }
            if previous.recovery_unknown() {
                return Err(Error::new(
                    "ADAPTER_JOURNAL_RECOVERY_UNKNOWN",
                    "operation has a durable intent without a saved terminal outcome",
                ));
            }
            if previous.intent.is_some() {
                return Err(Error::new(
                    "ADAPTER_INTENT_EXISTS",
                    "operation already has a native intent",
                ));
            }
            return Err(Error::new(
                "ADAPTER_JOURNAL_RECOVERY_UNKNOWN",
                "existing operation history does not authorize a new native effect",
            ));
        }
        self.append(
            &intent.operation_id,
            &JournalRecord {
                version: 2,
                operation_id: intent.operation_id.clone(),
                kind: "intent".into(),
                intent: Some(intent.clone()),
                outcome: None,
                outcome_sha256: None,
                result_params: None,
                result_sha256: None,
            },
        )
    }

    pub fn queue_outcome(&self, outcome: &RuntimeOutcome) -> Result<()> {
        let value = serde_json::to_value(outcome)?;
        self.queue_outcome_value(&value)
    }

    pub fn queue_outcome_value(&self, value: &Value) -> Result<()> {
        let operation_id = value["operation_id"]
            .as_str()
            .ok_or_else(|| Error::new("ADAPTER_JOURNAL", "saved outcome has no operation ID"))?;
        let digest = digest_json(value)?;
        let history = self.load(operation_id)?;
        if let Some(intent) = history
            .intent
            .as_ref()
            .filter(|intent| intent.method == "agent.reconcile")
        {
            let summary: RuntimeOutcome = serde_json::from_value(value.clone())
                .map_err(|_| recovery_outcome_unknown("reconcile summary is malformed"))?;
            self.validate_reconcile_summary(intent, &summary)?;
        }
        if value["details"].get("reconcile_operation_id").is_some() {
            let outcome: RuntimeOutcome = serde_json::from_value(value.clone()).map_err(|_| {
                recovery_outcome_unknown("linked reconciliation outcome is malformed")
            })?;
            let intent = history.intent.as_ref().ok_or_else(|| {
                recovery_outcome_unknown("linked reconciliation target has no saved intent")
            })?;
            self.validate_linked_reconcile_outcome(intent, &outcome)?;
        }
        if history.outcome_sha256.as_deref() != Some(digest.as_str()) {
            self.validate_outcome_publication_predecessor(operation_id, &history)?;
            if let Some(previous) = history.outcome.as_ref() {
                validate_outcome_successor(previous, value)?;
            }
            self.append(
                operation_id,
                &JournalRecord {
                    version: 2,
                    operation_id: operation_id.into(),
                    kind: "outcome".into(),
                    intent: None,
                    outcome: Some(value.clone()),
                    outcome_sha256: Some(digest.clone()),
                    result_params: None,
                    result_sha256: None,
                },
            )?;
        }
        self.write_pending(PendingItem {
            kind: "outcome".into(),
            key: operation_id.into(),
            payload: value.clone(),
        })
    }

    pub fn queue_observation(&self, event_id: &str, params: Value) -> Result<()> {
        self.write_pending(PendingItem {
            kind: "observation".into(),
            key: event_id.into(),
            payload: params,
        })
    }

    /// Save an immutable module.result request before sending it to Store.
    /// Reconnection retries must replay these exact bytes and provenance.
    pub fn queue_result(&self, params: &Value) -> Result<()> {
        let operation_id = params["operation_id"].as_str().ok_or_else(|| {
            Error::new("ADAPTER_JOURNAL", "saved result page has no operation ID")
        })?;
        let digest = digest_json(params)?;
        let history = self.load(operation_id)?;
        if let Some(previous) = history.result_sha256.as_deref() {
            if previous != digest {
                return Err(Error::new(
                    "ADAPTER_JOURNAL",
                    "result page conflicts with the immutable saved page",
                ));
            }
        } else {
            self.append(
                operation_id,
                &JournalRecord {
                    version: 2,
                    operation_id: operation_id.into(),
                    kind: "result_page".into(),
                    intent: None,
                    outcome: None,
                    outcome_sha256: None,
                    result_params: Some(params.clone()),
                    result_sha256: Some(digest.clone()),
                },
            )?;
        }
        self.write_pending(PendingItem {
            kind: "result".into(),
            key: operation_id.into(),
            payload: params.clone(),
        })
    }

    pub fn acknowledge_result(&self, params: &Value) -> Result<()> {
        let operation_id = params["operation_id"].as_str().ok_or_else(|| {
            Error::new("ADAPTER_OUTBOX", "acknowledged result has no operation ID")
        })?;
        let digest = digest_json(params)?;
        let history = self.load(operation_id)?;
        if history.result_sha256.as_deref() != Some(digest.as_str()) {
            return Err(Error::new(
                "ADAPTER_JOURNAL",
                "result acknowledgement differs from the saved page",
            ));
        }
        if history.torn_tail_evidence.is_some() && !self.repair_torn_result_ack(params)? {
            return Err(torn_ack_recovery_unknown());
        }
        let history = self.load(operation_id)?;
        if history.result_acknowledged_sha256.as_deref() == Some(digest.as_str()) {
            return Ok(());
        }
        if history.result_acknowledged_sha256.is_some() {
            return Err(torn_ack_recovery_unknown());
        }
        self.append(
            operation_id,
            &JournalRecord {
                version: 2,
                operation_id: operation_id.into(),
                kind: "result_acknowledged".into(),
                intent: None,
                outcome: None,
                outcome_sha256: None,
                result_params: None,
                result_sha256: Some(digest),
            },
        )
    }

    #[cfg(test)]
    pub fn pending_items(&self) -> Result<Vec<(PathBuf, PendingItem)>> {
        self.pending_items_excluding(&HashSet::new())
    }

    pub fn pending_items_excluding(
        &self,
        deferred_paths: &HashSet<PathBuf>,
    ) -> Result<Vec<(PathBuf, PendingItem)>> {
        let mut items = Vec::with_capacity(OUTBOX_BATCH);
        let entries = fs::read_dir(&self.outbox)
            .map_err(|_| Error::new("ADAPTER_OUTBOX", "outbox cannot be read"))?;
        for entry in entries {
            let entry =
                entry.map_err(|_| Error::new("ADAPTER_OUTBOX", "outbox entry cannot be read"))?;
            let path = entry.path();
            ensure_regular(&path, "outbox item")?;
            if deferred_paths.contains(&path) {
                continue;
            }
            if items.len() >= OUTBOX_BATCH {
                break;
            }
            let bytes = read_limited(&path, RECORD_LIMIT)?;
            let item: PendingItem = serde_json::from_slice(&bytes)
                .map_err(|_| Error::new("ADAPTER_OUTBOX", "outbox item is invalid"))?;
            items.push((path, item));
        }
        Ok(items)
    }

    pub fn recover_outbox(&self) -> Result<Vec<OperationRecoveryState>> {
        retention::begin_recovery(self)?;
        let mut recovery_states = Vec::new();
        let entries = fs::read_dir(&self.operations)
            .map_err(|_| Error::new("ADAPTER_JOURNAL", "operation directory cannot be read"))?;
        for entry in entries {
            let entry = entry
                .map_err(|_| Error::new("ADAPTER_JOURNAL", "operation entry cannot be read"))?;
            let path = entry.path();
            ensure_regular(&path, "operation journal")?;
            let Some(name) = path.file_stem().and_then(|v| v.to_str()) else {
                retention::mark_damaged(self)?;
                continue;
            };
            let observed_bytes = read_limited(&path, HISTORY_LIMIT)?;
            let history = match read_operation_history_bytes(&observed_bytes, None) {
                Ok(history) => history,
                Err(error) if error.code == "ADAPTER_JOURNAL" => {
                    retention::mark_damaged(self)?;
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
                Some(operation_id) => match self.operation_has_pending_delivery(operation_id) {
                    Ok(pending) => pending,
                    Err(_) => {
                        retention::mark_damaged(self)?;
                        true
                    }
                },
                None => {
                    retention::mark_damaged(self)?;
                    true
                }
            };
            match summarize_retained_operation(
                path.clone(),
                &observed_bytes,
                &history,
                &self.identity.binding_id,
                self.identity.generation,
                &self.identity.native_scope_key,
                pending_delivery,
            ) {
                Ok(operation) => {
                    retention::register_operation(self, operation)?;
                }
                Err(_) => retention::mark_damaged(self)?,
            }
            if let (Some(outcome), Some(digest)) =
                (history.outcome.as_ref(), history.outcome_sha256.as_ref())
            {
                let Some(operation_id) = outcome["operation_id"].as_str() else {
                    continue;
                };
                let expected = operation_key(operation_id);
                if name != expected {
                    continue;
                }
                let pending_path = self
                    .outbox
                    .join(format!("outcome-{}.json", operation_key(operation_id)));
                if history.acknowledged_sha256.as_deref() != Some(digest.as_str()) {
                    if outcome["details"].get("reconcile_operation_id").is_some() {
                        let linked: RuntimeOutcome = match serde_json::from_value(outcome.clone()) {
                            Ok(linked) => linked,
                            Err(_) => {
                                retention::mark_damaged(self)?;
                                continue;
                            }
                        };
                        let Some(intent) = history.intent.as_ref() else {
                            retention::mark_damaged(self)?;
                            continue;
                        };
                        if self
                            .validate_linked_reconcile_outcome(intent, &linked)
                            .is_err()
                        {
                            retention::mark_damaged(self)?;
                            continue;
                        }
                    }
                    if self
                        .write_pending(PendingItem {
                            kind: "outcome".into(),
                            key: operation_id.into(),
                            payload: outcome.clone(),
                        })
                        .is_err()
                    {
                        retention::mark_damaged(self)?;
                        continue;
                    }
                }
                match fs::symlink_metadata(&pending_path) {
                    Ok(_) => {
                        ensure_regular(&pending_path, "outbox item")?;
                        let bytes = read_limited(&pending_path, RECORD_LIMIT)?;
                        let pending: PendingItem = serde_json::from_slice(&bytes)
                            .map_err(|_| Error::new("ADAPTER_OUTBOX", "outbox item is invalid"))?;
                        if self
                            .validate_pending_outcome(&pending_path, &pending)
                            .is_err()
                        {
                            retention::mark_damaged(self)?;
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(_) => {
                        retention::mark_damaged(self)?;
                    }
                }
            }
            if let (Some(params), Some(digest)) = (
                history.result_params.as_ref(),
                history.result_sha256.as_ref(),
            ) && history.result_acknowledged_sha256.as_deref() != Some(digest.as_str())
            {
                let Some(operation_id) = params["operation_id"].as_str() else {
                    continue;
                };
                if name != operation_key(operation_id) {
                    continue;
                }
                self.write_pending(PendingItem {
                    kind: "result".into(),
                    key: operation_id.into(),
                    payload: params.clone(),
                })?;
            }
            if history.recovery_unknown() || history.torn_tail_evidence.is_some() {
                let operation_id = history
                    .intent
                    .as_ref()
                    .map(|intent| intent.operation_id.clone())
                    .or_else(|| {
                        history
                            .outcome
                            .as_ref()
                            .and_then(|outcome| outcome["operation_id"].as_str())
                            .map(ToOwned::to_owned)
                    })
                    .or_else(|| {
                        history
                            .result_params
                            .as_ref()
                            .and_then(|params| params["operation_id"].as_str())
                            .map(ToOwned::to_owned)
                    });
                if operation_id
                    .as_deref()
                    .is_some_and(|operation_id| name != operation_key(operation_id))
                {
                    continue;
                }
                recovery_states.push(OperationRecoveryState {
                    operation_key: name.to_owned(),
                    operation_id,
                    history,
                });
            }
        }
        retention::finish_recovery(self)?;
        Ok(recovery_states)
    }

    pub fn retention_pass_due_after_ack(&self) -> bool {
        self.acknowledgements_since_retention
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1)
            .is_multiple_of(RETENTION_ACK_INTERVAL)
    }

    /// Reclaim only acknowledged terminal journals whose local evidence has no
    /// unresolved reader. Dispatch/input sources remain available because a
    /// later `agent.result` read reopens their exact saved intent and admission;
    /// a per-page acknowledgement does not establish that no future reader exists.
    pub fn reclaim_acknowledged(
        &self,
        binding_id: &str,
        generation: i64,
        scope: &str,
    ) -> Result<RetentionSummary> {
        retention::reclaim_acknowledged(self, binding_id, generation, scope)
    }

    fn operation_has_pending_delivery(&self, operation_id: &str) -> Result<bool> {
        let key = operation_key(operation_id);
        for kind in ["outcome", "result"] {
            let path = self.outbox.join(format!("{kind}-{key}.json"));
            match fs::symlink_metadata(&path) {
                Ok(_) => {
                    ensure_regular(&path, "outbox item")?;
                    return Ok(true);
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => {
                    return Err(Error::new(
                        "ADAPTER_OUTBOX",
                        "pending operation delivery cannot be inspected",
                    ));
                }
            }
        }
        Ok(false)
    }

    fn checkpoint_operation_root(
        &self,
        root: Option<&str>,
        saved_scope: Option<&str>,
        binding_id: &str,
        generation: i64,
        scope: &str,
    ) -> Result<()> {
        let Some(root) = root else {
            return Ok(());
        };
        if saved_scope != Some(scope) {
            return Err(Error::new(
                "NATIVE_IDENTITY_MISMATCH",
                "acknowledged operation root lacks its exact native service scope",
            ));
        }
        self.remember_native_root(binding_id, generation, scope, root)?;
        if self
            .native_root_checkpoint_for_hello(binding_id, generation, scope)?
            .as_deref()
            != Some(root)
        {
            return Err(Error::new(
                "ADAPTER_NATIVE_CHECKPOINT",
                "native root checkpoint did not read back before operation removal",
            ));
        }
        Ok(())
    }

    pub fn acknowledge_outcome(&self, outcome: &Value) -> Result<()> {
        let operation_id = outcome["operation_id"].as_str().ok_or_else(|| {
            Error::new("ADAPTER_OUTBOX", "acknowledged outcome has no operation ID")
        })?;
        let digest = digest_json(outcome)?;
        let history = self.load(operation_id)?;
        if history.outcome_sha256.as_deref() != Some(digest.as_str()) {
            return Err(Error::new(
                "ADAPTER_JOURNAL",
                "outcome acknowledgement does not match the saved result",
            ));
        }
        if history.torn_tail_evidence.is_some() && !self.repair_torn_outcome_ack(outcome)? {
            return Err(torn_ack_recovery_unknown());
        }
        let history = self.load(operation_id)?;
        if history.acknowledged_sha256.as_deref() == Some(digest.as_str()) {
            return Ok(());
        }
        if history.acknowledged_sha256.is_some() {
            return Err(torn_ack_recovery_unknown());
        }
        self.append(
            operation_id,
            &acknowledged_outcome_record(operation_id, &digest),
        )
    }

    /// Remove only a bounded torn suffix after rescanning the exact saved
    /// outcome prefix. The suffix never proves that the host acknowledged it.
    pub fn repair_torn_outcome_ack(&self, outcome: &Value) -> Result<bool> {
        self.repair_torn_ack(outcome, SavedAcknowledgement::Outcome)
    }

    /// A result page is retained independently of the Operation outcome.
    /// Repair its exact saved prefix before resending it to Store; a torn ACK
    /// never authorizes native result replay or proves Store acknowledgement.
    pub fn repair_torn_result_ack(&self, params: &Value) -> Result<bool> {
        self.repair_torn_ack(params, SavedAcknowledgement::ResultPage)
    }

    fn repair_torn_ack(&self, payload: &Value, kind: SavedAcknowledgement) -> Result<bool> {
        let operation_id = payload["operation_id"].as_str().ok_or_else(|| {
            Error::new(
                "ADAPTER_OUTBOX",
                "saved acknowledgement has no operation ID",
            )
        })?;
        let digest = digest_json(payload)?;
        let path = self.operation_path(operation_id);
        ensure_regular(&path, "operation journal").map_err(|_| torn_ack_recovery_unknown())?;
        let mut file = OpenOptions::new()
            .read(true)
            .open(&path)
            .map_err(|_| torn_ack_recovery_unknown())?;
        let bytes = read_opened_file_for_repair(&path, &mut file)?;
        let opened_history = read_operation_history_bytes(&bytes, Some(operation_id))
            .map_err(|_| torn_ack_recovery_unknown())?;
        let Some(evidence) = opened_history.torn_tail_evidence.as_ref() else {
            return Ok(false);
        };
        let (saved_payload, saved_digest, acknowledged_sha256) = kind.fields(&opened_history);
        if saved_payload != Some(payload)
            || saved_digest != Some(digest.as_str())
            || acknowledged_sha256.is_some_and(|saved| saved != digest)
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
        if repaired_bytes.len() > HISTORY_LIMIT
            || opened_metadata.len() != bytes.len() as u64
            || !opened_file_matches_path(&file, &path)
        {
            return Err(torn_ack_recovery_unknown());
        }
        // The adapter retains its single-owner scope. Close the validated
        // reader before the write-through replacement, which Windows refuses
        // while the destination reader is open.
        drop(file);
        replace_private_durable(&path, &repaired_bytes).map_err(|_| torn_ack_recovery_unknown())?;

        ensure_regular(&path, "operation journal").map_err(|_| torn_ack_recovery_unknown())?;
        let mut repaired_file = OpenOptions::new()
            .read(true)
            .open(&path)
            .map_err(|_| torn_ack_recovery_unknown())?;
        let verified_bytes = read_opened_file_for_repair(&path, &mut repaired_file)?;
        let verified_history = read_operation_history_bytes(&verified_bytes, Some(operation_id))
            .map_err(|_| torn_ack_recovery_unknown())?;
        if verified_bytes != repaired_bytes
            || verified_history.torn_tail_evidence.is_some()
            || kind.fields(&verified_history)
                != (Some(payload), Some(digest.as_str()), acknowledged_sha256)
        {
            return Err(torn_ack_recovery_unknown());
        }
        retention::refresh_operation(self, operation_id);
        Ok(true)
    }

    pub fn remember_native_root(
        &self,
        binding_id: &str,
        generation: i64,
        scope: &str,
        root: &str,
    ) -> Result<()> {
        if root != expected_root_id(binding_id, generation) {
            return Err(Error::new(
                "NATIVE_IDENTITY_MISMATCH",
                "native checkpoint does not match the deterministic binding root",
            ));
        }
        if let Some(previous) = self.checkpoint_root(binding_id, generation, scope)? {
            if previous != root {
                return Err(Error::new(
                    "NATIVE_IDENTITY_MISMATCH",
                    "native root changed for this binding generation",
                ));
            }
            return Ok(());
        }
        let checkpoint = RootCheckpoint {
            version: 1,
            binding_id: binding_id.into(),
            generation,
            native_scope_key: scope.into(),
            native_root_id: root.into(),
        };
        let mut bytes = serde_json::to_vec(&checkpoint)?;
        bytes.push(b'\n');
        append_bytes(&self.checkpoints, &bytes)
    }

    pub fn native_root_for_hello(
        &self,
        binding_id: &str,
        generation: i64,
        scope: &str,
    ) -> Result<Option<String>> {
        let mut root_checkpoint = self.checkpoint_root(binding_id, generation, scope)?;
        // An outcome can be accepted by the host while its reply is lost. Its
        // durable outbox receipt is therefore also a valid identity checkpoint.
        let entries = fs::read_dir(&self.outbox)
            .map_err(|_| Error::new("ADAPTER_OUTBOX", "outbox cannot be read"))?;
        for entry in entries {
            let entry =
                entry.map_err(|_| Error::new("ADAPTER_OUTBOX", "outbox entry cannot be read"))?;
            let path = entry.path();
            ensure_regular(&path, "outbox item")?;
            let bytes = read_limited(&path, RECORD_LIMIT)?;
            let item: PendingItem = serde_json::from_slice(&bytes)
                .map_err(|_| Error::new("ADAPTER_OUTBOX", "outbox item is invalid"))?;
            if item.kind == "outcome"
                && item.payload["native_scope_key"] == scope
                && let Some(root) = item.payload["native_root_id"].as_str()
            {
                let history = match self.load(&item.key) {
                    Ok(history) => history,
                    Err(error) if error.code == "ADAPTER_JOURNAL" => continue,
                    Err(error) => return Err(error),
                };
                if let Some(intent) = history.intent
                    && intent.binding_id == binding_id
                    && intent.generation == generation
                    && intent.native_scope_key == scope
                {
                    remember_one_root(&mut root_checkpoint, root.into())?;
                }
            }
        }
        let entries = fs::read_dir(&self.operations)
            .map_err(|_| Error::new("ADAPTER_JOURNAL", "operation directory cannot be read"))?;
        for entry in entries {
            let entry = entry
                .map_err(|_| Error::new("ADAPTER_JOURNAL", "operation entry cannot be read"))?;
            let path = entry.path();
            ensure_regular(&path, "operation journal")?;
            let Some(name) = path.file_stem().and_then(|value| value.to_str()) else {
                continue;
            };
            let history = match self.load_by_path(&path) {
                Ok(history) => history,
                Err(error) if error.code == "ADAPTER_JOURNAL" => continue,
                Err(error) => return Err(error),
            };
            if let Some(intent) = history.intent
                && name == operation_key(&intent.operation_id)
                && intent.binding_id == binding_id
                && intent.generation == generation
                && intent.native_scope_key == scope
                && let Some(root) = intent.native_root_id
            {
                remember_one_root(&mut root_checkpoint, root)?;
            }
        }
        if root_checkpoint
            .as_deref()
            .is_some_and(|root| root != expected_root_id(binding_id, generation))
        {
            return Err(Error::new(
                "NATIVE_IDENTITY_MISMATCH",
                "saved root does not match the deterministic binding identity",
            ));
        }
        Ok(root_checkpoint)
    }

    /// Return only the local checkpoint written after a host-acknowledged
    /// outcome or Store-owned hello identity. Pending intents/outbox records
    /// may supply a candidate hello identity, but cannot prove Store state.
    pub fn native_root_checkpoint_for_hello(
        &self,
        binding_id: &str,
        generation: i64,
        scope: &str,
    ) -> Result<Option<String>> {
        self.checkpoint_root(binding_id, generation, scope)
    }

    fn checkpoint_root(
        &self,
        binding_id: &str,
        generation: i64,
        scope: &str,
    ) -> Result<Option<String>> {
        let mut root_checkpoint: Option<String> = None;
        if self.checkpoints.exists() {
            ensure_regular(&self.checkpoints, "native checkpoint")?;
            let bytes = read_limited(&self.checkpoints, HISTORY_LIMIT)?;
            let verdict = scan_jsonl(&bytes, RECORD_LIMIT, |_, frame| {
                let item: RootCheckpoint = serde_json::from_slice(&frame[..frame.len() - 1])
                    .map_err(|_| Error::new("ADAPTER_STATE", "native checkpoint is invalid"))?;
                if item.version != 1
                    || item.generation <= 0
                    || item.native_root_id != expected_root_id(&item.binding_id, item.generation)
                {
                    return Err(Error::new(
                        "ADAPTER_STATE",
                        "native checkpoint identity or version is invalid",
                    ));
                }
                if item.binding_id == binding_id
                    && item.generation == generation
                    && item.native_scope_key == scope
                {
                    remember_one_root(&mut root_checkpoint, item.native_root_id)?;
                }
                Ok(())
            })?;
            match verdict {
                JsonlScanVerdict::Complete { .. } => {}
                JsonlScanVerdict::TornLastFrame {
                    valid_bytes,
                    tail_digest,
                    tail_bytes,
                    ..
                } => {
                    return Err(Error::new(
                        "ADAPTER_NATIVE_CHECKPOINT_TORN",
                        format!(
                            "native checkpoint retained: valid_prefix_bytes={valid_bytes}, tail_bytes={tail_bytes}, tail_sha256={tail_digest}"
                        ),
                    ));
                }
                JsonlScanVerdict::Damaged {
                    valid_prefix_bytes,
                    reason,
                } => {
                    return Err(Error::new(
                        "ADAPTER_NATIVE_CHECKPOINT_DAMAGED",
                        format!(
                            "native checkpoint damaged at byte {valid_prefix_bytes}: {reason:?}"
                        ),
                    ));
                }
            }
        }
        Ok(root_checkpoint)
    }

    pub fn remove_pending(&self, path: &Path) -> Result<RemovalOutcome> {
        if path.parent() != Some(self.outbox.as_path()) {
            return Err(Error::new(
                "ADAPTER_OUTBOX",
                "acknowledged outbox path is outside its owner directory",
            ));
        }
        let retained_bytes = if path.exists() {
            ensure_regular(path, "outbox item")
                .map_err(|_| Error::new("ADAPTER_OUTBOX", "outbox item cannot be retained"))?;
            Some(read_limited(path, RECORD_LIMIT).map_err(|_| {
                Error::new(
                    "ADAPTER_OUTBOX",
                    "acknowledged outbox item cannot be retained",
                )
            })?)
        } else {
            None
        };
        let retention_operation_id = retained_bytes
            .as_deref()
            .and_then(|bytes| serde_json::from_slice::<PendingItem>(bytes).ok())
            .filter(|item| matches!(item.kind.as_str(), "outcome" | "result"))
            .map(|item| item.key);
        match remove_private_acknowledged(path) {
            Ok(outcome) => {
                if let Some(operation_id) = retention_operation_id {
                    retention::refresh_operation(self, &operation_id);
                }
                Ok(outcome)
            }
            Err(_) => {
                if let Some(bytes) = retained_bytes {
                    // A parent-directory sync failure may follow a successful
                    // unlink. Restore the exact acknowledged payload before
                    // returning the error so the next flush can retry it.
                    let _ = replace_private_durable(path, &bytes);
                }
                Err(Error::new(
                    "ADAPTER_OUTBOX",
                    "acknowledged outbox item could not be durably removed",
                ))
            }
        }
    }

    fn operation_path(&self, operation_id: &str) -> PathBuf {
        self.operations
            .join(format!("{}.jsonl", operation_key(operation_id)))
    }

    fn append(&self, operation_id: &str, record: &JournalRecord) -> Result<()> {
        let history = self.load(operation_id)?;
        if let Some(tail) = history.torn_tail_evidence.as_ref() {
            return Err(Error::new(
                "ADAPTER_JOURNAL_RECOVERY_UNKNOWN",
                format!(
                    "operation journal append withheld: valid_prefix_bytes={}, tail_bytes={}, tail_sha256={}",
                    tail.valid_prefix_bytes, tail.tail_bytes, tail.tail_sha256
                ),
            ));
        }
        let mut bytes = serde_json::to_vec(record)?;
        if bytes.len() >= RECORD_LIMIT {
            return Err(Error::new(
                "ADAPTER_JOURNAL",
                "journal record exceeds its frame limit",
            ));
        }
        bytes.push(b'\n');
        let path = self.operation_path(operation_id);
        if path.exists()
            && fs::metadata(&path)?
                .len()
                .saturating_add(bytes.len() as u64)
                > HISTORY_LIMIT as u64
        {
            return Err(Error::new(
                "ADAPTER_JOURNAL",
                "operation history exceeds its total byte boundary",
            ));
        }
        append_bytes(&path, &bytes)?;
        retention::refresh_operation(self, operation_id);
        Ok(())
    }

    fn write_pending(&self, item: PendingItem) -> Result<()> {
        let retention_operation_id =
            matches!(item.kind.as_str(), "outcome" | "result").then(|| item.key.clone());
        let prefix = match item.kind.as_str() {
            "outcome" => "outcome",
            "observation" => "observation",
            "result" => "result",
            _ => return Err(Error::new("ADAPTER_OUTBOX", "unsupported pending item")),
        };
        let path = self
            .outbox
            .join(format!("{prefix}-{}.json", operation_key(&item.key)));
        let bytes = serde_json::to_vec(&item)?;
        if bytes.len() > RECORD_LIMIT {
            return Err(Error::new(
                "ADAPTER_OUTBOX",
                "pending host message exceeds its record limit",
            ));
        }
        if path.exists() {
            ensure_regular(&path, "outbox item")?;
            let previous = read_limited(&path, RECORD_LIMIT)?;
            let previous_value: Value = serde_json::from_slice(&previous)
                .map_err(|_| Error::new("ADAPTER_OUTBOX", "existing outbox item is invalid"))?;
            let new_value = serde_json::to_value(&item)?;
            if previous_value != new_value {
                // The slot key is derived from the operation identity, so a
                // reconciled successor collides with the predecessor it
                // replaces. Refusing every changed payload would strand the
                // successor forever; exactly one promotion is allowed.
                if !self.supersede_unknown_with_linked_applied(&path, &item, &previous_value)? {
                    return Err(Error::new(
                        "ADAPTER_OUTBOX",
                        "pending item identity conflicts with saved result",
                    ));
                }
            } else {
                replace_private_durable(&path, &previous).map_err(|_| {
                    Error::new(
                        "ADAPTER_OUTBOX",
                        "pending item could not be durably published",
                    )
                })?;
            }
            if let Some(operation_id) = retention_operation_id {
                retention::refresh_operation(self, &operation_id);
            }
            return Ok(());
        }
        write_private_new(&path, &bytes)?;
        if let Some(operation_id) = retention_operation_id {
            retention::refresh_operation(self, &operation_id);
        }
        Ok(())
    }

    fn load_by_path(&self, path: &Path) -> Result<OperationHistory> {
        read_operation_history(path, None)
    }

    fn validate_intent_state_identity(&self, intent: &OperationIntent) -> Result<()> {
        let receipt = &intent.module_receipt;
        if intent.version != 2
            || intent.operation_id.trim().is_empty()
            || intent.binding_id != self.identity.binding_id
            || intent.generation != self.identity.generation
            || intent.native_scope_key != self.identity.native_scope_key
            || !same_route_digest(&intent.route_sha256, &self.identity.route_sha256)
            || receipt.validate().is_err()
            || receipt.operation_id != intent.operation_id
            || receipt.binding_id != intent.binding_id
            || receipt.binding_generation != intent.generation
        {
            return Err(recovery_outcome_unknown(
                "reconciliation intent differs from the durable adapter identity",
            ));
        }
        Ok(())
    }

    fn validate_linked_reconcile_outcome(
        &self,
        target_intent: &OperationIntent,
        outcome: &RuntimeOutcome,
    ) -> Result<()> {
        self.validate_intent_state_identity(target_intent)?;
        let reconcile_id = outcome.details["reconcile_operation_id"]
            .as_str()
            .filter(|id| !id.trim().is_empty())
            .ok_or_else(|| {
                recovery_outcome_unknown("reconciliation outcome has no exact reconcile link")
            })?;
        if outcome.operation_id != target_intent.operation_id
            || target_intent.method == "agent.reconcile"
            || reconcile_id == target_intent.operation_id
            || outcome.native_scope_key.as_deref() != Some(target_intent.native_scope_key.as_str())
            || outcome.native_root_id.as_deref() != target_intent.native_root_id.as_deref()
            || outcome.native_input_id.as_deref() != target_intent.native_input_id.as_deref()
            || outcome.details["native_replay"] != false
        {
            return Err(recovery_outcome_unknown(
                "linked target outcome differs from its immutable intent",
            ));
        }
        let target_receipt: ModuleReceiptIdentity =
            serde_json::from_value(outcome.details["module_receipt"].clone()).map_err(|_| {
                recovery_outcome_unknown("linked target outcome has no valid module receipt")
            })?;
        if target_receipt != target_intent.module_receipt {
            return Err(recovery_outcome_unknown(
                "linked target outcome receipt differs from its immutable intent",
            ));
        }

        let reconcile_history = self.load(reconcile_id)?;
        if reconcile_history.torn_tail_evidence.is_some() {
            return Err(recovery_outcome_unknown(
                "linked reconcile operation has a torn journal suffix",
            ));
        }
        let reconcile_intent = reconcile_history.intent.as_ref().ok_or_else(|| {
            recovery_outcome_unknown("linked reconcile operation has no saved intent")
        })?;
        self.validate_intent_state_identity(reconcile_intent)?;
        if reconcile_intent.operation_id != reconcile_id
            || reconcile_intent.method != "agent.reconcile"
            || reconcile_intent.reconcile_target_operation_id.as_deref()
                != Some(target_intent.operation_id.as_str())
            || reconcile_intent.binding_id != target_intent.binding_id
            || reconcile_intent.generation != target_intent.generation
            || reconcile_intent.native_scope_key != target_intent.native_scope_key
            || !same_route_digest(&reconcile_intent.route_sha256, &target_intent.route_sha256)
        {
            return Err(recovery_outcome_unknown(
                "reconcile intent does not identify this exact target and route",
            ));
        }

        match outcome.outcome {
            EffectOutcome::Unknown => {}
            EffectOutcome::Applied => {
                let exact_input_readback = target_intent.native_input_id.is_some()
                    && outcome.details["completion_condition"] == "native_input_admitted"
                    && outcome.details["evidence"] == "durable_session_history"
                    && outcome.details["execution_complete"] == false;
                let exact_session_readback = target_intent.native_input_id.is_none()
                    && outcome.details["completion_condition"] == "native_session_created"
                    && outcome.details["evidence"] == "exact_session_readback";
                if !matches!(
                    target_intent.method.as_str(),
                    "agent.open"
                        | "task.dispatch"
                        | "agent.send"
                        | "native.opencode.loop_step"
                        | "agent.reply"
                ) || !(exact_input_readback || exact_session_readback)
                {
                    return Err(recovery_outcome_unknown(
                        "Applied successor lacks the exact readback evidence for its intent",
                    ));
                }
            }
            EffectOutcome::Accepted | EffectOutcome::Rejected => {
                return Err(recovery_outcome_unknown(
                    "linked target outcome is outside the readback settlement states",
                ));
            }
        }
        Ok(())
    }

    fn validate_reconcile_summary(
        &self,
        reconcile_intent: &OperationIntent,
        summary: &RuntimeOutcome,
    ) -> Result<()> {
        self.validate_intent_state_identity(reconcile_intent)?;
        let target_id = reconcile_intent
            .reconcile_target_operation_id
            .as_deref()
            .filter(|id| !id.trim().is_empty())
            .ok_or_else(|| recovery_outcome_unknown("reconcile intent has no exact target"))?;
        let summary_receipt: ModuleReceiptIdentity =
            serde_json::from_value(summary.details["module_receipt"].clone()).map_err(|_| {
                recovery_outcome_unknown("reconcile summary has no valid module receipt")
            })?;
        if summary.operation_id != reconcile_intent.operation_id
            || summary.operation_id == target_id
            || summary.outcome != EffectOutcome::Applied
            || summary.native_scope_key.as_deref()
                != Some(reconcile_intent.native_scope_key.as_str())
            || summary.native_root_id.as_deref() != reconcile_intent.native_root_id.as_deref()
            || summary.native_input_id.is_some()
            || summary.details["target_operation_id"] != target_id
            || summary.details["completion_condition"] != "native_readback_recorded"
            || summary.details["resolved"].as_bool().is_none()
            || summary.details["native_replay"] != false
            || summary_receipt != reconcile_intent.module_receipt
        {
            return Err(recovery_outcome_unknown(
                "reconcile summary differs from its exact admitted Operation intent",
            ));
        }

        if summary.details["resolved"] == true {
            let target_history = self.load(target_id)?;
            if target_history.torn_tail_evidence.is_some() {
                return Err(recovery_outcome_unknown(
                    "resolved reconcile target has a torn journal suffix",
                ));
            }
            let target_intent = target_history.intent.as_ref().ok_or_else(|| {
                recovery_outcome_unknown("resolved reconcile target has no saved intent")
            })?;
            self.validate_intent_state_identity(target_intent)?;
            let target_outcome = target_history.outcome.as_ref().ok_or_else(|| {
                recovery_outcome_unknown("resolved reconcile target has no saved outcome")
            })?;
            let target_receipt: ModuleReceiptIdentity =
                serde_json::from_value(target_outcome["details"]["module_receipt"].clone())
                    .map_err(|_| {
                        recovery_outcome_unknown("resolved target outcome has no valid receipt")
                    })?;
            if target_intent.method == "agent.reconcile"
                || target_intent.binding_id != reconcile_intent.binding_id
                || target_intent.generation != reconcile_intent.generation
                || target_intent.native_scope_key != reconcile_intent.native_scope_key
                || !same_route_digest(&target_intent.route_sha256, &reconcile_intent.route_sha256)
                || target_outcome["outcome"] != "applied"
                || target_receipt != target_intent.module_receipt
            {
                return Err(recovery_outcome_unknown(
                    "resolved reconcile summary differs from the saved Applied target Operation",
                ));
            }
        }
        Ok(())
    }

    fn validate_outcome_publication_predecessor(
        &self,
        operation_id: &str,
        history: &OperationHistory,
    ) -> Result<()> {
        if history.torn_tail_evidence.is_some() {
            return Err(torn_ack_recovery_unknown());
        }
        let path = self
            .outbox
            .join(format!("outcome-{}.json", operation_key(operation_id)));
        let latest = history.outcome.as_ref();
        match fs::symlink_metadata(&path) {
            Ok(_) => {
                ensure_regular(&path, "outbox item")?;
                let bytes = read_limited(&path, RECORD_LIMIT)?;
                let pending: PendingItem = serde_json::from_slice(&bytes)
                    .map_err(|_| Error::new("ADAPTER_OUTBOX", "existing outbox item is invalid"))?;
                let Some(latest) = latest else {
                    return Err(Error::new(
                        "ADAPTER_OUTBOX",
                        "outbox outcome has no durable journal predecessor",
                    ));
                };
                let pending_digest = digest_json(&pending.payload)?;
                if pending.kind != "outcome"
                    || pending.key != operation_id
                    || pending.payload != *latest
                    || history.outcome_sha256.as_deref() != Some(pending_digest.as_str())
                {
                    return Err(Error::new(
                        "ADAPTER_OUTBOX",
                        "outbox slot is not the exact durable outcome predecessor",
                    ));
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if latest.is_some()
                    && history.outcome_sha256.as_deref().is_some_and(|digest| {
                        history.acknowledged_sha256.as_deref() != Some(digest)
                    })
                {
                    return Err(recovery_outcome_unknown(
                        "unacknowledged outcome predecessor has no deterministic outbox slot",
                    ));
                }
            }
            Err(_) => {
                return Err(Error::new(
                    "ADAPTER_OUTBOX",
                    "outbox predecessor cannot be inspected",
                ));
            }
        }
        Ok(())
    }

    pub fn validate_pending_outcome(&self, path: &Path, item: &PendingItem) -> Result<()> {
        let operation_id = item.payload["operation_id"]
            .as_str()
            .ok_or_else(|| Error::new("ADAPTER_OUTBOX", "saved outcome has no operation ID"))?;
        let expected_path = self
            .outbox
            .join(format!("outcome-{}.json", operation_key(operation_id)));
        if item.kind != "outcome" || item.key != operation_id || path != expected_path {
            return Err(Error::new(
                "ADAPTER_OUTBOX",
                "pending outcome identity differs from its deterministic slot",
            ));
        }
        let history = self.load(operation_id)?;
        if history.torn_tail_evidence.is_some()
            || history.outcome.as_ref() != Some(&item.payload)
            || history.outcome_sha256.as_deref() != Some(digest_json(&item.payload)?.as_str())
        {
            return Err(recovery_outcome_unknown(
                "pending outcome is not the exact latest durable revision",
            ));
        }
        if item.payload["details"]
            .get("reconcile_operation_id")
            .is_some()
        {
            let outcome: RuntimeOutcome =
                serde_json::from_value(item.payload.clone()).map_err(|_| {
                    recovery_outcome_unknown("pending linked reconciliation outcome is malformed")
                })?;
            let intent = history.intent.as_ref().ok_or_else(|| {
                recovery_outcome_unknown("pending linked target has no saved intent")
            })?;
            self.validate_linked_reconcile_outcome(intent, &outcome)?;
        }
        if let Some(intent) = history
            .intent
            .as_ref()
            .filter(|intent| intent.method == "agent.reconcile")
        {
            let summary: RuntimeOutcome = serde_json::from_value(item.payload.clone())
                .map_err(|_| recovery_outcome_unknown("pending reconcile summary is malformed"))?;
            self.validate_reconcile_summary(intent, &summary)?;
        }
        Ok(())
    }

    /// Promote the exact previous Unknown revision to its linked, verified
    /// Applied successor in place, without replaying a native mutation.
    ///
    /// The outbox slot is `outcome-<hashed operation key>.json`, so a
    /// predecessor and its reconciled successor share one deterministic path.
    /// A prior ACK remains part of history; it acknowledges only its own
    /// revision and does not prohibit this exact readback successor.
    fn supersede_unknown_with_linked_applied(
        &self,
        path: &Path,
        item: &PendingItem,
        previous_value: &Value,
    ) -> Result<bool> {
        if item.kind != "outcome" {
            return Ok(false);
        }
        if previous_value["kind"] != "outcome"
            || previous_value["key"] != item.key
            || previous_value["payload"]["operation_id"] != item.payload["operation_id"]
        {
            return Ok(false);
        }
        let previous = previous_value["payload"]["outcome"]
            .as_str()
            .ok_or_else(|| Error::new("ADAPTER_OUTBOX", "existing outbox item is invalid"))?;
        let successor = item.payload["outcome"]
            .as_str()
            .ok_or_else(|| Error::new("ADAPTER_JOURNAL", "saved outcome has no state"))?;
        // Only the recovery-ambiguous Unknown state may be upgraded, and only
        // by the verified native readback that produces Applied.
        if previous != "unknown" || successor != "applied" {
            return Ok(false);
        }
        if path
            != self
                .outbox
                .join(format!("outcome-{}.json", operation_key(&item.key)))
        {
            return Ok(false);
        }
        let history = self.load(&item.key)?;
        if history.torn_tail_evidence.is_some() {
            return Err(torn_ack_recovery_unknown());
        }
        let Some(current) = history.outcome.as_ref() else {
            return Ok(false);
        };
        let current_digest = digest_json(&item.payload)?;
        if current != &item.payload
            || history.outcome_sha256.as_deref() != Some(current_digest.as_str())
        {
            return Err(recovery_outcome_unknown(
                "outbox successor is not the exact latest durable outcome revision",
            ));
        }
        let Some(predecessor) = history.outcome_revisions.iter().rev().nth(1) else {
            return Err(recovery_outcome_unknown(
                "outbox promotion has no retained predecessor revision",
            ));
        };
        let previous_payload = &previous_value["payload"];
        let previous_digest = digest_json(previous_payload)?;
        if predecessor.outcome != *previous_payload
            || predecessor.sha256 != previous_digest
            || previous != "unknown"
            || successor != "applied"
            || history.outcome_revisions.last().is_none_or(|latest| {
                latest.outcome != item.payload || latest.sha256 != current_digest
            })
        {
            return Err(recovery_outcome_unknown(
                "outbox slot is not the exact Unknown predecessor of the durable Applied revision",
            ));
        }
        self.validate_pending_outcome(path, item)?;
        let bytes = serde_json::to_vec(item)?;
        if bytes.len() > RECORD_LIMIT {
            return Err(Error::new(
                "ADAPTER_OUTBOX",
                "pending host message exceeds its record limit",
            ));
        }
        replace_private_durable(path, &bytes).map_err(|_| {
            Error::new(
                "ADAPTER_OUTBOX",
                "pending item could not be durably published",
            )
        })?;
        retention::refresh_operation(self, &item.key);
        Ok(true)
    }
}

fn read_operation_history(
    path: &Path,
    expected_operation_id: Option<&str>,
) -> Result<OperationHistory> {
    ensure_regular(path, "operation journal")?;
    let bytes = read_limited(path, HISTORY_LIMIT)?;
    read_operation_history_bytes(&bytes, expected_operation_id)
}

fn read_operation_history_bytes(
    bytes: &[u8],
    expected_operation_id: Option<&str>,
) -> Result<OperationHistory> {
    let mut history = OperationHistory::default();
    let mut observed_operation_id: Option<String> = None;
    // File I/O is complete before this pure, in-memory domain decoder runs;
    // scanner validation failures therefore cannot hide read or Store errors.
    let verdict = scan_jsonl(bytes, RECORD_LIMIT, |_, frame| {
        let record: JournalRecord = serde_json::from_slice(&frame[..frame.len() - 1])
            .map_err(|_| Error::new("ADAPTER_JOURNAL", "operation journal record is invalid"))?;
        apply_operation_record(
            &mut history,
            &mut observed_operation_id,
            expected_operation_id,
            record,
        )
    })?;

    match verdict {
        JsonlScanVerdict::Complete { .. } => {}
        JsonlScanVerdict::TornLastFrame {
            valid_bytes,
            tail_digest,
            tail_bytes,
            ..
        } => {
            history.torn_tail_evidence = Some(TornTailEvidence {
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
                format!("operation journal damaged at byte {valid_prefix_bytes}: {reason:?}"),
            ));
        }
    }
    Ok(history)
}

fn acknowledged_outcome_record(operation_id: &str, digest: &str) -> JournalRecord {
    JournalRecord {
        version: 2,
        operation_id: operation_id.into(),
        kind: "acknowledged".into(),
        intent: None,
        outcome: None,
        outcome_sha256: Some(digest.into()),
        result_params: None,
        result_sha256: None,
    }
}

fn torn_ack_recovery_unknown() -> Error {
    Error::new(
        "ADAPTER_JOURNAL_RECOVERY_UNKNOWN",
        "torn journal suffix could not be safely removed from the exact saved payload prefix",
    )
}

fn recovery_outcome_unknown(message: &'static str) -> Error {
    Error::new("ADAPTER_JOURNAL_RECOVERY_UNKNOWN", message)
}

fn same_route_digest(left: &str, right: &str) -> bool {
    fn route_hex(value: &str) -> Option<&str> {
        let value = value.strip_prefix("sha256:").unwrap_or(value);
        (value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)))
        .then_some(value)
    }

    route_hex(left).is_some() && route_hex(left) == route_hex(right)
}

fn validate_outcome_successor(previous: &Value, successor: &Value) -> Result<()> {
    let linked_reconcile = successor["details"]["reconcile_operation_id"]
        .as_str()
        .is_some_and(|operation_id| !operation_id.trim().is_empty());
    if previous["operation_id"] == successor["operation_id"]
        && previous["outcome"] == "unknown"
        && successor["outcome"] == "applied"
        && linked_reconcile
        && successor["details"]["native_replay"] == false
    {
        return Ok(());
    }
    Err(recovery_outcome_unknown(
        "changed outcome is not an exact readback-linked Unknown-to-Applied successor",
    ))
}

fn read_opened_file_for_repair(path: &Path, file: &mut File) -> Result<Vec<u8>> {
    if !opened_file_matches_path(file, path) {
        return Err(torn_ack_recovery_unknown());
    }
    let initial_metadata = file.metadata().map_err(|_| torn_ack_recovery_unknown())?;
    if initial_metadata.len() > HISTORY_LIMIT as u64 {
        return Err(torn_ack_recovery_unknown());
    }

    let mut bytes = Vec::new();
    Read::by_ref(file)
        .take((HISTORY_LIMIT + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| torn_ack_recovery_unknown())?;
    if bytes.len() > HISTORY_LIMIT || initial_metadata.len() != bytes.len() as u64 {
        return Err(torn_ack_recovery_unknown());
    }
    if !opened_file_matches_path(file, path) {
        return Err(torn_ack_recovery_unknown());
    }

    file.seek(SeekFrom::Start(0))
        .map_err(|_| torn_ack_recovery_unknown())?;
    let mut confirmed_bytes = Vec::new();
    Read::by_ref(file)
        .take((HISTORY_LIMIT + 1) as u64)
        .read_to_end(&mut confirmed_bytes)
        .map_err(|_| torn_ack_recovery_unknown())?;
    let final_metadata = file.metadata().map_err(|_| torn_ack_recovery_unknown())?;
    if confirmed_bytes != bytes
        || confirmed_bytes.len() > HISTORY_LIMIT
        || final_metadata.len() != bytes.len() as u64
        || !opened_file_matches_path(file, path)
    {
        return Err(torn_ack_recovery_unknown());
    }
    Ok(bytes)
}

fn opened_file_matches_path(file: &File, path: &Path) -> bool {
    if ensure_regular(path, "operation journal").is_err() {
        return false;
    }
    swarm_process::opened_regular_file_matches_path(file, path).unwrap_or(false)
}

fn apply_operation_record(
    history: &mut OperationHistory,
    observed_operation_id: &mut Option<String>,
    expected_operation_id: Option<&str>,
    record: JournalRecord,
) -> Result<()> {
    if record.version != 2
        || expected_operation_id.is_some_and(|expected| expected != record.operation_id.as_str())
        || observed_operation_id
            .as_deref()
            .is_some_and(|observed| observed != record.operation_id.as_str())
    {
        return Err(Error::new(
            "ADAPTER_JOURNAL",
            "operation journal identity changed",
        ));
    }
    let operation_id = record.operation_id.clone();
    *observed_operation_id = Some(operation_id.clone());
    match record.kind.as_str() {
        "intent" => {
            let intent = record
                .intent
                .ok_or_else(|| Error::new("ADAPTER_JOURNAL", "journal intent is missing"))?;
            if intent.version != 2
                || intent.operation_id != operation_id
                || history.intent.as_ref().is_some_and(|old| old != &intent)
            {
                return Err(Error::new(
                    "ADAPTER_JOURNAL",
                    "operation intent conflicts with its checkpoint",
                ));
            }
            history.intent = Some(intent);
        }
        "outcome" => {
            let outcome = record
                .outcome
                .ok_or_else(|| Error::new("ADAPTER_JOURNAL", "journal outcome is missing"))?;
            if outcome["operation_id"] != operation_id {
                return Err(Error::new(
                    "ADAPTER_JOURNAL",
                    "journal outcome identity changed",
                ));
            }
            let digest = digest_json(&outcome)?;
            if record.outcome_sha256.as_deref() != Some(digest.as_str()) {
                return Err(Error::new(
                    "ADAPTER_JOURNAL",
                    "journal outcome digest mismatch",
                ));
            }
            if history.outcome_sha256.as_deref() != Some(digest.as_str()) {
                if let Some(previous) = history.outcome.as_ref() {
                    validate_outcome_successor(previous, &outcome)?;
                }
                history.outcome_revisions.push(OutcomeRevision {
                    outcome: outcome.clone(),
                    sha256: digest.clone(),
                    acknowledged: false,
                });
                // An ACK is scoped to the revision it followed. Keep earlier
                // ACKs in outcome_revisions while resetting the current slot.
                history.acknowledged_sha256 = None;
            }
            history.outcome = Some(outcome);
            history.outcome_sha256 = Some(digest);
        }
        "acknowledged" => {
            let digest = record.outcome_sha256.ok_or_else(|| {
                Error::new("ADAPTER_JOURNAL", "acknowledgement digest is missing")
            })?;
            if history.outcome_sha256.as_deref() != Some(digest.as_str()) {
                return Err(Error::new(
                    "ADAPTER_JOURNAL",
                    "acknowledgement does not match the latest saved outcome",
                ));
            }
            let latest = history.outcome_revisions.last_mut().ok_or_else(|| {
                Error::new("ADAPTER_JOURNAL", "acknowledgement has no outcome revision")
            })?;
            if latest.sha256 != digest {
                return Err(Error::new(
                    "ADAPTER_JOURNAL",
                    "acknowledgement does not match the latest outcome revision",
                ));
            }
            latest.acknowledged = true;
            history.acknowledged_sha256 = Some(digest);
        }
        "result_page" => {
            let params = record
                .result_params
                .ok_or_else(|| Error::new("ADAPTER_JOURNAL", "saved result page is missing"))?;
            if params["operation_id"] != operation_id {
                return Err(Error::new(
                    "ADAPTER_JOURNAL",
                    "saved result page identity changed",
                ));
            }
            let digest = digest_json(&params)?;
            if record.result_sha256.as_deref() != Some(digest.as_str())
                || history
                    .result_sha256
                    .as_deref()
                    .is_some_and(|old| old != digest)
            {
                return Err(Error::new(
                    "ADAPTER_JOURNAL",
                    "saved result page digest changed",
                ));
            }
            history.result_params = Some(params);
            history.result_sha256 = Some(digest);
        }
        "result_acknowledged" => {
            let digest = record.result_sha256.ok_or_else(|| {
                Error::new(
                    "ADAPTER_JOURNAL",
                    "result acknowledgement digest is missing",
                )
            })?;
            if history.result_sha256.as_deref() != Some(digest.as_str()) {
                return Err(Error::new(
                    "ADAPTER_JOURNAL",
                    "result acknowledgement does not match the saved page",
                ));
            }
            history.result_acknowledged_sha256 = Some(digest);
        }
        _ => return Err(Error::new("ADAPTER_JOURNAL", "unknown journal record")),
    }
    Ok(())
}

fn history_is_unresolved(history: &OperationHistory) -> bool {
    !history_is_releasable(history)
}

fn history_is_releasable(history: &OperationHistory) -> bool {
    if history.torn_tail_evidence.is_some() {
        return false;
    }
    let result_page_acknowledged = match (
        history.result_params.as_ref(),
        history.result_sha256.as_deref(),
        history.result_acknowledged_sha256.as_deref(),
    ) {
        (None, None, None) => false,
        (Some(_), Some(result_digest), Some(acknowledged)) => result_digest == acknowledged,
        _ => return false,
    };
    match history.outcome.as_ref() {
        Some(outcome) => {
            let Some(digest) = history.outcome_sha256.as_deref() else {
                return false;
            };
            let terminal_receipt =
                matches!(outcome["outcome"].as_str(), Some("applied" | "rejected"));
            let reconcile_resolved = history.intent.as_ref().is_none_or(|intent| {
                intent.method != "agent.reconcile"
                    || outcome["outcome"] == "rejected"
                    || outcome["details"]["resolved"].as_bool() == Some(true)
            });
            history.acknowledged_sha256.as_deref() == Some(digest)
                && terminal_receipt
                && reconcile_resolved
                && (history.result_params.is_none() || result_page_acknowledged)
        }
        // Store's exact module.result acknowledgement settles the read
        // Operation directly; it has no separate RuntimeOutcome record.
        // Limit this path to agent.result and the exact acknowledged page.
        None => {
            history
                .intent
                .as_ref()
                .is_some_and(|intent| intent.method == "agent.result")
                && result_page_acknowledged
        }
    }
}

fn has_acknowledged_result_page(history: &OperationHistory) -> bool {
    history
        .intent
        .as_ref()
        .is_some_and(|intent| intent.method == "agent.result")
        && matches!(
            (
                history.result_params.as_ref(),
                history.result_sha256.as_deref(),
                history.result_acknowledged_sha256.as_deref(),
            ),
            (Some(_), Some(digest), Some(acknowledged)) if digest == acknowledged
        )
}

fn ensure_regular(path: &Path, label: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|_| Error::new("ADAPTER_STATE", format!("{label} is unavailable")))?;
    if !metadata.is_file() {
        return Err(Error::new(
            "ADAPTER_STATE",
            format!("{label} is not a regular file"),
        ));
    }
    Ok(())
}

fn ensure_directory(path: &Path, label: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|_| Error::new("ADAPTER_STATE", format!("{label} is unavailable")))?;
    if !metadata.file_type().is_dir() {
        return Err(Error::new(
            "ADAPTER_STATE",
            format!("{label} must be a real directory, not a link"),
        ));
    }
    Ok(())
}

fn remember_one_root(slot: &mut Option<String>, candidate: String) -> Result<()> {
    if slot
        .as_deref()
        .is_some_and(|previous| previous != candidate.as_str())
    {
        return Err(Error::new(
            "NATIVE_IDENTITY_MISMATCH",
            "native root checkpoints conflict",
        ));
    }
    *slot = Some(candidate);
    Ok(())
}

fn expected_root_id(binding_id: &str, generation: i64) -> String {
    format!(
        "ses_swarm_{:x}",
        Sha256::digest(format!("{binding_id}/{generation}").as_bytes())
    )
}

fn read_limited(path: &Path, limit: usize) -> Result<Vec<u8>> {
    let file = File::open(path)
        .map_err(|_| Error::new("ADAPTER_STATE", "private state file cannot be read"))?;
    let mut bytes = Vec::new();
    file.take((limit + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| Error::new("ADAPTER_STATE", "private state read failed"))?;
    if bytes.len() > limit {
        return Err(Error::new(
            "ADAPTER_STATE",
            "private state record exceeds its limit",
        ));
    }
    Ok(bytes)
}

fn append_bytes(path: &Path, bytes: &[u8]) -> Result<()> {
    if bytes.len() > RECORD_LIMIT {
        return Err(Error::new(
            "ADAPTER_JOURNAL",
            "private record exceeds its frame limit",
        ));
    }
    if path.exists() {
        ensure_regular(path, "private journal")?;
        let mut file = OpenOptions::new()
            .append(true)
            .open(path)
            .map_err(|_| Error::new("ADAPTER_JOURNAL", "private journal cannot be opened"))?;
        private_permissions(path, false)?;
        file.write_all(bytes)
            .map_err(|_| Error::new("ADAPTER_JOURNAL", "private journal append failed"))?;
        file.sync_all()
            .map_err(|_| Error::new("ADAPTER_JOURNAL", "private journal flush failed"))?;
        Ok(())
    } else {
        write_private_new(path, bytes)
    }
}

pub fn operation_key(operation_id: &str) -> String {
    format!("{:x}", Sha256::digest(operation_id.as_bytes()))
}

pub fn digest_json(value: &Value) -> Result<String> {
    let canonical = serde_json::to_string(value)?;
    Ok(format!("sha256:{:x}", Sha256::digest(canonical.as_bytes())))
}

#[cfg(test)]
mod journal65_parity_tests {
    use super::*;
    use serde_json::json;
    use std::io::Write;

    const PARITY_BINDING: &str = "parity-binding";
    const PARITY_GENERATION: i64 = 1;
    const PARITY_SCOPE: &str = "parity-scope";
    const PARITY_ROUTE: &str =
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    fn parity_intent(operation_id: &str) -> OperationIntent {
        let module_receipt: ModuleReceiptIdentity = serde_json::from_value(json!({
            "schema_version": 1,
            "module_id": "eliot.opencode",
            "artifact": {"artifact_id": "fixture-opencode", "version": "0.1.0"},
            "protocol": {"major": 1, "minor": 0},
            "binding_id": PARITY_BINDING,
            "binding_generation": PARITY_GENERATION,
            "operation_id": operation_id,
            "input_sha256": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
        }))
        .unwrap();

        OperationIntent {
            version: 2,
            operation_id: operation_id.to_owned(),
            module_receipt,
            method: "agent.reply".to_owned(),
            binding_id: PARITY_BINDING.to_owned(),
            generation: PARITY_GENERATION,
            native_scope_key: PARITY_SCOPE.to_owned(),
            native_root_id: None,
            native_input_id: None,
            prompt_sha256: None,
            prompt_bytes: None,
            reconcile_target_operation_id: None,
            result_input_status: None,
            result_assistant: None,
            dispatch_admission: None,
            route_sha256: PARITY_ROUTE.to_owned(),
            model: json!({"id": "fixture-model"}),
            marker: json!({"fixture": "journal65-parity"}),
        }
    }

    fn parity_outcome(operation_id: &str) -> Value {
        json!({"operation_id": operation_id, "outcome": "unknown"})
    }

    fn open_parity_journal(root: &Path) -> Journal {
        Journal::open(root, PARITY_BINDING, PARITY_GENERATION, PARITY_SCOPE, PARITY_ROUTE)
            .unwrap()
    }

    fn fresh_root(prefix: &str) -> PathBuf {
        std::env::temp_dir().join(format!("swarm-opencode-journal65-{}-{}", prefix, uuid::Uuid::new_v4()))
    }

    #[test]
    fn load_and_load_by_path_agree_on_complete_torn_and_interior_damage() {
        let root = fresh_root("parity");
        let journal = open_parity_journal(&root);
        let operation_id = "parity-operation";
        journal.write_intent(&parity_intent(operation_id)).unwrap();
        let outcome = parity_outcome(operation_id);
        journal.queue_outcome_value(&outcome).unwrap();
        let path = journal.operation_path(operation_id);
        let complete_bytes = std::fs::read(&path).unwrap();

        let via_load = journal.load(operation_id).unwrap();
        let via_path = journal.load_by_path(&path).unwrap();
        assert!(via_load.torn_tail_evidence.is_none());
        assert!(via_path.torn_tail_evidence.is_none());
        assert_eq!(via_load.intent, via_path.intent);
        assert_eq!(via_load.outcome, via_path.outcome);
        assert_eq!(via_load.outcome, Some(outcome.clone()));
        assert_eq!(via_load.outcome_sha256, via_path.outcome_sha256);
        assert_eq!(via_load.acknowledged_sha256, via_path.acknowledged_sha256);
        assert_eq!(format!("{:?}", via_load), format!("{:?}", via_path));
        assert_eq!(std::fs::read(&path).unwrap(), complete_bytes);

        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"{\"kind\":\"outcome\"")
            .unwrap();
        let torn_bytes = std::fs::read(&path).unwrap();
        let via_load = journal.load(operation_id).unwrap();
        let via_path = journal.load_by_path(&path).unwrap();
        let load_tail = via_load.torn_tail_evidence.clone().expect("load reports torn tail");
        let path_tail = via_path
            .torn_tail_evidence
            .clone()
            .expect("load_by_path reports torn tail");
        assert_eq!(load_tail, path_tail);
        assert!(!via_load.native_replay_permitted());
        assert!(!via_path.native_replay_permitted());
        assert_eq!(via_load.intent, via_path.intent);
        assert_eq!(via_load.outcome, via_path.outcome);
        assert_eq!(format!("{:?}", via_load), format!("{:?}", via_path));
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
        let load_err = journal.load(operation_id).unwrap_err();
        let path_err = journal.load_by_path(&path).unwrap_err();
        assert_eq!(load_err.code, "ADAPTER_JOURNAL");
        assert_eq!(path_err.code, "ADAPTER_JOURNAL");
        assert_eq!(load_err.message, path_err.message);
        assert!(load_err.message.contains("damaged at byte"));
        assert_eq!(std::fs::read(&path).unwrap(), damaged);

        drop(journal);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn repair_refuses_file_changed_after_scan_and_keeps_new_bytes() {
        let root = fresh_root("changed");
        let journal = open_parity_journal(&root);
        let operation_id = "parity-changed-operation";
        journal.write_intent(&parity_intent(operation_id)).unwrap();
        let outcome = parity_outcome(operation_id);
        journal.queue_outcome_value(&outcome).unwrap();
        let path = journal.operation_path(operation_id);
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"{\"kind\":\"outcome\"")
            .unwrap();
        let scanned = journal.load(operation_id).unwrap();
        let evidence = scanned.torn_tail_evidence.clone().expect("torn tail scanned");
        let torn_bytes = std::fs::read(&path).unwrap();
        assert_eq!(torn_bytes.len(), evidence.valid_prefix_bytes + evidence.tail_bytes);

        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"{not-json}\n")
            .unwrap();
        let changed_bytes = std::fs::read(&path).unwrap();
        assert!(changed_bytes.len() > torn_bytes.len());
        assert!(changed_bytes.starts_with(&torn_bytes));

        let err = journal.repair_torn_outcome_ack(&outcome).unwrap_err();
        assert_eq!(err.code, "ADAPTER_JOURNAL_RECOVERY_UNKNOWN");
        assert_eq!(std::fs::read(&path).unwrap(), changed_bytes);

        let rescan = journal.load(operation_id).unwrap_err();
        assert_eq!(rescan.code, "ADAPTER_JOURNAL");
        assert!(rescan.message.contains("damaged at byte"));

        drop(journal);
        std::fs::remove_dir_all(root).unwrap();
    }
}
