use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering},
};
use swarm_contracts::{
    error::{Error, Result},
    runtime::{ModuleReceiptIdentity, RuntimeOutcome, TaskDispatchAdmissionReceipt},
};
use swarm_process::{
    JsonlScanVerdict, private_permissions, remove_private_durable, replace_private_durable,
    scan_jsonl, write_private_new,
};

const RECORD_LIMIT: usize = 1_048_576;
const HISTORY_LIMIT: usize = 8 * RECORD_LIMIT;
const OUTBOX_BATCH: usize = 32;
const RETENTION_DELETE_BATCH: usize = 32;
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
    pub result_params: Option<Value>,
    pub result_sha256: Option<String>,
    pub result_acknowledged_sha256: Option<String>,
    /// Evidence for an incomplete final frame. The retained prefix remains
    /// available, but the journal must not be appended to or replayed.
    pub torn_tail_evidence: Option<TornTailEvidence>,
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
    pub reclaimed: usize,
    pub complete: bool,
}

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
) -> Result<RetainedOperation> {
    ensure_regular(&path, "operation journal")?;
    let observed_bytes = read_limited(&path, HISTORY_LIMIT)?;
    let history = read_operation_history_bytes(&observed_bytes, None)?;
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
    if history_is_unresolved(&history)
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
        observed_sha256: format!("{:x}", Sha256::digest(&observed_bytes)),
        releasable: history_is_releasable(&history),
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

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct StateIdentity {
    version: u32,
    binding_id: String,
    generation: i64,
    native_scope_key: String,
    route_sha256: String,
}

pub struct Journal {
    operations: PathBuf,
    outbox: PathBuf,
    checkpoints: PathBuf,
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
            for existing in ["operations", "outbox", "native-checkpoints.jsonl"] {
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
        Ok(Self {
            operations,
            outbox,
            checkpoints,
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
        if history.outcome_sha256.as_deref() != Some(digest.as_str()) {
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

    pub fn pending_items(&self) -> Result<Vec<(PathBuf, PendingItem)>> {
        let mut items = Vec::with_capacity(OUTBOX_BATCH);
        let entries = fs::read_dir(&self.outbox)
            .map_err(|_| Error::new("ADAPTER_OUTBOX", "outbox cannot be read"))?;
        for entry in entries {
            let entry =
                entry.map_err(|_| Error::new("ADAPTER_OUTBOX", "outbox entry cannot be read"))?;
            if items.len() >= OUTBOX_BATCH {
                break;
            }
            let path = entry.path();
            ensure_regular(&path, "outbox item")?;
            let bytes = read_limited(&path, RECORD_LIMIT)?;
            let item: PendingItem = serde_json::from_slice(&bytes)
                .map_err(|_| Error::new("ADAPTER_OUTBOX", "outbox item is invalid"))?;
            items.push((path, item));
        }
        Ok(items)
    }

    pub fn recover_outbox(&self) -> Result<Vec<OperationRecoveryState>> {
        let mut recovery_states = Vec::new();
        let entries = fs::read_dir(&self.operations)
            .map_err(|_| Error::new("ADAPTER_JOURNAL", "operation directory cannot be read"))?;
        for entry in entries {
            let entry = entry
                .map_err(|_| Error::new("ADAPTER_JOURNAL", "operation entry cannot be read"))?;
            let path = entry.path();
            ensure_regular(&path, "operation journal")?;
            let Some(name) = path.file_stem().and_then(|v| v.to_str()) else {
                return Err(Error::new(
                    "ADAPTER_JOURNAL",
                    "operation filename is invalid",
                ));
            };
            let history = self.load_by_path(&path)?;
            if let (Some(outcome), Some(digest)) =
                (history.outcome.as_ref(), history.outcome_sha256.as_ref())
                && history.acknowledged_sha256.as_deref() != Some(digest.as_str())
            {
                let operation_id = outcome["operation_id"].as_str().ok_or_else(|| {
                    Error::new("ADAPTER_JOURNAL", "outcome operation ID is missing")
                })?;
                let expected = operation_key(operation_id);
                if name != expected {
                    return Err(Error::new(
                        "ADAPTER_JOURNAL",
                        "operation filename does not match its identity",
                    ));
                }
                self.write_pending(PendingItem {
                    kind: "outcome".into(),
                    key: operation_id.into(),
                    payload: outcome.clone(),
                })?;
            }
            if let (Some(params), Some(digest)) = (
                history.result_params.as_ref(),
                history.result_sha256.as_ref(),
            ) && history.result_acknowledged_sha256.as_deref() != Some(digest.as_str())
            {
                let operation_id = params["operation_id"].as_str().ok_or_else(|| {
                    Error::new("ADAPTER_JOURNAL", "result page operation ID is missing")
                })?;
                if name != operation_key(operation_id) {
                    return Err(Error::new(
                        "ADAPTER_JOURNAL",
                        "result page filename does not match its identity",
                    ));
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
                    return Err(Error::new(
                        "ADAPTER_JOURNAL",
                        "operation filename does not match its identity",
                    ));
                }
                recovery_states.push(OperationRecoveryState {
                    operation_key: name.to_owned(),
                    operation_id,
                    history,
                });
            }
        }
        Ok(recovery_states)
    }

    pub fn retention_pass_due_after_ack(&self) -> bool {
        self.acknowledgements_since_retention
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1)
            % RETENTION_ACK_INTERVAL
            == 0
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
        // References are derived from the exact unresolved journal intents.
        // Stream both passes instead of requiring the complete operation set
        // to fit a scan cap or retaining an inventory-sized path vector.
        let mut unresolved_references = BTreeSet::new();
        let entries = fs::read_dir(&self.operations)
            .map_err(|_| Error::new("ADAPTER_JOURNAL", "operation directory cannot be read"))?;
        for entry in entries {
            let entry = entry
                .map_err(|_| Error::new("ADAPTER_JOURNAL", "operation entry cannot be read"))?;
            let operation =
                inspect_retained_operation(entry.path(), binding_id, generation, scope)?;
            unresolved_references.extend(operation.references);
        }

        let mut summary = RetentionSummary::default();
        let mut batch_deferred = false;
        let entries = fs::read_dir(&self.operations)
            .map_err(|_| Error::new("ADAPTER_JOURNAL", "operation directory cannot be read"))?;
        for entry in entries {
            let entry = entry
                .map_err(|_| Error::new("ADAPTER_JOURNAL", "operation entry cannot be read"))?;
            let operation =
                inspect_retained_operation(entry.path(), binding_id, generation, scope)?;
            if !operation.releasable
                || unresolved_references.contains(&operation.operation_id)
                || operation.result_source
                || self.operation_has_pending_delivery(&operation.operation_id)?
            {
                continue;
            }
            if summary.reclaimed == RETENTION_DELETE_BATCH {
                batch_deferred = true;
                continue;
            }

            self.checkpoint_operation_root(
                operation.native_root_id.as_deref(),
                operation.native_scope_key.as_deref(),
                binding_id,
                generation,
                scope,
            )?;
            let current_bytes = read_limited(&operation.path, HISTORY_LIMIT)?;
            if format!("{:x}", Sha256::digest(&current_bytes)) != operation.observed_sha256 {
                return Err(Error::new(
                    "ADAPTER_JOURNAL",
                    "operation journal changed during retention classification",
                ));
            }
            match remove_private_durable(&operation.path) {
                Ok(_) => summary.reclaimed += 1,
                Err(_) => {
                    let _ = replace_private_durable(&operation.path, &current_bytes);
                    return Err(Error::new(
                        "ADAPTER_JOURNAL",
                        "acknowledged operation journal could not be durably removed",
                    ));
                }
            }
        }
        summary.complete = !batch_deferred;
        Ok(summary)
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
        self.append(
            operation_id,
            &JournalRecord {
                version: 2,
                operation_id: operation_id.into(),
                kind: "acknowledged".into(),
                intent: None,
                outcome: None,
                outcome_sha256: Some(digest),
                result_params: None,
                result_sha256: None,
            },
        )
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
                && let Some(intent) = self.load(&item.key)?.intent
                && intent.binding_id == binding_id
                && intent.generation == generation
                && intent.native_scope_key == scope
                && let Some(root) = item.payload["native_root_id"].as_str()
            {
                remember_one_root(&mut root_checkpoint, root.into())?;
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
                return Err(Error::new(
                    "ADAPTER_JOURNAL",
                    "operation filename is invalid",
                ));
            };
            let history = self.load_by_path(&path)?;
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

    pub fn remove_pending(&self, path: &Path) -> Result<()> {
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
        match remove_private_durable(path) {
            Ok(_) => Ok(()),
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
        append_bytes(&path, &bytes)
    }

    fn write_pending(&self, item: PendingItem) -> Result<()> {
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
            let new_value = serde_json::to_value(item)?;
            if previous_value != new_value {
                return Err(Error::new(
                    "ADAPTER_OUTBOX",
                    "pending item identity conflicts with saved result",
                ));
            }
            return replace_private_durable(&path, &previous).map_err(|_| {
                Error::new(
                    "ADAPTER_OUTBOX",
                    "pending item could not be durably published",
                )
            });
        }
        write_private_new(&path, &bytes)
    }

    fn load_by_path(&self, path: &Path) -> Result<OperationHistory> {
        read_operation_history(path, None)
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
    let verdict = scan_jsonl(&bytes, RECORD_LIMIT, |_, frame| {
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
            history.acknowledged_sha256.as_deref() == Some(digest)
                && matches!(
                    outcome["outcome"].as_str(),
                    Some("applied" | "accepted" | "rejected")
                )
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
