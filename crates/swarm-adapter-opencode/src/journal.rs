mod retention;
#[cfg(test)]
mod retention_tests;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
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
/// Per-call body-parse budget for the startup inventory, mirroring the
/// retention 32-step discipline. Legacy convergence enumerates names to
/// find the resume position; steady recovery reads only the durable sparse
/// tail. Bodies beyond the convergence window wait for a later pass, and
/// retention-cursor.json preserves progress across restarts.
pub(super) const MAX_STARTUP_SCAN_ENTRIES: usize = 32;

/// Real counters for startup scanning cost. `operation_bodies_parsed` and
/// `operation_bytes_read` cover inventory parses with direct reads (the
/// recovery window, the retention extension, and the hello operations-dir
/// fallback). Deletion-path re-reads stay bounded separately by the
/// retention step limit and are excluded. `outbox_entries_scanned` covers
/// the pending-bounded hello outbox scan; per-item receipt validation
/// reads behind it are pending-bounded as well and excluded.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct StartupScanCounts {
    pub operations_listed: u64,
    pub operation_bodies_parsed: u64,
    pub operation_bytes_read: u64,
    pub outbox_entries_scanned: u64,
    pub unresolved_found: u64,
}

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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StoreStartupRecovery {
    schema_version: u32,
    binding_id: String,
    generation: i64,
    worker_boot_id: String,
    native_root_id: Option<String>,
    complete: bool,
    unresolved_operations: Vec<StoreUnresolvedOperation>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StoreUnresolvedOperation {
    operation_id: String,
    method: String,
    state: String,
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
    pending_delivery: bool,
    references: Vec<String>,
    native_root_ids: Vec<String>,
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

    let mut native_root_ids = BTreeSet::new();
    if let Some(root) = intent_root {
        native_root_ids.insert(root.to_owned());
    }
    if let Some(root) = outcome_root {
        native_root_ids.insert(root.to_owned());
    }
    for revision in &history.outcome_revisions {
        if revision.outcome["native_scope_key"] == scope
            && let Some(root) = revision.outcome["native_root_id"].as_str()
        {
            native_root_ids.insert(root.to_owned());
        }
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
        pending_delivery,
        references,
        native_root_ids: native_root_ids.into_iter().collect(),
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

fn validate_pending_item_slot(
    outbox: &Path,
    path: &Path,
    item: &PendingItem,
) -> Result<Option<String>> {
    let Some(kind) = matches!(item.kind.as_str(), "outcome" | "result" | "observation")
        .then_some(item.kind.as_str())
    else {
        return Err(Error::new("ADAPTER_OUTBOX", "unsupported pending item"));
    };
    if item.key.trim().is_empty()
        || path != outbox.join(format!("{kind}-{}.json", operation_key(&item.key)))
    {
        return Err(Error::new(
            "ADAPTER_OUTBOX",
            "pending item identity differs from its deterministic slot",
        ));
    }
    let identity_field = if kind == "observation" {
        "event_id"
    } else {
        "operation_id"
    };
    if item.payload[identity_field].as_str() != Some(item.key.as_str()) {
        return Err(Error::new(
            "ADAPTER_OUTBOX",
            "pending item payload identity differs from its deterministic slot",
        ));
    }
    Ok(matches!(kind, "outcome" | "result").then(|| operation_key(&item.key)))
}

fn outbox_slot_identity(outbox: &Path, path: &Path) -> Option<(&'static str, String)> {
    if path.parent() != Some(outbox) {
        return None;
    }
    let name = path.file_name()?.to_str()?;
    let stem = name.strip_suffix(".json")?;
    for kind in ["outcome", "result", "observation"] {
        let Some(key) = stem.strip_prefix(&format!("{kind}-")) else {
            continue;
        };
        if key.len() == 64
            && key
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            && path == outbox.join(format!("{kind}-{key}.json"))
        {
            return Some((kind, key.to_owned()));
        }
    }
    None
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
    scan_bodies: AtomicU64,
    scan_bytes: AtomicU64,
    scan_listed: AtomicU64,
    scan_outbox: AtomicU64,
    scan_unresolved: AtomicU64,
    scan_damaged: AtomicU64,
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
                retention::ADDRESS_CHECKPOINT_DIR,
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
        let address_dir = root.join(retention::ADDRESS_CHECKPOINT_DIR);
        fs::create_dir_all(&operations).map_err(|_| {
            Error::new(
                "ADAPTER_STATE",
                "operation journal directory cannot be created",
            )
        })?;
        fs::create_dir_all(&outbox)
            .map_err(|_| Error::new("ADAPTER_STATE", "outbox directory cannot be created"))?;
        fs::create_dir_all(&address_dir).map_err(|_| {
            Error::new(
                "ADAPTER_STATE",
                "operation address checkpoint cannot be created",
            )
        })?;
        ensure_directory(&operations, "operation journal directory")?;
        ensure_directory(&outbox, "outbox directory")?;
        ensure_directory(&address_dir, "operation address checkpoint")?;
        private_permissions(&operations, true)?;
        private_permissions(&outbox, true)?;
        private_permissions(&address_dir, true)?;
        let checkpoints = root.join("native-checkpoints.jsonl");
        if !checkpoints.exists() {
            write_private_new(&checkpoints, &[])?;
        } else {
            ensure_regular(&checkpoints, "native checkpoint")?;
        }
        let retention = retention::RetentionIndex::open(root, &address_dir, &identity);
        Ok(Self {
            identity,
            operations,
            outbox,
            checkpoints,
            retention: Mutex::new(retention),
            acknowledgements_since_retention: AtomicUsize::new(0),
            scan_bodies: AtomicU64::new(0),
            scan_bytes: AtomicU64::new(0),
            scan_listed: AtomicU64::new(0),
            scan_outbox: AtomicU64::new(0),
            scan_unresolved: AtomicU64::new(0),
            scan_damaged: AtomicU64::new(0),
        })
    }

    pub fn load(&self, operation_id: &str) -> Result<OperationHistory> {
        let path = self.operation_path(operation_id);
        if !path.exists() {
            return Ok(OperationHistory::default());
        }
        self.load_by_path(&path, operation_id)
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
            let value: Value = match serde_json::from_slice(&bytes) {
                Ok(value) => value,
                Err(_) => {
                    // The file remains the evidence. A syntactically damaged
                    // item has no delivery identity we can safely replay.
                    // Keep the retention index closed and continue with the
                    // rest of this bounded delivery batch.
                    self.hold_damaged_record(&path, &bytes)?;
                    continue;
                }
            };
            let item: PendingItem = serde_json::from_value(value)
                .map_err(|_| Error::new("ADAPTER_OUTBOX", "outbox item schema is invalid"))?;
            validate_pending_item_slot(&self.outbox, &path, &item)?;
            items.push((path, item));
        }
        Ok(items)
    }
    /// Snapshot of the real startup scanning cost on this handle.
    pub fn startup_scan_counts(&self) -> StartupScanCounts {
        StartupScanCounts {
            operations_listed: self.scan_listed.load(Ordering::Relaxed),
            operation_bodies_parsed: self.scan_bodies.load(Ordering::Relaxed),
            operation_bytes_read: self.scan_bytes.load(Ordering::Relaxed),
            outbox_entries_scanned: self.scan_outbox.load(Ordering::Relaxed),
            unresolved_found: self.scan_unresolved.load(Ordering::Relaxed),
        }
    }

    /// Number of malformed journal or outbox record encounters held this boot.
    pub fn startup_damaged_record_observations(&self) -> u64 {
        self.scan_damaged.load(Ordering::Relaxed)
    }

    fn hold_damaged_record(&self, path: &Path, bytes: &[u8]) -> Result<()> {
        retention::mark_damaged(self)?;
        self.scan_damaged.fetch_add(1, Ordering::Relaxed);
        let name = path
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("<invalid-name>");
        eprintln!(
            "OpenCode recovery is holding damaged record {name} (sha256={:x})",
            Sha256::digest(bytes)
        );
        Ok(())
    }

    fn pending_file_is_malformed(&self, path: &Path) -> Result<bool> {
        match fs::symlink_metadata(path) {
            Ok(_) => ensure_regular(path, "outbox item")?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(_) => {
                return Err(Error::new(
                    "ADAPTER_STATE",
                    "outbox item cannot be inspected",
                ));
            }
        }
        let bytes = read_limited(path, RECORD_LIMIT)?;
        let value: Value = match serde_json::from_slice(&bytes) {
            Ok(value) => value,
            Err(_) => return Ok(true),
        };
        let _: PendingItem = serde_json::from_value(value)
            .map_err(|_| Error::new("ADAPTER_OUTBOX", "outbox item schema is invalid"))?;
        Ok(false)
    }

    /// True once one accumulation cycle has classified every listed file.
    /// A false value only defers deletion and boot reporting; files stay
    /// retained and the persisted cursor resumes the tail on the next boot.
    pub fn startup_recovery_complete(&self) -> bool {
        retention::scan_incomplete(self)
            .map(|incomplete| !incomplete)
            .unwrap_or(false)
    }

    /// Whether startup needs a legacy name census before it can serve the
    /// retained sparse closure. This is a metadata-only decision.
    pub fn startup_inventory_required(&self) -> Result<bool> {
        retention::startup_inventory_required(self)
    }

    /// Use only the already-durable sparse closure before hello. A cold or
    /// invalidated legacy inventory is deliberately left for bounded recovery
    /// after hello, so discovering old names cannot delay the handshake.
    #[cfg(test)]
    pub fn recover_outbox_before_hello(&self) -> Result<Vec<OperationRecoveryState>> {
        if !retention::durable_startup_complete(self)?
            || retention::legacy_migration_version(self)? < 1
            || retention::legacy_reclamation_pending(self)?
        {
            return Ok(Vec::new());
        }
        self.recover_outbox()
    }

    /// Enumerate legacy operation names without classifying their bodies.
    /// Owned startup runs this on a blocking worker so a cold directory cannot
    /// hold the module service loop before or between `module.next` polls.
    pub fn prepare_startup_inventory(&self) -> Result<()> {
        if !retention::startup_inventory_required(self)? {
            return Ok(());
        }
        if retention::has_cached_startup_names(self)? {
            return Ok(());
        }
        let (names, invalid) = self.list_operation_names()?;
        self.scan_listed
            .fetch_add(names.len() as u64 + invalid as u64, Ordering::Relaxed);
        retention::remember_startup_names(self, names, invalid)
    }

    /// Sorted valid operation keys plus a count of unnameable entries.
    /// Name enumeration is directory metadata only; bodies are parsed
    /// solely inside the bounded window selected from these names.
    fn list_operation_names(&self) -> Result<(Vec<String>, usize)> {
        let entries = fs::read_dir(&self.operations)
            .map_err(|_| Error::new("ADAPTER_JOURNAL", "operation directory cannot be read"))?;
        let mut names = Vec::new();
        let mut invalid = 0usize;
        for entry in entries {
            let entry = entry
                .map_err(|_| Error::new("ADAPTER_JOURNAL", "operation entry cannot be read"))?;
            match entry.path().file_stem().and_then(|value| value.to_str()) {
                Some(name) => names.push(name.to_owned()),
                None => invalid += 1,
            }
        }
        names.sort();
        Ok((names, invalid))
    }

    /// Targeted steady window over the durable closure: existence-checked
    /// keys only, with no directory enumeration. Missing keys with pending
    /// outbox slots hold the scan; the rest are stale phantoms dropped
    /// with a report.
    fn steady_recovery_window(&self) -> Result<Vec<String>> {
        let mut window = Vec::new();
        for key in retention::steady_window_keys(self, MAX_STARTUP_SCAN_ENTRIES)? {
            retention::mark_steady_visited(self, &key)?;
            let path = self.operations.join(format!("{key}.jsonl"));
            match fs::symlink_metadata(&path) {
                Ok(_) => window.push(key),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    let mut pending = false;
                    for kind in ["outcome", "result"] {
                        match fs::symlink_metadata(self.outbox.join(format!("{kind}-{key}.json"))) {
                            Ok(_) => {
                                pending = true;
                                break;
                            }
                            Err(nested) if nested.kind() == std::io::ErrorKind::NotFound => {}
                            Err(_) => {
                                return Err(Error::new(
                                    "ADAPTER_OUTBOX",
                                    "pending operation delivery cannot be inspected",
                                ));
                            }
                        }
                    }
                    if pending {
                        retention::mark_damaged(self)?;
                    } else {
                        retention::drop_durable_key(self, &key)?;
                        eprintln!(
                            "OpenCode startup inventory dropped a missing operation key with no pending delivery: {key}"
                        );
                    }
                }
                Err(_) => {
                    return Err(Error::new(
                        "ADAPTER_STATE",
                        "operation journal is unavailable",
                    ));
                }
            }
        }
        Ok(window)
    }

    pub fn recover_outbox(&self) -> Result<Vec<OperationRecoveryState>> {
        self.recover_outbox_selection(None, None, None, &BTreeSet::new())
    }

    /// The authenticated Store hello owns the current unresolved set. Read
    /// only those journals, pending deliveries and their exact references;
    /// legacy classification remains separate and cannot gain deletion
    /// authority from an operation's absence in this projection.
    pub fn recover_store_startup(
        &self,
        projection: &Value,
        boot_id: &str,
        root: Option<&str>,
    ) -> Result<Vec<OperationRecoveryState>> {
        let manifest: StoreStartupRecovery =
            serde_json::from_value(projection.clone()).map_err(|_| {
                Error::new(
                    "HOST_STARTUP_RECOVERY",
                    "startup recovery projection is invalid",
                )
            })?;
        if manifest.schema_version != 1
            || manifest.binding_id != self.identity.binding_id
            || manifest.generation != self.identity.generation
            || manifest.worker_boot_id != boot_id
            || manifest.native_root_id.as_deref() != root
            || !manifest.complete
            || manifest.unresolved_operations.len() > 4096
            || root.is_some_and(|value| {
                value != expected_root_id(&self.identity.binding_id, self.identity.generation)
            })
        {
            return Err(Error::new(
                "HOST_STARTUP_RECOVERY",
                "startup recovery projection is incomplete or belongs to another owner",
            ));
        }
        let mut keys = BTreeSet::new();
        let mut expected_operation_ids = BTreeMap::new();
        for operation in &manifest.unresolved_operations {
            let key = operation_key(&operation.operation_id);
            if operation.operation_id.is_empty()
                || operation.method.is_empty()
                || !matches!(
                    operation.state.as_str(),
                    "sending" | "native_accepted" | "outcome_unknown"
                )
                || !keys.insert(key.clone())
            {
                return Err(Error::new(
                    "HOST_STARTUP_RECOVERY",
                    "unresolved operation identity is invalid",
                ));
            }
            expected_operation_ids.insert(key, operation.operation_id.clone());
        }
        let manifest_keys = keys.clone();
        let mut damaged_outbox_paths = BTreeSet::new();
        // A Store-applied delivery can still lack its local ACK. Its outbox
        // receipt, rather than a settled-history scan, supplies that address.
        for entry in fs::read_dir(&self.outbox)
            .map_err(|_| Error::new("ADAPTER_OUTBOX", "outbox cannot be read"))?
        {
            let path = entry
                .map_err(|_| Error::new("ADAPTER_OUTBOX", "outbox entry cannot be read"))?
                .path();
            ensure_regular(&path, "outbox item")?;
            self.scan_outbox.fetch_add(1, Ordering::Relaxed);
            let bytes = read_limited(&path, RECORD_LIMIT)?;
            let value: Value = match serde_json::from_slice(&bytes) {
                Ok(value) => value,
                Err(_) => {
                    let Some((kind, key)) = outbox_slot_identity(&self.outbox, &path) else {
                        return Err(Error::new("ADAPTER_OUTBOX", "outbox item is invalid"));
                    };
                    if kind == "observation" {
                        self.hold_damaged_record(&path, &bytes)?;
                        continue;
                    }
                    if !matches!(kind, "outcome" | "result") || !manifest_keys.contains(&key) {
                        return Err(Error::new("ADAPTER_OUTBOX", "outbox item is invalid"));
                    }
                    self.hold_damaged_record(&path, &bytes)?;
                    damaged_outbox_paths.insert(path);
                    keys.insert(key);
                    continue;
                }
            };
            let item: PendingItem = serde_json::from_value(value)
                .map_err(|_| Error::new("ADAPTER_OUTBOX", "outbox item schema is invalid"))?;
            if let Some(key) = validate_pending_item_slot(&self.outbox, &path, &item)? {
                if let Some(expected) = expected_operation_ids.get(&key)
                    && expected != &item.key
                {
                    return Err(Error::new(
                        "ADAPTER_OUTBOX",
                        "pending item identity conflicts with the selected Store projection",
                    ));
                }
                expected_operation_ids
                    .entry(key.clone())
                    .or_insert_with(|| item.key.clone());
                keys.insert(key);
            }
        }
        let mut seen = BTreeSet::new();
        let mut recovered = Vec::new();
        while !keys.is_empty() {
            let mut window = Vec::new();
            for key in std::mem::take(&mut keys) {
                if !seen.insert(key.clone()) {
                    continue;
                }
                let path = self.operations.join(format!("{key}.jsonl"));
                match fs::symlink_metadata(&path) {
                    Ok(_) => window.push(key),
                    // Sending may have committed before any adapter intent.
                    // A missing local file is not a receipt permitting replay.
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(_) => {
                        return Err(Error::new(
                            "ADAPTER_STATE",
                            "selected operation journal cannot be inspected",
                        ));
                    }
                }
            }
            recovered.extend(self.recover_outbox_selection(
                Some(window),
                Some(&mut keys),
                Some(&mut expected_operation_ids),
                &damaged_outbox_paths,
            )?);
            keys.retain(|key| !seen.contains(key));
        }
        Ok(recovered)
    }

    fn recover_outbox_selection(
        &self,
        selected: Option<Vec<String>>,
        mut references: Option<&mut BTreeSet<String>>,
        mut expected_operation_ids: Option<&mut BTreeMap<String, String>>,
        damaged_outbox_paths: &BTreeSet<PathBuf>,
    ) -> Result<Vec<OperationRecoveryState>> {
        let store_selected = selected.is_some();
        let mut held_outbox_paths = damaged_outbox_paths.clone();
        if !store_selected {
            retention::begin_recovery(self)?;
        }
        let mut recovery_states = Vec::new();
        // Steady state serves the durable closure with targeted reads only:
        // no directory enumeration once one legacy convergence completed.
        // The windowed listing below runs solely while converging history.
        let window: Vec<String>;
        let mut legacy_inventory = false;
        if let Some(selected) = selected {
            window = selected;
        } else if !retention::startup_inventory_required(self)? {
            window = self.steady_recovery_window()?;
        } else {
            if !retention::has_cached_startup_names(self)? {
                let (names, invalid) = self.list_operation_names()?;
                self.scan_listed
                    .fetch_add(names.len() as u64 + invalid as u64, Ordering::Relaxed);
                retention::remember_startup_names(self, names, invalid)?;
            }
            let (selected, invalid) = retention::startup_window(self, MAX_STARTUP_SCAN_ENTRIES)?;
            if invalid > 0 {
                retention::mark_damaged(self)?;
            }
            legacy_inventory = true;
            window = selected;
        }
        for name in &window {
            let name = name.as_str();
            if legacy_inventory {
                retention::mark_startup_classified(self, name)?;
            }
            let path = self.operations.join(format!("{name}.jsonl"));
            ensure_regular(&path, "operation journal")?;
            let observed_bytes = read_limited(&path, HISTORY_LIMIT)?;
            self.scan_bodies.fetch_add(1, Ordering::Relaxed);
            self.scan_bytes
                .fetch_add(observed_bytes.len() as u64, Ordering::Relaxed);
            let expected_operation_id = expected_operation_ids
                .as_deref()
                .and_then(|identities| identities.get(name))
                .map(String::as_str);
            let history = match read_recovery_operation_history_bytes(
                &observed_bytes,
                expected_operation_id,
            )? {
                RecoveryHistoryRead::Valid(history) => history,
                RecoveryHistoryRead::Damaged(partial_history) => {
                    let known_operation_id = partial_history
                        .intent
                        .as_ref()
                        .map(|intent| intent.operation_id.as_str())
                        .or_else(|| {
                            partial_history
                                .outcome
                                .as_ref()
                                .and_then(|outcome| outcome["operation_id"].as_str())
                        })
                        .or_else(|| {
                            partial_history
                                .result_params
                                .as_ref()
                                .and_then(|params| params["operation_id"].as_str())
                        });
                    if known_operation_id.is_some() {
                        let partial = summarize_retained_operation(
                            path.clone(),
                            &observed_bytes,
                            &partial_history,
                            &self.identity.binding_id,
                            self.identity.generation,
                            &self.identity.native_scope_key,
                            true,
                        )?;
                        self.check_native_root_identity(&partial)?;
                    }
                    self.hold_damaged_record(&path, &observed_bytes)?;
                    continue;
                }
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
                Some(operation_id) => self.operation_has_pending_delivery(operation_id)?,
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
                    // Convergence-time conflict vigilance: a conflicting root
                    // fails the window now instead of hiding until a later
                    // full scan that steady state never performs.
                    self.check_native_root_identity(&operation)?;
                    if let Some(identities) = expected_operation_ids.as_deref_mut() {
                        remember_expected_operation_id(identities, &operation.operation_id)?;
                        for reference in &operation.references {
                            remember_expected_operation_id(identities, reference)?;
                        }
                    }
                    if let Some(references) = references.as_deref_mut() {
                        references.extend(operation.references.iter().map(|id| operation_key(id)));
                    }
                    retention::register_operation(self, operation)?;
                    if legacy_inventory {
                        retention::mark_steady_visited(self, name)?;
                    }
                }
                Err(error) => {
                    retention::mark_damaged(self)?;
                    if store_selected {
                        return Err(error);
                    }
                }
            }
            if let (Some(outcome), Some(digest)) =
                (history.outcome.as_ref(), history.outcome_sha256.as_ref())
            {
                let Some(operation_id) = outcome["operation_id"].as_str() else {
                    retention::mark_damaged(self)?;
                    continue;
                };
                let expected = operation_key(operation_id);
                if name != expected {
                    retention::mark_damaged(self)?;
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
                    let held = if held_outbox_paths.contains(&pending_path) {
                        true
                    } else if self.pending_file_is_malformed(&pending_path)? {
                        let bytes = read_limited(&pending_path, RECORD_LIMIT)?;
                        self.hold_damaged_record(&pending_path, &bytes)?;
                        held_outbox_paths.insert(pending_path.clone());
                        true
                    } else {
                        false
                    };
                    if !held {
                        self.write_pending(PendingItem {
                            kind: "outcome".into(),
                            key: operation_id.into(),
                            payload: outcome.clone(),
                        })?;
                    }
                }
                match fs::symlink_metadata(&pending_path) {
                    Ok(_) => {
                        if !held_outbox_paths.contains(&pending_path)
                            && self.pending_file_is_malformed(&pending_path)?
                        {
                            let bytes = read_limited(&pending_path, RECORD_LIMIT)?;
                            self.hold_damaged_record(&pending_path, &bytes)?;
                            held_outbox_paths.insert(pending_path.clone());
                        }
                        if !held_outbox_paths.contains(&pending_path) {
                            ensure_regular(&pending_path, "outbox item")?;
                            let bytes = read_limited(&pending_path, RECORD_LIMIT)?;
                            let pending: PendingItem =
                                serde_json::from_slice(&bytes).map_err(|_| {
                                    Error::new("ADAPTER_OUTBOX", "outbox item is invalid")
                                })?;
                            validate_pending_item_slot(&self.outbox, &pending_path, &pending)?;
                            if history.torn_tail_evidence.is_some() {
                                // Keep the outbox receipt available for the
                                // exact torn-tail repair performed immediately
                                // before delivery. The repair re-reads this
                                // payload and its digest against the durable
                                // prefix; recovery must not turn that verified
                                // deferred path into a native replay.
                                retention::mark_damaged(self)?;
                            } else {
                                self.validate_pending_outcome(&pending_path, &pending)?;
                            }
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(_) => {
                        return Err(Error::new(
                            "ADAPTER_STATE",
                            "pending outcome cannot be inspected",
                        ));
                    }
                }
            }
            if let (Some(params), Some(digest)) = (
                history.result_params.as_ref(),
                history.result_sha256.as_ref(),
            ) && history.result_acknowledged_sha256.as_deref() != Some(digest.as_str())
            {
                let Some(operation_id) = params["operation_id"].as_str() else {
                    retention::mark_damaged(self)?;
                    continue;
                };
                if name != operation_key(operation_id) {
                    retention::mark_damaged(self)?;
                    continue;
                }
                let pending_path = self
                    .outbox
                    .join(format!("result-{}.json", operation_key(operation_id)));
                if !held_outbox_paths.contains(&pending_path)
                    && self.pending_file_is_malformed(&pending_path)?
                {
                    let bytes = read_limited(&pending_path, RECORD_LIMIT)?;
                    self.hold_damaged_record(&pending_path, &bytes)?;
                    held_outbox_paths.insert(pending_path.clone());
                }
                if !held_outbox_paths.contains(&pending_path) {
                    self.write_pending(PendingItem {
                        kind: "result".into(),
                        key: operation_id.into(),
                        payload: params.clone(),
                    })?;
                }
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
                    retention::mark_damaged(self)?;
                    continue;
                }
                recovery_states.push(OperationRecoveryState {
                    operation_key: name.to_owned(),
                    operation_id,
                    history,
                });
                self.scan_unresolved.fetch_add(1, Ordering::Relaxed);
            }
        }
        if store_selected {
            // Do not mark the retention census complete: unknown/orphan and
            // settled result-source files require their own local proof.
            return Ok(recovery_states);
        }
        if legacy_inventory {
            // One durable cursor write per bounded pass, not per file: a crash
            // with an older position only revisits checked entries.
            let covered = retention::scan_covered(self)?;
            retention::store_startup_progress(
                self,
                if covered {
                    None
                } else {
                    window.last().cloned()
                },
                covered,
                true,
            )?;
        } else {
            // Steady pass over the durable closure: stability means every
            // currently-durable key was visited now; mid-pass additions
            // wait for the next pass. No enumeration happened above.
            let stable = retention::steady_coverage_complete(self)?;
            retention::store_startup_progress(self, None, stable, false)?;
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
        // The checkpoint is only a candidate. Every live per-operation
        // address memo is checked even when the root checkpoint exists.
        let expected = expected_root_id(binding_id, generation);
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
            self.scan_outbox.fetch_add(1, Ordering::Relaxed);
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
        for memo in retention::operation_address_memos(self)? {
            for root in memo.native_root_ids {
                if root != expected {
                    return Err(Error::new(
                        "NATIVE_IDENTITY_MISMATCH",
                        "operation address memo conflicts with the deterministic binding root",
                    ));
                }
                remember_one_root(&mut root_checkpoint, root)?;
            }
        }
        // A checkpoint cannot hide an unscanned legacy tail or a damaged
        // address memo. Durable closure completeness (persisted converged
        // addresses) plus the outbox/memo conflict checks above own coverage;
        // the caller still converges bounded recovery windows first, but a
        // fresh handle need not repeat an in-memory pass when durable state
        // is already complete.
        if !retention::durable_startup_complete(self)?
            || retention::startup_has_unknown_addresses(self)?
        {
            return Err(Error::new(
                "ADAPTER_STARTUP_RECOVERY_INCOMPLETE",
                "native root cannot be confirmed before address coverage completes",
            ));
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
        // Project the operation's own address and all known outgoing targets
        // before appending the journal frame. The memo is a bounded address
        // hint only; the operation history remains authoritative.
        let mut references = BTreeSet::new();
        let mut native_root_ids = BTreeSet::new();
        if let Some(intent) = history.intent.as_ref() {
            collect_intent_references(intent, &mut references);
            if let Some(root) = intent.native_root_id.as_ref() {
                native_root_ids.insert(root.clone());
            }
        }
        if let Some(outcome) = history.outcome.as_ref() {
            collect_outcome_references(outcome, &mut references);
            if let Some(root) = outcome["native_root_id"].as_str() {
                native_root_ids.insert(root.to_owned());
            }
        }
        for revision in &history.outcome_revisions {
            collect_outcome_references(&revision.outcome, &mut references);
            if let Some(root) = revision.outcome["native_root_id"].as_str() {
                native_root_ids.insert(root.to_owned());
            }
        }
        if let Some(intent) = record.intent.as_ref() {
            collect_intent_references(intent, &mut references);
            if let Some(root) = intent.native_root_id.as_ref() {
                native_root_ids.insert(root.clone());
            }
        }
        if let Some(outcome) = record.outcome.as_ref() {
            collect_outcome_references(outcome, &mut references);
            if let Some(root) = outcome["native_root_id"].as_str() {
                native_root_ids.insert(root.to_owned());
            }
        }
        let key = operation_key(operation_id);
        retention::note_operation_append(
            self,
            retention::OperationAddressMemo::new(
                &self.identity,
                operation_id.to_owned(),
                key,
                references.into_iter().collect(),
                native_root_ids.into_iter().collect(),
            ),
        )?;
        append_bytes(&path, &bytes)?;
        retention::refresh_operation(self, operation_id);
        Ok(())
    }
    /// Fail closed on a native root that conflicts with the deterministic
    /// binding identity. Scope-gated exactly like the hello check: foreign
    /// scopes stay ignored here as they are there.
    fn check_native_root_identity(&self, retained: &RetainedOperation) -> Result<()> {
        if retained.native_scope_key.as_deref() == Some(self.identity.native_scope_key.as_str())
            && retained.native_root_ids.iter().any(|root| {
                root != &expected_root_id(&self.identity.binding_id, self.identity.generation)
            })
        {
            return Err(Error::new(
                "NATIVE_IDENTITY_MISMATCH",
                "operation native root conflicts with the deterministic binding identity",
            ));
        }
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

    fn load_by_path(&self, path: &Path, expected_operation_id: &str) -> Result<OperationHistory> {
        read_operation_history(path, Some(expected_operation_id))
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
                let exact_reply_readback = target_intent.method == "agent.reply"
                    && target_intent.native_input_id.is_none()
                    && target_intent.prompt_sha256.is_none()
                    && target_intent.prompt_bytes.is_none()
                    && outcome.native_input_id.is_none()
                    && outcome.details["completion_condition"] == "native_reply_acknowledged"
                    && outcome.details["evidence"] == "complete_contiguous_native_reply_history"
                    && exact_reply_readback_identity(target_intent, outcome);
                let exact_non_reply_readback = target_intent.method != "agent.reply"
                    && (exact_input_readback || exact_session_readback);
                if !matches!(
                    target_intent.method.as_str(),
                    "agent.open"
                        | "task.dispatch"
                        | "agent.send"
                        | "native.opencode.loop_step"
                        | "agent.reply"
                ) || !(exact_non_reply_readback || exact_reply_readback)
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

fn collect_intent_references(intent: &OperationIntent, references: &mut BTreeSet<String>) {
    if let Some(target) = intent.reconcile_target_operation_id.as_ref() {
        references.insert(target.clone());
    }
    if let Some(result) = intent.result_input_status.as_ref() {
        references.insert(result.input_operation_id.clone());
    }
    if let Some(result) = intent.result_assistant.as_ref() {
        references.insert(result.input_operation_id.clone());
    }
}

fn collect_outcome_references(outcome: &Value, references: &mut BTreeSet<String>) {
    if let Some(operation_id) = outcome["details"]["reconcile_operation_id"].as_str() {
        references.insert(operation_id.to_owned());
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

enum RecoveryHistoryRead {
    Valid(OperationHistory),
    Damaged(OperationHistory),
}

/// Parse selected evidence without turning damaged bytes into invented
/// history. Only framing or invalid JSON syntax is held; valid JSON with an
/// invalid schema, identity, or semantics remains an error and fails closed.
fn read_recovery_operation_history_bytes(
    bytes: &[u8],
    expected_operation_id: Option<&str>,
) -> Result<RecoveryHistoryRead> {
    let mut history = OperationHistory::default();
    let mut observed_operation_id: Option<String> = None;
    let mut malformed_record = false;
    let mut validation_error = None;
    let verdict = scan_jsonl(bytes, RECORD_LIMIT, |_, frame| {
        let value: Value = match serde_json::from_slice(&frame[..frame.len() - 1]) {
            Ok(value) => value,
            Err(_) => {
                malformed_record = true;
                return Ok(());
            }
        };
        let record: JournalRecord = match serde_json::from_value(value) {
            Ok(record) => record,
            Err(_) => {
                let error = Error::new("ADAPTER_JOURNAL", "operation journal schema is invalid");
                validation_error = Some(error.clone());
                return Err(error);
            }
        };
        match apply_operation_record(
            &mut history,
            &mut observed_operation_id,
            expected_operation_id,
            record,
        ) {
            Ok(()) => Ok(()),
            Err(error) => {
                validation_error = Some(error.clone());
                Err(error)
            }
        }
    })?;

    if let Some(error) = validation_error {
        return Err(error);
    }

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
    if malformed_record {
        return Ok(RecoveryHistoryRead::Damaged(history));
    }
    Ok(RecoveryHistoryRead::Valid(history))
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

fn exact_reply_readback_identity(
    target_intent: &OperationIntent,
    outcome: &RuntimeOutcome,
) -> bool {
    const IDENTITY_FIELDS: [&str; 8] = [
        "kind",
        "action",
        "session_id",
        "request_id",
        "request_fingerprint",
        "affected_request_ids",
        "effect_payload_sha256",
        "feedback_present",
    ];

    let Some(saved) = target_intent
        .marker
        .get("native_opencode_reply")
        .filter(|value| value.is_object())
    else {
        return false;
    };
    if outcome.details.get("reply_readback_identity") != Some(saved) {
        return false;
    }
    let Some(fields) = saved.as_object() else {
        return false;
    };
    if fields.len() != IDENTITY_FIELDS.len()
        || IDENTITY_FIELDS
            .iter()
            .any(|field| !fields.contains_key(*field))
    {
        return false;
    }

    let Some(kind) = fields.get("kind").and_then(Value::as_str) else {
        return false;
    };
    let Some(action) = fields.get("action").and_then(Value::as_str) else {
        return false;
    };
    let Some(session_id) = fields.get("session_id").and_then(Value::as_str) else {
        return false;
    };
    let Some(request_id) = fields.get("request_id").and_then(Value::as_str) else {
        return false;
    };
    let Some(request_fingerprint) = fields.get("request_fingerprint").and_then(Value::as_str)
    else {
        return false;
    };
    let Some(affected) = fields.get("affected_request_ids").and_then(Value::as_array) else {
        return false;
    };
    if fields.get("feedback_present") != Some(&Value::Bool(false))
        || target_intent.native_root_id.as_deref() != Some(session_id)
        || outcome.native_root_id.as_deref() != Some(session_id)
        || !valid_reply_readback_id(session_id, "ses_")
        || !valid_reply_readback_sha256(request_fingerprint)
    {
        return false;
    }
    let prefix = match kind {
        "question" => "que_",
        "permission" => "per_",
        _ => return false,
    };
    if !valid_reply_readback_id(request_id, prefix) || affected.is_empty() || affected.len() > 64 {
        return false;
    }
    let mut affected_ids = BTreeSet::new();
    for value in affected {
        let Some(id) = value.as_str() else {
            return false;
        };
        if !valid_reply_readback_id(id, prefix) || !affected_ids.insert(id) {
            return false;
        }
    }
    if !affected_ids.contains(request_id) {
        return false;
    }

    let effect_digest = match fields.get("effect_payload_sha256") {
        Some(Value::String(value))
            if value
                .strip_prefix("sha256:")
                .is_some_and(valid_reply_readback_sha256) =>
        {
            Some(value.as_str())
        }
        Some(Value::Null) => None,
        _ => return false,
    };
    let only_target = affected.len() == 1 && affected_ids.contains(request_id);
    match (kind, action) {
        ("question", "reply") | ("permission", "once") => only_target && effect_digest.is_some(),
        ("question", "reject") => only_target && effect_digest.is_none(),
        ("permission", "reject") => {
            let expected = digest_json(&serde_json::json!({"reply":"reject"})).ok();
            expected.as_deref() == effect_digest
        }
        _ => false,
    }
}

fn valid_reply_readback_id(value: &str, prefix: &str) -> bool {
    value.starts_with(prefix)
        && value.len() > prefix.len()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_-".contains(&byte))
}

fn valid_reply_readback_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
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

fn remember_expected_operation_id(
    expected: &mut BTreeMap<String, String>,
    operation_id: &str,
) -> Result<()> {
    let key = operation_key(operation_id);
    if expected
        .get(&key)
        .is_some_and(|previous| previous != operation_id)
    {
        return Err(Error::new(
            "ADAPTER_JOURNAL",
            "operation key resolves to conflicting identities",
        ));
    }
    expected
        .entry(key)
        .or_insert_with(|| operation_id.to_owned());
    Ok(())
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
    const PARITY_ROUTE: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

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
        Journal::open(
            root,
            PARITY_BINDING,
            PARITY_GENERATION,
            PARITY_SCOPE,
            PARITY_ROUTE,
        )
        .unwrap()
    }

    fn fresh_root(prefix: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "swarm-opencode-journal65-{}-{}",
            prefix,
            uuid::Uuid::new_v4()
        ))
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
        let via_path = journal.load_by_path(&path, operation_id).unwrap();
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
        let via_path = journal.load_by_path(&path, operation_id).unwrap();
        let load_tail = via_load
            .torn_tail_evidence
            .clone()
            .expect("load reports torn tail");
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
        let path_err = journal.load_by_path(&path, operation_id).unwrap_err();
        assert_eq!(load_err.code, "ADAPTER_JOURNAL");
        assert_eq!(path_err.code, "ADAPTER_JOURNAL");
        assert_eq!(load_err.message, path_err.message);
        assert!(load_err.message.contains("damaged at byte"));
        assert_eq!(std::fs::read(&path).unwrap(), damaged);

        let newline = complete_bytes
            .iter()
            .position(|byte| *byte == b'\n')
            .expect("complete intent line");
        let mut mismatched_record: Value =
            serde_json::from_slice(&complete_bytes[..newline]).unwrap();
        mismatched_record["operation_id"] = json!("different-operation");
        let mut mismatched_identity = serde_json::to_vec(&mismatched_record).unwrap();
        mismatched_identity.push(b'\n');
        mismatched_identity.extend_from_slice(&complete_bytes[newline + 1..]);
        std::fs::write(&path, &mismatched_identity).unwrap();
        let load_err = journal.load(operation_id).unwrap_err();
        let path_err = journal.load_by_path(&path, operation_id).unwrap_err();
        assert_eq!(load_err.code, "ADAPTER_JOURNAL");
        assert_eq!(path_err.code, "ADAPTER_JOURNAL");
        assert_eq!(load_err.message, path_err.message);
        assert_eq!(std::fs::read(&path).unwrap(), mismatched_identity);

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

        let err = journal.repair_torn_outcome_ack(&outcome).unwrap_err();
        assert_eq!(err.code, "ADAPTER_JOURNAL_RECOVERY_UNKNOWN");
        assert_eq!(std::fs::read(&path).unwrap(), changed_bytes);

        let rescan = journal.load(operation_id).unwrap_err();
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
        let path = journal.operation_path(operation_id);

        let mut oversized_complete = vec![b'x'; RECORD_LIMIT];
        oversized_complete.push(b'\n');
        std::fs::write(&path, &oversized_complete).unwrap();
        let complete_by_id = journal.load(operation_id).unwrap_err();
        let complete_by_path = journal.load_by_path(&path, operation_id).unwrap_err();
        let expected_complete = format!(
            "operation journal damaged at byte 0: OversizedCompleteRecord {{ record_bytes: {}, max_record_bytes: {} }}",
            RECORD_LIMIT + 1,
            RECORD_LIMIT
        );
        assert_eq!(complete_by_id.code, "ADAPTER_JOURNAL");
        assert_eq!(complete_by_id.message, expected_complete);
        assert_eq!(complete_by_path.code, "ADAPTER_JOURNAL");
        assert_eq!(complete_by_path.message, expected_complete);
        assert_eq!(std::fs::read(&path).unwrap(), oversized_complete);

        let oversized_torn = vec![b'x'; RECORD_LIMIT + 1];
        std::fs::write(&path, &oversized_torn).unwrap();
        let torn_by_id = journal.load(operation_id).unwrap_err();
        let torn_by_path = journal.load_by_path(&path, operation_id).unwrap_err();
        let expected_torn = format!(
            "operation journal damaged at byte 0: OversizedTornTail {{ tail_bytes: {}, max_record_bytes: {} }}",
            RECORD_LIMIT + 1,
            RECORD_LIMIT
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
    fn d_zero_byte_journal_is_complete_empty_history() {
        let root = fresh_root("zero-byte");
        let journal = open_parity_journal(&root);
        let operation_id = "parity-zero-byte-operation";
        let path = journal.operation_path(operation_id);
        std::fs::write(&path, b"").unwrap();

        let by_id = journal.load(operation_id).unwrap();
        let by_path = journal.load_by_path(&path, operation_id).unwrap();
        assert_eq!(format!("{:?}", by_id), format!("{:?}", by_path));
        assert!(by_id.native_replay_permitted());
        assert!(by_path.native_replay_permitted());
        assert!(by_id.torn_tail_evidence.is_none());
        assert!(by_path.torn_tail_evidence.is_none());
        assert_eq!(std::fs::read(&path).unwrap(), b"");

        drop(journal);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn g_pending_operation_and_outbox_root_is_only_a_hello_candidate() {
        let root = fresh_root("hello-root-outbox");
        let journal = open_parity_journal(&root);
        let operation_id = "parity-hello-root-candidate";
        let root_id = expected_root_id(PARITY_BINDING, PARITY_GENERATION);
        let mut intent = parity_intent(operation_id);
        intent.native_root_id = Some(root_id.clone());
        journal.write_intent(&intent).unwrap();
        let outcome = json!({
            "operation_id": operation_id,
            "outcome": "unknown",
            "native_scope_key": PARITY_SCOPE,
            "native_root_id": root_id.clone()
        });
        journal.queue_outcome_value(&outcome).unwrap();
        journal.recover_outbox().unwrap();
        assert!(journal.startup_recovery_complete());

        let operation_path = journal.operation_path(operation_id);
        let outbox_path = root
            .join("outbox")
            .join(format!("outcome-{}.json", operation_key(operation_id)));
        assert!(operation_path.exists());
        assert!(outbox_path.exists());
        assert_eq!(
            journal
                .native_root_for_hello(PARITY_BINDING, PARITY_GENERATION, PARITY_SCOPE)
                .unwrap()
                .as_deref(),
            Some(root_id.as_str())
        );
        assert_eq!(
            journal
                .native_root_checkpoint_for_hello(PARITY_BINDING, PARITY_GENERATION, PARITY_SCOPE)
                .unwrap(),
            None
        );
        assert_eq!(
            std::fs::read(root.join("native-checkpoints.jsonl")).unwrap(),
            b""
        );

        drop(journal);
        std::fs::remove_dir_all(root).unwrap();
    }
}
