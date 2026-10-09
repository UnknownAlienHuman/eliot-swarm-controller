//! Standalone, attach-only Codex adapter. This package deliberately depends
//! only on the shared host client/contracts and its native JSON-RPC transport.

mod module_contract;
mod native_usage;

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    env,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    task::{Poll, Waker},
    time::Duration,
};

use base64::Engine as _;
use futures_util::{
    SinkExt, StreamExt,
    future::poll_fn,
    lock::Mutex as AsyncMutex,
    stream::{SplitSink, SplitStream},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use swarm_client::{IpcConfig, ModuleLink};
use swarm_contracts::{
    module_contract::ModuleContractClaim,
    runtime::{
        EffectOutcome, GoalContinuationAdmissionContext, GoalContinuationAdmissionReceipt,
        GoalTerminalEventRef, ModuleReceiptIdentity, NormalizedResultOriginContext,
        NormalizedResultPageSource, RuntimeCommand, RuntimeOutcome, TaskDispatchAdmissionReceipt,
        TaskDispatchContext,
    },
    task_prompt::TaskPromptEnvelopeV1,
};
use tokio::{net::TcpStream, time::timeout};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::{Message, client::IntoClientRequest, http::header::AUTHORIZATION},
};
use url::Url;
use uuid::Uuid;

pub const ARTIFACT_ID: &str = "codex-rust-controller.1";
pub const ARTIFACT_VERSION: &str = "5";
pub const MODULE_ID: &str = "codex";
const MAX_FRAME_BYTES: usize = 1_048_576;
const MAX_HISTORY_PAGES: usize = 100;
const PAGE_SIZE: u64 = 100;
const MAX_NORMALIZED_RESULT_PAGE_BYTES: usize = 24 * 1024;
const MAX_NORMALIZED_RESULT_BODY_BYTES: usize = 4 * 1024 * 1024;
const MAX_CHECKPOINT_BYTES: u64 = 4 * 1024 * 1024;
const MAX_LIVE_OPERATION_COUNT: usize = 256;
const MAX_LIVE_OPERATION_BYTES: usize = 2 * 1024 * 1024;
const MAX_LIVE_OPERATION_RECORD_BYTES: usize = 64 * 1024;
const MAX_OPERATION_ID_BYTES: usize = 512;
const MAX_LIVE_OPERATION_SLOT_BYTES: usize =
    MAX_LIVE_OPERATION_RECORD_BYTES + 2 * MAX_OPERATION_ID_BYTES;
const OPERATION_ACK_DIRECTORY: &str = "operation-acks";
const JOURNAL_MARKER_FILE: &str = "journal.active";
const JOURNAL_MARKER_BYTES: &[u8] = b"eliot-codex-journal-v1\n";
const MAX_OPERATION_ACK_BYTES: u64 = 64 * 1024;
const MAX_PENDING_OUTCOMES_PER_BATCH: usize = 32;
const MAX_PENDING_NATIVE_EVENT_BYTES: u64 = 1024 * 1024;
const MAX_PENDING_NATIVE_EVENT_ITEMS: usize = 1024;
const MAX_NATIVE_EVENT_BYTES: usize = 1024 * 1024;
const MAX_NATIVE_EVENT_ITEMS: usize = 1024;
const NATIVE_EVENT_FAULT_RESERVE_BYTES: usize = 512;
const MAX_PENDING_NATIVE_RPC: usize = 32;
const NATIVE_TIMEOUT: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const HOST_RETRY_DELAY: Duration = Duration::from_secs(1);

#[derive(Debug)]
pub enum AdapterError {
    Configuration,
    Owner,
    Checkpoint,
    Host,
    HostProtocol,
    NativeAttach,
}

impl AdapterError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Configuration => "CODEX_ADAPTER_CONFIGURATION_INVALID",
            Self::Owner => "CODEX_MODULE_OWNER_INVALID",
            Self::Checkpoint => "CODEX_CHECKPOINT_UNAVAILABLE",
            Self::Host => "CODEX_HOST_LINK_UNAVAILABLE",
            Self::HostProtocol => "CODEX_HOST_PROTOCOL_INVALID",
            Self::NativeAttach => "CODEX_NATIVE_ATTACH_UNAVAILABLE",
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdapterConfig {
    pub host_data_dir: PathBuf,
    pub ipc: Value,
    pub endpoint: String,
    #[serde(default)]
    pub token_env: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum NativeRootPhase {
    Pending,
    Candidate,
    Active,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RootApplicability {
    Current,
    Historical,
}

impl RootApplicability {
    fn as_str(self) -> &'static str {
        match self {
            Self::Current => "current",
            Self::Historical => "historical",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum NativeRpcEvent {
    NativeUsageChanged,
    AccountRateLimitsUpdated {
        method: String,
        rate_limits: NativeRateLimitSnapshot,
        raw_params: Value,
    },
    ThreadTokenUsageUpdated {
        method: String,
        usage: NativeThreadTokenUsage,
        raw_params: Value,
    },
    MalformedUsageNotification {
        method: String,
        params: Value,
    },
    CurrentNotification {
        method: String,
        params: Value,
    },
    NativeNotification {
        method: String,
        params: Value,
    },
    ServerRequest {
        id: Value,
        method: String,
        params: Value,
        reply: Value,
    },
    DemultiplexerFault {
        diagnostic_code: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NativeEventRecord {
    appserver_scope: Option<String>,
    event: NativeRpcEvent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NativeRateLimitSnapshot {
    credits: Option<NativeCreditsSnapshot>,
    individual_limit: Option<NativeSpendControlLimitSnapshot>,
    limit_id: Option<String>,
    limit_name: Option<String>,
    normal_model_slug: Option<String>,
    plan_type: Option<String>,
    primary: Option<NativeRateLimitWindow>,
    rate_limit_reached_type: Option<String>,
    secondary: Option<NativeRateLimitWindow>,
    spend_control_reached: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NativeRateLimitNotification {
    rate_limits: NativeRateLimitSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NativeCreditsSnapshot {
    balance: Option<String>,
    has_credits: bool,
    unlimited: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NativeSpendControlLimitSnapshot {
    limit: String,
    remaining_percent: i32,
    resets_at: i64,
    used: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NativeRateLimitWindow {
    resets_at: Option<i64>,
    used_percent: i32,
    window_duration_mins: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NativeThreadTokenUsage {
    thread_id: String,
    turn_id: String,
    token_usage: NativeThreadTokenUsageBreakdown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NativeThreadTokenUsageBreakdown {
    last: NativeTokenUsageCounts,
    model_context_window: Option<i64>,
    total: NativeTokenUsageCounts,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NativeTokenUsageCounts {
    cache_write_input_tokens: Option<i64>,
    cached_input_tokens: i64,
    input_tokens: i64,
    output_tokens: i64,
    reasoning_output_tokens: i64,
    total_tokens: i64,
}

#[derive(Debug, Clone)]
struct QueuedNativeEvent {
    sequence: u64,
    encoded_bytes: usize,
    record: NativeEventRecord,
}

#[derive(Default)]
struct NativeEventQueue {
    items: VecDeque<QueuedNativeEvent>,
    encoded_bytes: usize,
    next_sequence: u64,
    terminal: bool,
    fault_recorded: bool,
}

#[derive(Clone)]
struct NativeEventSink {
    queue: Arc<Mutex<NativeEventQueue>>,
    scope: Arc<Mutex<Option<String>>>,
    usage: Arc<Mutex<native_usage::Collector>>,
}

impl NativeEventSink {
    fn new() -> Self {
        Self {
            queue: Arc::new(Mutex::new(NativeEventQueue::default())),
            scope: Arc::new(Mutex::new(None)),
            usage: Arc::new(Mutex::new(native_usage::Collector::default())),
        }
    }

    fn set_scope(&self, scope: String) -> Result<(), NativeError> {
        let mut current_scope = self.scope.lock().map_err(|_| NativeError::Protocol)?;
        let mut queue = self.queue.lock().map_err(|_| NativeError::Protocol)?;
        let mut usage = self.usage.lock().map_err(|_| NativeError::Protocol)?;
        usage.connect();
        for event in &queue.items {
            if let NativeRpcEvent::AccountRateLimitsUpdated { rate_limits, .. } =
                &event.record.event
            {
                usage.rolling_update(rate_limits.clone());
            }
        }
        let mut encoded_bytes = 0usize;
        for event in &queue.items {
            let mut record = event.record.clone();
            record.appserver_scope = Some(scope.clone());
            encoded_bytes = encoded_bytes
                .checked_add(
                    serde_json::to_vec(&record)
                        .map_err(|_| NativeError::Protocol)?
                        .len(),
                )
                .ok_or(NativeError::DemultiplexerCapacity)?;
        }
        if encoded_bytes > MAX_NATIVE_EVENT_BYTES {
            queue.terminal = true;
            Self::record_fault_locked(&mut queue, Some(scope), "NATIVE_EVENT_BUFFER_LIMIT");
            return Err(NativeError::DemultiplexerCapacity);
        }
        for event in &mut queue.items {
            event.record.appserver_scope = Some(scope.clone());
            event.encoded_bytes = serde_json::to_vec(&event.record)
                .map_err(|_| NativeError::Protocol)?
                .len();
        }
        queue.encoded_bytes = encoded_bytes;
        *current_scope = Some(scope);
        Ok(())
    }

    fn queue_event(&self, event: NativeRpcEvent) -> Result<(), NativeError> {
        let scope = self.scope.lock().map_err(|_| NativeError::Protocol)?;
        {
            let mut usage = self.usage.lock().map_err(|_| NativeError::Protocol)?;
            match &event {
                NativeRpcEvent::AccountRateLimitsUpdated { rate_limits, .. } => {
                    usage.rolling_update(rate_limits.clone())
                }
                NativeRpcEvent::MalformedUsageNotification { method, .. }
                    if method == "account/rateLimits/updated" =>
                {
                    usage.malformed_update()
                }
                NativeRpcEvent::NativeNotification { method, .. }
                    if method == "account/updated" =>
                {
                    usage.invalidate_auth()
                }
                _ => {}
            }
        }
        let record = NativeEventRecord {
            appserver_scope: scope.clone(),
            event,
        };
        let encoded_bytes = serde_json::to_vec(&record)
            .map_err(|_| NativeError::Protocol)?
            .len();
        let mut queue = self.queue.lock().map_err(|_| NativeError::Protocol)?;
        if queue.terminal {
            return Err(NativeError::DemultiplexerCapacity);
        }
        let ordinary_byte_limit = MAX_NATIVE_EVENT_BYTES - NATIVE_EVENT_FAULT_RESERVE_BYTES;
        if queue.items.len() >= MAX_NATIVE_EVENT_ITEMS - 1
            || encoded_bytes > ordinary_byte_limit.saturating_sub(queue.encoded_bytes)
        {
            Self::record_fault_locked(
                &mut queue,
                record.appserver_scope.clone(),
                "NATIVE_EVENT_BUFFER_LIMIT",
            );
            return Err(NativeError::DemultiplexerCapacity);
        }
        Self::push_locked(&mut queue, record, encoded_bytes)?;
        Ok(())
    }

    fn record_fault(&self, diagnostic_code: &'static str) {
        if let Ok(mut usage) = self.usage.lock() {
            usage.closed();
        }
        if let Ok(scope) = self.scope.lock()
            && let Ok(mut queue) = self.queue.lock()
        {
            queue.terminal = true;
            Self::record_fault_locked(&mut queue, scope.clone(), diagnostic_code);
        }
    }

    fn record_fault_locked(
        queue: &mut NativeEventQueue,
        scope: Option<String>,
        diagnostic_code: &'static str,
    ) {
        if queue.fault_recorded {
            return;
        }
        let record = NativeEventRecord {
            appserver_scope: scope,
            event: NativeRpcEvent::DemultiplexerFault {
                diagnostic_code: diagnostic_code.into(),
            },
        };
        let encoded_bytes = serde_json::to_vec(&record).map_or(0, |value| value.len());
        if queue.items.len() < MAX_NATIVE_EVENT_ITEMS
            && encoded_bytes <= MAX_NATIVE_EVENT_BYTES.saturating_sub(queue.encoded_bytes)
        {
            let _ = Self::push_locked(queue, record, encoded_bytes);
        }
        queue.fault_recorded = true;
    }

    fn push_locked(
        queue: &mut NativeEventQueue,
        record: NativeEventRecord,
        encoded_bytes: usize,
    ) -> Result<(), NativeError> {
        queue.next_sequence = queue
            .next_sequence
            .checked_add(1)
            .ok_or(NativeError::DemultiplexerCapacity)?;
        queue.encoded_bytes = queue
            .encoded_bytes
            .checked_add(encoded_bytes)
            .ok_or(NativeError::DemultiplexerCapacity)?;
        queue.items.push_back(QueuedNativeEvent {
            sequence: queue.next_sequence,
            encoded_bytes,
            record,
        });
        Ok(())
    }

    fn snapshot(&self) -> Result<Vec<QueuedNativeEvent>, AdapterError> {
        self.queue
            .lock()
            .map_err(|_| AdapterError::Checkpoint)
            .map(|queue| queue.items.iter().cloned().collect())
    }

    fn acknowledge_through(&self, sequence: u64) -> Result<(), AdapterError> {
        let mut queue = self.queue.lock().map_err(|_| AdapterError::Checkpoint)?;
        while queue
            .items
            .front()
            .is_some_and(|event| event.sequence <= sequence)
        {
            if let Some(event) = queue.items.pop_front() {
                queue.encoded_bytes = queue.encoded_bytes.saturating_sub(event.encoded_bytes);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Checkpoint {
    version: u32,
    module_artifact_id: String,
    boot_id: String,
    binding_id: Option<String>,
    generation: Option<i64>,
    native_root_id: Option<String>,
    #[serde(default)]
    native_root_phase: Option<NativeRootPhase>,
    #[serde(default)]
    root_operation_id: Option<String>,
    native_scope_key: Option<String>,
    requested_model_provider: Option<String>,
    requested_model: Option<String>,
    workspace_root: Option<String>,
    effective_model_provider: Option<String>,
    effective_model: Option<String>,
    operations: BTreeMap<String, OperationRecord>,
    acknowledged_outcomes: BTreeSet<String>,
    observe_sequence: u64,
    pending_observation: Option<Value>,
    #[serde(default)]
    pending_native_events: Vec<NativeEventRecord>,
    #[serde(default)]
    native_usage: Option<swarm_contracts::native_usage::NativeUsageSnapshot>,
}

impl Checkpoint {
    fn new(boot_id: String) -> Self {
        Self {
            version: 2,
            module_artifact_id: ARTIFACT_ID.into(),
            boot_id,
            binding_id: None,
            generation: None,
            native_root_id: None,
            native_root_phase: None,
            root_operation_id: None,
            native_scope_key: None,
            requested_model_provider: None,
            requested_model: None,
            workspace_root: None,
            effective_model_provider: None,
            effective_model: None,
            operations: BTreeMap::new(),
            acknowledged_outcomes: BTreeSet::new(),
            observe_sequence: 0,
            pending_observation: None,
            pending_native_events: Vec::new(),
            native_usage: None,
        }
    }

    fn is_current_root(
        &self,
        root: &str,
        scope: &str,
        provider: &str,
        model: &str,
        workspace: &str,
    ) -> bool {
        self.native_root_phase == Some(NativeRootPhase::Active)
            && self.root_context_matches(root, scope, provider, model, workspace)
            && self.effective_model_provider.as_deref() == Some(provider)
            && self.effective_model.as_deref() == Some(model)
            && self.verified_open_operation_matches(root, scope, provider, model, workspace)
    }

    fn verified_open_operation_matches(
        &self,
        root: &str,
        scope: &str,
        provider: &str,
        model: &str,
        workspace: &str,
    ) -> bool {
        let Some(operation_id) = self.root_operation_id.as_deref() else {
            return false;
        };
        let Some(record) = self.operations.get(operation_id) else {
            return false;
        };
        record.method == "agent.open"
            && record.kind == "open"
            && record.native_root_id.as_deref() == Some(root)
            && record.native_scope_key.as_deref() == Some(scope)
            && record.requested_model_provider.as_deref() == Some(provider)
            && record.requested_model.as_deref() == Some(model)
            && record.workspace_root.as_deref().and_then(normalize_path)
                == normalize_path(workspace)
            && record.thread_configuration_readback.as_deref() == Some("verified")
    }

    fn active_root_proof_matches(&self) -> bool {
        let (Some(root), Some(scope), Some(provider), Some(model), Some(workspace)) = (
            self.native_root_id.as_deref(),
            self.native_scope_key.as_deref(),
            self.requested_model_provider.as_deref(),
            self.requested_model.as_deref(),
            self.workspace_root.as_deref(),
        ) else {
            return false;
        };
        self.root_context_matches(root, scope, provider, model, workspace)
            && self.effective_model_provider.as_deref() == Some(provider)
            && self.effective_model.as_deref() == Some(model)
            && self.verified_open_operation_matches(root, scope, provider, model, workspace)
    }

    fn root_context_matches(
        &self,
        root: &str,
        scope: &str,
        provider: &str,
        model: &str,
        workspace: &str,
    ) -> bool {
        self.native_root_id.as_deref() == Some(root)
            && self.native_scope_key.as_deref() == Some(scope)
            && self.requested_model_provider.as_deref() == Some(provider)
            && self.requested_model.as_deref() == Some(model)
            && self.workspace_root.as_deref().and_then(normalize_path) == normalize_path(workspace)
    }

    fn classify_root_applicability(
        &self,
        record: &OperationRecord,
        outcome: &RuntimeOutcome,
    ) -> Result<Option<RootApplicability>, AdapterError> {
        let (Some(root), Some(scope)) = (
            outcome.native_root_id.as_deref(),
            outcome.native_scope_key.as_deref(),
        ) else {
            return Ok(None);
        };
        let (Some(provider), Some(model)) = (
            outcome.details["requested_model_provider"].as_str(),
            outcome.details["requested_model"].as_str(),
        ) else {
            return Ok(None);
        };
        if record
            .native_root_id
            .as_deref()
            .is_some_and(|saved| saved != root)
            || record
                .native_scope_key
                .as_deref()
                .is_some_and(|saved| saved != scope)
            || record
                .requested_model_provider
                .as_deref()
                .is_some_and(|saved| saved != provider)
            || record
                .requested_model
                .as_deref()
                .is_some_and(|saved| saved != model)
        {
            return Err(AdapterError::Checkpoint);
        }
        if let Some(workspace) = self.workspace_root.as_deref()
            && self.is_current_root(root, scope, provider, model, workspace)
        {
            if record
                .workspace_root
                .as_deref()
                .is_some_and(|saved| normalize_path(saved) != normalize_path(workspace))
            {
                return Err(AdapterError::Checkpoint);
            }
            return Ok(Some(RootApplicability::Current));
        }
        let historical_context_matches = matches!(
            (record.method.as_str(), record.kind.as_str()),
            ("agent.open", "open") | ("agent.send", "send") | ("task.dispatch", "send")
        ) && record.native_root_id.as_deref() == Some(root)
            && record.native_scope_key.as_deref() == Some(scope)
            && record.requested_model_provider.as_deref() == Some(provider)
            && record.requested_model.as_deref() == Some(model)
            && record.thread_configuration_readback.as_deref() == Some("verified")
            && record
                .workspace_root
                .as_deref()
                .and_then(normalize_path)
                .is_some()
            && (outcome.details["native_thread_readback"] == "verified"
                || outcome.details["native_input_readback"] == "verified");
        if historical_context_matches {
            Ok(Some(RootApplicability::Historical))
        } else {
            Ok(None)
        }
    }

    fn restore_legacy_root_phase(&mut self) {
        if self.native_root_phase == Some(NativeRootPhase::Active)
            && !self.active_root_proof_matches()
        {
            self.native_root_phase = if self.native_root_id.is_some() {
                Some(NativeRootPhase::Candidate)
            } else if self.native_scope_key.is_some() || self.root_operation_id.is_some() {
                Some(NativeRootPhase::Pending)
            } else {
                None
            };
            self.effective_model_provider = None;
            self.effective_model = None;
        }
        if self.native_root_phase.is_some() {
            return;
        }
        let matching_open_operations: Vec<String> = self
            .operations
            .iter()
            .filter(|(_, record)| {
                record.method == "agent.open"
                    && record.kind == "open"
                    && record.native_root_id.as_deref() == self.native_root_id.as_deref()
                    && record.native_scope_key.as_deref() == self.native_scope_key.as_deref()
                    && record.requested_model_provider.as_deref()
                        == self.requested_model_provider.as_deref()
                    && record.requested_model.as_deref() == self.requested_model.as_deref()
                    && record.workspace_root.as_deref().and_then(normalize_path)
                        == self.workspace_root.as_deref().and_then(normalize_path)
            })
            .map(|(operation_id, _)| operation_id.clone())
            .collect();
        if matching_open_operations.len() == 1 {
            self.root_operation_id = matching_open_operations.into_iter().next();
        }
        if self.native_root_id.is_some() {
            // Old checkpoints have no proof-state marker. Re-read the exact
            // open operation before allowing this root to act as current.
            self.native_root_phase = Some(NativeRootPhase::Candidate);
            self.effective_model_provider = None;
            self.effective_model = None;
        } else if self.native_scope_key.is_some() || self.root_operation_id.is_some() {
            // A start may have taken effect without returning its root id.
            // Preserve the uncertainty and prevent a second thread/start.
            self.native_root_phase = Some(NativeRootPhase::Pending);
            self.effective_model_provider = None;
            self.effective_model = None;
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OperationRecord {
    method: String,
    kind: String,
    state: String,
    #[serde(default)]
    input_sha256: Option<String>,
    #[serde(default)]
    native_request_failure_code: Option<String>,
    #[serde(default)]
    native_rpc_error_code: Option<i64>,
    native_root_id: Option<String>,
    native_scope_key: Option<String>,
    requested_model_provider: Option<String>,
    requested_model: Option<String>,
    workspace_root: Option<String>,
    #[serde(default)]
    thread_configuration_readback: Option<String>,
    client_user_message_id: Option<String>,
    prompt_sha256: Option<String>,
    prompt_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    prompt_contract_revision: Option<String>,
    delivery: Option<String>,
    expected_turn_id: Option<String>,
    returned_turn_id: Option<String>,
    returned_turn_status: Option<String>,
    native_input_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    dispatch_admission: Option<TaskDispatchAdmissionReceipt>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    continuation_admission: Option<GoalContinuationAdmissionReceipt>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pending_result_page: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    result_page_acknowledgement: Option<ResultPageAcknowledgement>,
    outcome: Option<Value>,
}

impl OperationRecord {
    fn intent(method: &str, kind: &str) -> Self {
        Self {
            method: method.into(),
            kind: kind.into(),
            state: "native_effect_may_have_started".into(),
            input_sha256: None,
            native_request_failure_code: None,
            native_rpc_error_code: None,
            native_root_id: None,
            native_scope_key: None,
            requested_model_provider: None,
            requested_model: None,
            workspace_root: None,
            thread_configuration_readback: None,
            client_user_message_id: None,
            prompt_sha256: None,
            prompt_bytes: None,
            prompt_contract_revision: None,
            delivery: None,
            expected_turn_id: None,
            returned_turn_id: None,
            returned_turn_status: None,
            native_input_id: None,
            dispatch_admission: None,
            continuation_admission: None,
            pending_result_page: None,
            result_page_acknowledgement: None,
            outcome: None,
        }
    }

    fn compacted(method: String, kind: String, outcome: Value, input_sha256: String) -> Self {
        Self {
            method,
            kind,
            state: "terminal_acknowledged_compacted".into(),
            input_sha256: Some(input_sha256),
            native_request_failure_code: None,
            native_rpc_error_code: None,
            native_root_id: None,
            native_scope_key: None,
            requested_model_provider: None,
            requested_model: None,
            workspace_root: None,
            thread_configuration_readback: None,
            client_user_message_id: None,
            prompt_sha256: None,
            prompt_bytes: None,
            prompt_contract_revision: None,
            delivery: None,
            expected_turn_id: None,
            returned_turn_id: None,
            returned_turn_status: None,
            native_input_id: None,
            dispatch_admission: None,
            continuation_admission: None,
            pending_result_page: None,
            result_page_acknowledgement: None,
            outcome: Some(outcome),
        }
    }

    fn compacted_result_page(
        method: String,
        kind: String,
        input_sha256: String,
        acknowledgement: ResultPageAcknowledgement,
    ) -> Self {
        Self {
            method,
            kind,
            state: "terminal_acknowledged_compacted".into(),
            input_sha256: Some(input_sha256),
            native_request_failure_code: None,
            native_rpc_error_code: None,
            native_root_id: None,
            native_scope_key: None,
            requested_model_provider: None,
            requested_model: None,
            workspace_root: None,
            thread_configuration_readback: None,
            client_user_message_id: None,
            prompt_sha256: None,
            prompt_bytes: None,
            prompt_contract_revision: None,
            delivery: None,
            expected_turn_id: None,
            returned_turn_id: None,
            returned_turn_status: None,
            native_input_id: None,
            dispatch_admission: None,
            continuation_admission: None,
            pending_result_page: None,
            result_page_acknowledgement: Some(acknowledgement),
            outcome: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResultPageAcknowledgement {
    input_sha256: String,
    artifact_ref: String,
    source: NormalizedResultPageSource,
    offset_bytes: u64,
    byte_length: u64,
    total_bytes: u64,
    page_sha256: String,
}

impl ResultPageAcknowledgement {
    fn validate(&self, operation_id: &str) -> Result<(), AdapterError> {
        let end = self
            .offset_bytes
            .checked_add(self.byte_length)
            .ok_or(AdapterError::Checkpoint)?;
        if !valid_sha256(&self.input_sha256)
            || self.artifact_ref.trim().is_empty()
            || self.artifact_ref.len() > MAX_OPERATION_ID_BYTES
            || self.source.validate().is_err()
            || self.source.result_operation_id != operation_id
            || self.source.result_input_sha256 != self.input_sha256
            || !valid_sha256(&self.page_sha256)
            || self.total_bytes != self.source.payload_bytes
            || end > self.total_bytes
            || self.byte_length > MAX_NORMALIZED_RESULT_PAGE_BYTES as u64
        {
            return Err(AdapterError::Checkpoint);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OperationTombstone {
    version: u32,
    operation_id: String,
    method: String,
    kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    outcome: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    result_page_acknowledgement: Option<ResultPageAcknowledgement>,
}

/// A write-ahead full-snapshot journal. A torn latest record makes startup
/// fail closed, so a possible native write cannot be replayed from an older
/// checkpoint. Older snapshots are pruned only after publishing the new one;
/// terminal host-acknowledged IDs remain in the keyed tombstone index.
struct Journal {
    directory: PathBuf,
    sequence: u64,
    state: Checkpoint,
    native_events: NativeEventSink,
}

impl Journal {
    fn open(directory: PathBuf, boot_id: String) -> Result<Self, AdapterError> {
        if !directory.is_absolute() {
            return Err(AdapterError::Owner);
        }
        fs::create_dir_all(&directory).map_err(|_| AdapterError::Checkpoint)?;
        let mut latest_sequence = 0u64;
        for entry in fs::read_dir(&directory).map_err(|_| AdapterError::Checkpoint)? {
            let entry = entry.map_err(|_| AdapterError::Checkpoint)?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let Some(number) = name
                .strip_prefix("checkpoint-")
                .and_then(|value| value.strip_suffix(".json"))
            else {
                if name.starts_with("checkpoint-") {
                    return Err(AdapterError::Checkpoint);
                }
                continue;
            };
            if !(16..=20).contains(&number.len())
                || !number.bytes().all(|byte| byte.is_ascii_digit())
            {
                return Err(AdapterError::Checkpoint);
            }
            let file_type = entry.file_type().map_err(|_| AdapterError::Checkpoint)?;
            if !file_type.is_file() || file_type.is_symlink() {
                return Err(AdapterError::Checkpoint);
            }
            if entry
                .metadata()
                .map_err(|_| AdapterError::Checkpoint)?
                .len()
                > MAX_CHECKPOINT_BYTES
            {
                return Err(AdapterError::Checkpoint);
            }
            let sequence = number
                .parse::<u64>()
                .map_err(|_| AdapterError::Checkpoint)?;
            if sequence == 0 || format!("{sequence:016}") != number {
                return Err(AdapterError::Checkpoint);
            }
            latest_sequence = latest_sequence.max(sequence);
        }
        let marker_path = directory.join(JOURNAL_MARKER_FILE);
        let marker_present = match fs::symlink_metadata(&marker_path) {
            Ok(_) => {
                if read_bounded_file(&marker_path, JOURNAL_MARKER_BYTES.len() as u64)?
                    != JOURNAL_MARKER_BYTES
                {
                    return Err(AdapterError::Checkpoint);
                }
                true
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(_) => return Err(AdapterError::Checkpoint),
        };
        if marker_present && latest_sequence == 0 {
            return Err(AdapterError::Checkpoint);
        }
        let (sequence, mut state) = if latest_sequence > 0 {
            let path = directory.join(format!("checkpoint-{latest_sequence:016}.json"));
            let bytes = read_bounded_file(&path, MAX_CHECKPOINT_BYTES)?;
            let state: Checkpoint =
                serde_json::from_slice(&bytes).map_err(|_| AdapterError::Checkpoint)?;
            (latest_sequence, state)
        } else {
            (0, Checkpoint::new(boot_id.clone()))
        };
        if state.version != 2 || state.module_artifact_id != ARTIFACT_ID {
            return Err(AdapterError::Checkpoint);
        }
        state.restore_legacy_root_phase();
        match state.native_root_phase {
            Some(NativeRootPhase::Pending) if state.native_root_id.is_some() => {
                return Err(AdapterError::Checkpoint);
            }
            Some(NativeRootPhase::Candidate | NativeRootPhase::Active)
                if state.native_root_id.is_none() =>
            {
                return Err(AdapterError::Checkpoint);
            }
            Some(NativeRootPhase::Active) if state.root_operation_id.is_none() => {
                return Err(AdapterError::Checkpoint);
            }
            _ => {}
        }
        let prior_boot = state.boot_id.clone();
        state.boot_id = boot_id;
        if prior_boot != state.boot_id {
            state.pending_observation = None;
            if let Some(usage) = &mut state.native_usage {
                usage.freshness = swarm_contracts::native_usage::UsageFreshness::Stale;
            }
        }
        let mut journal = Self {
            directory,
            sequence,
            state,
            native_events: NativeEventSink::new(),
        };
        journal.compact_acknowledged()?;
        journal.validate_live_bounds()?;
        // Persist the new launcher boot even when the native thread stays
        // attached; host recovery owns departure verification. This full
        // snapshot subsumes older snapshots, which are pruned after publish.
        journal.save()?;
        Ok(journal)
    }

    fn save(&mut self) -> Result<(), AdapterError> {
        self.validate_live_bounds()?;
        let bytes = serde_json::to_vec(&self.state).map_err(|_| AdapterError::Checkpoint)?;
        if bytes.len() as u64 > MAX_CHECKPOINT_BYTES {
            return Err(AdapterError::Checkpoint);
        }
        // Mark this directory as initialized before the first snapshot can be
        // published. If a checkpoint is later lost, startup must not mistake
        // the initialized journal for a fresh one.
        self.ensure_journal_marker()?;
        let next = self
            .sequence
            .checked_add(1)
            .ok_or(AdapterError::Checkpoint)?;
        let path = self.directory.join(format!("checkpoint-{next:016}.json"));
        let temporary = self
            .directory
            .join(format!(".checkpoint-{}-{}.tmp", next, Uuid::new_v4()));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|_| AdapterError::Checkpoint)?;
        if file.write_all(&bytes).is_err() || file.sync_all().is_err() {
            drop(file);
            let _ = fs::remove_file(&temporary);
            return Err(AdapterError::Checkpoint);
        }
        drop(file);
        if path.exists() {
            let _ = fs::remove_file(&temporary);
            return Err(AdapterError::Checkpoint);
        }
        if fs::rename(&temporary, &path).is_err() {
            let _ = fs::remove_file(&temporary);
            return Err(AdapterError::Checkpoint);
        }
        self.sequence = next;
        self.prune_old_checkpoints()
    }

    fn native_event_sink(&self) -> NativeEventSink {
        self.native_events.clone()
    }

    fn capture_native_events(&mut self) -> Result<bool, AdapterError> {
        let queued = self.native_events.snapshot()?;
        let Some(last_sequence) = queued.last().map(|event| event.sequence) else {
            return Ok(false);
        };
        let mut pending = self.state.pending_native_events.clone();
        pending.extend(queued.iter().map(|event| event.record.clone()));
        let encoded = serde_json::to_vec(&pending).map_err(|_| AdapterError::Checkpoint)?;
        if pending.len() > MAX_PENDING_NATIVE_EVENT_ITEMS
            || encoded.len() as u64 > MAX_PENDING_NATIVE_EVENT_BYTES
        {
            return Err(AdapterError::Checkpoint);
        }
        let prior = std::mem::replace(&mut self.state.pending_native_events, pending);
        let usage = self
            .native_events
            .usage
            .lock()
            .map_err(|_| AdapterError::Checkpoint)?
            .snapshot();
        let prior_usage = std::mem::replace(&mut self.state.native_usage, usage);
        if self.save().is_err() {
            self.state.pending_native_events = prior;
            self.state.native_usage = prior_usage;
            return Err(AdapterError::Checkpoint);
        }
        self.native_events.acknowledge_through(last_sequence)?;
        Ok(true)
    }

    fn ensure_journal_marker(&self) -> Result<(), AdapterError> {
        let path = self.directory.join(JOURNAL_MARKER_FILE);
        match fs::symlink_metadata(&path) {
            Ok(_) => {
                let bytes = read_bounded_file(&path, JOURNAL_MARKER_BYTES.len() as u64)?;
                return if bytes == JOURNAL_MARKER_BYTES {
                    Ok(())
                } else {
                    Err(AdapterError::Checkpoint)
                };
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(AdapterError::Checkpoint),
        }
        let temporary = self
            .directory
            .join(format!(".journal-active-{}.tmp", Uuid::new_v4()));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|_| AdapterError::Checkpoint)?;
        if file.write_all(JOURNAL_MARKER_BYTES).is_err() || file.sync_all().is_err() {
            drop(file);
            let _ = fs::remove_file(&temporary);
            return Err(AdapterError::Checkpoint);
        }
        drop(file);
        if path.exists() || fs::rename(&temporary, &path).is_err() {
            let _ = fs::remove_file(&temporary);
            let bytes = read_bounded_file(&path, JOURNAL_MARKER_BYTES.len() as u64)?;
            return if bytes == JOURNAL_MARKER_BYTES {
                Ok(())
            } else {
                Err(AdapterError::Checkpoint)
            };
        }
        Ok(())
    }

    fn live_usage(&self) -> Result<(usize, usize), AdapterError> {
        let mut bytes = self
            .state
            .acknowledged_outcomes
            .iter()
            .try_fold(0usize, |total, operation_id| {
                if operation_id.len() <= MAX_OPERATION_ID_BYTES {
                    total.checked_add(operation_id.len())
                } else {
                    None
                }
            })
            .ok_or(AdapterError::Checkpoint)?;
        for (operation_id, record) in &self.state.operations {
            if operation_id.len() > MAX_OPERATION_ID_BYTES {
                return Err(AdapterError::Checkpoint);
            }
            let encoded = serde_json::to_vec(record).map_err(|_| AdapterError::Checkpoint)?;
            if encoded.len() > MAX_LIVE_OPERATION_RECORD_BYTES {
                return Err(AdapterError::Checkpoint);
            }
            bytes = bytes
                .checked_add(operation_id.len())
                .and_then(|total| total.checked_add(encoded.len()))
                .ok_or(AdapterError::Checkpoint)?;
        }
        Ok((self.state.operations.len(), bytes))
    }

    fn validate_live_bounds(&self) -> Result<(), AdapterError> {
        let (count, bytes) = self.live_usage()?;
        let native_event_bytes = serde_json::to_vec(&self.state.pending_native_events)
            .map_err(|_| AdapterError::Checkpoint)?
            .len();
        if count > MAX_LIVE_OPERATION_COUNT
            || bytes > MAX_LIVE_OPERATION_BYTES
            || self.state.pending_native_events.len() > MAX_PENDING_NATIVE_EVENT_ITEMS
            || native_event_bytes as u64 > MAX_PENDING_NATIVE_EVENT_BYTES
        {
            return Err(AdapterError::Checkpoint);
        }
        Ok(())
    }

    fn can_start_operation(&self, reconciliation: bool) -> Result<bool, AdapterError> {
        let (count, bytes) = self.live_usage()?;
        // Each command result is persisted before it is reported and
        // acknowledged before the host can give us another command. Ordinary
        // work reserves a later reconcile receipt and target-result growth;
        // reconciliation reserves its receipt and possible target growth.
        let reserved_slots = if reconciliation { 1 } else { 2 };
        let reserved_bytes = MAX_LIVE_OPERATION_SLOT_BYTES
            .checked_mul(if reconciliation { 2 } else { 3 })
            .ok_or(AdapterError::Checkpoint)?;
        Ok(count
            .checked_add(reserved_slots)
            .is_some_and(|needed| needed <= MAX_LIVE_OPERATION_COUNT)
            && bytes
                .checked_add(reserved_bytes)
                .is_some_and(|needed| needed <= MAX_LIVE_OPERATION_BYTES))
    }

    fn operation_record(
        &self,
        operation_id: &str,
    ) -> Result<Option<OperationRecord>, AdapterError> {
        if operation_id.len() > MAX_OPERATION_ID_BYTES {
            return Err(AdapterError::Checkpoint);
        }
        if let Some(record) = self.state.operations.get(operation_id) {
            return Ok(Some(record.clone()));
        }
        let Some(tombstone) = self.read_operation_tombstone(operation_id)? else {
            return Ok(None);
        };
        if let Some(outcome) = tombstone.outcome {
            let input_sha256 = receipt_identity_from_value(&outcome)?.input_sha256;
            return Ok(Some(OperationRecord::compacted(
                tombstone.method,
                tombstone.kind,
                outcome,
                input_sha256,
            )));
        }
        let acknowledgement = tombstone
            .result_page_acknowledgement
            .ok_or(AdapterError::Checkpoint)?;
        acknowledgement.validate(operation_id)?;
        Ok(Some(OperationRecord::compacted_result_page(
            tombstone.method,
            tombstone.kind,
            acknowledgement.input_sha256.clone(),
            acknowledgement,
        )))
    }

    fn operation_ack_path(
        &self,
        operation_id: &str,
        create: bool,
    ) -> Result<Option<PathBuf>, AdapterError> {
        let digest = digest_hex(operation_id.as_bytes());
        let root = self.directory.join(OPERATION_ACK_DIRECTORY);
        if create {
            ensure_directory(&root)?;
        } else if !checked_directory_exists(&root)? {
            return Ok(None);
        }
        let shard = root.join(&digest[..2]);
        if create {
            ensure_directory(&shard)?;
        } else if !checked_directory_exists(&shard)? {
            return Ok(None);
        }
        Ok(Some(shard.join(format!("{}.json", &digest[2..]))))
    }

    fn read_operation_tombstone(
        &self,
        operation_id: &str,
    ) -> Result<Option<OperationTombstone>, AdapterError> {
        if operation_id.len() > MAX_OPERATION_ID_BYTES {
            return Err(AdapterError::Checkpoint);
        }
        let Some(path) = self.operation_ack_path(operation_id, false)? else {
            return Ok(None);
        };
        match fs::symlink_metadata(&path) {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(AdapterError::Checkpoint),
        };
        let bytes = read_bounded_file(&path, MAX_OPERATION_ACK_BYTES)?;
        let tombstone: OperationTombstone =
            serde_json::from_slice(&bytes).map_err(|_| AdapterError::Checkpoint)?;
        if tombstone.version != 1
            || tombstone.operation_id != operation_id
            || tombstone.method.is_empty()
            || tombstone.kind.is_empty()
        {
            return Err(AdapterError::Checkpoint);
        }
        if operation_id.len() > MAX_OPERATION_ID_BYTES
            || tombstone.outcome.is_some() == tombstone.result_page_acknowledgement.is_some()
        {
            return Err(AdapterError::Checkpoint);
        }
        if let Some(encoded) = tombstone.outcome.as_ref() {
            let outcome: RuntimeOutcome =
                serde_json::from_value(encoded.clone()).map_err(|_| AdapterError::Checkpoint)?;
            let receipt = receipt_identity_from_outcome(&outcome)?;
            if outcome.operation_id != operation_id
                || receipt.operation_id != operation_id
                || !matches!(
                    outcome.outcome,
                    EffectOutcome::Applied | EffectOutcome::Rejected
                )
                || tombstone.result_page_acknowledgement.is_some()
            {
                return Err(AdapterError::Checkpoint);
            }
        } else if let Some(acknowledgement) = tombstone.result_page_acknowledgement.as_ref() {
            acknowledgement.validate(operation_id)?;
            if tombstone.method != "agent.result" || tombstone.kind != "result_page" {
                return Err(AdapterError::Checkpoint);
            }
        }
        Ok(Some(tombstone))
    }

    fn write_operation_tombstone(
        &self,
        operation_id: &str,
        record: &OperationRecord,
    ) -> Result<(), AdapterError> {
        if operation_id.len() > MAX_OPERATION_ID_BYTES {
            return Err(AdapterError::Checkpoint);
        }
        let outcome = record.outcome.clone().ok_or(AdapterError::Checkpoint)?;
        let parsed: RuntimeOutcome =
            serde_json::from_value(outcome.clone()).map_err(|_| AdapterError::Checkpoint)?;
        if parsed.operation_id != operation_id
            || !matches!(
                parsed.outcome,
                EffectOutcome::Applied | EffectOutcome::Rejected
            )
        {
            return Err(AdapterError::Checkpoint);
        }
        let receipt = receipt_identity_from_outcome(&parsed)?;
        if record.input_sha256.as_deref() != Some(receipt.input_sha256.as_str()) {
            return Err(AdapterError::Checkpoint);
        }
        let tombstone = OperationTombstone {
            version: 1,
            operation_id: operation_id.to_owned(),
            method: record.method.clone(),
            kind: record.kind.clone(),
            outcome: Some(outcome),
            result_page_acknowledgement: None,
        };
        self.write_operation_tombstone_value(operation_id, &tombstone)
    }

    fn write_result_page_tombstone(
        &self,
        operation_id: &str,
        record: &OperationRecord,
    ) -> Result<(), AdapterError> {
        let acknowledgement = record
            .result_page_acknowledgement
            .clone()
            .ok_or(AdapterError::Checkpoint)?;
        acknowledgement.validate(operation_id)?;
        if record.method != "agent.result"
            || record.kind != "result_page"
            || record.pending_result_page.is_some()
            || record.outcome.is_some()
            || record.input_sha256.as_deref() != Some(acknowledgement.input_sha256.as_str())
        {
            return Err(AdapterError::Checkpoint);
        }
        let tombstone = OperationTombstone {
            version: 1,
            operation_id: operation_id.to_owned(),
            method: record.method.clone(),
            kind: record.kind.clone(),
            outcome: None,
            result_page_acknowledgement: Some(acknowledgement),
        };
        self.write_operation_tombstone_value(operation_id, &tombstone)
    }

    fn write_operation_tombstone_value(
        &self,
        operation_id: &str,
        tombstone: &OperationTombstone,
    ) -> Result<(), AdapterError> {
        if operation_id.len() > MAX_OPERATION_ID_BYTES {
            return Err(AdapterError::Checkpoint);
        }
        if let Some(existing) = self.read_operation_tombstone(operation_id)? {
            return if existing.method == tombstone.method
                && existing.kind == tombstone.kind
                && existing.outcome == tombstone.outcome
                && existing.result_page_acknowledgement == tombstone.result_page_acknowledgement
            {
                Ok(())
            } else {
                Err(AdapterError::Checkpoint)
            };
        }
        let bytes = serde_json::to_vec(tombstone).map_err(|_| AdapterError::Checkpoint)?;
        if bytes.len() as u64 > MAX_OPERATION_ACK_BYTES {
            return Err(AdapterError::Checkpoint);
        }
        let Some(path) = self.operation_ack_path(operation_id, true)? else {
            return Err(AdapterError::Checkpoint);
        };
        let parent = path.parent().ok_or(AdapterError::Checkpoint)?;
        let temporary = parent.join(format!(".operation-ack-{}.tmp", Uuid::new_v4()));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|_| AdapterError::Checkpoint)?;
        if file.write_all(&bytes).is_err() || file.sync_all().is_err() {
            drop(file);
            let _ = fs::remove_file(&temporary);
            return Err(AdapterError::Checkpoint);
        }
        drop(file);
        if path.exists() || fs::rename(&temporary, &path).is_err() {
            let _ = fs::remove_file(&temporary);
            if self
                .read_operation_tombstone(operation_id)?
                .is_some_and(|existing| {
                    existing.method == tombstone.method
                        && existing.kind == tombstone.kind
                        && existing.outcome == tombstone.outcome
                        && existing.result_page_acknowledgement
                            == tombstone.result_page_acknowledgement
                })
            {
                return Ok(());
            }
            return Err(AdapterError::Checkpoint);
        }
        Ok(())
    }

    fn compact_acknowledged(&mut self) -> Result<(), AdapterError> {
        let orphan_acknowledgments = self
            .state
            .acknowledged_outcomes
            .iter()
            .filter(|operation_id| !self.state.operations.contains_key(*operation_id))
            .cloned()
            .collect::<Vec<_>>();
        for operation_id in &orphan_acknowledgments {
            if self.read_operation_tombstone(operation_id)?.is_none() {
                return Err(AdapterError::Checkpoint);
            }
        }
        let operation_ids = self.state.operations.keys().cloned().collect::<Vec<_>>();
        let mut compact = Vec::new();
        for operation_id in operation_ids {
            if self.state.root_operation_id.as_deref() == Some(operation_id.as_str()) {
                continue;
            }
            let record = self
                .state
                .operations
                .get(&operation_id)
                .ok_or(AdapterError::Checkpoint)?
                .clone();
            if let Some(tombstone) = self.read_operation_tombstone(&operation_id)? {
                if tombstone.method != record.method || tombstone.kind != record.kind {
                    return Err(AdapterError::Checkpoint);
                }
                let matches = match (
                    tombstone.outcome.as_ref(),
                    tombstone.result_page_acknowledgement.as_ref(),
                ) {
                    (Some(outcome), None) => record.outcome.as_ref() == Some(outcome),
                    (None, Some(acknowledgement)) => {
                        record.outcome.is_none()
                            && record.pending_result_page.is_none()
                            && record.result_page_acknowledgement.as_ref() == Some(acknowledgement)
                    }
                    _ => false,
                };
                if !matches {
                    return Err(AdapterError::Checkpoint);
                }
                compact.push(operation_id);
            } else if record.result_page_acknowledgement.is_some()
                && record.pending_result_page.is_none()
                && record.outcome.is_none()
            {
                self.write_result_page_tombstone(&operation_id, &record)?;
                compact.push(operation_id);
            } else if self.state.acknowledged_outcomes.contains(&operation_id)
                && is_terminal_record(&record, &operation_id)?
            {
                self.write_operation_tombstone(&operation_id, &record)?;
                compact.push(operation_id);
            }
        }
        for operation_id in compact {
            self.state.operations.remove(&operation_id);
            self.state.acknowledged_outcomes.remove(&operation_id);
        }
        for operation_id in orphan_acknowledgments {
            self.state.acknowledged_outcomes.remove(&operation_id);
        }
        Ok(())
    }

    fn prune_old_checkpoints(&self) -> Result<(), AdapterError> {
        for entry in fs::read_dir(&self.directory).map_err(|_| AdapterError::Checkpoint)? {
            let entry = entry.map_err(|_| AdapterError::Checkpoint)?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let Some(number) = name
                .strip_prefix("checkpoint-")
                .and_then(|value| value.strip_suffix(".json"))
            else {
                if name.starts_with("checkpoint-") {
                    return Err(AdapterError::Checkpoint);
                }
                continue;
            };
            if !(16..=20).contains(&number.len())
                || !number.bytes().all(|byte| byte.is_ascii_digit())
            {
                return Err(AdapterError::Checkpoint);
            }
            let sequence = number
                .parse::<u64>()
                .map_err(|_| AdapterError::Checkpoint)?;
            if sequence == 0 || format!("{sequence:016}") != number {
                return Err(AdapterError::Checkpoint);
            }
            let file_type = entry.file_type().map_err(|_| AdapterError::Checkpoint)?;
            if !file_type.is_file() || file_type.is_symlink() || sequence > self.sequence {
                return Err(AdapterError::Checkpoint);
            }
            if sequence < self.sequence {
                fs::remove_file(entry.path()).map_err(|_| AdapterError::Checkpoint)?;
            }
        }
        Ok(())
    }

    fn bind(&mut self, binding_id: &str, generation: i64) -> Result<(), AdapterError> {
        if generation <= 0
            || self
                .state
                .binding_id
                .as_deref()
                .is_some_and(|old| old != binding_id)
            || self.state.generation.is_some_and(|old| old != generation)
        {
            return Err(AdapterError::Checkpoint);
        }
        if self.state.binding_id.as_deref() == Some(binding_id)
            && self.state.generation == Some(generation)
        {
            return Ok(());
        }
        self.state.binding_id = Some(binding_id.to_owned());
        self.state.generation = Some(generation);
        self.save()
    }

    fn store_outcome(
        &mut self,
        outcome: &RuntimeOutcome,
        method: &str,
        kind: &str,
    ) -> Result<(), AdapterError> {
        let operation_id = outcome.operation_id.clone();
        let receipt = receipt_identity_from_outcome(outcome)?;
        if receipt.operation_id != operation_id
            || self.state.binding_id.as_deref() != Some(receipt.binding_id.as_str())
            || self.state.generation != Some(receipt.binding_generation)
        {
            return Err(AdapterError::Checkpoint);
        }
        let input_sha256 = receipt.input_sha256.as_str();
        let encoded = serde_json::to_value(outcome).map_err(|_| AdapterError::Checkpoint)?;
        let dispatch_admission = outcome
            .details
            .get("dispatch_admission")
            .filter(|value| !value.is_null())
            .map(|value| {
                let admission: TaskDispatchAdmissionReceipt =
                    serde_json::from_value(value.clone()).map_err(|_| AdapterError::Checkpoint)?;
                admission.validate().map_err(|_| AdapterError::Checkpoint)?;
                if admission.operation_id != operation_id
                    || admission.binding_id != receipt.binding_id
                    || admission.binding_generation != receipt.binding_generation
                    || admission.module_receipt != receipt
                    || admission.native_input_id != outcome.native_input_id
                {
                    return Err(AdapterError::Checkpoint);
                }
                Ok(admission)
            })
            .transpose()?;
        let continuation_admission = outcome
            .details
            .get("goal_continuation_admission")
            .filter(|value| !value.is_null())
            .map(|value| {
                if method != "agent.send" {
                    return Err(AdapterError::Checkpoint);
                }
                let admission: GoalContinuationAdmissionReceipt =
                    serde_json::from_value(value.clone()).map_err(|_| AdapterError::Checkpoint)?;
                admission.validate().map_err(|_| AdapterError::Checkpoint)?;
                if admission.context.operation_id != operation_id
                    || admission.context.binding_id != receipt.binding_id
                    || admission.context.binding_generation != receipt.binding_generation
                    || admission.module_receipt != receipt
                    || admission.native_input_id != outcome.native_input_id
                    || admission.context.continuation.method != "agent.send"
                {
                    return Err(AdapterError::Checkpoint);
                }
                Ok(admission)
            })
            .transpose()?;
        if let Some(tombstone) = self.read_operation_tombstone(&operation_id)? {
            return if tombstone.method == method
                && tombstone.kind == kind
                && tombstone.outcome.as_ref() == Some(&encoded)
            {
                Ok(())
            } else {
                Err(AdapterError::Checkpoint)
            };
        }
        if let Some(record) = self.state.operations.get_mut(&operation_id) {
            if record.method != method
                || record.kind != kind
                || record
                    .input_sha256
                    .as_deref()
                    .is_some_and(|saved| saved != input_sha256)
            {
                return Err(AdapterError::Checkpoint);
            }
            let admission_changed = if let Some(admission) = dispatch_admission.as_ref() {
                reconcile_dispatch_admission(record, admission)?
            } else {
                false
            };
            let continuation_changed = if let Some(admission) = continuation_admission.as_ref() {
                reconcile_continuation_admission(record, admission)?
            } else {
                false
            };
            if record.outcome.as_ref() == Some(&encoded) {
                return if admission_changed || continuation_changed {
                    self.save()
                } else {
                    Ok(())
                };
            }
            // Reconciliation may upgrade a previously acknowledged Unknown
            // result after later native history evidence. New bytes need a
            // fresh host acknowledgment under the same operation ID.
            self.state.acknowledged_outcomes.remove(&operation_id);
            record.input_sha256 = Some(input_sha256.to_owned());
            record.state = "reported_pending".into();
            record.outcome = Some(encoded);
        } else {
            self.state.acknowledged_outcomes.remove(&operation_id);
            let mut record = OperationRecord::intent(method, kind);
            record.input_sha256 = Some(input_sha256.to_owned());
            record.dispatch_admission = dispatch_admission;
            record.continuation_admission = continuation_admission;
            record.state = "reported_pending".into();
            record.outcome = Some(encoded);
            self.state.operations.insert(operation_id.clone(), record);
        }
        self.save()
    }

    fn validate_existing_goal_terminal_event(
        &self,
        outcome: &RuntimeOutcome,
    ) -> Result<(), AdapterError> {
        let Some(existing) = outcome.details.get("goal_terminal_event") else {
            return Ok(());
        };
        let (Some(root), Some(turn), Some(input)) = (
            outcome.native_root_id.as_deref(),
            outcome.turn_id.as_deref(),
            outcome.native_input_id.as_deref(),
        ) else {
            return Err(AdapterError::Checkpoint);
        };
        let operation = self
            .operation_record(&outcome.operation_id)?
            .ok_or(AdapterError::Checkpoint)?;
        if self
            .state
            .classify_root_applicability(&operation, outcome)?
            .is_none()
            && !matches!(
                (operation.method.as_str(), operation.kind.as_str()),
                ("agent.send", "send") | ("task.dispatch", "send")
            )
        {
            return Err(AdapterError::Checkpoint);
        }
        let event: GoalTerminalEventRef = serde_json::from_value(existing["event"].clone())
            .map_err(|_| AdapterError::Checkpoint)?;
        event.validate().map_err(|_| AdapterError::Checkpoint)?;
        if event.seq > self.state.observe_sequence
            || existing["schema_id"]
                != swarm_contracts::module_contract::GOAL_TERMINAL_EVIDENCE_SCHEMA_ID
            || existing["schema_version"] != 1
            || existing["source"] != "codex"
            || existing["reader_revision"] != "codex-turn-journal-v1"
            || existing["event_record"]["id"].as_str() != Some(event.id.as_str())
            || existing["event_record"]["operation_id"].as_str()
                != Some(outcome.operation_id.as_str())
            || existing["event_record"]["native_root_id"].as_str() != Some(root)
            || existing["event_record"]["turn_id"].as_str() != Some(turn)
            || existing["event_record"]["native_input_id"].as_str() != Some(input)
            || existing["event_record"]["seq"].as_u64() != Some(event.seq)
            || existing["event_record"]["kind"].as_str() != Some("turn.completed")
            || existing["event_record"]["status"].as_str() != Some("completed")
            || event.sha256
                != digest_hex(
                    canonical_json(&existing["event_record"])
                        .map_err(|_| AdapterError::Checkpoint)?
                        .as_bytes(),
                )
        {
            return Err(AdapterError::Checkpoint);
        }
        Ok(())
    }

    /// Seal a newly read native `turn.completed` outcome before its first
    /// journal persistence. Replayed outcomes are handled by the exact-byte
    /// path and never receive a new EventRef.
    fn seal_goal_terminal_event(
        &mut self,
        outcome: &mut RuntimeOutcome,
    ) -> Result<(), AdapterError> {
        if outcome.outcome != EffectOutcome::Applied
            || outcome.details["completion_condition"] != "native_turn_completed"
            || outcome.details["execution_complete"] != true
            || outcome.details["task_completion"] != "unknown"
            || outcome.details["disposition"] != "completed"
            || outcome.details["native_input_readback"] != "verified"
            || outcome.details["native_turn_readback"] != "verified"
            || outcome.details["native_turn_status"] != "completed"
            || outcome.details["client_user_message_id"] != outcome.operation_id
        {
            return Ok(());
        }
        let (Some(root), Some(turn), Some(input)) = (
            outcome.native_root_id.as_deref(),
            outcome.turn_id.as_deref(),
            outcome.native_input_id.as_deref(),
        ) else {
            return Err(AdapterError::Checkpoint);
        };
        if root.is_empty() || turn.is_empty() || input.is_empty() {
            return Err(AdapterError::Checkpoint);
        }
        let continuation_admission = outcome
            .details
            .get("goal_continuation_admission")
            .filter(|value| !value.is_null())
            .map(|value| {
                let admission: GoalContinuationAdmissionReceipt =
                    serde_json::from_value(value.clone()).map_err(|_| AdapterError::Checkpoint)?;
                admission.validate().map_err(|_| AdapterError::Checkpoint)?;
                let module_receipt: ModuleReceiptIdentity =
                    serde_json::from_value(outcome.details["module_receipt"].clone())
                        .map_err(|_| AdapterError::Checkpoint)?;
                if admission.context.operation_id != outcome.operation_id
                    || admission.context.continuation.method != "agent.send"
                    || admission.module_receipt != module_receipt
                    || admission.native_input_id.as_deref() != Some(input)
                {
                    return Err(AdapterError::Checkpoint);
                }
                Ok(admission)
            })
            .transpose()?;
        if continuation_admission.is_some() && outcome.details.get("dispatch_admission").is_some() {
            return Err(AdapterError::Checkpoint);
        }
        if outcome.details.get("goal_terminal_event").is_some() {
            self.validate_existing_goal_terminal_event(outcome)?;
            return Ok(());
        }
        // A Goal terminal marker is eligible only for a real task dispatch
        // or a Store-authenticated Goal continuation admission. Ordinary
        // taskless agent.send operations retain their normal receipt without
        // manufactured Task identity.
        let dispatch_admission = outcome
            .details
            .get("dispatch_admission")
            .filter(|value| !value.is_null())
            .map(|value| {
                let admission: TaskDispatchAdmissionReceipt =
                    serde_json::from_value(value.clone()).map_err(|_| AdapterError::Checkpoint)?;
                admission.validate().map_err(|_| AdapterError::Checkpoint)?;
                if admission.operation_id != outcome.operation_id
                    || admission.native_input_id.as_deref() != outcome.native_input_id.as_deref()
                {
                    return Err(AdapterError::Checkpoint);
                }
                Ok(admission)
            })
            .transpose()?;
        if dispatch_admission.is_none() && continuation_admission.is_none() {
            return Ok(());
        }
        let (Some(scope), Some(provider), Some(model), Some(workspace)) = (
            outcome.native_scope_key.as_deref(),
            outcome.details["requested_model_provider"].as_str(),
            outcome.details["requested_model"].as_str(),
            self.state.workspace_root.as_deref(),
        ) else {
            return Err(AdapterError::Checkpoint);
        };
        if !self
            .state
            .is_current_root(root, scope, provider, model, workspace)
        {
            return Err(AdapterError::Checkpoint);
        }
        let sequence = self
            .state
            .observe_sequence
            .checked_add(1)
            .ok_or(AdapterError::Checkpoint)?;
        let event_id = format!("{}:{sequence}", self.state.boot_id);
        let event_record = json!({
            "id": event_id,
            "seq": sequence,
            "kind": "turn.completed",
            "operation_id": outcome.operation_id,
            "native_root_id": root,
            "turn_id": turn,
            "native_input_id": input,
            "status": "completed",
            "input_sha256": outcome.details["prompt_sha256"],
        });
        let sha256 = digest_hex(
            canonical_json(&event_record)
                .map_err(|_| AdapterError::Checkpoint)?
                .as_bytes(),
        );
        self.state.observe_sequence = sequence;
        outcome.details["goal_terminal_event"] = json!({
            "schema_id": swarm_contracts::module_contract::GOAL_TERMINAL_EVIDENCE_SCHEMA_ID,
            "schema_version": 1,
            "source": "codex",
            "reader_revision": "codex-turn-journal-v1",
            "event": {"id": event_id, "seq": sequence, "sha256": sha256},
            "event_record": event_record,
        });
        // `store_outcome` immediately follows this helper and writes the
        // outcome plus the advanced journal sequence in one snapshot.
        Ok(())
    }

    fn pending_outcomes(&self) -> Result<Vec<RuntimeOutcome>, AdapterError> {
        self.state
            .operations
            .iter()
            .filter(|(id, record)| {
                record.outcome.is_some() && !self.state.acknowledged_outcomes.contains(*id)
            })
            .filter_map(|(_, record)| record.outcome.as_ref())
            .take(MAX_PENDING_OUTCOMES_PER_BATCH)
            .map(|value| {
                serde_json::from_value(value.clone()).map_err(|_| AdapterError::Checkpoint)
            })
            .collect()
    }

    fn store_result_page(
        &mut self,
        operation_id: &str,
        input_sha256: &str,
        params: Value,
    ) -> Result<(), AdapterError> {
        let acknowledgement =
            result_page_acknowledgement(&params, operation_id, input_sha256, None)?;
        if let Some(tombstone) = self.read_operation_tombstone(operation_id)? {
            return if tombstone.method == "agent.result"
                && tombstone.kind == "result_page"
                && tombstone
                    .result_page_acknowledgement
                    .as_ref()
                    .is_some_and(|saved| {
                        saved.input_sha256 == acknowledgement.input_sha256
                            && saved.source == acknowledgement.source
                            && saved.offset_bytes == acknowledgement.offset_bytes
                            && saved.byte_length == acknowledgement.byte_length
                            && saved.total_bytes == acknowledgement.total_bytes
                            && saved.page_sha256 == acknowledgement.page_sha256
                    })
            {
                Ok(())
            } else {
                Err(AdapterError::Checkpoint)
            };
        }
        if let Some(record) = self.state.operations.get_mut(operation_id) {
            if record.method != "agent.result"
                || record.kind != "result_page"
                || record.input_sha256.as_deref() != Some(input_sha256)
                || record.outcome.is_some()
            {
                return Err(AdapterError::Checkpoint);
            }
            if let Some(saved) = record.pending_result_page.as_ref() {
                return if saved == &params {
                    Ok(())
                } else {
                    Err(AdapterError::Checkpoint)
                };
            }
            if record.result_page_acknowledgement.as_ref() == Some(&acknowledgement) {
                return Ok(());
            }
            record.pending_result_page = Some(params);
            record.result_page_acknowledgement = None;
            record.state = "result_page_pending".into();
        } else {
            let mut record = OperationRecord::intent("agent.result", "result_page");
            record.input_sha256 = Some(input_sha256.to_owned());
            record.pending_result_page = Some(params);
            record.state = "result_page_pending".into();
            self.state
                .operations
                .insert(operation_id.to_owned(), record);
        }
        self.save()
    }

    fn pending_result_pages(&self) -> Vec<(String, Value)> {
        self.state
            .operations
            .iter()
            .filter_map(|(operation_id, record)| {
                record
                    .pending_result_page
                    .as_ref()
                    .map(|params| (operation_id.clone(), params.clone()))
            })
            .take(MAX_PENDING_OUTCOMES_PER_BATCH)
            .collect()
    }

    fn acknowledge_result_page(
        &mut self,
        operation_id: &str,
        response: &Value,
    ) -> Result<(), AdapterError> {
        let Some(record) = self.state.operations.get(operation_id).cloned() else {
            return if self
                .read_operation_tombstone(operation_id)?
                .is_some_and(|tombstone| tombstone.result_page_acknowledgement.is_some())
            {
                Ok(())
            } else {
                Err(AdapterError::Checkpoint)
            };
        };
        let params = record
            .pending_result_page
            .as_ref()
            .ok_or(AdapterError::Checkpoint)?;
        let input_sha256 = record
            .input_sha256
            .as_deref()
            .ok_or(AdapterError::Checkpoint)?;
        if response["recorded"] != true {
            return Err(AdapterError::HostProtocol);
        }
        let artifact_ref = response["artifact_ref"]
            .as_str()
            .filter(|value| !value.trim().is_empty())
            .ok_or(AdapterError::HostProtocol)?;
        let acknowledgement =
            result_page_acknowledgement(params, operation_id, input_sha256, Some(artifact_ref))?;
        acknowledgement.validate(operation_id)?;
        let current = self
            .state
            .operations
            .get_mut(operation_id)
            .ok_or(AdapterError::Checkpoint)?;
        current.pending_result_page = None;
        current.result_page_acknowledgement = Some(acknowledgement);
        current.state = "result_page_acknowledged".into();
        self.save()?;

        let record = self
            .state
            .operations
            .get(operation_id)
            .cloned()
            .ok_or(AdapterError::Checkpoint)?;
        self.write_result_page_tombstone(operation_id, &record)?;
        self.state.operations.remove(operation_id);
        self.state.acknowledged_outcomes.remove(operation_id);
        self.save()
    }

    fn acknowledge_outcome(&mut self, operation_id: &str) -> Result<(), AdapterError> {
        let Some(record) = self.state.operations.get(operation_id).cloned() else {
            return if self.read_operation_tombstone(operation_id)?.is_some() {
                Ok(())
            } else {
                Err(AdapterError::Checkpoint)
            };
        };
        if is_terminal_record(&record, operation_id)? {
            // `module.outcome` has returned only after the Store accepted the
            // result. Publish the durable dedupe marker before removing the
            // record from the current full snapshot.
            self.write_operation_tombstone(operation_id, &record)?;
            self.state.operations.remove(operation_id);
            self.state.acknowledged_outcomes.remove(operation_id);
        } else {
            if self.state.acknowledged_outcomes.contains(operation_id) {
                return Ok(());
            }
            self.state
                .acknowledged_outcomes
                .insert(operation_id.to_owned());
        }
        self.save()
    }

    fn next_observation(
        &mut self,
        ready: bool,
        normalized_results: bool,
    ) -> Result<Value, AdapterError> {
        if let Some(pending) = &self.state.pending_observation {
            return Ok(pending.clone());
        }
        self.state.observe_sequence = self
            .state
            .observe_sequence
            .checked_add(1)
            .ok_or(AdapterError::Checkpoint)?;
        let mut supported = vec![
            "agent.open",
            "task.dispatch",
            "agent.send",
            "agent.reconcile",
        ];
        let mut unsupported = vec![
            "agent.recover",
            "agent.refresh",
            "agent.result",
            "agent.reply",
            "agent.configure",
            "agent.goal",
            "agent.background",
            "tools",
            "family_enumeration",
            "task_completion",
        ];
        if normalized_results {
            supported.push("agent.result");
            unsupported.retain(|capability| *capability != "agent.result");
        }
        let state = json!({
            "module_artifact_id": ARTIFACT_ID,
            "boot_id": self.state.boot_id.as_str(),
            "native_usage": self.state.native_usage,
            "native_root_id": self.active_root_id(),
            "native": {
                "root_id": self.active_root_id(),
                "candidate_root_id": if self.state.native_root_phase == Some(NativeRootPhase::Candidate) { self.state.native_root_id.as_deref() } else { None },
                "root_phase": self.state.native_root_phase,
                "root_operation_id": self.state.root_operation_id.as_deref(),
                "scope_key": if self.active_root_id().is_some() { self.state.native_scope_key.as_deref() } else { None },
                "candidate_scope_key": if self.state.native_root_phase.is_some() { self.state.native_scope_key.as_deref() } else { None },
                "ready": ready,
                "events": self.state.pending_native_events.iter().filter(|record| !matches!(&record.event,
                    NativeRpcEvent::AccountRateLimitsUpdated { .. } | NativeRpcEvent::NativeUsageChanged
                    | NativeRpcEvent::MalformedUsageNotification { .. }) && !matches!(&record.event,
                    NativeRpcEvent::NativeNotification { method, .. } if method.starts_with("account/"))).collect::<Vec<_>>(),
            },
            "describe": {
                "module_artifact_id": ARTIFACT_ID,
                "requested_model_provider": self.state.requested_model_provider.as_deref(),
                "requested_model": self.state.requested_model.as_deref(),
                "effective_model_provider": self.state.effective_model_provider.as_deref(),
                "effective_model": self.state.effective_model.as_deref(),
                "served_model_status": "unknown",
                "billing_status": "unknown",
                "capabilities": {
                    "supported": supported,
                    "unsupported": unsupported
                }
            }
        });
        let pending = json!({
            "event_id": format!("{}:{}", self.state.boot_id, self.state.observe_sequence),
            "sequence": self.state.observe_sequence,
            "state": state,
        });
        self.state.pending_observation = Some(pending.clone());
        self.save()?;
        Ok(pending)
    }

    fn acknowledge_observation(&mut self) -> Result<(), AdapterError> {
        self.state.pending_observation = None;
        self.state.pending_native_events.clear();
        self.save()
    }

    fn active_root_id(&self) -> Option<&str> {
        let (Some(root), Some(scope), Some(provider), Some(model), Some(workspace)) = (
            self.state.native_root_id.as_deref(),
            self.state.native_scope_key.as_deref(),
            self.state.requested_model_provider.as_deref(),
            self.state.requested_model.as_deref(),
            self.state.workspace_root.as_deref(),
        ) else {
            return None;
        };
        self.state
            .is_current_root(root, scope, provider, model, workspace)
            .then_some(root)
    }
}

fn is_terminal_record(record: &OperationRecord, operation_id: &str) -> Result<bool, AdapterError> {
    let Some(value) = record.outcome.as_ref() else {
        return Ok(false);
    };
    let outcome: RuntimeOutcome =
        serde_json::from_value(value.clone()).map_err(|_| AdapterError::Checkpoint)?;
    if outcome.operation_id != operation_id {
        return Err(AdapterError::Checkpoint);
    }
    Ok(matches!(
        outcome.outcome,
        EffectOutcome::Applied | EffectOutcome::Rejected
    ))
}

fn checked_directory_exists(path: &Path) -> Result<bool, AdapterError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => Ok(true),
        Ok(_) => Err(AdapterError::Checkpoint),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(_) => Err(AdapterError::Checkpoint),
    }
}

fn ensure_directory(path: &Path) -> Result<(), AdapterError> {
    if !checked_directory_exists(path)? {
        match fs::create_dir(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(_) => return Err(AdapterError::Checkpoint),
        }
    }
    if !checked_directory_exists(path)? {
        return Err(AdapterError::Checkpoint);
    }
    Ok(())
}

fn read_bounded_file(path: &Path, maximum_bytes: u64) -> Result<Vec<u8>, AdapterError> {
    let metadata = fs::symlink_metadata(path).map_err(|_| AdapterError::Checkpoint)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() > maximum_bytes {
        return Err(AdapterError::Checkpoint);
    }
    let file = File::open(path).map_err(|_| AdapterError::Checkpoint)?;
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(maximum_bytes.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|_| AdapterError::Checkpoint)?;
    if bytes.len() as u64 > maximum_bytes {
        return Err(AdapterError::Checkpoint);
    }
    Ok(bytes)
}

#[derive(Debug, Clone)]
enum NativeError {
    Attach,
    EndpointInvalid,
    CredentialUnavailable,
    IdentityUnverified,
    HttpRejected(u16),
    Transport,
    Protocol,
    DemultiplexerCapacity,
    PendingRequestLimit,
    Rejected { rpc_code: Option<i64> },
}

impl NativeError {
    fn diagnostic_code(&self) -> &'static str {
        match self {
            Self::Attach => "NATIVE_ATTACH_UNAVAILABLE",
            Self::EndpointInvalid => "NATIVE_ENDPOINT_INVALID",
            Self::CredentialUnavailable => "NATIVE_AUTH_CREDENTIAL_UNAVAILABLE",
            Self::IdentityUnverified => "NATIVE_SERVER_IDENTITY_UNVERIFIED",
            Self::HttpRejected(401 | 403) => "NATIVE_AUTH_OR_ACCESS_UNAVAILABLE",
            Self::HttpRejected(429) => "NATIVE_UPSTREAM_LIMITED",
            Self::HttpRejected(404) => "NATIVE_ENDPOINT_UNAVAILABLE",
            Self::HttpRejected(_) => "NATIVE_WEBSOCKET_HANDSHAKE_REJECTED",
            Self::Transport => "NATIVE_TRANSPORT_UNAVAILABLE",
            Self::Protocol => "NATIVE_PROTOCOL_INVALID",
            Self::DemultiplexerCapacity => "NATIVE_EVENT_BUFFER_LIMIT",
            Self::PendingRequestLimit => "NATIVE_RPC_PENDING_LIMIT",
            Self::Rejected {
                rpc_code: Some(-32602),
            } => "NATIVE_RPC_INVALID_PARAMS",
            Self::Rejected { .. } => "NATIVE_RPC_REQUEST_REJECTED",
        }
    }

    fn rpc_code(&self) -> Option<i64> {
        match self {
            Self::Rejected { rpc_code } => *rpc_code,
            _ => None,
        }
    }

    fn http_status(&self) -> Option<u16> {
        match self {
            Self::HttpRejected(status) => Some(*status),
            _ => None,
        }
    }
}

fn turn_error_diagnostic(turn: &Value) -> &'static str {
    let info = &turn["error"]["codexErrorInfo"];
    if let Some(code) = info.as_str() {
        return match code {
            "unauthorized" => "NATIVE_AUTH_UNAVAILABLE",
            "usageLimitExceeded" => "NATIVE_USAGE_LIMIT_EXCEEDED",
            "rateLimitExceeded" => "NATIVE_RATE_LIMIT_EXCEEDED",
            "serverOverloaded" => "NATIVE_PROVIDER_OVERLOADED",
            "badRequest" => "NATIVE_PROVIDER_REQUEST_REJECTED",
            "internalServerError" => "NATIVE_PROVIDER_FAILED",
            "contextWindowExceeded" => "NATIVE_CONTEXT_LIMIT_EXCEEDED",
            "sessionBudgetExceeded" => "NATIVE_SESSION_BUDGET_EXCEEDED",
            "flexUnavailable" => "NATIVE_PROVIDER_CAPACITY_UNAVAILABLE",
            _ => "NATIVE_TURN_FAILED",
        };
    }
    let Some(object) = info.as_object() else {
        return "NATIVE_TURN_FAILED";
    };
    for key in [
        "httpConnectionFailed",
        "responseStreamConnectionFailed",
        "responseStreamDisconnected",
        "responseTooManyFailedAttempts",
        "activeTurnNotSteerable",
    ] {
        if let Some(status) = object
            .get(key)
            .and_then(|value| value.get("httpStatusCode"))
            .and_then(Value::as_u64)
        {
            return match status {
                401 | 403 => "NATIVE_AUTH_OR_ACCESS_UNAVAILABLE",
                404 => "NATIVE_UPSTREAM_ENDPOINT_UNAVAILABLE",
                429 => "NATIVE_RATE_LIMIT_EXCEEDED",
                500..=599 => "NATIVE_PROVIDER_UNAVAILABLE",
                _ => "NATIVE_PROVIDER_REQUEST_FAILED",
            };
        }
        if object.contains_key(key) {
            return match key {
                "activeTurnNotSteerable" => "NATIVE_TURN_NOT_STEERABLE",
                "responseStreamDisconnected" => "NATIVE_PROVIDER_STREAM_INTERRUPTED",
                "responseTooManyFailedAttempts" => "NATIVE_PROVIDER_RETRIES_EXHAUSTED",
                _ => "NATIVE_PROVIDER_CONNECTION_FAILED",
            };
        }
    }
    "NATIVE_TURN_FAILED"
}

fn turn_error_http_status(turn: &Value) -> Option<u64> {
    let info = &turn["error"]["codexErrorInfo"];
    [
        "httpConnectionFailed",
        "responseStreamConnectionFailed",
        "responseStreamDisconnected",
        "responseTooManyFailedAttempts",
    ]
    .into_iter()
    .filter_map(|key| {
        info.get(key)
            .and_then(|value| value.get("httpStatusCode"))
            .and_then(Value::as_u64)
    })
    .find(|status| (100..=599).contains(status))
}

type NativeSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;
type NativeWriter = Arc<AsyncMutex<SplitSink<NativeSocket, Message>>>;

struct NativeClient {
    writer: Option<NativeWriter>,
    demux: Arc<NativeRpcDemultiplexer>,
    events: NativeEventSink,
    reader_task: Option<tokio::task::JoinHandle<()>>,
    endpoint_identity: String,
    server_name: String,
    server_version: String,
}

struct NativeRpcDemultiplexer {
    state: Mutex<NativeRpcState>,
    events: NativeEventSink,
}

#[derive(Default)]
struct NativeRpcState {
    pending_responses: HashMap<String, Arc<Mutex<NativeResponseSlot>>>,
    terminal_error: Option<NativeError>,
}

#[derive(Default)]
struct NativeResponseSlot {
    response: Option<Result<Value, NativeError>>,
    waker: Option<Waker>,
}

struct NativeResponseRegistration {
    demux: Arc<NativeRpcDemultiplexer>,
    key: String,
    slot: Arc<Mutex<NativeResponseSlot>>,
}

impl Drop for NativeResponseRegistration {
    fn drop(&mut self) {
        if let Ok(mut state) = self.demux.state.lock()
            && state
                .pending_responses
                .get(&self.key)
                .is_some_and(|slot| Arc::ptr_eq(slot, &self.slot))
        {
            state.pending_responses.remove(&self.key);
        }
    }
}

impl NativeRpcDemultiplexer {
    fn new(events: NativeEventSink) -> Self {
        Self {
            state: Mutex::new(NativeRpcState::default()),
            events,
        }
    }

    fn event_for_notification(method: String, params: Value) -> NativeRpcEvent {
        match method.as_str() {
            "account/rateLimits/updated" => {
                match serde_json::from_value::<NativeRateLimitNotification>(params.clone()) {
                    Ok(notification) => NativeRpcEvent::AccountRateLimitsUpdated {
                        method,
                        rate_limits: notification.rate_limits,
                        raw_params: params,
                    },
                    Err(_) => NativeRpcEvent::MalformedUsageNotification { method, params },
                }
            }
            "thread/tokenUsage/updated" => {
                match serde_json::from_value::<NativeThreadTokenUsage>(params.clone()) {
                    Ok(usage) => NativeRpcEvent::ThreadTokenUsageUpdated {
                        method,
                        usage,
                        raw_params: params,
                    },
                    Err(_) => NativeRpcEvent::MalformedUsageNotification { method, params },
                }
            }
            _ if method.starts_with("thread/")
                || method.starts_with("turn/")
                || method.starts_with("item/") =>
            {
                NativeRpcEvent::CurrentNotification { method, params }
            }
            _ => NativeRpcEvent::NativeNotification { method, params },
        }
    }

    fn register(self: &Arc<Self>, key: String) -> Result<NativeResponseRegistration, NativeError> {
        let mut state = self.state.lock().map_err(|_| NativeError::Protocol)?;
        if let Some(error) = &state.terminal_error {
            return Err(error.clone());
        }
        if state.pending_responses.len() >= MAX_PENDING_NATIVE_RPC
            || state.pending_responses.contains_key(&key)
        {
            return Err(NativeError::PendingRequestLimit);
        }
        let slot = Arc::new(Mutex::new(NativeResponseSlot::default()));
        state
            .pending_responses
            .insert(key.clone(), Arc::clone(&slot));
        Ok(NativeResponseRegistration {
            demux: Arc::clone(self),
            key,
            slot,
        })
    }

    fn terminal_error(&self) -> Result<Option<NativeError>, NativeError> {
        self.state
            .lock()
            .map(|state| state.terminal_error.clone())
            .map_err(|_| NativeError::Protocol)
    }

    fn fail(&self, error: NativeError) {
        let pending = match self.state.lock() {
            Ok(mut state) => {
                if state.terminal_error.is_some() {
                    return;
                }
                state.terminal_error = Some(error.clone());
                std::mem::take(&mut state.pending_responses)
            }
            Err(_) => {
                self.events.record_fault("NATIVE_RPC_DEMUX_POISONED");
                return;
            }
        };
        for slot in pending.into_values() {
            if let Ok(mut response) = slot.lock() {
                response.response = Some(Err(error.clone()));
                if let Some(waker) = response.waker.take() {
                    waker.wake();
                }
            }
        }
        self.events.record_fault(error.diagnostic_code());
    }

    fn queue_event(&self, event: NativeRpcEvent) -> Result<(), NativeError> {
        self.events.queue_event(event)
    }

    fn route_response(&self, packet: Value) -> Result<(), NativeError> {
        let id = packet.get("id").ok_or(NativeError::Protocol)?;
        let key = Self::response_key(id)?;
        let slot = {
            let mut state = self.state.lock().map_err(|_| NativeError::Protocol)?;
            if let Some(error) = &state.terminal_error {
                return Err(error.clone());
            }
            state
                .pending_responses
                .remove(&key)
                .ok_or(NativeError::Protocol)?
        };
        let response = Self::response(packet);
        let waker = {
            let mut slot = slot.lock().map_err(|_| NativeError::Protocol)?;
            slot.response = Some(response);
            slot.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        Ok(())
    }

    fn response_key(id: &Value) -> Result<String, NativeError> {
        serde_json::to_string(id).map_err(|_| NativeError::Protocol)
    }

    fn response(packet: Value) -> Result<Value, NativeError> {
        if let Some(error) = packet.get("error") {
            return Err(NativeError::Rejected {
                rpc_code: error.get("code").and_then(Value::as_i64),
            });
        }
        packet.get("result").cloned().ok_or(NativeError::Protocol)
    }
}

impl NativeClient {
    async fn attach(
        endpoint: &str,
        bearer: Option<&str>,
        credential_unavailable: bool,
        events: NativeEventSink,
    ) -> Result<Self, NativeError> {
        if credential_unavailable {
            return Err(NativeError::CredentialUnavailable);
        }
        let parsed = Url::parse(endpoint).map_err(|_| NativeError::EndpointInvalid)?;
        if !matches!(parsed.scheme(), "ws" | "wss") || parsed.host_str().is_none() {
            return Err(NativeError::EndpointInvalid);
        }
        if !parsed.username().is_empty() || parsed.password().is_some() {
            return Err(NativeError::EndpointInvalid);
        }
        let mut safe = parsed.clone();
        let _ = safe.set_username("");
        let _ = safe.set_password(None);
        safe.set_query(None);
        safe.set_fragment(None);
        let endpoint_identity = digest_hex(safe.as_str().as_bytes());
        let mut request = endpoint
            .into_client_request()
            .map_err(|_| NativeError::EndpointInvalid)?;
        if let Some(token) = bearer {
            let value = format!("Bearer {token}")
                .parse()
                .map_err(|_| NativeError::Attach)?;
            request.headers_mut().insert(AUTHORIZATION, value);
        }
        let config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
            .max_message_size(Some(MAX_FRAME_BYTES))
            .max_frame_size(Some(MAX_FRAME_BYTES));
        let connection = timeout(
            CONNECT_TIMEOUT,
            connect_async_with_config(request, Some(config), false),
        )
        .await
        .map_err(|_| NativeError::Attach)?
        .map_err(|error| match error {
            tokio_tungstenite::tungstenite::Error::Http(response) => {
                NativeError::HttpRejected(response.status().as_u16())
            }
            _ => NativeError::Attach,
        })?;
        let (socket, _) = connection;
        let (writer, reader) = socket.split();
        let writer = Arc::new(AsyncMutex::new(writer));
        let demux = Arc::new(NativeRpcDemultiplexer::new(events.clone()));
        let reader_task = tokio::spawn(read_native_messages(
            reader,
            Arc::clone(&writer),
            Arc::clone(&demux),
        ));
        let mut client = Self {
            writer: Some(writer),
            demux,
            events,
            reader_task: Some(reader_task),
            endpoint_identity,
            server_name: "unknown-server".into(),
            server_version: "unknown-version".into(),
        };
        let initialized = client
            .request(
                "initialize",
                json!({
                    "clientInfo": {
                        "name": "eliot-swarm-codex-controller",
                        "title": "ELIOT Swarm Codex Controller",
                        "version": env!("CARGO_PKG_VERSION")
                    },
                    "capabilities": {"experimentalApi": false}
                }),
            )
            .await?;
        client.server_name = initialized["serverInfo"]["name"]
            .as_str()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or("unknown-server")
            .to_owned();
        client.server_version = initialized["serverInfo"]["version"]
            .as_str()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or("unknown-version")
            .to_owned();
        client
            .events
            .set_scope(client.scope_key())
            .map_err(|_| NativeError::Protocol)?;
        client.notify("initialized", None).await?;
        // Optional subscription reads use this same authenticated connection.
        // Unsupported reads do not prevent ordinary native session work.
        let _ = client
            .request("account/read", json!({"refreshToken":false}))
            .await;
        let before = client
            .events
            .usage
            .lock()
            .map_err(|_| NativeError::Protocol)?
            .revision();
        let limits = client
            .request("account/rateLimits/read", json!({}))
            .await
            .map_err(|error| error.rpc_code() == Some(-32601));
        client
            .events
            .usage
            .lock()
            .map_err(|_| NativeError::Protocol)?
            .full_read(before, limits);
        client
            .events
            .queue_event(NativeRpcEvent::NativeUsageChanged)?;
        Ok(client)
    }

    fn unavailable(error: NativeError, events: NativeEventSink) -> Self {
        let demux = Arc::new(NativeRpcDemultiplexer::new(events.clone()));
        demux.fail(error);
        Self {
            writer: None,
            demux,
            events,
            reader_task: None,
            endpoint_identity: String::from("unavailable"),
            server_name: String::from("unknown-server"),
            server_version: String::from("unknown-version"),
        }
    }

    async fn refresh_usage_after_auth_change(&mut self) -> Result<(), NativeError> {
        let marker = {
            let mut collector = self
                .events
                .usage
                .lock()
                .map_err(|_| NativeError::Protocol)?;
            collector.take_refresh_revision()
        };
        let Some(marker) = marker else {
            return Ok(());
        };
        let result = self
            .request("account/rateLimits/read", json!({}))
            .await
            .map_err(|error| error.rpc_code() == Some(-32601));
        self.events
            .usage
            .lock()
            .map_err(|_| NativeError::Protocol)?
            .full_read(marker, result);
        self.events.queue_event(NativeRpcEvent::NativeUsageChanged)
    }

    fn scope_key(&self) -> String {
        format!(
            "codex-appserver:{}:{}:{}",
            self.endpoint_identity, self.server_name, self.server_version
        )
    }

    fn identity_is_known(&self) -> bool {
        self.server_name != "unknown-server" && self.server_version != "unknown-version"
    }

    async fn notify(&self, method: &str, params: Option<Value>) -> Result<(), NativeError> {
        let writer = self.writer.as_ref().ok_or_else(|| {
            self.demux
                .terminal_error()
                .ok()
                .flatten()
                .unwrap_or(NativeError::Transport)
        })?;
        if let Some(error) = self.demux.terminal_error()? {
            return Err(error);
        }
        let mut packet = json!({"method":method});
        if let Some(params) = params {
            packet["params"] = params;
        }
        match timeout(NATIVE_TIMEOUT, send_native_packet(writer, packet)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => {
                self.demux.fail(error.clone());
                Err(error)
            }
            Err(_) => {
                self.demux.fail(NativeError::Transport);
                Err(NativeError::Transport)
            }
        }
    }

    async fn request(&self, method: &str, params: Value) -> Result<Value, NativeError> {
        if !matches!(
            method,
            "initialize"
                | "thread/start"
                | "thread/read"
                | "thread/items/list"
                | "thread/turns/list"
                | "turn/start"
                | "turn/steer"
        ) {
            return Err(NativeError::Protocol);
        }
        let id = Uuid::new_v4().to_string();
        let key = NativeRpcDemultiplexer::response_key(&json!(id))?;
        let registration = self.demux.register(key.clone())?;
        let frame = serde_json::to_string(&json!({
            "id":id,
            "method":method,
            "params":params,
        }))
        .map_err(|_| NativeError::Protocol)?;
        if frame.len() > MAX_FRAME_BYTES {
            return Err(NativeError::Protocol);
        }
        let writer = self.writer.as_ref().ok_or(NativeError::Transport)?;
        match timeout(
            NATIVE_TIMEOUT,
            send_native_frame(writer, Message::Text(frame.into())),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                self.demux.fail(error.clone());
                return Err(error);
            }
            Err(_) => {
                self.demux.fail(NativeError::Transport);
                return Err(NativeError::Transport);
            }
        }
        let slot = Arc::clone(&registration.slot);
        let response = timeout(
            NATIVE_TIMEOUT,
            poll_fn(move |context| {
                let Ok(mut slot) = slot.lock() else {
                    return Poll::Ready(Err(NativeError::Protocol));
                };
                if let Some(response) = slot.response.take() {
                    Poll::Ready(response)
                } else {
                    slot.waker = Some(context.waker().clone());
                    Poll::Pending
                }
            }),
        )
        .await;
        match response {
            Ok(response) => response,
            Err(_) => {
                self.demux.fail(NativeError::Transport);
                Err(NativeError::Transport)
            }
        }
    }
}

impl Drop for NativeClient {
    fn drop(&mut self) {
        if let Some(reader_task) = self.reader_task.take() {
            reader_task.abort();
        }
    }
}

async fn send_native_packet(writer: &NativeWriter, packet: Value) -> Result<(), NativeError> {
    let frame = serde_json::to_string(&packet).map_err(|_| NativeError::Protocol)?;
    if frame.len() > MAX_FRAME_BYTES {
        return Err(NativeError::Protocol);
    }
    send_native_frame(writer, Message::Text(frame.into())).await
}

async fn send_native_frame(writer: &NativeWriter, message: Message) -> Result<(), NativeError> {
    writer
        .lock()
        .await
        .send(message)
        .await
        .map_err(|_| NativeError::Transport)
}

async fn read_native_messages(
    mut reader: SplitStream<NativeSocket>,
    writer: NativeWriter,
    demux: Arc<NativeRpcDemultiplexer>,
) {
    loop {
        let message = match reader.next().await {
            Some(Ok(message)) => message,
            Some(Err(_)) | None => {
                demux.fail(NativeError::Transport);
                return;
            }
        };
        match message {
            Message::Text(text) => {
                if text.len() > MAX_FRAME_BYTES {
                    demux.fail(NativeError::Protocol);
                    return;
                }
                let packet = match serde_json::from_str::<Value>(text.as_str()) {
                    Ok(packet) => packet,
                    Err(_) => {
                        demux.fail(NativeError::Protocol);
                        return;
                    }
                };
                if let Some(method) = packet.get("method").and_then(Value::as_str) {
                    let params = packet.get("params").cloned().unwrap_or(Value::Null);
                    if let Some(id) = packet.get("id").cloned() {
                        let reply = match decline_server_request(method, &params) {
                            Some(result) => json!({"id":id.clone(),"result":result}),
                            None => json!({
                                "id":id.clone(),
                                "error":{"code":-32601,"message":"unsupported server request"}
                            }),
                        };
                        if demux
                            .queue_event(NativeRpcEvent::ServerRequest {
                                id,
                                method: method.to_owned(),
                                params,
                                reply: reply.clone(),
                            })
                            .is_err()
                        {
                            demux.fail(NativeError::DemultiplexerCapacity);
                            return;
                        }
                        if let Err(error) = send_native_packet(&writer, reply).await {
                            demux.fail(error);
                            return;
                        }
                    } else if demux
                        .queue_event(NativeRpcDemultiplexer::event_for_notification(
                            method.to_owned(),
                            params,
                        ))
                        .is_err()
                    {
                        demux.fail(NativeError::DemultiplexerCapacity);
                        return;
                    }
                    continue;
                }
                if demux.route_response(packet).is_err() {
                    demux.fail(NativeError::Protocol);
                    return;
                }
            }
            Message::Ping(payload) => {
                if send_native_frame(&writer, Message::Pong(payload))
                    .await
                    .is_err()
                {
                    demux.fail(NativeError::Transport);
                    return;
                }
            }
            Message::Pong(_) => {}
            Message::Close(_) | Message::Binary(_) | Message::Frame(_) => {
                demux.fail(NativeError::Protocol);
                return;
            }
        }
    }
}

fn decline_server_request(method: &str, params: &Value) -> Option<Value> {
    match method {
        "item/commandExecution/requestApproval" | "item/fileChange/requestApproval" => {
            Some(json!({"decision":"decline"}))
        }
        "applyPatchApproval" | "execCommandApproval" => Some(json!({"decision":"denied"})),
        "item/permissions/requestApproval" => Some(json!({"permissions":{},"scope":"turn"})),
        "item/tool/call" => Some(json!({"contentItems":[],"success":false})),
        "item/tool/requestUserInput" => {
            let mut answers = serde_json::Map::new();
            if let Some(questions) = params.get("questions").and_then(Value::as_array) {
                for question in questions {
                    if let Some(id) = question.get("id").and_then(Value::as_str) {
                        answers.insert(id.to_owned(), json!({"answers":[]}));
                    }
                }
            }
            Some(json!({"answers":answers}))
        }
        "mcpServer/elicitation/request" => Some(json!({"action":"decline"})),
        _ => None,
    }
}

#[derive(Clone)]
struct HistoryMatch {
    item_id: String,
    turn_id: String,
    text: String,
}

enum HistoryRead {
    Complete(Vec<HistoryMatch>),
    Truncated(Vec<HistoryMatch>),
    Failed(NativeError),
}

enum TurnRead {
    Found(Value),
    Missing,
    Truncated,
    Failed(NativeError),
}

struct AssistantResponse {
    item_id: String,
    text: String,
}

enum AssistantRead {
    Found(AssistantResponse),
    Missing,
    Truncated,
    Failed(NativeError),
}

impl NativeClient {
    async fn read_thread(&mut self, thread_id: &str) -> Result<Value, NativeError> {
        self.request(
            "thread/read",
            json!({"threadId":thread_id,"includeTurns":false}),
        )
        .await
    }

    async fn read_history(&mut self, thread_id: &str, client_id: &str) -> HistoryRead {
        let mut cursor: Option<String> = None;
        let mut seen = HashSet::new();
        let mut matches = Vec::new();
        for _ in 0..MAX_HISTORY_PAGES {
            let mut params = json!({
                "threadId":thread_id,
                "limit":PAGE_SIZE,
                "sortDirection":"desc",
            });
            if let Some(value) = cursor.as_ref() {
                params["cursor"] = json!(value);
            }
            let response = match self.request("thread/items/list", params).await {
                Ok(response) => response,
                Err(error) => return HistoryRead::Failed(error),
            };
            let Some(data) = response.get("data").and_then(Value::as_array) else {
                return HistoryRead::Failed(NativeError::Protocol);
            };
            for entry in data {
                let item = &entry["item"];
                if item["type"] != "userMessage" || item["clientId"] != client_id {
                    continue;
                }
                let Some(item_id) = item["id"].as_str().filter(|s| !s.is_empty()) else {
                    return HistoryRead::Failed(NativeError::Protocol);
                };
                let Some(turn_id) = entry["turnId"].as_str().filter(|s| !s.is_empty()) else {
                    return HistoryRead::Failed(NativeError::Protocol);
                };
                let Some(parts) = item["content"].as_array() else {
                    return HistoryRead::Failed(NativeError::Protocol);
                };
                let mut text = String::new();
                for part in parts {
                    if part["type"] != "text" {
                        return HistoryRead::Failed(NativeError::Protocol);
                    }
                    let Some(part_text) = part["text"].as_str() else {
                        return HistoryRead::Failed(NativeError::Protocol);
                    };
                    text.push_str(part_text);
                }
                matches.push(HistoryMatch {
                    item_id: item_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    text,
                });
            }
            match response.get("nextCursor") {
                None | Some(Value::Null) => return HistoryRead::Complete(matches),
                Some(Value::String(next)) if next.is_empty() => {
                    return HistoryRead::Complete(matches);
                }
                Some(Value::String(next)) if !seen.insert(next.clone()) => {
                    return HistoryRead::Truncated(matches);
                }
                Some(Value::String(next)) => cursor = Some(next.clone()),
                Some(_) => return HistoryRead::Failed(NativeError::Protocol),
            }
        }
        HistoryRead::Truncated(matches)
    }

    async fn read_turn(&mut self, thread_id: &str, turn_id: &str) -> TurnRead {
        let mut cursor: Option<String> = None;
        let mut seen = HashSet::new();
        let mut found = None;
        for _ in 0..MAX_HISTORY_PAGES {
            let mut params = json!({
                "threadId":thread_id,
                "limit":PAGE_SIZE,
                "sortDirection":"desc",
            });
            if let Some(value) = cursor.as_ref() {
                params["cursor"] = json!(value);
            }
            let response = match self.request("thread/turns/list", params).await {
                Ok(response) => response,
                Err(error) => return TurnRead::Failed(error),
            };
            let Some(data) = response.get("data").and_then(Value::as_array) else {
                return TurnRead::Failed(NativeError::Protocol);
            };
            for turn in data {
                let Some(id) = turn
                    .get("id")
                    .and_then(Value::as_str)
                    .filter(|id| !id.is_empty())
                else {
                    return TurnRead::Failed(NativeError::Protocol);
                };
                if turn.get("status").and_then(Value::as_str).is_none() {
                    return TurnRead::Failed(NativeError::Protocol);
                }
                if id == turn_id {
                    if found.is_some() {
                        return TurnRead::Failed(NativeError::Protocol);
                    }
                    found = Some(turn.clone());
                }
            }
            match response.get("nextCursor") {
                None | Some(Value::Null) => {
                    return found.map_or(TurnRead::Missing, TurnRead::Found);
                }
                Some(Value::String(next)) if next.is_empty() => {
                    return found.map_or(TurnRead::Missing, TurnRead::Found);
                }
                Some(Value::String(next)) if !seen.insert(next.clone()) => {
                    return TurnRead::Truncated;
                }
                Some(Value::String(next)) => cursor = Some(next.clone()),
                Some(_) => return TurnRead::Failed(NativeError::Protocol),
            }
        }
        TurnRead::Truncated
    }

    async fn read_final_assistant_response(
        &mut self,
        thread_id: &str,
        turn_id: &str,
    ) -> AssistantRead {
        let mut cursor: Option<String> = None;
        let mut seen_cursors = HashSet::new();
        let mut seen_item_ids = HashSet::new();
        let mut last_final = None;
        let mut last_phase_less = None;
        for _ in 0..MAX_HISTORY_PAGES {
            let mut params = json!({
                "threadId":thread_id,
                "turnId":turn_id,
                "limit":PAGE_SIZE,
                "sortDirection":"asc",
            });
            if let Some(value) = cursor.as_ref() {
                params["cursor"] = json!(value);
            }
            let response = match self.request("thread/items/list", params).await {
                Ok(response) => response,
                Err(error) => return AssistantRead::Failed(error),
            };
            let Some(data) = response.get("data").and_then(Value::as_array) else {
                return AssistantRead::Failed(NativeError::Protocol);
            };
            for entry in data {
                if entry["turnId"].as_str() != Some(turn_id) {
                    return AssistantRead::Failed(NativeError::Protocol);
                }
                let item = &entry["item"];
                if item["type"] != "agentMessage" {
                    continue;
                }
                let Some(item_id) = item["id"].as_str().filter(|value| !value.is_empty()) else {
                    return AssistantRead::Failed(NativeError::Protocol);
                };
                if !seen_item_ids.insert(item_id.to_owned()) {
                    return AssistantRead::Failed(NativeError::Protocol);
                }
                let Some(text) = item["text"].as_str() else {
                    return AssistantRead::Failed(NativeError::Protocol);
                };
                if text.len() > MAX_NORMALIZED_RESULT_BODY_BYTES {
                    return AssistantRead::Truncated;
                }
                let selected = AssistantResponse {
                    item_id: item_id.to_owned(),
                    text: text.to_owned(),
                };
                match item.get("phase") {
                    Some(Value::String(phase)) if phase == "final_answer" => {
                        last_final = Some(selected);
                    }
                    Some(Value::String(phase)) if phase == "commentary" => {}
                    None | Some(Value::Null) => last_phase_less = Some(selected),
                    Some(_) => return AssistantRead::Failed(NativeError::Protocol),
                }
            }
            match response.get("nextCursor") {
                None | Some(Value::Null) => {
                    return last_final
                        .or(last_phase_less)
                        .map_or(AssistantRead::Missing, AssistantRead::Found);
                }
                Some(Value::String(next)) if next.is_empty() => {
                    return last_final
                        .or(last_phase_less)
                        .map_or(AssistantRead::Missing, AssistantRead::Found);
                }
                Some(Value::String(next)) if !seen_cursors.insert(next.clone()) => {
                    return AssistantRead::Truncated;
                }
                Some(Value::String(next)) => cursor = Some(next.clone()),
                Some(_) => return AssistantRead::Failed(NativeError::Protocol),
            }
        }
        AssistantRead::Truncated
    }
}

fn digest_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn canonical_json(value: &Value) -> Result<String, AdapterError> {
    fn ordered(value: &Value) -> Value {
        match value {
            Value::Object(map) => {
                let sorted: BTreeMap<_, _> = map
                    .iter()
                    .map(|(key, value)| (key.clone(), ordered(value)))
                    .collect();
                Value::Object(sorted.into_iter().collect())
            }
            Value::Array(values) => Value::Array(values.iter().map(ordered).collect()),
            value => value.clone(),
        }
    }
    serde_json::to_string(&ordered(value)).map_err(|_| AdapterError::HostProtocol)
}

fn result_page_acknowledgement(
    params: &Value,
    operation_id: &str,
    input_sha256: &str,
    artifact_ref: Option<&str>,
) -> Result<ResultPageAcknowledgement, AdapterError> {
    if params["operation_id"] != operation_id || !valid_sha256(input_sha256) {
        return Err(AdapterError::HostProtocol);
    }
    let page = &params["page"];
    let source: NormalizedResultPageSource =
        serde_json::from_value(page["source"].clone()).map_err(|_| AdapterError::HostProtocol)?;
    source.validate().map_err(|_| AdapterError::HostProtocol)?;
    let offset_bytes = page["offset_bytes"]
        .as_u64()
        .ok_or(AdapterError::HostProtocol)?;
    let byte_length = page["byte_length"]
        .as_u64()
        .ok_or(AdapterError::HostProtocol)?;
    let total_bytes = page["total_bytes"]
        .as_u64()
        .ok_or(AdapterError::HostProtocol)?;
    let page_sha256 = page["page_sha256"]
        .as_str()
        .filter(|digest| valid_sha256(digest))
        .ok_or(AdapterError::HostProtocol)?;
    let encoded = page["content_base64"]
        .as_str()
        .ok_or(AdapterError::HostProtocol)?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|_| AdapterError::HostProtocol)?;
    let end = offset_bytes
        .checked_add(byte_length)
        .ok_or(AdapterError::HostProtocol)?;
    let expected_media_type = "text/plain; charset=utf-8";
    if source.schema_id != swarm_contracts::module_contract::NORMALIZED_RESULT_PAGE_SCHEMA_ID
        || source.result_operation_id != operation_id
        || source.result_input_sha256 != input_sha256
        || source.native_response_identity.is_none()
        || source.payload_bytes > MAX_NORMALIZED_RESULT_BODY_BYTES as u64
        || total_bytes != source.payload_bytes
        || byte_length != bytes.len() as u64
        || byte_length > MAX_NORMALIZED_RESULT_PAGE_BYTES as u64
        || end > total_bytes
        || (byte_length == 0 && offset_bytes < total_bytes)
        || page["eof"] != (end == total_bytes)
        || page["media_type"] != expected_media_type
        || digest_hex(&bytes) != page_sha256
        || (offset_bytes == 0 && end == total_bytes && source.payload_sha256 != page_sha256)
    {
        return Err(AdapterError::HostProtocol);
    }
    let acknowledgement = ResultPageAcknowledgement {
        input_sha256: input_sha256.to_owned(),
        artifact_ref: artifact_ref.unwrap_or_default().to_owned(),
        source,
        offset_bytes,
        byte_length,
        total_bytes,
        page_sha256: page_sha256.to_owned(),
    };
    if let Some(artifact_ref) = artifact_ref {
        if artifact_ref.trim().is_empty() || artifact_ref.len() > MAX_OPERATION_ID_BYTES {
            return Err(AdapterError::HostProtocol);
        }
        acknowledgement.validate(operation_id)?;
    }
    Ok(acknowledgement)
}

fn reconcile_dispatch_admission(
    record: &mut OperationRecord,
    admission: &TaskDispatchAdmissionReceipt,
) -> Result<bool, AdapterError> {
    if record.method != "task.dispatch" || admission.operation_id.is_empty() {
        return Err(AdapterError::Checkpoint);
    }
    let Some(previous) = record.dispatch_admission.as_ref() else {
        record.dispatch_admission = Some(admission.clone());
        return Ok(true);
    };
    let mut same_except_native_id = previous.clone();
    same_except_native_id.native_input_id = admission.native_input_id.clone();
    if same_except_native_id != *admission {
        return Err(AdapterError::Checkpoint);
    }
    if previous.native_input_id == admission.native_input_id {
        return Ok(false);
    }
    let provisional_id = record.client_user_message_id.as_deref();
    let new_native_id = admission.native_input_id.as_deref();
    if new_native_id.is_none_or(str::is_empty)
        || previous
            .native_input_id
            .as_deref()
            .is_some_and(|saved| Some(saved) != provisional_id)
    {
        return Err(AdapterError::Checkpoint);
    }
    record.dispatch_admission = Some(admission.clone());
    Ok(true)
}

fn reconcile_continuation_admission(
    record: &mut OperationRecord,
    admission: &GoalContinuationAdmissionReceipt,
) -> Result<bool, AdapterError> {
    if record.method != "agent.send"
        || admission.context.operation_id.is_empty()
        || admission.context.continuation.method != "agent.send"
    {
        return Err(AdapterError::Checkpoint);
    }
    let Some(previous) = record.continuation_admission.as_ref() else {
        record.continuation_admission = Some(admission.clone());
        return Ok(true);
    };
    let mut same_except_native_id = previous.clone();
    same_except_native_id.native_input_id = admission.native_input_id.clone();
    if same_except_native_id != *admission {
        return Err(AdapterError::Checkpoint);
    }
    if previous.native_input_id == admission.native_input_id {
        return Ok(false);
    }
    let provisional_id = record.client_user_message_id.as_deref();
    let new_native_id = admission.native_input_id.as_deref();
    if new_native_id.is_none_or(str::is_empty)
        || previous
            .native_input_id
            .as_deref()
            .is_some_and(|saved| Some(saved) != provisional_id)
    {
        return Err(AdapterError::Checkpoint);
    }
    record.continuation_admission = Some(admission.clone());
    Ok(true)
}

fn normalize_path(value: &str) -> Option<String> {
    let path = Path::new(value);
    if !path.is_absolute() {
        return None;
    }
    Some(
        path.to_string_lossy()
            .replace('/', "\\")
            .trim_end_matches('\\')
            .to_lowercase(),
    )
}

fn validate_route(command: &RuntimeCommand) -> Result<(String, String, String), &'static str> {
    if command.route["runtime"] != "codex" || command.route["module_artifact_id"] != ARTIFACT_ID {
        return Err("ROUTE_ARTIFACT_MISMATCH");
    }
    let options = &command.route["native_options"];
    let provider = options["modelProvider"]
        .as_str()
        .filter(|s| !s.trim().is_empty())
        .ok_or("EXPLICIT_MODEL_PROVIDER_REQUIRED")?;
    let model = options["model"]
        .as_str()
        .filter(|s| !s.trim().is_empty())
        .ok_or("EXPLICIT_MODEL_REQUIRED")?;
    let workspace = options["workspaceRoot"]
        .as_str()
        .filter(|s| !s.trim().is_empty())
        .ok_or("ABSOLUTE_WORKSPACE_REQUIRED")?;
    if normalize_path(workspace).is_none() {
        return Err("ABSOLUTE_WORKSPACE_REQUIRED");
    }
    Ok((provider.to_owned(), model.to_owned(), workspace.to_owned()))
}

fn prompt_for(command: &RuntimeCommand) -> Result<String, &'static str> {
    let input = &command.input;
    let prompt = if command.method == "task.dispatch" {
        if let Some(value) = input.get("task_prompt") {
            let envelope: TaskPromptEnvelopeV1 =
                serde_json::from_value(value.clone()).map_err(|_| "TASK_PROMPT_INVALID")?;
            envelope
                .validate_shape()
                .map_err(|_| "TASK_PROMPT_INVALID")?;
            let context: TaskDispatchContext = serde_json::from_value(
                input
                    .get("task_dispatch_context")
                    .cloned()
                    .ok_or("TASK_PROMPT_CONTEXT_REQUIRED")?,
            )
            .map_err(|_| "TASK_PROMPT_CONTEXT_INVALID")?;
            context
                .validate()
                .map_err(|_| "TASK_PROMPT_CONTEXT_INVALID")?;
            if context.operation_id != command.operation_id
                || context.binding_id != command.binding_id
                || context.binding_generation != command.generation
                || envelope.attempt_id != context.attempt_id
                || envelope.task_id != context.task_id
                || envelope.task_revision != context.task_revision
                || envelope.task_snapshot_sha256 != context.task_snapshot_sha256
                || envelope.prompt_sha256 != digest_hex(envelope.prompt.as_bytes())
            {
                return Err("TASK_PROMPT_IDENTITY_MISMATCH");
            }
            envelope.prompt
        } else {
            return Err("TASK_PROMPT_REQUIRED");
        }
    } else {
        input["text"]
            .as_str()
            .filter(|s| !s.trim().is_empty())
            .ok_or("PROMPT_REQUIRED")?
            .to_owned()
    };
    if prompt.trim().is_empty() {
        return Err("PROMPT_REQUIRED");
    }
    Ok(prompt)
}

fn native_send_payload(
    root: &str,
    prompt: &str,
    model: &str,
    operation_id: &str,
    steer: bool,
    expected_turn_id: Option<&str>,
) -> Value {
    let input = json!([{"type":"text","text":prompt}]);
    if steer {
        json!({
            "threadId": root,
            "expectedTurnId": expected_turn_id,
            "input": input,
            "clientUserMessageId": operation_id,
        })
    } else {
        json!({
            "threadId": root,
            "input": input,
            "model": model,
            "clientUserMessageId": operation_id,
        })
    }
}

fn normalized_dispatch_admission(
    command: &RuntimeCommand,
    claim: &ModuleContractClaim,
    boot_id: &str,
    payload: &Value,
) -> Result<Option<TaskDispatchAdmissionReceipt>, AdapterError> {
    if command.method != "task.dispatch" || !module_contract::normalized_dispatch_enabled(claim) {
        return Ok(None);
    }
    let context: TaskDispatchContext =
        serde_json::from_value(command.input["task_dispatch_context"].clone())
            .map_err(|_| AdapterError::HostProtocol)?;
    context.validate().map_err(|_| AdapterError::HostProtocol)?;
    if context.operation_id != command.operation_id
        || context.binding_id != command.binding_id
        || context.binding_generation != command.generation
        || context.worker_boot_id != boot_id
    {
        return Err(AdapterError::HostProtocol);
    }
    let source_text = command.input["text"]
        .as_str()
        .filter(|text| !text.trim().is_empty())
        .ok_or(AdapterError::HostProtocol)?;
    if context.source_text_sha256 != digest_hex(source_text.as_bytes())
        || context.source_text_bytes != source_text.len() as u64
    {
        return Err(AdapterError::HostProtocol);
    }
    let input_sha256 = command
        .input_sha256
        .as_deref()
        .filter(|digest| valid_sha256(digest))
        .ok_or(AdapterError::HostProtocol)?;
    let module_receipt = module_contract::receipt_identity(
        claim,
        &command.binding_id,
        command.generation,
        &command.operation_id,
        input_sha256,
    )?;
    let prompt = prompt_for(command).map_err(|_| AdapterError::HostProtocol)?;
    if payload["input"][0]["text"].as_str() != Some(prompt.as_str()) {
        return Err(AdapterError::HostProtocol);
    }
    let native_payload = prompt.as_bytes();
    let receipt = TaskDispatchAdmissionReceipt {
        schema_version: 1,
        module_receipt,
        operation_id: context.operation_id,
        binding_id: context.binding_id,
        binding_generation: context.binding_generation,
        worker_boot_id: context.worker_boot_id,
        attempt_id: context.attempt_id,
        task_id: context.task_id,
        task_revision: context.task_revision,
        task_snapshot_sha256: context.task_snapshot_sha256,
        source_text_sha256: context.source_text_sha256,
        source_text_bytes: context.source_text_bytes,
        native_payload_sha256: digest_hex(native_payload),
        native_payload_bytes: native_payload.len() as u64,
        native_input_id: Some(command.operation_id.clone()),
    };
    receipt.validate().map_err(|_| AdapterError::HostProtocol)?;
    Ok(Some(receipt))
}

fn normalized_goal_continuation_admission(
    command: &RuntimeCommand,
    claim: &ModuleContractClaim,
    boot_id: &str,
    payload: &Value,
) -> Result<Option<GoalContinuationAdmissionReceipt>, AdapterError> {
    let Some(context_value) = command.input.get("goal_continuation_context") else {
        return Ok(None);
    };
    if command.method != "agent.send" || command.input["delivery"] != "next_turn" {
        return Err(AdapterError::HostProtocol);
    }
    let context: GoalContinuationAdmissionContext =
        serde_json::from_value(context_value.clone()).map_err(|_| AdapterError::HostProtocol)?;
    context.validate().map_err(|_| AdapterError::HostProtocol)?;
    if context.operation_id != command.operation_id
        || context.binding_id != command.binding_id
        || context.binding_generation != command.generation
        || context.worker_boot_id != boot_id
        || payload["clientUserMessageId"] != command.operation_id
    {
        return Err(AdapterError::HostProtocol);
    }
    let input_sha256 = command
        .input_sha256
        .as_deref()
        .filter(|digest| valid_sha256(digest))
        .ok_or(AdapterError::HostProtocol)?;
    let module_receipt = module_contract::receipt_identity(
        claim,
        &command.binding_id,
        command.generation,
        &command.operation_id,
        input_sha256,
    )?;
    let native_payload = serde_json::to_vec(payload).map_err(|_| AdapterError::HostProtocol)?;
    let receipt = GoalContinuationAdmissionReceipt {
        schema_id: module_contract::GOAL_CONTINUATION_ADMISSION_SCHEMA_ID.to_owned(),
        schema_version: GoalContinuationAdmissionReceipt::VERSION,
        context,
        module_receipt,
        native_payload_sha256: digest_hex(&native_payload),
        native_payload_bytes: native_payload.len() as u64,
        native_input_id: None,
    };
    receipt.validate().map_err(|_| AdapterError::HostProtocol)?;
    Ok(Some(receipt))
}

fn outcome(
    operation_id: &str,
    disposition: EffectOutcome,
    root: Option<&str>,
    scope: Option<&str>,
    turn: Option<&str>,
    input: Option<&str>,
    details: Value,
) -> RuntimeOutcome {
    RuntimeOutcome {
        operation_id: operation_id.to_owned(),
        outcome: disposition,
        native_scope_key: scope.map(str::to_owned),
        native_root_id: root.map(str::to_owned),
        turn_id: turn.map(str::to_owned),
        native_input_id: input.map(str::to_owned),
        details,
    }
}

fn decode_outcome(value: &Value, operation_id: &str) -> RuntimeOutcome {
    serde_json::from_value(value.clone()).unwrap_or_else(|_| {
        outcome(
            operation_id,
            EffectOutcome::Unknown,
            None,
            None,
            None,
            None,
            json!({"diagnostic_code":"CHECKPOINT_OUTCOME_INVALID","native_replay":false}),
        )
    })
}

fn annotate_root_applicability(
    state: &Checkpoint,
    record: &OperationRecord,
    outcome: &mut RuntimeOutcome,
) -> Result<(), AdapterError> {
    if let Some(applicability) = state.classify_root_applicability(record, outcome)? {
        outcome.details["root_applicability"] = json!(applicability.as_str());
    } else if record.method == "agent.open"
        && outcome.native_root_id.is_some()
        && outcome.outcome == EffectOutcome::Applied
    {
        // Old checkpoints could record a started root from the start reply
        // alone. Keep that open outcome Unknown until native config readback
        // proves it is compatible.
        outcome.outcome = EffectOutcome::Unknown;
        outcome.details["diagnostic_code"] = json!("ROOT_COMPATIBILITY_UNVERIFIED");
        outcome.details["completion_condition"] = Value::Null;
        outcome.details["execution_complete"] = json!(false);
        outcome.details["task_completion"] = json!("unknown");
        outcome.details["disposition"] = json!("unknown");
        outcome.details["native_replay"] = json!(false);
    }
    Ok(())
}

fn receipt_identity_from_outcome(
    outcome: &RuntimeOutcome,
) -> Result<ModuleReceiptIdentity, AdapterError> {
    let receipt: ModuleReceiptIdentity =
        serde_json::from_value(outcome.details["module_receipt"].clone())
            .map_err(|_| AdapterError::Checkpoint)?;
    receipt.validate().map_err(|_| AdapterError::Checkpoint)?;
    if receipt.operation_id != outcome.operation_id {
        return Err(AdapterError::Checkpoint);
    }
    Ok(receipt)
}

fn receipt_identity_from_value(value: &Value) -> Result<ModuleReceiptIdentity, AdapterError> {
    let outcome: RuntimeOutcome =
        serde_json::from_value(value.clone()).map_err(|_| AdapterError::Checkpoint)?;
    receipt_identity_from_outcome(&outcome)
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn attach_module_receipt(
    outcome: &mut RuntimeOutcome,
    claim: &ModuleContractClaim,
    binding_id: &str,
    generation: i64,
    operation_id: &str,
    input_sha256: &str,
) -> Result<(), AdapterError> {
    if outcome.operation_id != operation_id || !valid_sha256(input_sha256) {
        return Err(AdapterError::HostProtocol);
    }
    let expected = module_contract::receipt_identity(
        claim,
        binding_id,
        generation,
        operation_id,
        input_sha256,
    )?;
    let details = outcome
        .details
        .as_object_mut()
        .ok_or(AdapterError::HostProtocol)?;
    if let Some(existing) = details.get("module_receipt") {
        let saved: ModuleReceiptIdentity =
            serde_json::from_value(existing.clone()).map_err(|_| AdapterError::HostProtocol)?;
        if saved != expected {
            return Err(AdapterError::HostProtocol);
        }
    } else {
        details.insert(
            "module_receipt".to_owned(),
            serde_json::to_value(expected).map_err(|_| AdapterError::HostProtocol)?,
        );
    }
    Ok(())
}

fn attach_command_receipt(
    outcome: &mut RuntimeOutcome,
    command: &RuntimeCommand,
    claim: &ModuleContractClaim,
) -> Result<(), AdapterError> {
    let input_sha256 = command
        .input_sha256
        .as_deref()
        .filter(|digest| valid_sha256(digest))
        .ok_or(AdapterError::HostProtocol)?;
    attach_module_receipt(
        outcome,
        claim,
        &command.binding_id,
        command.generation,
        &command.operation_id,
        input_sha256,
    )
}

fn validate_saved_receipt(
    outcome: &RuntimeOutcome,
    claim: &ModuleContractClaim,
    binding_id: &str,
    generation: i64,
    operation_id: &str,
    input_sha256: &str,
) -> Result<(), AdapterError> {
    let saved = receipt_identity_from_outcome(outcome)?;
    let expected = module_contract::receipt_identity(
        claim,
        binding_id,
        generation,
        operation_id,
        input_sha256,
    )?;
    if saved != expected {
        return Err(AdapterError::Checkpoint);
    }
    Ok(())
}

fn validate_saved_dispatch_admission(
    outcome: &RuntimeOutcome,
    command: &RuntimeCommand,
    claim: &ModuleContractClaim,
    boot_id: &str,
) -> Result<(), AdapterError> {
    if command.method != "task.dispatch" || !module_contract::normalized_dispatch_enabled(claim) {
        return Ok(());
    }
    let details = outcome
        .details
        .as_object()
        .ok_or(AdapterError::Checkpoint)?;
    let admission = details
        .get("dispatch_admission")
        .map(|value| {
            let receipt: TaskDispatchAdmissionReceipt =
                serde_json::from_value(value.clone()).map_err(|_| AdapterError::Checkpoint)?;
            receipt.validate().map_err(|_| AdapterError::Checkpoint)?;
            Ok::<_, AdapterError>(receipt)
        })
        .transpose()?;
    match outcome.outcome {
        EffectOutcome::Applied | EffectOutcome::Accepted => {
            let receipt = admission.ok_or(AdapterError::Checkpoint)?;
            let input_sha256 = command
                .input_sha256
                .as_deref()
                .filter(|digest| valid_sha256(digest))
                .ok_or(AdapterError::Checkpoint)?;
            let expected = module_contract::receipt_identity(
                claim,
                &command.binding_id,
                command.generation,
                &command.operation_id,
                input_sha256,
            )?;
            let context = receipt.context();
            let source_text = command.input["text"]
                .as_str()
                .filter(|text| !text.trim().is_empty())
                .ok_or(AdapterError::Checkpoint)?;
            if receipt.module_receipt != expected
                || context.operation_id != command.operation_id
                || context.binding_id != command.binding_id
                || context.binding_generation != command.generation
                || context.worker_boot_id != boot_id
                || context.source_text_sha256 != digest_hex(source_text.as_bytes())
                || context.source_text_bytes != source_text.len() as u64
                || receipt.native_input_id.as_deref() != outcome.native_input_id.as_deref()
            {
                return Err(AdapterError::Checkpoint);
            }
        }
        EffectOutcome::Rejected | EffectOutcome::Unknown => {
            if admission.is_some() {
                return Err(AdapterError::Checkpoint);
            }
        }
    }
    Ok(())
}

fn validate_saved_goal_continuation_admission(
    outcome: &RuntimeOutcome,
    command: &RuntimeCommand,
    claim: &ModuleContractClaim,
    boot_id: &str,
) -> Result<(), AdapterError> {
    let details = outcome
        .details
        .as_object()
        .ok_or(AdapterError::Checkpoint)?;
    let saved = details
        .get("goal_continuation_admission")
        .map(|value| {
            let receipt: GoalContinuationAdmissionReceipt =
                serde_json::from_value(value.clone()).map_err(|_| AdapterError::Checkpoint)?;
            receipt.validate().map_err(|_| AdapterError::Checkpoint)?;
            Ok::<_, AdapterError>(receipt)
        })
        .transpose()?;
    let context = command.input.get("goal_continuation_context");
    match (context, saved, outcome.outcome) {
        (None, None, _) => Ok(()),
        (None, Some(_), _) => Err(AdapterError::Checkpoint),
        (Some(_), None, EffectOutcome::Accepted | EffectOutcome::Applied) => {
            Err(AdapterError::Checkpoint)
        }
        (Some(context_value), Some(receipt), EffectOutcome::Accepted | EffectOutcome::Applied) => {
            let expected_context: GoalContinuationAdmissionContext =
                serde_json::from_value(context_value.clone())
                    .map_err(|_| AdapterError::Checkpoint)?;
            expected_context
                .validate()
                .map_err(|_| AdapterError::Checkpoint)?;
            let input_sha256 = command
                .input_sha256
                .as_deref()
                .filter(|digest| valid_sha256(digest))
                .ok_or(AdapterError::Checkpoint)?;
            let expected = module_contract::receipt_identity(
                claim,
                &command.binding_id,
                command.generation,
                &command.operation_id,
                input_sha256,
            )?;
            if receipt.context != expected_context
                || receipt.module_receipt != expected
                || expected_context.operation_id != command.operation_id
                || expected_context.binding_id != command.binding_id
                || expected_context.binding_generation != command.generation
                || expected_context.worker_boot_id != boot_id
                || receipt.native_input_id.as_deref() != outcome.native_input_id.as_deref()
                || receipt.native_input_id.is_none()
                || details.contains_key("dispatch_admission")
            {
                return Err(AdapterError::Checkpoint);
            }
            Ok(())
        }
        (Some(_), None, EffectOutcome::Rejected | EffectOutcome::Unknown) => Ok(()),
        (Some(_), Some(_), EffectOutcome::Rejected | EffectOutcome::Unknown) => {
            Err(AdapterError::Checkpoint)
        }
    }
}

fn base_details(state: &Checkpoint) -> Value {
    json!({
        "module_artifact_id": ARTIFACT_ID,
        "native_root_phase": state.native_root_phase,
        "root_operation_id": state.root_operation_id.as_deref(),
        "requested_model_provider": state.requested_model_provider.as_deref(),
        "requested_model": state.requested_model.as_deref(),
        "effective_model_provider": state.effective_model_provider.as_deref(),
        "effective_model": state.effective_model.as_deref(),
        "served_model": null,
        "served_model_status": "unknown",
        "billing_status": "unknown",
        "fallback_used": false,
    })
}

fn unknown_send(
    record: &OperationRecord,
    operation_id: &str,
    code: &'static str,
) -> RuntimeOutcome {
    let mut details = json!({
        "diagnostic_code": code,
        "native_replay": false,
        "native_input_readback": "unverified",
        "native_request_failure_code": record.native_request_failure_code,
        "native_rpc_error_code": record.native_rpc_error_code,
        "client_user_message_id": record.client_user_message_id,
        "prompt_sha256": record.prompt_sha256,
        "prompt_bytes": record.prompt_bytes,
        "prompt_contract_revision": record.prompt_contract_revision,
        "requested_model_provider": record.requested_model_provider,
        "requested_model": record.requested_model,
        "served_model": null,
        "served_model_status": "unknown",
        "billing_status": "unknown",
        "fallback_used": false,
    });
    details["completion_condition"] = Value::Null;
    outcome(
        operation_id,
        EffectOutcome::Unknown,
        record.native_root_id.as_deref(),
        record.native_scope_key.as_deref(),
        None,
        None,
        details,
    )
}

fn accepted_after_exact_input(
    record: &OperationRecord,
    operation_id: &str,
    turn_id: &str,
    native_input_id: &str,
    code: &'static str,
    turn_readback: &'static str,
) -> RuntimeOutcome {
    let mut accepted = unknown_send(record, operation_id, code);
    accepted.outcome = EffectOutcome::Accepted;
    accepted.turn_id = Some(turn_id.to_owned());
    accepted.native_input_id = Some(native_input_id.to_owned());
    accepted.details["diagnostic_code"] = json!(code);
    accepted.details["native_input_readback"] = json!("verified");
    accepted.details["native_turn_readback"] = json!(turn_readback);
    accepted.details["completion_condition"] = json!("native_input_admitted");
    accepted.details["execution_complete"] = json!(false);
    accepted.details["task_completion"] = json!("unknown");
    accepted.details["disposition"] = json!("admitted");
    accepted.details["native_replay"] = json!(false);
    if let Some(admission) = record.dispatch_admission.as_ref()
        && let Ok(value) = serde_json::to_value(admission)
    {
        accepted.details["dispatch_admission"] = value;
    }
    if let Some(admission) = record.continuation_admission.as_ref()
        && let Ok(value) = serde_json::to_value(admission)
    {
        accepted.details["goal_continuation_admission"] = value;
    }
    accepted
}

async fn reconcile_send(
    native: &mut NativeClient,
    record: &OperationRecord,
    operation_id: &str,
) -> RuntimeOutcome {
    let (Some(root), Some(client_id), Some(expected_digest), Some(expected_bytes)) = (
        record.native_root_id.as_deref(),
        record.client_user_message_id.as_deref(),
        record.prompt_sha256.as_deref(),
        record.prompt_bytes,
    ) else {
        return unknown_send(record, operation_id, "NATIVE_IDENTITY_UNAVAILABLE");
    };
    let (matches, history_truncated) = match native.read_history(root, client_id).await {
        HistoryRead::Complete(matches) => (matches, false),
        HistoryRead::Truncated(matches) if record.delivery.as_deref() == Some("steer") => {
            (matches, true)
        }
        HistoryRead::Truncated(_) => {
            return unknown_send(record, operation_id, "NATIVE_HISTORY_PAGE_LIMIT");
        }
        HistoryRead::Failed(error) => {
            let mut unresolved = unknown_send(record, operation_id, error.diagnostic_code());
            with_native_failure_details(&mut unresolved.details, &error, "history_readback");
            return unresolved;
        }
    };
    if matches.is_empty() {
        return unknown_send(record, operation_id, "NATIVE_ITEM_NOT_OBSERVED");
    }
    if matches.len() != 1 {
        return unknown_send(record, operation_id, "NATIVE_ITEM_CORRELATION_NOT_UNIQUE");
    }
    let matched = &matches[0];
    let mut dispatch_admission = record.dispatch_admission.clone();
    if let Some(admission) = dispatch_admission.as_mut() {
        match admission.native_input_id.as_deref() {
            Some(id) if id == matched.item_id => {}
            Some(id) if id == client_id => {
                // `clientUserMessageId` is the request correlation key, while
                // `item.id` is the durable native input identity. Promote the
                // provisional key only after the unique exact-content
                // readback above has linked them.
                admission.native_input_id = Some(matched.item_id.clone());
            }
            None => admission.native_input_id = Some(matched.item_id.clone()),
            Some(_) => {
                return unknown_send(
                    record,
                    operation_id,
                    "NATIVE_DISPATCH_ADMISSION_ID_MISMATCH",
                );
            }
        }
    }
    let mut continuation_admission = record.continuation_admission.clone();
    if let Some(admission) = continuation_admission.as_mut() {
        match admission.native_input_id.as_deref() {
            Some(id) if id == matched.item_id => {}
            Some(id) if id == client_id => {
                admission.native_input_id = Some(matched.item_id.clone());
            }
            None => admission.native_input_id = Some(matched.item_id.clone()),
            Some(_) => {
                return unknown_send(
                    record,
                    operation_id,
                    "NATIVE_CONTINUATION_ADMISSION_ID_MISMATCH",
                );
            }
        }
    }
    let mut verified_record = record.clone();
    verified_record.dispatch_admission = dispatch_admission.clone();
    verified_record.continuation_admission = continuation_admission.clone();
    if digest_hex(matched.text.as_bytes()) != expected_digest
        || matched.text.len() as u64 != expected_bytes
        || record
            .returned_turn_id
            .as_deref()
            .is_some_and(|id| id != matched.turn_id)
        || record.delivery.as_deref() == Some("steer")
            && record.expected_turn_id.as_deref() != Some(matched.turn_id.as_str())
    {
        return unknown_send(
            record,
            operation_id,
            "NATIVE_ITEM_CONTENT_OR_IDENTITY_MISMATCH",
        );
    }
    if history_truncated {
        return accepted_after_exact_input(
            &verified_record,
            operation_id,
            &matched.turn_id,
            &matched.item_id,
            "NATIVE_HISTORY_PAGE_LIMIT",
            "not_read_due_to_history_truncation",
        );
    }
    let turn = match native.read_turn(root, &matched.turn_id).await {
        TurnRead::Found(turn) => turn,
        TurnRead::Missing => {
            return accepted_after_exact_input(
                &verified_record,
                operation_id,
                &matched.turn_id,
                &matched.item_id,
                "NATIVE_TURN_NOT_OBSERVED",
                "not_observed",
            );
        }
        TurnRead::Truncated => {
            return accepted_after_exact_input(
                &verified_record,
                operation_id,
                &matched.turn_id,
                &matched.item_id,
                "NATIVE_TURN_PAGE_LIMIT",
                "truncated",
            );
        }
        TurnRead::Failed(error) => {
            let mut unresolved = accepted_after_exact_input(
                &verified_record,
                operation_id,
                &matched.turn_id,
                &matched.item_id,
                error.diagnostic_code(),
                "failed",
            );
            with_native_failure_details(&mut unresolved.details, &error, "turn_readback");
            return unresolved;
        }
    };
    if turn.get("id").and_then(Value::as_str) != Some(matched.turn_id.as_str()) {
        return accepted_after_exact_input(
            &verified_record,
            operation_id,
            &matched.turn_id,
            &matched.item_id,
            "NATIVE_TURN_IDENTITY_MISMATCH",
            "mismatch",
        );
    }
    let Some(turn_status) = turn.get("status").and_then(Value::as_str) else {
        return accepted_after_exact_input(
            &verified_record,
            operation_id,
            &matched.turn_id,
            &matched.item_id,
            "NATIVE_TURN_STATUS_UNAVAILABLE",
            "invalid",
        );
    };
    let status_is_completed = turn_status == "completed";
    let status_is_in_progress = turn_status == "inProgress";
    let status_is_failed = matches!(turn_status, "failed" | "interrupted");
    if !status_is_completed && !status_is_in_progress && !status_is_failed {
        return accepted_after_exact_input(
            &verified_record,
            operation_id,
            &matched.turn_id,
            &matched.item_id,
            "NATIVE_TURN_STATUS_UNRECOGNIZED",
            "invalid",
        );
    }
    if !status_is_failed && turn.get("error").is_some_and(|error| !error.is_null()) {
        return accepted_after_exact_input(
            &verified_record,
            operation_id,
            &matched.turn_id,
            &matched.item_id,
            "NATIVE_TURN_STATUS_CONTRADICTORY",
            "contradictory",
        );
    }
    let mut details = json!({
        "module_artifact_id": ARTIFACT_ID,
        "completion_condition": if status_is_completed {
            "native_turn_completed"
        } else if status_is_failed {
            "native_turn_failed"
        } else {
            "native_input_admitted"
        },
        "native_input_readback": "verified",
        "native_turn_readback": "verified",
        "native_turn_status": turn_status,
        "execution_complete": status_is_completed || status_is_failed,
        "task_completion": "unknown",
        "disposition": if status_is_completed {
            "completed"
        } else if status_is_failed {
            "failed"
        } else {
            "admitted"
        },
        "client_user_message_id": client_id,
        "prompt_sha256": expected_digest,
        "prompt_bytes": expected_bytes,
        "prompt_contract_revision": record.prompt_contract_revision,
        "native_replay": false,
        "native_request_failure_code": record.native_request_failure_code,
        "native_rpc_error_code": record.native_rpc_error_code,
        "requested_model_provider": record.requested_model_provider.as_deref(),
        "requested_model": record.requested_model.as_deref(),
        "effective_model_provider": record.requested_model_provider.as_deref(),
        "effective_model": record.requested_model.as_deref(),
        "effective_model_status": "thread_configuration_verified",
        "served_model": null,
        "served_model_status": "unknown",
        "billing_status": "unknown",
        "fallback_used": false,
    });
    if !status_is_failed && let Some(admission) = dispatch_admission.as_ref() {
        details["dispatch_admission"] = match serde_json::to_value(admission) {
            Ok(value) => value,
            Err(_) => {
                return unknown_send(
                    record,
                    operation_id,
                    "DISPATCH_ADMISSION_SERIALIZATION_FAILED",
                );
            }
        };
    }
    if !status_is_failed && let Some(admission) = continuation_admission.as_ref() {
        details["goal_continuation_admission"] = match serde_json::to_value(admission) {
            Ok(value) => value,
            Err(_) => {
                return unknown_send(
                    record,
                    operation_id,
                    "GOAL_CONTINUATION_ADMISSION_SERIALIZATION_FAILED",
                );
            }
        };
    }
    if status_is_failed {
        let failure_code = turn_error_diagnostic(&turn);
        details["diagnostic_code"] = json!(failure_code);
        if let Some(status) = turn_error_http_status(&turn) {
            details["native_provider_http_status"] = json!(status);
        }
        details["native_turn_failure_class"] = json!(if turn_status == "interrupted" {
            "interrupted"
        } else {
            "failed"
        });
        return outcome(
            operation_id,
            EffectOutcome::Unknown,
            Some(root),
            record.native_scope_key.as_deref(),
            Some(&matched.turn_id),
            Some(&matched.item_id),
            details,
        );
    }
    outcome(
        operation_id,
        if status_is_completed {
            EffectOutcome::Applied
        } else {
            EffectOutcome::Accepted
        },
        Some(root),
        record.native_scope_key.as_deref(),
        Some(&matched.turn_id),
        Some(&matched.item_id),
        details,
    )
}

async fn reconcile_open(
    journal: &mut Journal,
    native: &mut NativeClient,
    record: &OperationRecord,
    operation_id: &str,
) -> RuntimeOutcome {
    let (Some(root), Some(scope), Some(provider), Some(model), Some(workspace)) = (
        record.native_root_id.as_deref(),
        record.native_scope_key.as_deref(),
        record.requested_model_provider.as_deref(),
        record.requested_model.as_deref(),
        record.workspace_root.as_deref(),
    ) else {
        let mut details = json!({
            "diagnostic_code": record.native_request_failure_code.as_deref().unwrap_or("THREAD_START_OUTCOME_UNKNOWN"),
            "native_failure_code": record.native_request_failure_code,
            "native_rpc_error_code": record.native_rpc_error_code,
            "requested_model_provider": record.requested_model_provider.as_deref(),
            "requested_model": record.requested_model.as_deref(),
            "native_replay":false
        });
        details["completion_condition"] = Value::Null;
        return outcome(
            operation_id,
            EffectOutcome::Unknown,
            None,
            record.native_scope_key.as_deref(),
            None,
            None,
            details,
        );
    };
    if native.scope_key() != scope || !native.identity_is_known() {
        return outcome(
            operation_id,
            EffectOutcome::Unknown,
            Some(root),
            Some(scope),
            None,
            None,
            json!({"diagnostic_code":"NATIVE_SCOPE_CHANGED","native_replay":false}),
        );
    }
    let thread = match native.read_thread(root).await {
        Ok(response) => response["thread"].clone(),
        Err(error) => {
            let mut details = json!({
                "diagnostic_code": error.diagnostic_code(),
                "native_replay": false,
                "requested_model_provider": provider,
                "requested_model": model,
            });
            with_native_failure_details(&mut details, &error, "thread_read_reconciliation");
            return outcome(
                operation_id,
                EffectOutcome::Unknown,
                Some(root),
                Some(scope),
                None,
                None,
                details,
            );
        }
    };
    let observed_cwd = thread["cwd"].as_str();
    let adoption = adopt_verified_root(journal, operation_id, scope, root, &thread);
    let exact = adoption.is_ok();
    let mut details = json!({
        "module_artifact_id": ARTIFACT_ID,
        "native_thread_readback": if exact { "verified" } else { "mismatch" },
        "native_replay": false,
        "requested_model_provider": provider,
        "requested_model": model,
        "effective_model_provider": thread["modelProvider"],
        "effective_model": thread["model"],
        "effective_model_status": if exact { "thread_configuration_verified" } else { "unknown" },
        "served_model": null,
        "served_model_status": "unknown",
        "billing_status": "unknown",
        "fallback_used": false,
        "thread": {
            "id": thread["id"],
            "model_provider": thread["modelProvider"],
            "model": thread["model"],
            "cwd": observed_cwd,
            "workspace_status": if observed_cwd.and_then(normalize_path) == normalize_path(workspace) { "workspace_exact" } else { "workspace_mismatch" },
        }
    });
    if !exact {
        details["diagnostic_code"] =
            json!(adoption.err().unwrap_or("THREAD_CONFIGURATION_MISMATCH"));
        return outcome(
            operation_id,
            EffectOutcome::Unknown,
            Some(root),
            Some(scope),
            None,
            None,
            details,
        );
    }
    details["completion_condition"] = json!("native_thread_opened");
    outcome(
        operation_id,
        EffectOutcome::Applied,
        Some(root),
        Some(scope),
        None,
        None,
        details,
    )
}

fn rejected(command: &RuntimeCommand, code: &'static str, state: &Checkpoint) -> RuntimeOutcome {
    let mut details = base_details(state);
    details["diagnostic_code"] = json!(code);
    details["pre_input_failure"] = json!(true);
    details["native_replay"] = json!(false);
    outcome(
        &command.operation_id,
        EffectOutcome::Rejected,
        state.native_root_id.as_deref(),
        state.native_scope_key.as_deref(),
        None,
        None,
        details,
    )
}

fn with_native_failure_details(details: &mut Value, error: &NativeError, stage: &str) {
    details["native_failure_stage"] = json!(stage);
    details["native_failure_code"] = json!(error.diagnostic_code());
    if let Some(status) = error.http_status() {
        details["native_http_status"] = json!(status);
    }
    if let Some(code) = error.rpc_code() {
        details["native_rpc_error_code"] = json!(code);
    }
}

fn rejected_before_input(
    command: &RuntimeCommand,
    state: &Checkpoint,
    error: &NativeError,
    stage: &str,
    requested_provider: Option<&str>,
    requested_model: Option<&str>,
) -> RuntimeOutcome {
    let mut details = base_details(state);
    if let Some(provider) = requested_provider {
        details["requested_model_provider"] = json!(provider);
    }
    if let Some(model) = requested_model {
        details["requested_model"] = json!(model);
    }
    details["diagnostic_code"] = json!(error.diagnostic_code());
    details["pre_input_failure"] = json!(true);
    details["native_replay"] = json!(false);
    with_native_failure_details(&mut details, error, stage);
    outcome(
        &command.operation_id,
        EffectOutcome::Rejected,
        state.native_root_id.as_deref(),
        state.native_scope_key.as_deref(),
        None,
        None,
        details,
    )
}

fn adopt_verified_root(
    journal: &mut Journal,
    operation_id: &str,
    scope: &str,
    root: &str,
    thread: &Value,
) -> Result<(), &'static str> {
    if journal.state.native_root_phase == Some(NativeRootPhase::Active)
        && journal.state.native_root_id.as_deref() != Some(root)
    {
        return Err("ACTIVE_ROOT_CONFLICT");
    }
    if !matches!(
        journal.state.native_root_phase,
        Some(NativeRootPhase::Candidate | NativeRootPhase::Active)
    ) || journal.state.root_operation_id.as_deref() != Some(operation_id)
        || journal.state.native_root_id.as_deref() != Some(root)
    {
        return Err("OPEN_OPERATION_MISMATCH");
    }
    let record = journal
        .state
        .operations
        .get(operation_id)
        .ok_or("OPEN_OPERATION_MISMATCH")?;
    let (Some(provider), Some(model), Some(workspace)) = (
        journal.state.requested_model_provider.as_deref(),
        journal.state.requested_model.as_deref(),
        journal.state.workspace_root.as_deref(),
    ) else {
        return Err("THREAD_CONFIGURATION_MISMATCH");
    };
    if record.method != "agent.open"
        || record.kind != "open"
        || record.native_root_id.as_deref() != Some(root)
    {
        return Err("OPEN_OPERATION_MISMATCH");
    }
    if record.native_scope_key.as_deref() != Some(scope)
        || journal.state.native_scope_key.as_deref() != Some(scope)
    {
        return Err("NATIVE_SCOPE_CHANGED");
    }
    if record.requested_model_provider.as_deref() != Some(provider)
        || record.requested_model.as_deref() != Some(model)
        || record.workspace_root.as_deref().and_then(normalize_path) != normalize_path(workspace)
    {
        return Err("THREAD_CONFIGURATION_MISMATCH");
    }
    if thread["id"].as_str() != Some(root)
        || thread["modelProvider"].as_str() != Some(provider)
        || thread["model"].as_str() != Some(model)
        || thread["cwd"].as_str().and_then(normalize_path) != normalize_path(workspace)
    {
        return Err("THREAD_CONFIGURATION_MISMATCH");
    }
    // This is the sole Pending/Candidate -> Active transition. Its inputs are
    // the exact open Operation and a fresh native thread/read response.
    let prior_phase = journal.state.native_root_phase;
    let prior_effective_provider = journal.state.effective_model_provider.clone();
    let prior_effective_model = journal.state.effective_model.clone();
    let prior_readback = journal
        .state
        .operations
        .get(operation_id)
        .and_then(|record| record.thread_configuration_readback.clone());
    if let Some(record) = journal.state.operations.get_mut(operation_id) {
        record.thread_configuration_readback = Some("verified".into());
    }
    journal.state.effective_model_provider = Some(provider.to_owned());
    journal.state.effective_model = Some(model.to_owned());
    journal.state.native_root_phase = Some(NativeRootPhase::Active);
    if !journal
        .state
        .is_current_root(root, scope, provider, model, workspace)
    {
        journal.state.native_root_phase = prior_phase;
        journal.state.effective_model_provider = prior_effective_provider;
        journal.state.effective_model = prior_effective_model;
        if let Some(record) = journal.state.operations.get_mut(operation_id) {
            record.thread_configuration_readback = prior_readback;
        }
        return Err("THREAD_CONFIGURATION_MISMATCH");
    }
    if journal.save().is_err() {
        journal.state.native_root_phase = prior_phase;
        journal.state.effective_model_provider = prior_effective_provider;
        journal.state.effective_model = prior_effective_model;
        if let Some(record) = journal.state.operations.get_mut(operation_id) {
            record.thread_configuration_readback = prior_readback;
        }
        return Err("CHECKPOINT_WRITE_FAILED");
    }
    Ok(())
}

async fn open_operation(
    command: &RuntimeCommand,
    journal: &mut Journal,
    native: &mut NativeClient,
) -> RuntimeOutcome {
    if command.native_root_id.is_some() {
        return rejected(command, "OPEN_ROOT_ALREADY_ASSIGNED", &journal.state);
    }
    if let Some(previous) = journal.state.operations.get(&command.operation_id) {
        if let Some(result) = &previous.outcome {
            return serde_json::from_value(result.clone()).unwrap_or_else(|_| {
                outcome(
                    &command.operation_id,
                    EffectOutcome::Unknown,
                    None,
                    None,
                    None,
                    None,
                    json!({"diagnostic_code":"CHECKPOINT_OUTCOME_INVALID","native_replay":false}),
                )
            });
        }
        let mut details = base_details(&journal.state);
        details["diagnostic_code"] = json!(
            previous
                .native_request_failure_code
                .as_deref()
                .unwrap_or("THREAD_START_OUTCOME_UNKNOWN")
        );
        details["native_request_failure_code"] =
            json!(previous.native_request_failure_code.as_deref());
        details["native_rpc_error_code"] = json!(previous.native_rpc_error_code);
        details["requested_model_provider"] = json!(previous.requested_model_provider.as_deref());
        details["requested_model"] = json!(previous.requested_model.as_deref());
        details["native_replay"] = json!(false);
        return outcome(
            &command.operation_id,
            EffectOutcome::Unknown,
            None,
            None,
            None,
            None,
            details,
        );
    }
    if journal.state.native_root_phase.is_some() {
        return rejected(command, "THREAD_ALREADY_OPEN", &journal.state);
    }
    let (provider, model, workspace) = match validate_route(command) {
        Ok(route) => route,
        Err(code) => return rejected(command, code, &journal.state),
    };
    let canonical_workspace = normalize_path(&workspace).expect("validated absolute workspace");
    if !native.identity_is_known() {
        return rejected_before_input(
            command,
            &journal.state,
            &NativeError::IdentityUnverified,
            "server_identity",
            Some(&provider),
            Some(&model),
        );
    }
    let scope = native.scope_key();
    let mut record = OperationRecord::intent("agent.open", "open");
    record.input_sha256 = command.input_sha256.clone();
    record.native_scope_key = Some(scope.clone());
    record.requested_model_provider = Some(provider.clone());
    record.requested_model = Some(model.clone());
    record.workspace_root = Some(canonical_workspace.clone());
    journal.state.requested_model_provider = Some(provider.clone());
    journal.state.requested_model = Some(model.clone());
    journal.state.workspace_root = Some(canonical_workspace);
    journal.state.native_scope_key = Some(scope.clone());
    journal.state.effective_model_provider = None;
    journal.state.effective_model = None;
    journal.state.native_root_phase = Some(NativeRootPhase::Pending);
    journal.state.root_operation_id = Some(command.operation_id.clone());
    journal
        .state
        .operations
        .insert(command.operation_id.clone(), record);
    if journal.save().is_err() {
        return outcome(
            &command.operation_id,
            EffectOutcome::Rejected,
            None,
            None,
            None,
            None,
            json!({"diagnostic_code":"CHECKPOINT_WRITE_FAILED","native_replay":false}),
        );
    }
    let response = native
        .request(
            "thread/start",
            json!({"cwd":workspace,"modelProvider":provider,"model":model}),
        )
        .await;
    let response = match response {
        Ok(response) => response,
        Err(error) => {
            if let Some(record) = journal.state.operations.get_mut(&command.operation_id) {
                record.native_request_failure_code = Some(error.diagnostic_code().to_owned());
                record.native_rpc_error_code = error.rpc_code();
            }
            let saved = journal.save().is_ok();
            let mut details = base_details(&journal.state);
            details["diagnostic_code"] = json!(if saved {
                "THREAD_START_OUTCOME_UNKNOWN"
            } else {
                "CHECKPOINT_WRITE_FAILED"
            });
            details["native_replay"] = json!(false);
            with_native_failure_details(&mut details, &error, "thread_start_after_effect_marker");
            details["requested_model_provider"] = json!(provider);
            details["requested_model"] = json!(model);
            return outcome(
                &command.operation_id,
                EffectOutcome::Unknown,
                None,
                Some(&scope),
                None,
                None,
                details,
            );
        }
    };
    let started_thread = &response["thread"];
    let thread_id = started_thread["id"].as_str().filter(|s| !s.is_empty());
    let Some(thread_id) = thread_id else {
        let mut details = base_details(&journal.state);
        details["diagnostic_code"] = json!("THREAD_START_IDENTITY_MISSING");
        details["native_replay"] = json!(false);
        return outcome(
            &command.operation_id,
            EffectOutcome::Unknown,
            None,
            Some(&scope),
            None,
            None,
            details,
        );
    };
    journal.state.native_root_id = Some(thread_id.to_owned());
    journal.state.native_root_phase = Some(NativeRootPhase::Candidate);
    if let Some(record) = journal.state.operations.get_mut(&command.operation_id) {
        record.native_root_id = Some(thread_id.to_owned());
    }
    if journal.save().is_err() {
        return outcome(
            &command.operation_id,
            EffectOutcome::Unknown,
            Some(thread_id),
            Some(&scope),
            None,
            None,
            json!({"diagnostic_code":"CHECKPOINT_WRITE_FAILED","native_replay":false}),
        );
    }
    let readback = match native.read_thread(thread_id).await {
        Ok(response) => response["thread"].clone(),
        Err(error) => {
            let mut details = base_details(&journal.state);
            details["diagnostic_code"] = json!(error.diagnostic_code());
            details["native_replay"] = json!(false);
            with_native_failure_details(&mut details, &error, "thread_read_after_start");
            return outcome(
                &command.operation_id,
                EffectOutcome::Unknown,
                Some(thread_id),
                Some(&scope),
                None,
                None,
                details,
            );
        }
    };
    let adoption =
        adopt_verified_root(journal, &command.operation_id, &scope, thread_id, &readback);
    let mut details = base_details(&journal.state);
    details["native_replay"] = json!(false);
    let adoption_error = adoption.err();
    details["native_thread_readback"] = json!(if adoption_error.is_none() {
        "verified"
    } else {
        "mismatch"
    });
    details["thread"] = json!({
        "id": thread_id,
        "model_provider": readback["modelProvider"],
        "model": readback["model"],
        "cwd": readback["cwd"],
        "workspace_status": if readback["cwd"].as_str().and_then(normalize_path) == normalize_path(&workspace) { "workspace_exact" } else { "workspace_mismatch" },
    });
    if let Some(code) = adoption_error {
        details["diagnostic_code"] = json!(code);
        return outcome(
            &command.operation_id,
            EffectOutcome::Unknown,
            Some(thread_id),
            Some(&scope),
            None,
            None,
            details,
        );
    }
    details["completion_condition"] = json!("native_thread_opened");
    outcome(
        &command.operation_id,
        EffectOutcome::Applied,
        Some(thread_id),
        Some(&scope),
        None,
        None,
        details,
    )
}

async fn send_operation(
    command: &RuntimeCommand,
    journal: &mut Journal,
    native: &mut NativeClient,
    claim: &ModuleContractClaim,
    boot_id: &str,
) -> RuntimeOutcome {
    if let Some(previous) = journal.state.operations.get(&command.operation_id) {
        if let Some(result) = &previous.outcome {
            return decode_outcome(result, &command.operation_id);
        }
        let previous_is_current = match (
            previous.native_root_id.as_deref(),
            previous.native_scope_key.as_deref(),
            previous.requested_model_provider.as_deref(),
            previous.requested_model.as_deref(),
            previous.workspace_root.as_deref(),
        ) {
            (Some(root), Some(scope), Some(provider), Some(model), Some(workspace)) => journal
                .state
                .is_current_root(root, scope, provider, model, workspace),
            _ => false,
        };
        if !previous_is_current {
            return unknown_send(
                previous,
                &command.operation_id,
                "NATIVE_OPERATION_CONTEXT_CHANGED",
            );
        }
        if command.method == "task.dispatch"
            && module_contract::normalized_dispatch_enabled(claim)
            && previous.dispatch_admission.is_none()
        {
            return unknown_send(
                previous,
                &command.operation_id,
                "DISPATCH_ADMISSION_INTENT_MISSING",
            );
        }
        let continuation_context_present = command.input.get("goal_continuation_context").is_some();
        if continuation_context_present != previous.continuation_admission.is_some() {
            return unknown_send(
                previous,
                &command.operation_id,
                "GOAL_CONTINUATION_ADMISSION_INTENT_MISSING",
            );
        }
        if journal.state.native_scope_key.as_deref() != Some(native.scope_key().as_str()) {
            return unknown_send(previous, &command.operation_id, "NATIVE_SCOPE_CHANGED");
        }
        return reconcile_send(native, previous, &command.operation_id).await;
    }
    let (provider, model, workspace) = match validate_route(command) {
        Ok(route) => route,
        Err(code) => return rejected(command, code, &journal.state),
    };
    let canonical_workspace = normalize_path(&workspace).expect("validated absolute workspace");
    let root = match (journal.active_root_id(), command.native_root_id.as_deref()) {
        (Some(expected), Some(supplied)) if expected == supplied => expected.to_owned(),
        _ => return rejected(command, "NATIVE_IDENTITY_MISMATCH", &journal.state),
    };
    let Some(root_scope) = journal.state.native_scope_key.as_deref() else {
        return rejected(command, "NATIVE_SCOPE_CHANGED", &journal.state);
    };
    if !journal
        .state
        .is_current_root(&root, root_scope, &provider, &model, &workspace)
    {
        return rejected(command, "ROUTE_CONFIGURATION_MISMATCH", &journal.state);
    }
    let prompt = match prompt_for(command) {
        Ok(prompt) => prompt,
        Err(code) => return rejected(command, code, &journal.state),
    };
    let digest = digest_hex(prompt.as_bytes());
    let byte_count = prompt.len() as u64;
    let delivery = command.input["delivery"].as_str();
    if command.method == "agent.send" && !matches!(delivery, Some("next_turn" | "steer")) {
        return rejected(command, "DELIVERY_MODE_UNSUPPORTED", &journal.state);
    }
    let steer = command.method == "agent.send" && delivery == Some("steer");
    let expected_turn = if steer {
        match command.input["expected_turn_id"]
            .as_str()
            .filter(|s| !s.is_empty())
        {
            Some(id) => Some(id.to_owned()),
            None => return rejected(command, "EXPECTED_TURN_ID_REQUIRED", &journal.state),
        }
    } else {
        None
    };
    if !native.identity_is_known() {
        return rejected_before_input(
            command,
            &journal.state,
            &NativeError::IdentityUnverified,
            "server_identity",
            Some(&provider),
            Some(&model),
        );
    }
    let scope = native.scope_key();
    if journal.state.native_scope_key.as_deref() != Some(scope.as_str())
        || !native.identity_is_known()
    {
        return rejected(command, "NATIVE_SCOPE_CHANGED", &journal.state);
    }
    let thread = match native.read_thread(&root).await {
        Ok(value) => value["thread"].clone(),
        Err(error) => {
            return rejected_before_input(
                command,
                &journal.state,
                &error,
                "thread_read_preflight",
                Some(&provider),
                Some(&model),
            );
        }
    };
    if thread["id"].as_str() != Some(root.as_str())
        || thread["modelProvider"].as_str() != Some(provider.as_str())
        || thread["model"].as_str() != Some(model.as_str())
        || thread["cwd"].as_str().and_then(normalize_path) != normalize_path(&workspace)
    {
        return rejected(command, "THREAD_CONFIGURATION_MISMATCH", &journal.state);
    }
    if !steer && thread["status"]["type"] != "idle" {
        return rejected(command, "THREAD_NOT_IDLE", &journal.state);
    }
    let native_payload = native_send_payload(
        &root,
        &prompt,
        &model,
        &command.operation_id,
        steer,
        expected_turn.as_deref(),
    );
    let dispatch_admission =
        match normalized_dispatch_admission(command, claim, boot_id, &native_payload) {
            Ok(admission) => admission,
            Err(_) => return rejected(command, "TASK_DISPATCH_CONTEXT_INVALID", &journal.state),
        };
    let continuation_admission =
        match normalized_goal_continuation_admission(command, claim, boot_id, &native_payload) {
            Ok(admission) => admission,
            Err(_) => {
                return rejected(command, "GOAL_CONTINUATION_CONTEXT_INVALID", &journal.state);
            }
        };
    let mut record = OperationRecord::intent(&command.method, "send");
    record.input_sha256 = command.input_sha256.clone();
    record.native_root_id = Some(root.clone());
    record.native_scope_key = Some(scope.clone());
    record.requested_model_provider = Some(provider.clone());
    record.requested_model = Some(model.clone());
    record.workspace_root = Some(canonical_workspace);
    record.thread_configuration_readback = Some("verified".into());
    record.client_user_message_id = Some(command.operation_id.clone());
    record.prompt_sha256 = Some(digest);
    record.prompt_bytes = Some(byte_count);
    if command.method == "task.dispatch" {
        record.prompt_contract_revision =
            Some(swarm_contracts::task_prompt::TASK_PROMPT_CONTRACT_REVISION.to_owned());
    }
    record.delivery = Some(if steer { "steer" } else { "next_turn" }.into());
    record.expected_turn_id = expected_turn.clone();
    record.dispatch_admission = dispatch_admission;
    record.continuation_admission = continuation_admission;
    journal
        .state
        .operations
        .insert(command.operation_id.clone(), record);
    if journal.save().is_err() {
        return outcome(
            &command.operation_id,
            EffectOutcome::Rejected,
            Some(&root),
            Some(&scope),
            None,
            None,
            json!({"diagnostic_code":"CHECKPOINT_WRITE_FAILED","native_replay":false}),
        );
    }
    let request = if steer {
        native
            .request("turn/steer", native_payload.clone())
            .await
            .map(|response| {
                (
                    response["turnId"].as_str().map(str::to_owned),
                    Some("inProgress".to_owned()),
                )
            })
    } else {
        native
            .request("turn/start", native_payload)
            .await
            .map(|response| {
                (
                    response["turn"]["id"].as_str().map(str::to_owned),
                    response["turn"]["status"].as_str().map(str::to_owned),
                )
            })
    };
    match request {
        Ok((Some(turn_id), turn_status)) => {
            if let Some(record) = journal.state.operations.get_mut(&command.operation_id) {
                record.returned_turn_id = Some(turn_id);
                record.returned_turn_status = turn_status;
            }
            if journal.save().is_err() {
                let record = journal
                    .state
                    .operations
                    .get(&command.operation_id)
                    .expect("saved send marker remains");
                return unknown_send(record, &command.operation_id, "CHECKPOINT_WRITE_FAILED");
            }
        }
        Ok((None, _)) => {}
        Err(error) => {
            if let Some(record) = journal.state.operations.get_mut(&command.operation_id) {
                record.native_request_failure_code = Some(error.diagnostic_code().to_owned());
                record.native_rpc_error_code = error.rpc_code();
            }
            let _ = journal.save();
        }
    }
    let record = journal
        .state
        .operations
        .get(&command.operation_id)
        .expect("send marker persisted");
    reconcile_send(native, record, &command.operation_id).await
}

async fn reconcile_operation(
    command: &RuntimeCommand,
    journal: &mut Journal,
    native: &mut NativeClient,
    target_record: Option<OperationRecord>,
    claim: &ModuleContractClaim,
) -> Result<Vec<RuntimeOutcome>, AdapterError> {
    let Some(input_sha256) = command
        .input_sha256
        .as_deref()
        .filter(|digest| valid_sha256(digest))
    else {
        return Err(AdapterError::HostProtocol);
    };
    let target_id = command.input["operation_id"]
        .as_str()
        .filter(|s| !s.is_empty());
    let Some(target_id) = target_id else {
        let mut result = rejected(command, "RECONCILIATION_TARGET_REQUIRED", &journal.state);
        attach_command_receipt(&mut result, command, claim)?;
        return Ok(vec![result]);
    };
    let Some(target_input_sha256) = command.target_input_sha256.as_deref() else {
        let mut result = rejected(
            command,
            "RECONCILIATION_TARGET_DIGEST_REQUIRED",
            &journal.state,
        );
        attach_command_receipt(&mut result, command, claim)?;
        return Ok(vec![result]);
    };
    if !valid_sha256(target_input_sha256) {
        let mut result = rejected(
            command,
            "RECONCILIATION_TARGET_DIGEST_MISMATCH",
            &journal.state,
        );
        attach_command_receipt(&mut result, command, claim)?;
        return Ok(vec![result]);
    }
    let Some(target) = target_record else {
        let mut receipt = outcome(
            &command.operation_id,
            EffectOutcome::Applied,
            journal.state.native_root_id.as_deref(),
            journal.state.native_scope_key.as_deref(),
            None,
            None,
            json!({"completion_condition":"native_readback_completed","target_operation_id":target_id,"resolved":false,"disposition":"reconciliation_context_unavailable","native_replay":false}),
        );
        receipt.details["diagnostic_code"] = json!("RECONCILIATION_CONTEXT_UNAVAILABLE");
        attach_module_receipt(
            &mut receipt,
            claim,
            &command.binding_id,
            command.generation,
            &command.operation_id,
            input_sha256,
        )?;
        return Ok(vec![receipt]);
    };
    if target.input_sha256.as_deref() != Some(target_input_sha256) {
        let mut result = rejected(
            command,
            "RECONCILIATION_TARGET_DIGEST_MISMATCH",
            &journal.state,
        );
        attach_command_receipt(&mut result, command, claim)?;
        return Ok(vec![result]);
    }
    let method_kind_matches = matches!(
        (target.method.as_str(), target.kind.as_str()),
        ("agent.open", "open") | ("agent.send", "send") | ("task.dispatch", "send")
    );
    if !method_kind_matches {
        let mut result = rejected(command, "RECONCILIATION_TARGET_UNSUPPORTED", &journal.state);
        attach_command_receipt(&mut result, command, claim)?;
        return Ok(vec![result]);
    }
    let mut target_result = if let Some(value) = target.outcome.as_ref() {
        let result: RuntimeOutcome =
            serde_json::from_value(value.clone()).map_err(|_| AdapterError::Checkpoint)?;
        validate_saved_receipt(
            &result,
            claim,
            &command.binding_id,
            command.generation,
            target_id,
            target_input_sha256,
        )?;
        let mut result = result;
        annotate_root_applicability(&journal.state, &target, &mut result)?;
        Some(result)
    } else {
        None
    };
    let mut disposition = String::from("recorded_outcome");
    let prior_resolved = target_result.as_ref().is_some_and(|value| {
        matches!(
            value.outcome,
            EffectOutcome::Applied | EffectOutcome::Rejected
        )
    });
    let target_context_matches = match (
        target.native_root_id.as_deref(),
        target.native_scope_key.as_deref(),
        target.requested_model_provider.as_deref(),
        target.requested_model.as_deref(),
        target.workspace_root.as_deref(),
    ) {
        (Some(root), Some(scope), Some(provider), Some(model), Some(workspace)) => journal
            .state
            .root_context_matches(root, scope, provider, model, workspace),
        _ => false,
    };
    let target_is_current = match (
        target.native_root_id.as_deref(),
        target.native_scope_key.as_deref(),
        target.requested_model_provider.as_deref(),
        target.requested_model.as_deref(),
        target.workspace_root.as_deref(),
    ) {
        (Some(root), Some(scope), Some(provider), Some(model), Some(workspace)) => journal
            .state
            .is_current_root(root, scope, provider, model, workspace),
        _ => false,
    };
    if target.kind == "send" && !prior_resolved {
        let mut reconciled = None;
        if target_is_current {
            if !native.identity_is_known()
                || journal.state.native_scope_key.as_deref() != Some(native.scope_key().as_str())
            {
                reconciled = Some(unknown_send(&target, target_id, "NATIVE_SCOPE_CHANGED"));
            } else {
                reconciled = Some(reconcile_send(native, &target, target_id).await);
            }
        }
        let mut result = reconciled.unwrap_or_else(|| {
            unknown_send(
                &target,
                target_id,
                if target_is_current {
                    "NATIVE_CLIENT_UNAVAILABLE"
                } else {
                    "NATIVE_OPERATION_CONTEXT_CHANGED"
                },
            )
        });
        result.details["reconcile_operation_id"] = json!(command.operation_id.as_str());
        disposition = if matches!(result.outcome, EffectOutcome::Applied) {
            String::from("native_item_readback_verified")
        } else {
            result.details["diagnostic_code"]
                .as_str()
                .unwrap_or("native_readback_unresolved")
                .to_owned()
        };
        attach_module_receipt(
            &mut result,
            claim,
            &command.binding_id,
            command.generation,
            target_id,
            target_input_sha256,
        )?;
        journal.seal_goal_terminal_event(&mut result)?;
        journal.store_outcome(&result, &target.method, &target.kind)?;
        target_result = Some(result);
    }
    let legacy_candidate_needs_proof = target.kind == "open"
        && journal.state.native_root_phase == Some(NativeRootPhase::Candidate)
        && journal.state.root_operation_id.as_deref() == Some(target_id);
    if target.kind == "open" && (!prior_resolved || legacy_candidate_needs_proof) {
        if target.native_root_id.is_some() && target_context_matches {
            let mut value = reconcile_open(journal, native, &target, target_id).await;
            value.details["reconcile_operation_id"] = json!(command.operation_id.as_str());
            disposition = if matches!(value.outcome, EffectOutcome::Applied) {
                String::from("native_thread_readback_verified")
            } else {
                value.details["diagnostic_code"]
                    .as_str()
                    .unwrap_or("thread_start_uncorrelated")
                    .to_owned()
            };
            attach_module_receipt(
                &mut value,
                claim,
                &command.binding_id,
                command.generation,
                target_id,
                target_input_sha256,
            )?;
            journal.seal_goal_terminal_event(&mut value)?;
            journal.store_outcome(&value, &target.method, &target.kind)?;
            target_result = Some(value);
        } else {
            let mut unresolved = outcome(
                target_id,
                EffectOutcome::Unknown,
                target.native_root_id.as_deref(),
                target.native_scope_key.as_deref(),
                None,
                None,
                json!({
                    "diagnostic_code": if !target_context_matches { "NATIVE_OPERATION_CONTEXT_CHANGED" } else { target.native_request_failure_code.as_deref().unwrap_or("THREAD_START_OUTCOME_UNKNOWN") },
                    "native_request_failure_code": target.native_request_failure_code.as_deref(),
                    "native_rpc_error_code": target.native_rpc_error_code,
                    "requested_model_provider": target.requested_model_provider.as_deref(),
                    "requested_model": target.requested_model.as_deref(),
                    "native_replay":false
                }),
            );
            unresolved.details["reconcile_operation_id"] = json!(command.operation_id.as_str());
            disposition = if target_context_matches {
                String::from("thread_start_uncorrelated")
            } else {
                String::from("native_operation_context_changed")
            };
            attach_module_receipt(
                &mut unresolved,
                claim,
                &command.binding_id,
                command.generation,
                target_id,
                target_input_sha256,
            )?;
            journal.seal_goal_terminal_event(&mut unresolved)?;
            journal.store_outcome(&unresolved, &target.method, &target.kind)?;
            target_result = Some(unresolved);
        }
    }
    let resolved = target_result.as_ref().is_some_and(|value| {
        matches!(
            value.outcome,
            EffectOutcome::Applied | EffectOutcome::Rejected
        )
    });
    let mut reconcile = outcome(
        &command.operation_id,
        EffectOutcome::Applied,
        journal.state.native_root_id.as_deref(),
        journal.state.native_scope_key.as_deref(),
        None,
        None,
        json!({
            "completion_condition":"native_readback_completed",
            "target_operation_id":target_id,
            "resolved":resolved,
            "disposition":disposition,
            "native_replay":false,
        }),
    );
    attach_module_receipt(
        &mut reconcile,
        claim,
        &command.binding_id,
        command.generation,
        &command.operation_id,
        input_sha256,
    )?;
    let mut results = Vec::new();
    if let Some(result) = target_result {
        results.push(result);
    }
    results.push(reconcile);
    Ok(results)
}

async fn handle_command(
    command: RuntimeCommand,
    journal: &mut Journal,
    native: &mut NativeClient,
    claim: &ModuleContractClaim,
    boot_id: &str,
) -> Result<Vec<RuntimeOutcome>, AdapterError> {
    let Some(input_sha256) = command
        .input_sha256
        .as_deref()
        .filter(|digest| valid_sha256(digest))
    else {
        return Err(AdapterError::HostProtocol);
    };
    if command.operation_id.is_empty() || command.operation_id.len() > MAX_OPERATION_ID_BYTES {
        return Err(AdapterError::HostProtocol);
    }
    if journal.state.binding_id.as_deref() != Some(command.binding_id.as_str())
        || journal.state.generation != Some(command.generation)
    {
        return Err(AdapterError::HostProtocol);
    }
    let previous = journal.operation_record(&command.operation_id)?;
    if previous.as_ref().is_some_and(|record| {
        record.method != command.method
            || record
                .input_sha256
                .as_deref()
                .is_some_and(|saved| saved != input_sha256)
    }) {
        return Err(AdapterError::HostProtocol);
    }
    if let Some(record) = &previous {
        if record.input_sha256.as_deref() != Some(input_sha256) {
            return Err(AdapterError::Checkpoint);
        }
        // A saved page or its host-acknowledged tombstone is immutable. Retry
        // the exact pending bytes through report_pending; never re-read a
        // possibly changed native transcript for the same result Operation.
        if record.method == "agent.result"
            && record.kind == "result_page"
            && (record.pending_result_page.is_some()
                || record.result_page_acknowledgement.is_some())
        {
            return Ok(Vec::new());
        }
        if let Some(saved) = &record.outcome {
            let mut result: RuntimeOutcome =
                serde_json::from_value(saved.clone()).map_err(|_| AdapterError::Checkpoint)?;
            validate_saved_receipt(
                &result,
                claim,
                &command.binding_id,
                command.generation,
                &command.operation_id,
                input_sha256,
            )?;
            validate_saved_dispatch_admission(&result, &command, claim, boot_id)?;
            validate_saved_goal_continuation_admission(&result, &command, claim, boot_id)?;
            journal.validate_existing_goal_terminal_event(&result)?;
            if record.method == "agent.open"
                && record.kind == "open"
                && journal.state.native_root_phase == Some(NativeRootPhase::Candidate)
                && journal.state.root_operation_id.as_deref() == Some(command.operation_id.as_str())
                && record
                    .outcome
                    .as_ref()
                    .and_then(|saved| serde_json::from_value::<RuntimeOutcome>(saved.clone()).ok())
                    .is_some_and(|saved| {
                        matches!(
                            saved.outcome,
                            EffectOutcome::Applied | EffectOutcome::Accepted
                        )
                    })
            {
                return Ok(Vec::new());
            }
            annotate_root_applicability(&journal.state, record, &mut result)?;
            return Ok(vec![result]);
        }
    }
    let mut result = if previous.is_none()
        && !journal.can_start_operation(command.method == "agent.reconcile")?
    {
        let mut failure = rejected(&command, "JOURNAL_LIVE_CAPACITY_REACHED", &journal.state);
        attach_command_receipt(&mut failure, &command, claim)?;
        vec![failure]
    } else {
        let reconciliation_target = if command.method == "agent.reconcile" {
            command.input["operation_id"]
                .as_str()
                .map(|operation_id| journal.operation_record(operation_id))
                .transpose()?
                .flatten()
        } else {
            None
        };
        match command.method.as_str() {
            "agent.open" => {
                let mut result = open_operation(&command, journal, native).await;
                attach_command_receipt(&mut result, &command, claim)?;
                vec![result]
            }
            "task.dispatch" | "agent.send" => {
                let mut result = send_operation(&command, journal, native, claim, boot_id).await;
                attach_command_receipt(&mut result, &command, claim)?;
                vec![result]
            }
            "agent.reconcile" => {
                let result =
                    reconcile_operation(&command, journal, native, reconciliation_target, claim)
                        .await;
                journal.capture_native_events()?;
                result?
            }
            "agent.result" if module_contract::normalized_result_enabled(claim) => {
                match build_normalized_result_page(&command, journal, native, claim).await {
                    Ok(params) => {
                        journal.capture_native_events()?;
                        journal.store_result_page(&command.operation_id, input_sha256, params)?;
                        Vec::new()
                    }
                    Err(code) => {
                        let mut failure = rejected(&command, code, &journal.state);
                        attach_command_receipt(&mut failure, &command, claim)?;
                        vec![failure]
                    }
                }
            }
            _ => {
                let mut result = rejected(&command, "CAPABILITY_UNAVAILABLE", &journal.state);
                result.details["capability"] = json!(command.method);
                attach_command_receipt(&mut result, &command, claim)?;
                vec![result]
            }
        }
    };
    journal.capture_native_events()?;
    for outcome in &mut result {
        journal.seal_goal_terminal_event(outcome)?;
        let (method, kind) = if outcome.operation_id == command.operation_id {
            let kind = match command.method.as_str() {
                "agent.open" => String::from("open"),
                "agent.send" | "task.dispatch" => String::from("send"),
                "agent.reconcile" => String::from("reconcile"),
                "agent.result" => String::from("result_page"),
                _ => String::from("receipt"),
            };
            (command.method.clone(), kind)
        } else {
            let record = journal
                .operation_record(&outcome.operation_id)?
                .ok_or(AdapterError::Checkpoint)?;
            (record.method, record.kind)
        };
        journal.store_outcome(outcome, &method, &kind)?;
    }
    Ok(result)
}

async fn build_normalized_result_page(
    command: &RuntimeCommand,
    journal: &Journal,
    native: &mut NativeClient,
    claim: &ModuleContractClaim,
) -> Result<Value, &'static str> {
    if command.method != "agent.result" || !module_contract::normalized_result_enabled(claim) {
        return Err("CODEX_RESULT_CONTRACT_UNAVAILABLE");
    }
    let selector = &command.input["selector"];
    let selector_object = selector
        .as_object()
        .filter(|object| object.len() == 2)
        .ok_or("CODEX_RESULT_SELECTOR_INVALID")?;
    if selector_object.get("kind").and_then(Value::as_str) != Some("codex_assistant_result") {
        return Err("CODEX_RESULT_SELECTOR_UNSUPPORTED");
    }
    let target_operation_id = selector_object
        .get("input_operation_id")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or("CODEX_RESULT_SELECTOR_INVALID")?;
    let origin: NormalizedResultOriginContext =
        serde_json::from_value(command.input["normalized_result_origin"].clone())
            .map_err(|_| "CODEX_RESULT_ORIGIN_INVALID")?;
    origin
        .validate()
        .map_err(|_| "CODEX_RESULT_ORIGIN_INVALID")?;
    let result_input_sha256 = command
        .input_sha256
        .as_deref()
        .filter(|digest| valid_sha256(digest))
        .ok_or("CODEX_RESULT_INPUT_DIGEST_INVALID")?;
    let target_input_sha256 = command
        .target_input_sha256
        .as_deref()
        .filter(|digest| valid_sha256(digest))
        .ok_or("CODEX_RESULT_TARGET_DIGEST_INVALID")?;
    let (route_provider, route_model, route_workspace) =
        validate_route(command).map_err(|_| "CODEX_RESULT_ROUTE_INVALID")?;
    if command.binding_id != origin.binding_id
        || command.generation != origin.binding_generation
        || target_operation_id != origin.target_operation_id
        || target_input_sha256 != origin.target_input_sha256
        || journal.state.requested_model_provider.as_deref() != Some(route_provider.as_str())
        || journal.state.requested_model.as_deref() != Some(route_model.as_str())
        || journal
            .state
            .workspace_root
            .as_deref()
            .and_then(normalize_path)
            != normalize_path(&route_workspace)
        || digest_hex(
            canonical_json(selector)
                .map_err(|_| "CODEX_RESULT_SELECTOR_INVALID")?
                .as_bytes(),
        ) != origin.selector_sha256
    {
        return Err("CODEX_RESULT_ORIGIN_MISMATCH");
    }

    let target_record = journal
        .operation_record(target_operation_id)
        .map_err(|_| "CODEX_RESULT_TARGET_UNAVAILABLE")?
        .ok_or("CODEX_RESULT_TARGET_UNAVAILABLE")?;
    if target_record.method != "task.dispatch"
        || target_record.kind != "send"
        || target_record.input_sha256.as_deref() != Some(target_input_sha256)
    {
        return Err("CODEX_RESULT_TARGET_MISMATCH");
    }
    let encoded_outcome = target_record
        .outcome
        .as_ref()
        .ok_or("CODEX_RESULT_DISPATCH_RECEIPT_UNAVAILABLE")?;
    let dispatch_outcome: RuntimeOutcome = serde_json::from_value(encoded_outcome.clone())
        .map_err(|_| "CODEX_RESULT_DISPATCH_RECEIPT_INVALID")?;
    let target_receipt = receipt_identity_from_outcome(&dispatch_outcome)
        .map_err(|_| "CODEX_RESULT_DISPATCH_RECEIPT_INVALID")?;
    let expected_target_receipt = module_contract::receipt_identity(
        claim,
        &command.binding_id,
        command.generation,
        target_operation_id,
        target_input_sha256,
    )
    .map_err(|_| "CODEX_RESULT_DISPATCH_RECEIPT_INVALID")?;
    if dispatch_outcome.operation_id != target_operation_id
        || target_receipt != expected_target_receipt
        || !matches!(
            dispatch_outcome.outcome,
            EffectOutcome::Applied | EffectOutcome::Accepted
        )
    {
        return Err("CODEX_RESULT_DISPATCH_RECEIPT_INVALID");
    }
    let completion_condition = dispatch_outcome.details["completion_condition"]
        .as_str()
        .ok_or("CODEX_RESULT_DISPATCH_RECEIPT_INVALID")?;
    let execution_complete = dispatch_outcome.details["execution_complete"]
        .as_bool()
        .ok_or("CODEX_RESULT_DISPATCH_RECEIPT_INVALID")?;
    let task_completion = dispatch_outcome.details["task_completion"]
        .as_str()
        .ok_or("CODEX_RESULT_DISPATCH_RECEIPT_INVALID")?;
    let disposition = dispatch_outcome.details["disposition"]
        .as_str()
        .ok_or("CODEX_RESULT_DISPATCH_RECEIPT_INVALID")?;
    let producer_completed = completion_condition == "native_turn_completed"
        && execution_complete
        && task_completion == "unknown"
        && disposition == "completed"
        && matches!(dispatch_outcome.outcome, EffectOutcome::Applied);
    let producer_admitted = completion_condition == "native_input_admitted"
        && !execution_complete
        && task_completion == "unknown"
        && disposition == "admitted"
        && matches!(dispatch_outcome.outcome, EffectOutcome::Accepted);
    let origin_completed = origin.producer.completion_condition == "native_turn_completed"
        && origin.producer.execution_complete
        && origin.producer.task_completion == "unknown"
        && origin.producer.disposition == "completed";
    let origin_admitted = origin.producer.completion_condition == "native_input_admitted"
        && !origin.producer.execution_complete
        && origin.producer.task_completion == "unknown"
        && origin.producer.disposition == "admitted";
    let exact_producer_snapshot = origin.producer.completion_condition == completion_condition
        && origin.producer.execution_complete == execution_complete
        && origin.producer.task_completion == task_completion
        && origin.producer.disposition == disposition;
    // The Store seals producer facts when the result Operation is admitted.
    // A later reconciliation can monotonically upgrade the target's retained
    // outcome from input-admitted to turn-completed; it cannot rewrite that
    // earlier source snapshot.
    let monotone_completion = origin_admitted && producer_completed;
    if ((!producer_completed && !producer_admitted)
        || (!origin_completed && !origin_admitted)
        || (!exact_producer_snapshot && !monotone_completion))
        || origin.producer.module_receipt != target_receipt
    {
        return Err("CODEX_RESULT_DISPATCH_RECEIPT_MISMATCH");
    }
    let admission: TaskDispatchAdmissionReceipt =
        serde_json::from_value(dispatch_outcome.details["dispatch_admission"].clone())
            .map_err(|_| "CODEX_RESULT_DISPATCH_ADMISSION_INVALID")?;
    admission
        .validate()
        .map_err(|_| "CODEX_RESULT_DISPATCH_ADMISSION_INVALID")?;
    let context = admission.context();
    let native_input_id = origin
        .producer
        .native_input_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or("CODEX_RESULT_INPUT_ID_UNAVAILABLE")?;
    let turn_id = dispatch_outcome
        .turn_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or("CODEX_RESULT_TURN_ID_UNAVAILABLE")?;
    let native_root_id = dispatch_outcome
        .native_root_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or("CODEX_RESULT_THREAD_ID_UNAVAILABLE")?;
    let native_scope_key = dispatch_outcome
        .native_scope_key
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or("CODEX_RESULT_NATIVE_SCOPE_UNAVAILABLE")?;
    let native_prompt_sha256 = dispatch_outcome.details["prompt_sha256"]
        .as_str()
        .filter(|digest| valid_sha256(digest))
        .ok_or("CODEX_RESULT_DISPATCH_RECEIPT_INVALID")?;
    let native_prompt_bytes = dispatch_outcome.details["prompt_bytes"]
        .as_u64()
        .ok_or("CODEX_RESULT_DISPATCH_RECEIPT_INVALID")?;
    if admission.module_receipt != target_receipt
        || admission.operation_id != target_operation_id
        || admission.binding_id != command.binding_id
        || admission.binding_generation != command.generation
        || admission.worker_boot_id.trim().is_empty()
        || admission.native_input_id.as_deref() != Some(native_input_id)
        || dispatch_outcome.native_input_id.as_deref() != Some(native_input_id)
        || dispatch_outcome.details["client_user_message_id"] != target_operation_id
        || context.attempt_id != origin.attempt_id
        || context.task_id != origin.task_id
        || context.task_revision != origin.task_revision
        || context.task_snapshot_sha256 != origin.task_snapshot_sha256
        || context.source_text_sha256 != origin.producer.source_text_sha256
        || context.source_text_bytes != origin.producer.source_text_bytes
        || admission.native_payload_sha256 != origin.producer.native_payload_sha256
        || admission.native_payload_bytes != origin.producer.native_payload_bytes
        || origin.producer.assignment_id != target_operation_id
        || origin.producer.dispatch_operation_id != target_operation_id
        || target_record
            .client_user_message_id
            .as_deref()
            .is_some_and(|id| id != target_operation_id)
        || target_record
            .prompt_sha256
            .as_deref()
            .is_some_and(|digest| digest != native_prompt_sha256)
        || target_record
            .prompt_bytes
            .is_some_and(|bytes| bytes != native_prompt_bytes)
        || target_record
            .returned_turn_id
            .as_deref()
            .is_some_and(|saved| saved != turn_id)
        || target_record
            .native_root_id
            .as_deref()
            .is_some_and(|saved| saved != native_root_id)
        || target_record
            .native_scope_key
            .as_deref()
            .is_some_and(|saved| saved != native_scope_key)
        || target_record
            .workspace_root
            .as_deref()
            .and_then(normalize_path)
            .is_some_and(|saved| {
                Some(saved)
                    != journal
                        .state
                        .workspace_root
                        .as_deref()
                        .and_then(normalize_path)
            })
        || dispatch_outcome.details["requested_model_provider"].as_str()
            != journal.state.requested_model_provider.as_deref()
        || dispatch_outcome.details["requested_model"].as_str()
            != journal.state.requested_model.as_deref()
    {
        return Err("CODEX_RESULT_DISPATCH_ADMISSION_MISMATCH");
    }

    if !journal.state.is_current_root(
        native_root_id,
        native_scope_key,
        &route_provider,
        &route_model,
        &route_workspace,
    ) {
        return Err("CODEX_RESULT_NATIVE_SCOPE_CHANGED");
    }
    if !native.identity_is_known() || native.scope_key() != native_scope_key {
        return Err("CODEX_RESULT_NATIVE_SCOPE_CHANGED");
    }
    let thread = native
        .read_thread(native_root_id)
        .await
        .map_err(|error| error.diagnostic_code())?;
    let thread = &thread["thread"];
    if thread["id"].as_str() != Some(native_root_id)
        || thread["modelProvider"].as_str() != journal.state.requested_model_provider.as_deref()
        || thread["model"].as_str() != journal.state.requested_model.as_deref()
        || thread["cwd"].as_str().and_then(normalize_path)
            != journal
                .state
                .workspace_root
                .as_deref()
                .and_then(normalize_path)
    {
        return Err("CODEX_RESULT_THREAD_CONFIGURATION_MISMATCH");
    }
    let history = match native
        .read_history(native_root_id, target_operation_id)
        .await
    {
        HistoryRead::Complete(matches) => matches,
        HistoryRead::Truncated(_) => return Err("CODEX_RESULT_HISTORY_TRUNCATED"),
        HistoryRead::Failed(error) => return Err(error.diagnostic_code()),
    };
    if history.len() != 1 {
        return Err(if history.is_empty() {
            "CODEX_RESULT_INPUT_NOT_OBSERVED"
        } else {
            "CODEX_RESULT_INPUT_NOT_UNIQUE"
        });
    }
    let history = &history[0];
    if history.item_id != native_input_id
        || history.turn_id != turn_id
        || digest_hex(history.text.as_bytes()) != native_prompt_sha256
        || history.text.len() as u64 != native_prompt_bytes
    {
        return Err("CODEX_RESULT_INPUT_IDENTITY_MISMATCH");
    }
    let turn = match native.read_turn(native_root_id, turn_id).await {
        TurnRead::Found(turn) => turn,
        TurnRead::Missing => return Err("CODEX_RESULT_TURN_NOT_OBSERVED"),
        TurnRead::Truncated => return Err("CODEX_RESULT_TURN_READBACK_TRUNCATED"),
        TurnRead::Failed(error) => return Err(error.diagnostic_code()),
    };
    if turn["id"].as_str() != Some(turn_id) {
        return Err("CODEX_RESULT_TURN_IDENTITY_MISMATCH");
    }
    match turn["status"].as_str() {
        Some("completed") if turn.get("error").is_none_or(Value::is_null) => {}
        Some("completed") => return Err("CODEX_RESULT_TURN_CONTRADICTORY"),
        Some("failed" | "interrupted") => return Err("CODEX_RESULT_TURN_FAILED"),
        Some(_) => return Err("CODEX_RESULT_TURN_NOT_COMPLETED"),
        None => return Err("CODEX_RESULT_TURN_STATUS_UNAVAILABLE"),
    }
    let response = match native
        .read_final_assistant_response(native_root_id, turn_id)
        .await
    {
        AssistantRead::Found(response) => response,
        AssistantRead::Missing => return Err("CODEX_RESULT_FINAL_RESPONSE_NOT_OBSERVED"),
        AssistantRead::Truncated => return Err("CODEX_RESULT_RESPONSE_READBACK_TRUNCATED"),
        AssistantRead::Failed(error) => return Err(error.diagnostic_code()),
    };
    let body = response.text.as_bytes();
    if body.is_empty() {
        return Err("CODEX_RESULT_FINAL_RESPONSE_EMPTY");
    }
    if body.len() > MAX_NORMALIZED_RESULT_BODY_BYTES {
        return Err("CODEX_RESULT_RESPONSE_TOO_LARGE");
    }
    let response_sha256 = digest_hex(body);
    if let Some(expected) = command
        .input
        .get("normalized_result_payload_identity")
        .filter(|value| value.is_object())
        && (expected["sha256"].as_str() != Some(response_sha256.as_str())
            || expected["byte_length"].as_u64() != Some(body.len() as u64)
            || expected["complete"] == false)
    {
        return Err("CODEX_RESULT_PAYLOAD_IDENTITY_MISMATCH");
    }
    let offset = command.input["offset_bytes"].as_u64().unwrap_or(0);
    let requested = command.input["length_bytes"]
        .as_u64()
        .unwrap_or(MAX_NORMALIZED_RESULT_PAGE_BYTES as u64)
        .min(MAX_NORMALIZED_RESULT_PAGE_BYTES as u64);
    let total = body.len() as u64;
    if offset > total || (requested == 0 && offset < total) {
        return Err("CODEX_RESULT_RANGE_INVALID");
    }
    let end = offset.saturating_add(requested).min(total);
    let start = usize::try_from(offset).map_err(|_| "CODEX_RESULT_RANGE_INVALID")?;
    let stop = usize::try_from(end).map_err(|_| "CODEX_RESULT_RANGE_INVALID")?;
    let selected = &body[start..stop];
    let result_receipt = module_contract::receipt_identity(
        claim,
        &command.binding_id,
        command.generation,
        &command.operation_id,
        result_input_sha256,
    )
    .map_err(|_| "CODEX_RESULT_RECEIPT_INVALID")?;
    let source = NormalizedResultPageSource {
        schema_id: swarm_contracts::module_contract::NORMALIZED_RESULT_PAGE_SCHEMA_ID.to_owned(),
        schema_version: 1,
        origin,
        result_operation_id: command.operation_id.clone(),
        result_input_sha256: result_input_sha256.to_owned(),
        result_module_receipt: result_receipt,
        payload_sha256: response_sha256,
        payload_bytes: total,
        native_response_identity: Some(response.item_id),
        execution_complete: false,
        task_completion: "unknown".to_owned(),
        native_replay: false,
    };
    source
        .validate()
        .map_err(|_| "CODEX_RESULT_SOURCE_INVALID")?;
    Ok(json!({
        "operation_id":command.operation_id,
        "page":{
            "source":source,
            "offset_bytes":offset,
            "byte_length":selected.len(),
            "total_bytes":total,
            "eof":end == total,
            "media_type":"text/plain; charset=utf-8",
            "content_base64":base64::engine::general_purpose::STANDARD.encode(selected),
            "page_sha256":digest_hex(selected),
        }
    }))
}

async fn connect_host(
    config: &AdapterConfig,
    context: &module_contract::ModuleRuntimeContext,
    ipc: &IpcConfig,
) -> Result<ModuleLink, AdapterError> {
    ModuleLink::connect(&config.host_data_dir, &context.credential, ipc)
        .await
        .map_err(|_| AdapterError::Host)
}

async fn report_pending(
    host: &mut ModuleLink,
    journal: &mut Journal,
    normalized_results: bool,
) -> Result<(), AdapterError> {
    loop {
        let pages = journal.pending_result_pages();
        if pages.is_empty() {
            break;
        }
        for (operation_id, params) in pages {
            let response = host.result(params).await.map_err(|_| AdapterError::Host)?;
            journal.acknowledge_result_page(&operation_id, &response)?;
        }
    }
    loop {
        let batch = journal.pending_outcomes()?;
        if batch.is_empty() {
            break;
        }
        let mut compatibility_deferred = false;
        for result in batch {
            let record = journal
                .operation_record(&result.operation_id)?
                .ok_or(AdapterError::Checkpoint)?;
            journal.validate_existing_goal_terminal_event(&result)?;
            let mut result = result;
            annotate_root_applicability(&journal.state, &record, &mut result)?;
            if record.method == "agent.open"
                && record.kind == "open"
                && journal.state.native_root_phase == Some(NativeRootPhase::Candidate)
                && journal.state.root_operation_id.as_deref() == Some(result.operation_id.as_str())
                && record
                    .outcome
                    .as_ref()
                    .and_then(|saved| serde_json::from_value::<RuntimeOutcome>(saved.clone()).ok())
                    .is_some_and(|saved| {
                        matches!(
                            saved.outcome,
                            EffectOutcome::Applied | EffectOutcome::Accepted
                        )
                    })
            {
                compatibility_deferred = true;
                continue;
            }
            let operation_id = result.operation_id.clone();
            let value = serde_json::to_value(&result).map_err(|_| AdapterError::Checkpoint)?;
            host.outcome(value).await.map_err(|_| AdapterError::Host)?;
            journal.acknowledge_outcome(&operation_id)?;
        }
        if compatibility_deferred {
            break;
        }
    }
    let pending = journal.next_observation(false, normalized_results)?;
    host.observe(pending.clone())
        .await
        .map_err(|_| AdapterError::Host)?;
    journal.acknowledge_observation()?;
    Ok(())
}

/// Run the module protocol. The app-server is only contacted with WebSocket
/// client operations; this adapter contains no process start/stop code.
pub async fn run(config: AdapterConfig) -> Result<(), AdapterError> {
    if !config.host_data_dir.is_absolute() {
        return Err(AdapterError::Configuration);
    }
    let context = module_contract::load_runtime_context()?;
    let mut journal = Journal::open(context.state_dir.clone(), context.worker.boot_id.clone())?;
    let ipc: IpcConfig =
        serde_json::from_value(config.ipc.clone()).map_err(|_| AdapterError::Configuration)?;
    let mut credential_unavailable = false;
    let token = match config.token_env.as_deref() {
        Some(name) if !name.trim().is_empty() => match env::var(name) {
            Ok(value) if !value.trim().is_empty() => Some(value),
            _ => {
                credential_unavailable = true;
                None
            }
        },
        Some(_) => {
            credential_unavailable = true;
            None
        }
        None => None,
    };
    let endpoint = config.endpoint.clone();
    let mut native: Option<NativeClient> = None;
    loop {
        let mut host = match connect_host(&config, &context, &ipc).await {
            Ok(host) => host,
            Err(_) => {
                tokio::time::sleep(HOST_RETRY_DELAY).await;
                continue;
            }
        };
        let hello = json!({
            "boot_id": journal.state.boot_id,
            "module_artifact_id": ARTIFACT_ID,
            "native_root_id": journal.active_root_id(),
            "native_scope_key": if journal.active_root_id().is_some() { journal.state.native_scope_key.as_deref() } else { None },
            "native_ready": false,
            "managed_owner": context.worker.owner_record.clone(),
        });
        let hello_result = match host.hello(hello, Some(&context.claim)).await {
            Ok(value) => value,
            Err(_) => {
                tokio::time::sleep(HOST_RETRY_DELAY).await;
                continue;
            }
        };
        module_contract::validate_negotiated_hello(&hello_result, &context)?;
        journal.bind(&context.binding_id, context.generation)?;
        if native.is_none() {
            let events = journal.native_event_sink();
            native = Some(
                match NativeClient::attach(
                    &endpoint,
                    token.as_deref(),
                    credential_unavailable,
                    events.clone(),
                )
                .await
                {
                    Ok(native) => native,
                    Err(error) => NativeClient::unavailable(error, events),
                },
            );
        }
        let native = native.as_mut().expect("native session initialized once");
        if let Err(error) = native.refresh_usage_after_auth_change().await {
            native.events.record_fault(error.diagnostic_code());
        }
        journal.capture_native_events()?;
        let normalized_results = module_contract::normalized_result_enabled(&context.claim);
        match report_pending(&mut host, &mut journal, normalized_results).await {
            Ok(()) => {}
            Err(AdapterError::Host) => {
                tokio::time::sleep(HOST_RETRY_DELAY).await;
                continue;
            }
            Err(error) => return Err(error),
        }
        loop {
            let response = match host.next().await {
                Ok(value) => value,
                Err(_) => break,
            };
            if let Err(error) = native.refresh_usage_after_auth_change().await {
                native.events.record_fault(error.diagnostic_code());
            }
            let captured_events = journal.capture_native_events()?;
            if let Some(raw) = response.get("command").filter(|value| !value.is_null()) {
                let command: RuntimeCommand = match serde_json::from_value(raw.clone()) {
                    Ok(command) => command,
                    Err(_) => return Err(AdapterError::HostProtocol),
                };
                let outcomes = handle_command(
                    command,
                    &mut journal,
                    native,
                    &context.claim,
                    &context.worker.boot_id,
                )
                .await?;
                for result in outcomes {
                    let operation_id = result.operation_id.clone();
                    let value =
                        serde_json::to_value(&result).map_err(|_| AdapterError::Checkpoint)?;
                    if host.outcome(value).await.is_err() {
                        break;
                    }
                    journal.acknowledge_outcome(&operation_id)?;
                }
                match report_pending(&mut host, &mut journal, normalized_results).await {
                    Ok(()) => {}
                    Err(AdapterError::Host) => break,
                    Err(error) => return Err(error),
                }
            } else if captured_events {
                match report_pending(&mut host, &mut journal, normalized_results).await {
                    Ok(()) => {}
                    Err(AdapterError::Host) => break,
                    Err(error) => return Err(error),
                }
            } else {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        tokio::time::sleep(HOST_RETRY_DELAY).await;
    }
}
