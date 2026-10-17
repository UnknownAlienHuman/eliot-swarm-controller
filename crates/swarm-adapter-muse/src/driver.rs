//! Muse's owned command consumer. Native admission, readback, result delivery
//! and Task acceptance remain separate. No native retry occurs on IPC reconnect.
//!
//! Integration fences are intentional: journal v1 cannot restore the complete
//! Task/model/work context, journal child replies or session-MCP configuration.
//! Those paths fail closed rather than create an alternate private state file.

use crate::{
    commands::{
        self, CommandEncoding, EncodeContext, ModelConfiguration, PendingRequestFacts,
        RecoveryFacts, RequestedSelectors, ValidatedTaskPrompt, WorkFacts,
    },
    journal::{self, Journal, NativeInvocation, Scope},
    module_runtime::{ModuleOwnedBootstrap, NativeOptions, NativeRuntime, fault},
    msp::{MspTransport, Notification, PendingRequest, RequestError},
    observations::{
        ChildSnapshotInput, NativeEventInput, ObservationProjector, PendingInventoryInput,
        ProjectionFence, ReadMethod, ReductionStatus, RootSnapshotInput,
    },
    results::{self, ResultRequest},
    schema,
};
use jsonrpsee_core::server::RpcModule;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use swarm_client::ModuleLink;
use swarm_contracts::{
    EffectOutcome, ModuleReceiptIdentity, RuntimeCommand, RuntimeOutcome,
    TaskDispatchAdmissionReceipt,
    error::{Error, Result},
};
use swarm_process::read_private_durable;
use tokio::{sync::mpsc, task::JoinHandle};

const FINGERPRINT: &str = "sha256:7469c9e352e67def4a59df7e439984d7194fa351e1c8b7abb34060fd977ced81";
const MAX_PENDING: usize = 2_000;
const MAX_CONTEXTS: usize = 256;
const MAX_STREAM_ITEMS: usize = 2_000;

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis().min(i64::MAX as u128) as i64)
}
fn object(value: Value) -> Result<Map<String, Value>> {
    value
        .as_object()
        .cloned()
        .ok_or_else(|| fault("MUSE_NATIVE_SHAPE_INVALID"))
}
fn jf(error: journal::Fault) -> Error {
    fault(error.code())
}

#[derive(Clone)]
struct ControlRequest {
    method: String,
    params: Value,
    revision: u64,
}
#[derive(Clone)]
struct SavedCommand {
    command: RuntimeCommand,
    receipt: ModuleReceiptIdentity,
    invocation: NativeInvocation,
    task: Option<Arc<ValidatedTaskPrompt>>,
    requested: Option<RequestedSelectors>,
    // Original immutable work sample, never silently refreshed for a retry.
    work: Option<Value>,
    ack: Option<Value>,
    finished: bool,
}
struct TaskTurn {
    task: Arc<ValidatedTaskPrompt>,
    receipt: ModuleReceiptIdentity,
    turn: String,
    host_admitted: bool,
}
struct ItemIdentity {
    turn: String,
    kind: crate::streams::KnownItemKind,
    order: u32,
    disabled: bool,
}
struct StreamJob {
    facts: crate::streams::KnownRootTaskStream,
    notification: Notification,
    prior_sequence: u64,
}

struct State {
    scope: Option<Scope>,
    journal: Option<Journal>,
    confirmed_root: Option<String>,
    restored: bool,
    workspace: Option<std::path::PathBuf>,
    transport: Option<Arc<MspTransport>>,
    start_attempted: bool,
    initialized: bool,
    ready: bool,
    durability: commands::SessionDurability,
    fingerprint_warning: bool,
    session_mcp_granted: bool,
    projector: Option<ObservationProjector>,
    fence: Option<ProjectionFence>,
    event_number: u64,
    versions: BTreeMap<String, u64>,
    requested: RequestedSelectors,
    standing_effort: Option<String>,
    model: ModelConfiguration,
    pending: BTreeMap<String, ControlRequest>,
    contexts: BTreeMap<String, SavedCommand>,
    task_turns: BTreeMap<String, TaskTurn>,
    items: BTreeMap<String, ItemIdentity>,
    stream_sequences: BTreeMap<String, u64>,
    discovery_cursor: Option<String>,
    discovery_truncated: bool,
    dirty: u64,
    delivered: u64,
    diagnostic: Option<&'static str>,
    stream_gaps: u64,
}
impl State {
    fn empty() -> Self {
        Self {
            scope: None,
            journal: None,
            confirmed_root: None,
            restored: false,
            workspace: None,
            transport: None,
            start_attempted: false,
            initialized: false,
            ready: false,
            durability: commands::SessionDurability::Unrecognized,
            fingerprint_warning: false,
            session_mcp_granted: false,
            projector: None,
            fence: None,
            event_number: 0,
            versions: BTreeMap::new(),
            requested: RequestedSelectors::default(),
            standing_effort: None,
            model: ModelConfiguration::Unknown,
            pending: BTreeMap::new(),
            contexts: BTreeMap::new(),
            task_turns: BTreeMap::new(),
            items: BTreeMap::new(),
            stream_sequences: BTreeMap::new(),
            discovery_cursor: None,
            discovery_truncated: true,
            dirty: 1,
            delivered: 0,
            diagnostic: None,
            stream_gaps: 0,
        }
    }
    fn changed(&mut self) {
        self.dirty = self.dirty.saturating_add(1);
    }
    fn close_usage(&mut self, at_ms: i64) {
        if let (Some(projector), Some(fence)) = (&self.projector, &self.fence) {
            let closed = projector.close_usage_connection(fence, at_ms);
            if closed.status != ReductionStatus::Ignored {
                self.projector = Some(closed.projector);
                self.changed();
            }
        }
    }
    fn revision(&self, session: &str) -> u64 {
        *self.versions.get(session).unwrap_or(&0)
    }
    fn children(&self) -> Vec<String> {
        self.projector
            .as_ref()
            .and_then(|p| p.observation().ok())
            .and_then(|p| p["observed_children"].as_array().cloned())
            .unwrap_or_default()
            .iter()
            .filter_map(|c| c["sessionId"].as_str().map(str::to_owned))
            .collect()
    }
    fn member(&self, session: &str) -> bool {
        self.confirmed_root.as_deref() == Some(session)
            || self.children().iter().any(|id| id == session)
    }
}

pub struct Driver {
    bootstrap: Arc<ModuleOwnedBootstrap>,
    native: Arc<dyn NativeRuntime>,
    state: Mutex<State>,
    mutation: tokio::sync::Mutex<()>,
    stop: AtomicBool,
    event_task: tokio::sync::Mutex<Option<JoinHandle<()>>>,
    streams: mpsc::Sender<StreamJob>,
    stream_jobs: tokio::sync::Mutex<mpsc::Receiver<StreamJob>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ScopeMarker {
    format: String,
    version: u16,
    scope: Scope,
}

impl Driver {
    pub fn new(
        bootstrap: Arc<ModuleOwnedBootstrap>,
        native: Arc<dyn NativeRuntime>,
    ) -> Result<Arc<Self>> {
        let mut state = State::empty();
        let marker = bootstrap.state_dir.join("muse-rust-scope.v1.json");
        match std::fs::symlink_metadata(&marker) {
            Ok(_) => {
                // Only this new artifact's immutable marker is inspected. The
                // journal rejects legacy Node state and validates all contents.
                let saved: ScopeMarker =
                    serde_json::from_slice(&read_private_durable(&marker, 64 * 1024)?)
                        .map_err(|_| fault("MUSE_SCOPE_INVALID"))?;
                let scope = saved.scope;
                if saved.format != "eliot.muse.rust-private-state"
                    || saved.version != 1
                    || scope.module_client_id != bootstrap.credential.client_id
                    || scope.module_id != bootstrap.contract.module_id
                    || scope.artifact != bootstrap.contract.artifact
                    || scope.binding_id != bootstrap.binding_id
                    || scope.generation != bootstrap.generation
                {
                    return Err(fault("MUSE_SCOPE_MISMATCH"));
                }
                state.journal = Some(Journal::open_owned(scope.clone()).map_err(jf)?);
                state.scope = Some(scope.clone());
                state.restored = true;
                // The root in a prepared intent is not proof it ever existed.
                state.diagnostic = Some("MUSE_RESTORED_CONTEXT_READ_REQUIRED");
                install_projector(&mut state, &scope, &bootstrap.worker.boot_id)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(fault("MUSE_SCOPE_READ_UNKNOWN")),
        }
        Self::with_state(bootstrap, native, state)
    }
    fn with_state(
        bootstrap: Arc<ModuleOwnedBootstrap>,
        native: Arc<dyn NativeRuntime>,
        state: State,
    ) -> Result<Arc<Self>> {
        let (streams, receiver) = mpsc::channel(256);
        let driver = Arc::new(Self {
            bootstrap,
            native,
            state: Mutex::new(state),
            mutation: tokio::sync::Mutex::new(()),
            stop: AtomicBool::new(false),
            event_task: tokio::sync::Mutex::new(None),
            streams,
            stream_jobs: tokio::sync::Mutex::new(receiver),
        });
        Ok(driver)
    }
    fn lock(&self) -> Result<MutexGuard<'_, State>> {
        self.state
            .lock()
            .map_err(|_| fault("MUSE_DRIVER_LOCK_FAILED"))
    }
    pub fn stopping(&self) -> bool {
        self.stop.load(Ordering::Acquire)
    }
    pub fn request_stop(&self) {
        self.stop.store(true, Ordering::Release);
    }
    pub fn hello(&self) -> Value {
        let mut hello = json!({"boot_id":self.bootstrap.worker.boot_id,"module_artifact_id":self.bootstrap.contract.artifact.artifact_id,
            "native_ready":false,"managed_owner":{"token":self.bootstrap.worker.owner_record["token"],"process":self.bootstrap.worker.owner_record["process"]}});
        if let Ok(s) = self.lock() {
            if let (Some(root), Some(scope)) = (&s.confirmed_root, &s.scope) {
                hello["native_root_id"] = json!(root);
                hello["native_scope_key"] = json!(scope.native_namespace);
                hello["native_ready"] = json!(s.ready && !self.stopping());
            }
        }
        hello
    }
    fn receipt(&self, command: &RuntimeCommand) -> Result<ModuleReceiptIdentity> {
        let receipt = ModuleReceiptIdentity {
            schema_version: 1,
            module_id: self.bootstrap.contract.module_id.clone(),
            artifact: self.bootstrap.contract.artifact.clone(),
            protocol: self.bootstrap.contract.protocol.clone(),
            binding_id: self.bootstrap.binding_id.clone(),
            binding_generation: self.bootstrap.generation,
            operation_id: command.operation_id.clone(),
            input_sha256: command
                .input_sha256
                .clone()
                .ok_or_else(|| fault("MUSE_RECEIPT_INVALID"))?,
        };
        receipt
            .validate()
            .map_err(|_| fault("MUSE_RECEIPT_INVALID"))?;
        if command.binding_id != receipt.binding_id
            || command.generation != receipt.binding_generation
            || command.route["module_artifact_id"] != receipt.artifact.artifact_id.as_str()
        {
            return Err(fault("MUSE_RECEIPT_INVALID"));
        }
        Ok(receipt)
    }
    fn router(self: &Arc<Self>) -> Result<RpcModule<()>> {
        let mut router = RpcModule::new(());
        for method in ["approval/request", "userInput/request"] {
            let weak = Arc::downgrade(self);
            router
                .register_method(method, move |params, _, _| {
                    let value: Value = params.parse()?;
                    schema::validate_method_parameters(method, &value).map_err(|_| {
                        jsonrpsee_types::ErrorObjectOwned::owned(
                            -32602,
                            "MUSE_REQUEST_INVALID",
                            None::<()>,
                        )
                    })?;
                    let driver = weak.upgrade().ok_or_else(|| {
                        jsonrpsee_types::ErrorObjectOwned::owned(
                            -32603,
                            "MUSE_DRIVER_STOPPED",
                            None::<()>,
                        )
                    })?;
                    driver.control_request(method, value).map_err(|_| {
                        jsonrpsee_types::ErrorObjectOwned::owned(
                            -32603,
                            "MUSE_PENDING_REQUEST_UNAVAILABLE",
                            None::<()>,
                        )
                    })?;
                    // MSP presentation receipt only. No approval/answer decision.
                    jsonrpsee_core::RpcResult::<Value>::Ok(json!({}))
                })
                .map_err(|_| fault("MUSE_ROUTER_INVALID"))?;
        }
        Ok(router)
    }
    fn control_request(&self, method: &str, params: Value) -> Result<()> {
        let session = params["sessionId"]
            .as_str()
            .ok_or_else(|| fault("MUSE_REQUEST_INVALID"))?;
        let id = params[if method == "approval/request" {
            "approvalId"
        } else {
            "userInputId"
        }]
        .as_str()
        .ok_or_else(|| fault("MUSE_REQUEST_INVALID"))?;
        let mut s = self.lock()?;
        if !s.member(session) {
            return Err(fault("MUSE_REQUEST_OUTSIDE_FAMILY"));
        }
        if s.pending.len() >= MAX_PENDING || serde_json::to_vec(&params)?.len() > 64 * 1024 {
            return Err(fault("MUSE_PENDING_CAPACITY"));
        }
        let revision = s
            .revision(session)
            .checked_add(1)
            .ok_or_else(|| fault("MUSE_EVENT_REVISION_EXHAUSTED"))?;
        s.versions.insert(session.into(), revision);
        s.event_number = s
            .event_number
            .checked_add(1)
            .ok_or_else(|| fault("MUSE_EVENT_REVISION_EXHAUSTED"))?;
        if let (Some(projector), Some(fence)) = (&s.projector, &s.fence) {
            s.projector = Some(
                projector
                    .apply_event(NativeEventInput {
                        fence,
                        event_revision: s.event_number,
                        at_ms: now(),
                        event: &json!({"method":method,"params":params}),
                    })
                    .projector,
            );
        }
        s.pending.insert(
            format!("{session}:{method}:{id}"),
            ControlRequest {
                method: method.into(),
                params,
                revision,
            },
        );
        s.changed();
        Ok(())
    }
    async fn start(self: &Arc<Self>, command: &RuntimeCommand) -> Result<Arc<MspTransport>> {
        {
            let mut s = self.lock()?;
            if let Some(transport) = &s.transport {
                return if s.initialized {
                    Ok(transport.clone())
                } else {
                    Err(fault("MUSE_INITIALIZE_UNRESOLVED"))
                };
            }
            if s.start_attempted || self.stopping() {
                return Err(fault("MUSE_NATIVE_OWNER_REATTACH_REFUSED"));
            }
            // Even startup cancellation is potentially post-spawn. Never retry
            // it here or manufacture native/family NoEffect evidence.
            s.start_attempted = true;
        }
        let options = NativeOptions::from_route(&command.route, &self.bootstrap.native_executable)?;
        self.lock()?.workspace = Some(options.workspace.clone());
        let attachment = self.native.start(&options, self.router()?).await?;
        let transport = attachment.transport.clone();
        self.lock()?.transport = Some(transport.clone());
        let weak = Arc::downgrade(self);
        let mut notifications = attachment.notifications;
        *self.event_task.lock().await = Some(tokio::spawn(async move {
            while let Some(notification) = notifications.recv().await {
                let Some(driver) = weak.upgrade() else {
                    return;
                };
                if driver.notification(notification).is_err() {
                    driver.mark_gap("MUSE_NOTIFICATION_PROJECTION_UNAVAILABLE");
                }
            }
            if let Some(driver) = weak.upgrade() {
                if let Ok(mut s) = driver.lock() {
                    s.ready = false;
                    s.close_usage(now());
                    s.diagnostic = Some("MUSE_NATIVE_TRANSPORT_ENDED");
                    s.changed();
                }
            }
        }));
        let init=read_rpc(&transport,"initialize",json!({"clientInfo":{"name":"eliot_swarm_controller","version":"0.1.0"},"capabilities":{"requestedCapabilities":["sessionMcp"]}})).await?;
        validate_initialize(&init)?;
        let namespace = native_namespace(&init)?;
        {
            let s = self.lock()?;
            if s.scope
                .as_ref()
                .is_some_and(|scope| scope.native_namespace != namespace)
            {
                return Err(fault("MUSE_NATIVE_NAMESPACE_MISMATCH"));
            }
        }
        transport
            .notify("initialized", Map::new())
            .map_err(|_| fault("MUSE_INITIALIZED_UNAVAILABLE"))?
            .await
            .map_err(|_| fault("MUSE_INITIALIZED_UNAVAILABLE"))?
            .map_err(|_| fault("MUSE_INITIALIZED_UNAVAILABLE"))?;
        let mut s = self.lock()?;
        s.initialized = true;
        s.durability = commands::session_durability(&init);
        s.fingerprint_warning = init["schema"]["fingerprint"] != FINGERPRINT;
        s.session_mcp_granted = init["grantedCapabilities"]
            .as_array()
            .is_some_and(|grants| grants.iter().any(|g| g == "sessionMcp"));
        // Only observed invocation facts; no native auth/config bodies copied.
        if s.scope.is_none() {
            let root = uuid::Uuid::now_v7().to_string();
            let scope = Scope {
                module_client_id: self.bootstrap.credential.client_id.clone(),
                module_id: self.bootstrap.contract.module_id.clone(),
                artifact: self.bootstrap.contract.artifact.clone(),
                binding_id: self.bootstrap.binding_id.clone(),
                generation: self.bootstrap.generation,
                native_namespace: namespace,
                native_root_id: root,
            };
            s.journal = Some(Journal::create_owned(scope.clone()).map_err(jf)?);
            s.scope = Some(scope.clone());
            install_projector(&mut s, &scope, &self.bootstrap.worker.boot_id)?;
        }
        let scope = s
            .scope
            .clone()
            .ok_or_else(|| fault("MUSE_SCOPE_UNAVAILABLE"))?;
        s.journal
            .as_mut()
            .ok_or_else(|| fault("MUSE_JOURNAL_UNAVAILABLE"))?
            .record_durability(
                &scope,
                journal::Durability::from_initialize_declaration(init.get("sessionDurability")),
            )
            .map_err(jf)?;
        if let (Some(p), Some(f)) = (&s.projector, &s.fence) {
            s.projector = Some(
                p.begin_usage_connection(f, &self.bootstrap.worker.boot_id, now())
                    .projector,
            );
        }
        s.changed();
        Ok(transport)
    }

    pub async fn execute(self: Arc<Self>, command: RuntimeCommand) -> Result<()> {
        let receipt = self.receipt(&command)?;
        // Serialize mutations/readback context, not the independently drained
        // event stream or host outbox. No mutex guard crosses an RPC await.
        let _command_guard = self.mutation.lock().await;
        if self.stopping() {
            return Err(fault("MUSE_DRIVER_STOPPED"));
        }
        let result = self.execute_inner(&command, &receipt).await;
        if let Err(error) = result {
            // Conservative after any start/prepare/send ambiguity; never turn
            // child/transport failure into a NoEffect or Task completion claim.
            let mut s = self.lock()?;
            s.diagnostic = Some("MUSE_COMMAND_UNRESOLVED");
            s.changed();
            let Some(scope) = s.scope.clone() else {
                return Err(error);
            };
            let effect = if error.code == "MUSE_NATIVE_COMMAND_REJECTED" {
                EffectOutcome::Rejected
            } else {
                EffectOutcome::Unknown
            };
            let mut outcome = outcome(
                &receipt,
                &scope,
                effect,
                json!({"diagnostic_code":closed(&error.code),"execution_complete":false}),
                None,
                None,
            );
            if s.confirmed_root.is_none() {
                outcome.native_root_id = None;
            }
            s.journal
                .as_mut()
                .ok_or_else(|| fault("MUSE_JOURNAL_UNAVAILABLE"))?
                .queue_outcome(&scope, receipt, outcome)
                .map_err(jf)?;
        }
        Ok(())
    }
    async fn execute_inner(
        self: &Arc<Self>,
        command: &RuntimeCommand,
        receipt: &ModuleReceiptIdentity,
    ) -> Result<()> {
        let starts = matches!(
            command.method.as_str(),
            "agent.open" | "agent.recover" | "agent.reconcile"
        );
        let transport = if starts {
            self.start(command).await?
        } else {
            let s = self.lock()?;
            if !s.initialized {
                return Err(fault("MUSE_NATIVE_NOT_OPEN"));
            }
            s.transport
                .clone()
                .ok_or_else(|| fault("MUSE_NATIVE_NOT_OPEN"))?
        };
        if command.method == "agent.reconcile" {
            return self.reconcile(command, receipt, &transport).await;
        }
        let (root, restored, ready) = {
            let s = self.lock()?;
            (
                s.scope
                    .as_ref()
                    .map(|s| s.native_root_id.clone())
                    .ok_or_else(|| fault("MUSE_SCOPE_UNAVAILABLE"))?,
                s.restored,
                s.ready,
            )
        };
        if command.method == "agent.open" {
            // A restored prepared start may only be reconciled under its same
            // command identity, never replaced by another root/start request.
            let s = self.lock()?;
            let scope = s
                .scope
                .as_ref()
                .ok_or_else(|| fault("MUSE_SCOPE_UNAVAILABLE"))?;
            if restored
                || s.confirmed_root.is_some()
                || !s.contexts.is_empty()
                || !s
                    .journal
                    .as_ref()
                    .ok_or_else(|| fault("MUSE_JOURNAL_UNAVAILABLE"))?
                    .unresolved_native(scope)
                    .map_err(jf)?
                    .is_empty()
            {
                return Err(fault("MUSE_ROOT_CREATION_RECONCILE_REQUIRED"));
            }
        } else if command.method == "agent.recover" {
            let snapshot = self.snapshot(&transport, &root).await?;
            if snapshot.value["sessionId"] != root {
                return Err(fault("MUSE_NATIVE_ROOT_MISMATCH"));
            }
            // A root marker alone does not authorize resuming an ambiguous
            // session/start. Journal's unresolved original must be reconciled.
            let mut s = self.lock()?;
            let scope = s
                .scope
                .clone()
                .ok_or_else(|| fault("MUSE_SCOPE_UNAVAILABLE"))?;
            if !s
                .journal
                .as_ref()
                .ok_or_else(|| fault("MUSE_JOURNAL_UNAVAILABLE"))?
                .unresolved_native(&scope)
                .map_err(jf)?
                .is_empty()
            {
                return Err(fault("MUSE_PRIOR_NATIVE_COMMAND_UNRESOLVED"));
            }
            s.journal
                .as_mut()
                .ok_or_else(|| fault("MUSE_JOURNAL_UNAVAILABLE"))?
                .record_exact_root_read(&scope, &root)
                .map_err(jf)?;
            s.confirmed_root = Some(root.clone());
        } else if !ready {
            return Err(fault("MUSE_NATIVE_RECOVERY_REQUIRED"));
        }
        if restored
            && !matches!(
                command.method.as_str(),
                "agent.recover" | "agent.refresh" | "agent.result"
            )
        {
            return Err(fault("MUSE_DURABLE_CONTEXT_EXTENSION_REQUIRED"));
        }
        if command.method == "agent.reply" {
            let session = command.input["reply"]["params"]["sessionId"]
                .as_str()
                .ok_or_else(|| fault("MUSE_REPLY_SESSION_INVALID"))?;
            self.refresh_one(&transport, session).await?;
        }
        let result_input = if command.method == "agent.result" {
            ResultRequest::parse(&command.input).map_err(|e| fault(e.code))?;
            Some(command.input.clone())
        } else {
            None
        };
        let needs_work = matches!(
            command.method.as_str(),
            "agent.send" | "task.dispatch" | "agent.recover"
        ) || command.method == "agent.goal"
            && matches!(
                command.input["action"].as_str(),
                Some("set" | "edit" | "resume")
            );
        let work = if needs_work {
            Some(self.snapshot(&transport, &root).await?)
        } else {
            None
        };
        let id = uuid::Uuid::now_v7().to_string();
        let (encoding, revision) = {
            let s = self.lock()?;
            let children = s.children();
            let pending = if command.method == "agent.reply" {
                find_request(&s, &command.input)
            } else {
                None
            };
            let current = s.revision(&root);
            let cx = EncodeContext {
                expected_receipt: receipt,
                worker_boot_id: &self.bootstrap.worker.boot_id,
                native_root_id: s.confirmed_root.as_deref(),
                fresh_root_session_id: if command.method == "agent.open" {
                    Some(&root)
                } else {
                    None
                },
                native_command_id: Some(&id),
                initialized: s.initialized,
                observed_children: &children,
                requested: &s.requested,
                work: work.as_ref().map(|w| WorkFacts {
                    read_method: "session/read",
                    session_id: &root,
                    model_id: w.value["modelId"].as_str(),
                    provider_id: w.value["providerId"].as_str(),
                    status: w.value["status"].as_str().unwrap_or("unknown"),
                    active_turn_id: w.value["activeTurnId"].as_str(),
                    sampled_event_version: w.revision,
                    current_event_version: current,
                    model_configuration: s.model,
                    standing_effort: s.standing_effort.as_deref(),
                }),
                pending_request: pending.map(|p| PendingRequestFacts {
                    method: &p.method,
                    params: &p.params,
                    sampled_event_version: p.revision,
                    current_event_version: s.revision(p.params["sessionId"].as_str().unwrap_or("")),
                }),
                recovery: Some(RecoveryFacts {
                    durability: s.durability,
                    native_ready: s.ready,
                }),
                validated_result_input: result_input.as_ref(),
                session_mcp_granted: s.session_mcp_granted,
            };
            (
                commands::encode(command, &cx)
                    .map_err(|_| fault("MUSE_COMMAND_CONTRACT_REJECTED"))?,
                current,
            )
        };
        match encoding {
            CommandEncoding::Native {
                request,
                task_prompt,
                requested,
            } => {
                let (pending, saved) = {
                    let mut s = self.lock()?;
                    let scope = s
                        .scope
                        .clone()
                        .ok_or_else(|| fault("MUSE_SCOPE_UNAVAILABLE"))?;
                    if s.contexts.len() >= MAX_CONTEXTS {
                        return Err(fault("MUSE_COMMAND_CONTEXT_CAPACITY"));
                    }
                    // These exact public payloads need a journal contract extension.
                    // Do not flatten model, discard MCP fields or retarget a child.
                    if request.params["sessionId"] != scope.native_root_id {
                        return Err(fault("MUSE_JOURNAL_CHILD_REPLY_EXTENSION_REQUIRED"));
                    }
                    if request.params.contains_key("config") {
                        return Err(fault("MUSE_JOURNAL_MCP_EXTENSION_REQUIRED"));
                    }
                    if request.params.contains_key("model") {
                        return Err(fault("MUSE_JOURNAL_MODEL_EXTENSION_REQUIRED"));
                    }
                    let invocation =
                        NativeInvocation::new(&scope, request.method, &id, request.params)
                            .map_err(jf)?;
                    if needs_work
                        && (s.revision(&root) != revision
                            || work.as_ref().is_some_and(|w| w.revision != revision))
                    {
                        return Err(fault("MUSE_WORK_SAMPLE_STALE"));
                    }
                    if command.method == "agent.reply"
                        && find_request(&s, &command.input).is_none_or(|p| {
                            p.revision != s.revision(p.params["sessionId"].as_str().unwrap_or(""))
                        })
                    {
                        return Err(fault("MUSE_PENDING_REQUEST_STALE"));
                    }
                    let saved = SavedCommand {
                        command: command.clone(),
                        receipt: receipt.clone(),
                        invocation: invocation.clone(),
                        task: task_prompt.map(Arc::new),
                        requested,
                        work: work.as_ref().map(|w| w.value.clone()),
                        ack: None,
                        finished: false,
                    };
                    let (method, params) = {
                        let guard = s
                            .journal
                            .as_mut()
                            .ok_or_else(|| fault("MUSE_JOURNAL_UNAVAILABLE"))?
                            .prepare_native(&scope, receipt.clone(), invocation.clone())
                            .map_err(jf)?;
                        let exact = guard.invocation();
                        // Final event/work check and enqueue have no intervening
                        // await. The state mutex also excludes event reductions.
                        (exact.method().to_owned(), exact.params().map_err(jf)?)
                    };
                    s.contexts
                        .insert(command.operation_id.clone(), saved.clone());
                    let pending = transport
                        .submit_request(&method, params)
                        .map_err(|_| fault("MUSE_NATIVE_WRITE_UNCERTAIN"))?;
                    (pending, saved)
                };
                let ack = await_pending(pending).await?;
                self.complete(&saved, ack, &transport).await
            }
            CommandEncoding::RefreshFirstRead { session_id, .. } => {
                self.refresh(&transport, &session_id).await?;
                self.queue(receipt,EffectOutcome::Applied,json!({"completion_condition":"native_read_completed","execution_complete":false}),None,None)
            }
            CommandEncoding::ResultReadRequired { validated_input } => {
                let page = results::read_result(&transport, &validated_input, |selector| {
                    !self.stopping() && self.lock().is_ok_and(|s| s.member(selector.session_id()))
                })
                .await
                .map_err(|e| fault(e.code))?;
                let mut s = self.lock()?;
                let scope = s
                    .scope
                    .clone()
                    .ok_or_else(|| fault("MUSE_SCOPE_UNAVAILABLE"))?;
                s.journal
                    .as_mut()
                    .ok_or_else(|| fault("MUSE_JOURNAL_UNAVAILABLE"))?
                    .queue_result(&scope, receipt.clone(), page)
                    .map_err(jf)
            }
            CommandEncoding::ReconciliationRequired(_) => {
                Err(fault("MUSE_RECONCILIATION_INTERNAL_INVALID"))
            }
        }
    }

    async fn complete(
        &self,
        saved: &SavedCommand,
        ack: Value,
        transport: &Arc<MspTransport>,
    ) -> Result<()> {
        self.finish_ack(saved, ack)?;
        if matches!(
            saved.invocation.method(),
            "session/start" | "session/resume"
        ) {
            let root = self.lock()?.confirmed_root.clone();
            if let Some(root) = root {
                let _ = self.refresh(transport, &root).await;
            }
        }
        Ok(())
    }
    fn finish_ack(&self, saved: &SavedCommand, ack: Value) -> Result<()> {
        let method = saved.invocation.method();
        validate_ack(method, &ack)?;
        let mut s = self.lock()?;
        let scope = s
            .scope
            .clone()
            .ok_or_else(|| fault("MUSE_SCOPE_UNAVAILABLE"))?;
        s.journal
            .as_mut()
            .ok_or_else(|| fault("MUSE_JOURNAL_UNAVAILABLE"))?
            .record_native_ack(
                &scope,
                &saved.receipt.operation_id,
                saved.invocation.command_id(),
                ack.clone(),
            )
            .map_err(jf)?;
        if let Some(entry) = s.contexts.get_mut(&saved.receipt.operation_id) {
            entry.ack = Some(ack.clone());
        }
        let mut details =
            json!({"execution_complete":false,"native_command_id":saved.invocation.command_id()});
        let mut turn = None;
        let mut input = None;
        match method {
            "session/start" | "session/resume" => {
                let definition = if method == "session/start" {
                    "SessionStartResult"
                } else {
                    "SessionResumeResult"
                };
                schema::validate_definition(definition, &ack)
                    .map_err(|_| fault("MUSE_LIFECYCLE_ACK_INVALID"))?;
                if ack["session"]["sessionId"] != scope.native_root_id {
                    return Err(fault("MUSE_NATIVE_ROOT_MISMATCH"));
                }
                if !same_workspace(
                    &ack["session"]["workspaceRoot"],
                    &saved.command.route["native_options"]["workspaceRoot"],
                ) {
                    return Err(fault("MUSE_NATIVE_WORKSPACE_MISMATCH"));
                }
                let requested = saved.requested.as_ref().unwrap_or(&s.requested);
                commands::validate_requested_readback(
                    requested,
                    ack["session"]["modelId"].as_str(),
                    ack["session"]["providerId"].as_str(),
                )
                .map_err(|_| fault("MUSE_MODEL_READBACK_MISMATCH"))?;
                if let Some(requested) = &saved.requested {
                    s.requested = requested.clone();
                }
                s.confirmed_root = Some(scope.native_root_id.clone());
                s.ready = true;
                s.model = if s.restored {
                    ModelConfiguration::Unknown
                } else {
                    ModelConfiguration::Settled
                };
                details["completion_condition"] = json!(if method == "session/start" {
                    "native_session_created"
                } else {
                    "native_session_resumed"
                });
                if method == "session/resume" {
                    details["resume_boot_id"] = json!(self.bootstrap.worker.boot_id);
                    details["model_input_replayed"] = json!(false);
                }
            }
            "turn/start" | "turn/steer" => {
                let native_turn = ack["turnId"]
                    .as_str()
                    .filter(|t| !t.is_empty())
                    .ok_or_else(|| fault("MUSE_INPUT_ACK_INVALID"))?
                    .to_owned();
                turn = Some(native_turn.clone());
                input = Some(saved.invocation.command_id().to_owned());
                details["completion_condition"] = json!("native_input_admitted");
                if let Some(task) = &saved.task {
                    let admission =
                        task_admission(task, &saved.receipt, saved.invocation.command_id());
                    admission
                        .validate()
                        .map_err(|_| fault("MUSE_TASK_ADMISSION_INVALID"))?;
                    details["dispatch_admission"] = serde_json::to_value(admission)?;
                    if s.task_turns
                        .get(&native_turn)
                        .is_some_and(|old| old.receipt != saved.receipt)
                    {
                        return Err(fault("MUSE_TASK_TURN_AMBIGUOUS"));
                    }
                    let host_admitted = s
                        .task_turns
                        .get(&native_turn)
                        .is_some_and(|old| old.host_admitted);
                    s.task_turns.insert(
                        native_turn.clone(),
                        TaskTurn {
                            task: task.clone(),
                            receipt: saved.receipt.clone(),
                            turn: native_turn,
                            host_admitted,
                        },
                    );
                }
            }
            "session/setReasoningEffort" => {
                let params = saved.invocation.params().map_err(jf)?;
                s.standing_effort = params["reasoningEffort"].as_str().map(str::to_owned);
                s.requested.reasoning_effort = s.standing_effort.clone();
                details["completion_condition"] = json!("native_configuration_applied");
                details["boundary"] = json!("subsequent_turns");
            }
            "session/setApprovalMode" => {
                if ack["effectiveMode"]["mode"] != saved.invocation.params().map_err(jf)?["mode"] {
                    return Err(fault("MUSE_APPROVAL_MODE_READBACK_MISMATCH"));
                }
                details["completion_condition"] = json!("native_configuration_applied");
            }
            "session/setModel" => {
                s.model = ModelConfiguration::Pending;
                return Err(fault("MUSE_JOURNAL_MODEL_EXTENSION_REQUIRED"));
            }
            _ => {
                details["completion_condition"] = json!(if method.starts_with("goal/") {
                    "native_goal_admitted"
                } else {
                    "native_reply_admitted"
                });
            }
        }
        let value = outcome(
            &saved.receipt,
            &scope,
            EffectOutcome::Applied,
            details,
            turn,
            input,
        );
        s.journal
            .as_mut()
            .ok_or_else(|| fault("MUSE_JOURNAL_UNAVAILABLE"))?
            .queue_outcome(&scope, saved.receipt.clone(), value)
            .map_err(jf)?;
        if let Some(entry) = s.contexts.get_mut(&saved.receipt.operation_id) {
            entry.finished = true;
        }
        s.diagnostic = None;
        s.changed();
        Ok(())
    }

    async fn reconcile(
        &self,
        command: &RuntimeCommand,
        receipt: &ModuleReceiptIdentity,
        transport: &Arc<MspTransport>,
    ) -> Result<()> {
        let target = command.input["operation_id"]
            .as_str()
            .ok_or_else(|| fault("MUSE_RECONCILIATION_TARGET_INVALID"))?;
        let saved = {
            let s = self.lock()?;
            s.contexts
                .get(target)
                .cloned()
                .ok_or_else(|| fault("MUSE_DURABLE_RECONCILIATION_CONTEXT_REQUIRED"))?
        };
        if command.target_input_sha256.as_deref() != Some(saved.receipt.input_sha256.as_str())
            || target == command.operation_id
            || command.native_root_id != self.lock()?.confirmed_root
        {
            return Err(fault("MUSE_RECONCILIATION_TARGET_INVALID"));
        }
        if saved.finished {
            return self.queue(receipt,EffectOutcome::Applied,json!({"target_operation_id":target,"resubmitted":false,"disposition":"awaiting_host_receipt"}),None,None);
        }
        if let Some(ack) = &saved.ack {
            self.complete(&saved, ack.clone(), transport).await?;
            return self.queue(receipt,EffectOutcome::Applied,json!({"target_operation_id":target,"resubmitted":false,"disposition":"native_ack_readback"}),None,None);
        }
        if saved.command.method == "agent.reply" {
            let session = saved.command.input["reply"]["params"]["sessionId"]
                .as_str()
                .ok_or_else(|| fault("MUSE_REPLY_SESSION_INVALID"))?;
            self.refresh_one(transport, session).await?;
        }
        let fresh_work = if let Some(original) = &saved.work {
            let root = self
                .lock()?
                .scope
                .as_ref()
                .map(|s| s.native_root_id.clone())
                .ok_or_else(|| fault("MUSE_SCOPE_UNAVAILABLE"))?;
            let fresh = self.snapshot(transport, &root).await?;
            if !same_work(original, &fresh.value) {
                self.block_target(target, "MUSE_RECONCILIATION_WORK_CHANGED")?;
                return Err(fault("MUSE_RECONCILIATION_WORK_CHANGED"));
            }
            Some(fresh)
        } else {
            None
        };
        let (ack, pending) = {
            let mut s = self.lock()?;
            let scope = s
                .scope
                .clone()
                .ok_or_else(|| fault("MUSE_SCOPE_UNAVAILABLE"))?;
            if validate_retry(
                &s,
                &scope,
                &self.bootstrap.worker.boot_id,
                &saved,
                fresh_work.as_ref(),
            )
            .is_err()
            {
                s.journal
                    .as_mut()
                    .ok_or_else(|| fault("MUSE_JOURNAL_UNAVAILABLE"))?
                    .block_reconciliation(&scope, target, "MUSE_RECONCILIATION_FENCE_CHANGED")
                    .map_err(jf)?;
                return Err(fault("MUSE_RECONCILIATION_FENCE_CHANGED"));
            }
            let action = s
                .journal
                .as_mut()
                .ok_or_else(|| fault("MUSE_JOURNAL_UNAVAILABLE"))?
                .explicit_reconcile(&scope, target, saved.invocation.payload_sha256())
                .map_err(jf)?;
            match action {
                journal::Reconciliation::Write(guard) => {
                    let exact = guard.invocation();
                    (
                        None,
                        Some(
                            transport
                                .submit_request(exact.method(), exact.params().map_err(jf)?)
                                .map_err(|_| fault("MUSE_NATIVE_WRITE_UNCERTAIN"))?,
                        ),
                    )
                }
                journal::Reconciliation::AckReadback(ack) => (Some(ack), None),
                journal::Reconciliation::AwaitingHostReceipt
                | journal::Reconciliation::Finalized => (None, None),
                journal::Reconciliation::Blocked(_) => {
                    return Err(fault("MUSE_RECONCILIATION_BLOCKED"));
                }
            }
        };
        let resubmitted = pending.is_some();
        if let Some(ack) = ack {
            self.complete(&saved, ack, transport).await?;
        } else if let Some(pending) = pending {
            self.complete(&saved, await_pending(pending).await?, transport)
                .await?;
        }
        self.queue(receipt,EffectOutcome::Applied,json!({"target_operation_id":target,"resubmitted":resubmitted,"completion_condition":"explicit_same_command_reconciliation","execution_complete":false}),None,None)
    }
    fn block_target(&self, target: &str, code: &str) -> Result<()> {
        let mut s = self.lock()?;
        let scope = s
            .scope
            .clone()
            .ok_or_else(|| fault("MUSE_SCOPE_UNAVAILABLE"))?;
        s.journal
            .as_mut()
            .ok_or_else(|| fault("MUSE_JOURNAL_UNAVAILABLE"))?
            .block_reconciliation(&scope, target, code)
            .map_err(jf)
    }

    fn queue(
        &self,
        receipt: &ModuleReceiptIdentity,
        effect: EffectOutcome,
        details: Value,
        turn: Option<String>,
        input: Option<String>,
    ) -> Result<()> {
        let mut s = self.lock()?;
        let scope = s
            .scope
            .clone()
            .ok_or_else(|| fault("MUSE_SCOPE_UNAVAILABLE"))?;
        let mut value = outcome(receipt, &scope, effect, details, turn, input);
        if s.confirmed_root.is_none() {
            value.native_root_id = None;
        }
        s.journal
            .as_mut()
            .ok_or_else(|| fault("MUSE_JOURNAL_UNAVAILABLE"))?
            .queue_outcome(&scope, receipt.clone(), value)
            .map_err(jf)
    }
    async fn snapshot(&self, transport: &MspTransport, session: &str) -> Result<Snapshot> {
        let revision = self.lock()?.revision(session);
        let read = read_rpc(
            transport,
            "session/read",
            json!({"sessionId":session,"excludeItems":true}),
        )
        .await?;
        schema::validate_definition("SessionReadResult", &read)
            .map_err(|_| fault("MUSE_SESSION_READ_INVALID"))?;
        if read["session"]["sessionId"] != session || self.lock()?.revision(session) != revision {
            return Err(fault("MUSE_SESSION_READ_RACED"));
        }
        {
            let s = self.lock()?;
            if s.scope
                .as_ref()
                .is_some_and(|scope| scope.native_root_id == session)
                && s.workspace.as_ref().is_none_or(|workspace| {
                    !same_workspace(&read["session"]["workspaceRoot"], &json!(workspace))
                })
            {
                return Err(fault("MUSE_NATIVE_WORKSPACE_MISMATCH"));
            }
        }
        Ok(Snapshot {
            value: read["session"].clone(),
            revision,
            view_cursor: read["viewCursor"]
                .as_str()
                .ok_or_else(|| fault("MUSE_SESSION_READ_INVALID"))?
                .into(),
        })
    }
    async fn refresh(&self, transport: &Arc<MspTransport>, session: &str) -> Result<()> {
        self.refresh_one(transport, session).await?;
        let root = self.lock()?.confirmed_root.clone();
        if root.as_deref() == Some(session) {
            self.discover_children(transport, session).await?;
            let children = self.lock()?.children();
            // One bounded family pass, never a child runner or subagent result
            // fetch. Unread children remain explicit partial observations.
            for child in children.iter().take(32) {
                if self.refresh_one(transport, child).await.is_err() {
                    self.mark_gap("MUSE_CHILD_READ_UNAVAILABLE");
                }
            }
        }
        Ok(())
    }
    async fn discover_children(&self, transport: &MspTransport, root: &str) -> Result<()> {
        let mut cursor = self.lock()?.discovery_cursor.clone();
        let mut seen = std::collections::BTreeSet::new();
        for _ in 0..10 {
            if self.lock()?.confirmed_root.as_deref() != Some(root) {
                return Err(fault("MUSE_DISCOVERY_SCOPE_CHANGED"));
            }
            let mut params = json!({"sessionId":root,"limit":100,"direction":"forward"});
            if let Some(cursor) = &cursor {
                params["cursor"] = json!(cursor);
            }
            let page = read_rpc(transport, "view/page", params).await?;
            schema::validate_definition("ViewPageResult", &page)
                .map_err(|_| fault("MUSE_DISCOVERY_PAGE_INVALID"))?;
            let mut s = self.lock()?;
            for event in page["events"]
                .as_array()
                .ok_or_else(|| fault("MUSE_DISCOVERY_PAGE_INVALID"))?
            {
                if !matches!(
                    event["method"].as_str(),
                    Some("item/started" | "item/updated" | "item/completed")
                ) || event["params"]["sessionId"] != root
                    || !matches!(
                        event["params"]["item"]["kind"].as_str(),
                        Some("subagent" | "reminderChild")
                    )
                {
                    continue;
                }
                schema::validate_method_parameters(
                    event["method"]
                        .as_str()
                        .ok_or_else(|| fault("MUSE_DISCOVERY_PAGE_INVALID"))?,
                    &event["params"],
                )
                .map_err(|_| fault("MUSE_DISCOVERY_PAGE_INVALID"))?;
                s.event_number = s
                    .event_number
                    .checked_add(1)
                    .ok_or_else(|| fault("MUSE_EVENT_REVISION_EXHAUSTED"))?;
                if let (Some(p), Some(f)) = (&s.projector, &s.fence) {
                    s.projector = Some(
                        p.apply_event(NativeEventInput {
                            fence: f,
                            event_revision: s.event_number,
                            at_ms: now(),
                            event,
                        })
                        .projector,
                    );
                }
            }
            if page["nextCursor"].is_null() {
                s.discovery_cursor = None;
                s.discovery_truncated = false;
                s.changed();
                return Ok(());
            }
            let next = page["nextCursor"]
                .as_str()
                .filter(|s| !s.is_empty() && s.len() <= 8192)
                .ok_or_else(|| fault("MUSE_DISCOVERY_PAGE_INVALID"))?;
            if cursor.as_deref() == Some(next) || !seen.insert(next.to_owned()) {
                return Err(fault("MUSE_DISCOVERY_CURSOR_STALLED"));
            }
            cursor = Some(next.into());
            s.discovery_cursor = cursor.clone();
            s.discovery_truncated = true;
            s.changed();
        }
        Ok(())
    }
    async fn refresh_one(&self, transport: &Arc<MspTransport>, session: &str) -> Result<()> {
        if !self.lock()?.member(session) {
            return Err(fault("MUSE_REFRESH_OUTSIDE_FAMILY"));
        }
        let snapshot = self.snapshot(transport, session).await?;
        read_rpc(
            transport,
            "view/subscribe",
            json!({"sessionId":session,"after":snapshot.view_cursor}),
        )
        .await?;
        let pending_revision = self.lock()?.revision(session);
        let inventory = read_rpc(
            transport,
            "approval/listPending",
            json!({"sessionId":session}),
        )
        .await?;
        self.apply_refresh(session, &snapshot, pending_revision, &inventory)?;
        let (usage_generation, fence) = {
            let s = self.lock()?;
            (
                s.projector.as_ref().map_or(0, |p| p.usage_generation()),
                s.fence
                    .clone()
                    .ok_or_else(|| fault("MUSE_PROJECTION_UNAVAILABLE"))?,
            )
        };
        if let Ok(usage) = read_rpc(transport, "usage/read", json!({})).await {
            let mut s = self.lock()?;
            if let Some(p) = &s.projector {
                s.projector = Some(
                    p.apply_usage_read(&fence, usage_generation, now(), &usage, false)
                        .projector,
                );
                s.changed();
            }
        }
        Ok(())
    }
    fn apply_refresh(
        &self,
        session: &str,
        snapshot: &Snapshot,
        pending_revision: u64,
        inventory: &Value,
    ) -> Result<()> {
        let mut s = self.lock()?;
        let fence = s
            .fence
            .clone()
            .ok_or_else(|| fault("MUSE_PROJECTION_UNAVAILABLE"))?;
        let p = s
            .projector
            .as_ref()
            .ok_or_else(|| fault("MUSE_PROJECTION_UNAVAILABLE"))?;
        let local_revision = p.session_revision(session);
        if s.revision(session) != snapshot.revision || pending_revision != snapshot.revision {
            return Err(fault("MUSE_SESSION_READ_RACED"));
        }
        let reduced = if session == fence.native_root_id {
            p.apply_root_snapshot(RootSnapshotInput {
                fence: &fence,
                expected_session_revision: local_revision,
                at_ms: now(),
                read_method: ReadMethod::SessionRead,
                snapshot: &snapshot.value,
            })
        } else {
            let observation = p
                .observation()
                .map_err(|_| fault("MUSE_PROJECTION_UNAVAILABLE"))?;
            let child = observation["observed_children"]
                .as_array()
                .and_then(|rows| rows.iter().find(|c| c["sessionId"] == session))
                .ok_or_else(|| fault("MUSE_CHILD_PROVENANCE_UNAVAILABLE"))?;
            p.apply_child_snapshot(ChildSnapshotInput {
                fence: &fence,
                child_session_id: session,
                parent_session_id: child["parentSessionId"]
                    .as_str()
                    .ok_or_else(|| fault("MUSE_CHILD_PROVENANCE_UNAVAILABLE"))?,
                item_id: child["itemId"]
                    .as_str()
                    .ok_or_else(|| fault("MUSE_CHILD_PROVENANCE_UNAVAILABLE"))?,
                parent_turn_id: child["parentTurnId"].as_str(),
                item_revision: child["revision"]
                    .as_u64()
                    .ok_or_else(|| fault("MUSE_CHILD_PROVENANCE_UNAVAILABLE"))?,
                expected_session_revision: local_revision,
                at_ms: now(),
                read_method: ReadMethod::SessionRead,
                snapshot: Some(&snapshot.value),
                failure: None,
            })
        };
        if reduced.status != ReductionStatus::Applied {
            return Err(fault("MUSE_REFRESH_PROJECTION_UNRESOLVED"));
        }
        let p = reduced.projector;
        let replaced = p.replace_pending(PendingInventoryInput {
            fence: &fence,
            session_id: session,
            expected_session_revision: p.session_revision(session),
            inventory: &inventory,
        });
        if replaced.status != ReductionStatus::Applied {
            return Err(fault("MUSE_PENDING_INVENTORY_UNRESOLVED"));
        }
        let replacement = inventory_controls(session, &inventory, pending_revision)?;
        s.pending.retain(|_, p| p.params["sessionId"] != session);
        if s.pending.len().saturating_add(replacement.len()) > MAX_PENDING {
            return Err(fault("MUSE_PENDING_CAPACITY"));
        }
        s.pending.extend(replacement);
        s.projector = Some(replaced.projector);
        s.changed();
        Ok(())
    }

    fn notification(&self, notification: Notification) -> Result<()> {
        let method = notification.envelope["method"].as_str().unwrap_or("");
        let p = &notification.envelope["params"];
        match schema::validate_method_parameters(method, p) {
            Ok(()) => {}
            Err(schema::SchemaError::UnknownMethod(_)) => {}
            Err(_) => return Err(fault("MUSE_NOTIFICATION_SCHEMA_INVALID")),
        }
        let mut s = self.lock()?;
        s.event_number = s
            .event_number
            .checked_add(1)
            .ok_or_else(|| fault("MUSE_EVENT_REVISION_EXHAUSTED"))?;
        if let Some(session) = p["sessionId"].as_str() {
            if s.member(session) {
                let rev = s.revision(session).saturating_add(1);
                s.versions.insert(session.into(), rev);
            }
        }
        let is_delta = method == "item/delta";
        if let (Some(projector), Some(fence)) = (&s.projector, &s.fence) {
            let reduction = projector.apply_event(NativeEventInput {
                fence,
                event_revision: s.event_number,
                at_ms: now(),
                event: &notification.envelope,
            });
            s.projector = Some(reduction.projector);
        }
        // Full native items establish item->turn only after a Task ACK bound
        // that turn to an exact original Operation. Deltas never create scope.
        if matches!(method, "item/started" | "item/updated" | "item/completed")
            && s.confirmed_root.as_deref() == p["sessionId"].as_str()
        {
            let item = &p["item"];
            if let (Some(id), Some(turn)) = (item["itemId"].as_str(), item["turnId"].as_str()) {
                if !id.is_empty()
                    && id.len() <= 512
                    && item["revision"].as_u64().is_some_and(|r| r > 0)
                    && s.task_turns.contains_key(turn)
                    && s.items.len() < MAX_STREAM_ITEMS
                {
                    let kind = match item["kind"].as_str() {
                        Some("agentMessage") => Some(crate::streams::KnownItemKind::AgentMessage),
                        Some("reasoning") => Some(crate::streams::KnownItemKind::Reasoning),
                        Some("toolCall") => Some(crate::streams::KnownItemKind::ToolCall),
                        _ => None,
                    };
                    if let Some(kind) = kind {
                        let order = s.items.len() as u32;
                        if let Some(old) = s.items.get_mut(id) {
                            if old.turn != turn || old.kind != kind {
                                old.disabled = true;
                                stream_gap(&mut s, "MUSE_STREAM_ITEM_IDENTITY_CONFLICT");
                            }
                        } else {
                            s.items.insert(
                                id.into(),
                                ItemIdentity {
                                    turn: turn.into(),
                                    kind,
                                    order,
                                    disabled: false,
                                },
                            );
                        }
                    }
                }
            }
        }
        if let Some(facts) = stream_facts(&s, &self.bootstrap, &notification) {
            let prior_sequence = *s
                .stream_sequences
                .get(&facts.scope.operation_id)
                .unwrap_or(&0);
            let first = prior_sequence
                .checked_add(1)
                .ok_or_else(|| fault("MUSE_STREAM_SEQUENCE_EXHAUSTED"))?;
            let frames = crate::streams::encode_notification(&facts, &notification, first)?;
            if let Some(last) = frames.last() {
                // Reserve every logical record BEFORE queuing, even when the
                // queue/link/ACK is lost. Later delivery exposes an actual gap.
                s.stream_sequences
                    .insert(facts.scope.operation_id.clone(), last.source_sequence);
                if self
                    .streams
                    .try_send(StreamJob {
                        facts,
                        notification,
                        prior_sequence,
                    })
                    .is_err()
                {
                    stream_gap(&mut s, "MUSE_STREAM_PRODUCER_GAP");
                }
            }
        } else if is_delta && !s.task_turns.is_empty() {
            stream_gap(&mut s, "MUSE_STREAM_SOURCE_SCOPE_UNAVAILABLE");
        }
        // Per-token content is never sent via module.observe. Stable lifecycle,
        // pending inventory and usage reductions use its bounded projection.
        if !is_delta {
            s.changed();
        }
        Ok(())
    }
    fn mark_gap(&self, code: &'static str) {
        if let Ok(mut s) = self.lock() {
            stream_gap(&mut s, code);
        }
    }
    /// Optional data-plane handoff for Root's shared-link integration. Invoke
    /// outside the native consumer, only after ModuleLink supports correlated
    /// concurrent requests on ONE authenticated physical link. No second hello.
    /// Every error is local loss; never bubble it into native/outbox handling.
    pub async fn publish_next_stream(&self, link: &mut ModuleLink) -> bool {
        let job = self.stream_jobs.lock().await.try_recv().ok();
        let Some(job) = job else {
            return false;
        };
        if self.stopping() {
            return false;
        }
        let admitted = self.lock().is_ok_and(|s| {
            s.task_turns
                .get(&job.facts.scope.turn_id)
                .is_some_and(|turn| {
                    turn.host_admitted && turn.receipt.operation_id == job.facts.scope.operation_id
                })
        });
        if !admitted {
            self.mark_gap("MUSE_STREAM_SOURCE_NOT_ADMITTED");
            return false;
        }
        let mut sequence = job.prior_sequence;
        let delivered = crate::streams::publish_notification(
            link,
            &job.facts,
            &job.notification,
            &mut sequence,
        )
        .await
        .is_ok();
        if !delivered {
            self.mark_gap("MUSE_STREAM_DELIVERY_GAP");
        }
        delivered
    }
    pub async fn flush(&self, link: &mut ModuleLink) -> Result<()> {
        let (scope, tickets) = {
            let s = self.lock()?;
            match (&s.scope, &s.journal) {
                (Some(scope), Some(journal)) => {
                    (Some(scope.clone()), journal.outbox(scope).map_err(jf)?)
                }
                _ => (None, Vec::new()),
            }
        };
        if let Some(scope) = scope {
            for ticket in tickets {
                let params: Value = serde_json::from_str(ticket.params_json())
                    .map_err(|_| fault("MUSE_OUTBOX_INVALID"))?;
                let dispatch_admitted = ticket.method() == "module.outcome"
                    && params["outcome"] == "applied"
                    && params["details"]["dispatch_admission"].is_object();
                match ticket.method() {
                    "module.outcome" => {
                        link.outcome(params).await?;
                    }
                    "module.result" => {
                        link.result(params).await?;
                    }
                    _ => return Err(fault("MUSE_OUTBOX_METHOD_INVALID")),
                }
                let mut s = self.lock()?;
                let retired = s
                    .journal
                    .as_mut()
                    .ok_or_else(|| fault("MUSE_JOURNAL_UNAVAILABLE"))?
                    .acknowledge(&scope, &ticket)
                    .map_err(jf)?;
                if dispatch_admitted && retired == journal::AckDisposition::Retired {
                    for turn in s
                        .task_turns
                        .values_mut()
                        .filter(|turn| turn.receipt.operation_id == ticket.operation_id())
                    {
                        turn.host_admitted = true;
                    }
                }
            }
        }
        let sample = {
            let s = self.lock()?;
            if s.dirty == s.delivered {
                None
            } else {
                let mut view = if s.confirmed_root.is_some() {
                    s.projector
                        .as_ref()
                        .map(|p| {
                            p.observation()
                                .map_err(|_| fault("MUSE_OBSERVATION_UNAVAILABLE"))
                        })
                        .transpose()?
                        .unwrap_or_else(
                            || json!({"execution":"unknown","family_completeness":"partial"}),
                        )
                } else {
                    json!({"execution":"unknown","family_completeness":"partial","boot_id":self.bootstrap.worker.boot_id})
                };
                view["native_ready"] = json!(s.ready);
                view["fingerprint_warning"] = json!(s.fingerprint_warning);
                view["diagnostic_code"] = json!(s.diagnostic);
                view["stream_gap_count"] = json!(s.stream_gaps);
                view["native_mcp_inventory_state"] = json!("unsupported");
                Some((
                    s.dirty,
                    json!({"event_id":format!("{}:{}",self.bootstrap.worker.boot_id,s.dirty),"sequence":s.dirty,"state":view}),
                ))
            }
        };
        if let Some((revision, params)) = sample {
            link.observe(params).await?;
            self.lock()?.delivered = revision;
        }
        Ok(())
    }
    pub async fn stop_native(&self) {
        self.request_stop();
        let transport = self.lock().ok().and_then(|mut s| {
            s.ready = false;
            s.close_usage(now());
            s.transport.clone()
        });
        if let Some(transport) = transport {
            let _ = transport.close().await;
        }
        let mut task = self.event_task.lock().await;
        // Borrow the retained handle across await; cancellation cannot detach
        // it before the outer driver retries shutdown/custodian join.
        if let Some(handle) = task.as_mut() {
            let _ = handle.await;
        }
        *task = None;
        // Outer module_runtime retains and joins the custodian AFTER this MSP
        // barrier and its own stdout/stderr drains. No blocking child Drop.
    }
}

struct Snapshot {
    value: Value,
    revision: u64,
    view_cursor: String,
}
fn stream_gap(s: &mut State, code: &'static str) {
    if s.diagnostic != Some(code) {
        s.diagnostic = Some(code);
        s.stream_gaps = s.stream_gaps.saturating_add(1);
        s.changed();
    }
}
fn validate_ack(method: &str, ack: &Value) -> Result<()> {
    let definition = match method {
        "session/start" => "SessionStartResult",
        "session/resume" => "SessionResumeResult",
        "turn/start" => "TurnStartResult",
        "turn/steer" => "TurnSteerResult",
        "session/setModel" => "SessionSetModelResult",
        "session/setApprovalMode" => "SessionSetApprovalModeResult",
        "session/setReasoningEffort" => "SessionSetReasoningEffortResult",
        "goal/set" | "goal/edit" | "goal/pause" | "goal/resume" | "goal/clear" => {
            "GoalCommandResult"
        }
        "approval/decide" => "ApprovalDecideResult",
        "userInput/answer" => "UserInputAnswerResult",
        "userInput/cancel" => "UserInputCancelResult",
        "userInput/clarify" => "UserInputClarifyResult",
        _ => return Err(fault("MUSE_NATIVE_ACK_METHOD_INVALID")),
    };
    schema::validate_definition(definition, ack)
        .map_err(|_| fault("MUSE_NATIVE_ACK_SCHEMA_INVALID"))
}
fn closed(code: &str) -> &str {
    if code.len() <= 128
        && code
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
    {
        code
    } else {
        "MUSE_COMMAND_UNRESOLVED"
    }
}
fn validate_initialize(init: &Value) -> Result<()> {
    // An unknown durability declaration stays unknown; validate the rest of
    // the public initialization shape without converting it to absent/durable.
    let mut compatibility = init.clone();
    if commands::session_durability(init) == commands::SessionDurability::Unrecognized {
        compatibility
            .as_object_mut()
            .ok_or_else(|| fault("MUSE_INITIALIZE_SCHEMA_INVALID"))?
            .remove("sessionDurability");
    }
    schema::validate_definition("InitializeResult", &compatibility)
        .map_err(|_| fault("MUSE_INITIALIZE_SCHEMA_INVALID"))?;
    if init["schema"]["version"] != 1 {
        return Err(fault("MUSE_SCHEMA_VERSION_UNSUPPORTED"));
    }
    // Fingerprint mismatch is advisory under the public SDK contract. The
    // pinned schema file SHA is deliberately never used as wire fingerprint.
    Ok(())
}
fn native_namespace(init: &Value) -> Result<String> {
    let home = init["museHome"]
        .as_str()
        .ok_or_else(|| fault("MUSE_NATIVE_NAMESPACE_INVALID"))?;
    let path = std::path::Path::new(home);
    if !path.is_absolute() {
        return Err(fault("MUSE_NATIVE_NAMESPACE_INVALID"));
    }
    let canonical =
        std::fs::canonicalize(path).map_err(|_| fault("MUSE_NATIVE_NAMESPACE_UNAVAILABLE"))?;
    let canonical = canonical
        .to_str()
        .ok_or_else(|| fault("MUSE_NATIVE_NAMESPACE_INVALID"))?;
    Ok(format!(
        "muse:{}:{}",
        init["platformOs"]
            .as_str()
            .ok_or_else(|| fault("MUSE_NATIVE_NAMESPACE_INVALID"))?,
        if init["platformFamily"] == "windows" {
            canonical.to_lowercase()
        } else {
            canonical.into()
        }
    ))
}
fn install_projector(state: &mut State, scope: &Scope, boot: &str) -> Result<()> {
    let fence = ProjectionFence {
        binding_id: scope.binding_id.clone(),
        binding_generation: scope.generation,
        native_root_id: scope.native_root_id.clone(),
        native_scope_key: scope.native_namespace.clone(),
        bridge_boot_id: boot.into(),
    };
    state.projector = Some(
        ObservationProjector::new(fence.clone()).map_err(|_| fault("MUSE_PROJECTION_INVALID"))?,
    );
    state.fence = Some(fence);
    Ok(())
}
async fn read_rpc(transport: &MspTransport, method: &str, params: Value) -> Result<Value> {
    match schema::validate_method_parameters(method, &params) {
        Ok(()) => {}
        Err(schema::SchemaError::MethodHasNoParameters(_)) if params == json!({}) => {}
        Err(_) => return Err(fault("MUSE_NATIVE_PARAMETERS_INVALID")),
    }
    await_pending(
        transport
            .submit_request(method, object(params)?)
            .map_err(|_| fault("MUSE_NATIVE_READ_UNAVAILABLE"))?,
    )
    .await
}
async fn await_pending(pending: PendingRequest) -> Result<Value> {
    pending
        .submitted
        .await
        .map_err(|_| fault("MUSE_NATIVE_WRITE_UNCERTAIN"))?
        .map_err(|_| fault("MUSE_NATIVE_WRITE_UNCERTAIN"))?;
    pending
        .response
        .await
        .map_err(|_| fault("MUSE_NATIVE_RESPONSE_UNKNOWN"))?
        .map_err(|error| match error {
            RequestError::Rpc(jsonrpsee_core::client::Error::Call(error))
                if error.code() == -32030
                    && error
                        .data()
                        .and_then(|data| serde_json::from_str::<Value>(data.get()).ok())
                        .is_some_and(|data| data["kind"] == "commandRejected") =>
            {
                fault("MUSE_NATIVE_COMMAND_REJECTED")
            }
            RequestError::Transport(_) => fault("MUSE_NATIVE_TRANSPORT_UNKNOWN"),
            RequestError::Rpc(_) => fault("MUSE_NATIVE_RPC_UNKNOWN"),
        })
}
fn outcome(
    receipt: &ModuleReceiptIdentity,
    scope: &Scope,
    effect: EffectOutcome,
    mut details: Value,
    turn_id: Option<String>,
    native_input_id: Option<String>,
) -> RuntimeOutcome {
    details["module_receipt"] = json!(receipt);
    RuntimeOutcome {
        operation_id: receipt.operation_id.clone(),
        outcome: effect,
        native_scope_key: Some(scope.native_namespace.clone()),
        native_root_id: Some(scope.native_root_id.clone()),
        turn_id,
        native_input_id,
        details,
    }
}
fn task_admission(
    task: &ValidatedTaskPrompt,
    receipt: &ModuleReceiptIdentity,
    native_id: &str,
) -> TaskDispatchAdmissionReceipt {
    let c = &task.context;
    let e = &task.envelope;
    TaskDispatchAdmissionReceipt {
        schema_version: 1,
        module_receipt: receipt.clone(),
        operation_id: c.operation_id.clone(),
        binding_id: c.binding_id.clone(),
        binding_generation: c.binding_generation,
        worker_boot_id: c.worker_boot_id.clone(),
        attempt_id: c.attempt_id.clone(),
        task_id: c.task_id.clone(),
        task_revision: c.task_revision,
        task_snapshot_sha256: c.task_snapshot_sha256.clone(),
        source_text_sha256: c.source_text_sha256.clone(),
        source_text_bytes: c.source_text_bytes,
        native_payload_sha256: e.prompt_sha256.clone(),
        native_payload_bytes: e.prompt_bytes,
        native_input_id: Some(native_id.into()),
    }
}
fn same_work(a: &Value, b: &Value) -> bool {
    [
        "sessionId",
        "status",
        "activeTurnId",
        "modelId",
        "providerId",
        "reasoningEffort",
    ]
    .iter()
    .all(|key| a[*key] == b[*key])
}
fn same_workspace(a: &Value, b: &Value) -> bool {
    let (Some(a), Some(b)) = (a.as_str(), b.as_str()) else {
        return false;
    };
    match (std::fs::canonicalize(a), std::fs::canonicalize(b)) {
        (Ok(a), Ok(b)) => a == b,
        _ => false,
    }
}
fn validate_retry(
    s: &State,
    scope: &Scope,
    boot: &str,
    saved: &SavedCommand,
    work: Option<&Snapshot>,
) -> Result<()> {
    let children = s.children();
    let pending = find_request(s, &saved.command.input);
    let cx = EncodeContext {
        expected_receipt: &saved.receipt,
        worker_boot_id: boot,
        native_root_id: s.confirmed_root.as_deref(),
        fresh_root_session_id: Some(&scope.native_root_id),
        native_command_id: Some(saved.invocation.command_id()),
        initialized: s.initialized,
        observed_children: &children,
        requested: &s.requested,
        work: work.map(|w| WorkFacts {
            read_method: "session/read",
            session_id: &scope.native_root_id,
            model_id: w.value["modelId"].as_str(),
            provider_id: w.value["providerId"].as_str(),
            status: w.value["status"].as_str().unwrap_or("unknown"),
            active_turn_id: w.value["activeTurnId"].as_str(),
            sampled_event_version: w.revision,
            current_event_version: s.revision(&scope.native_root_id),
            model_configuration: s.model,
            standing_effort: s.standing_effort.as_deref(),
        }),
        pending_request: pending.map(|p| PendingRequestFacts {
            method: &p.method,
            params: &p.params,
            sampled_event_version: p.revision,
            current_event_version: s.revision(p.params["sessionId"].as_str().unwrap_or("")),
        }),
        recovery: Some(RecoveryFacts {
            durability: s.durability,
            native_ready: s.ready,
        }),
        validated_result_input: None,
        session_mcp_granted: s.session_mcp_granted,
    };
    let CommandEncoding::Native { request, .. } = commands::encode(&saved.command, &cx)
        .map_err(|_| fault("MUSE_RECONCILIATION_FENCE_CHANGED"))?
    else {
        return Err(fault("MUSE_RECONCILIATION_FENCE_CHANGED"));
    };
    if request.method != saved.invocation.method()
        || request.params != saved.invocation.params().map_err(jf)?
    {
        return Err(fault("MUSE_RECONCILIATION_FENCE_CHANGED"));
    }
    Ok(())
}
fn find_request<'a>(s: &'a State, input: &Value) -> Option<&'a ControlRequest> {
    let p = &input["reply"]["params"];
    let method = if input["reply"]["method"] == "approval/decide" {
        "approval/request"
    } else {
        "userInput/request"
    };
    s.pending.values().find(|entry| {
        entry.method == method
            && entry.params["sessionId"] == p["sessionId"]
            && entry.params[if method == "approval/request" {
                "approvalId"
            } else {
                "userInputId"
            }] == p[if method == "approval/request" {
                "approvalId"
            } else {
                "userInputId"
            }]
    })
}
fn inventory_controls(
    session: &str,
    inventory: &Value,
    revision: u64,
) -> Result<BTreeMap<String, ControlRequest>> {
    let mut pending = BTreeMap::new();
    for (field, method, id_field) in [
        ("approvals", "approval/request", "approvalId"),
        ("userInputs", "userInput/request", "userInputId"),
    ] {
        let entries = inventory[field]
            .as_array()
            .ok_or_else(|| fault("MUSE_PENDING_INVENTORY_INVALID"))?;
        for params in entries {
            schema::validate_method_parameters(method, params)
                .map_err(|_| fault("MUSE_PENDING_INVENTORY_INVALID"))?;
            if params["sessionId"] != session
                || pending.len() >= MAX_PENDING
                || serde_json::to_vec(params)?.len() > 64 * 1024
            {
                return Err(fault("MUSE_PENDING_INVENTORY_INVALID"));
            }
            let id = params[id_field]
                .as_str()
                .ok_or_else(|| fault("MUSE_PENDING_INVENTORY_INVALID"))?;
            if pending
                .insert(
                    format!("{session}:{method}:{id}"),
                    ControlRequest {
                        method: method.into(),
                        params: params.clone(),
                        revision,
                    },
                )
                .is_some()
            {
                return Err(fault("MUSE_PENDING_INVENTORY_INVALID"));
            }
        }
    }
    Ok(pending)
}
fn stream_facts(
    s: &State,
    bootstrap: &ModuleOwnedBootstrap,
    notification: &Notification,
) -> Option<crate::streams::KnownRootTaskStream> {
    let p = &notification.envelope["params"];
    let session = p["sessionId"].as_str()?;
    if !s.ready || s.confirmed_root.as_deref() != Some(session) {
        return None;
    }
    let item_id = p["itemId"]
        .as_str()
        .or_else(|| p["item"]["itemId"].as_str());
    let item = item_id.and_then(|id| {
        s.items
            .get(id)
            .filter(|i| !i.disabled)
            .map(|item| (id, item))
    });
    let turn = p["turnId"]
        .as_str()
        .or_else(|| p["item"]["turnId"].as_str())
        .or_else(|| item.map(|(_, i)| i.turn.as_str()))?;
    let task = s.task_turns.get(turn)?;
    if task.turn != turn
        || task.receipt.binding_id != bootstrap.binding_id
        || task.receipt.binding_generation != bootstrap.generation
        || task.task.context.worker_boot_id != bootstrap.worker.boot_id
    {
        return None;
    }
    let c = &task.task.context;
    Some(crate::streams::KnownRootTaskStream {
        scope: swarm_contracts::native_stream::StreamScope {
            binding_id: c.binding_id.clone(),
            generation: c.binding_generation,
            boot_id: c.worker_boot_id.clone(),
            task_id: c.task_id.clone(),
            task_revision: c.task_revision,
            attempt_id: c.attempt_id.clone(),
            operation_id: c.operation_id.clone(),
            native_session_id: session.into(),
            turn_id: turn.into(),
        },
        item: item.map(|(id, i)| crate::streams::KnownRootTaskItem {
            item_id: id.into(),
            turn_id: i.turn.clone(),
            item_order: i.order,
            kind: i.kind,
        }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        module_runtime::{NativeAttachment, NativeFuture},
        msp::Options,
    };
    use sha2::{Digest, Sha256};
    use std::{
        path::{Path, PathBuf},
        sync::atomic::AtomicUsize,
    };
    use swarm_client::{HostConnectionConfig, IpcConfig, ipc_endpoint};
    use swarm_contracts::{
        Credential,
        module_catalog::{ModuleDescriptor, ProtocolVersion},
        module_contract::ModuleContractClaim,
    };
    use swarm_process::module_owner::VerifiedModuleWorker;
    use tokio::io::{
        AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, DuplexStream,
    };

    const ROOT: &str = "019a0000-0000-7000-8000-000000000099";
    const BOOT: &str = "fixture-driver-boot";
    fn digest(bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
    }
    struct Directory(PathBuf);
    impl Directory {
        fn new() -> Self {
            let dir = std::env::temp_dir().join(format!("sm-{}", uuid::Uuid::now_v7()));
            std::fs::create_dir(&dir).unwrap();
            Self(dir.canonicalize().unwrap())
        }
    }
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    fn initialize(home: &Path) -> Value {
        json!({"experimentalApi":false,"grantedCapabilities":[],"museHome":home,
            "platformFamily":if cfg!(windows){"windows"}else{"unix"},
            "platformOs":if cfg!(windows){"windows"}else if cfg!(target_os="macos"){"macos"}else{"linux"},
            "schema":{"version":1,"fingerprint":FINGERPRINT},"serverInfo":{"name":"synthetic-msp","version":"1.3.0"},"userAgent":"synthetic"})
    }
    fn session(workspace: &Path) -> Value {
        json!({"sessionId":ROOT,"status":"idle","activeTurnId":null,"createdAt":"2026-10-16T00:00:00Z","updatedAt":"2026-10-16T00:00:00Z","forkedFrom":null,"modelId":null,"providerId":null,"path":"synthetic-session-log","turnCount":0,"workspaceRoot":workspace})
    }
    fn bootstrap(dir: &Path) -> Arc<ModuleOwnedBootstrap> {
        let exe = std::env::current_exe().unwrap();
        let mut descriptor: Value =
            serde_json::from_str(include_str!("../registration/descriptor.template.json")).unwrap();
        descriptor["launch"]["executable"] = json!(exe);
        let descriptor: ModuleDescriptor = serde_json::from_value(descriptor).unwrap();
        let contract = ModuleContractClaim::from_descriptor(
            &descriptor,
            ProtocolVersion { major: 1, minor: 0 },
        )
        .unwrap();
        Arc::new(ModuleOwnedBootstrap {
            host: HostConnectionConfig {
                schema_version: 1,
                host_data_dir: dir.into(),
                ipc: IpcConfig::default(),
            },
            credential: Credential {
                client_id: "synthetic-module".into(),
                token: "synthetic-only".into(),
            },
            worker: VerifiedModuleWorker {
                boot_id: BOOT.into(),
                owner_record: json!({"token":"synthetic-owner","process":{"pid":1}}),
                process_image_identity: json!({}),
            },
            contract,
            binding_id: "synthetic-binding".into(),
            generation: 1,
            state_dir: dir.into(),
            native_executable: exe,
        })
    }
    struct PipeRuntime {
        pipe: Mutex<Option<DuplexStream>>,
        starts: AtomicUsize,
        drained: AtomicBool,
        joined: AtomicBool,
    }
    impl NativeRuntime for PipeRuntime {
        fn start<'a>(
            &'a self,
            _options: &'a NativeOptions,
            router: RpcModule<()>,
        ) -> NativeFuture<'a, NativeAttachment> {
            Box::pin(async move {
                self.starts.fetch_add(1, Ordering::SeqCst);
                let pipe = self
                    .pipe
                    .lock()
                    .unwrap()
                    .take()
                    .ok_or_else(|| fault("MUSE_FIXTURE_DUPLICATE_START"))?;
                let (read, write) = tokio::io::split(pipe);
                // Root-owned cfg(test)-only wrapper. This bypass cannot exist
                // in a production build; owner verification remains unchanged.
                let (transport, notifications) =
                    MspTransport::attach_fixture(read, write, router, Options::default())
                        .map_err(|_| fault("MUSE_FIXTURE_PIPE_INVALID"))?;
                Ok(NativeAttachment {
                    transport: Arc::new(transport),
                    notifications,
                })
            })
        }
        fn drain_stdio(&self) -> NativeFuture<'_, ()> {
            Box::pin(async {
                self.drained.store(true, Ordering::SeqCst);
                Ok(())
            })
        }
        fn join_custodians(&self) -> NativeFuture<'_, ()> {
            Box::pin(async {
                assert!(self.drained.load(Ordering::SeqCst));
                self.joined.store(true, Ordering::SeqCst);
                Ok(())
            })
        }
    }
    fn fixture(dir: &Path) -> (Arc<Driver>, Arc<PipeRuntime>, DuplexStream) {
        let bootstrap = bootstrap(dir);
        let (owned, peer) = tokio::io::duplex(64 * 1024);
        let runtime = Arc::new(PipeRuntime {
            pipe: Mutex::new(Some(owned)),
            starts: AtomicUsize::new(0),
            drained: AtomicBool::new(false),
            joined: AtomicBool::new(false),
        });
        let scope = Scope {
            module_client_id: bootstrap.credential.client_id.clone(),
            module_id: bootstrap.contract.module_id.clone(),
            artifact: bootstrap.contract.artifact.clone(),
            binding_id: bootstrap.binding_id.clone(),
            generation: 1,
            native_namespace: native_namespace(&initialize(dir)).unwrap(),
            native_root_id: ROOT.into(),
        };
        let mut state = State::empty();
        state.journal = Some(Journal::create_fixture(dir, scope.clone(), BOOT.into()).unwrap());
        state.scope = Some(scope.clone());
        install_projector(&mut state, &scope, BOOT).unwrap();
        let driver = Driver::with_state(bootstrap, runtime.clone(), state).unwrap();
        (driver, runtime, peer)
    }
    fn command(driver: &Driver, operation: &str, method: &str, input: Value) -> RuntimeCommand {
        RuntimeCommand {
            operation_id: operation.into(),
            method: method.into(),
            created_at_ms: 1,
            binding_id: driver.bootstrap.binding_id.clone(),
            generation: 1,
            native_root_id: if method == "agent.open" {
                None
            } else {
                Some(ROOT.into())
            },
            route: json!({"module_artifact_id":driver.bootstrap.contract.artifact.artifact_id,"native_options":{"workspaceRoot":driver.bootstrap.state_dir}}),
            input_sha256: Some(digest(operation.as_bytes())),
            input,
            target_input_sha256: None,
        }
    }
    fn dispatch(driver: &Driver) -> RuntimeCommand {
        let mut c = command(
            driver,
            "02-dispatch",
            "task.dispatch",
            json!({"text":"abc"}),
        );
        c.input["task_prompt"] = json!({"schema_id":"swarm.task_prompt","schema_version":1,"task_id":"task","task_revision":2,"attempt_id":"attempt","task_snapshot_sha256":"b".repeat(64),"prompt_sha256":digest(b"abc"),"prompt_bytes":3,"prompt":"abc"});
        c.input["task_dispatch_context"] = json!({"schema_version":1,"operation_id":c.operation_id,"binding_id":c.binding_id,"binding_generation":1,"worker_boot_id":BOOT,"task_id":"task","task_revision":2,"attempt_id":"attempt","task_snapshot_sha256":"b".repeat(64),"source_text_sha256":digest(b"abc"),"source_text_bytes":3});
        c
    }
    async fn send<W: AsyncWrite + Unpin>(writer: &mut W, value: Value) {
        writer
            .write_all(format!("{}\n", serde_json::to_string(&value).unwrap()).as_bytes())
            .await
            .unwrap();
        writer.flush().await.unwrap();
    }
    async fn native_peer(
        peer: DuplexStream,
        workspace: PathBuf,
        calls: Arc<Mutex<Vec<Value>>>,
        drop_turn_ack: bool,
    ) {
        let (read, mut write) = tokio::io::split(peer);
        let mut lines = BufReader::new(read).lines();
        let mut initialized = false;
        while let Some(line) = lines.next_line().await.unwrap() {
            let request: Value = serde_json::from_str(&line).unwrap();
            let method = request["method"].as_str().unwrap();
            calls.lock().unwrap().push(request.clone());
            let result = match method {
                "initialize" => initialize(&workspace),
                "initialized" => {
                    initialized = true;
                    continue;
                }
                "session/start" => {
                    assert!(initialized);
                    assert_eq!(request["params"]["sessionId"], ROOT);
                    assert!(
                        commands::validate_native_command_id(
                            request["params"]["commandId"].as_str().unwrap()
                        )
                        .is_ok()
                    );
                    assert!(request["params"].get("modelId").is_none());
                    json!({"session":session(&workspace),"viewCursor":"root:0"})
                }
                "session/read" => {
                    json!({"session":session(&workspace),"viewCursor":"root:1","history":{"mode":"none","items":null,"snapshot":null},"pendingRequests":[]})
                }
                "view/subscribe" => json!({"viewCursor":"root:1"}),
                "approval/listPending" => json!({"approvals":[],"userInputs":[]}),
                "usage/read" => json!({"usage":null}),
                "turn/start" => {
                    assert!(initialized);
                    assert_eq!(
                        request["params"]["input"],
                        json!([{"type":"text","text":"abc"}])
                    );
                    if drop_turn_ack {
                        return;
                    }
                    json!({"commandId":request["params"]["commandId"],"status":"accepted","disposition":"started","startedNewTurn":true,"turnId":"native-turn"})
                }
                "view/page" => {
                    json!({"events":[{"method":"item/completed","params":{"sessionId":ROOT,"viewCursor":"result:7","sourceRange":{"stream":{"kind":"run","id":"synthetic-run"},"first":{"id":"synthetic-first","sequence":1},"last":{"id":"synthetic-last","sequence":2}},"item":{"itemId":"answer","revision":7,"kind":"agentMessage","status":"completed","turnId":"native-turn","text":"synthetic result"}}}],"nextCursor":null})
                }
                _ => panic!("unexpected synthetic native method"),
            };
            send(
                &mut write,
                json!({"jsonrpc":"2.0","id":request["id"],"result":result}),
            )
            .await;
        }
    }
    trait FixtureIo: AsyncRead + AsyncWrite + Unpin + Send {}
    impl<T: AsyncRead + AsyncWrite + Unpin + Send> FixtureIo for T {}
    async fn host_connection(
        stream: Box<dyn FixtureIo>,
        drop_result_ack: bool,
        records: Arc<Mutex<Vec<Value>>>,
    ) {
        let (read, mut write) = tokio::io::split(stream);
        let mut lines = BufReader::new(read).lines();
        while let Some(line) = lines.next_line().await.unwrap() {
            let request: Value = serde_json::from_str(&line).unwrap();
            let method = request["method"].as_str().unwrap();
            let result = match method {
                "client.hello" => {
                    assert_eq!(request["params"]["token"], "synthetic-only");
                    json!({})
                }
                "module.hello" => {
                    assert_eq!(
                        request["params"]["module_contract"]["artifact"]["artifact_id"],
                        "muse-rust-controller.1"
                    );
                    json!({"binding_id":"synthetic-binding","generation":1})
                }
                "module.outcome" | "module.result" | "module.observe" => {
                    records.lock().unwrap().push(request.clone());
                    if method == "module.result" && drop_result_ack {
                        return;
                    }
                    json!({"stored":true})
                }
                _ => panic!("unexpected synthetic Host method"),
            };
            send(
                &mut write,
                json!({"jsonrpc":"2.0","id":request["id"],"result":result}),
            )
            .await;
            if method == "module.observe" {
                return;
            }
        }
    }
    fn host_pair(dir: &Path, records: Arc<Mutex<Vec<Value>>>) -> JoinHandle<()> {
        let endpoint = ipc_endpoint(dir).unwrap();
        #[cfg(unix)]
        {
            let listener = tokio::net::UnixListener::bind(endpoint).unwrap();
            tokio::spawn(async move {
                for first in [true, false] {
                    let (stream, _) = listener.accept().await.unwrap();
                    host_connection(Box::new(stream), first, records.clone()).await;
                }
            })
        }
        #[cfg(windows)]
        {
            use tokio::net::windows::named_pipe::ServerOptions;
            let mut server = ServerOptions::new()
                .first_pipe_instance(true)
                .create(&endpoint)
                .unwrap();
            tokio::spawn(async move {
                for first in [true, false] {
                    server.connect().await.unwrap();
                    let next = ServerOptions::new().create(&endpoint).unwrap();
                    host_connection(Box::new(server), first, records.clone()).await;
                    server = next;
                }
            })
        }
    }
    async fn shutdown(driver: &Driver, runtime: &PipeRuntime) {
        driver.stop_native().await;
        runtime.drain_stdio().await.unwrap();
        runtime.join_custodians().await.unwrap();
        assert!(runtime.joined.load(Ordering::SeqCst));
    }

    #[test]
    fn muse_driver_public_initialize_fingerprint_and_durability_are_separate() {
        let dir = Directory::new();
        let mut init = initialize(&dir.0);
        assert!(validate_initialize(&init).is_ok());
        assert_eq!(
            commands::session_durability(&init),
            commands::SessionDurability::Durable
        );
        init["schema"]["fingerprint"] = json!(format!("sha256:{}", "a".repeat(64)));
        assert!(validate_initialize(&init).is_ok());
        init["sessionDurability"] = json!("future-profile");
        assert!(validate_initialize(&init).is_ok());
        assert_eq!(
            commands::session_durability(&init),
            commands::SessionDurability::Unrecognized
        );
        init["schema"]["version"] = json!(2);
        assert_eq!(
            validate_initialize(&init).unwrap_err().code,
            "MUSE_SCHEMA_VERSION_UNSUPPORTED"
        );
    }
    #[test]
    fn muse_driver_native_connection_end_stales_usage_once() {
        let fence = ProjectionFence {
            binding_id: "synthetic-binding".into(),
            binding_generation: 1,
            native_root_id: ROOT.into(),
            native_scope_key: "synthetic-native-scope".into(),
            bridge_boot_id: BOOT.into(),
        };
        let projector = ObservationProjector::new(fence.clone())
            .unwrap()
            .begin_usage_connection(&fence, "synthetic-connection", 1000)
            .projector;
        assert_eq!(
            projector.observation().unwrap()["native_usage"]["freshness"],
            "current"
        );
        let mut state = State::empty();
        state.fence = Some(fence);
        state.projector = Some(projector);
        state.close_usage(1001);
        assert_eq!(
            state.projector.as_ref().unwrap().observation().unwrap()["native_usage"]["freshness"],
            "stale"
        );
        let revision = state.dirty;
        state.close_usage(1002);
        assert_eq!(
            state.dirty, revision,
            "repeated close does not invent a new usage observation"
        );
    }

    #[test]
    fn muse_driver_admission_requires_actual_public_ack_shape() {
        assert!(
            validate_ack(
                "turn/start",
                &json!({"commandId":"id","status":"accepted","turnId":"turn"})
            )
            .is_err()
        );
        assert!(validate_ack("turn/start",&json!({"commandId":"id","status":"accepted","turnId":"turn","disposition":"queued","startedNewTurn":false})).is_ok());
        assert!(
            validate_ack(
                "session/setApprovalMode",
                &json!({"commandId":"id","status":"accepted","effectiveMode":{"mode":"allowAll"}})
            )
            .is_err()
        );
    }
    #[tokio::test]
    async fn muse_driver_connected_open_dispatch_result_lost_host_ack_no_native_replay() {
        let dir = Directory::new();
        let (driver, runtime, peer) = fixture(&dir.0);
        let calls = Arc::new(Mutex::new(Vec::new()));
        let native = tokio::spawn(native_peer(peer, dir.0.clone(), calls.clone(), false));
        driver
            .clone()
            .execute(command(&driver, "01-open", "agent.open", json!({})))
            .await
            .unwrap();
        assert!(driver.lock().unwrap().ready);
        driver.clone().execute(dispatch(&driver)).await.unwrap();
        driver.clone().execute(command(&driver,"03-result","agent.result",json!({"selector":{"kind":"message","session_id":ROOT,"item_id":"answer","item_revision":7}}))).await.unwrap();
        let records = Arc::new(Mutex::new(Vec::new()));
        let host = host_pair(&dir.0, records.clone());
        let mut first = driver.bootstrap.link(&driver).await.unwrap();
        assert!(driver.flush(&mut first).await.is_err());
        drop(first);
        let retained = {
            let s = driver.lock().unwrap();
            let scope = s.scope.as_ref().unwrap();
            s.journal.as_ref().unwrap().outbox(scope).unwrap()
        };
        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0].method(), "module.result");
        let exact = retained[0].params_json().to_owned();
        let mut second = driver.bootstrap.link(&driver).await.unwrap();
        driver.flush(&mut second).await.unwrap();
        drop(second);
        host.await.unwrap();
        {
            let s = driver.lock().unwrap();
            assert!(
                s.journal
                    .as_ref()
                    .unwrap()
                    .outbox(s.scope.as_ref().unwrap())
                    .unwrap()
                    .is_empty()
            );
        }
        {
            let observed = records.lock().unwrap();
            let pages: Vec<_> = observed
                .iter()
                .filter(|r| r["method"] == "module.result")
                .collect();
            assert_eq!(pages.len(), 2);
            assert_eq!(pages[0]["params"], pages[1]["params"]);
            assert_eq!(
                serde_json::from_str::<Value>(&exact).unwrap(),
                pages[0]["params"]
            );
            let dispatch = observed
                .iter()
                .find(|r| r["params"]["operation_id"] == "02-dispatch")
                .unwrap();
            assert_eq!(dispatch["params"]["outcome"], "applied");
            assert_eq!(
                dispatch["params"]["details"]["completion_condition"],
                "native_input_admitted"
            );
            assert_eq!(dispatch["params"]["details"]["execution_complete"], false);
            assert_eq!(
                dispatch["params"]["details"]["dispatch_admission"]["task_revision"],
                2
            );
        }
        shutdown(&driver, &runtime).await;
        native.await.unwrap();
        assert_eq!(runtime.starts.load(Ordering::SeqCst), 1);
        let calls = calls.lock().unwrap();
        assert_eq!(
            calls
                .iter()
                .filter(|r| r["method"] == "session/start")
                .count(),
            1
        );
        assert_eq!(
            calls.iter().filter(|r| r["method"] == "turn/start").count(),
            1
        );
    }
    #[tokio::test]
    async fn muse_driver_lost_native_ack_stop_never_freshens_root_or_replays_input() {
        let dir = Directory::new();
        let (driver, runtime, peer) = fixture(&dir.0);
        let calls = Arc::new(Mutex::new(Vec::new()));
        let native = tokio::spawn(native_peer(peer, dir.0.clone(), calls.clone(), true));
        driver
            .clone()
            .execute(command(&driver, "01-open", "agent.open", json!({})))
            .await
            .unwrap();
        driver.clone().execute(dispatch(&driver)).await.unwrap();
        {
            let s = driver.lock().unwrap();
            let unresolved = s
                .journal
                .as_ref()
                .unwrap()
                .unresolved_native(s.scope.as_ref().unwrap())
                .unwrap();
            assert_eq!(unresolved.len(), 1);
            assert_eq!(unresolved[0].0, "02-dispatch");
            assert!(!unresolved[0].2);
            assert!(s.task_turns.is_empty());
        }
        driver
            .clone()
            .execute(command(&driver, "04-open", "agent.open", json!({})))
            .await
            .unwrap();
        shutdown(&driver, &runtime).await;
        native.await.unwrap();
        let calls = calls.lock().unwrap();
        assert_eq!(
            calls.iter().filter(|r| r["method"] == "turn/start").count(),
            1
        );
        assert_eq!(
            calls
                .iter()
                .filter(|r| r["method"] == "session/start")
                .count(),
            1
        );
        assert_eq!(runtime.starts.load(Ordering::SeqCst), 1);
    }
    #[tokio::test]
    async fn muse_driver_delta_cannot_invent_task_scope_or_trigger_observe_per_token() {
        let dir = Directory::new();
        let (driver, runtime, _peer) = fixture(&dir.0);
        let before = {
            let mut state = driver.lock().unwrap();
            state.confirmed_root = Some(ROOT.into());
            state.ready = true;
            state.dirty
        };
        driver.notification(Notification{envelope:json!({"method":"item/delta","params":{"sessionId":ROOT,"itemId":"unknown","field":"text","delta":"text","viewCursor":"opaque"}})}).unwrap();
        {
            let state = driver.lock().unwrap();
            assert!(state.stream_sequences.is_empty());
            assert_eq!(state.dirty, before);
        }
        shutdown(&driver, &runtime).await;
    }
}
