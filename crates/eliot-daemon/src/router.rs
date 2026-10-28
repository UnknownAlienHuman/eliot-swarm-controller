use crate::{
    bus::Bus,
    monitor::Monitoring,
    plugin_host::{AgentCredentials, PluginHost, PluginHostOptions, SecretSet, UnknownCommands},
    workspaces::WorkspaceManager,
};
use eliot_core::{
    Agent, AgentState, Command, CommandState, Error, Event, ProcessIdentity, Result, SecretPolicy,
    SubmitCommand, Turn, TurnState, WorkspacePhase, WorkspacePreparationInput,
    classify_dispatched_error,
};
use eliot_plugin::{
    MAX_EVENT_BYTES, MAX_OBSERVATION_CURSOR_BYTES, MAX_OBSERVATION_PAGE_BYTES, ObservationPage,
    ObservationQuery,
};
use futures_util::future::BoxFuture;
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{Mutex as AsyncMutex, broadcast, watch},
    time::{Instant, timeout_at},
};

const PAGE_MAX: usize = 1_000;
const PROCESS_CURSOR_MAX_BYTES: usize = 2_048;
const EVENT_SCAN_MAX: usize = 1_000;
const EVENT_RESPONSE_MAX_BYTES: usize = 900 * 1024;
const EVENT_RESPONSE_WRAPPER_RESERVE_BYTES: usize = 4 * 1024;
const EVENT_RESPONSE_ENTRIES_MAX_BYTES: usize =
    EVENT_RESPONSE_MAX_BYTES - EVENT_RESPONSE_WRAPPER_RESERVE_BYTES;
const LIVE_WAIT_MAX_MS: u64 = 30_000;
const STATUS_BLOCKER_MAX: usize = 64;
const CONTROLLED_PLUGIN_SOURCE: &str = "stream";

type StoreHandle = Arc<AsyncMutex<eliot_store::Store>>;

const MODE_OPEN: u8 = 0;
const MODE_DRAIN: u8 = 1;
const MODE_FORCE: u8 = 2;

#[derive(Clone)]
pub(crate) struct Router {
    inner: Arc<RouterInner>,
}

struct RouterInner {
    data_root: PathBuf,
    store: StoreHandle,
    bus: Bus,
    owner_token: String,
    secrets: SecretSet,
    agent_credentials: AgentCredentials,
    unknown_commands: UnknownCommands,
    unknown_causes: Mutex<HashMap<String, String>>,
    plugin_host: PluginHost,
    monitoring: Monitoring,
    workspaces: WorkspaceManager,
    workspace_recovery_error: Mutex<Option<String>>,
    mode: AtomicU8,
    shutdown_ready: AtomicBool,
    progress: watch::Sender<u64>,
}

#[derive(Clone)]
enum Principal {
    Owner,
    Agent { id: String, generation: i64 },
}

struct EventFilter {
    agents: HashSet<String>,
    kinds: HashSet<String>,
    sources: HashSet<String>,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessPageCursor {
    agent: String,
    generation: i64,
    scope: String,
    #[serde(deserialize_with = "deserialize_cursor_identity")]
    identity: ProcessIdentity,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ObserverCommandResult {
    agent_id: String,
    generation: i64,
    command_id: String,
    native_scope: String,
    native_ref: String,
    state: String,
    observation_state: String,
    #[serde(default)]
    metadata: Option<Value>,
    #[serde(default)]
    coverage: Option<Value>,
    #[serde(default)]
    reader_closed: Option<bool>,
}

fn deserialize_cursor_identity<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<ProcessIdentity, D::Error> {
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Identity {
        host_boot_id: String,
        pid: u32,
        start_time: u64,
    }
    let identity = <Identity as serde::Deserialize<'de>>::deserialize(deserializer)?;
    Ok(ProcessIdentity {
        host_boot_id: identity.host_boot_id,
        pid: identity.pid,
        start_time: identity.start_time,
    })
}

impl Router {
    pub(crate) fn new(
        data_root: std::path::PathBuf,
        owner_token: String,
        store: StoreHandle,
        bus: Bus,
    ) -> Self {
        let secrets: SecretSet = Arc::new(Mutex::new(HashSet::from([owner_token.clone()])));
        let agent_credentials: AgentCredentials = Arc::new(Mutex::new(Default::default()));
        let unknown_commands: UnknownCommands = Arc::new(Mutex::new(Default::default()));
        let (progress, _) = watch::channel(0_u64);
        let monitoring = Monitoring::new(Arc::clone(&store), bus.clone(), Arc::clone(&secrets));
        let workspaces = WorkspaceManager::new(
            data_root.clone(),
            Arc::clone(&store),
            bus.clone(),
            Arc::clone(&secrets),
            progress.clone(),
            monitoring.clone(),
        );
        let plugin_host = PluginHost::new(
            PluginHostOptions::new(data_root.clone(), owner_token.clone()),
            Arc::clone(&store),
            bus.clone(),
            Arc::clone(&secrets),
            Arc::clone(&agent_credentials),
            Arc::clone(&unknown_commands),
            progress.clone(),
            monitoring.clone(),
        );
        Self {
            inner: Arc::new(RouterInner {
                data_root,
                store,
                bus,
                owner_token,
                secrets,
                agent_credentials,
                unknown_commands,
                unknown_causes: Mutex::new(HashMap::new()),
                plugin_host,
                monitoring,
                workspaces,
                workspace_recovery_error: Mutex::new(None),
                mode: AtomicU8::new(MODE_OPEN),
                shutdown_ready: AtomicBool::new(false),
                progress,
            }),
        }
    }

    pub(crate) async fn handle(&self, method: &str, wire_params: Value) -> Result<Value> {
        let (principal, params) = self.authenticate(wire_params).await?;
        match method {
            "daemon.status" => {
                require_owner(&principal)?;
                reject_unknown_keys(&params, &[], method)?;
                self.status().await
            }
            "daemon.shutdown" => {
                require_owner(&principal)?;
                self.shutdown(&params, &principal).await
            }
            "command.submit" => self.submit(&params, &principal).await,
            "command.get" => self.command_get(&params, &principal).await,
            "agent.get" => self.agent_get(&params, &principal).await,
            "agent.discover" => self.agent_discover(&params, &principal).await,
            "agent.history" => self.agent_history(&params, &principal).await,
            "agent.list" => self.agent_list(&params, &principal).await,
            "agent.result" => self.agent_result(&params, &principal).await,
            "agent.processes" => self.agent_processes(&params, &principal).await,
            "agent.stats" => self.agent_stats(&params, &principal).await,
            "agent.git" => self.agent_git(&params, &principal).await,
            "agent.edits" => self.agent_edits(&params, &principal).await,
            "turn.get" => self.turn_get(&params, &principal).await,
            "events.read" => self.events_read(&params, &principal).await,
            "events.subscribe" => self.events_subscribe(&params, &principal).await,
            _ => Err(Error::new(
                "unsupported",
                format!("daemon method {method} is not implemented"),
            )),
        }
    }

    pub(crate) fn progress_receiver(&self) -> watch::Receiver<u64> {
        self.inner.progress.subscribe()
    }

    pub(crate) fn monitoring(&self) -> Monitoring {
        self.inner.monitoring.clone()
    }

    pub(crate) fn response_finished(&self, shutdown_requested: bool) {
        if shutdown_requested && self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN {
            self.inner.shutdown_ready.store(true, Ordering::SeqCst);
        }
        self.signal_progress();
    }

    pub(crate) fn request_force(&self) {
        self.inner.plugin_host.close_discovery_admission();
        self.inner.mode.store(MODE_FORCE, Ordering::SeqCst);
        self.inner.shutdown_ready.store(true, Ordering::SeqCst);
        self.signal_progress();
    }

    pub(crate) fn is_force_requested(&self) -> bool {
        self.inner.mode.load(Ordering::SeqCst) == MODE_FORCE
    }

    pub(crate) async fn should_exit(&self) -> Result<bool> {
        match self.inner.mode.load(Ordering::SeqCst) {
            MODE_FORCE => Ok(self.inner.shutdown_ready.load(Ordering::SeqCst)),
            MODE_DRAIN if self.inner.shutdown_ready.load(Ordering::SeqCst) => {
                Ok(self.status_blockers().await?.is_empty())
            }
            MODE_DRAIN => Ok(false),
            _ => Ok(false),
        }
    }

    pub(crate) async fn force_owned_processes(&self) {
        self.inner.workspaces.stop_all(true).await;
        self.inner.plugin_host.force_all().await;
    }

    pub(crate) async fn recover_workspaces(&self) -> Result<()> {
        for id in self.inner.workspaces.recover().await? {
            let linked = {
                let store = self.inner.store.lock().await;
                let Some(command) = store.get_command(&id)? else {
                    continue;
                };
                if command.state != CommandState::Accepted
                    || !matches!(command.verb.as_str(), "launch" | "resume")
                {
                    continue;
                }
                let Some(target) = command.target.as_deref() else {
                    continue;
                };
                let Some(preparation) = store.get_workspace(target)? else {
                    continue;
                };
                if preparation.start_command_id != id
                    || !matches!(
                        preparation.phase,
                        WorkspacePhase::Ready | WorkspacePhase::SetupReady
                    )
                {
                    continue;
                }
                store
                    .get_command(&format!("{id}/send"))?
                    .filter(|linked| {
                        linked.target.as_deref() == Some(target)
                            && linked.generation == Some(preparation.target_generation)
                    })
                    .map(|linked| linked.id)
            };
            tokio::spawn(self.clone().process_command(id, linked));
        }
        self.signal_progress();
        Ok(())
    }

    pub(crate) fn note_workspace_recovery_failure(&self, code: &str) {
        *self
            .inner
            .workspace_recovery_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(code.to_owned());
        self.signal_progress();
    }

    pub(crate) fn note_events(&self, events: &[Event]) {
        note_unknown(&self.inner.unknown_commands, events);
        self.signal_progress();
    }

    fn signal_progress(&self) {
        self.inner.progress.send_modify(|value| {
            *value = value.wrapping_add(1);
        });
    }

    async fn authenticate(&self, wire_params: Value) -> Result<(Principal, Value)> {
        let mut envelope = wire_params.as_object().cloned().ok_or_else(|| {
            Error::new(
                "invalid",
                "daemon params must contain an authentication envelope",
            )
        })?;
        let auth = envelope
            .remove("_auth")
            .ok_or_else(|| Error::new("not_allowed", "daemon call is missing credentials"))?;
        let payload = envelope
            .remove("_payload")
            .ok_or_else(|| Error::new("invalid", "daemon call is missing its payload"))?;
        if !envelope.is_empty() || !payload.is_object() {
            return Err(Error::new("invalid", "daemon call envelope is malformed"));
        }
        let auth = auth
            .as_object()
            .ok_or_else(|| Error::new("not_allowed", "daemon credentials are malformed"))?;
        let kind = auth.get("kind").and_then(Value::as_str).unwrap_or_default();
        let credential = auth
            .get("credential")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if credential.is_empty() {
            return Err(Error::new(
                "not_allowed",
                "daemon credentials are incomplete",
            ));
        }
        match kind {
            "owner" => {
                if secure_eq(credential, &self.inner.owner_token) {
                    Ok((Principal::Owner, payload))
                } else {
                    Err(Error::new("not_allowed", "owner credential is invalid"))
                }
            }
            "agent" => {
                let id = auth
                    .get("agent_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let generation = auth
                    .get("generation")
                    .and_then(Value::as_i64)
                    .ok_or_else(|| Error::new("not_allowed", "Agent generation is missing"))?;
                if !self.inner.plugin_host.controls_generation(id, generation) {
                    return Err(Error::new(
                        "stale_generation",
                        "Agent generation has no live plugin owner",
                    ));
                }
                let stored_token = {
                    let store = self.inner.store.lock().await;
                    store.get_agent_token(id, generation)?
                };
                if !stored_token
                    .as_deref()
                    .is_some_and(|known| secure_eq(credential, known))
                {
                    return Err(Error::new(
                        "not_allowed",
                        "Agent credential is invalid or revoked",
                    ));
                }
                let agent = {
                    let store = self.inner.store.lock().await;
                    store.get_agent(id)?
                }
                .ok_or_else(|| Error::new("not_found", format!("Agent {id} was not found")))?;
                if agent.generation != generation || agent.is_terminal() {
                    return Err(Error::new(
                        "stale_generation",
                        "Agent credential does not match a live generation",
                    ));
                }
                if !self.inner.plugin_host.controls_generation(id, generation) {
                    return Err(Error::new(
                        "stale_generation",
                        "Agent generation lost its live plugin owner during authentication",
                    ));
                }
                {
                    self.inner
                        .agent_credentials
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .insert((id.to_owned(), generation), credential.to_owned());
                    self.inner
                        .secrets
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .insert(credential.to_owned());
                }
                Ok((
                    Principal::Agent {
                        id: id.to_owned(),
                        generation,
                    },
                    payload,
                ))
            }
            _ => Err(Error::new("not_allowed", "daemon principal is unsupported")),
        }
    }

    async fn submit(&self, params: &Value, principal: &Principal) -> Result<Value> {
        reject_unknown_keys(
            params,
            &[
                "id",
                "target",
                "verb",
                "args",
                "expected_generation",
                "expected_turn_id",
                "request_id",
            ],
            "command.submit",
        )?;
        let mut request: SubmitCommand = serde_json::from_value(params.clone())
            .map_err(|_| Error::new("invalid", "command.submit params are malformed"))?;
        authorize_command(principal, &mut request)?;
        if matches!(request.verb.as_str(), "attach" | "detach") {
            require_owner(principal)?;
        }
        if request.verb == "worktree.remove" {
            require_owner(principal)?;
        }
        if is_native_control_verb(&request.verb)
            && let Some(target) = request.target.as_deref()
        {
            let store = self.inner.store.lock().await;
            // Preserve idempotent command retries; Store checks new controls
            // transactionally after resolving an existing command binding.
            if store.get_command(&request.id)?.is_none() {
                let agent = store.get_agent(target)?.ok_or_else(|| {
                    Error::new("not_found", format!("Agent {target} was not found"))
                })?;
                if agent.observe_only {
                    return Err(Error::new(
                        "not_allowed",
                        "native control is unavailable for an observe-only Agent",
                    ));
                }
            }
        }
        let mode = self.inner.mode.load(Ordering::SeqCst);
        if mode == MODE_FORCE {
            return Err(Error::new("unavailable", "daemon is forcing shutdown"));
        }
        if request.verb == "detach" {
            self.hydrate_detach_generation(&mut request).await?;
        }
        if matches!(
            request.verb.as_str(),
            "launch" | "resume" | "send" | "attach"
        ) && mode != MODE_OPEN
        {
            return Err(Error::new(
                "unavailable",
                "daemon is draining and rejects new launch, resume, send, or attach commands",
            ));
        }
        let mut preparation = None;
        if request.verb == "launch" {
            reject_launch_derived_keys(&request.args)?;
            if self.hydrate_retained_launch(&mut request).await? {
                preparation = retained_preparation(&request.args)?;
            } else {
                let frozen = async {
                    normalize_launch(&mut request)?;
                    self.freeze_launch_admission(&mut request).await
                }
                .await;
                preparation = match frozen {
                    Ok(input) => input,
                    Err(error) => {
                        // Another identical admission can win while config/Git
                        // readback is in flight. Reuse its original frozen input.
                        if self.hydrate_retained_launch(&mut request).await? {
                            retained_preparation(&request.args)?
                        } else {
                            return Err(error);
                        }
                    }
                };
            }
        } else if request.verb == "attach" {
            normalize_attach(&request)?;
            if !self.hydrate_retained_attach(&mut request).await?
                && let Err(error) = freeze_attach_profile(&self.inner.data_root, &mut request)
                && !self.hydrate_retained_attach(&mut request).await?
            {
                return Err(error);
            }
        } else if request.verb == "resume" {
            normalize_resume(&mut request)?;
        } else if matches!(
            request.verb.as_str(),
            "send" | "steer" | "interrupt" | "approve" | "deny" | "answer"
        ) {
            validate_plugin_command_args(&request)?;
        }
        if contains_known_secret(&request.args, &self.inner.secrets) {
            return Err(Error::new(
                "secret_in_payload",
                "durable command text contains a known credential",
            ));
        }
        if let Some(input) = &preparation {
            let metadata = serde_json::to_value(input)
                .map_err(|_| Error::new("internal", "workspace input could not be encoded"))?;
            if contains_known_secret(&metadata, &self.inner.secrets) {
                return Err(Error::new(
                    "secret_in_payload",
                    "workspace input contains a known credential",
                ));
            }
        }
        if request.request_id.as_deref().is_some_and(|value| {
            contains_known_secret(&Value::String(value.to_owned()), &self.inner.secrets)
        }) || request.expected_turn_id.as_deref().is_some_and(|value| {
            contains_known_secret(&Value::String(value.to_owned()), &self.inner.secrets)
        }) {
            return Err(Error::new(
                "secret_in_payload",
                "durable command binding contains a known credential",
            ));
        }
        if matches!(request.verb.as_str(), "launch" | "resume" | "send")
            && request
                .args
                .get("text")
                .and_then(Value::as_str)
                .is_some_and(|text| text.len() > MAX_EVENT_BYTES)
        {
            return Err(Error::new(
                "frame_too_large",
                "command text exceeds the 64 KiB persisted-text limit",
            ));
        }

        let admission = {
            let mut store = self.inner.store.lock().await;
            if request.verb == "answer" {
                validate_answer_request(&store, &request)?;
            }
            let principal = principal_name(principal);
            let now = now_ms();
            let submitted = match preparation.take() {
                Some(input) => store.submit_with_workspace(&principal, request.clone(), input, now),
                None => store.submit(&principal, request.clone(), now),
            };
            let admission = match submitted {
                Ok(admission) => admission,
                Err(error) if error.code == "id_conflict" && request.verb == "launch" => {
                    if let Some(retained) = retained_launch_profile(&store, &request)? {
                        install_retained_profile(&mut request, retained)?;
                        store.submit(&principal, request, now)?
                    } else {
                        return Err(error);
                    }
                }
                Err(error) if error.code == "id_conflict" && request.verb == "attach" => {
                    if install_retained_attach_profile(&store, &mut request)? {
                        store.submit(&principal, request, now)?
                    } else {
                        return Err(error);
                    }
                }
                Err(error) => return Err(error),
            };
            self.publish_locked(&admission.events);
            admission
        };
        note_unknown(&self.inner.unknown_commands, &admission.events);
        if admission.command.state == CommandState::Unknown {
            self.inner
                .unknown_commands
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(admission.command.id.clone());
        }

        if admission.is_new {
            let command_id = admission.command.id.clone();
            let linked_id = admission
                .linked_command
                .as_ref()
                .map(|command| command.id.clone());
            let router = self.clone();
            tokio::spawn(router.process_command(command_id, linked_id));
        }
        self.signal_progress();
        Ok(submit_result(
            &admission.command,
            admission.linked_command.as_ref(),
        ))
    }

    async fn hydrate_retained_launch(&self, request: &mut SubmitCommand) -> Result<bool> {
        if let Some(retained) = {
            let store = self.inner.store.lock().await;
            retained_launch_profile(&store, request)?
        } {
            install_retained_profile(request, retained)?;
            return Ok(true);
        }
        Ok(false)
    }

    async fn hydrate_retained_attach(&self, request: &mut SubmitCommand) -> Result<bool> {
        let store = self.inner.store.lock().await;
        install_retained_attach_profile(&store, request)
    }

    async fn hydrate_detach_generation(&self, request: &mut SubmitCommand) -> Result<()> {
        let store = self.inner.store.lock().await;
        if let Some(command) = store.get_command(&request.id)? {
            if command.verb != "detach" {
                return Err(Error::new(
                    "id_conflict",
                    "mutation id is already bound to another command",
                ));
            }
            if request.expected_generation.is_none() {
                request.expected_generation = command.expected_generation;
            }
            return Ok(());
        }
        if request.expected_generation.is_none() {
            let target = request
                .target
                .as_deref()
                .ok_or_else(|| Error::new("invalid", "detach requires an Agent target"))?;
            let agent = store
                .get_agent(target)?
                .ok_or_else(|| Error::new("not_found", format!("Agent {target} was not found")))?;
            request.expected_generation = Some(agent.generation);
        }
        Ok(())
    }

    async fn freeze_launch_admission(
        &self,
        request: &mut SubmitCommand,
    ) -> Result<Option<WorkspacePreparationInput>> {
        if self.freeze_launch_profile(request).await? {
            return retained_preparation(&request.args);
        }
        let Some(branch) = request.args.get("worktree").and_then(Value::as_str) else {
            if request
                .args
                .get("profile_snapshot")
                .and_then(|profile| profile.get("setup"))
                .and_then(Value::as_array)
                .is_some_and(|setup| !setup.is_empty())
            {
                return Err(Error::new(
                    "unsupported",
                    "profile setup requires --worktree",
                ));
            }
            return Ok(None);
        };
        if contains_known_secret(&request.args, &self.inner.secrets) {
            return Err(Error::new(
                "secret_in_payload",
                "durable launch contains a known credential",
            ));
        }
        let input = self
            .inner
            .workspaces
            .freeze_launch(
                required_text(&request.args, "workspace")?,
                branch,
                request.args.get("base").and_then(Value::as_str),
                request
                    .args
                    .get("profile_snapshot")
                    .ok_or_else(|| Error::new("internal", "launch profile was not frozen"))?,
            )
            .await?;
        let metadata = serde_json::to_value(&input)
            .map_err(|_| Error::new("internal", "workspace input could not be encoded"))?;
        request
            .args
            .as_object_mut()
            .ok_or_else(|| Error::new("invalid", "launch args must be an object"))?
            .insert("workspace_preparation".to_owned(), metadata);
        Ok(Some(input))
    }

    async fn freeze_launch_profile(&self, request: &mut SubmitCommand) -> Result<bool> {
        if self.hydrate_retained_launch(request).await? {
            return Ok(true);
        }

        let profile_name = required_text(&request.args, "profile")?;
        let snapshot = crate::config::resolve_profile(&self.inner.data_root, profile_name)?;
        let harness = snapshot
            .get("harness")
            .and_then(Value::as_str)
            .filter(|harness| !harness.trim().is_empty())
            .ok_or_else(|| Error::new("invalid", "resolved profile must name its harness"))?
            .to_owned();
        install_frozen_profile(request, &harness, snapshot, true)?;
        Ok(false)
    }

    async fn command_get(&self, params: &Value, principal: &Principal) -> Result<Value> {
        reject_unknown_keys(params, &["id"], "command.get")?;
        let id = required_text(params, "id")?;
        let mut command = {
            let store = self.inner.store.lock().await;
            store.get_command(id)?
        }
        .ok_or_else(|| Error::new("not_found", format!("command {id} was not found")))?;
        authorize_command_read(principal, &command)?;
        if command.state == CommandState::Unknown {
            command.turn_id = None;
        } else if let Some(cause) = self.unknown_cause(id) {
            command.state = CommandState::Unknown;
            command.outcome = Some(json!({"code": "unknown", "cause": cause}));
            command.turn_id = None;
        }
        serde_json::to_value(command)
            .map_err(|error| Error::new("internal", format!("encode command: {error}")))
    }

    async fn agent_discover(&self, params: &Value, principal: &Principal) -> Result<Value> {
        require_owner(principal)?;
        reject_unknown_keys(
            params,
            &["profile", "harness", "native_scope", "cursor", "limit"],
            "agent.discover",
        )?;
        if self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN {
            return Err(Error::new(
                "unavailable",
                "daemon is shutting down and rejects new discovery",
            ));
        }
        let name = required_text(params, "profile")?;
        let harness = optional_text(params, "harness")?;
        let native_scope = optional_text(params, "native_scope")?.map(str::to_owned);
        let cursor = optional_text(params, "cursor")?.map(str::to_owned);
        let limit = page_limit(params)?;
        let profile = crate::config::resolve_profile(&self.inner.data_root, name)?;
        if harness
            .is_some_and(|harness| profile.get("harness").and_then(Value::as_str) != Some(harness))
        {
            return Err(Error::new(
                "invalid",
                "harness does not match the selected profile",
            ));
        }
        if self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN {
            return Err(Error::new(
                "unavailable",
                "daemon is shutting down and rejects new discovery",
            ));
        }
        self.inner
            .plugin_host
            .discover(profile, native_scope, cursor, limit)
            .await
    }

    async fn agent_history(&self, params: &Value, principal: &Principal) -> Result<Value> {
        require_owner(principal)?;
        reject_unknown_keys(
            params,
            &[
                "id",
                "generation",
                "source",
                "view",
                "turn_id",
                "cursor",
                "limit",
            ],
            "agent.history",
        )?;
        let id = required_text(params, "id")?;
        let generation = optional_observation_generation(params)?;
        let source = required_text(params, "source")?;
        if !matches!(source, "native" | "rollout") {
            return Err(Error::new(
                "invalid",
                "history source must be native or rollout",
            ));
        }
        let view = required_text(params, "view")?;
        let turn_id = optional_text(params, "turn_id")?.map(str::to_owned);
        if turn_id.as_ref().is_some_and(|turn_id| {
            turn_id.len() > MAX_OBSERVATION_CURSOR_BYTES || turn_id.chars().any(char::is_control)
        }) {
            return Err(Error::new("invalid", "history turn_id exceeds its bound"));
        }
        if (view == "items") != turn_id.is_some() {
            return Err(Error::new(
                "invalid",
                "history items require a turn_id and other views forbid it",
            ));
        }
        let query = ObservationQuery {
            source: source.to_owned(),
            view: view.to_owned(),
            turn_id,
            cursor: optional_text(params, "cursor")?.map(str::to_owned),
            limit: page_limit(params)?,
        };
        query.validate()?;

        let agent = {
            let store = self.inner.store.lock().await;
            store
                .get_agent(id)?
                .ok_or_else(|| Error::new("not_found", format!("Agent {id} was not found")))?
        };
        if generation.is_some_and(|generation| generation != agent.generation) {
            return Err(Error::new(
                "stale_generation",
                "native history is bound to the current observer generation",
            ));
        }
        if !agent.observe_only || agent.own_identity {
            return Err(Error::new(
                "not_allowed",
                "native history requires an external observer without control identity",
            ));
        }
        let native_scope = agent
            .native_scope
            .as_deref()
            .filter(|scope| !scope.is_empty())
            .ok_or_else(|| Error::new("bad_state", "Agent has no native scope"))?;
        let native_ref = agent
            .native_ref
            .as_deref()
            .filter(|native_ref| !native_ref.is_empty())
            .ok_or_else(|| Error::new("bad_state", "Agent has no native reference"))?;

        // This is a private native read. It is deliberately not reduced, stored,
        // published, or used to settle a command or input acknowledgement.
        let raw = self
            .inner
            .plugin_host
            .snapshot_observer(
                &agent,
                serde_json::to_value(&query).map_err(|_| {
                    Error::new("internal", "observation query could not be encoded")
                })?,
            )
            .await?;
        let object = raw
            .as_object()
            .ok_or_else(|| Error::new("source_integrity", "observation page is not an object"))?;
        const PAGE_FIELDS: &[&str] = &[
            "agent_id",
            "generation",
            "native_scope",
            "native_ref",
            "source",
            "view",
            "entries",
            "next_cursor",
            "coverage",
        ];
        if object
            .keys()
            .any(|key| !PAGE_FIELDS.contains(&key.as_str()))
        {
            return Err(Error::new(
                "source_integrity",
                "observation page contains unexpected fields",
            ));
        }
        let page: ObservationPage = serde_json::from_value(raw).map_err(|_| {
            Error::new(
                "source_integrity",
                "plugin returned an invalid observation page",
            )
        })?;
        page.validate()?;
        if page.agent_id != agent.id
            || page.generation != agent.generation
            || page.native_scope != native_scope
            || page.native_ref != native_ref
            || page.source != query.source
            || page.view != query.view
            || page.entries.len() > query.limit
            || !page.coverage.is_object()
            || page
                .next_cursor
                .as_deref()
                .is_some_and(|cursor| query.cursor.as_deref() == Some(cursor))
        {
            return Err(Error::new(
                "source_integrity",
                "observation page does not match its subject or query",
            ));
        }
        if serde_json::to_vec(&page)
            .map_err(|_| Error::new("internal", "observation page could not be encoded"))?
            .len()
            > MAX_OBSERVATION_PAGE_BYTES
        {
            return Err(Error::new(
                "frame_too_large",
                "observation page exceeds its byte bound",
            ));
        }
        let mut value = serde_json::to_value(page)
            .map_err(|_| Error::new("internal", "observation page could not be encoded"))?;
        self.redact_native_value(&mut value);
        Ok(value)
    }

    async fn agent_get(&self, params: &Value, principal: &Principal) -> Result<Value> {
        reject_unknown_keys(params, &["id"], "agent.get")?;
        let id = required_text(params, "id")?;
        authorize_agent_read(principal, id, None)?;
        let (agent, source_states, observer_active) = {
            let store = self.inner.store.lock().await;
            let agent = store
                .get_agent(id)?
                .ok_or_else(|| Error::new("not_found", format!("Agent {id} was not found")))?;
            let mut source_states = store.list_source_states(id, agent.generation)?;
            if let Some(workspace) = store.get_workspace(id)?
                && workspace.target_generation != agent.generation
            {
                source_states.extend(store.list_source_states(id, workspace.target_generation)?);
            }
            let observer_active =
                agent.observe_only && store.observer_active(&agent.id, agent.generation)?;
            (agent, source_states, observer_active)
        };
        let live_presence = if agent.observe_only {
            self.inner
                .plugin_host
                .observes_generation(&agent.id, agent.generation)
        } else {
            self.inner
                .plugin_host
                .controls_generation(&agent.id, agent.generation)
        };
        let state_requires_presence =
            matches!(
                agent.state,
                AgentState::Idle | AgentState::Busy | AgentState::Waiting
            ) || (agent.observe_only && agent.state == AgentState::Unknown && observer_active);
        if state_requires_presence && !live_presence {
            return Err(Error::new(
                "unknown",
                "persisted Agent state has no live source owner",
            ));
        }
        let observation = self.observation_view(&agent).await;
        let mut value = serde_json::to_value(agent)
            .map_err(|error| Error::new("internal", format!("encode Agent: {error}")))?;
        value["observation"] = observation;
        value["source_states"] = json!(source_states);
        Ok(value)
    }

    async fn observation_view(&self, agent: &Agent) -> Value {
        match self.inner.monitoring.view(agent).await {
            Ok(value) => value,
            Err(error) => json!({"available": false, "error_code": error.code}),
        }
    }

    async fn agent_processes(&self, params: &Value, principal: &Principal) -> Result<Value> {
        reject_unknown_keys(
            params,
            &["id", "generation", "scope", "after", "limit"],
            "agent.processes",
        )?;
        let id = required_text(params, "id")?;
        let generation = optional_observation_generation(params)?;
        authorize_agent_read(principal, id, generation)?;
        let scope = optional_text(params, "scope")?.unwrap_or("agent");
        if !matches!(scope, "agent" | "workspace") {
            return Err(Error::new(
                "invalid",
                "process scope must be agent or workspace",
            ));
        }
        let after = optional_text(params, "after")?
            .map(|cursor| {
                if cursor.len() > PROCESS_CURSOR_MAX_BYTES {
                    return Err(Error::new("invalid", "process cursor exceeds its bound"));
                }
                let cursor: ProcessPageCursor = serde_json::from_str(cursor)
                    .map_err(|_| Error::new("invalid", "invalid process cursor"))?;
                let identity = &cursor.identity;
                if identity.pid == 0
                    || identity.start_time == 0
                    || identity.host_boot_id.is_empty()
                    || identity.host_boot_id.len() > 512
                    || identity.host_boot_id.chars().any(char::is_control)
                {
                    return Err(Error::new("invalid", "invalid process cursor identity"));
                }
                Ok(cursor)
            })
            .transpose()?;
        let limit = page_limit(params)?;
        let (generation, processes) = {
            let store = self.inner.store.lock().await;
            let agent = store
                .get_agent(id)?
                .ok_or_else(|| Error::new("not_found", format!("Agent {id} was not found")))?;
            let workspace = if scope == "workspace" {
                store.get_workspace(id)?
            } else {
                None
            };
            let generation = match generation {
                Some(generation) => generation,
                None if scope == "workspace" => {
                    workspace
                        .as_ref()
                        .ok_or_else(|| Error::new("not_found", "Agent has no managed workspace"))?
                        .target_generation
                }
                None => agent.generation,
            };
            if generation > agent.generation
                && (scope != "workspace"
                    || workspace.as_ref().map(|row| row.target_generation) != Some(generation))
            {
                return Err(Error::new(
                    "stale_generation",
                    "process generation does not exist",
                ));
            }
            if scope == "workspace" {
                authorize_agent_read(principal, id, Some(generation))?;
            }
            if let Some(cursor) = &after
                && (cursor.agent != id || cursor.generation != generation || cursor.scope != scope)
            {
                return Err(Error::new(
                    "invalid",
                    "process cursor does not match the Agent, generation, and scope",
                ));
            }
            let owner = format!("{scope}:{id}#{generation}");
            (
                generation,
                store.list_processes(
                    &owner,
                    after.as_ref().map(|cursor| &cursor.identity),
                    limit,
                )?,
            )
        };
        let next = processes
            .last()
            .map(|process| {
                let cursor = serde_json::to_string(&ProcessPageCursor {
                    agent: id.to_owned(),
                    generation,
                    scope: scope.to_owned(),
                    identity: process.identity.clone(),
                })
                .map_err(|_| Error::new("internal", "cannot encode process cursor"))?;
                if cursor.len() > PROCESS_CURSOR_MAX_BYTES {
                    return Err(Error::new(
                        "frame_too_large",
                        "encoded process cursor exceeds its bound",
                    ));
                }
                Ok(cursor)
            })
            .transpose()?;
        let mut value =
            json!({"agent": id, "generation": generation, "processes": processes, "next": next});
        if scope == "workspace" {
            value["scope"] = json!(scope);
        }
        Ok(value)
    }

    async fn agent_git(&self, params: &Value, principal: &Principal) -> Result<Value> {
        reject_unknown_keys(params, &["id", "generation"], "agent.git")?;
        let id = required_text(params, "id")?;
        let expected = optional_observation_generation(params)?;
        authorize_agent_read(principal, id, expected)?;
        let agent = {
            let store = self.inner.store.lock().await;
            store.get_agent(id)?
        }
        .ok_or_else(|| Error::new("not_found", format!("Agent {id} was not found")))?;
        if expected.is_some_and(|generation| generation != agent.generation) {
            return Err(Error::new(
                "stale_generation",
                "Git view requires the current Agent generation",
            ));
        }
        let mut value = match self.inner.monitoring.git_view(&agent).await {
            Ok(value) => value,
            Err(error) => json!({"available": false, "error_code": error.code}),
        };
        value["agent"] = json!(agent.id);
        value["generation"] = json!(agent.generation);
        Ok(value)
    }

    async fn agent_edits(&self, params: &Value, principal: &Principal) -> Result<Value> {
        reject_unknown_keys(
            params,
            &["id", "generation", "after", "limit"],
            "agent.edits",
        )?;
        let id = required_text(params, "id")?;
        let generation = optional_observation_generation(params)?;
        authorize_agent_read(principal, id, generation)?;
        let after = match params.get("after") {
            None | Some(Value::Null) => None,
            Some(Value::String(path))
                if !path.is_empty() && path.len() <= 64 * 1_024 && !path.contains('\0') =>
            {
                Some(path.as_str())
            }
            Some(_) => return Err(Error::new("invalid", "invalid edit cursor")),
        };
        let limit = page_limit(params)?;
        let (generation, edits) = {
            let store = self.inner.store.lock().await;
            let agent = store
                .get_agent(id)?
                .ok_or_else(|| Error::new("not_found", format!("Agent {id} was not found")))?;
            let generation = generation.unwrap_or(agent.generation);
            if generation > agent.generation {
                return Err(Error::new(
                    "stale_generation",
                    "edit generation does not exist",
                ));
            }
            (
                generation,
                store.list_agent_edits(id, generation, after, limit)?,
            )
        };
        let next = edits.last().map(|edit| edit.path.clone());
        Ok(json!({"agent": id, "generation": generation, "edits": edits, "next": next}))
    }

    async fn agent_stats(&self, params: &Value, principal: &Principal) -> Result<Value> {
        reject_unknown_keys(params, &["id", "generation"], "agent.stats")?;
        let id = required_text(params, "id")?;
        let expected = optional_observation_generation(params)?;
        authorize_agent_read(principal, id, expected)?;
        let agent = {
            let store = self.inner.store.lock().await;
            store.get_agent(id)?
        }
        .ok_or_else(|| Error::new("not_found", format!("Agent {id} was not found")))?;
        if expected.is_some_and(|generation| generation != agent.generation) {
            return Err(Error::new(
                "stale_generation",
                "stats require the current Agent generation",
            ));
        }
        let mut value = self.observation_view(&agent).await;
        value["agent"] = json!(agent.id);
        value["generation"] = json!(agent.generation);
        Ok(value)
    }

    async fn agent_list(&self, params: &Value, principal: &Principal) -> Result<Value> {
        reject_unknown_keys(params, &["after", "limit", "observations"], "agent.list")?;
        let after = optional_text(params, "after")?;
        let limit = page_limit(params)?;
        let observations = match params.get("observations") {
            None => false,
            Some(value) => value
                .as_bool()
                .ok_or_else(|| Error::new("invalid", "observations must be a boolean"))?,
        };
        let agents = {
            let store = self.inner.store.lock().await;
            match principal {
                Principal::Owner => store.list_agents(after, limit)?,
                Principal::Agent { id, .. } => {
                    store.get_agent(id)?.into_iter().collect::<Vec<Agent>>()
                }
            }
        };
        if observations {
            let mut rows = Vec::new();
            let mut encoded_bytes = 0_usize;
            for agent in agents {
                let mut row = serde_json::to_value(&agent)
                    .map_err(|_| Error::new("internal", "cannot encode Agent projection"))?;
                row["observation"] = self.observation_view(&agent).await;
                let row_bytes = serde_json::to_vec(&row)
                    .map_err(|_| Error::new("internal", "cannot encode Agent observation"))?
                    .len();
                if encoded_bytes.saturating_add(row_bytes + 1) > EVENT_RESPONSE_ENTRIES_MAX_BYTES {
                    if rows.is_empty() {
                        return Err(Error::new(
                            "frame_too_large",
                            "Agent observation exceeds the page byte limit",
                        ));
                    }
                    break;
                }
                encoded_bytes += row_bytes + 1;
                rows.push(row);
            }
            let next = match principal {
                Principal::Owner => rows.last().and_then(|row| row.get("id")).cloned(),
                Principal::Agent { .. } => None,
            };
            return Ok(json!({"agents": rows, "next": next}));
        }
        let next = match principal {
            Principal::Owner => agents.last().map(|agent| Value::String(agent.id.clone())),
            Principal::Agent { .. } => None,
        };
        Ok(json!({
            "agents": agents,
            "next": next,
        }))
    }

    async fn agent_result(&self, params: &Value, principal: &Principal) -> Result<Value> {
        reject_unknown_keys(
            params,
            &["id", "generation", "turn_id", "limit"],
            "agent.result",
        )?;
        let id = required_text(params, "id")?;
        authorize_agent_read(
            principal,
            id,
            params.get("generation").and_then(Value::as_i64),
        )?;
        let (agent_metadata, generation, mut turns) = {
            let store = self.inner.store.lock().await;
            let agent = store
                .get_agent(id)?
                .ok_or_else(|| Error::new("not_found", format!("Agent {id} was not found")))?;
            let generation = params
                .get("generation")
                .and_then(Value::as_i64)
                .unwrap_or(agent.generation);
            let turns = if let Some(turn_id) = params.get("turn_id").and_then(Value::as_str) {
                vec![store.get_turn(id, generation, turn_id)?.ok_or_else(|| {
                    Error::new("not_found", format!("turn {turn_id} was not found"))
                })?]
            } else {
                store.list_turns(id, generation, page_limit(params)?)?
            };
            (agent, generation, turns)
        };
        self.mask_unknown_turns(id, generation, &mut turns).await?;
        Ok(json!({
            "agent": id,
            "generation": generation,
            "turns": turns,
            "agent_metadata": agent_metadata,
        }))
    }

    async fn turn_get(&self, params: &Value, principal: &Principal) -> Result<Value> {
        reject_unknown_keys(params, &["agent", "generation", "turn_id"], "turn.get")?;
        let agent = required_text(params, "agent")?;
        let generation = required_i64(params, "generation")?;
        authorize_agent_read(principal, agent, Some(generation))?;
        let turn_id = required_text(params, "turn_id")?;
        let mut turn = {
            let store = self.inner.store.lock().await;
            store.get_turn(agent, generation, turn_id)?
        }
        .ok_or_else(|| Error::new("not_found", format!("turn {turn_id} was not found")))?;
        self.mask_unknown_turns(agent, generation, std::slice::from_mut(&mut turn))
            .await?;
        serde_json::to_value(turn)
            .map_err(|error| Error::new("internal", format!("encode turn: {error}")))
    }

    async fn events_read(&self, params: &Value, principal: &Principal) -> Result<Value> {
        reject_unknown_keys(params, &["after", "limit", "filter"], "events.read")?;
        let after = params.get("after").and_then(Value::as_i64).unwrap_or(0);
        let limit = page_limit(params)?;
        let filter = parse_filter(params.get("filter"), principal)?;
        let (events, next, high_water, truncated, _) = self
            .read_filtered(
                after,
                limit,
                None,
                EVENT_RESPONSE_ENTRIES_MAX_BYTES,
                &filter,
            )
            .await?;
        Ok(json!({
            "events": events,
            "next": next,
            "high_water": high_water,
            "truncated": truncated,
        }))
    }

    async fn events_subscribe(&self, params: &Value, principal: &Principal) -> Result<Value> {
        reject_unknown_keys(
            params,
            &["after", "limit", "wait_ms", "filter"],
            "events.subscribe",
        )?;
        let after = params.get("after").and_then(Value::as_i64).unwrap_or(0);
        if after < 0 {
            return Err(Error::new("invalid", "event cursor must be nonnegative"));
        }
        let limit = page_limit(params)?;
        let wait_ms = match params.get("wait_ms") {
            None => 0,
            Some(value) => value
                .as_u64()
                .ok_or_else(|| Error::new("invalid", "wait_ms must be a nonnegative integer"))?,
        }
        .min(LIVE_WAIT_MAX_MS);
        let filter = parse_filter(params.get("filter"), principal)?;

        // Subscribe before reading high-water so commits are either replayed or buffered.
        let mut receiver = self.inner.bus.subscribe();
        let (mut events, mut cursor, mut high_water, mut truncated, mut encoded_bytes) = self
            .read_filtered(
                after,
                limit,
                None,
                EVENT_RESPONSE_ENTRIES_MAX_BYTES,
                &filter,
            )
            .await?;
        let mut lagged = false;

        if wait_ms > 0 && events.len() < limit && !truncated {
            let deadline = Instant::now() + Duration::from_millis(wait_ms);
            while events.len() < limit && !truncated {
                match timeout_at(deadline, receiver.recv()).await {
                    Ok(Ok(event)) => {
                        if event.seq <= cursor {
                            continue;
                        }
                        let remaining = limit - events.len();
                        let remaining_bytes =
                            EVENT_RESPONSE_ENTRIES_MAX_BYTES.saturating_sub(encoded_bytes);
                        let replay = self
                            .read_filtered(
                                cursor,
                                remaining,
                                Some(event.seq),
                                remaining_bytes,
                                &filter,
                            )
                            .await?;
                        events.extend(replay.0);
                        cursor = replay.1;
                        high_water = high_water.max(event.seq).max(replay.2);
                        encoded_bytes = encoded_bytes.saturating_add(replay.4);
                        truncated |= replay.3;
                    }
                    Ok(Err(broadcast::error::RecvError::Lagged(_))) => {
                        lagged = true;
                        let remaining = limit - events.len();
                        let remaining_bytes =
                            EVENT_RESPONSE_ENTRIES_MAX_BYTES.saturating_sub(encoded_bytes);
                        let replay = self
                            .read_filtered(cursor, remaining, None, remaining_bytes, &filter)
                            .await?;
                        events.extend(replay.0);
                        cursor = replay.1;
                        high_water = high_water.max(replay.2);
                        truncated |= replay.3;
                        break;
                    }
                    Ok(Err(broadcast::error::RecvError::Closed)) => {
                        return Err(Error::new("unavailable", "event bus is closed"));
                    }
                    Err(_) => break,
                }
            }
        }
        Ok(json!({
            "events": events,
            "next": cursor,
            "high_water": high_water,
            "lagged": lagged,
            "truncated": truncated,
        }))
    }

    async fn read_filtered(
        &self,
        after: i64,
        limit: usize,
        through: Option<i64>,
        byte_budget: usize,
        filter: &EventFilter,
    ) -> Result<(Vec<Event>, i64, i64, bool, usize)> {
        if after < 0 || limit == 0 || limit > PAGE_MAX {
            return Err(Error::new("invalid", "invalid event query bounds"));
        }
        let byte_budget = byte_budget.min(EVENT_RESPONSE_ENTRIES_MAX_BYTES);
        let (events, cursor, high_water, encoded_bytes) = {
            let store = self.inner.store.lock().await;
            let high_water = match through {
                Some(bound) => bound,
                None => store.high_water()?,
            };
            if high_water < after {
                return Err(Error::new(
                    "invalid",
                    "event cursor is above the high-water mark",
                ));
            }
            let mut cursor = after;
            let mut scanned = 0;
            let mut events = Vec::new();
            let mut encoded_bytes = 0_usize;
            let mut byte_limited = false;
            while cursor < high_water && events.len() < limit && scanned < EVENT_SCAN_MAX {
                let page_limit = (EVENT_SCAN_MAX - scanned).clamp(1, 256);
                let page = store.read_events(cursor, page_limit, Some(high_water))?;
                if page.is_empty() {
                    cursor = high_water;
                    break;
                }
                for event in &page {
                    scanned += 1;
                    if filter.matches(event) {
                        let cost = encoded_event_cost(event)?;
                        if encoded_bytes.saturating_add(cost) > byte_budget {
                            byte_limited = true;
                            break;
                        }
                        encoded_bytes += cost;
                        events.push(event.clone());
                    }
                    cursor = event.seq;
                    if events.len() == limit {
                        break;
                    }
                }
                if byte_limited || events.len() == limit {
                    break;
                }
            }
            (events, cursor, high_water, encoded_bytes)
        };
        let truncated = cursor < high_water;
        Ok((events, cursor, high_water, truncated, encoded_bytes))
    }

    async fn status(&self) -> Result<Value> {
        let blockers = self.status_blockers().await?;
        let mode = self.mode_name();
        let (agent_count, write_faulted, unsettled_commands) = {
            let store = self.inner.store.lock().await;
            (
                store.agent_count()?,
                store.is_write_faulted(),
                store.list_unsettled_commands(STATUS_BLOCKER_MAX)?,
            )
        };
        let mut unknown_commands = self.unknown_ids();
        let unknown_commands_count = unknown_commands.len();
        let unknown_commands_truncated = unknown_commands.len() > STATUS_BLOCKER_MAX;
        unknown_commands.truncate(STATUS_BLOCKER_MAX);
        let unsettled_commands_truncated = unsettled_commands.len() == STATUS_BLOCKER_MAX;
        let unsettled_command_details = unsettled_commands
            .iter()
            .map(|command| {
                json!({
                    "id": command.id,
                    "agent": command.target,
                    "generation": command.generation,
                    "verb": command.verb,
                    "state": command.state,
                })
            })
            .collect::<Vec<_>>();
        Ok(json!({
            "state": if mode == "open" { "running" } else { mode },
            "admission": mode,
            "agents": agent_count,
            "store_write_faulted": write_faulted,
            "owned_plugins": self.inner.plugin_host.live_count(),
            "observer_readers": self.inner.plugin_host.observer_count(),
            "discovery_readers": self.inner.plugin_host.reader_count(),
            "workspace_custody": crate::workspaces::custody_capability(),
            "workspace_recovery_error": self.inner.workspace_recovery_error.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner).clone(),
            "unknown_commands_count": unknown_commands_count,
            "unknown_commands": unknown_commands,
            "unknown_commands_truncated": unknown_commands_truncated,
            "unsettled_commands_count": unsettled_commands.len(),
            "unsettled_commands": unsettled_command_details,
            "unsettled_commands_truncated": unsettled_commands_truncated,
            "blockers": blockers,
        }))
    }

    async fn status_blockers(&self) -> Result<Vec<Value>> {
        let (agents, active_observers, unsettled_commands, write_faulted) = {
            let store = self.inner.store.lock().await;
            let agents = store.list_nonterminal_agents(STATUS_BLOCKER_MAX)?;
            let active_observers = agents
                .iter()
                .map(|agent| {
                    if agent.observe_only {
                        store.observer_active(&agent.id, agent.generation)
                    } else {
                        Ok(false)
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            let unsettled_commands = store.list_unsettled_commands(STATUS_BLOCKER_MAX)?;
            (
                agents,
                active_observers,
                unsettled_commands,
                store.is_write_faulted(),
            )
        };
        let mut blockers = Vec::new();
        if self.inner.workspaces.has_owned_processes() {
            blockers.push(json!({"kind": "workspace_processes"}));
        }
        let discovery_readers = self.inner.plugin_host.reader_count();
        if discovery_readers > 0 {
            blockers.push(json!({"kind": "discovery_readers", "count": discovery_readers}));
        }
        for (agent, source_active) in agents.into_iter().zip(active_observers) {
            let observer_present = self
                .inner
                .plugin_host
                .observes_generation(&agent.id, agent.generation);
            if agent.observe_only
                && agent.state == AgentState::Unknown
                && !source_active
                && !observer_present
            {
                continue;
            }
            blockers.push(json!({
                "kind": "agent_live",
                "agent": agent.id,
                "generation": agent.generation,
                "state": agent.state,
            }));
        }
        let mut unsettled_ids = HashSet::new();
        for command in unsettled_commands {
            unsettled_ids.insert(command.id.clone());
            blockers.push(json!({
                "kind": "command_unsettled",
                "command_id": command.id,
                "state": command.state,
            }));
        }
        if write_faulted {
            blockers.push(json!({"kind": "store_write_faulted"}));
        }
        let unknown_ids = self.unknown_ids();
        for id in unknown_ids.iter().take(STATUS_BLOCKER_MAX) {
            if unsettled_ids.contains(id) {
                continue;
            }
            blockers.push(json!({"kind": "command_unknown", "command_id": id}));
        }
        if unknown_ids.len() > STATUS_BLOCKER_MAX {
            blockers.push(json!({
                "kind": "command_unknown_more",
                "count": unknown_ids.len() - STATUS_BLOCKER_MAX,
            }));
        }
        Ok(blockers)
    }

    async fn shutdown(&self, params: &Value, principal: &Principal) -> Result<Value> {
        require_owner(principal)?;
        reject_unknown_keys(params, &["id", "mode"], "daemon.shutdown")?;
        let mode = required_text(params, "mode")?;
        if mode != "drain" && mode != "force" {
            return Err(Error::new(
                "invalid",
                "daemon.shutdown mode must be drain or force",
            ));
        }
        let id = required_text(params, "id")?;
        let result = if mode == "force" {
            json!({"state": "dispatched", "mode": "force"})
        } else {
            let blockers = self.status_blockers().await?;
            json!({
                "state": if blockers.is_empty() { "applied" } else { "dispatched" },
                "mode": "drain",
                "blockers": blockers,
            })
        };
        let write = {
            let mut store = self.inner.store.lock().await;
            match store.record_write(
                id,
                &principal_name(principal),
                "daemon.shutdown",
                &json!({"mode": mode}),
                &result,
                now_ms(),
            ) {
                Ok(write) => Some(write.result),
                Err(_error) if mode == "force" && store.is_write_faulted() => None,
                Err(error) => return Err(error),
            }
        };
        let response = write.unwrap_or_else(|| {
            json!({
                "state": "dispatched",
                "mode": "force",
                "store_write_faulted": true,
            })
        });
        self.inner.plugin_host.close_discovery_admission();
        self.inner.mode.store(
            if mode == "force" {
                MODE_FORCE
            } else {
                MODE_DRAIN
            },
            Ordering::SeqCst,
        );
        self.inner.shutdown_ready.store(false, Ordering::SeqCst);
        self.signal_progress();
        Ok(response)
    }

    fn process_command(self, id: String, linked_id: Option<String>) -> BoxFuture<'static, ()> {
        Box::pin(self.process_command_inner(id, linked_id))
    }

    async fn process_command_inner(self, id: String, linked_id: Option<String>) {
        let mut receiver = self.inner.bus.subscribe();
        let command = {
            let store = self.inner.store.lock().await;
            match store.get_command(&id) {
                Ok(Some(command)) => command,
                _ => return,
            }
        };
        if matches!(command.verb.as_str(), "launch" | "resume")
            && let Err(error) = self.inner.workspaces.prepare(&id).await
        {
            self.settle_plugin_error(&id, &error).await;
            self.signal_progress();
            return;
        }
        let resume_source_state = match self.capture_resume_source_state(&id).await {
            Ok(source_state) => source_state,
            Err(error) => {
                self.reject_undispatched(&id, &error.code).await;
                return;
            }
        };
        let dispatched = loop {
            let change = {
                let mut store = self.inner.store.lock().await;
                let result = match store.dispatch(&id, now_ms()) {
                    Ok(change) => {
                        self.publish_locked(&change.events);
                        Ok(change)
                    }
                    Err(error) => Err(error),
                };
                (result, store.is_write_faulted())
            };
            match change.0 {
                Ok(change) if change.value.state == CommandState::Queued => {
                    let target = change.value.target.clone();
                    let generation = change.value.generation;
                    loop {
                        match receiver.recv().await {
                            Ok(event)
                                if event.agent == target
                                    && event.generation == generation
                                    && (event.kind.starts_with("turn.")
                                        || event.kind.starts_with("session.")
                                        || event.kind.starts_with("request.")) =>
                            {
                                break;
                            }
                            Ok(_) => continue,
                            Err(broadcast::error::RecvError::Lagged(_)) => break,
                            Err(broadcast::error::RecvError::Closed) => {
                                self.reject_undispatched(&id, "event bus closed").await;
                                return;
                            }
                        }
                    }
                }
                Ok(change) => break change.value,
                Err(error) => {
                    if change.1 {
                        self.note_unknown_id(&id, "store_dispatch_outcome_unknown");
                    } else if matches!(
                        error.code.as_str(),
                        "bad_state" | "stale_generation" | "not_found"
                    ) {
                        self.reject_undispatched(&id, &error.code).await;
                    }
                    self.signal_progress();
                    return;
                }
            }
        };

        let target = match dispatched.target.as_deref() {
            Some(target) => target.to_owned(),
            None => return,
        };
        let agent = {
            let store = self.inner.store.lock().await;
            match store.get_agent(&target) {
                Ok(Some(agent)) => agent,
                _ => return,
            }
        };

        let _result = match dispatched.verb.as_str() {
            "attach" => {
                match self
                    .inner
                    .plugin_host
                    .attach_observer(agent.clone(), dispatched.clone(), None)
                    .await
                {
                    Ok(mut value) => {
                        self.redact_native_value(&mut value);
                        match validate_observer_command_result(
                            value,
                            &agent,
                            &dispatched,
                            "attached",
                        ) {
                            Ok(snapshot) => {
                                let _ = self.settle_observe_attach(&id, snapshot.clone()).await;
                                Ok(snapshot)
                            }
                            Err(error) => {
                                self.settle_plugin_error(&id, &error).await;
                                Err(error)
                            }
                        }
                    }
                    Err(error) => {
                        self.settle_plugin_error(&id, &error).await;
                        Err(error)
                    }
                }
            }
            "worktree.remove" => match self.inner.workspaces.remove(&dispatched).await {
                Ok(value) => Ok(value),
                Err(error) => {
                    self.settle_plugin_error(&id, &error).await;
                    Err(error)
                }
            },
            "launch" | "resume" => {
                let start = self
                    .inner
                    .plugin_host
                    .start(agent, dispatched.clone(), resume_source_state)
                    .await;
                match start {
                    Ok(value) => {
                        let _ = self
                            .settle(&id, CommandState::Applied, Some(json!({"state": "ready"})))
                            .await;
                        if let Some(linked_id) = linked_id {
                            tokio::spawn(self.clone().process_command(linked_id, None));
                        }
                        Ok(value)
                    }
                    Err(error) => {
                        self.settle_plugin_error(&id, &error).await;
                        Err(error)
                    }
                }
            }
            "send" => {
                let response = self
                    .inner
                    .plugin_host
                    .command(&agent, dispatched.clone())
                    .await;
                match response {
                    Ok(value) => {
                        let response_state = value.get("state").and_then(Value::as_str);
                        if let Some(state) = match response_state {
                            Some("rejected") => Some(CommandState::Rejected),
                            Some("failed") => Some(CommandState::Failed),
                            Some("unknown") => Some(CommandState::Unknown),
                            _ => None,
                        } {
                            let mut value = value;
                            if state == CommandState::Unknown
                                && let Some(cause) = self
                                    .inner
                                    .plugin_host
                                    .take_late_batch_cause(&agent.id, agent.generation)
                            {
                                value["cause"] = Value::String(cause);
                            }
                            self.redact_native_value(&mut value);
                            let _ = self.settle(&id, state, Some(value.clone())).await;
                            return;
                        }
                        if response_state != Some("applied") {
                            let error = Error::new(
                                "unknown",
                                "plugin send response omitted its command outcome",
                            );
                            self.settle_plugin_error(&id, &error).await;
                            return;
                        }
                        let expected_turn = value
                            .get("turn_id")
                            .and_then(Value::as_str)
                            .unwrap_or_default();
                        let current = {
                            let store = self.inner.store.lock().await;
                            store.get_command(&id)
                        };
                        match current {
                            Ok(Some(command))
                                if command.state == CommandState::Applied
                                    && command.turn_id.as_deref() == Some(expected_turn) =>
                            {
                                Ok(value)
                            }
                            Ok(Some(command)) if command.state == CommandState::Unknown => {
                                Err(Error::new(
                                    "unknown",
                                    "plugin event batch did not establish a committed terminal turn",
                                ))
                            }
                            Ok(Some(_)) => {
                                let error = Error::new(
                                    "unknown",
                                    "plugin returned without committed turn association",
                                );
                                self.settle_plugin_error(&id, &error).await;
                                Err(error)
                            }
                            Ok(None) => Err(Error::new("not_found", "command disappeared")),
                            Err(error) => Err(error),
                        }
                    }
                    Err(error) => {
                        self.settle_plugin_error(&id, &error).await;
                        Err(error)
                    }
                }
            }
            "steer" | "interrupt" | "approve" | "deny" | "answer" => {
                match self
                    .inner
                    .plugin_host
                    .command(&agent, dispatched.clone())
                    .await
                {
                    Ok(mut value) => {
                        let state = match value.get("state").and_then(Value::as_str) {
                            Some("applied") => CommandState::Applied,
                            Some("rejected") => CommandState::Rejected,
                            Some("failed") => CommandState::Failed,
                            Some("unknown") => CommandState::Unknown,
                            _ => {
                                let error = Error::new(
                                    "unknown",
                                    "plugin command response omitted its outcome",
                                );
                                self.settle_plugin_error(&id, &error).await;
                                return;
                            }
                        };
                        if state == CommandState::Unknown
                            && let Some(cause) = self
                                .inner
                                .plugin_host
                                .take_late_batch_cause(&agent.id, agent.generation)
                        {
                            value["cause"] = Value::String(cause);
                        }
                        self.redact_native_value(&mut value);
                        let _ = self.settle(&id, state, Some(value.clone())).await;
                        Ok(value)
                    }
                    Err(error) => {
                        self.settle_plugin_error(&id, &error).await;
                        Err(error)
                    }
                }
            }
            "stop" => {
                // Store dispatch atomically latches a preparation cancel before
                // the manager signals its custody. Otherwise this is native stop.
                match self.inner.workspaces.cancel(&id).await {
                    Ok(true) => {
                        self.signal_progress();
                        return;
                    }
                    Ok(false) => {}
                    Err(error) => {
                        self.settle_plugin_error(&id, &error).await;
                        self.signal_progress();
                        return;
                    }
                }
                let force = dispatched
                    .args
                    .get("force")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                match self
                    .inner
                    .plugin_host
                    .stop(&agent, dispatched.clone(), force)
                    .await
                {
                    Ok(value) => {
                        let mut summary = json!({
                            "state": "applied",
                            "custody": value.get("custody").cloned().unwrap_or(Value::Null),
                            "graceful": value.get("graceful").cloned().unwrap_or(Value::Bool(false)),
                        });
                        if let Some(diagnostics) = value.get("diagnostics") {
                            summary["diagnostics"] = diagnostics.clone();
                        }
                        self.redact_native_value(&mut summary);
                        let _ = self.settle(&id, CommandState::Applied, Some(summary)).await;
                        Ok(value)
                    }
                    Err(error) => {
                        self.settle_plugin_error(&id, &error).await;
                        Err(error)
                    }
                }
            }
            "detach" => {
                match self
                    .inner
                    .plugin_host
                    .detach_observer(&agent, dispatched.clone())
                    .await
                {
                    Ok(value) => match validate_observer_command_result(
                        value,
                        &agent,
                        &dispatched,
                        "detached",
                    ) {
                        Ok(outcome) => {
                            let _ = self.settle_observe_detach(&id, outcome.clone()).await;
                            Ok(outcome)
                        }
                        Err(error) => {
                            self.settle_plugin_error(&id, &error).await;
                            Err(error)
                        }
                    },
                    Err(error) => {
                        self.settle_plugin_error(&id, &error).await;
                        Err(error)
                    }
                }
            }
            _ => Err(Error::new("unsupported", "command verb is not implemented")),
        };
        self.signal_progress();
    }

    async fn capture_resume_source_state(&self, command_id: &str) -> Result<Option<Value>> {
        let (generation, source, latest_turn) = {
            let store = self.inner.store.lock().await;
            let command = store
                .get_command(command_id)?
                .ok_or_else(|| Error::new("not_found", "resume command was not found"))?;
            if command.verb != "resume" {
                return Ok(None);
            }
            if command.state != CommandState::Accepted {
                return Err(Error::new(
                    "bad_state",
                    "resume source state must be captured before dispatch",
                ));
            }
            let agent_id = command
                .target
                .ok_or_else(|| Error::new("not_found", "resume target Agent is missing"))?;
            let generation = command.expected_generation.ok_or_else(|| {
                Error::new(
                    "stale_generation",
                    "resume did not capture its old generation",
                )
            })?;
            if command.generation != Some(generation) {
                return Err(Error::new(
                    "stale_generation",
                    "resume command generation does not match its captured generation",
                ));
            }
            let source =
                store.read_source_state(&agent_id, generation, CONTROLLED_PLUGIN_SOURCE)?;
            let latest_turn = store
                .list_turns(&agent_id, generation, 1)?
                .into_iter()
                .next()
                .map(|turn| {
                    json!({
                        "turn_id": turn.turn_id,
                        "state": turn.state,
                        "status": turn.status,
                        "started_at": turn.started_at,
                        "completed_at": turn.completed_at,
                    })
                });
            (generation, source, latest_turn)
        };
        let mut source_state = json!({
            "retained_generation": generation,
            "retained_source": source,
            "current_turn": latest_turn,
        });
        self.redact_native_value(&mut source_state);
        Ok(Some(source_state))
    }

    async fn reject_undispatched(&self, id: &str, reason: &str) {
        let outcome = json!({"reason": reason});
        let _ = self.settle(id, CommandState::Rejected, Some(outcome)).await;
    }

    async fn settle_observe_attach(&self, id: &str, snapshot: Value) -> Option<Command> {
        let (result, write_faulted) = {
            let mut store = self.inner.store.lock().await;
            let result = store.settle_observe_attach(id, snapshot, now_ms());
            if let Ok(change) = &result {
                self.publish_locked(&change.events);
            }
            (result, store.is_write_faulted())
        };
        match result {
            Ok(change) => {
                self.note_unknown(&change.events);
                let command = change.value;
                self.signal_progress();
                Some(command)
            }
            Err(_) => {
                let cause = if write_faulted {
                    "observe_attach_store_faulted"
                } else {
                    "observe_attach_settlement_failed"
                };
                self.note_unknown_id(id, cause);
                self.signal_progress();
                None
            }
        }
    }

    async fn settle_observe_detach(&self, id: &str, outcome: Value) -> Option<Command> {
        let (result, write_faulted) = {
            let mut store = self.inner.store.lock().await;
            let result = store.settle_observe_detach(id, outcome, now_ms());
            if let Ok(change) = &result {
                self.publish_locked(&change.events);
            }
            (result, store.is_write_faulted())
        };
        match result {
            Ok(change) => {
                self.note_unknown(&change.events);
                let command = change.value;
                self.signal_progress();
                Some(command)
            }
            Err(_) => {
                let cause = if write_faulted {
                    "observe_detach_store_faulted"
                } else {
                    "observe_detach_settlement_failed"
                };
                self.note_unknown_id(id, cause);
                self.signal_progress();
                None
            }
        }
    }

    fn redact_native_value(&self, value: &mut Value) {
        let secrets = self
            .inner
            .secrets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        crate::redaction::redact_outcome(value, &secrets);
    }

    async fn settle_plugin_error(&self, id: &str, error: &Error) {
        let current = {
            let store = self.inner.store.lock().await;
            store.get_command(id)
        };
        let (current_state, command_target, previous_outcome) = match current {
            Ok(Some(command)) => (
                Some(command.state),
                command.target.zip(command.generation),
                command.outcome,
            ),
            _ => (None, None, None),
        };
        let late_batch_cause = command_target.as_ref().and_then(|(agent, generation)| {
            self.inner
                .plugin_host
                .take_late_batch_cause(agent, *generation)
        });
        let secrets = self
            .inner
            .secrets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let error = crate::redaction::redact_error(error.clone(), &secrets);
        crate::logging::record(
            &SecretPolicy::new(&secrets),
            &error.code,
            &[("command", id)],
            Some(&error),
        );
        if current_state.is_some_and(|state| state.is_terminal())
            || current_state == Some(CommandState::Unknown) && error.diagnostics.is_empty()
        {
            return;
        }
        let state = if current_state == Some(CommandState::Unknown) {
            CommandState::Unknown
        } else {
            classify_dispatched_error(&error.code)
        };
        let cause = if state == CommandState::Unknown {
            late_batch_cause.unwrap_or_else(|| unknown_cause(&error))
        } else {
            String::new()
        };
        let mut outcome = if current_state == Some(CommandState::Unknown) {
            previous_outcome.unwrap_or_else(|| json!({}))
        } else if state == CommandState::Unknown {
            json!({"code": error.code, "cause": cause})
        } else {
            json!({"code": error.code})
        };
        for diagnostic in &error.diagnostics {
            crate::redaction::attach_stderr(
                &mut outcome,
                diagnostic.source,
                diagnostic.snapshot.clone(),
                &secrets,
            );
        }
        self.redact_native_value(&mut outcome);
        let _ = self.settle(id, state, Some(outcome)).await;
    }

    async fn settle(
        &self,
        id: &str,
        state: CommandState,
        mut outcome: Option<Value>,
    ) -> Option<Command> {
        if let Some(outcome) = outcome.as_mut() {
            let envelope = json!({"command_id": id, "state": state, "outcome": null});
            let overhead = serde_json::to_vec(&envelope)
                .map_or(usize::MAX, |bytes| bytes.len().saturating_sub(4));
            crate::redaction::bound_outcome_diagnostics(
                outcome,
                MAX_EVENT_BYTES.saturating_sub(overhead),
            );
        }
        let persistence_cause = outcome
            .as_ref()
            .and_then(|value| value.get("cause"))
            .and_then(Value::as_str)
            .unwrap_or("store_settlement_failed")
            .to_owned();
        let (result, write_faulted) = {
            let mut store = self.inner.store.lock().await;
            // A source batch may have settled this command after the error
            // handler's initial read. Retain that outcome under the writer lock.
            let result = match store.get_command(id) {
                Ok(Some(command)) if command.state.is_terminal() => Ok(eliot_store::Change {
                    value: command,
                    events: Vec::new(),
                }),
                Ok(_) => store.settle(id, state, outcome, now_ms()),
                Err(error) => Err(error),
            };
            if let Ok(change) = &result {
                self.publish_locked(&change.events);
            }
            (result, store.is_write_faulted())
        };
        match result {
            Ok(change) => {
                self.note_unknown(&change.events);
                if change.value.state.is_terminal()
                    && let (Some(agent), Some(generation)) =
                        (change.value.target.as_deref(), change.value.generation)
                {
                    self.forget_token_if_revoked(agent, generation).await;
                }
                let command = change.value;
                self.signal_progress();
                Some(command)
            }
            Err(_) => {
                if write_faulted {
                    self.note_unknown_id(id, &persistence_cause);
                }
                self.signal_progress();
                None
            }
        }
    }

    async fn forget_token_if_revoked(&self, agent: &str, generation: i64) {
        let token = {
            let store = self.inner.store.lock().await;
            store.get_agent_token(agent, generation)
        };
        if matches!(token, Ok(None)) {
            crate::plugin_host::forget_revoked_agent_token(
                &self.inner.agent_credentials,
                &self.inner.secrets,
                &(agent.to_owned(), generation),
            );
        }
    }

    fn publish_locked(&self, events: &[Event]) {
        self.inner.bus.publish(events);
        self.note_unknown(events);
    }

    fn note_unknown(&self, events: &[Event]) {
        note_unknown(&self.inner.unknown_commands, events);
    }

    fn note_unknown_id(&self, id: &str, cause: &str) {
        self.inner
            .unknown_commands
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(id.to_owned());
        self.inner
            .unknown_causes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(id.to_owned(), cause.to_owned());
    }

    fn unknown_cause(&self, id: &str) -> Option<String> {
        self.inner
            .unknown_causes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(id)
            .cloned()
    }

    async fn mask_unknown_turns(
        &self,
        agent: &str,
        generation: i64,
        turns: &mut [Turn],
    ) -> Result<()> {
        let memory_unknown = self.unknown_ids().into_iter().collect::<HashSet<String>>();
        let (associated, maybe_more) = {
            let store = self.inner.store.lock().await;
            let commands = store.list_unsettled_commands(STATUS_BLOCKER_MAX)?;
            let maybe_more = commands.len() == STATUS_BLOCKER_MAX;
            let associated = commands
                .into_iter()
                .filter(|command| {
                    command.state == CommandState::Unknown || memory_unknown.contains(&command.id)
                })
                .filter(|command| {
                    command.target.as_deref() == Some(agent)
                        && command.generation == Some(generation)
                })
                .filter_map(|command| command.turn_id)
                .collect::<HashSet<_>>();
            (associated, maybe_more)
        };
        let found_exact_turn = turns.iter().any(|turn| associated.contains(&turn.turn_id));
        if maybe_more && !found_exact_turn {
            return Err(Error::new(
                "unavailable",
                "unsettled-command view is truncated; turn completion cannot be confirmed",
            ));
        }
        for turn in turns {
            if associated.contains(&turn.turn_id) {
                turn.state = TurnState::Unknown;
                turn.status = None;
            }
        }
        Ok(())
    }

    fn unknown_ids(&self) -> Vec<String> {
        let mut ids: Vec<_> = self
            .inner
            .unknown_commands
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .cloned()
            .collect();
        ids.sort();
        ids
    }

    fn mode_name(&self) -> &'static str {
        match self.inner.mode.load(Ordering::SeqCst) {
            MODE_DRAIN => "draining",
            MODE_FORCE => "forcing",
            _ => "open",
        }
    }
}

impl EventFilter {
    fn matches(&self, event: &Event) -> bool {
        (self.agents.is_empty()
            || event
                .agent
                .as_ref()
                .is_some_and(|agent| self.agents.contains(agent)))
            && (self.kinds.is_empty() || self.kinds.contains(&event.kind))
            && (self.sources.is_empty() || self.sources.contains(&event.source))
    }
}

fn normalize_attach(request: &SubmitCommand) -> Result<()> {
    if request.target.is_some() {
        return Err(Error::new(
            "invalid",
            "attach does not take an Agent target",
        ));
    }
    if request.request_id.is_some() || request.expected_turn_id.is_some() {
        return Err(Error::new(
            "invalid",
            "observe attach does not take a native request or turn binding",
        ));
    }
    reject_unknown_keys(
        &request.args,
        &[
            "mode",
            "profile",
            "harness",
            "native_scope",
            "native_ref",
            "project",
        ],
        "attach args",
    )?;
    if required_text(&request.args, "mode")? != "observe" {
        return Err(Error::new(
            "unsupported",
            "attach supports observe mode only",
        ));
    }
    for field in ["profile", "harness", "native_scope", "native_ref"] {
        let value = required_text(&request.args, field)?;
        if value.chars().any(char::is_control) {
            return Err(Error::new(
                "invalid",
                format!("attach {field} contains a control character"),
            ));
        }
    }
    let _ = optional_text(&request.args, "project")?;
    Ok(())
}

fn freeze_attach_profile(data_root: &std::path::Path, request: &mut SubmitCommand) -> Result<()> {
    let profile_name = required_text(&request.args, "profile")?;
    let harness = required_text(&request.args, "harness")?.to_owned();
    let snapshot = crate::config::resolve_profile(data_root, profile_name)?;
    if snapshot.get("harness").and_then(Value::as_str) != Some(harness.as_str()) {
        return Err(Error::new(
            "invalid",
            "attach harness does not match the selected profile",
        ));
    }
    install_frozen_profile(request, &harness, snapshot, true)
}

fn validate_observer_command_result(
    mut value: Value,
    agent: &Agent,
    command: &Command,
    expected_state: &str,
) -> Result<Value> {
    let result: ObserverCommandResult = serde_json::from_value(value.clone()).map_err(|_| {
        Error::new(
            "source_integrity",
            "plugin returned an invalid observer command result",
        )
    })?;
    let native_scope = agent
        .native_scope
        .as_deref()
        .ok_or_else(|| Error::new("source_integrity", "observer Agent has no native scope"))?;
    let native_ref = agent
        .native_ref
        .as_deref()
        .ok_or_else(|| Error::new("source_integrity", "observer Agent has no native reference"))?;
    if result.agent_id != agent.id
        || result.generation != agent.generation
        || result.command_id != command.id
        || command.target.as_deref() != Some(agent.id.as_str())
        || command.generation != Some(agent.generation)
        || result.native_scope != native_scope
        || result.native_ref != native_ref
        || result.state != expected_state
        || result.observation_state != "unknown"
        || result.reader_closed == Some(false)
    {
        return Err(Error::new(
            "source_integrity",
            "observer result does not match its committed subject, command, or state",
        ));
    }
    if expected_state == "attached"
        && (!result.metadata.as_ref().is_some_and(Value::is_object)
            || !result.coverage.as_ref().is_some_and(Value::is_object))
    {
        return Err(Error::new(
            "source_integrity",
            "observer attach omitted bounded metadata or coverage",
        ));
    }
    if result
        .metadata
        .as_ref()
        .is_some_and(|metadata| !metadata.is_object())
        || result
            .coverage
            .as_ref()
            .is_some_and(|coverage| !coverage.is_object())
    {
        return Err(Error::new(
            "source_integrity",
            "observer metadata and coverage must be objects",
        ));
    }
    let encoded = serde_json::to_vec(&value)
        .map_err(|_| Error::new("internal", "observer result could not be encoded"))?;
    if encoded.len() > MAX_EVENT_BYTES {
        return Err(Error::new(
            "frame_too_large",
            "observer command result exceeds its byte bound",
        ));
    }
    if let Some(object) = value.as_object_mut() {
        // Reader custody is an implementation detail; the Store result wire is
        // the exact subject/command binding plus observation metadata.
        object.remove("reader_closed");
    }
    Ok(value)
}

fn encoded_event_cost(event: &Event) -> Result<usize> {
    let encoded = serde_json::to_vec(event)
        .map_err(|error| Error::new("internal", format!("encode event page entry: {error}")))?;
    Ok(encoded.len().saturating_add(1))
}

fn normalize_launch(request: &mut SubmitCommand) -> Result<()> {
    if request.verb != "launch" {
        return Ok(());
    }
    reject_launch_derived_keys(&request.args)?;
    if let Some(worktree) = request.args.get("worktree") {
        if worktree
            .as_str()
            .is_none_or(|branch| branch.trim().is_empty())
        {
            return Err(Error::new("invalid", "worktree must name a new branch"));
        }
    } else if request.args.get("base").is_some() {
        return Err(Error::new("invalid", "base requires worktree"));
    }
    if request
        .args
        .get("base")
        .is_some_and(|base| base.as_str().is_none_or(|base| base.trim().is_empty()))
    {
        return Err(Error::new("invalid", "base must be a nonempty local ref"));
    }
    let workspace = request
        .args
        .get("workspace")
        .and_then(Value::as_str)
        .filter(|workspace| !workspace.trim().is_empty())
        .ok_or_else(|| Error::new("invalid", "launch requires a workspace path"))?;
    let workspace = std::fs::canonicalize(workspace)
        .map_err(|_| Error::new("invalid", "launch workspace must be an existing directory"))?;
    if !workspace.is_dir() {
        return Err(Error::new(
            "invalid",
            "launch workspace must be a directory",
        ));
    }
    let workspace = workspace
        .to_str()
        .ok_or_else(|| Error::new("invalid", "workspace path is not valid UTF-8"))?
        .to_owned();
    let args = request
        .args
        .as_object_mut()
        .ok_or_else(|| Error::new("invalid", "launch args must be an object"))?;
    if !args
        .get("profile")
        .and_then(Value::as_str)
        .is_some_and(|profile| !profile.trim().is_empty())
    {
        return Err(Error::new("invalid", "launch requires a profile name"));
    }
    args.insert("workspace".to_owned(), Value::String(workspace.clone()));
    args.insert("project".to_owned(), Value::String(workspace));
    Ok(())
}

fn normalize_resume(request: &mut SubmitCommand) -> Result<()> {
    let args = request
        .args
        .as_object()
        .ok_or_else(|| Error::new("invalid", "resume args must be an object"))?;
    for reserved in [
        "profile",
        "harness",
        "profile_snapshot",
        "workspace",
        "project",
        "source_plan",
        "target_environment",
        "resume_from",
        "native_scope",
        "native_ref",
        "prompt",
        "worktree",
        "base",
        "workspace_preparation",
        "repo_common_dir",
        "base_oid",
        "worktree_path",
        "frozen_setup",
        "setup",
        "target_generation",
    ] {
        if args.contains_key(reserved) {
            return Err(Error::new(
                "invalid",
                format!("resume field {reserved} is retained by the Agent or Store"),
            ));
        }
    }
    Ok(())
}

fn validate_plugin_command_args(request: &SubmitCommand) -> Result<()> {
    let args = request
        .args
        .as_object()
        .ok_or_else(|| Error::new("invalid", "command args must be an object"))?;
    for reserved in ["expected_turn_id", "turn_id", "request_id"] {
        if args.contains_key(reserved) {
            return Err(Error::new(
                "invalid",
                format!("command field {reserved} is captured by the daemon or Store"),
            ));
        }
    }
    Ok(())
}

fn validate_answer_request(store: &eliot_store::Store, request: &SubmitCommand) -> Result<()> {
    if store.get_command(&request.id)?.is_some() {
        // Store enforces the retained principal/payload binding on this retry.
        return Ok(());
    }
    let Some(target) = request.target.as_deref() else {
        return Ok(());
    };
    let agent = match store.get_agent(target) {
        Ok(Some(agent)) => agent,
        Ok(None) => return Ok(()),
        Err(error) if matches!(error.code.as_str(), "invalid" | "not_found") => return Ok(()),
        Err(_) => {
            return Err(Error::new(
                "unavailable",
                "pending request details are unavailable before answer admission",
            ));
        }
    };
    if request
        .expected_generation
        .is_some_and(|expected| expected != agent.generation)
    {
        return Ok(());
    }
    let pending = match request.request_id.as_deref() {
        Some(request_id) => agent
            .pending
            .iter()
            .find(|pending| pending.id == request_id)
            .cloned(),
        None if agent.pending.len() == 1 => agent.pending.first().cloned(),
        None => None,
    };
    let Some(pending) = pending else {
        return Ok(());
    };
    if request
        .expected_turn_id
        .as_deref()
        .is_some_and(|turn_id| turn_id != pending.turn_id)
    {
        return Ok(());
    }
    if contains_secret_question(&pending.details) {
        return Err(Error::new(
            "unsupported",
            "secret native request answers are not supported",
        ));
    }
    Ok(())
}

struct RetainedLaunchProfile {
    harness: String,
    snapshot: Value,
    command_has_harness: bool,
    command_has_snapshot: bool,
    workspace: Option<Value>,
    project: Option<Value>,
    preparation: Option<Value>,
}

fn install_retained_attach_profile(
    store: &eliot_store::Store,
    request: &mut SubmitCommand,
) -> Result<bool> {
    let Some(command) = store.get_command(&request.id)? else {
        return Ok(false);
    };
    if command.verb != "attach" {
        return Err(Error::new(
            "id_conflict",
            "mutation id is already bound to another command",
        ));
    }
    let snapshot = command
        .args
        .get("profile_snapshot")
        .filter(|snapshot| snapshot.is_object())
        .cloned();
    let snapshot = match snapshot {
        Some(snapshot) => snapshot,
        None => command
            .target
            .as_deref()
            .map(|target| store.get_agent(target))
            .transpose()?
            .flatten()
            .map(|agent| agent.profile_snapshot)
            .filter(Value::is_object)
            .ok_or_else(|| {
                Error::new("bad_state", "retained observe attach has no frozen profile")
            })?,
    };
    request
        .args
        .as_object_mut()
        .ok_or_else(|| Error::new("invalid", "attach args must be an object"))?
        .insert("profile_snapshot".to_owned(), snapshot);
    Ok(true)
}

fn retained_launch_profile(
    store: &eliot_store::Store,
    request: &SubmitCommand,
) -> Result<Option<RetainedLaunchProfile>> {
    let Some(command) = store.get_command(&request.id)? else {
        return Ok(None);
    };
    if command.verb != "launch" {
        return Err(Error::new(
            "id_conflict",
            "mutation id is bound to another command",
        ));
    }

    let command_has_harness = command.args.get("harness").is_some();
    let command_has_snapshot = command.args.get("profile_snapshot").is_some();
    let workspace = command.args.get("workspace").cloned();
    let project = command.args.get("project").cloned();
    let preparation = command.args.get("workspace_preparation").cloned();
    let stored_snapshot = command
        .args
        .get("profile_snapshot")
        .filter(|snapshot| snapshot.is_object())
        .cloned();
    let stored_harness = command
        .args
        .get("harness")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    if let (Some(harness), Some(snapshot)) = (stored_harness, stored_snapshot) {
        return Ok(Some(RetainedLaunchProfile {
            harness,
            snapshot,
            command_has_harness,
            command_has_snapshot,
            workspace,
            project,
            preparation,
        }));
    }

    // Older L1 admissions did not keep the resolved route in command.args. Their
    // Agent row is already the retained source of truth, so reuse it without a
    // config read and preserve the original command payload for Store id binding.
    if let Some(target) = command.target.as_deref()
        && let Some(agent) = store.get_agent(target)?
    {
        return Ok(Some(RetainedLaunchProfile {
            harness: agent.harness,
            snapshot: agent.profile_snapshot,
            command_has_harness,
            command_has_snapshot,
            workspace,
            project,
            preparation,
        }));
    }

    Err(Error::new(
        "bad_state",
        "retained launch has no frozen profile on its Agent row",
    ))
}

fn install_frozen_profile(
    request: &mut SubmitCommand,
    harness: &str,
    snapshot: Value,
    require_snapshot_harness: bool,
) -> Result<()> {
    if !matches!(harness, "fake" | "codex") {
        return Err(Error::new(
            "unsupported",
            format!("unsupported launch harness {harness}"),
        ));
    }
    let Some(snapshot_object) = snapshot.as_object() else {
        return Err(Error::new(
            "invalid",
            "resolved profile snapshot must be a JSON object",
        ));
    };
    match snapshot_object.get("harness").and_then(Value::as_str) {
        Some(snapshot_harness) if snapshot_harness != harness => {
            return Err(Error::new(
                "invalid",
                "resolved profile harness does not match its snapshot",
            ));
        }
        None if require_snapshot_harness => {
            return Err(Error::new(
                "invalid",
                "resolved profile snapshot must include its harness",
            ));
        }
        _ => {}
    }
    validate_pass_env_references(snapshot_object, require_snapshot_harness)?;
    let args = request
        .args
        .as_object_mut()
        .ok_or_else(|| Error::new("invalid", "launch args must be an object"))?;
    args.insert("harness".to_owned(), Value::String(harness.to_owned()));
    args.insert("profile_snapshot".to_owned(), snapshot);
    Ok(())
}

fn install_retained_profile(
    request: &mut SubmitCommand,
    retained: RetainedLaunchProfile,
) -> Result<()> {
    let RetainedLaunchProfile {
        harness,
        snapshot,
        command_has_harness,
        command_has_snapshot,
        workspace,
        project,
        preparation,
    } = retained;
    let same_workspace = match (
        request.args.get("workspace").and_then(Value::as_str),
        workspace.as_ref().and_then(Value::as_str),
    ) {
        (Some(requested), Some(stored)) => same_retained_workspace(requested, stored),
        _ => false,
    };
    // Match the exact payload shape that Store admitted. This keeps retries of
    // pre-snapshot L1 commands bound to their original caller arguments.
    let args = request
        .args
        .as_object_mut()
        .ok_or_else(|| Error::new("invalid", "launch args must be an object"))?;
    if command_has_harness {
        args.insert("harness".to_owned(), Value::String(harness));
    } else {
        args.remove("harness");
    }
    if command_has_snapshot {
        args.insert("profile_snapshot".to_owned(), snapshot);
    } else {
        args.remove("profile_snapshot");
    }
    if let Some(preparation) = preparation {
        args.insert("workspace_preparation".to_owned(), preparation);
    } else {
        args.remove("workspace_preparation");
    }
    // Normalize only the same caller workspace without touching the filesystem.
    // A changed path/branch/base/text stays in the payload for Store's id_conflict.
    if same_workspace {
        if let Some(workspace) = workspace {
            args.insert("workspace".to_owned(), workspace);
        }
        if let Some(project) = project {
            args.insert("project".to_owned(), project);
        }
    }
    Ok(())
}

fn retained_preparation(args: &Value) -> Result<Option<WorkspacePreparationInput>> {
    args.get("workspace_preparation")
        .map(|input| {
            serde_json::from_value(input.clone())
                .map_err(|_| Error::new("internal", "retained workspace input is invalid"))
        })
        .transpose()
}

fn same_retained_workspace(requested: &str, stored: &str) -> bool {
    if requested.trim().is_empty() || stored.is_empty() {
        return false;
    }
    let (Ok(requested), Ok(stored)) = (std::path::absolute(requested), std::path::absolute(stored))
    else {
        return false;
    };
    #[cfg(windows)]
    {
        let normalized = |path: &std::path::Path| {
            let path = path.to_string_lossy().replace('\\', "/");
            match path.strip_prefix("//?/UNC/") {
                Some(rest) => format!("//{rest}"),
                None => path.strip_prefix("//?/").unwrap_or(&path).to_owned(),
            }
            .trim_end_matches('/')
            .to_owned()
        };
        normalized(&requested).eq_ignore_ascii_case(&normalized(&stored))
    }
    #[cfg(not(windows))]
    {
        requested == stored
    }
}

fn reject_launch_derived_keys(args: &Value) -> Result<()> {
    let args = args
        .as_object()
        .ok_or_else(|| Error::new("invalid", "launch args must be an object"))?;
    for reserved in [
        "harness",
        "profile_snapshot",
        "source_plan",
        "target_environment",
        "workspace_preparation",
        "repo_common_dir",
        "base_oid",
        "worktree_path",
        "frozen_setup",
        "setup",
        "target_generation",
    ] {
        if args.contains_key(reserved) {
            return Err(Error::new(
                "invalid",
                format!("launch field {reserved} is resolved by the daemon"),
            ));
        }
    }
    Ok(())
}

fn validate_pass_env_references(
    snapshot: &serde_json::Map<String, Value>,
    required: bool,
) -> Result<()> {
    let Some(value) = snapshot.get("pass_env") else {
        if required {
            return Err(Error::new(
                "invalid",
                "resolved profile must store pass_env references",
            ));
        }
        return Ok(());
    };
    let names = value
        .as_array()
        .ok_or_else(|| Error::new("invalid", "profile pass_env must be an array of names"))?;
    for name in names {
        let name = name
            .as_str()
            .filter(|name| valid_environment_name(name))
            .ok_or_else(|| {
                Error::new(
                    "invalid",
                    "profile pass_env entries must be environment variable names",
                )
            })?;
        if name
            .get(..12)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("ELIOT_AGENT_"))
        {
            return Err(Error::new(
                "invalid",
                "profile pass_env cannot override Eliot Agent identity variables",
            ));
        }
    }
    Ok(())
}

fn valid_environment_name(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some('_' | 'A'..='Z' | 'a'..='z'))
        && chars.all(|character| character == '_' || character.is_ascii_alphanumeric())
}

fn is_native_control_verb(verb: &str) -> bool {
    matches!(
        verb,
        "send"
            | "steer"
            | "inject"
            | "interrupt"
            | "abort"
            | "approve"
            | "deny"
            | "answer"
            | "stop"
            | "resume"
            | "configure"
            | "compact"
            | "fork"
            | "background"
    )
}

fn authorize_command(principal: &Principal, request: &mut SubmitCommand) -> Result<()> {
    let Principal::Agent { id, generation } = principal else {
        return Ok(());
    };
    if !matches!(
        request.verb.as_str(),
        "send" | "stop" | "steer" | "interrupt" | "approve" | "deny" | "answer"
    ) {
        return Err(Error::new(
            "not_allowed",
            "managed Agent credentials cannot launch another Agent",
        ));
    }
    if request.target.as_deref().is_some_and(|target| target != id) {
        return Err(Error::new(
            "not_allowed",
            "managed Agent credentials are scoped to their own Agent",
        ));
    }
    request.target = Some(id.clone());
    if request
        .expected_generation
        .is_some_and(|expected| expected != *generation)
    {
        return Err(Error::new(
            "stale_generation",
            "command generation does not match the caller",
        ));
    }
    request.expected_generation = Some(*generation);
    Ok(())
}

fn authorize_command_read(principal: &Principal, command: &Command) -> Result<()> {
    if let Principal::Agent { id, generation } = principal
        && (command.target.as_deref() != Some(id.as_str())
            || command
                .generation
                .is_some_and(|actual| actual != *generation))
    {
        return Err(Error::new(
            "not_allowed",
            "command is outside the caller's scope",
        ));
    }
    Ok(())
}

fn authorize_agent_read(principal: &Principal, id: &str, generation: Option<i64>) -> Result<()> {
    if let Principal::Agent {
        id: own_id,
        generation: own_generation,
    } = principal
        && (id != own_id || generation.is_some_and(|actual| actual != *own_generation))
    {
        return Err(Error::new(
            "not_allowed",
            "Agent read is outside the caller's scope",
        ));
    }
    Ok(())
}

fn require_owner(principal: &Principal) -> Result<()> {
    if matches!(principal, Principal::Owner) {
        Ok(())
    } else {
        Err(Error::new(
            "not_allowed",
            "method requires the owner principal",
        ))
    }
}

fn principal_name(principal: &Principal) -> String {
    match principal {
        Principal::Owner => "owner".to_owned(),
        Principal::Agent { id, generation } => format!("agent:{id}:{generation}"),
    }
}

fn submit_result(command: &Command, linked: Option<&Command>) -> Value {
    json!({
        "command_id": command.id,
        "agent": command.target,
        "generation": command.generation,
        "state": command.state,
        "outcome": command.outcome,
        "linked_command": linked.map(|command| command.id.as_str()),
    })
}

fn parse_filter(raw: Option<&Value>, principal: &Principal) -> Result<EventFilter> {
    let mut filter = EventFilter {
        agents: HashSet::new(),
        kinds: HashSet::new(),
        sources: HashSet::new(),
    };
    if let Some(raw) = raw {
        let object = raw
            .as_object()
            .ok_or_else(|| Error::new("invalid", "event filter must be an object"))?;
        reject_unknown_map_keys(object, &["agents", "kinds", "sources"], "event filter")?;
        filter.agents = string_set(object.get("agents"), "agents")?;
        filter.kinds = string_set(object.get("kinds"), "kinds")?;
        filter.sources = string_set(object.get("sources"), "sources")?;
    }
    if let Principal::Agent { id, .. } = principal
        && !filter.agents.is_empty()
        && !filter.agents.contains(id)
    {
        return Err(Error::new(
            "not_allowed",
            "event filter is outside the caller's scope",
        ));
    }
    if let Principal::Agent { id, .. } = principal {
        filter.agents.clear();
        filter.agents.insert(id.clone());
    }
    Ok(filter)
}

fn string_set(value: Option<&Value>, field: &str) -> Result<HashSet<String>> {
    let Some(value) = value else {
        return Ok(HashSet::new());
    };
    let array = value
        .as_array()
        .ok_or_else(|| Error::new("invalid", format!("event filter {field} must be an array")))?;
    let mut result = HashSet::new();
    if array.len() > PAGE_MAX {
        return Err(Error::new(
            "frame_too_large",
            format!("event filter {field} has too many values"),
        ));
    }
    for item in array {
        let text = item
            .as_str()
            .filter(|text| !text.is_empty())
            .ok_or_else(|| {
                Error::new(
                    "invalid",
                    format!("event filter {field} values must be non-empty strings"),
                )
            })?;
        result.insert(text.to_owned());
    }
    Ok(result)
}

fn contains_secret_question(value: &Value) -> bool {
    match value {
        Value::Array(items) => items.iter().any(contains_secret_question),
        Value::Object(object) => {
            object.get("is_secret").and_then(Value::as_bool) == Some(true)
                || object.get("isSecret").and_then(Value::as_bool) == Some(true)
                || object.values().any(contains_secret_question)
        }
        _ => false,
    }
}

fn contains_known_secret(value: &Value, secrets: &SecretSet) -> bool {
    let secrets = secrets
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    contains_secret_value(value, &secrets)
}

fn contains_secret_value(value: &Value, secrets: &HashSet<String>) -> bool {
    match value {
        Value::String(text) => secrets
            .iter()
            .any(|secret| !secret.is_empty() && text.contains(secret)),
        Value::Array(items) => items
            .iter()
            .any(|item| contains_secret_value(item, secrets)),
        Value::Object(object) => object
            .values()
            .any(|item| contains_secret_value(item, secrets)),
        _ => false,
    }
}

fn note_unknown(unknown: &UnknownCommands, events: &[Event]) {
    let mut unknown = unknown
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    for event in events
        .iter()
        .filter(|event| event.kind == "command.unknown")
    {
        if let Some(id) = event.data.get("command_id").and_then(Value::as_str) {
            unknown.insert(id.to_owned());
        }
    }
}

fn required_text<'a>(params: &'a Value, field: &str) -> Result<&'a str> {
    params
        .get(field)
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
        .ok_or_else(|| Error::new("invalid", format!("{field} must be a non-empty string")))
}

fn optional_text<'a>(params: &'a Value, field: &str) -> Result<Option<&'a str>> {
    match params.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) if !text.trim().is_empty() => Ok(Some(text)),
        _ => Err(Error::new(
            "invalid",
            format!("{field} must be a non-empty string"),
        )),
    }
}

fn required_i64(params: &Value, field: &str) -> Result<i64> {
    params
        .get(field)
        .and_then(Value::as_i64)
        .filter(|value| *value >= 0)
        .ok_or_else(|| Error::new("invalid", format!("{field} must be a nonnegative integer")))
}

fn optional_observation_generation(params: &Value) -> Result<Option<i64>> {
    match params.get("generation") {
        None | Some(Value::Null) => Ok(None),
        Some(_) => required_i64(params, "generation").map(Some),
    }
}

fn page_limit(params: &Value) -> Result<usize> {
    let limit = match params.get("limit") {
        None => 100,
        Some(value) => value
            .as_u64()
            .ok_or_else(|| Error::new("invalid", "limit must be a positive integer"))?,
    };
    if limit == 0 || limit > PAGE_MAX as u64 {
        return Err(Error::new("invalid", "limit must be between 1 and 1000"));
    }
    Ok(limit as usize)
}

fn reject_unknown_keys(params: &Value, allowed: &[&str], method: &str) -> Result<()> {
    let object = params
        .as_object()
        .ok_or_else(|| Error::new("invalid", format!("{method} params must be an object")))?;
    reject_unknown_map_keys(object, allowed, method)
}

fn reject_unknown_map_keys(
    object: &serde_json::Map<String, Value>,
    allowed: &[&str],
    method: &str,
) -> Result<()> {
    if let Some(key) = object.keys().find(|key| !allowed.contains(&key.as_str())) {
        Err(Error::new(
            "unsupported",
            format!("{method} field {key} is unsupported"),
        ))
    } else {
        Ok(())
    }
}

fn secure_eq(left: &str, right: &str) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.bytes()
        .zip(right.bytes())
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

fn unknown_cause(error: &Error) -> String {
    if let Some(cause) = error
        .message
        .strip_prefix("events.put outcome unknown: ")
        .or_else(|| error.message.strip_prefix("plugin outcome unknown: "))
        && !cause.is_empty()
        && cause
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return cause.chars().take(64).collect();
    }
    if error.message.contains("timed out") {
        "plugin_rpc_timeout".to_owned()
    } else if error.message.contains("closed") {
        "plugin_connection_lost".to_owned()
    } else {
        "plugin_outcome_unknown".to_owned()
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}
