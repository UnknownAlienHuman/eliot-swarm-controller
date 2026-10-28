use super::{PluginClient, PluginConnection};
use crate::bus::Bus;
use crate::monitor::Monitoring;
use crate::platform::OwnedChild;
use eliot_core::{Agent, Command, Error, Result, SourceBatch, SourceGap, SourceGapAck};
use eliot_plugin::PluginManifest;
use serde_json::{Map, Value, json};
use std::{
    collections::{HashMap, HashSet},
    ffi::OsString,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Arc, Mutex, Weak},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io,
    process::Command as ProcessCommand,
    sync::{Mutex as AsyncMutex, mpsc, oneshot, watch},
    time::{Instant, timeout_at},
};
use ulid::Ulid;

const RPC_DEADLINE: Duration = Duration::from_secs(30);
const RPC_GRACE: Duration = Duration::from_secs(2);
const STOP_GRACE: Duration = Duration::from_secs(5);

pub(crate) type SecretSet = Arc<Mutex<HashSet<String>>>;
pub(crate) type AgentCredentials = Arc<Mutex<HashMap<(String, i64), String>>>;
pub(crate) type UnknownCommands = Arc<Mutex<HashSet<String>>>;

type StoreHandle = Arc<AsyncMutex<eliot_store::Store>>;

pub(crate) struct PluginHostOptions {
    data_root: PathBuf,
    owner_token: String,
}

impl PluginHostOptions {
    pub(crate) fn new(data_root: PathBuf, owner_token: String) -> Self {
        Self {
            data_root,
            owner_token,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct AgentKey {
    id: String,
    generation: i64,
}

struct RuntimeEntry {
    sender: mpsc::Sender<RuntimeMessage>,
    verbs: HashSet<String>,
}

enum RuntimeMessage {
    Command {
        command: Command,
        reply: oneshot::Sender<Result<Value>>,
    },
    Stop {
        command: Command,
        force: bool,
        reply: oneshot::Sender<Result<Value>>,
    },
    DaemonForce {
        reply: oneshot::Sender<Result<()>>,
    },
}

struct HostState {
    data_root: PathBuf,
    owner_token: String,
    store: StoreHandle,
    bus: Bus,
    monitoring: Monitoring,
    secrets: SecretSet,
    agent_credentials: AgentCredentials,
    unknown_commands: UnknownCommands,
    progress: watch::Sender<u64>,
    late_batch_causes: Mutex<HashMap<AgentKey, String>>,
    running: Mutex<HashMap<AgentKey, RuntimeEntry>>,
}

struct AgentActorConfig {
    state: Weak<HostState>,
    monitoring: Monitoring,
    key: AgentKey,
    agent: Agent,
    start_command: Command,
    instance_dir: String,
    plugin_credential: String,
    target_environment: Value,
    source_credentials: Arc<[String]>,
    resume_from: Option<Value>,
    source_state: Option<Value>,
}

struct EventSource {
    store: StoreHandle,
    bus: Bus,
    monitoring: Monitoring,
    source_credentials: Arc<[String]>,
    state: Weak<HostState>,
    key: AgentKey,
    agent_id: String,
    generation: i64,
    expected_credential: String,
}

#[derive(Clone)]
pub(crate) struct PluginHost {
    state: Arc<HostState>,
}

impl PluginHost {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        options: PluginHostOptions,
        store: StoreHandle,
        bus: Bus,
        secrets: SecretSet,
        agent_credentials: AgentCredentials,
        unknown_commands: UnknownCommands,
        progress: watch::Sender<u64>,
        monitoring: Monitoring,
    ) -> Self {
        let PluginHostOptions {
            data_root,
            owner_token,
        } = options;
        Self {
            state: Arc::new(HostState {
                data_root,
                owner_token,
                store,
                bus,
                monitoring,
                secrets,
                agent_credentials,
                unknown_commands,
                progress,
                late_batch_causes: Mutex::new(HashMap::new()),
                running: Mutex::new(HashMap::new()),
            }),
        }
    }

    pub(crate) async fn start(
        &self,
        agent: Agent,
        command: Command,
        mut source_state: Option<Value>,
    ) -> Result<Value> {
        if !matches!(command.verb.as_str(), "launch" | "resume")
            || command.target.as_deref() != Some(agent.id.as_str())
            || command.generation != Some(agent.generation)
        {
            return Err(Error::new(
                "stale_generation",
                "start command does not match its Agent generation",
            ));
        }
        let workspace = agent
            .workspace
            .as_deref()
            .ok_or_else(|| Error::new("invalid", "Agent launch requires a workspace"))?;
        if !Path::new(workspace).is_dir() {
            return Err(Error::new(
                "invalid",
                "Agent launch workspace must be an existing directory",
            ));
        }
        if !matches!(agent.harness.as_str(), "fake" | "codex") || agent.generation <= 0 {
            return Err(Error::new(
                "unsupported",
                format!("unsupported Agent harness {}", agent.harness),
            ));
        }
        if !agent.profile_snapshot.is_object() {
            return Err(Error::new(
                "invalid",
                "Agent profile snapshot must be a JSON object",
            ));
        }
        let resume_from = if command.verb == "resume" {
            let retained_generation = source_state
                .as_ref()
                .and_then(|state| state.get("retained_generation"))
                .and_then(Value::as_i64);
            if retained_generation != command.expected_generation {
                return Err(Error::new(
                    "stale_generation",
                    "resume readback does not match the retained Agent generation",
                ));
            }
            let (Some(native_scope), Some(native_ref)) =
                (agent.native_scope.as_ref(), agent.native_ref.as_ref())
            else {
                return Err(Error::new(
                    "bad_state",
                    "resume requires the Agent's retained native subject",
                ));
            };
            Some(json!({
                "native_scope": native_scope,
                "native_ref": native_ref,
            }))
        } else {
            None
        };
        let key = AgentKey {
            id: agent.id.clone(),
            generation: agent.generation,
        };
        {
            let running = self
                .state
                .running
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if running.contains_key(&key) {
                return Err(Error::new(
                    "bad_state",
                    "an owned plugin already exists for this Agent generation",
                ));
            }
        }

        let (instance_dir, credential) = self.create_instance_dir(&key)?;
        let agent_token = {
            let store = self.state.store.lock().await;
            store
                .get_agent_token(&agent.id, agent.generation)?
                .ok_or_else(|| {
                    Error::new(
                        "bad_state",
                        "Store did not commit the generation credential before plugin start",
                    )
                })?
        };
        let mut source_credentials = vec![
            self.state.owner_token.clone(),
            agent_token.clone(),
            credential.clone(),
        ];
        source_credentials.sort_by_key(|secret| std::cmp::Reverse(secret.len()));
        let source_credentials: Arc<[String]> = source_credentials.into();
        if let Some(source_state) = source_state.as_mut() {
            crate::redaction::redact_value(source_state, &source_credentials);
        }
        let target_environment = build_target_environment(&agent, &agent_token)?;
        {
            let mut credentials = self
                .state
                .agent_credentials
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            credentials.insert((key.id.clone(), key.generation), agent_token.clone());
            self.state
                .secrets
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(agent_token.clone());
        }
        self.state.monitoring.prepare_generation(&agent).await?;
        let spawned = self.spawn_plugin(&agent, &instance_dir);
        let mut child = match spawned {
            Ok(child) => child,
            Err(error) => return Err(error),
        };
        let observer = match child.observer() {
            Ok(observer) => observer,
            Err(error) => {
                terminate_child(child, &self.state.monitoring, &agent).await;
                return Err(error);
            }
        };
        if let Err(error) = self.state.monitoring.attach(&agent, observer).await {
            terminate_child(child, &self.state.monitoring, &agent).await;
            return Err(error);
        }
        self.state
            .secrets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(credential.clone());
        let stdin = child.take_stdin();
        let stdout = child.take_stdout();
        let stderr = child.take_stderr();
        let (Some(stdin), Some(stdout), Some(stderr)) = (stdin, stdout, stderr) else {
            terminate_child(child, &self.state.monitoring, &agent).await;
            self.remove_plugin_secret(&credential);
            return Err(Error::new(
                "process_pipes",
                "Agent plugin did not expose all three owned pipes",
            ));
        };

        tokio::spawn(async move {
            let mut stderr = stderr;
            let mut sink = io::sink();
            let _ = io::copy(&mut stderr, &mut sink).await;
        });

        let connection = PluginConnection::new(stdout, stdin);
        let client = connection.client();
        let event_task = tokio::spawn(serve_plugin_requests(
            connection,
            EventSource {
                store: Arc::clone(&self.state.store),
                bus: self.state.bus.clone(),
                monitoring: self.state.monitoring.clone(),
                source_credentials: Arc::clone(&source_credentials),
                state: Arc::downgrade(&self.state),
                key: key.clone(),
                agent_id: key.id.clone(),
                generation: key.generation,
                expected_credential: credential.clone(),
            },
        ));
        let (message_tx, message_rx) = mpsc::channel(16);
        let (started_tx, started_rx) = oneshot::channel();
        let actor_key = key.clone();
        {
            let mut running = self
                .state
                .running
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            running.insert(
                actor_key.clone(),
                RuntimeEntry {
                    sender: message_tx,
                    verbs: ["launch", "send", "stop"]
                        .into_iter()
                        .map(str::to_owned)
                        .collect(),
                },
            );
        }
        let actor = AgentActorConfig {
            state: Arc::downgrade(&self.state),
            monitoring: self.state.monitoring.clone(),
            key: actor_key,
            agent: agent.clone(),
            start_command: command.clone(),
            instance_dir: instance_dir.to_string_lossy().into_owned(),
            plugin_credential: credential.clone(),
            target_environment,
            source_credentials,
            resume_from,
            source_state,
        };
        tokio::spawn(async move {
            run_agent(actor, child, client, event_task, message_rx, started_tx).await;
        });

        match timeout_at(Instant::now() + RPC_DEADLINE + RPC_GRACE, started_rx).await {
            Ok(Ok(Ok(result))) => Ok(result),
            Ok(Ok(Err(error))) => Err(error),
            Ok(Err(_)) => Err(Error::new(
                "unknown",
                "Agent plugin ended before start readiness was established",
            )),
            Err(_) => Err(Error::new(
                "unknown",
                "Agent plugin start exceeded its absolute deadline",
            )),
        }
    }

    pub(crate) async fn command(&self, agent: &Agent, command: Command) -> Result<Value> {
        let key = AgentKey {
            id: agent.id.clone(),
            generation: agent.generation,
        };
        if command.target.as_deref() != Some(agent.id.as_str())
            || command.generation != Some(agent.generation)
        {
            return Err(Error::new(
                "stale_generation",
                "command does not match the owned Agent generation",
            ));
        }
        match command.verb.as_str() {
            "steer" | "interrupt" if command.turn_id.is_none() => {
                return Err(Error::new(
                    "stale_generation",
                    "turn control requires the captured active turn",
                ));
            }
            "approve" | "deny" | "answer"
                if command.request_id.is_none() || command.turn_id.is_none() =>
            {
                return Err(Error::new(
                    "bad_state",
                    "request reply requires its captured request and turn ids",
                ));
            }
            _ => {}
        }
        let sender = self.sender(&key, &command.verb)?;
        let (reply, response) = oneshot::channel();
        timeout_at(
            Instant::now() + RPC_DEADLINE,
            sender.send(RuntimeMessage::Command { command, reply }),
        )
        .await
        .map_err(|_| Error::new("busy", "Agent plugin command queue is full"))?
        .map_err(|_| Error::new("unknown", "owned Agent plugin is no longer available"))?;
        match timeout_at(Instant::now() + RPC_DEADLINE + RPC_GRACE, response).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(Error::new(
                "unknown",
                "Agent plugin ended before the command outcome was returned",
            )),
            Err(_) => Err(Error::new(
                "unknown",
                "Agent plugin command exceeded its absolute deadline",
            )),
        }
    }

    pub(crate) async fn stop(&self, agent: &Agent, command: Command, force: bool) -> Result<Value> {
        let key = AgentKey {
            id: agent.id.clone(),
            generation: agent.generation,
        };
        let sender = self.sender(&key, "stop")?;
        let (reply, response) = oneshot::channel();
        timeout_at(
            Instant::now() + RPC_DEADLINE,
            sender.send(RuntimeMessage::Stop {
                command,
                force,
                reply,
            }),
        )
        .await
        .map_err(|_| Error::new("busy", "Agent plugin command queue is full"))?
        .map_err(|_| Error::new("unknown", "owned Agent plugin is no longer available"))?;
        match timeout_at(
            Instant::now() + RPC_DEADLINE + STOP_GRACE + STOP_GRACE + RPC_GRACE,
            response,
        )
        .await
        {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(Error::new(
                "unknown",
                "Agent plugin ended before stop custody was settled",
            )),
            Err(_) => Err(Error::new(
                "unknown",
                "Agent plugin stop exceeded its absolute deadline",
            )),
        }
    }

    pub(crate) async fn force_all(&self) {
        let entries: Vec<_> = self
            .state
            .running
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|(key, entry)| (key.clone(), entry.sender.clone()))
            .collect();
        for (key, sender) in entries {
            let (reply, response) = oneshot::channel();
            if sender
                .send(RuntimeMessage::DaemonForce { reply })
                .await
                .is_ok()
            {
                let _ = timeout_at(
                    Instant::now() + RPC_DEADLINE + STOP_GRACE + STOP_GRACE + RPC_GRACE,
                    response,
                )
                .await;
            } else {
                self.mark_lost(&key).await;
            }
        }
    }

    pub(crate) fn live_count(&self) -> usize {
        self.state
            .running
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    pub(crate) fn controls_generation(&self, agent: &str, generation: i64) -> bool {
        self.state
            .running
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key(&AgentKey {
                id: agent.to_owned(),
                generation,
            })
    }

    pub(crate) fn take_late_batch_cause(&self, agent: &str, generation: i64) -> Option<String> {
        self.state
            .late_batch_causes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&AgentKey {
                id: agent.to_owned(),
                generation,
            })
    }

    fn sender(&self, key: &AgentKey, verb: &str) -> Result<mpsc::Sender<RuntimeMessage>> {
        self.state
            .running
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(key)
            .ok_or_else(|| {
                Error::new(
                    "not_found",
                    format!(
                        "no owned plugin exists for {} generation {}",
                        key.id, key.generation
                    ),
                )
            })
            .and_then(|entry| {
                if entry.verbs.contains(verb) {
                    Ok(entry.sender.clone())
                } else {
                    Err(Error::new(
                        "unsupported",
                        format!("Agent plugin does not advertise {verb}"),
                    ))
                }
            })
    }

    fn install_runtime_verbs(
        state: &Weak<HostState>,
        key: &AgentKey,
        verbs: HashSet<String>,
    ) -> Result<()> {
        let state = state
            .upgrade()
            .ok_or_else(|| Error::new("unknown", "plugin host stopped during initialization"))?;
        let mut running = state
            .running
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = running.get_mut(key).ok_or_else(|| {
            Error::new(
                "unknown",
                "Agent plugin ownership changed during initialization",
            )
        })?;
        entry.verbs = verbs;
        Ok(())
    }

    fn create_instance_dir(&self, key: &AgentKey) -> Result<(PathBuf, String)> {
        let parent = self.state.data_root.join("plugins");
        std::fs::create_dir_all(&parent).map_err(|error| {
            Error::new(
                "platform_io",
                format!("create plugin instance root: {error}"),
            )
        })?;
        let instance_dir = parent.join(format!("{}-g{}-{}", key.id, key.generation, Ulid::new()));
        std::fs::create_dir(&instance_dir).map_err(|error| {
            Error::new(
                "platform_io",
                format!("create plugin instance directory: {error}"),
            )
        })?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&instance_dir, std::fs::Permissions::from_mode(0o700))
                .map_err(|error| {
                    Error::new(
                        "platform_io",
                        format!("restrict plugin instance directory: {error}"),
                    )
                })?;
        }
        let credential = format!("{}{}", Ulid::new(), Ulid::new());
        Ok((instance_dir, credential))
    }

    fn spawn_plugin(&self, agent: &Agent, _instance_dir: &Path) -> Result<OwnedChild> {
        let executable = std::env::current_exe().map_err(|error| {
            Error::new("platform_io", format!("locate Eliot executable: {error}"))
        })?;
        let workspace = agent
            .workspace
            .as_deref()
            .ok_or_else(|| Error::new("invalid", "Agent launch requires a workspace"))?;
        let mut command = ProcessCommand::new(executable);
        command
            .arg("--data-dir")
            .arg(&self.state.data_root)
            .arg("plugin")
            .arg(&agent.harness)
            .current_dir(workspace)
            .env_clear()
            .envs(minimal_plugin_environment())
            .kill_on_drop(true)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        OwnedChild::spawn(command)
    }

    fn remove_plugin_secret(&self, credential: &str) {
        self.state
            .secrets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(credential);
    }

    async fn mark_lost(&self, key: &AgentKey) {
        fence_runtime_generation(&self.state, key);
        {
            let mut store = self.state.store.lock().await;
            if let Ok(events) = store.mark_lost(&key.id, key.generation, now_ms()) {
                self.state.bus.publish(&events);
                note_unknown_commands(&self.state, &events);
            }
        }
        forget_if_revoked(&self.state, key).await;
        signal_progress(&self.state);
    }
}

async fn run_agent(
    actor: AgentActorConfig,
    mut child: OwnedChild,
    client: PluginClient,
    mut event_task: tokio::task::JoinHandle<()>,
    mut messages: mpsc::Receiver<RuntimeMessage>,
    started: oneshot::Sender<Result<Value>>,
) {
    let AgentActorConfig {
        state,
        monitoring,
        key,
        agent,
        start_command,
        instance_dir,
        plugin_credential,
        target_environment,
        source_credentials,
        resume_from,
        source_state,
    } = actor;
    let mut last_command_id = start_command.id.clone();
    let init = client
        .call(
            "initialize",
            json!({
                "protocol": "eliot-plugin/1",
                "credential": plugin_credential,
                "instance_dir": instance_dir,
                "config": {"harness": agent.harness},
            }),
            deadline(),
        )
        .await;
    let init = match init {
        Ok(value) => value,
        Err(error) => {
            let _ = started.send(Err(redact_native_error(error, &source_credentials)));
            terminate_child(child, &monitoring, &agent).await;
            let _ = event_task.await;
            cleanup_actor(&state, &key, &plugin_credential, false).await;
            return;
        }
    };
    let manifest: PluginManifest = match serde_json::from_value(init) {
        Ok(manifest) => manifest,
        Err(_) => {
            let _ = started.send(Err(Error::new(
                "unsupported",
                "Agent plugin returned a malformed manifest",
            )));
            terminate_child(child, &monitoring, &agent).await;
            let _ = event_task.await;
            cleanup_actor(&state, &key, &plugin_credential, false).await;
            return;
        }
    };
    let source_plan = match manifest_source_plan(&manifest, &agent.harness) {
        Ok(source_plan) => source_plan,
        Err(error) => {
            let _ = started.send(Err(error));
            terminate_child(child, &monitoring, &agent).await;
            let _ = event_task.await;
            cleanup_actor(&state, &key, &plugin_credential, false).await;
            return;
        }
    };
    if !manifest
        .verbs
        .iter()
        .any(|verb| verb == &start_command.verb)
    {
        let _ = started.send(Err(Error::new(
            "unsupported",
            format!("Agent plugin does not advertise {}", start_command.verb),
        )));
        terminate_child(child, &monitoring, &agent).await;
        let _ = event_task.await;
        cleanup_actor(&state, &key, &plugin_credential, false).await;
        return;
    }
    let verbs: HashSet<String> = manifest.verbs.iter().cloned().collect();
    if let Err(error) = PluginHost::install_runtime_verbs(&state, &key, verbs) {
        let _ = started.send(Err(error));
        terminate_child(child, &monitoring, &agent).await;
        let _ = event_task.await;
        cleanup_actor(&state, &key, &plugin_credential, false).await;
        return;
    }

    let start_params = json!({
        "agent_id": agent.id,
        "generation": agent.generation,
        "command_id": start_command.id,
        "profile": agent.profile_snapshot,
        "workspace": agent.workspace,
        "target_environment": target_environment,
        "note": null,
        "source_plan": source_plan,
        "source_state": source_state,
        "resume_from": resume_from,
        "initial": null,
    });
    let start = client.call("agent.start", start_params, deadline()).await;
    match start {
        Ok(value) if validate_start_result(&value, &agent, &start_command).is_ok() => {
            let _ = started.send(Ok(value));
        }
        Ok(_) => {
            let error = Error::new("unknown", "plugin start did not prove session readiness");
            let _ = started.send(Err(error.clone()));
            if error.code != "unknown" {
                terminate_child(child, &monitoring, &agent).await;
                let _ = event_task.await;
                cleanup_actor(&state, &key, &plugin_credential, false).await;
                return;
            }
        }
        Err(error) => {
            let error = redact_native_error(error, &source_credentials);
            let uncertain = error.code == "unknown";
            let _ = started.send(Err(error));
            if !uncertain {
                terminate_child(child, &monitoring, &agent).await;
                let _ = event_task.await;
                cleanup_actor(&state, &key, &plugin_credential, false).await;
                return;
            }
        }
    }

    let mut planned_exit = false;
    loop {
        tokio::select! {
            status = child.wait() => {
                if let Some(state) = state.upgrade() {
                    fence_command_routing(&state, &key);
                }
                if status.is_ok() {
                    let _ = monitoring.sample_generation(&agent).await;
                }
                // Keep source authorization intact throughout the bounded drain.
                let gap_reason = match timeout_at(
                    Instant::now() + STOP_GRACE,
                    &mut event_task,
                ).await {
                    Ok(Ok(())) => "root_exit_without_session_terminal",
                    Ok(Err(_)) => "root_exit_source_failed",
                    Err(_) => {
                        event_task.abort();
                        let _ = timeout_at(
                            Instant::now() + RPC_GRACE,
                            &mut event_task,
                        ).await;
                        "root_exit_drain_timeout"
                    }
                };
                let source_settled = record_root_exit_gap(&state, &key, gap_reason).await.is_ok();
                #[cfg(windows)]
                let control_gone = match child.wait_family_empty(Duration::ZERO).await {
                    Ok(true) => true,
                    Ok(false) => {
                        child.terminate().is_ok()
                            && matches!(child.wait_family_empty(STOP_GRACE).await, Ok(true))
                    }
                    Err(_) => false,
                };
                // Unix custody remains degraded: leader exit proves native
                // control loss, not that every descendant has been contained.
                #[cfg(not(windows))]
                let control_gone = status.is_ok();
                if control_gone {
                    let _ = monitoring.sample_generation(&agent).await;
                }
                if source_settled && control_gone && !planned_exit {
                    mark_lost_state(&state, &key).await;
                }
                break;
            }
            event_result = &mut event_task => {
                if let Some(state) = state.upgrade() {
                    fence_command_routing(&state, &key);
                }
                let gap_reason = if event_result.is_ok() {
                    "stream_eof_without_session_terminal"
                } else {
                    "stream_task_failed"
                };
                let source_settled = record_root_exit_gap(&state, &key, gap_reason).await.is_ok();
                let control_gone = child.terminate().is_ok()
                    && wait_child(&mut child, STOP_GRACE, &monitoring, &agent).await;
                if source_settled && control_gone {
                    mark_lost_state(&state, &key).await;
                }
                planned_exit = true;
                break;
            }
            message = messages.recv() => {
                let Some(message) = message else {
                    break;
                };
                match message {
                    RuntimeMessage::Command { command, reply } => {
                        last_command_id = command.id.clone();
                        let verb = command.verb.clone();
                        let result = match plugin_command_args(&command) {
                            Err(error) => Err(error),
                            Ok(args) => {
                                let params = json!({
                                    "agent_id": agent.id,
                                    "generation": agent.generation,
                                    "command_id": command.id,
                                    "verb": verb.clone(),
                                    "turn_id": command.turn_id,
                                    "request_id": command.request_id,
                                    "args": args,
                                });
                                client.call("agent.command", params, deadline()).await
                                    .map_err(|error| redact_native_error(error, &source_credentials))
                                    .and_then(|mut value| {
                                        crate::redaction::redact_value(
                                            &mut value,
                                            &source_credentials,
                                        );
                                        validate_agent_command_result(value, &verb)
                                    })
                            }
                        };
                        let _ = reply.send(result);
                    }
                    RuntimeMessage::Stop { command, force, reply } => {
                        let snapshot_command_id = last_command_id.clone();
                        last_command_id = command.id.clone();
                        let params = json!({
                            "agent_id": agent.id,
                            "generation": agent.generation,
                            "command_id": command.id,
                            "force": force,
                        });
                        let cooperative = client
                            .call("agent.stop", params, deadline())
                            .await
                            .map_err(|error| redact_native_error(error, &source_credentials))
                            .and_then(validate_applied);
                        match cooperative {
                            Ok(result) => {
                                let guarantee = child.guarantee();
                                let shutdown = client
                                    .call("shutdown", json!({}), deadline())
                                    .await
                                    .and_then(validate_shutdown);
                                if shutdown.is_ok() {
                                    let _ = client
                                        .close_input(Instant::now() + STOP_GRACE)
                                        .await;
                                }
                                let graceful = wait_child(&mut child, STOP_GRACE, &monitoring, &agent).await;
                                let mut exited = graceful;
                                if !exited {
                                    let _ = child.terminate();
                                    exited = wait_child(&mut child, STOP_GRACE, &monitoring, &agent).await;
                                }
                                if !exited {
                                    drop(child);
                                    let _ = event_task.await;
                                    if let Some(state) = state.upgrade() {
                                        fence_runtime_generation(&state, &key);
                                    }
                                    planned_exit = true;
                                    let _ = reply.send(Err(Error::new(
                                        "unknown",
                                        "stop could not confirm owned process-family exit",
                                    )));
                                    break;
                                }
                                let _ = event_task.await;
                                if let Some(state) = state.upgrade() {
                                    fence_runtime_generation(&state, &key);
                                }
                                planned_exit = true;
                                let _ = reply.send(Ok(json!({
                                    "state": "applied",
                                    "custody": custody_label(guarantee),
                                    "graceful": graceful,
                                    "native": result,
                                })));
                                break;
                            }
                            Err(_) if force => {
                                let snapshot_command = snapshot_command_id.clone();
                                let snapshot = client.call(
                                    "agent.snapshot",
                                    json!({
                                        "agent_id": agent.id,
                                        "generation": agent.generation,
                                        "command_id": snapshot_command,
                                    }),
                                    Instant::now() + Duration::from_secs(5),
                                ).await.ok();
                                let guarantee = child.guarantee();
                                let terminated = child.terminate().is_ok();
                                let exited = wait_child(&mut child, STOP_GRACE, &monitoring, &agent).await;
                                if !terminated || !exited {
                                    drop(child);
                                    let _ = event_task.await;
                                    mark_lost_state(&state, &key).await;
                                    planned_exit = true;
                                    let _ = reply.send(Err(Error::new(
                                        "unknown",
                                        format!(
                                            "force-stop could not confirm owned custody termination ({})",
                                            custody_label(guarantee)
                                        ),
                                    )));
                                    break;
                                }
                                let _ = event_task.await;
                                if let Some(state) = state.upgrade() {
                                    fence_runtime_generation(&state, &key);
                                }
                                planned_exit = true;
                                let mut response = json!({
                                    "state": "applied",
                                    "custody": custody_label(guarantee),
                                    "terminated": true,
                                    "cooperative": "unknown",
                                });
                                if let Some(snapshot) = snapshot {
                                    response["snapshot"] = snapshot;
                                }
                                let _ = reply.send(Ok(response));
                                break;
                            }
                            Err(error) => {
                                if error.code == "unknown" {
                                    let _ = client.call(
                                        "agent.snapshot",
                                        json!({
                                            "agent_id": agent.id,
                                            "generation": agent.generation,
                                            "command_id": snapshot_command_id,
                                        }),
                                        Instant::now() + Duration::from_secs(5),
                                    ).await;
                                }
                                let _ = reply.send(Err(error));
                            }
                        }
                    }
                    RuntimeMessage::DaemonForce { reply } => {
                        let guarantee = child.guarantee();
                        let terminated = child.terminate().is_ok();
                        let exited = wait_child(&mut child, STOP_GRACE, &monitoring, &agent).await;
                        if !terminated || !exited {
                            drop(child);
                            let _ = event_task.await;
                            mark_lost_state(&state, &key).await;
                            planned_exit = true;
                            let _ = reply.send(Err(Error::new(
                                "unknown",
                                format!(
                                    "daemon force could not confirm owned custody termination ({})",
                                    custody_label(guarantee)
                                ),
                            )));
                            break;
                        }
                        let _ = event_task.await;
                        mark_lost_state(&state, &key).await;
                        planned_exit = true;
                        let _ = reply.send(Ok(()));
                        break;
                    }
                }
            }
        }
    }
    cleanup_actor(&state, &key, &plugin_credential, planned_exit).await;
}

async fn serve_plugin_requests(mut connection: PluginConnection, source: EventSource) {
    let EventSource {
        store,
        bus,
        monitoring,
        source_credentials,
        state,
        key,
        agent_id,
        generation,
        expected_credential,
    } = source;
    let client = connection.client();
    while let Some(request) = connection.next_request().await {
        let request_id = request.id.clone();
        let method = request.method.clone();
        let request_deadline = deadline();
        let result = if !matches!(
            method.as_str(),
            "events.put" | "source.gap" | "source.state" | "process.put"
        ) {
            Err(Error::new(
                "unsupported",
                "plugin host only accepts events.put, source.gap, source.state, and process.put",
            ))
        } else {
            let mut params = request.params;
            let credential = params
                .as_object_mut()
                .and_then(|object| object.remove("credential"))
                .and_then(|value| value.as_str().map(ToOwned::to_owned));
            if !credential
                .as_deref()
                .is_some_and(|credential| secure_eq(credential, &expected_credential))
            {
                if method == "events.put" {
                    Err(late_batch_error(
                        &state,
                        &agent_id,
                        generation,
                        "plugin_credential_invalid",
                    ))
                } else {
                    Err(Error::new("not_allowed", "plugin credential is invalid"))
                }
            } else {
                match method.as_str() {
                    "process.put" => {
                        let current = state.upgrade().is_some_and(|state| {
                            state
                                .running
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .contains_key(&key)
                        });
                        if !current
                            || params.get("agent_id").and_then(Value::as_str)
                                != Some(agent_id.as_str())
                            || params.get("generation").and_then(Value::as_i64) != Some(generation)
                        {
                            Err(Error::new(
                                "stale_generation",
                                "process.put does not match the current plugin generation",
                            ))
                        } else {
                            match (
                                params
                                    .get("pid")
                                    .and_then(Value::as_u64)
                                    .and_then(|pid| u32::try_from(pid).ok())
                                    .filter(|pid| *pid > 0),
                                params.get("role").and_then(Value::as_str),
                            ) {
                                (Some(pid), Some("harness")) => {
                                    match timeout_at(
                                        request_deadline,
                                        monitoring
                                            .process_put(&agent_id, generation, pid, "harness"),
                                    )
                                    .await
                                    {
                                        Ok(result) => result,
                                        Err(_) => Err(Error::new(
                                            "unknown",
                                            "process.put acknowledgement deadline elapsed",
                                        )),
                                    }
                                }
                                _ => Err(Error::new(
                                    "invalid",
                                    "process.put requires a nonzero PID and the harness role",
                                )),
                            }
                        }
                    }
                    "events.put" => match serde_json::from_value::<SourceBatch>(params) {
                        Ok(mut batch)
                            if batch.agent_id == agent_id && batch.generation == generation =>
                        {
                            match redact_source_batch(&mut batch, &source_credentials) {
                                Err(error) => Err(error),
                                Ok(()) => match timeout_at(request_deadline, store.lock()).await {
                                    Ok(mut store) => match store.put_batch(batch, now_ms()) {
                                        Ok(commit) => {
                                            bus.publish(&commit.events);
                                            drop(store);
                                            serde_json::to_value(commit.ack).map_err(|_| {
                                                late_batch_error(
                                                    &state,
                                                    &agent_id,
                                                    generation,
                                                    "ack_encoding_failed",
                                                )
                                            })
                                        }
                                        Err(error) => {
                                            let cause = if store.is_write_faulted() {
                                                "store_write_faulted".to_owned()
                                            } else {
                                                format!("store_{}", safe_cause_code(&error.code))
                                            };
                                            Err(late_batch_error(
                                                &state, &agent_id, generation, &cause,
                                            ))
                                        }
                                    },
                                    Err(_) => Err(late_batch_error(
                                        &state,
                                        &agent_id,
                                        generation,
                                        "store_lock_deadline",
                                    )),
                                },
                            }
                        }
                        Ok(_) => Err(late_batch_error(
                            &state,
                            &agent_id,
                            generation,
                            "source_generation_mismatch",
                        )),
                        Err(_) => Err(late_batch_error(
                            &state,
                            &agent_id,
                            generation,
                            "events_put_malformed",
                        )),
                    },
                    "source.gap" => match serde_json::from_value::<SourceGap>(params) {
                        Ok(mut gap) if gap.agent_id == agent_id && gap.generation == generation => {
                            match redact_source_gap(&mut gap, &source_credentials) {
                                Err(error) => Err(error),
                                Ok(())
                                    if gap.agent_id == agent_id && gap.generation == generation =>
                                {
                                    match timeout_at(request_deadline, store.lock()).await {
                                        Ok(mut store) => {
                                            match store.record_source_gap(gap, now_ms()) {
                                                Ok(change) => {
                                                    bus.publish(&change.events);
                                                    drop(store);
                                                    let ack: SourceGapAck = change.value;
                                                    serde_json::to_value(ack).map_err(
                                                        |_| {
                                                            Error::new(
                                                                "internal",
                                                                "source.gap acknowledgement could not be encoded",
                                                            )
                                                        },
                                                    )
                                                }
                                                Err(_) if store.is_write_faulted() => {
                                                    Err(Error::new(
                                                        "unknown",
                                                        "source.gap outcome is unknown; settle with source.state",
                                                    ))
                                                }
                                                Err(error) => Err(redact_native_error(
                                                    error,
                                                    &source_credentials,
                                                )),
                                            }
                                        }
                                        Err(_) => Err(Error::new(
                                            "busy",
                                            "Store lock deadline elapsed for source.gap",
                                        )),
                                    }
                                }
                                Ok(()) => Err(Error::new(
                                    "not_allowed",
                                    "source.gap binding does not match this Agent generation",
                                )),
                            }
                        }
                        Ok(_) => Err(Error::new(
                            "not_allowed",
                            "source.gap binding does not match this Agent generation",
                        )),
                        Err(_) => Err(Error::new("invalid", "source.gap params are malformed")),
                    },
                    "source.state" => match source_state_binding(&params, &agent_id, generation) {
                        Ok(source_name) => match timeout_at(request_deadline, store.lock()).await {
                            Ok(store) => {
                                match store.read_source_state(&agent_id, generation, &source_name) {
                                    Ok(mut source_state) => {
                                        crate::redaction::redact_value(
                                            &mut source_state,
                                            &source_credentials,
                                        );
                                        Ok(source_state)
                                    }
                                    Err(error) => {
                                        Err(redact_native_error(error, &source_credentials))
                                    }
                                }
                            }
                            Err(_) => Err(Error::new(
                                "busy",
                                "Store lock deadline elapsed for source.state",
                            )),
                        },
                        Err(error) => Err(error),
                    },
                    _ => Err(Error::new(
                        "unsupported",
                        "plugin request method is not implemented",
                    )),
                }
            }
        };
        let _ = client.respond(request_id, result, request_deadline).await;
    }
    if let Some(state) = state.upgrade() {
        fence_runtime_generation(&state, &key);
        signal_progress(&state);
    }
}

fn late_batch_error(
    state: &Weak<HostState>,
    agent_id: &str,
    generation: i64,
    cause: &str,
) -> Error {
    if let Some(state) = state.upgrade() {
        state
            .late_batch_causes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                AgentKey {
                    id: agent_id.to_owned(),
                    generation,
                },
                cause.to_owned(),
            );
    }
    Error::new("unknown", format!("events.put outcome unknown: {cause}"))
}

fn safe_cause_code(code: &str) -> String {
    let safe: String = code
        .chars()
        .filter(|character| character.is_ascii_alphanumeric() || *character == '_')
        .take(48)
        .collect();
    if safe.is_empty() {
        "store_error".to_owned()
    } else {
        safe
    }
}

fn redact_source_batch(batch: &mut SourceBatch, credentials: &[String]) -> Result<()> {
    for event in &mut batch.events {
        if crate::redaction::redact_value(&mut event.data, credentials) {
            let Some(data) = event.data.as_object_mut() else {
                return Err(Error::new(
                    "invalid",
                    "redacted native event data must be an object",
                ));
            };
            data.insert("redacted".to_owned(), Value::Bool(true));
        }
    }
    Ok(())
}

fn redact_source_gap(gap: &mut SourceGap, credentials: &[String]) -> Result<()> {
    let mut value = serde_json::to_value(&*gap)
        .map_err(|_| Error::new("internal", "source.gap could not be encoded"))?;
    crate::redaction::redact_value(&mut value, credentials);
    *gap = serde_json::from_value(value)
        .map_err(|_| Error::new("invalid", "source.gap became invalid after redaction"))?;
    Ok(())
}

fn source_state_binding(
    params: &Value,
    expected_agent: &str,
    expected_generation: i64,
) -> Result<String> {
    let object = params
        .as_object()
        .ok_or_else(|| Error::new("invalid", "source.state params must be an object"))?;
    if object
        .keys()
        .any(|key| !["agent_id", "generation", "source"].contains(&key.as_str()))
    {
        return Err(Error::new(
            "invalid",
            "source.state contains an unsupported field",
        ));
    }
    let agent = object
        .get("agent_id")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::new("invalid", "source.state requires agent_id"))?;
    if agent != expected_agent {
        return Err(Error::new(
            "not_allowed",
            "source.state is outside this plugin's Agent binding",
        ));
    }
    let generation = object
        .get("generation")
        .and_then(Value::as_i64)
        .ok_or_else(|| Error::new("invalid", "source.state requires generation"))?;
    if generation != expected_generation {
        return Err(Error::new(
            "stale_generation",
            "source.state generation does not match this plugin instance",
        ));
    }
    object
        .get("source")
        .and_then(Value::as_str)
        .filter(|source| !source.trim().is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| Error::new("invalid", "source.state requires source"))
}

fn redact_native_error(error: Error, credentials: &[String]) -> Error {
    Error::new(
        error.code,
        crate::redaction::redact_text(&error.message, credentials),
    )
}

fn plugin_command_args(command: &Command) -> Result<Value> {
    if !command.args.is_object() {
        return Err(Error::new(
            "invalid",
            "plugin command args must be an object",
        ));
    }
    match command.verb.as_str() {
        "send" => {}
        "steer" | "interrupt" => {
            command.turn_id.as_ref().ok_or_else(|| {
                Error::new("stale_generation", "turn control has no captured turn id")
            })?;
        }
        "approve" | "deny" | "answer" => {
            command
                .turn_id
                .as_ref()
                .ok_or_else(|| Error::new("bad_state", "request reply has no captured turn id"))?;
            command.request_id.as_ref().ok_or_else(|| {
                Error::new("bad_state", "request reply has no captured request id")
            })?;
        }
        _ => {
            return Err(Error::new(
                "unsupported",
                format!("plugin command verb {} is not implemented", command.verb),
            ));
        }
    }
    Ok(command.args.clone())
}

fn validate_agent_command_result(value: Value, verb: &str) -> Result<Value> {
    match value.get("state").and_then(Value::as_str) {
        Some("applied") if verb == "send" => validate_send_result(value),
        Some("applied" | "rejected" | "failed" | "unknown") => Ok(value),
        _ => Err(Error::new(
            "unknown",
            "plugin command response omitted its command outcome",
        )),
    }
}

fn validate_send_result(value: Value) -> Result<Value> {
    if value.get("state").and_then(Value::as_str) != Some("applied")
        || value
            .get("turn_id")
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
    {
        return Err(Error::new(
            "unknown",
            "plugin send response did not include its exact turn association",
        ));
    }
    Ok(value)
}

fn validate_start_result(value: &Value, agent: &Agent, command: &Command) -> Result<()> {
    if value.get("state").and_then(Value::as_str) == Some("ready")
        && value.get("agent_id").and_then(Value::as_str) == Some(agent.id.as_str())
        && value.get("generation").and_then(Value::as_i64) == Some(agent.generation)
        && value.get("command_id").and_then(Value::as_str) == Some(command.id.as_str())
    {
        Ok(())
    } else {
        Err(Error::new(
            "unknown",
            "plugin start response did not prove readiness for the requested generation",
        ))
    }
}

fn validate_applied(value: Value) -> Result<Value> {
    if value.get("state").and_then(Value::as_str) == Some("applied") {
        Ok(value)
    } else {
        Err(Error::new(
            "unknown",
            "plugin stop response did not prove the session ended",
        ))
    }
}

fn validate_shutdown(value: Value) -> Result<Value> {
    if value.get("state").and_then(Value::as_str) == Some("shutdown") {
        Ok(value)
    } else {
        Err(Error::new(
            "unknown",
            "plugin shutdown response did not confirm shutdown",
        ))
    }
}

fn manifest_source_plan(manifest: &PluginManifest, harness: &str) -> Result<Value> {
    if manifest.name != harness
        || manifest.protocol != "eliot-plugin/1"
        || !manifest.available
        || manifest.mode != "per_agent"
        || manifest.version.trim().is_empty()
        || manifest.harness_version.trim().is_empty()
    {
        return Err(unsupported_manifest());
    }

    let verbs: HashSet<_> = manifest.verbs.iter().map(String::as_str).collect();
    let implemented_verbs = [
        "launch",
        "send",
        "stop",
        "resume",
        "steer",
        "interrupt",
        "approve",
        "deny",
        "answer",
    ];
    if verbs.len() != manifest.verbs.len()
        || verbs.len() < 3
        || !["launch", "send", "stop"]
            .iter()
            .all(|required| verbs.contains(required))
        || verbs.iter().any(|verb| !implemented_verbs.contains(verb))
        || !manifest.child_verbs.is_empty()
        || manifest.features.start_requires_input
        || manifest.features.native_children
    {
        return Err(unsupported_manifest());
    }

    if !matches!(harness, "fake" | "codex") {
        return Err(unsupported_manifest());
    }
    let expected_families = ["message", "session", "turn"];
    if manifest.sources.len() != 1 {
        return Err(unsupported_manifest());
    }
    let controlled = manifest
        .sources
        .get("controlled")
        .ok_or_else(unsupported_manifest)?;
    let tool_expected = harness == "codex";
    let request_expected = harness == "codex";
    let expected_count = expected_families.len()
        + if tool_expected { 1 } else { 0 }
        + if request_expected { 1 } else { 0 };
    if controlled.len() != expected_count
        || controlled.contains_key("tool") != tool_expected
        || controlled.contains_key("request") != request_expected
        || controlled.keys().any(|family| {
            !["message", "request", "session", "tool", "turn"].contains(&family.as_str())
        })
        || expected_families
            .iter()
            .any(|family| controlled.get(*family).map(String::as_str) != Some("stream"))
        || (tool_expected && controlled.get("tool").map(String::as_str) != Some("stream"))
        || (request_expected && controlled.get("request").map(String::as_str) != Some("stream"))
    {
        return Err(unsupported_manifest());
    }

    Ok(Value::Object(
        controlled
            .iter()
            .map(|(family, mechanism)| (family.clone(), Value::String(mechanism.clone())))
            .collect(),
    ))
}

fn unsupported_manifest() -> Error {
    Error::new(
        "unsupported",
        "plugin manifest does not match the implemented per-Agent capabilities",
    )
}

#[cfg(windows)]
const BASE_ENVIRONMENT_NAMES: &[&str] = &[
    "USERPROFILE",
    "HOME",
    "LOCALAPPDATA",
    "SystemRoot",
    "PATH",
    "TEMP",
];

#[cfg(not(windows))]
const BASE_ENVIRONMENT_NAMES: &[&str] = &["HOME", "PATH", "TEMP"];

fn minimal_plugin_environment() -> Vec<(OsString, OsString)> {
    BASE_ENVIRONMENT_NAMES
        .iter()
        .filter_map(|name| std::env::var_os(name).map(|value| (OsString::from(*name), value)))
        .collect()
}

pub(crate) fn build_process_environment(profile: &Value) -> Result<Map<String, Value>> {
    let mut environment = Map::new();
    for name in BASE_ENVIRONMENT_NAMES {
        if let Some(value) = std::env::var_os(name) {
            let value = value.into_string().map_err(|_| {
                Error::new(
                    "invalid",
                    format!("base environment variable {name} is not valid Unicode"),
                )
            })?;
            insert_environment(&mut environment, name, value);
        }
    }

    let pass_env = match profile.get("pass_env") {
        Some(Value::Array(names)) => Some(names.as_slice()),
        None => None,
        _ => {
            return Err(Error::new(
                "invalid",
                "frozen profile pass_env must be an array of names",
            ));
        }
    };
    if let Some(pass_env) = pass_env {
        for value in pass_env {
            let name = value
                .as_str()
                .filter(|name| valid_environment_name(name))
                .ok_or_else(|| {
                    Error::new(
                        "invalid",
                        "frozen profile pass_env entries must be environment variable names",
                    )
                })?;
            if name
                .get(..12)
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("ELIOT_AGENT_"))
            {
                return Err(Error::new(
                    "invalid",
                    "frozen profile pass_env cannot override Eliot Agent identity variables",
                ));
            }
            if let Some(value) = std::env::var_os(name) {
                let value = value.into_string().map_err(|_| {
                    Error::new(
                        "invalid",
                        format!("pass_env variable {name} is not valid Unicode"),
                    )
                })?;
                insert_environment(&mut environment, name, value);
            }
        }
    }

    Ok(environment)
}

fn build_target_environment(agent: &Agent, agent_token: &str) -> Result<Value> {
    if agent.harness == "codex"
        && !agent
            .profile_snapshot
            .get("pass_env")
            .is_some_and(Value::is_array)
    {
        return Err(Error::new(
            "invalid",
            "frozen profile pass_env must be an array of names",
        ));
    }
    let mut environment = build_process_environment(&agent.profile_snapshot)?;
    insert_environment(&mut environment, "ELIOT_AGENT_ID", agent.id.clone());
    insert_environment(
        &mut environment,
        "ELIOT_AGENT_GENERATION",
        agent.generation.to_string(),
    );
    insert_environment(
        &mut environment,
        "ELIOT_AGENT_TOKEN",
        agent_token.to_owned(),
    );
    Ok(Value::Object(environment))
}

fn insert_environment(environment: &mut Map<String, Value>, name: &str, value: String) {
    #[cfg(windows)]
    if let Some(existing) = environment
        .keys()
        .find(|existing| existing.eq_ignore_ascii_case(name))
        .cloned()
    {
        environment.insert(existing, Value::String(value));
        return;
    }
    environment.insert(name.to_owned(), Value::String(value));
}

fn valid_environment_name(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some('_' | 'A'..='Z' | 'a'..='z'))
        && chars.all(|character| character == '_' || character.is_ascii_alphanumeric())
}

fn deadline() -> Instant {
    Instant::now() + RPC_DEADLINE
}

async fn wait_child(
    child: &mut OwnedChild,
    duration: Duration,
    monitoring: &Monitoring,
    agent: &Agent,
) -> bool {
    if !matches!(
        timeout_at(Instant::now() + duration, child.wait()).await,
        Ok(Ok(_))
    ) {
        return false;
    }
    let _ = monitoring.sample_generation(agent).await;
    #[cfg(windows)]
    {
        if matches!(child.wait_family_empty(duration).await, Ok(true)) {
            // Consume the OS observer's verified-empty certificate before the
            // owning Job is dropped; exited database rows alone are not proof.
            let _ = monitoring.sample_generation(agent).await;
            true
        } else {
            false
        }
    }
    #[cfg(not(windows))]
    {
        true
    }
}

async fn terminate_child(mut child: OwnedChild, monitoring: &Monitoring, agent: &Agent) {
    if child.terminate().is_ok() && wait_child(&mut child, STOP_GRACE, monitoring, agent).await {
        return;
    }
    drop(child);
}

fn custody_label(guarantee: crate::platform::CustodyGuarantee) -> &'static str {
    match guarantee {
        #[cfg(windows)]
        crate::platform::CustodyGuarantee::Exact => "exact",
        #[cfg(unix)]
        crate::platform::CustodyGuarantee::Degraded => "degraded",
        #[cfg(not(windows))]
        crate::platform::CustodyGuarantee::Unknown => "unknown",
    }
}

// The loss transaction preserves terminal projections but does not record gaps.
// Store serialization lets a terminal fact committed during cancellation win.
async fn record_root_exit_gap(state: &Weak<HostState>, key: &AgentKey, reason: &str) -> Result<()> {
    let state = state
        .upgrade()
        .ok_or_else(|| Error::new("unavailable", "plugin host is unavailable"))?;
    let mut store = state.store.lock().await;
    let agent = store
        .get_agent(&key.id)?
        .ok_or_else(|| Error::new("not_found", "exited plugin Agent was not found"))?;
    if agent.generation != key.generation {
        return Err(Error::new(
            "stale_generation",
            "exited plugin generation is no longer current",
        ));
    }
    if agent.is_terminal() {
        return Ok(());
    }
    let source = store.read_source_state(&key.id, key.generation, "stream")?;
    let families = serde_json::from_value(
        source
            .get("families")
            .cloned()
            .ok_or_else(|| Error::new("source_integrity", "stream has no owned families"))?,
    )
    .map_err(|_| Error::new("source_integrity", "stream families are malformed"))?;
    let change = store.record_source_gap(
        SourceGap {
            gap_id: format!("host-root-exit:{}:{}", key.id, key.generation),
            agent_id: key.id.clone(),
            generation: key.generation,
            source: "stream".to_owned(),
            families,
            reason: reason.to_owned(),
            from: source
                .get("cursor")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            to: None,
        },
        now_ms(),
    )?;
    state.bus.publish(&change.events);
    Ok(())
}

async fn mark_lost_state(state: &Weak<HostState>, key: &AgentKey) {
    let Some(state) = state.upgrade() else {
        return;
    };
    fence_runtime_generation(&state, key);
    {
        let mut store = state.store.lock().await;
        if let Ok(events) = store.mark_lost(&key.id, key.generation, now_ms()) {
            state.bus.publish(&events);
            note_unknown_commands(&state, &events);
        }
    }
    forget_if_revoked(&state, key).await;
    signal_progress(&state);
}

fn signal_progress(state: &HostState) {
    state.progress.send_modify(|value| {
        *value = value.wrapping_add(1);
    });
}

async fn forget_if_revoked(state: &HostState, key: &AgentKey) {
    let token = {
        let store = state.store.lock().await;
        store.get_agent_token(&key.id, key.generation)
    };
    if matches!(token, Ok(None)) {
        forget_revoked_agent_token(
            &state.agent_credentials,
            &state.secrets,
            &(key.id.clone(), key.generation),
        );
    }
}

fn note_unknown_commands(state: &HostState, events: &[eliot_core::Event]) {
    let mut unknown = state
        .unknown_commands
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    for event in events
        .iter()
        .filter(|event| event.kind == "command.unknown")
    {
        if let Some(command_id) = event.data.get("command_id").and_then(Value::as_str) {
            unknown.insert(command_id.to_owned());
        }
    }
}

async fn cleanup_actor(
    state: &Weak<HostState>,
    key: &AgentKey,
    plugin_credential: &str,
    _planned_exit: bool,
) {
    let Some(state) = state.upgrade() else {
        return;
    };
    fence_runtime_generation(&state, key);
    let mut secrets = state
        .secrets
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if !plugin_credential.is_empty() {
        secrets.remove(plugin_credential);
    }
}

// Stop new command routing without revoking authorization for buffered source facts.
fn fence_command_routing(state: &HostState, key: &AgentKey) {
    state
        .running
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(key);
}

fn fence_runtime_generation(state: &HostState, key: &AgentKey) {
    fence_command_routing(state, key);
    let credential = state
        .agent_credentials
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(&(key.id.clone(), key.generation));
    if let Some(credential) = credential {
        state
            .secrets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&credential);
    }
}

pub(crate) fn forget_revoked_agent_token(
    credentials: &AgentCredentials,
    secrets: &SecretSet,
    key: &(String, i64),
) {
    let removed = credentials
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(key);
    let Some(token) = removed else {
        return;
    };
    secrets
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(&token);
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

pub(crate) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}
