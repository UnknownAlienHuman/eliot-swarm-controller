use super::{PluginClient, PluginConnection};
use crate::bus::Bus;
use crate::platform::OwnedChild;
use eliot_core::{Agent, Command, Error, Result, SourceBatch};
use eliot_plugin::PluginManifest;
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
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

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct AgentKey {
    id: String,
    generation: i64,
}

struct RuntimeEntry {
    sender: mpsc::Sender<RuntimeMessage>,
}

enum RuntimeMessage {
    Send {
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
    store: StoreHandle,
    bus: Bus,
    secrets: SecretSet,
    agent_credentials: AgentCredentials,
    unknown_commands: UnknownCommands,
    progress: watch::Sender<u64>,
    late_batch_causes: Mutex<HashMap<AgentKey, String>>,
    running: Mutex<HashMap<AgentKey, RuntimeEntry>>,
}

struct AgentActorConfig {
    state: Weak<HostState>,
    key: AgentKey,
    agent: Agent,
    start_command: Command,
    instance_dir: String,
    plugin_credential: String,
    agent_token: String,
}

struct EventSource {
    store: StoreHandle,
    bus: Bus,
    secrets: SecretSet,
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
    pub(crate) fn new(
        data_root: PathBuf,
        store: StoreHandle,
        bus: Bus,
        secrets: SecretSet,
        agent_credentials: AgentCredentials,
        unknown_commands: UnknownCommands,
        progress: watch::Sender<u64>,
    ) -> Self {
        Self {
            state: Arc::new(HostState {
                data_root,
                store,
                bus,
                secrets,
                agent_credentials,
                unknown_commands,
                progress,
                late_batch_causes: Mutex::new(HashMap::new()),
                running: Mutex::new(HashMap::new()),
            }),
        }
    }

    pub(crate) async fn start(&self, agent: Agent, command: Command) -> Result<Value> {
        let workspace = agent
            .workspace
            .as_deref()
            .ok_or_else(|| Error::new("invalid", "fake launch requires a workspace"))?;
        if !Path::new(workspace).is_dir() {
            return Err(Error::new(
                "invalid",
                "fake launch workspace must be an existing directory",
            ));
        }
        if agent.profile != "fake" || agent.harness != "fake" || agent.generation <= 0 {
            return Err(Error::new(
                "unsupported",
                "L1 plugin host only starts the fake harness and profile",
            ));
        }
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
        let spawned = self.spawn_plugin(&agent, &instance_dir);
        let mut child = match spawned {
            Ok(child) => child,
            Err(error) => return Err(error),
        };
        self.state
            .secrets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(credential.clone());
        let stdin = child.take_stdin();
        let stdout = child.take_stdout();
        let stderr = child.take_stderr();
        let (Some(stdin), Some(stdout), Some(stderr)) = (stdin, stdout, stderr) else {
            terminate_child(child).await;
            self.remove_plugin_secret(&credential);
            return Err(Error::new(
                "process_pipes",
                "fake plugin did not expose all three owned pipes",
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
                secrets: Arc::clone(&self.state.secrets),
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
            running.insert(actor_key.clone(), RuntimeEntry { sender: message_tx });
        }
        let actor = AgentActorConfig {
            state: Arc::downgrade(&self.state),
            key: actor_key,
            agent: agent.clone(),
            start_command: command.clone(),
            instance_dir: instance_dir.to_string_lossy().into_owned(),
            plugin_credential: credential.clone(),
            agent_token: agent_token.clone(),
        };
        tokio::spawn(async move {
            run_agent(actor, child, client, event_task, message_rx, started_tx).await;
        });

        match timeout_at(Instant::now() + RPC_DEADLINE + RPC_GRACE, started_rx).await {
            Ok(Ok(Ok(result))) => Ok(result),
            Ok(Ok(Err(error))) => Err(error),
            Ok(Err(_)) => Err(Error::new(
                "unknown",
                "fake plugin ended before start readiness was established",
            )),
            Err(_) => Err(Error::new(
                "unknown",
                "fake plugin start exceeded its absolute deadline",
            )),
        }
    }

    pub(crate) async fn send(&self, agent: &Agent, command: Command) -> Result<Value> {
        let key = AgentKey {
            id: agent.id.clone(),
            generation: agent.generation,
        };
        let sender = self.sender(&key)?;
        let (reply, response) = oneshot::channel();
        timeout_at(
            Instant::now() + RPC_DEADLINE,
            sender.send(RuntimeMessage::Send { command, reply }),
        )
        .await
        .map_err(|_| Error::new("busy", "fake plugin command queue is full"))?
        .map_err(|_| Error::new("unknown", "owned fake plugin is no longer available"))?;
        match timeout_at(Instant::now() + RPC_DEADLINE + RPC_GRACE, response).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(Error::new(
                "unknown",
                "fake plugin ended before the command outcome was returned",
            )),
            Err(_) => Err(Error::new(
                "unknown",
                "fake plugin command exceeded its absolute deadline",
            )),
        }
    }

    pub(crate) async fn stop(&self, agent: &Agent, command: Command, force: bool) -> Result<Value> {
        let key = AgentKey {
            id: agent.id.clone(),
            generation: agent.generation,
        };
        let sender = self.sender(&key)?;
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
        .map_err(|_| Error::new("busy", "fake plugin command queue is full"))?
        .map_err(|_| Error::new("unknown", "owned fake plugin is no longer available"))?;
        match timeout_at(
            Instant::now() + RPC_DEADLINE + STOP_GRACE + STOP_GRACE + RPC_GRACE,
            response,
        )
        .await
        {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(Error::new(
                "unknown",
                "fake plugin ended before stop custody was settled",
            )),
            Err(_) => Err(Error::new(
                "unknown",
                "fake plugin stop exceeded its absolute deadline",
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

    fn sender(&self, key: &AgentKey) -> Result<mpsc::Sender<RuntimeMessage>> {
        self.state
            .running
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(key)
            .map(|entry| entry.sender.clone())
            .ok_or_else(|| {
                Error::new(
                    "not_found",
                    format!(
                        "no owned plugin exists for {} generation {}",
                        key.id, key.generation
                    ),
                )
            })
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
            .ok_or_else(|| Error::new("invalid", "fake launch requires a workspace"))?;
        let mut command = ProcessCommand::new(executable);
        command
            .arg("--data-dir")
            .arg(&self.state.data_root)
            .arg("plugin")
            .arg("fake")
            .current_dir(workspace)
            .env_clear()
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
        key,
        agent,
        start_command,
        instance_dir,
        plugin_credential,
        agent_token,
    } = actor;
    let mut last_command_id = start_command.id.clone();
    let init = client
        .call(
            "initialize",
            json!({
                "protocol": "eliot-plugin/1",
                "credential": plugin_credential,
                "instance_dir": instance_dir,
                "config": {"harness": "fake"},
            }),
            deadline(),
        )
        .await;
    let init = match init {
        Ok(value) => value,
        Err(error) => {
            let _ = started.send(Err(error));
            terminate_child(child).await;
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
                "fake plugin returned a malformed manifest",
            )));
            terminate_child(child).await;
            let _ = event_task.await;
            cleanup_actor(&state, &key, &plugin_credential, false).await;
            return;
        }
    };
    if !valid_fake_manifest(&manifest) {
        let _ = started.send(Err(Error::new(
            "unsupported",
            "plugin manifest does not advertise the implemented fake capabilities",
        )));
        terminate_child(child).await;
        let _ = event_task.await;
        cleanup_actor(&state, &key, &plugin_credential, false).await;
        return;
    }

    let start_params = json!({
        "agent_id": agent.id,
        "generation": agent.generation,
        "command_id": start_command.id,
        "profile": agent.profile,
        "workspace": agent.workspace,
        "target_environment": {
            "ELIOT_AGENT_ID": agent.id,
            "ELIOT_AGENT_GENERATION": agent.generation.to_string(),
            "ELIOT_AGENT_TOKEN": agent_token,
        },
        "note": null,
        "source_plan": {
            "session": "stream",
            "turn": "stream",
            "message": "stream",
        },
        "source_state": null,
        "resume_from": null,
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
                terminate_child(child).await;
                let _ = event_task.await;
                cleanup_actor(&state, &key, &plugin_credential, false).await;
                return;
            }
        }
        Err(error) => {
            let uncertain = error.code == "unknown";
            let _ = started.send(Err(error));
            if !uncertain {
                terminate_child(child).await;
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
                let _ = status;
                let _ = event_task.await;
                if !planned_exit {
                    mark_lost_state(&state, &key).await;
                }
                break;
            }
            event_result = &mut event_task => {
                let _ = event_result;
                mark_lost_state(&state, &key).await;
                if child.terminate().is_ok() && wait_child(&mut child, STOP_GRACE).await {
                    planned_exit = true;
                } else {
                    drop(child);
                    planned_exit = true;
                }
                break;
            }
            message = messages.recv() => {
                let Some(message) = message else {
                    break;
                };
                match message {
                    RuntimeMessage::Send { command, reply } => {
                        last_command_id = command.id.clone();
                        let args = command.args.clone();
                        let params = json!({
                            "agent_id": agent.id,
                            "generation": agent.generation,
                            "command_id": command.id,
                            "verb": "send",
                            "args": args,
                        });
                        let result = client.call("agent.command", params, deadline()).await
                            .and_then(validate_send_result);
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
                                let graceful = wait_child(&mut child, STOP_GRACE).await;
                                let mut exited = graceful;
                                if !exited {
                                    let _ = child.terminate();
                                    exited = wait_child(&mut child, STOP_GRACE).await;
                                }
                                if !exited {
                                    drop(child);
                                    let _ = event_task.await;
                                    if let Some(state) = state.upgrade() {
                                        fence_runtime_generation(&state, &key);
                                    }
                                    planned_exit = true;
                                    let _ = reply.send(Ok(json!({
                                        "state": "applied",
                                        "custody": "unknown",
                                        "graceful": false,
                                        "native": result,
                                    })));
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
                                let exited = wait_child(&mut child, STOP_GRACE).await;
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
                        let exited = wait_child(&mut child, STOP_GRACE).await;
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
        secrets,
        state,
        key,
        agent_id,
        generation,
        expected_credential,
    } = source;
    let client = connection.client();
    while let Some(request) = connection.next_request().await {
        let request_id = request.id.clone();
        let request_deadline = deadline();
        let result = if request.method != "events.put" {
            Err(Error::new(
                "unsupported",
                "plugin host only accepts events.put in the L1 harness",
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
                Err(late_batch_error(
                    &state,
                    &agent_id,
                    generation,
                    "plugin_credential_invalid",
                ))
            } else {
                match serde_json::from_value::<SourceBatch>(params) {
                    Ok(batch) if batch.agent_id == agent_id && batch.generation == generation => {
                        let contains_secret = serde_json::to_value(&batch)
                            .ok()
                            .is_some_and(|value| contains_known_secret(&value, &secrets));
                        if contains_secret {
                            Err(Error::new(
                                "secret_in_payload",
                                "durable plugin event contains a known credential",
                            ))
                        } else {
                            match timeout_at(request_deadline, store.lock()).await {
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
                                        Err(late_batch_error(&state, &agent_id, generation, &cause))
                                    }
                                },
                                Err(_) => Err(late_batch_error(
                                    &state,
                                    &agent_id,
                                    generation,
                                    "store_lock_deadline",
                                )),
                            }
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

fn valid_fake_manifest(manifest: &PluginManifest) -> bool {
    manifest.name == "fake"
        && manifest.protocol == "eliot-plugin/1"
        && manifest.available
        && manifest.mode == "per_agent"
        && manifest.verbs.len() == 3
        && manifest.child_verbs.is_empty()
        && !manifest.features.start_requires_input
        && !manifest.features.native_children
        && ["launch", "send", "stop"]
            .iter()
            .all(|verb| manifest.verbs.iter().any(|actual| actual == verb))
}

fn deadline() -> Instant {
    Instant::now() + RPC_DEADLINE
}

async fn wait_child(child: &mut OwnedChild, duration: Duration) -> bool {
    matches!(
        timeout_at(Instant::now() + duration, child.wait()).await,
        Ok(Ok(_))
    )
}

async fn terminate_child(mut child: OwnedChild) {
    if child.terminate().is_ok() && wait_child(&mut child, STOP_GRACE).await {
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

fn fence_runtime_generation(state: &HostState, key: &AgentKey) {
    state
        .running
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(key);
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
