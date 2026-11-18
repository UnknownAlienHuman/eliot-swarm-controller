use super::{PluginClient, PluginConnection};
mod actor;
mod discovery;
mod native_children;
mod observation;
mod reader;
mod source_handoff;
mod source_intake;
use crate::bus::Bus;
use crate::common::{now_ms, valid_environment_name};
use crate::monitor::Monitoring;
use crate::platform::OwnedChild;
use crate::platform::custody::AgentSpawnError;
use actor::run_agent_inner;
use eliot_core::{
    Agent, Command, ContextItem, Error, InjectArgs, InjectPoint, Result, SecretPolicy, SourceBatch,
    SourceGap, SourceGapAck, StderrSource,
};
use eliot_plugin::{PluginManifest, StderrCapture, StderrView, spawn_plugin_stderr, sync::LockExt};

use serde_json::{Map, Value, json};
use source_intake::serve_plugin_requests;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    ffi::OsString,
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    process::Stdio,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    process::Command as ProcessCommand,
    sync::{Mutex as AsyncMutex, Semaphore, mpsc, oneshot, watch},
    time::{Instant, timeout_at},
};
use ulid::Ulid;

const RPC_DEADLINE: Duration = Duration::from_secs(30);
/// Bound for one `agent.start` round trip and its outer absolute wait.
/// Cold native startup is sequential inside that single RPC: credential
/// transfer (up to its own 30 s budget) then serve readiness (up to its own
/// 30 s) then the session phases. Measured on this machine: ~10 s hot-cache
/// launch-to-ready, ~20 s warm, and a true-cold OpenCode start that exceeded
/// the old 30 s RPC (`plugin_rpc_timeout`, unknown launch). 90 s covers the
/// two sequential 30 s cold budgets plus the session chain; every per-phase
/// inner deadline still fires first on a genuine wedge, so a dead start
/// still settles unknown instead of pinning the launch.
const START_TIMEOUT: Duration = Duration::from_secs(90);
const RPC_GRACE: Duration = Duration::from_secs(2);
const STOP_GRACE: Duration = Duration::from_secs(5);
/// After the plugin leader exited on its own, how long the rest of its
/// family (native descendants) may take to follow before the generation's
/// Job is terminated. Ordinary children exit with their parent well within
/// this; a detached daemon a native tool started (`git fsmonitor--daemon`)
/// never does, and waiting the full `STOP_GRACE` for it held every graceful
/// stop, and so `agent.delete`, for 5 s.
const FAMILY_GRACE: Duration = Duration::from_secs(1);
const MAX_PENDING_SNAPSHOT_READS: usize = 1;

#[derive(Debug)]
pub(crate) enum StartError {
    NotSpawned(Error),
    Uncertain(Error),
}

#[derive(Clone, Copy)]
struct StopDeadlines {
    queued_until: Instant,
    rpc_until: Instant,
    custody_until: Instant,
    source_until: Instant,
    reply_until: Instant,
}

impl StopDeadlines {
    fn new(now: Instant) -> Self {
        let queued_until = now + RPC_DEADLINE;
        // agent.stop + shutdown; the force snapshot fits the second RPC window.
        let rpc_until = queued_until
            + eliot_plugin::native_lifecycle::STOP_INTERRUPT_TIMEOUT
            + RPC_DEADLINE
            + RPC_DEADLINE;
        // close_input, then two attempts, each with leader and family waits.
        let custody_until = rpc_until + STOP_GRACE + (STOP_GRACE + STOP_GRACE) * 2;
        // Callback drain/abort-join and one bounded Store settlement.
        let source_until = custody_until + STOP_GRACE + RPC_GRACE + RPC_DEADLINE;
        let reply_until = source_until + RPC_GRACE;
        Self {
            queued_until,
            rpc_until,
            custody_until,
            source_until,
            reply_until,
        }
    }

    fn rpc_deadline(self, budget: Duration) -> Instant {
        self.rpc_deadline_at(Instant::now(), budget)
    }

    fn rpc_deadline_at(self, now: Instant, budget: Duration) -> Instant {
        (now + budget).min(self.rpc_until)
    }
}

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

/// Own loss suppression until the force command's Store settlement commits.
/// Actor teardown may finish first, so retain its proven loss for cancellation.
pub(crate) struct ForceStopGuard {
    state: Weak<HostState>,
    key: AgentKey,
    deferred_loss: Arc<AtomicBool>,
    committed: bool,
}

impl ForceStopGuard {
    // Caller holds the Store lock, also used by mark_lost_state.
    #[cfg(any(windows, test))]
    fn new(state: &Arc<HostState>, key: AgentKey) -> Result<Self> {
        let deferred_loss = Arc::new(AtomicBool::new(false));
        let mut planned = state.planned_stops.lock_recover();
        if planned.contains_key(&key) {
            return Err(Error::new("unknown", "force-stop is already settling"));
        }
        planned.insert(key.clone(), Arc::clone(&deferred_loss));
        Ok(Self {
            state: Arc::downgrade(state),
            key,
            deferred_loss,
            committed: false,
        })
    }

    pub(crate) fn finish(mut self) {
        self.committed = true;
    }
}

impl Drop for ForceStopGuard {
    fn drop(&mut self) {
        let Some(state) = self.state.upgrade() else {
            return;
        };
        // Clear synchronously even when the enclosing future is cancelled.
        // Only this attempt owns the entry; actor cleanup must not remove it.
        state.planned_stops.lock_recover().remove(&self.key);
        if !self.committed
            && self.deferred_loss.load(Ordering::Acquire)
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            let key = self.key.clone();
            runtime.spawn(async move {
                mark_lost_state(&Arc::downgrade(&state), &key).await;
            });
        }
        // Without a runtime, the persisted exit/drain facts remain available
        // to restart reconciliation; Drop must not panic trying to spawn.
    }
}

struct RuntimeEntry {
    sender: mpsc::Sender<RuntimeMessage>,
    start_command_id: String,
    verbs: HashSet<String>,
    stderr: StderrView,
    source_credentials: Arc<[String]>,
}

enum RuntimeMessage {
    Command {
        command: Command,
        dispatch_seq: Option<i64>,
        reply: oneshot::Sender<Result<Value>>,
    },
    Snapshot {
        agent_id: String,
        generation: i64,
        command_id: String,
        turn_id: Option<String>,
        reply: oneshot::Sender<Result<Value>>,
    },
    Stop {
        command: Command,
        turn_id: Option<String>,
        force: bool,
        deadlines: StopDeadlines,
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
    planned_stops: Mutex<HashMap<AgentKey, Arc<AtomicBool>>>,
    startup_gate: AsyncMutex<()>,
    startup_closed: AtomicBool,
    observers: Mutex<HashMap<AgentKey, Arc<observation::ObserverEntry>>>,
    drift_warned: Mutex<HashSet<(String, String)>>,
    readers: Mutex<HashMap<String, discovery::Reader>>,
    discovery_closed: AtomicBool,
    reader_slots: Arc<Semaphore>,
    launch_reader_slots: Arc<Semaphore>,
    #[cfg(test)]
    snapshot_turn_calls: AtomicUsize,
    #[cfg(test)]
    test_controlled: Mutex<HashSet<AgentKey>>,
}

struct AgentActorConfig {
    state: Weak<HostState>,
    monitoring: Monitoring,
    key: AgentKey,
    agent: Agent,
    start_command: Command,
    instance_dir: String,
    store_root: PathBuf,
    plugin_credential: String,
    target_environment: Value,
    source_credentials: Arc<[String]>,
    resume_from: Option<Value>,
    source_state: Option<Value>,
    stderr: StderrView,
}

struct PendingFailure {
    reply: Option<oneshot::Sender<Result<Value>>>,
    error: Error,
    command_id: String,
}

struct PluginCommandCompletion {
    command: Command,
    reply: oneshot::Sender<Result<Value>>,
    result: Result<Value>,
}

struct PluginSnapshotCompletion {
    agent_id: String,
    generation: i64,
    command_id: String,
    reply: oneshot::Sender<Result<Value>>,
    result: Result<Value>,
}

type PendingSteerFuture = Pin<Box<dyn Future<Output = PluginCommandCompletion> + Send>>;
type PendingSnapshotFuture = Pin<Box<dyn Future<Output = PluginSnapshotCompletion> + Send>>;

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
    observation: Option<Agent>,
}

#[derive(Clone)]
pub(crate) struct PluginHost {
    state: Arc<HostState>,
}

fn resume_source_generation(command: &Command) -> Result<i64> {
    let dispatched_generation = command
        .generation
        .ok_or_else(|| Error::new("stale_generation", "resume has no dispatched generation"))?;
    if dispatched_generation <= 1 {
        return Err(Error::new(
            "stale_generation",
            "resume requires a positive retained generation",
        ));
    }
    let previous_generation = dispatched_generation
        .checked_sub(1)
        .ok_or_else(|| Error::new("stale_generation", "resume has no previous generation"))?;
    if command
        .expected_generation
        .is_some_and(|expected| expected != previous_generation)
    {
        return Err(Error::new(
            "stale_generation",
            "resume expected generation differs from the dispatched target",
        ));
    }
    Ok(command.expected_generation.unwrap_or(previous_generation))
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
                planned_stops: Mutex::new(HashMap::new()),
                startup_gate: AsyncMutex::new(()),
                startup_closed: AtomicBool::new(false),
                observers: Mutex::new(HashMap::new()),
                drift_warned: Mutex::new(HashSet::new()),
                readers: Mutex::new(HashMap::new()),
                discovery_closed: AtomicBool::new(false),
                reader_slots: Arc::new(Semaphore::new(discovery::MAX_READERS)),
                launch_reader_slots: Arc::new(Semaphore::new(discovery::MAX_LAUNCH_READERS)),
                #[cfg(test)]
                snapshot_turn_calls: AtomicUsize::new(0),
                #[cfg(test)]
                test_controlled: Mutex::new(HashSet::new()),
            }),
        }
    }

    pub(crate) async fn start(
        &self,
        agent: Agent,
        command: Command,
        source_state: Option<Value>,
    ) -> std::result::Result<Value, StartError> {
        let mut may_have_spawned = false;
        self.start_inner(agent, command, source_state, &mut may_have_spawned).await.map_err(|error| {
            if may_have_spawned {
                StartError::Uncertain(error)
            } else {
                StartError::NotSpawned(error.with_where("swarm.start.pre_spawn").with_origin(
                    eliot_core::FailureOrigin::Swarm,
                    "Swarm refused this generation before creating any OS child; no native effect was dispatched",
                ))
            }
        })
    }

    async fn start_inner(
        &self,
        mut agent: Agent,
        command: Command,
        mut source_state: Option<Value>,
        may_have_spawned: &mut bool,
    ) -> Result<Value> {
        let launch_started = Instant::now();
        if agent.observe_only {
            return Err(Error::new(
                "not_allowed",
                "an observed Agent cannot enter controlled startup",
            ));
        }
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
        if !matches!(
            agent.harness.as_str(),
            "codex" | "muse" | "claude" | "opencode" | "antigravity"
        ) || agent.generation <= 0
        {
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
            let resume_generation = resume_source_generation(&command)?;
            if retained_generation != Some(resume_generation) {
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
        if agent.harness == "antigravity" {
            let cmd = agent.profile_snapshot["cmd"].as_str().unwrap_or("agy");
            let deadline = std::time::Instant::now() + crate::adhoc::DISCOVERY_BUDGET;
            let (program, _) =
                crate::adhoc::reader_executable(cmd, "antigravity", deadline).await?;
            agent.profile_snapshot["cmd"] = Value::String(program);
        }
        let key = AgentKey {
            id: agent.id.clone(),
            generation: agent.generation,
        };
        {
            let running = self.state.running.lock_recover();
            if running.contains_key(&key) {
                *may_have_spawned = true;
                return Err(Error::new(
                    "bad_state",
                    "an owned plugin already exists for this Agent generation",
                ));
            }
        }

        // Validate the committed credential and frozen environment before
        // creating anything on disk; both are pure reads of admitted state.
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
        let mut target_environment =
            build_target_environment(&self.state.data_root, &agent, &agent_token)?;
        if let Some(root) = self
            .state
            .store
            .lock()
            .await
            .agent_artifact_root(&agent.id, agent.generation)?
        {
            target_environment
                .as_object_mut()
                .ok_or_else(|| Error::new("invalid", "target environment is not an object"))?
                .insert("ELIOT_ARTIFACT_ROOT".to_owned(), Value::String(root.path));
        }
        // Cleanup closes admission, then waits for this gate before snapshotting
        // running. Cover the first filesystem effect through child ownership
        // handoff, including the awaits before registration; never hold Store.
        let prepared_at = Instant::now();
        let startup = self.state.startup_gate.lock().await;
        let gate_acquired_at = Instant::now();
        if self.state.startup_closed.load(Ordering::SeqCst) {
            return Err(Error::new(
                "native_not_dispatched",
                "controlled startup is closed",
            ));
        }
        // Another startup may have registered while this one waited for the gate.
        if self.state.running.lock_recover().contains_key(&key) {
            *may_have_spawned = true;
            return Err(Error::new(
                "bad_state",
                "an owned plugin already exists for this Agent generation",
            ));
        }
        let (instance_dir, credential) = self.create_instance_dir(&key)?;
        let source_credentials = source_secret_snapshot(
            &agent.profile_snapshot,
            &target_environment,
            vec![
                self.state.owner_token.clone(),
                agent_token.clone(),
                credential.clone(),
            ],
        );
        if let Some(source_state) = source_state.as_mut() {
            crate::redaction::redact_value(source_state, &source_credentials);
        }
        {
            let mut credentials = self.state.agent_credentials.lock_recover();
            credentials.insert((key.id.clone(), key.generation), agent_token.clone());
            self.state
                .secrets
                .lock_recover()
                .insert(agent_token.clone());
        }
        // The generated directory is never given to the child on these paths
        // (spawn ignores it; the path is handed over only in the later
        // initialize RPC), so empty-only removal is safe. On Windows a failed
        // spawn may briefly create a suspended child, but it is terminated
        // before executing and never receives this path. Remove exactly the
        // generated directory, never recursively.
        let monitoring_started = Instant::now();
        if let Err(error) = self.state.monitoring.prepare_generation(&agent).await {
            remove_empty_instance_dir(&instance_dir);
            return Err(error);
        }
        let spawn_started = Instant::now();
        let spawned = self.spawn_plugin(&agent, &instance_dir).await;
        let mut child = match spawned {
            Ok(child) => {
                *may_have_spawned = true;
                child
            }
            Err(error) => {
                remove_empty_instance_dir(&instance_dir);
                return Err(match error {
                    AgentSpawnError::NotSpawned(error) => error,
                    AgentSpawnError::Uncertain(error) => {
                        *may_have_spawned = true;
                        error
                    }
                });
            }
        };
        let spawned_at = Instant::now();
        let observer = match child.observer() {
            Ok(observer) => observer,
            Err(error) => {
                let exited = terminate_child(child, &self.state.monitoring, &agent).await;
                settle_owned_source_exit(
                    &Arc::downgrade(&self.state),
                    &key,
                    None,
                    exited,
                    "plugin_startup_exit",
                )
                .await?;
                return Err(error);
            }
        };
        if let Err(error) = self.state.monitoring.attach(&agent, observer).await {
            let exited = terminate_child(child, &self.state.monitoring, &agent).await;
            settle_owned_source_exit(
                &Arc::downgrade(&self.state),
                &key,
                None,
                exited,
                "plugin_startup_exit",
            )
            .await?;
            return Err(error);
        }
        self.state.secrets.lock_recover().insert(credential.clone());
        let stdin = child.take_stdin();
        let stdout = child.take_stdout();
        let stderr = child.take_stderr();
        let (Some(stdin), Some(stdout), Some(stderr)) = (stdin, stdout, stderr) else {
            let exited = terminate_child(child, &self.state.monitoring, &agent).await;
            let source_exit = settle_owned_source_exit(
                &Arc::downgrade(&self.state),
                &key,
                None,
                exited,
                "plugin_startup_exit",
            )
            .await;
            self.remove_plugin_secret(&credential);
            source_exit?;
            return Err(Error::new(
                "process_pipes",
                "Agent plugin did not expose all three owned pipes",
            ));
        };

        let telemetry_policy = Arc::new(SecretPolicy::new(source_credentials.iter()));
        let debug_policy = Arc::clone(&telemetry_policy);
        let debug_agent = agent.id.clone();
        let debug_generation = agent.generation as u64;
        let stderr_capture = spawn_plugin_stderr(stderr, telemetry_policy, move |line| {
            if crate::logging::is_debug()
                && let Some(record) = eliot_plugin::parse_debug_line(line)
            {
                let mut ids: Vec<_> = record
                    .ids
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_str()))
                    .collect();
                ids.retain(|(key, _)| *key != "agent_id" && *key != "generation");
                ids.push(("agent_id", debug_agent.as_str()));
                let mut fields: Vec<_> = record
                    .fields
                    .iter()
                    .map(|(k, v)| (k.as_str(), *v))
                    .collect();
                fields.retain(|(key, _)| *key != "generation");
                fields.push(("generation", debug_generation));
                crate::logging::record_debug(&debug_policy, &record.code, &ids, &fields);
            }
        });
        let stderr_view = stderr_capture.view();

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
                observation: None,
            },
        ));
        let (message_tx, message_rx) = mpsc::channel(16);
        let (started_tx, started_rx) = oneshot::channel();
        let actor_key = key.clone();
        {
            let mut running = self.state.running.lock_recover();
            running.insert(
                actor_key.clone(),
                RuntimeEntry {
                    sender: message_tx,
                    start_command_id: command.id.clone(),
                    verbs: ["launch", "send", "stop"]
                        .into_iter()
                        .map(str::to_owned)
                        .collect(),
                    stderr: stderr_view.clone(),
                    source_credentials: Arc::clone(&source_credentials),
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
            store_root: self.state.data_root.join("plugins"),
            plugin_credential: credential.clone(),
            target_environment,
            source_credentials: Arc::clone(&source_credentials),
            resume_from,
            source_state,
            stderr: stderr_view.clone(),
        };
        tokio::spawn(async move {
            run_agent(
                actor,
                child,
                client,
                event_task,
                message_rx,
                started_tx,
                stderr_capture,
            )
            .await;
        });
        drop(startup);
        let registered_at = Instant::now();

        let result = match timeout_at(Instant::now() + START_TIMEOUT + RPC_GRACE, started_rx).await
        {
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
        };
        let millis =
            |duration: std::time::Duration| duration.as_millis().min(u128::from(u64::MAX)) as u64;
        crate::logging::record_debug(
            &SecretPolicy::new(source_credentials.iter()),
            "agent.start_timing",
            &[("agent_id", &agent.id), ("command_id", &command.id)],
            &[
                ("prepare_ms", millis(prepared_at - launch_started)),
                ("gate_wait_ms", millis(gate_acquired_at - prepared_at)),
                ("instance_ms", millis(monitoring_started - gate_acquired_at)),
                ("monitoring_ms", millis(spawn_started - monitoring_started)),
                ("spawn_ms", millis(spawned_at - spawn_started)),
                ("register_ms", millis(registered_at - spawned_at)),
                ("ready_wait_ms", millis(registered_at.elapsed())),
                ("elapsed_ms", millis(launch_started.elapsed())),
            ],
        );
        result.map_err(|error| diagnose_error(error, &stderr_view, &source_credentials))
    }

    pub(crate) async fn command(
        &self,
        agent: &Agent,
        command: Command,
        dispatch_seq: Option<i64>,
    ) -> Result<Value> {
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
        let (sender, stderr, credentials) = self.sender(&key, &command.verb)?;
        let wait_for_steer = command.verb == "steer";
        let (reply, response) = oneshot::channel();
        timeout_at(
            Instant::now() + RPC_DEADLINE,
            sender.send(RuntimeMessage::Command {
                command,
                dispatch_seq,
                reply,
            }),
        )
        .await
        .map_err(|_| Error::new("busy", "Agent plugin command queue is full"))?
        .map_err(|_| Error::new("unknown", "owned Agent plugin is no longer available"))?;
        let result = if wait_for_steer {
            match response.await {
                Ok(result) => result,
                Err(_) => Err(Error::new(
                    "unknown",
                    "Agent plugin ended before the steer outcome was returned",
                )),
            }
        } else {
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
        };
        result.map_err(|error| diagnose_error(error, &stderr, &credentials))
    }

    pub(crate) async fn stop(
        &self,
        agent: &Agent,
        command: Command,
        force: bool,
    ) -> Result<(Value, Option<ForceStopGuard>)> {
        if command.verb != "stop"
            || command.target.as_deref() != Some(agent.id.as_str())
            || command.generation != Some(agent.generation)
        {
            return Err(Error::new(
                "stale_generation",
                "stop does not match the owned Agent generation",
            ));
        }
        #[cfg(windows)]
        if force {
            return timeout_at(
                Instant::now() + RPC_DEADLINE + STOP_GRACE + RPC_GRACE,
                self.force_stop_generation(agent, &command),
            )
            .await
            .map_err(|_| Error::new("unknown", "force-stop custody settlement timed out"))?;
        }
        let key = AgentKey {
            id: agent.id.clone(),
            generation: agent.generation,
        };
        let (sender, stderr, credentials) = match self.sender(&key, "stop") {
            Ok(found) => found,
            Err(error) => {
                return Err(if error.code == "not_found" {
                    // This exact-generation lookup happens before enqueue, so no
                    // native stop was dispatched for the missing runtime.
                    error.with_code("native_not_dispatched")
                } else {
                    error
                });
            }
        };
        let (reply, response) = oneshot::channel();
        let deadlines = StopDeadlines::new(Instant::now());
        enqueue_stop(
            &sender,
            RuntimeMessage::Stop {
                command,
                turn_id: agent.active_turn.clone(),
                force,
                deadlines,
                reply,
            },
            deadlines.queued_until,
        )
        .await?;
        let result = match timeout_at(deadlines.reply_until, response).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(Error::new(
                "unknown",
                "Agent plugin ended before stop custody was settled",
            )),
            Err(_) => Err(Error::new(
                "unknown",
                "Agent plugin stop exceeded its absolute deadline",
            )),
        };
        result
            .map(|value| (value, None))
            .map_err(|error| diagnose_error(error, &stderr, &credentials))
    }

    /// Read one exact native turn through the owned plugin (`agent.snapshot`
    /// with a turn id). A readback, not a dispatched verb: it never mutates
    /// native state and carries no outcome semantics of its own.
    pub(crate) async fn snapshot_turn(
        &self,
        agent: &Agent,
        command_id: &str,
        turn_id: &str,
    ) -> Result<Value> {
        #[cfg(test)]
        self.state
            .snapshot_turn_calls
            .fetch_add(1, Ordering::Relaxed);
        let key = AgentKey {
            id: agent.id.clone(),
            generation: agent.generation,
        };
        let (sender, stderr, credentials) = self.runtime_sender(&key)?;
        let (reply, response) = oneshot::channel();
        timeout_at(
            Instant::now() + RPC_DEADLINE,
            sender.send(RuntimeMessage::Snapshot {
                agent_id: agent.id.clone(),
                generation: agent.generation,
                command_id: command_id.to_owned(),
                turn_id: Some(turn_id.to_owned()),
                reply,
            }),
        )
        .await
        .map_err(|_| Error::new("busy", "Agent plugin command queue is full"))?
        .map_err(|_| Error::new("unknown", "owned Agent plugin is no longer available"))?;
        let result = match timeout_at(Instant::now() + RPC_DEADLINE + RPC_GRACE, response).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(Error::new(
                "unknown",
                "Agent plugin ended before the turn snapshot was returned",
            )),
            Err(_) => Err(Error::new(
                "unknown",
                "Agent plugin snapshot exceeded its absolute deadline",
            )),
        };
        result.map_err(|error| diagnose_error(error, &stderr, &credentials))
    }

    /// Low-priority read-only status probe. A full runtime queue returns
    /// `busy` immediately so a diagnosis cannot delay a user control.
    pub(crate) async fn probe_snapshot(
        &self,
        agent: &Agent,
        turn: Option<(&str, &str)>,
    ) -> Result<Value> {
        let key = AgentKey {
            id: agent.id.clone(),
            generation: agent.generation,
        };
        let (sender, command_id, stderr, credentials) = {
            let running = self.state.running.lock_recover();
            let entry = running.get(&key).ok_or_else(|| {
                Error::new(
                    "not_found",
                    format!(
                        "no owned plugin exists for {} generation {}",
                        key.id, key.generation
                    ),
                )
            })?;
            (
                entry.sender.clone(),
                turn.map(|(command_id, _)| command_id.to_owned())
                    .unwrap_or_else(|| entry.start_command_id.clone()),
                entry.stderr.clone(),
                Arc::clone(&entry.source_credentials),
            )
        };
        let (reply, response) = oneshot::channel();
        sender
            .try_send(RuntimeMessage::Snapshot {
                agent_id: agent.id.clone(),
                generation: agent.generation,
                command_id,
                turn_id: turn.map(|(_, turn_id)| turn_id.to_owned()),
                reply,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => {
                    Error::new("busy", "Agent plugin command queue is full")
                }
                mpsc::error::TrySendError::Closed(_) => {
                    Error::new("unknown", "owned Agent plugin is no longer available")
                }
            })?;
        let result = match timeout_at(Instant::now() + RPC_DEADLINE + RPC_GRACE, response).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(Error::new(
                "unknown",
                "Agent plugin ended before the status snapshot was returned",
            )),
            Err(_) => Err(Error::new(
                "unknown",
                "Agent plugin status snapshot exceeded its absolute deadline",
            )),
        };
        result.map_err(|error| diagnose_error(error, &stderr, &credentials))
    }

    /// Turn-snapshot attempts so far. Test-only: proves a replayed
    /// resolution returns its stored response without native I/O.
    #[cfg(test)]
    pub(crate) fn snapshot_turn_calls(&self) -> usize {
        self.state.snapshot_turn_calls.load(Ordering::Relaxed)
    }

    pub(crate) fn close_startup_admission(&self) {
        self.state.startup_closed.store(true, Ordering::SeqCst);
    }

    pub(crate) async fn force_all(&self) {
        self.close_startup_admission();
        let entries: Vec<_> = {
            let _startup = self.state.startup_gate.lock().await;
            self.state
                .running
                .lock_recover()
                .iter()
                .map(|(key, entry)| (key.clone(), entry.sender.clone()))
                .collect()
        };
        if !self.cancel_discoveries().await {
            crate::logging::record(
                &SecretPolicy::default(),
                "daemon.metadata_reader_cleanup_unconfirmed",
                &[("source", "discovery")],
                None,
            );
        }
        self.cancel_observers().await;
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
                self.mark_reader_unavailable(&key).await;
            }
        }
    }

    pub(crate) fn live_count(&self) -> usize {
        self.state.running.lock_recover().len()
    }

    pub(crate) fn controls_generation(&self, agent: &str, generation: i64) -> bool {
        let key = AgentKey {
            id: agent.to_owned(),
            generation,
        };
        #[cfg(test)]
        if self.state.test_controlled.lock_recover().contains(&key) {
            return true;
        }
        self.state.running.lock_recover().contains_key(&key)
    }

    /// Mark one generation as plugin-controlled without starting a runtime.
    /// Test-only: lets notice tests drive `offer_notice` end to end through
    /// the ordinary `send` admission.
    #[cfg(test)]
    pub(crate) fn mark_generation_controlled_for_test(&self, agent: &str, generation: i64) {
        self.state.test_controlled.lock_recover().insert(AgentKey {
            id: agent.to_owned(),
            generation,
        });
    }

    pub(crate) fn take_late_batch_cause(&self, agent: &str, generation: i64) -> Option<String> {
        self.state
            .late_batch_causes
            .lock_recover()
            .remove(&AgentKey {
                id: agent.to_owned(),
                generation,
            })
    }

    fn sender(
        &self,
        key: &AgentKey,
        verb: &str,
    ) -> Result<(mpsc::Sender<RuntimeMessage>, StderrView, Arc<[String]>)> {
        self.state
            .running
            .lock_recover()
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
                    Ok((
                        entry.sender.clone(),
                        entry.stderr.clone(),
                        Arc::clone(&entry.source_credentials),
                    ))
                } else {
                    Err(Error::new(
                        "native_not_dispatched",
                        format!("Agent plugin does not advertise {verb}"),
                    ))
                }
            })
    }

    /// Channel to an owned plugin runtime without a harness-verb gate, for
    /// host-level readback (`agent.snapshot`) rather than dispatched verbs.
    fn runtime_sender(
        &self,
        key: &AgentKey,
    ) -> Result<(mpsc::Sender<RuntimeMessage>, StderrView, Arc<[String]>)> {
        self.state
            .running
            .lock_recover()
            .get(key)
            .map(|entry| {
                (
                    entry.sender.clone(),
                    entry.stderr.clone(),
                    Arc::clone(&entry.source_credentials),
                )
            })
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

    fn install_runtime_verbs(
        state: &Weak<HostState>,
        key: &AgentKey,
        verbs: HashSet<String>,
    ) -> Result<()> {
        let state = state
            .upgrade()
            .ok_or_else(|| Error::new("unknown", "plugin host stopped during initialization"))?;
        let mut running = state.running.lock_recover();
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
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            self.create_instance_dir_with(key, |path| {
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            })
        }
        #[cfg(not(unix))]
        {
            self.create_instance_dir_with(key, |_| Ok(()))
        }
    }

    fn create_instance_dir_with(
        &self,
        key: &AgentKey,
        restrict: impl FnOnce(&Path) -> std::io::Result<()>,
    ) -> Result<(PathBuf, String)> {
        let parent = self.state.data_root.join("plugins");
        #[cfg(unix)]
        {
            use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
            std::fs::DirBuilder::new()
                .mode(0o700)
                .recursive(true)
                .create(&parent)
                .map_err(|error| {
                    Error::new(
                        "platform_io",
                        format!("create plugin instance root: {error}"),
                    )
                })?;
            if parent.is_dir() {
                std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o700)).map_err(
                    |error| {
                        Error::new(
                            "platform_io",
                            format!("restrict plugin instance root: {error}"),
                        )
                    },
                )?;
            }
        }
        #[cfg(not(unix))]
        {
            std::fs::create_dir_all(&parent).map_err(|error| {
                Error::new(
                    "platform_io",
                    format!("create plugin instance root: {error}"),
                )
            })?;
        }
        let instance_dir = parent.join(format!("{}-g{}-{}", key.id, key.generation, Ulid::new()));
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            std::fs::DirBuilder::new()
                .mode(0o700)
                .create(&instance_dir)
                .map_err(|error| {
                    Error::new(
                        "platform_io",
                        format!("create plugin instance directory: {error}"),
                    )
                })?;
        }
        #[cfg(not(unix))]
        {
            std::fs::create_dir(&instance_dir).map_err(|error| {
                Error::new(
                    "platform_io",
                    format!("create plugin instance directory: {error}"),
                )
            })?;
        }
        if let Err(error) = restrict(&instance_dir) {
            remove_empty_instance_dir(&instance_dir);
            return Err(Error::new(
                "platform_io",
                format!("restrict plugin instance directory: {error}"),
            ));
        }
        let credential = format!("{}{}", Ulid::new(), Ulid::new());
        Ok((instance_dir, credential))
    }

    async fn spawn_plugin(
        &self,
        agent: &Agent,
        _instance_dir: &Path,
    ) -> std::result::Result<OwnedChild, AgentSpawnError> {
        let workspace = agent.workspace.as_deref().ok_or_else(|| {
            AgentSpawnError::NotSpawned(Error::new("invalid", "Agent launch requires a workspace"))
        })?;
        let namespace = self
            .state
            .store
            .lock()
            .await
            .generation_custody_namespace(&agent.id, agent.generation)
            .map_err(AgentSpawnError::NotSpawned)?
            .ok_or_else(|| {
                AgentSpawnError::NotSpawned(Error::new(
                    "process_custody_unrecorded",
                    "Agent generation has no recorded custody namespace",
                ))
            })?;
        let command = self
            .plugin_command(&agent.harness, Path::new(workspace))
            .map_err(AgentSpawnError::NotSpawned)?;
        OwnedChild::spawn_agent(command, &namespace, &agent.id, agent.generation)
    }

    fn spawn_plugin_process(&self, harness: &str, workspace: &Path) -> Result<OwnedChild> {
        OwnedChild::spawn(self.plugin_command(harness, workspace)?)
    }

    fn plugin_command(&self, harness: &str, workspace: &Path) -> Result<ProcessCommand> {
        let executable = std::env::current_exe().map_err(|error| {
            Error::new("platform_io", format!("locate Eliot executable: {error}"))
        })?;
        let mut command = ProcessCommand::new(executable);
        command
            .arg("--data-dir")
            .arg(&self.state.data_root)
            .arg("plugin")
            .arg(harness)
            .current_dir(workspace)
            .env_clear()
            .envs(minimal_plugin_environment())
            .kill_on_drop(true)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        Ok(command)
    }

    fn remove_plugin_secret(&self, credential: &str) {
        self.state.secrets.lock_recover().remove(credential);
    }

    /// Force is a custody operation, independent of the plugin command queue.
    /// Keep startup serialized through closure so a missing routing entry cannot
    /// race a newly created Job. A fenced actor still drains its source after
    /// the kernel terminates its family; neither EOF nor an absent name alone
    /// permits an Applied result or capacity release.
    #[cfg(windows)]
    async fn force_stop_generation(
        &self,
        agent: &Agent,
        command: &Command,
    ) -> Result<(Value, Option<ForceStopGuard>)> {
        let _startup = self.state.startup_gate.lock().await;
        let key = AgentKey {
            id: agent.id.clone(),
            generation: agent.generation,
        };
        let (namespace, guard) = {
            let store = self.state.store.lock().await;
            let current = store
                .get_agent(&agent.id)?
                .ok_or_else(|| Error::new("not_found", "force-stop Agent was not found"))?;
            let retained = store
                .get_command(&command.id)?
                .ok_or_else(|| Error::new("not_found", "force-stop command was not found"))?;
            if current.generation != agent.generation
                || current.observe_only
                || retained.verb != "stop"
                || retained.target.as_deref() != Some(agent.id.as_str())
                || retained.generation != Some(agent.generation)
                || retained.args.get("force").and_then(Value::as_bool) != Some(true)
                || !matches!(
                    retained.state,
                    eliot_core::CommandState::Dispatched | eliot_core::CommandState::Unknown
                )
            {
                return Err(Error::new(
                    "stale_generation",
                    "force-stop binding is no longer current",
                ));
            }
            // Source EOF can remove routing before the actor finishes draining.
            // Publish planned exit under the same Store lock used by loss
            // settlement so it cannot turn this stop Unknown in that race.
            let namespace = store
                .generation_custody_namespace(&agent.id, agent.generation)?
                .ok_or_else(|| {
                    Error::new("unknown", "generation custody namespace was not recorded")
                })?;
            if namespace != store.custody_namespace()? {
                return Err(Error::new(
                    "process_custody_foreign",
                    "generation custody belongs to another data-directory binding; its Job was left untouched",
                ));
            }
            let guard = ForceStopGuard::new(&self.state, key)?;
            (namespace, guard)
        };
        let name =
            crate::platform::custody::agent_custody_name(&namespace, &agent.id, agent.generation);
        tokio::task::spawn_blocking(move || {
            crate::platform::windows::terminate_named_generation_job(&name)
        })
        .await
        .map_err(|_| Error::new("unknown", "force-stop Job termination task failed"))?
        .map_err(|_| {
            Error::new(
                "unknown",
                "force-stop could not verify an empty generation Job",
            )
        })?;
        loop {
            // The observer refreshes exact creation-time identities and closes
            // OS custody only after a complete empty Job sample. Store requires
            // all recorded processes exited AND the host reader-drain record.
            self.state
                .monitoring
                .sample_generation(agent)
                .await
                .map_err(|_| Error::new("unknown", "force-stop process readback is unconfirmed"))?;
            if self
                .state
                .store
                .lock()
                .await
                .has_owned_exit_and_reader_drain(&agent.id, agent.generation)?
            {
                return Ok((
                    json!({
                        "state": "applied", "custody": "exact", "terminated": true,
                        "graceful": false,
                    }),
                    Some(guard),
                ));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn mark_reader_unavailable(&self, key: &AgentKey) {
        let _ = settle_drained_source_exit(
            &Arc::downgrade(&self.state),
            key,
            false,
            false,
            "daemon_force_actor_unavailable",
        )
        .await;
        fence_runtime_generation(&self.state, key);
        signal_progress(&self.state);
    }
}

/// Remove exactly a generated instance directory while it is still empty.
/// `remove_dir` refuses nonempty or missing paths; this never deletes recursively.
fn remove_empty_instance_dir(path: &Path) {
    let _ = std::fs::remove_dir(path);
}

async fn run_agent(
    actor: AgentActorConfig,
    child: OwnedChild,
    client: PluginClient,
    event_task: tokio::task::JoinHandle<()>,
    messages: mpsc::Receiver<RuntimeMessage>,
    started: oneshot::Sender<Result<Value>>,
    stderr_capture: StderrCapture,
) {
    let credentials = Arc::clone(&actor.source_credentials);
    let key = actor.key.clone();
    let mut failure = None;
    run_agent_inner(
        actor,
        child,
        client,
        event_task,
        messages,
        started,
        &mut failure,
    )
    .await;
    // All existing child/custody cleanup paths have completed or dropped their
    // owning handle. Finish the sole stderr drain before the final error view.
    let snapshot = stderr_capture.finish(Instant::now() + STOP_GRACE).await;
    if let Some(failure) = failure {
        let error = redact_native_error(
            failure.error.with_stderr(StderrSource::Plugin, snapshot),
            &credentials,
        );
        record_plugin_error(
            &error,
            &key.id,
            key.generation,
            &failure.command_id,
            &credentials,
        );
        if let Some(reply) = failure.reply {
            let _ = reply.send(Err(error));
        }
    }
}

/// Flag a harness that behaves differently from its verified record, once
/// per harness and installed surface per daemon run, at Agent start.
/// Installed facts come from the read-only `--version` probe; a missing
/// probe, an unverified harness, or a matching surface never flags and never
/// fails the start.
async fn flag_harness_drift(state: &HostState, agent: &eliot_core::Agent) {
    let harness = agent.harness.clone();
    let entry = crate::adhoc::harness_installation_async(&harness).await;
    let installed = entry
        .get("version")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    // Schema fingerprints stay unset where the harness exposes no runtime
    // marker on this base; the installed version carries the comparison.
    let drift = eliot_core::check_harness_drift(&harness, installed.as_deref(), None);
    let Some(drift) = drift else {
        return;
    };
    let key = (harness, installed.unwrap_or_default());
    if !state.drift_warned.lock_recover().insert(key) {
        return;
    }
    let mut store = state.store.lock().await;
    if let Ok(event) = store.commit_observe_event(
        Some(&agent.id),
        Some(agent.generation),
        eliot_core::HARNESS_DRIFT_KIND,
        drift.payload(),
        crate::common::now_ms(),
    ) {
        state.bus.publish(&[event]);
    }
}

async fn initialize_agent(actor: &AgentActorConfig, client: &PluginClient) -> Result<Value> {
    let init = client
        .call(
            "initialize",
            json!({
                "protocol": "eliot-plugin/1",
                "credential": actor.plugin_credential,
                "instance_dir": actor.instance_dir,
                "store_root": actor.store_root,
                "config": {"harness": actor.agent.harness},
            }),
            deadline(),
        )
        .await
        .map_err(|error| redact_native_error(error, &actor.source_credentials))?;
    let manifest: PluginManifest = serde_json::from_value(init)
        .map_err(|_| Error::new("unsupported", "Agent plugin returned a malformed manifest"))?;
    require_proxy_capability(&actor.agent.profile_snapshot, &manifest)?;
    let source_plan = manifest_source_plan(&manifest, &actor.agent.harness)?;
    if !manifest
        .verbs
        .iter()
        .any(|verb| verb == &actor.start_command.verb)
    {
        return Err(Error::new(
            "unsupported",
            format!(
                "Agent plugin does not advertise {}",
                actor.start_command.verb
            ),
        ));
    }
    // At Agent start, flag a harness that behaves differently from its
    // verified record. A missing probe or matching surface never flags;
    // the flag never fails the start.
    if let Some(state) = actor.state.upgrade() {
        flag_harness_drift(&state, &actor.agent).await;
    }
    PluginHost::install_runtime_verbs(
        &actor.state,
        &actor.key,
        manifest.verbs.into_iter().collect(),
    )?;
    let plan: BTreeMap<String, String> = serde_json::from_value(source_plan.clone())
        .map_err(|_| Error::new("internal", "validated source plan could not be decoded"))?;
    let state = actor
        .state
        .upgrade()
        .ok_or_else(|| Error::new("unknown", "plugin host stopped during initialization"))?;
    {
        let mut store = state.store.lock().await;
        store.install_plugin_sources(
            &actor.agent.id,
            actor.agent.generation,
            &actor.start_command.id,
            &plan,
            now_ms(),
        )?;
    }
    Ok(source_plan)
}

fn require_proxy_capability(profile: &Value, manifest: &PluginManifest) -> Result<()> {
    if profile.get("route_binding").is_some() && manifest.proxy != "responses" {
        return Err(Error::new(
            "unsupported",
            "plugin does not advertise Responses-compatible proxy routing",
        ));
    }
    Ok(())
}

async fn cleanup_startup(
    actor: &AgentActorConfig,
    child: OwnedChild,
    event_task: &mut tokio::task::JoinHandle<()>,
    pending_failure: &mut Option<PendingFailure>,
    proven_no_dispatch: bool,
) {
    let exited = terminate_child(child, &actor.monitoring, &actor.agent).await;
    // `settle_owned_source_exit` returns Ok only after positive custody exit,
    // completed reader drain, and the committed exit gap (see
    // `settle_drained_source_exit`). This failed-start path never enters the
    // actor loop, so settle the generation lost here: `mark_lost` is a no-op
    // for terminal or stale generations and fences further dispatch. A
    // proven `native_not_dispatched` refusal is excluded: inferring loss
    // would turn the dispatched start command Unknown before the router
    // classifies it, discarding the authoritative Rejected outcome.
    let settled = settle_owned_source_exit(
        &actor.state,
        &actor.key,
        Some(event_task),
        exited,
        "plugin_startup_exit",
    )
    .await;
    if settled.is_ok() && !proven_no_dispatch {
        mark_lost_state(&actor.state, &actor.key).await;
    } else if let Err(error) = settled {
        pending_failure.get_or_insert(PendingFailure {
            reply: None,
            error,
            command_id: actor.start_command.id.clone(),
        });
    }
    cleanup_actor(&actor.state, &actor.key, &actor.plugin_credential, false).await;
}

/// Retire one validated failed-start plugin without entering the live actor
/// loop: the plugin's normal shutdown, then the owned-exit wait the existing
/// stop path uses. On proven exit the owned-exit proof path releases the
/// capacity reservation; on unproven exit the reservation is kept and the
/// custody failure is reported like the other unconfirmed paths. The
/// authoritative Failed outcome itself is preserved by the caller, which
/// already holds the validated value. Never marks the generation lost: the
/// router settles it Failed.
async fn retire_failed_start(
    client: &PluginClient,
    child: &mut OwnedChild,
    event_task: &mut tokio::task::JoinHandle<()>,
    actor: &AgentActorConfig,
    pending_failure: &mut Option<PendingFailure>,
) {
    let deadlines = StopDeadlines::new(Instant::now());
    let (_, exited) =
        shutdown_and_wait_child(client, child, &actor.monitoring, &actor.agent, deadlines).await;
    let settled = settle_owned_source_exit(
        &actor.state,
        &actor.key,
        Some(event_task),
        exited,
        "plugin_failed_start_exit",
    )
    .await;
    if let Some(state) = actor.state.upgrade() {
        fence_runtime_generation(&state, &actor.key);
    }
    if let Err(error) = settled {
        pending_failure.get_or_insert(PendingFailure {
            reply: None,
            error,
            command_id: actor.start_command.id.clone(),
        });
    }
    cleanup_actor(&actor.state, &actor.key, &actor.plugin_credential, false).await;
}

fn snapshot_future(
    client: PluginClient,
    turn_id: Option<String>,
    stderr: StderrView,
    source_credentials: Arc<[String]>,
    mut completion: PluginSnapshotCompletion,
) -> PendingSnapshotFuture {
    Box::pin(async move {
        let params = json!({
            "agent_id": completion.agent_id.clone(),
            "generation": completion.generation,
            "command_id": completion.command_id.clone(),
            "turn_id": turn_id,
        });
        let result = client
            .call("agent.snapshot", params, deadline())
            .await
            .map_err(|error| redact_native_error(error, &source_credentials))
            .map(|mut value| {
                crate::redaction::redact_outcome(&mut value, &source_credentials);
                value
            });
        completion.result = diagnose_result(result, &stderr, &source_credentials);
        completion
    })
}

fn late_batch_error(
    state: &Weak<HostState>,
    agent_id: &str,
    generation: i64,
    cause: &str,
) -> Error {
    if let Some(state) = state.upgrade() {
        state.late_batch_causes.lock_recover().insert(
            AgentKey {
                id: agent_id.to_owned(),
                generation,
            },
            cause.to_owned(),
        );
    }
    Error::new("unknown", format!("events.put outcome unknown: {cause}"))
}

fn record_store_batch_rejection(
    credentials: &[String],
    key: &AgentKey,
    source: &str,
    batch_id: &str,
    event_kind: &str,
    cause: &str,
    error: &Error,
) {
    let policy = SecretPolicy::new(credentials.iter());
    let generation = key.generation.to_string();
    let code = format!("events_put_{}", safe_cause_code(cause));
    let validation = safe_store_validation_reason(error);
    crate::logging::record(
        &policy,
        &code,
        &[
            ("agent_id", &key.id),
            ("generation", &generation),
            ("source", source),
            ("batch_id", batch_id),
            ("event_kind", event_kind),
            ("validation", &validation),
        ],
        None,
    );
}

fn safe_store_validation_reason(error: &Error) -> String {
    if error.code != "invalid" {
        return "store_error".to_owned();
    }
    let message = error.message.as_str();
    if message == "native batch must contain an event" {
        return "empty_batch".to_owned();
    }
    if message == "unknown Agent state requires observe_only" {
        return "unknown_agent_state".to_owned();
    }
    if message == "request.closed must not acknowledge a command" {
        return "request_closed_command_id_present".to_owned();
    }
    if message.contains(" requires non-empty string field ") {
        return "required_field_missing".to_owned();
    }
    if message.ends_with(" must be a string") {
        return "field_not_string".to_owned();
    }
    if message.ends_with(" must be an object") || message.ends_with(" must be a usage object") {
        return "field_shape_invalid".to_owned();
    }
    if message.ends_with(" must be a non-empty string") {
        return "field_value_invalid".to_owned();
    }
    "invalid_unclassified".to_owned()
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
    fn protected_data(value: &Value, policy: &SecretPolicy) -> bool {
        match value {
            Value::Object(values) => values.iter().any(|(key, value)| {
                policy.contains_text(key)
                    || (matches!(
                        key.as_str(),
                        "agent_id"
                            | "command_id"
                            | "turn_id"
                            | "request_id"
                            | "item_id"
                            | "native_scope"
                            | "native_ref"
                    ) && policy.contains_value(value))
                    || protected_data(value, policy)
            }),
            Value::Array(values) => values.iter().any(|value| protected_data(value, policy)),
            _ => false,
        }
    }

    let policy = SecretPolicy::new(credentials);
    // Validate the whole batch before changing text: dedup/correlation identities
    // and object keys must never be rewritten into a different durable payload.
    if [&batch.batch_id, &batch.agent_id, &batch.source]
        .into_iter()
        .any(|value| policy.contains_text(value))
        || batch
            .cursor
            .as_deref()
            .is_some_and(|value| policy.contains_text(value))
        || batch.events.iter().any(|event| {
            policy.contains_text(&event.native_id)
                || policy.contains_text(&event.kind)
                || protected_data(&event.data, &policy)
        })
    {
        return Err(Error::new(
            "secret_in_payload",
            "native source payload contains a known secret in immutable metadata",
        ));
    }
    for event in &mut batch.events {
        if policy.redact_values(&mut event.data) {
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

async fn record_source_gap(
    store: &StoreHandle,
    bus: &Bus,
    params: Value,
    binding: source_handoff::SourceBinding<'_>,
    credentials: &[String],
    until: Instant,
) -> Result<Value> {
    let mut gap: SourceGap = serde_json::from_value(params)
        .map_err(|_| Error::new("invalid", "source.gap params are malformed"))?;
    if gap.agent_id != binding.agent_id || gap.generation != binding.generation {
        return Err(Error::new(
            "not_allowed",
            "source.gap binding does not match this Agent generation",
        ));
    }
    // Redaction changes only reason; immutable binding validation stays above.
    redact_source_gap(&mut gap, credentials)?;
    let subject = binding.expected_subject()?;
    let (result, write_faulted) = {
        let mut store = timeout_at(until, store.lock())
            .await
            .map_err(|_| Error::new("busy", "Store lock deadline elapsed for source.gap"))?;
        let result = match subject {
            Some(subject) => store.record_observed_source_gap(gap, subject, now_ms()),
            None => store.record_source_gap(gap, now_ms()),
        };
        if let Ok(change) = &result {
            bus.publish(&change.events);
        }
        (result, store.is_write_faulted())
    };
    match result {
        Ok(change) => {
            let ack: SourceGapAck = change.value;
            serde_json::to_value(ack).map_err(|_| {
                Error::new(
                    "internal",
                    "source.gap acknowledgement could not be encoded",
                )
            })
        }
        Err(_) if write_faulted => Err(Error::new(
            "unknown",
            "source.gap outcome is unknown; settle with source.state",
        )),
        Err(error) => Err(redact_native_error(error, credentials)),
    }
}

fn redact_source_gap(gap: &mut SourceGap, credentials: &[String]) -> Result<()> {
    let policy = SecretPolicy::new(credentials);
    if [&gap.gap_id, &gap.agent_id, &gap.source]
        .into_iter()
        .chain(gap.families.iter())
        .any(|value| policy.contains_text(value))
        || [&gap.from, &gap.to]
            .into_iter()
            .flatten()
            .any(|value| policy.contains_text(value))
    {
        return Err(Error::new(
            "secret_in_payload",
            "native source payload contains a known secret in immutable metadata",
        ));
    }
    gap.reason = policy.redact_text(&gap.reason);
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
    crate::redaction::redact_error(error, credentials)
}

fn diagnose_error(error: Error, stderr: &StderrView, credentials: &[String]) -> Error {
    // This view is obtained from running[AgentKey] and drains exactly one
    // owned process pipe. A disk marker from another process/generation can
    // never enter this path. Marker evidence never replaces the effect code.
    let snapshot = stderr.snapshot();
    let mut error = error;
    for line in snapshot.text.lines() {
        if let Some(json) = line.strip_prefix("eliot_crash ")
            && let Ok(marker) = serde_json::from_str::<eliot_core::api::CrashMarker>(json)
            && marker.code == "plugin.panicked"
            && marker.validate().is_ok()
        {
            let evidence = error
                .origin_evidence
                .as_deref()
                .unwrap_or("original failure origin unclassified");
            error.origin_evidence = Some(format!("{evidence}; {}", marker.summary()).into());
        }
    }
    redact_native_error(
        error.with_stderr(StderrSource::Plugin, snapshot),
        credentials,
    )
}

fn diagnose_result(
    result: Result<Value>,
    stderr: &StderrView,
    credentials: &[String],
) -> Result<Value> {
    match result {
        Err(error) => Err(diagnose_error(error, stderr, credentials)),
        Ok(mut value) => {
            if matches!(
                value.get("state").and_then(Value::as_str),
                Some("failed" | "rejected" | "unknown")
            ) {
                crate::redaction::attach_stderr(
                    &mut value,
                    StderrSource::Plugin,
                    stderr.snapshot(),
                    credentials,
                );
            } else {
                crate::redaction::redact_outcome(&mut value, credentials);
            }
            Ok(value)
        }
    }
}

fn record_plugin_error(
    error: &Error,
    agent: &str,
    generation: i64,
    command: &str,
    credentials: &[String],
) {
    let generation = generation.to_string();
    crate::logging::record(
        &SecretPolicy::new(credentials),
        &error.code,
        &[
            ("agent", agent),
            ("generation", &generation),
            ("command", command),
        ],
        Some(error),
    );
}

fn plugin_command_args(command: &Command) -> Result<Value> {
    let mut args = command
        .args
        .as_object()
        .cloned()
        .ok_or_else(|| Error::new("invalid", "plugin command args must be an object"))?;
    match command.verb.as_str() {
        "send" => {
            args.remove("notice_kind");
        }
        "configure" => {
            // The model/effort overlay rides to the plugin verbatim: the
            // plugin performs the harness-native switch (OpenCode,
            // Antigravity) and proves it before the Router stores anything.
            // A configure carries no turn or request binding.
        }
        "steer" | "interrupt" => {
            command.turn_id.as_ref().ok_or_else(|| {
                Error::new("stale_generation", "turn control has no captured turn id")
            })?;
        }
        "inject" => {
            // Direct active_turn inject: keep the original typed payload and the
            // captured turn. Any builder failure here is known before plugin
            // I/O, so it classifies native_not_dispatched (Rejected), never
            // a generic unsupported Unknown.
            let typed: InjectArgs = serde_json::from_value(command.args.clone())
                .map_err(|_| Error::new("native_not_dispatched", "inject args are malformed"))?;
            if typed.point != InjectPoint::ActiveTurn {
                return Err(Error::new(
                    "native_not_dispatched",
                    "inject point is not a direct active_turn command",
                ));
            }
            command.turn_id.as_ref().ok_or_else(|| {
                Error::new(
                    "native_not_dispatched",
                    "active_turn inject has no captured turn id",
                )
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
    Ok(Value::Object(args))
}

/// Effective model/effort for one send command's retained configuration
/// revision, resolved from the Store before native turn construction.
/// `None` when the send runs under the frozen snapshot (revision 0); the
/// plugin then uses its session values. A resolution failure aborts before
/// any native I/O is attempted.
async fn resolve_send_config(
    state: &Weak<HostState>,
    command: &Command,
) -> Result<Option<eliot_store::AgentConfig>> {
    if command.verb != "send" {
        return Ok(None);
    }
    let Some(state) = state.upgrade() else {
        return Err(Error::new(
            "unknown",
            "plugin host stopped before command resolution",
        ));
    };
    let store = state.store.lock().await;
    store.send_effective_config(&command.id)
}

/// Effective model/effort overrides for a `resume` running under an
/// inherited `configure` overlay on a sticky-session harness (OpenCode,
/// Antigravity, Claude, Muse): the retained native session already runs
/// the overlay's effective values, so the plugin must realign to them
/// instead of reverting to the frozen snapshot. `None` on launch, on
/// resumes without an overlay, and on harnesses that apply overlays per
/// turn at send time (Codex). A resolution failure is a typed startup
/// error: starting with the frozen values would contradict the explicit
/// overlay inheritance, so the caller fails startup before any native I/O.
async fn resolve_resume_config(
    actor: &AgentActorConfig,
) -> Result<Option<(Option<String>, Option<String>, i64)>> {
    if actor.start_command.verb != "resume"
        || !matches!(
            actor.agent.harness.as_str(),
            "opencode" | "antigravity" | "claude" | "muse"
        )
    {
        return Ok(None);
    }
    let Some(state) = actor.state.upgrade() else {
        return Err(Error::new(
            "unknown",
            "plugin host stopped before command resolution",
        ));
    };
    let store = state.store.lock().await;
    store
        .command_effective_config(
            &actor.start_command.id,
            &["launch", "resume", "send", "configure"],
        )
        .map(|config| {
            config.map(|config| {
                let effort = if actor.agent.harness == "muse" {
                    None
                } else {
                    config.effort
                };
                (config.model, effort, config.revision)
            })
        })
}

/// Build the `agent.command` wire params for one dispatched command, relaying
/// Store-committed FIFO `next_input` items as top-level `context` (omitted
/// when empty). The reserved `context_items` member is stripped so `args`
/// stays the original send input. A resolved configuration overlay adds the
/// effective model/effort overrides for `send` only; every other verb keeps
/// its exact historical params.
fn agent_command_params(
    agent: &Agent,
    command: &Command,
    dispatch_seq: Option<i64>,
    config: Option<&eliot_store::AgentConfig>,
) -> Result<Value> {
    let mut args = plugin_command_args(command)?;
    let context = args
        .as_object_mut()
        .and_then(|object| object.remove("context_items"))
        .map(serde_json::from_value::<Vec<ContextItem>>)
        .transpose()
        .map_err(|_| Error::new("source_integrity", "send context_items are malformed"))?
        .unwrap_or_default();
    let mut params = json!({
        "agent_id": agent.id,
        "generation": agent.generation,
        "command_id": command.id,
        "verb": command.verb,
        "turn_id": command.turn_id,
        "request_id": command.request_id,
        "args": args,
        "dispatch_seq": dispatch_seq,
    });
    if !context.is_empty() {
        params["context"] = serde_json::to_value(&context)
            .map_err(|_| Error::new("internal", "context items could not be encoded"))?;
    }
    if let Some(config) = config.filter(|_| command.verb == "send") {
        if let Some(model) = config.model.as_deref() {
            params["model_override"] = Value::String(model.to_owned());
        }
        if let Some(effort) = config.effort.as_deref() {
            params["effort_override"] = Value::String(effort.to_owned());
        }
        params["config_revision"] = json!(config.revision);
    }
    Ok(params)
}

fn validate_agent_command_result(value: Value, harness: &str, command: &Command) -> Result<Value> {
    let verb = command.verb.as_str();
    if verb == "models" {
        return validate_models_catalog_result(value);
    }
    match value.get("state").and_then(Value::as_str) {
        Some("applied") if verb == "send" => validate_send_result(value),
        Some("applied") if verb == "configure" && harness == "claude" => {
            let accepted = value.get("accepted_model").and_then(Value::as_str);
            if accepted.is_some() && accepted == command.args.get("model").and_then(Value::as_str) {
                Ok(value)
            } else {
                Err(Error::new(
                    "unknown",
                    "Claude configure reply did not prove acceptance of the requested selector",
                ))
            }
        }
        Some("applied") if verb == "configure" => validate_configure_result(value),
        Some("applied" | "rejected" | "failed" | "unknown") => Ok(value),
        _ => Err(Error::new(
            "unknown",
            "plugin command response omitted its command outcome",
        )),
    }
}

/// An applied native `configure` proves what it switched: the effective
/// model (always, for both switching harnesses) and the effective effort
/// where the harness has that knob (Antigravity `thought_level`), each
/// with its native-echo/GET verification flag. Anything less is not an
/// application and must not reach the Store.
fn validate_configure_result(value: Value) -> Result<Value> {
    let model = value.get("model").and_then(Value::as_str);
    let model_verified = value.get("model_verified").and_then(Value::as_bool);
    let effort_verified = value
        .get("effort_verified")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let proven = model.is_some_and(|model| !model.trim().is_empty())
        && model_verified == Some(true)
        && value.get("effort").is_none_or(|effort| {
            effort.is_null()
                || effort
                    .as_str()
                    .is_some_and(|effort| !effort.trim().is_empty())
        })
        && (value.get("effort").and_then(Value::as_str).is_none() || effort_verified);
    if proven {
        Ok(value)
    } else {
        Err(Error::new(
            "unknown",
            "plugin configure response omitted its verified native proof",
        ))
    }
}

fn complete_plugin_command(
    completion: PluginCommandCompletion,
    agent: &Agent,
    stderr: &StderrView,
    source_credentials: &[String],
    pending_failure: &mut Option<PendingFailure>,
    reader_closed: bool,
) {
    let PluginCommandCompletion {
        command,
        reply,
        result,
    } = completion;
    let result = result
        .map_err(|error| redact_native_error(error, source_credentials))
        .and_then(|mut value| {
            crate::redaction::redact_outcome(&mut value, source_credentials);
            validate_agent_command_result(value, &agent.harness, &command)
        });
    let command_id = command.id;

    match result {
        Err(error) if error.code == "unknown" && reader_closed && pending_failure.is_none() => {
            // Retain one exact caller until actor cleanup finishes stderr.
            *pending_failure = Some(PendingFailure {
                reply: Some(reply),
                error,
                command_id,
            });
        }
        result => {
            let result = diagnose_result(result, stderr, source_credentials);
            if let Err(error) = &result {
                record_plugin_error(
                    error,
                    &agent.id,
                    agent.generation,
                    &command_id,
                    source_credentials,
                );
            }
            let _ = reply.send(result);
        }
    }
}

/// A read-only live session catalogue: the `model.list` response shape
/// with `session` provenance. No state is claimed; the shape alone proves
/// the read.
fn validate_models_catalog_result(value: Value) -> Result<Value> {
    if value.get("provenance").and_then(Value::as_str) != Some("session") {
        return Err(Error::new(
            "source_integrity",
            "plugin session catalogue omitted its live provenance",
        ));
    }
    let models = value
        .get("models")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::new("source_integrity", "plugin session catalogue is malformed"))?;
    if models.len() > 1024 {
        return Err(Error::new(
            "source_integrity",
            "plugin session catalogue exceeds 1024 entries",
        ));
    }
    for model in models {
        let id = model.get("id").and_then(Value::as_str).unwrap_or_default();
        if id.trim().is_empty() || id.len() > 256 || id.chars().any(char::is_control) {
            return Err(Error::new(
                "source_integrity",
                "plugin session catalogue entry is malformed",
            ));
        }
    }
    if value
        .get("default")
        .is_some_and(|default| !default.is_null())
    {
        return Err(Error::new(
            "source_integrity",
            "plugin session catalogue names an unexpected default",
        ));
    }
    Ok(value)
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

/// True for a validated `agent.start` result carrying the plugin's
/// authoritative proven-no-effect verdict. Only `validate_start_result`
/// decides admission; this splits the admitted shapes so a failed start
/// retires instead of entering the live actor loop.
fn is_failed_start_result(value: &Value) -> bool {
    value.get("state").and_then(Value::as_str) == Some("failed")
}

fn validate_start_result(value: &Value, agent: &Agent, command: &Command) -> Result<()> {
    // Ready establishes the session; failed is the plugin's authoritative
    // proven-no-effect verdict (returned instead of an error only for checks
    // before any native session effect). Both require the exact
    // agent/generation/command binding.
    let state = value.get("state").and_then(Value::as_str);
    if matches!(state, Some("ready") | Some("failed"))
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
        "inject",
        "attach",
        "detach",
        "configure",
    ];
    if verbs.len() != manifest.verbs.len()
        || verbs.len() < 3
        || !["launch", "send", "stop"]
            .iter()
            .all(|required| verbs.contains(required))
        || verbs.iter().any(|verb| !implemented_verbs.contains(verb))
        || !manifest.child_verbs.is_empty()
        || manifest.features.start_requires_input
        || (manifest.features.native_children
            && eliot_core::native_child_families_for_harness(harness).is_none())
    {
        return Err(unsupported_manifest());
    }

    if !matches!(
        harness,
        "codex" | "muse" | "claude" | "opencode" | "antigravity"
    ) {
        return Err(unsupported_manifest());
    }
    let mandatory_families = ["message", "session", "turn"];
    let allowed_families = ["message", "request", "session", "tool", "turn"];
    if manifest
        .sources
        .keys()
        .any(|mode| !matches!(mode.as_str(), "controlled" | "observed"))
    {
        return Err(unsupported_manifest());
    }
    match manifest.sources.get("observed") {
        Some(observed)
            if manifest.features.observe
                && !observed.is_empty()
                && observed.iter().all(|(family, mechanism)| {
                    allowed_families.contains(&family.as_str()) && mechanism == "native_read"
                }) => {}
        None if !manifest.features.observe => {}
        _ => return Err(unsupported_manifest()),
    }
    let controlled = manifest
        .sources
        .get("controlled")
        .ok_or_else(unsupported_manifest)?;
    if controlled
        .keys()
        .any(|family| !allowed_families.contains(&family.as_str()))
        || mandatory_families
            .iter()
            .any(|family| controlled.get(*family).map(String::as_str) != Some("stream"))
        || controlled.values().any(|mechanism| mechanism != "stream")
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
    "SystemDrive",
    "ProgramData",
    "PATHEXT",
    "PATH",
    "TEMP",
];

#[cfg(not(windows))]
const BASE_ENVIRONMENT_NAMES: &[&str] = &["HOME", "PATH", "TEMP"];

pub(crate) fn minimal_plugin_environment() -> Vec<(OsString, OsString)> {
    BASE_ENVIRONMENT_NAMES
        .iter()
        .filter_map(|name| std::env::var_os(name).map(|value| (OsString::from(*name), value)))
        .collect()
}

/// Freeze known execution values once, from the exact environment sent to this
/// source. Do not infer secrets from variable names or reread mutable process env.
fn source_secret_snapshot(
    profile: &Value,
    environment: &Value,
    mut known: Vec<String>,
) -> Arc<[String]> {
    if let (Some(names), Some(environment)) = (
        profile.get("pass_env").and_then(Value::as_array),
        environment.as_object(),
    ) {
        for name in names.iter().filter_map(Value::as_str) {
            if let Some(value) = environment.iter().find_map(|(key, value)| {
                let matches = if cfg!(windows) {
                    key.eq_ignore_ascii_case(name)
                } else {
                    key == name
                };
                matches.then(|| value.as_str()).flatten()
            }) {
                known.push(value.to_owned());
            }
        }
    }
    known.into()
}

fn metadata_secret_snapshot(
    profile: &Value,
    environment: &Value,
    known: Vec<String>,
) -> Arc<[String]> {
    let mut policy_profile = profile.clone();
    if let Some(names) = policy_profile
        .get_mut("pass_env")
        .and_then(Value::as_array_mut)
    {
        // Exclude contributions by NAME, never subtract a matching secret value.
        names.retain(|name| {
            name.as_str().is_none_or(|name| {
                !crate::config::CODEX_STORE_SELECTORS
                    .iter()
                    .any(|selector| crate::config::native_store_selector(name) == Some(*selector))
            })
        });
    }
    source_secret_snapshot(&policy_profile, environment, known)
}

pub(crate) fn build_process_environment(profile: &Value) -> Result<Map<String, Value>> {
    build_process_environment_with(profile, |name| std::env::var_os(name))
}

/// A prepared metadata reader uses the same private environment that was
/// verified before cache lookup. Legacy inputs can still be read uncached.
pub(crate) struct MetadataRoute {
    pub(crate) environment: Value,
    pub(crate) identity: Option<String>,
}

pub(crate) fn metadata_route(profile: &Value) -> Result<MetadataRoute> {
    let (environment, identity) =
        prepare_process_environment(profile, |name| std::env::var_os(name))?;
    let identity =
        identity.filter(|_| crate::config::metadata_route_scope_known(profile, &environment));
    Ok(MetadataRoute {
        environment: Value::Object(environment),
        identity,
    })
}

fn captured_store_inputs_valid(values: &Map<String, Value>) -> bool {
    values
        .values()
        .all(|value| value.is_null() || value.is_string())
        && (values.len() == 2 || values.len() == 5)
        && values
            .keys()
            .all(|name| crate::config::native_store_selector(name).is_some())
        && ["CODEX_HOME", "CODEX_SQLITE_HOME"]
            .into_iter()
            .all(|selector| {
                values
                    .keys()
                    .filter(|name| crate::config::native_store_selector(name) == Some(selector))
                    .count()
                    == 1
            })
        && (values.len() == 2
            || ["CLAUDE_CONFIG_DIR", "XDG_CONFIG_HOME", "XDG_DATA_HOME"]
                .into_iter()
                .all(|selector| {
                    values
                        .keys()
                        .filter(|name| crate::config::native_store_selector(name) == Some(selector))
                        .count()
                        == 1
                }))
}

fn build_process_environment_with(
    profile: &Value,
    lookup: impl FnMut(&str) -> Option<OsString>,
) -> Result<Map<String, Value>> {
    prepare_process_environment(profile, lookup).map(|(environment, _)| environment)
}

fn prepare_process_environment(
    profile: &Value,
    mut lookup: impl FnMut(&str) -> Option<OsString>,
) -> Result<(Map<String, Value>, Option<String>)> {
    let captured = profile
        .get("pass_env_values")
        .map(|value| {
            value
                .as_object()
                .filter(|values| captured_store_inputs_valid(values))
                .ok_or_else(|| Error::new("invalid", "frozen native store inputs are malformed"))
        })
        .transpose()?;
    if captured.is_none() && profile["harness"] == "codex" {
        return Err(Error::new(
            "invalid",
            "frozen native store inputs are missing",
        ));
    }
    let mut environment = Map::new();
    for name in BASE_ENVIRONMENT_NAMES {
        if let Some(value) = lookup(name) {
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
            if let Some(selector) = crate::config::native_store_selector(name) {
                let frozen = captured.into_iter().flatten().find_map(|(name, value)| {
                    (crate::config::native_store_selector(name) == Some(selector)).then_some(value)
                });
                match frozen {
                    Some(Value::String(value)) => {
                        insert_environment(&mut environment, selector, value.to_owned());
                        continue;
                    }
                    // A captured null suppresses future ambient values. Empty
                    // stays empty.
                    Some(_) => continue,
                    // A legacy snapshot predates this selector: keep the
                    // previous live lookup instead of failing the launch.
                    None => {}
                }
            }
            if let Some(value) = lookup(name) {
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

    let identity = match profile.get("native_route_identity") {
        None => None,
        Some(frozen) => {
            let current = crate::config::native_route_identity(profile, |name| {
                Ok(environment.iter().find_map(|(key, value)| {
                    (key == name || (cfg!(windows) && key.eq_ignore_ascii_case(name)))
                        .then(|| value.as_str().map(str::to_owned))
                        .flatten()
                }))
            })?;
            if frozen.as_str() != Some(current.as_str()) {
                return Err(Error::new(
                    "unavailable",
                    "frozen native route could not be verified in the private environment",
                ));
            }
            Some(current)
        }
    };
    Ok((environment, identity))
}

fn build_target_environment(data_root: &Path, agent: &Agent, agent_token: &str) -> Result<Value> {
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
    let home = data_root
        .to_str()
        .ok_or_else(|| Error::new("invalid", "data directory path is not UTF-8"))?;
    insert_environment(&mut environment, "ELIOT_HOME", home.to_owned());
    let scope_dir = crate::daemon::normalize_identity_data_dir(data_root);
    let scope_dir = scope_dir
        .to_str()
        .ok_or_else(|| Error::new("invalid", "data directory path is not UTF-8"))?;
    insert_environment(
        &mut environment,
        crate::daemon::AGENT_DATA_DIR_ENV,
        scope_dir.to_owned(),
    );
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
    let executable = std::env::current_exe()
        .map_err(|error| Error::new("platform_io", format!("locate Eliot executable: {error}")))?;
    let controller_dir = executable
        .parent()
        .ok_or_else(|| Error::new("platform_io", "Eliot executable has no parent directory"))?;
    // Native shells split PATH without quote awareness (Windows PowerShell
    // ignores the quotes join_paths emits), so a separator in the controller
    // directory can never resolve bare `swarm`. Reject before native spawn
    // instead of silently resolving a legacy binary. DataRoot is unaffected:
    // only the controller parent actually placed in PATH is gated.
    #[cfg(windows)]
    let separator_present = controller_dir
        .as_os_str()
        .as_encoded_bytes()
        .contains(&b';');
    #[cfg(not(windows))]
    let separator_present = controller_dir
        .as_os_str()
        .as_encoded_bytes()
        .contains(&b':');
    if separator_present {
        // Pre-effect gate (before instance/monitoring/spawn): native I/O
        // provably did not start, so the existing classifier settles Rejected.
        return Err(Error::new(
            "native_not_dispatched",
            "controller directory path cannot be represented in native PATH",
        ));
    }
    #[cfg(windows)]
    let existing_path = environment
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("PATH"))
        .and_then(|(_, value)| value.as_str());
    #[cfg(not(windows))]
    let existing_path = environment.get("PATH").and_then(Value::as_str);
    let mut entries = vec![controller_dir.to_owned()];
    if let Some(existing) = existing_path.filter(|path| !path.is_empty()) {
        entries.extend(std::env::split_paths(existing));
    }
    let path_value = std::env::join_paths(entries)
        .map_err(|_| {
            Error::new(
                "invalid",
                "controller directory path cannot be represented in PATH",
            )
        })?
        .into_string()
        .map_err(|_| Error::new("invalid", "managed target PATH is not valid Unicode"))?;
    insert_environment(&mut environment, "PATH", path_value);
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

fn deadline() -> Instant {
    Instant::now() + RPC_DEADLINE
}

fn start_deadline() -> Instant {
    Instant::now() + START_TIMEOUT
}

async fn enqueue_stop(
    sender: &mpsc::Sender<RuntimeMessage>,
    message: RuntimeMessage,
    until: Instant,
) -> Result<()> {
    // Cancelling pending mpsc::send cannot enqueue its value. SendError likewise
    // returns an unreceived value; neither path can have reached native I/O.
    timeout_at(until, sender.send(message))
        .await
        .map_err(|_| {
            Error::new(
                "native_not_dispatched",
                "stop was not enqueued before its queue deadline",
            )
        })?
        .map_err(|_| {
            Error::new(
                "native_not_dispatched",
                "stop queue closed before admission",
            )
        })
}

fn prepare_queued_stop_call(
    client: &PluginClient,
    params: Value,
    deadlines: StopDeadlines,
) -> Result<impl std::future::Future<Output = Result<Value>> + '_> {
    if Instant::now() >= deadlines.queued_until {
        return Err(Error::new(
            "native_not_dispatched",
            "stop queue deadline elapsed before native dispatch",
        ));
    }
    Ok(client.call(
        "agent.stop",
        params,
        deadlines
            .rpc_deadline(RPC_DEADLINE + eliot_plugin::native_lifecycle::STOP_INTERRUPT_TIMEOUT),
    ))
}

async fn wait_child(
    child: &mut OwnedChild,
    duration: Duration,
    monitoring: &Monitoring,
    agent: &Agent,
) -> bool {
    wait_child_family(child, duration, duration, monitoring, agent).await
}

/// Wait `leader` for the plugin process, then `family` for its Job to
/// empty; false when either does not happen in time.
async fn wait_child_family(
    child: &mut OwnedChild,
    leader: Duration,
    family: Duration,
    monitoring: &Monitoring,
    agent: &Agent,
) -> bool {
    #[cfg(target_os = "linux")]
    {
        linux_wait_child_family(child, leader, family, monitoring, agent).await
    }
    #[cfg(not(target_os = "linux"))]
    {
        legacy_wait_child_family(child, leader, family, monitoring, agent).await
    }
}

/// Linux family wait: observe the leader exit without reaping while its birth
/// identity still anchors the group signal, drain the owned group, terminate
/// what remains, verify the group is empty except the leader, and only then
/// reap. Returns false unless the leader exited and the group drained; the
/// caller then terminates and retries. Never signals a bare reused PGID:
/// every group signal goes through the birth-checked leader identity, which
/// `wait()` clears only after the group is proven empty.
#[cfg(target_os = "linux")]
async fn linux_wait_child_family(
    child: &mut OwnedChild,
    leader: Duration,
    family: Duration,
    monitoring: &Monitoring,
    agent: &Agent,
) -> bool {
    if !matches!(
        timeout_at(Instant::now() + leader, child.wait_exit_unreaped()).await,
        Ok(Ok(()))
    ) {
        return false;
    }
    let _ = monitoring.sample_generation(agent).await;
    if !linux_drain_owned_group(child, family).await {
        return false;
    }
    let _ = monitoring.sample_generation(agent).await;
    // Reap only after the group is proven empty except the leader: the
    // numeric PGID cannot be reused between the final observation and this
    // reap because the exited leader is still unreaped.
    matches!(
        timeout_at(Instant::now() + family, child.wait()).await,
        Ok(Ok(_))
    )
}

/// Linux owned-group drain after the leader exit was observed without
/// reaping: bounded drain, terminate leftovers while the unreaped leader
/// anchors the birth-checked group signal, verify the group is empty except
/// the leader, and close the fork race with one more birth-checked signal
/// plus a zero-delay observation. Escaped descendants (setsid) stay
/// degraded/orphan_possible at settlement; only the still-owned ordinary
/// group is cleaned here.
#[cfg(target_os = "linux")]
pub(super) async fn linux_drain_owned_group(child: &mut OwnedChild, family: Duration) -> bool {
    let mut group_empty = matches!(child.wait_group_empty_except_leader(family).await, Ok(true));
    if !group_empty {
        let _ = child.terminate();
        group_empty = matches!(child.wait_group_empty_except_leader(family).await, Ok(true));
    }
    if group_empty {
        // An empty observation grants no signal authority by itself. Reuse
        // the retained leader's birth check, then observe the group again
        // pre-reap.
        let _ = child.terminate();
        group_empty = matches!(
            child.wait_group_empty_except_leader(Duration::ZERO).await,
            Ok(true)
        );
    }
    group_empty
}

#[cfg(not(target_os = "linux"))]
async fn legacy_wait_child_family(
    child: &mut OwnedChild,
    leader: Duration,
    family: Duration,
    monitoring: &Monitoring,
    agent: &Agent,
) -> bool {
    if !matches!(
        timeout_at(Instant::now() + leader, child.wait()).await,
        Ok(Ok(_))
    ) {
        return false;
    }
    let _ = monitoring.sample_generation(agent).await;
    #[cfg(windows)]
    {
        if matches!(child.wait_family_empty(family).await, Ok(true)) {
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
        let _ = family;
        true
    }
}

async fn wait_stop_child(
    child: &mut OwnedChild,
    monitoring: &Monitoring,
    agent: &Agent,
    until: Instant,
) -> bool {
    timeout_at(until, wait_child(child, STOP_GRACE, monitoring, agent))
        .await
        .unwrap_or(false)
}

async fn terminate_child(mut child: OwnedChild, monitoring: &Monitoring, agent: &Agent) -> bool {
    let exited =
        child.terminate().is_ok() && wait_child(&mut child, STOP_GRACE, monitoring, agent).await;
    drop(child);
    exited
}

/// True when the Store's current controlled projection proves the captured
/// generation ended normally: exact generation still current, state exactly
/// Stopped (committed native session end or applied stop), controlled
/// authority. Failed/Lost and advanced generations do not count; ack
/// uncertainty stays separate from this durable logical-End readback.
/// Terminal settlement revokes the token, so derived own_identity is always
/// false here and is not checked.
async fn has_committed_session_ended(
    state: &Weak<HostState>,
    key: &AgentKey,
    until: Instant,
) -> bool {
    let Some(host) = state.upgrade() else {
        return false;
    };
    let Ok(store) = timeout_at(until, host.store.lock()).await else {
        return false;
    };
    match store.get_agent(&key.id) {
        Ok(Some(agent)) => {
            agent.generation == key.generation
                && agent.state == eliot_core::AgentState::Stopped
                && !agent.observe_only
        }
        _ => false,
    }
}

/// Private shutdown, input close, then graceful wait with one
/// terminate-and-wait fallback. Returns (graceful, exited); callers settle
/// sources, fence, and reply. Shared by the Applied path and an ordinary
/// Unknown stop whose captured generation has durable logical End.
async fn shutdown_and_wait_child(
    client: &PluginClient,
    child: &mut OwnedChild,
    monitoring: &Monitoring,
    agent: &Agent,
    deadlines: StopDeadlines,
) -> (bool, bool) {
    let shutdown = if Instant::now() < deadlines.rpc_until {
        client
            .call("shutdown", json!({}), deadlines.rpc_deadline(RPC_DEADLINE))
            .await
            .and_then(validate_shutdown)
    } else {
        Err(Error::new(
            "unknown",
            "stop RPC deadline elapsed before shutdown",
        ))
    };
    if shutdown.is_ok() {
        let _ = client
            .close_input((Instant::now() + STOP_GRACE).min(deadlines.custody_until))
            .await;
    }
    // The harness's own exit gets the full grace; what it leaves behind
    // gets `FAMILY_GRACE`, then the Job terminate below ends it.
    let graceful = timeout_at(
        deadlines.custody_until,
        wait_child_family(child, STOP_GRACE, FAMILY_GRACE, monitoring, agent),
    )
    .await
    .unwrap_or(false);
    let mut exited = graceful;
    if !exited {
        let _ = child.terminate();
        exited = wait_stop_child(child, monitoring, agent, deadlines.custody_until).await;
    }
    (graceful, exited)
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

// Reader availability is independent of custody. Finishing or abandoning this
// reader must be recorded even when native control loss remains unconfirmed.
async fn settle_owned_source_exit(
    state: &Weak<HostState>,
    key: &AgentKey,
    event_task: Option<&mut tokio::task::JoinHandle<()>>,
    exited: bool,
    reason: &'static str,
) -> Result<()> {
    let (reason, reader_closed) = if let Some(task) = event_task {
        match timeout_at(Instant::now() + STOP_GRACE, &mut *task).await {
            Ok(Ok(())) => (reason, true),
            Ok(Err(_)) => ("owned_exit_source_failed", true),
            Err(_) => {
                task.abort();
                let joined = timeout_at(Instant::now() + RPC_GRACE, task).await.is_ok();
                ("owned_exit_drain_timeout", joined)
            }
        }
    } else {
        // The startup caller has not armed a reader; confirmed custody exit
        // proves its pipes cannot later acquire publication authority.
        (reason, exited)
    };
    settle_drained_source_exit(state, key, reader_closed, exited, reason).await
}

async fn settle_drained_source_exit(
    state: &Weak<HostState>,
    key: &AgentKey,
    reader_closed: bool,
    exited: bool,
    reason: &'static str,
) -> Result<()> {
    let settlement = async {
        if reader_closed && exited {
            record_root_exit_gap(state, key, reason).await?;
            let state = state
                .upgrade()
                .ok_or_else(|| Error::new("unavailable", "plugin host is unavailable"))?;
            let mut store = timeout_at(deadline(), state.store.lock())
                .await
                .map_err(|_| Error::new("unknown", "owned exit capacity release timed out"))?;
            // This call is reachable only after both physical exit and reader
            // drain were proven above. Closing a source alone never frees capacity.
            let events = store.record_owned_control_exit(&key.id, key.generation, now_ms())?;
            state.bus.publish(&events);
            drop(store);
            if native_children::is_codex_producer(&state.store, &key.id).await? {
                native_children::mark_children_unavailable(
                    &state.store,
                    &state.bus,
                    &key.id,
                    key.generation,
                    reason,
                    reader_closed,
                )
                .await?;
            }
            return Ok(());
        }
        let state = state
            .upgrade()
            .ok_or_else(|| Error::new("unavailable", "plugin host is unavailable"))?;
        let events = {
            let mut store = timeout_at(deadline(), state.store.lock())
                .await
                .map_err(|_| {
                    Error::new(
                        "unknown",
                        "native reader availability persistence timed out",
                    )
                })?;
            store.record_native_reader_unavailable(
                &key.id,
                key.generation,
                reason,
                reader_closed,
                now_ms(),
            )?
        };
        state.bus.publish(&events);
        if native_children::is_codex_producer(&state.store, &key.id).await? {
            native_children::mark_children_unavailable(
                &state.store,
                &state.bus,
                &key.id,
                key.generation,
                reason,
                reader_closed,
            )
            .await?;
        }
        Ok(())
    }
    .await;
    settlement.map_err(|mut error: Error| {
        error.code = "unknown".to_owned();
        error.message = "owned exit native source settlement is unconfirmed"
            .to_owned()
            .into();
        error
    })?;
    if !reader_closed {
        return Err(Error::new(
            "unknown",
            "owned source task cleanup is unconfirmed",
        ));
    }
    if !exited {
        return Err(Error::new("unknown", "owned custody exit is unconfirmed"));
    }
    Ok(())
}

// Drain terminal facts first. Retiring generations close only their own rows;
// current nonterminal generations also gap the actual native family owners.
async fn record_root_exit_gap(state: &Weak<HostState>, key: &AgentKey, reason: &str) -> Result<()> {
    let state = state
        .upgrade()
        .ok_or_else(|| Error::new("unavailable", "plugin host is unavailable"))?;
    let mut store = timeout_at(deadline(), state.store.lock())
        .await
        .map_err(|_| Error::new("unknown", "native source exit persistence timed out"))?;
    let agent = store
        .get_agent(&key.id)?
        .ok_or_else(|| Error::new("not_found", "exited plugin Agent was not found"))?;
    if agent.generation < key.generation {
        return Err(Error::new(
            "stale_generation",
            "exited plugin generation is no longer current",
        ));
    }
    let now = now_ms();
    if agent.generation == key.generation && !agent.is_terminal() {
        let events = store.record_native_exit_gaps(&key.id, key.generation, reason, now)?;
        state.bus.publish(&events);
    }
    // A zero-family predecessor still has a transport lifetime to close. A
    // committed native terminal fact suppresses gaps, never this source cleanup.
    let events = store.close_native_sources(&key.id, key.generation, now)?;
    state.bus.publish(&events);
    Ok(())
}

async fn mark_lost_state(state: &Weak<HostState>, key: &AgentKey) {
    mark_lost_state_with_exit(state, key, None, None).await;
}

/// Mark an agent generation lost with its process exit evidence: the
/// numeric exit code, the Windows NTSTATUS hex for crash statuses, and the
/// bounded redacted stderr tail recorded on the `agent.lost` event. Exit
/// evidence comes from the observed plugin process wait; the tail is the
/// already-redacted capture snapshot.
async fn mark_lost_state_with_exit(
    state: &Weak<HostState>,
    key: &AgentKey,
    exit: Option<eliot_core::ProcessExit>,
    stderr_tail: Option<String>,
) {
    let Some(state) = state.upgrade() else {
        return;
    };
    fence_runtime_generation(&state, key);
    {
        let mut store = state.store.lock().await;
        if let Some(deferred) = state.planned_stops.lock_recover().get(key) {
            deferred.store(true, Ordering::Release);
            return;
        }
        if let Ok(events) =
            store.mark_lost_with_exit(&key.id, key.generation, now_ms(), exit, stderr_tail)
        {
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
    let mut unknown = state.unknown_commands.lock_recover();
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
    let mut secrets = state.secrets.lock_recover();
    if !plugin_credential.is_empty() {
        secrets.remove(plugin_credential);
    }
}

// Stop new command routing without revoking authorization for buffered source facts.
fn fence_command_routing(state: &HostState, key: &AgentKey) {
    state.running.lock_recover().remove(key);
}

fn fence_runtime_generation(state: &HostState, key: &AgentKey) {
    fence_command_routing(state, key);
    let credential = state
        .agent_credentials
        .lock_recover()
        .remove(&(key.id.clone(), key.generation));
    if let Some(credential) = credential {
        state.secrets.lock_recover().remove(&credential);
    }
}

pub(crate) fn forget_revoked_agent_token(
    credentials: &AgentCredentials,
    secrets: &SecretSet,
    key: &(String, i64),
) {
    let removed = credentials.lock_recover().remove(key);
    let Some(token) = removed else {
        return;
    };
    secrets.lock_recover().remove(&token);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn plugin_panic_evidence_is_pipe_local_and_preserves_rejection() {
        let marker = eliot_core::api::CrashMarker {
            code: "plugin.panicked".to_owned(),
            thread: "main".to_owned(),
            location: "plugin.rs:42".to_owned(),
            message: "payload omitted".to_owned(),
            backtrace: String::new(),
            timestamp: 1,
        };
        let a = eliot_plugin::spawn_stderr(
            std::io::Cursor::new(
                format!("eliot_crash {}\n", serde_json::to_string(&marker).unwrap()).into_bytes(),
            ),
            Arc::new(SecretPolicy::default()),
        );
        let b = eliot_plugin::spawn_stderr(
            std::io::Cursor::new(b"ordinary stderr\n"),
            Arc::new(SecretPolicy::default()),
        );
        let (view_a, view_b) = (a.view(), b.view());
        a.finish(Instant::now() + RPC_DEADLINE).await;
        b.finish(Instant::now() + RPC_DEADLINE).await;
        let original = Error::new("native_not_dispatched", "no effect");
        let other = diagnose_error(original.clone(), &view_b, &[]);
        assert!(other.origin_evidence.is_none());
        let owned = diagnose_error(original.clone(), &view_a, &[]);
        assert!(
            owned
                .origin_evidence
                .as_deref()
                .unwrap()
                .contains("plugin.rs:42")
        );
        for error in [owned, other] {
            assert_eq!(error.code, original.code);
            assert_eq!(
                eliot_core::classify_dispatched_error(&error.code),
                eliot_core::CommandState::Rejected
            );
        }
    }
    use eliot_core::{AgentState, CommandState, NativeEvent, SubmitCommand};

    #[test]
    fn target_environment_scopes_identity_to_the_issuing_data_dir() {
        let mut agent = Agent::new("a1", "proj", "muse", "fake", 0);
        agent.generation = 1;
        agent.profile_snapshot = json!({
            "pass_env_values": {"CODEX_HOME": null, "CODEX_SQLITE_HOME": null},
        });
        let data_root =
            std::env::temp_dir().join(format!("eliot-q20-spawn-test-{}", std::process::id()));
        let environment = build_target_environment(&data_root, &agent, "agent-token")
            .expect("target environment");
        let data_dir = environment
            .get("ELIOT_AGENT_DATA_DIR")
            .and_then(Value::as_str)
            .expect("issuing data dir is injected");
        assert_eq!(
            Path::new(data_dir),
            data_root.as_path(),
            "the issuing data dir is injected normalized"
        );
        assert_eq!(
            environment.get("ELIOT_HOME").and_then(Value::as_str),
            data_root.to_str(),
        );
        assert_eq!(
            environment.get("ELIOT_AGENT_ID").and_then(Value::as_str),
            Some("a1"),
        );
        assert!(
            crate::daemon::identity_data_dirs_equal(&data_root.join("."), Path::new(data_dir)),
            "spelling variants of the issuing dir stay in scope"
        );
        assert!(
            !crate::daemon::identity_data_dirs_equal(
                &data_root.join("sibling"),
                Path::new(data_dir)
            ),
            "a sibling dir is out of scope"
        );
    }

    fn resume_actor(
        state: Weak<HostState>,
        monitoring: Monitoring,
        command_id: &str,
    ) -> AgentActorConfig {
        let agent = Agent::new("a1", "proj", "opencode", "fake", 0);
        let command = Command {
            id: command_id.to_owned(),
            principal: "owner".to_owned(),
            method: "command.submit".to_owned(),
            verb: "resume".to_owned(),
            target: Some("a1".to_owned()),
            selector: None,
            expected_generation: None,
            generation: Some(1),
            args: json!({}),
            state: CommandState::Dispatched,
            turn_id: None,
            request_id: None,
            outcome: None,
            created_at: 0,
            updated_at: 0,
            probe: false,
            config_revision: 0,
            config: None,
        };
        let capture = eliot_plugin::spawn_stderr(
            std::io::Cursor::new(Vec::new()),
            Arc::new(SecretPolicy::default()),
        );
        AgentActorConfig {
            state,
            monitoring,
            key: AgentKey {
                id: "a1".to_owned(),
                generation: 1,
            },
            agent,
            start_command: command,
            instance_dir: "synthetic".to_owned(),
            store_root: PathBuf::from("synthetic-data-root"),
            plugin_credential: "credential".to_owned(),
            target_environment: json!({}),
            source_credentials: Arc::from(["secret".to_owned()]),
            resume_from: None,
            source_state: None,
            stderr: capture.view(),
        }
    }

    #[tokio::test]
    async fn resume_config_dead_host_state_fails_startup() {
        let store = Arc::new(AsyncMutex::new(
            eliot_store::Store::open(Path::new(":memory:")).expect("open store"),
        ));
        let monitoring = Monitoring::new(store, Bus::new(16), Arc::new(Mutex::new(HashSet::new())));
        let actor = resume_actor(Weak::new(), monitoring, "resume-1");
        let error = resolve_resume_config(&actor)
            .await
            .expect_err("dead host state fails startup");
        assert_eq!(error.code, "unknown");
    }

    #[tokio::test]
    async fn resume_config_missing_command_fails_startup() {
        let store = Arc::new(AsyncMutex::new(
            eliot_store::Store::open(Path::new(":memory:")).expect("open store"),
        ));
        let bus = Bus::new(64);
        let secrets = Arc::new(Mutex::new(HashSet::new()));
        let monitoring = Monitoring::new(store.clone(), bus.clone(), secrets.clone());
        let (progress, _) = watch::channel(0);
        let host = PluginHost::new(
            PluginHostOptions::new(
                PathBuf::from("synthetic-data-root"),
                "owner-test".to_owned(),
            ),
            store.clone(),
            bus.clone(),
            secrets,
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashSet::new())),
            progress,
            monitoring.clone(),
        );
        let actor = resume_actor(Arc::downgrade(&host.state), monitoring, "resume-missing");
        let error = resolve_resume_config(&actor)
            .await
            .expect_err("missing resume command fails startup");
        assert_eq!(error.code, "not_found");
    }

    #[test]
    fn send_params_carry_the_resolved_revision_overrides() {
        let mut agent = Agent::new("a1", "proj", "codex", "fake", 0);
        agent.generation = 1;
        agent.state = AgentState::Idle;
        let command = Command {
            id: "send-1".to_owned(),
            principal: "owner".to_owned(),
            method: "command.submit".to_owned(),
            verb: "send".to_owned(),
            target: Some("a1".to_owned()),
            selector: None,
            expected_generation: Some(1),
            generation: Some(1),
            args: json!({"text": "hi"}),
            state: CommandState::Dispatched,
            turn_id: None,
            request_id: None,
            outcome: None,
            created_at: 0,
            updated_at: 0,
            probe: false,
            config_revision: 2,
            config: None,
        };
        let config = eliot_store::AgentConfig {
            agent: "a1".to_owned(),
            generation: 1,
            revision: 2,
            requested_model: Some("m1".to_owned()),
            requested_effort: None,
            model: Some("m1".to_owned()),
            effort: Some("low".to_owned()),
            application: "next_turn".to_owned(),
            provenance: "configure".to_owned(),
            command_id: "configure-1".to_owned(),
            created_at: 0,
        };
        let params = agent_command_params(&agent, &command, Some(7), Some(&config)).unwrap();
        assert_eq!(params["model_override"], json!("m1"));
        assert_eq!(params["effort_override"], json!("low"));
        assert_eq!(params["config_revision"], json!(2));
        assert_eq!(params["dispatch_seq"], json!(7));
        // Frozen sends keep their exact historical params.
        let plain = agent_command_params(&agent, &command, None, None).unwrap();
        assert!(plain.get("model_override").is_none());
        assert!(plain.get("effort_override").is_none());
        assert!(plain.get("config_revision").is_none());
        // Overlays never leak into other verbs.
        let steer = Command {
            verb: "steer".to_owned(),
            turn_id: Some("turn-1".to_owned()),
            ..command.clone()
        };
        let steered = agent_command_params(&agent, &steer, None, Some(&config)).unwrap();
        assert!(steered.get("model_override").is_none());
        assert!(steered.get("config_revision").is_none());
    }

    #[test]
    fn resume_source_generation_uses_the_checked_dispatched_predecessor() {
        let command = Command {
            id: "resume-generation".to_owned(),
            principal: "owner".to_owned(),
            method: "command.submit".to_owned(),
            verb: "resume".to_owned(),
            target: Some("a7".to_owned()),
            selector: None,
            expected_generation: None,
            generation: Some(3),
            args: json!({}),
            state: CommandState::Dispatched,
            turn_id: None,
            request_id: None,
            outcome: None,
            created_at: 0,
            updated_at: 0,
            probe: false,
            config_revision: 0,
            config: None,
        };
        assert_eq!(resume_source_generation(&command).unwrap(), 2);

        let explicit = Command {
            expected_generation: Some(2),
            ..command.clone()
        };
        assert_eq!(resume_source_generation(&explicit).unwrap(), 2);

        let mismatched = Command {
            expected_generation: Some(1),
            ..command.clone()
        };
        assert_eq!(
            resume_source_generation(&mismatched).unwrap_err().code,
            "stale_generation"
        );

        let zero_target = Command {
            generation: Some(0),
            ..command.clone()
        };
        assert_eq!(
            resume_source_generation(&zero_target).unwrap_err().code,
            "stale_generation"
        );
        let first_generation_target = Command {
            generation: Some(1),
            ..command.clone()
        };
        assert_eq!(
            resume_source_generation(&first_generation_target)
                .unwrap_err()
                .code,
            "stale_generation"
        );
        let negative_target = Command {
            generation: Some(-1),
            ..command
        };
        assert_eq!(
            resume_source_generation(&negative_target).unwrap_err().code,
            "stale_generation"
        );
    }

    fn test_manifest(name: &str, harness_version: &str) -> PluginManifest {
        use std::collections::BTreeMap;
        let controlled = BTreeMap::from([
            ("message".to_owned(), "stream".to_owned()),
            ("session".to_owned(), "stream".to_owned()),
            ("turn".to_owned(), "stream".to_owned()),
        ]);
        PluginManifest {
            name: name.to_owned(),
            version: "test".to_owned(),
            protocol: "eliot-plugin/1".to_owned(),
            harness_version: harness_version.to_owned(),
            available: true,
            mode: "per_agent".to_owned(),
            verbs: vec![
                "launch".to_owned(),
                "resume".to_owned(),
                "send".to_owned(),
                "stop".to_owned(),
                "interrupt".to_owned(),
            ],
            child_verbs: Vec::new(),
            features: eliot_plugin::protocol::ManifestFeatures {
                start_requires_input: false,
                native_children: false,
                discover: true,
                observe: true,
            },
            sources: BTreeMap::from([
                ("controlled".to_owned(), controlled),
                (
                    "observed".to_owned(),
                    BTreeMap::from([
                        ("message".to_owned(), "native_read".to_owned()),
                        ("session".to_owned(), "native_read".to_owned()),
                        ("turn".to_owned(), "native_read".to_owned()),
                    ]),
                ),
            ]),
            proxy: "none".to_owned(),
        }
    }

    #[test]
    fn route_binding_requires_the_existing_responses_manifest_capability() {
        let direct = json!({"harness":"codex"});
        let routed = json!({"harness":"codex", "route_binding":{"name":"opencodex"}});
        let mut manifest = test_manifest("codex", "0.161.0");

        require_proxy_capability(&direct, &manifest).unwrap();
        assert_eq!(
            require_proxy_capability(&routed, &manifest)
                .unwrap_err()
                .code,
            "unsupported"
        );
        manifest.proxy = "responses".to_owned();
        require_proxy_capability(&direct, &manifest).unwrap();
        require_proxy_capability(&routed, &manifest).unwrap();
    }

    #[test]
    fn controlled_codex_manifest_allows_tool_stream_family() {
        let mut codex = test_manifest("codex", "0.161.0");
        codex
            .sources
            .get_mut("controlled")
            .expect("Codex controlled sources")
            .insert("request".to_owned(), "stream".to_owned());
        codex
            .sources
            .get_mut("controlled")
            .unwrap()
            .insert("tool".to_owned(), "stream".to_owned());
        assert_eq!(
            manifest_source_plan(&codex, "codex")
                .unwrap()
                .as_object()
                .unwrap()
                .len(),
            5
        );
        let mut unknown_harness = codex.clone();
        unknown_harness.name = "unknown-harness".to_owned();
        assert_eq!(
            manifest_source_plan(&unknown_harness, "unknown-harness")
                .unwrap_err()
                .code,
            "unsupported"
        );
    }

    #[test]
    fn controlled_opencode_manifest_requires_request_stream_family() {
        let mut opencode = test_manifest("opencode", "2.0.32");
        // Capability validation is generic since #332: only the
        // message/session/turn families are mandatory, request stays optional.
        let plan = manifest_source_plan(&opencode, "opencode")
            .expect("OpenCode manifest without request is supported");
        assert_eq!(plan.as_object().unwrap().len(), 3);
        for family in ["message", "session", "turn"] {
            assert_eq!(plan[family], "stream");
        }
        opencode
            .sources
            .get_mut("controlled")
            .expect("controlled sources")
            .insert("request".to_owned(), "stream".to_owned());
        let plan = manifest_source_plan(&opencode, "opencode")
            .expect("OpenCode request stream is supported");
        assert_eq!(plan.as_object().unwrap().len(), 4);
        for family in ["message", "request", "session", "turn"] {
            assert_eq!(plan[family], "stream");
        }
    }

    const SECRET: &str = "__payload_guard_secret__";
    const SECRET_ERROR: &str =
        "native source payload contains a known secret in immutable metadata";

    #[test]
    fn native_store_metadata_excludes_selector_contributions_not_matching_secrets() {
        let profile = json!({"pass_env":["CODEX_HOME", "CODEX_SQLITE_HOME", "OTHER_CREDENTIAL"]});
        let environment = json!({"CODEX_HOME":"same-literal", "CODEX_SQLITE_HOME":"sqlite-only", "OTHER_CREDENTIAL":"same-literal"});
        let policy = metadata_secret_snapshot(
            &profile,
            &environment,
            vec![
                "owner-secret".to_owned(),
                "instance-secret".to_owned(),
                "agent-secret".to_owned(),
            ],
        );
        let policy = SecretPolicy::new(policy.iter());
        assert!(!policy.contains_text("sqlite-only"));
        for value in [
            "same-literal",
            "owner-secret",
            "instance-secret",
            "agent-secret",
        ] {
            assert!(policy.contains_text(value));
        }
        let selectors_only = json!({"pass_env":["CODEX_HOME", "CODEX_SQLITE_HOME"]});
        let metadata = metadata_secret_snapshot(
            &selectors_only,
            &environment,
            vec!["same-literal".to_owned()],
        );
        assert!(SecretPolicy::new(metadata.iter()).contains_text("same-literal"));
        let diagnostics = source_secret_snapshot(&selectors_only, &environment, Vec::new());
        assert!(SecretPolicy::new(diagnostics.iter()).contains_text("sqlite-only"));
    }

    #[test]
    fn native_store_inputs_runtime_uses_capture_and_final_secret_environment() {
        let mut profile =
            json!({"pass_env":["CODEX_HOME", "CODEX_SQLITE_HOME", "EPHEMERAL_TOKEN"]});
        crate::config::capture_native_store_inputs(&mut profile, |name| match name {
            "CODEX_HOME" => Some(OsString::from("../original-home")),
            "CODEX_SQLITE_HOME" => None,
            _ => panic!("only selectors may be captured"),
        })
        .unwrap();
        for current in ["credential-before-rotation", "credential-after-rotation"] {
            let environment = Value::Object(
                build_process_environment_with(&profile, |name| {
                    assert!(
                        crate::config::native_store_selector(name).is_none(),
                        "captured selectors must not be reread"
                    );
                    (name == "EPHEMERAL_TOKEN").then(|| OsString::from(current))
                })
                .unwrap(),
            );
            assert_eq!(environment["CODEX_HOME"], "../original-home");
            assert!(environment.get("CODEX_SQLITE_HOME").is_none());
            assert_eq!(environment["EPHEMERAL_TOKEN"], current);
            let secrets = source_secret_snapshot(&profile, &environment, Vec::new());
            assert!(secrets.iter().any(|value| value == current));
            assert!(secrets.iter().any(|value| value == "../original-home"));
            assert!(!profile.to_string().contains(current));
        }
        profile["pass_env_values"]["CODEX_HOME"] = json!("");
        let environment = build_process_environment_with(&profile, |name| {
            assert!(crate::config::native_store_selector(name).is_none());
            None
        })
        .unwrap();
        assert_eq!(environment["CODEX_HOME"], "");
    }

    #[test]
    fn legacy_and_changed_native_routes_cannot_reuse_quota() {
        let legacy = json!({"harness":"claude", "pass_env":["CLAUDE_CONFIG_DIR","ANTHROPIC_BASE_URL"],
            "pass_env_values":{"CODEX_HOME":null, "CODEX_SQLITE_HOME":null}});
        let lookup =
            |name: &str| (name == "CLAUDE_CONFIG_DIR").then(|| OsString::from("private-dir"));
        let (environment, verified) = prepare_process_environment(&legacy, lookup).unwrap();
        assert_eq!(environment["CLAUDE_CONFIG_DIR"], "private-dir");
        assert!(verified.is_none());
        let mut captured = legacy.clone();
        crate::config::capture_native_store_inputs_with_lookups(&mut captured, lookup, |_| None)
            .unwrap();
        let (_, verified) = prepare_process_environment(&captured, lookup).unwrap();
        assert_eq!(
            verified.as_deref(),
            captured["native_route_identity"].as_str()
        );
        for changed in ["CLAUDE_CONFIG_DIR", "ANTHROPIC_BASE_URL"] {
            let result = prepare_process_environment(&captured, |name| {
                if name == changed {
                    Some(OsString::from("changed"))
                } else {
                    lookup(name)
                }
            });
            assert_eq!(result.unwrap_err().code, "unavailable");
        }
        let mut states = std::collections::HashSet::new();
        for (names, selected) in [
            (json!([]), None),
            (json!(["CLAUDE_CONFIG_DIR"]), None),
            (json!(["CLAUDE_CONFIG_DIR"]), Some("")),
        ] {
            let mut snapshot = legacy.clone();
            snapshot["pass_env"] = names;
            crate::config::capture_native_store_inputs_with_lookups(
                &mut snapshot,
                |name| {
                    (name == "CLAUDE_CONFIG_DIR")
                        .then_some(selected)
                        .flatten()
                        .map(OsString::from)
                },
                |_| None,
            )
            .unwrap();
            assert!(
                states.insert(
                    snapshot["native_route_identity"]
                        .as_str()
                        .unwrap()
                        .to_owned()
                )
            );
        }
    }

    #[test]
    fn native_store_inputs_malformed_capture_is_rejected() {
        for capture in [
            Value::Null,
            json!([]),
            json!({}),
            json!({"CODEX_HOME":"x"}),
            json!({"CODEX_HOME":false,"CODEX_SQLITE_HOME":null}),
            json!({"CODEX_HOME":null,"CODEX_SQLITE_HOME":null,"EPHEMERAL_TOKEN":"never durable"}),
        ] {
            let profile = json!({"pass_env":["CODEX_HOME"],"pass_env_values":capture});
            assert_eq!(
                build_process_environment_with(&profile, |_| panic!(
                    "malformed capture must reject before lookup"
                ))
                .unwrap_err()
                .code,
                "invalid"
            );
        }
    }

    #[test]
    fn windows_baseline_forwards_systemdrive_and_programdata() {
        let profile = json!({"pass_env_values": {"CODEX_HOME": null, "CODEX_SQLITE_HOME": null}});
        let environment = build_process_environment_with(&profile, |name| match name {
            "SystemDrive" => Some(OsString::from("C:")),
            "ProgramData" => Some(OsString::from("C:\\ProgramData")),
            _ => None,
        })
        .unwrap();
        #[cfg(windows)]
        {
            assert_eq!(environment["SystemDrive"], "C:");
            assert_eq!(environment["ProgramData"], "C:\\ProgramData");
        }
        #[cfg(not(windows))]
        {
            assert!(environment.get("SystemDrive").is_none());
            assert!(environment.get("ProgramData").is_none());
        }
        let missing = build_process_environment_with(&profile, |_| None).unwrap();
        assert!(missing.get("SystemDrive").is_none());
        assert!(missing.get("ProgramData").is_none());
    }

    fn batch() -> SourceBatch {
        SourceBatch {
            batch_id: "batch-1".to_owned(),
            agent_id: "a1".to_owned(),
            generation: 1,
            source: "stream".to_owned(),
            cursor: Some("cursor-1".to_owned()),
            events: vec![NativeEvent {
                ts: None,
                kind: "session.ready".to_owned(),
                native_id: "ready-1".to_owned(),
                data: json!({"command_id":"launch-command", "text":SECRET}),
            }],
        }
    }

    fn gap() -> SourceGap {
        SourceGap {
            gap_id: "gap-1".to_owned(),
            agent_id: "a1".to_owned(),
            generation: 1,
            source: "stream".to_owned(),
            families: vec!["session".to_owned()],
            reason: format!("reader lost {SECRET}"),
            from: Some("cursor-1".to_owned()),
            to: Some("cursor-2".to_owned()),
        }
    }

    fn assert_secret_error(error: Error) {
        assert_eq!(error.code, "secret_in_payload");
        assert_eq!(error.message.as_str(), SECRET_ERROR);
        assert!(!serde_json::to_string(&error).unwrap().contains(SECRET));
    }

    #[test]
    fn source_batch_rejects_secret_identifiers_without_mutation() {
        for path in [
            "/batch_id",
            "/agent_id",
            "/source",
            "/cursor",
            "/events/0/native_id",
            "/events/0/kind",
        ] {
            let mut value = serde_json::to_value(batch()).unwrap();
            *value.pointer_mut(path).unwrap() = json!(format!("prefix-{SECRET}"));
            let mut batch: SourceBatch = serde_json::from_value(value).unwrap();
            let original = batch.clone();
            assert_secret_error(redact_source_batch(&mut batch, &[SECRET.to_owned()]).unwrap_err());
            assert_eq!(batch, original, "immutable field {path}");
        }
    }

    #[test]
    fn source_batch_rejects_nested_keys_and_correlations_before_text_redaction() {
        let mut cases = vec![json!({"nested":[{"deeper":{(SECRET):"safe"}}]})];
        for field in [
            "agent_id",
            "command_id",
            "turn_id",
            "request_id",
            "item_id",
            "native_scope",
            "native_ref",
        ] {
            cases.push(json!({"nested":[{(field):SECRET}]}));
        }
        for data in cases {
            let mut batch = batch();
            // A later invalid event must not leave earlier mutable text changed.
            batch.events.push(NativeEvent {
                ts: None,
                kind: "message.completed".to_owned(),
                native_id: "message-1".to_owned(),
                data,
            });
            let original = batch.clone();
            assert_secret_error(redact_source_batch(&mut batch, &[SECRET.to_owned()]).unwrap_err());
            assert_eq!(batch, original);
        }
    }

    #[test]
    fn source_batch_redacts_only_mutable_values() {
        let mut batch = batch();
        batch.events[0].data["nested"] = json!([{"body":format!("text {SECRET}")}]);
        let mut expected = batch.clone();
        expected.events[0].data["text"] = json!("[REDACTED]");
        expected.events[0].data["nested"][0]["body"] = json!("text [REDACTED]");
        expected.events[0].data["redacted"] = json!(true);
        redact_source_batch(&mut batch, &[SECRET.to_owned()]).unwrap();
        assert_eq!(batch, expected);
    }

    #[test]
    fn source_gap_rejects_secret_metadata_and_redacts_only_reason() {
        for path in [
            "/gap_id",
            "/agent_id",
            "/source",
            "/families/0",
            "/from",
            "/to",
        ] {
            let mut value = serde_json::to_value(gap()).unwrap();
            *value.pointer_mut(path).unwrap() = json!(SECRET);
            let mut gap: SourceGap = serde_json::from_value(value).unwrap();
            let original = gap.clone();
            assert_secret_error(redact_source_gap(&mut gap, &[SECRET.to_owned()]).unwrap_err());
            assert_eq!(gap, original, "immutable field {path}");
        }
        let mut gap = gap();
        let mut expected = gap.clone();
        expected.reason = "reader lost [REDACTED]".to_owned();
        redact_source_gap(&mut gap, &[SECRET.to_owned()]).unwrap();
        assert_eq!(gap, expected);
    }

    #[tokio::test]
    async fn source_payload_guards_precede_store_commit_and_bus_publish() {
        let mut store = eliot_store::Store::open(Path::new(":memory:")).unwrap();
        let command_id = Ulid::new().to_string();
        let now = now_ms();
        let admission = store
            .submit(
                "owner",
                SubmitCommand {
                    id: command_id.clone(),
                    target: None,
                    selector: None,
                    verb: "launch".to_owned(),
                    args: json!({"project":"payload-guard", "workspace":"synthetic-workspace", "harness":"codex", "profile":"fixture", "profile_snapshot":{}}),
                    expected_generation: Some(0),
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                },
                now,
            )
            .unwrap();
        let agent_id = admission.command.target.unwrap();
        store.dispatch(&command_id, now).unwrap();
        crate::test_support::install_fixture_sources(&mut store, &agent_id, &command_id, 1, now);
        let before_events = store.read_events(0, 1000, None).unwrap();
        let before_source = store.read_source_state(&agent_id, 1, "stream").unwrap();
        let store = Arc::new(AsyncMutex::new(store));
        let bus = Bus::new(16);
        let mut published = bus.subscribe();
        let credential = "private-payload-guard";
        let (host_io, peer_io) = tokio::io::duplex(16 * 1024);
        let (host_read, host_write) = tokio::io::split(host_io);
        let (peer_read, peer_write) = tokio::io::split(peer_io);
        let host = PluginConnection::new(host_read, host_write);
        let peer = PluginConnection::new(peer_read, peer_write);
        let client = peer.client();
        let task = tokio::spawn(serve_plugin_requests(
            host,
            EventSource {
                store: store.clone(),
                bus: bus.clone(),
                monitoring: Monitoring::new(
                    store.clone(),
                    bus,
                    Arc::new(Mutex::new(HashSet::new())),
                ),
                source_credentials: Arc::from([credential.to_owned(), SECRET.to_owned()]),
                state: Weak::new(),
                key: AgentKey {
                    id: agent_id.clone(),
                    generation: 1,
                },
                agent_id: agent_id.clone(),
                generation: 1,
                expected_credential: credential.to_owned(),
                observation: None,
            },
        ));
        let mut batch = batch();
        batch.agent_id = agent_id.clone();
        batch.events[0].data["command_id"] = json!(command_id);
        for path in ["/batch_id", "/cursor", "/events/0/native_id"] {
            let mut params = serde_json::to_value(&batch).unwrap();
            *params.pointer_mut(path).unwrap() = json!(SECRET);
            params["credential"] = json!(credential);
            assert_secret_error(
                client
                    .call("events.put", params, deadline())
                    .await
                    .unwrap_err(),
            );
        }
        let mut params = serde_json::to_value(&batch).unwrap();
        params["events"][0]["data"]["nested"] = json!([{"deeper":{(SECRET):"safe"}}]);
        params["credential"] = json!(credential);
        assert_secret_error(
            client
                .call("events.put", params, deadline())
                .await
                .unwrap_err(),
        );

        let mut gap = gap();
        gap.agent_id = agent_id.clone();
        for path in ["/gap_id", "/source", "/families/0", "/from", "/to"] {
            let mut params = serde_json::to_value(&gap).unwrap();
            *params.pointer_mut(path).unwrap() = json!(SECRET);
            params["credential"] = json!(credential);
            assert_secret_error(
                client
                    .call("source.gap", params, deadline())
                    .await
                    .unwrap_err(),
            );
        }
        {
            let store = store.lock().await;
            assert_eq!(store.read_events(0, 1000, None).unwrap(), before_events);
            assert_eq!(
                store.read_source_state(&agent_id, 1, "stream").unwrap(),
                before_source
            );
        }
        assert!(matches!(
            published.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));

        // The same live boundary accepts valid payloads and commits safe text
        // without changing the IDs/cursors acknowledged to the producer.
        let mut params = serde_json::to_value(&batch).unwrap();
        params["credential"] = json!(credential);
        let ack = client.call("events.put", params, deadline()).await.unwrap();
        assert_eq!(ack["cursor"], json!("cursor-1"));
        let event = published.try_recv().unwrap();
        assert_eq!(event.native_id.as_deref(), Some("ready-1"));
        assert_eq!(event.data["text"], json!("[REDACTED]"));
        assert_eq!(event.data["redacted"], json!(true));
        let mut params = serde_json::to_value(&gap).unwrap();
        params["credential"] = json!(credential);
        let ack = client.call("source.gap", params, deadline()).await.unwrap();
        assert_eq!(ack["gap_id"], json!("gap-1"));
        let events = store.lock().await.read_events(0, 1000, None).unwrap();
        let gap_event = events
            .iter()
            .find(|event| event.kind == "source.gap")
            .unwrap();
        assert_eq!(gap_event.data["reason"], json!("reader lost [REDACTED]"));
        assert!(!serde_json::to_string(&events).unwrap().contains(SECRET));

        client.close_input(deadline()).await.unwrap();
        timeout_at(deadline(), task).await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn events_put_malformed_params_are_definite_invalid() {
        let mut store = eliot_store::Store::open(Path::new(":memory:")).unwrap();
        let command_id = Ulid::new().to_string();
        let now = now_ms();
        let agent_id = store
            .submit(
                "owner",
                SubmitCommand {
                    id: command_id.clone(),
                    target: None,
                    selector: None,
                    verb: "launch".to_owned(),
                    args: json!({"project":"malformed-guard", "workspace":"synthetic-workspace", "harness":"codex", "profile":"fixture", "profile_snapshot":{}}),
                    expected_generation: Some(0),
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                },
                now,
            )
            .unwrap()
            .command
            .target
            .unwrap();
        store.dispatch(&command_id, now).unwrap();
        crate::test_support::install_fixture_sources(&mut store, &agent_id, &command_id, 1, now);
        let before = store.high_water().unwrap();
        let store = Arc::new(AsyncMutex::new(store));
        let bus = Bus::new(16);
        let mut published = bus.subscribe();
        let credential = "private-malformed-guard";
        let (host_io, peer_io) = tokio::io::duplex(16 * 1024);
        let (host_read, host_write) = tokio::io::split(host_io);
        let (peer_read, peer_write) = tokio::io::split(peer_io);
        let host = PluginConnection::new(host_read, host_write);
        let peer = PluginConnection::new(peer_read, peer_write);
        let client = peer.client();
        let task = tokio::spawn(serve_plugin_requests(
            host,
            EventSource {
                store: store.clone(),
                bus: bus.clone(),
                monitoring: Monitoring::new(
                    store.clone(),
                    bus,
                    Arc::new(Mutex::new(HashSet::new())),
                ),
                source_credentials: Arc::from([credential.to_owned()]),
                state: Weak::new(),
                key: AgentKey {
                    id: agent_id.clone(),
                    generation: 1,
                },
                agent_id: agent_id.clone(),
                generation: 1,
                expected_credential: credential.to_owned(),
                observation: None,
            },
        ));
        // Decode happens before the Store transaction opens, so a malformed
        // body is a definite rejection: never committed, never `unknown`.
        for params in [
            json!({"credential": credential}),
            json!({"credential": credential, "agent_id": agent_id, "generation": "one"}),
        ] {
            let error = client
                .call("events.put", params, deadline())
                .await
                .unwrap_err();
            assert_eq!(error.code, "invalid");
        }
        assert_eq!(store.lock().await.high_water().unwrap(), before);
        assert!(matches!(
            published.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));
        client.close_input(deadline()).await.unwrap();
        timeout_at(deadline(), task).await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn source_handoff_private_boundary_readback_and_owned_exit_follow_owners() {
        let mut store = eliot_store::Store::open(Path::new(":memory:")).unwrap();
        let command_id = Ulid::new().to_string();
        let agent_id = store
            .submit(
                "owner",
                SubmitCommand {
                    id: command_id.clone(),
                    target: None,
                    selector: None,
                    verb: "launch".to_owned(),
                    args: json!({"project":"handoff-boundary", "workspace":"synthetic-workspace", "harness":"codex", "profile":"fixture", "profile_snapshot":{}}),
                    expected_generation: Some(0),
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                },
                now_ms(),
            )
            .unwrap()
            .command
            .target
            .unwrap();
        store.dispatch(&command_id, now_ms()).unwrap();
        crate::test_support::install_fixture_source_plan(
            &mut store,
            &agent_id,
            &command_id,
            1,
            now_ms(),
            &["message", "request", "session", "turn"],
        );
        store
            .put_batch(
                SourceBatch {
                    batch_id: "initial-ready".to_owned(),
                    agent_id: agent_id.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    cursor: Some("edge-1".to_owned()),
                    events: vec![NativeEvent {
                        ts: None,
                        kind: "session.ready".to_owned(),
                        native_id: "initial-ready".to_owned(),
                        data: json!({"command_id":command_id}),
                    }],
                },
                now_ms(),
            )
            .unwrap();
        store
            .settle(
                &command_id,
                eliot_core::CommandState::Applied,
                None,
                now_ms(),
            )
            .unwrap();
        let before = store.high_water().unwrap();
        let store = Arc::new(AsyncMutex::new(store));
        let bus = Bus::new(64);
        let mut published = bus.subscribe();
        let secrets = Arc::new(Mutex::new(HashSet::new()));
        let monitoring = Monitoring::new(store.clone(), bus.clone(), secrets.clone());
        let (progress, _) = watch::channel(0);
        let host = PluginHost::new(
            PluginHostOptions::new(
                PathBuf::from("synthetic-data-root"),
                "owner-test".to_owned(),
            ),
            store.clone(),
            bus.clone(),
            secrets,
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashSet::new())),
            progress,
            monitoring.clone(),
        );
        let key = AgentKey {
            id: agent_id.clone(),
            generation: 1,
        };
        let credential = "private-handoff-peer";
        let (host_io, peer_io) = tokio::io::duplex(16 * 1024);
        let (host_read, host_write) = tokio::io::split(host_io);
        let (peer_read, peer_write) = tokio::io::split(peer_io);
        let peer = PluginConnection::new(peer_read, peer_write);
        let client = peer.client();
        let task = tokio::spawn(serve_plugin_requests(
            PluginConnection::new(host_read, host_write),
            EventSource {
                store: store.clone(),
                bus: bus.clone(),
                monitoring,
                source_credentials: Arc::from([credential.to_owned(), SECRET.to_owned()]),
                state: Arc::downgrade(&host.state),
                key: key.clone(),
                agent_id: agent_id.clone(),
                generation: 1,
                expected_credential: credential.to_owned(),
                observation: None,
            },
        ));
        let handoff = json!({
            "credential":credential, "agent_id":agent_id, "generation":1,
            "source":"stream", "families":["message","turn"],
            "successor":"transcript", "mechanism":"fake_recovery", "cursor":"edge-1",
        });
        for (field, value, code) in [
            ("credential", json!("wrong-private-peer"), "not_allowed"),
            ("agent_id", json!("a999"), "not_allowed"),
            ("generation", json!(2), "stale_generation"),
            ("cursor", json!(SECRET), "secret_in_payload"),
        ] {
            let mut params = handoff.clone();
            params[field] = value;
            let error = client
                .call("source.handoff", params, deadline())
                .await
                .unwrap_err();
            assert_eq!(error.code, code, "{field}");
            assert!(!serde_json::to_string(&error).unwrap().contains(SECRET));
        }
        assert_eq!(store.lock().await.high_water().unwrap(), before);
        assert!(matches!(
            published.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));
        // Cancel the pending caller after observing its committed publication,
        // without consuming the wire ACK. Recover only through source.state.
        let first_event = {
            let pending_ack = client.call("source.handoff", handoff.clone(), deadline());
            tokio::pin!(pending_ack);
            tokio::select! {
                biased;
                event = published.recv() => event.unwrap(),
                _ = &mut pending_ack => panic!("handoff replied before committed publication"),
            }
        };
        let committed = store.lock().await.high_water().unwrap();
        let state = client
            .call(
                "source.state",
                json!({
                    "credential":credential, "agent_id":agent_id, "generation":1, "source":"stream",
                }),
                deadline(),
            )
            .await
            .unwrap();
        let ack = state["last_handoff"].clone();
        assert!(ack.is_object());
        assert_eq!(ack["seq"].as_i64(), Some(committed));
        let persisted = store.lock().await.read_events(before, 1000, None).unwrap();
        assert!(persisted.contains(&first_event));
        let mut delivered_ack_event = Some(first_event.seq) == ack["seq"].as_i64();
        while let Ok(event) = published.try_recv() {
            assert!(persisted.contains(&event));
            delivered_ack_event |= Some(event.seq) == ack["seq"].as_i64();
        }
        assert!(delivered_ack_event);
        // No native operation is retried to obtain the retained handoff proof.
        assert!(
            state["owners"]
                .as_array()
                .unwrap()
                .iter()
                .any(|owner| owner["family"] == "turn"
                    && owner["source"] == "transcript"
                    && owner["mechanism"] == "fake_recovery")
        );
        assert!(
            state["owners"]
                .as_array()
                .unwrap()
                .iter()
                .any(|owner| owner["family"] == "request" && owner["source"] == "stream")
        );
        assert_eq!(
            client
                .call("source.handoff", handoff.clone(), deadline())
                .await
                .unwrap(),
            ack
        );
        assert_eq!(store.lock().await.high_water().unwrap(), committed);
        assert!(matches!(
            published.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));

        let mut mixed = handoff.clone();
        mixed["families"] = json!(["session", "turn"]);
        mixed["cursor"] = json!("mixed-edge");
        assert_eq!(
            client
                .call("source.handoff", mixed, deadline())
                .await
                .unwrap_err()
                .code,
            "source_integrity"
        );
        assert_eq!(store.lock().await.high_water().unwrap(), committed);
        let mut remainder = handoff.clone();
        remainder["families"] = json!(["request", "session"]);
        remainder["cursor"] = json!("edge-1");
        let final_ack = client
            .call("source.handoff", remainder, deadline())
            .await
            .unwrap();
        let after_final = store.lock().await.high_water().unwrap();
        assert_eq!(
            client
                .call("source.handoff", handoff, deadline())
                .await
                .unwrap_err()
                .code,
            "source_integrity"
        );
        assert_eq!(store.lock().await.high_water().unwrap(), after_final);
        let predecessor = client
            .call(
                "source.state",
                json!({
                    "credential":credential, "agent_id":agent_id, "generation":1, "source":"stream",
                }),
                deadline(),
            )
            .await
            .unwrap();
        assert_eq!(predecessor["families"], json!([]));
        assert_eq!(predecessor["state"], json!("open"));
        assert_eq!(predecessor["last_handoff"], final_ack);
        assert!(
            predecessor["owners"]
                .as_array()
                .unwrap()
                .iter()
                .any(|owner| {
                    owner["family"] == "request"
                        && owner["source"] == "transcript"
                        && owner["mechanism"] == "fake_recovery"
                })
        );

        let send_id = Ulid::new().to_string();
        {
            let mut store = store.lock().await;
            store
                .submit(
                    "owner",
                    SubmitCommand {
                        id: send_id.clone(),
                        target: Some(agent_id.clone()),
                        selector: None,
                        verb: "send".to_owned(),
                        args: json!({"text":"successor fact"}),
                        expected_generation: Some(1),
                        expected_turn_id: None,
                        request_id: None,
                        probe: false,
                    },
                    now_ms(),
                )
                .unwrap();
            store.dispatch(&send_id, now_ms()).unwrap();
        }
        client.call("events.put", json!({
            "credential":credential, "agent_id":agent_id, "generation":1,
            "batch_id":"successor-facts", "source":"transcript", "cursor":"tail-1",
            "events":[
                {"kind":"turn.started", "native_id":"successor-turn", "data":{"command_id":send_id, "turn_id":"recovered-turn"}},
                {"kind":"message.completed", "native_id":"successor-message", "data":{"command_id":send_id, "turn_id":"recovered-turn", "text":"recovered"}}
            ],
        }), deadline()).await.unwrap();
        assert_eq!(
            store
                .lock()
                .await
                .get_agent(&agent_id)
                .unwrap()
                .unwrap()
                .state,
            eliot_core::AgentState::Busy
        );
        assert_eq!(
            store
                .lock()
                .await
                .get_turn(&agent_id, 1, "recovered-turn")
                .unwrap()
                .unwrap()
                .state,
            eliot_core::TurnState::Running
        );
        // Exercise the actual Host EOF gap/loss sequence without starting a process.
        client.close_input(deadline()).await.unwrap();
        timeout_at(deadline(), task).await.unwrap().unwrap();
        record_root_exit_gap(
            &Arc::downgrade(&host.state),
            &key,
            "root_exit_without_session_terminal",
        )
        .await
        .unwrap();
        mark_lost_state(&Arc::downgrade(&host.state), &key).await;
        let store = store.lock().await;
        assert_eq!(
            store.get_agent(&agent_id).unwrap().unwrap().state,
            eliot_core::AgentState::Lost
        );
        assert_eq!(
            store
                .get_turn(&agent_id, 1, "recovered-turn")
                .unwrap()
                .unwrap()
                .state,
            eliot_core::TurnState::Unknown
        );
        let events = store.read_events(after_final, 1000, None).unwrap();
        let gap = events
            .iter()
            .find(|event| event.kind == "source.gap" && event.data["source"] == "transcript")
            .unwrap();
        let mut families: Vec<String> =
            serde_json::from_value(gap.data["families"].clone()).unwrap();
        families.sort();
        assert_eq!(families, ["message", "request", "session", "turn"]);
        for source in ["stream", "transcript"] {
            assert_eq!(
                store.read_source_state(&agent_id, 1, source).unwrap()["state"],
                json!("closed")
            );
        }
        assert_eq!(
            store.read_source_state(&agent_id, 1, "stream").unwrap()["gap_id"],
            Value::Null
        );
    }

    #[tokio::test]
    async fn source_handoff_private_history_observer_is_explicitly_denied() {
        let mut store = eliot_store::Store::open(Path::new(":memory:")).unwrap();
        let id = Ulid::new().to_string();
        let agent_id = store
            .submit(
                "owner",
                SubmitCommand {
                    id: id.clone(),
                    target: None,
                    selector: None,
                    verb: "attach".to_owned(),
                    args: json!({"profile":"observer-fixture", "harness":"codex", "mode":"observe",
                "native_scope":"fixture-scope", "native_ref":"fixture-ref", "profile_snapshot":{}}),
                    expected_generation: Some(0),
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                },
                now_ms(),
            )
            .unwrap()
            .command
            .target
            .unwrap();
        store.dispatch(&id, now_ms()).unwrap();
        let agent = store.get_agent(&agent_id).unwrap().unwrap();
        assert!(agent.observe_only);
        assert!(!agent.own_identity);
        let before = store.high_water().unwrap();
        let store = Arc::new(AsyncMutex::new(store));
        let error = observation::callback_error(
            &store,
            Some(&agent),
            "source.handoff",
            &json!({
                "agent_id":agent_id, "generation":1, "source":"observe", "families":["turn"],
                "successor":"transcript", "mechanism":"fake_recovery", "cursor":"edge",
            }),
            deadline(),
        )
        .await
        .unwrap();
        assert_eq!(error.code, "not_allowed");
        assert_eq!(store.lock().await.high_water().unwrap(), before);
    }

    #[tokio::test]
    async fn observer_live_callbacks_use_settled_subject_and_committed_grant() {
        let mut store = eliot_store::Store::open(Path::new(":memory:")).unwrap();
        let attach_id = Ulid::new().to_string();
        let agent_id = store
            .submit(
                "owner",
                SubmitCommand {
                    id: attach_id.clone(),
                    target: None,
                    selector: None,
                    verb: "attach".to_owned(),
                    args: json!({"profile":"observer-fixture", "harness":"codex", "mode":"observe",
                "native_scope":"fixture-scope", "native_ref":"fixture-ref", "profile_snapshot":{}}),
                    expected_generation: Some(0),
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                },
                now_ms(),
            )
            .unwrap()
            .command
            .target
            .unwrap();
        store.dispatch(&attach_id, now_ms()).unwrap();
        let agent = store.get_agent(&agent_id).unwrap().unwrap();
        let store = Arc::new(AsyncMutex::new(store));
        let bus = Bus::new(64);
        let monitoring = Monitoring::new(
            store.clone(),
            bus.clone(),
            Arc::new(Mutex::new(HashSet::new())),
        );
        let credential = "observer-private-peer";
        let (host_io, peer_io) = tokio::io::duplex(16 * 1024);
        let (host_read, host_write) = tokio::io::split(host_io);
        let (peer_read, peer_write) = tokio::io::split(peer_io);
        let peer = PluginConnection::new(peer_read, peer_write);
        let client = peer.client();
        let task = tokio::spawn(serve_plugin_requests(
            PluginConnection::new(host_read, host_write),
            EventSource {
                store: store.clone(),
                bus: bus.clone(),
                monitoring,
                source_credentials: Arc::from([credential.to_owned(), SECRET.to_owned()]),
                state: Weak::new(),
                key: AgentKey {
                    id: agent_id.clone(),
                    generation: 1,
                },
                agent_id: agent_id.clone(),
                generation: 1,
                expected_credential: credential.to_owned(),
                observation: Some(agent.clone()),
            },
        ));
        let readback = json!({"credential":credential, "agent_id":agent_id, "generation":1, "source":"observe"});
        assert_eq!(
            client
                .call("source.state", readback.clone(), deadline())
                .await
                .unwrap()["observer_ready"],
            json!(false)
        );
        // The producer waits for this existing attach settlement, never probing CAS readiness.
        store
            .lock()
            .await
            .settle_observe_attach(
                &attach_id,
                json!({
                    "agent_id":agent_id, "generation":1, "command_id":attach_id,
                    "native_scope":"fixture-scope", "native_ref":"fixture-ref", "state":"attached",
                    "observation_state":"unknown", "metadata":{}, "coverage":{},
                }),
                now_ms(),
            )
            .unwrap();
        assert_eq!(
            client
                .call("source.state", readback.clone(), deadline())
                .await
                .unwrap()["observer_ready"],
            json!(true)
        );

        let handoff = json!({"credential":credential, "agent_id":agent_id, "generation":1,
            "source":"observe", "families":["message","tool","turn"], "successor":"transcript", "mechanism":"rollout_tail",
            "gap":{"gap_id":"live-edge", "reason":"history excluded", "from":null, "to":"baseline-0"}});
        let before = store.lock().await.high_water().unwrap();
        for (field, value) in [
            ("successor", json!("git")),
            ("families", json!(["process"])),
            ("families", json!(["session"])),
            ("mechanism", json!("native_stream")),
        ] {
            let mut params = handoff.clone();
            params[field] = value;
            assert_eq!(
                client
                    .call("source.handoff", params, deadline())
                    .await
                    .unwrap_err()
                    .code,
                "not_allowed"
            );
        }
        assert_eq!(
            client
                .call("process.put", json!({"credential":credential}), deadline())
                .await
                .unwrap_err()
                .code,
            "not_allowed"
        );
        // A private ObservationPage is never interpreted as normalized live records.
        assert_eq!(
            client
                .call(
                    "events.put",
                    json!({"credential":credential, "agent_id":agent_id, "generation":1,
            "source":"rollout", "view":"turns", "entries":[{"state":"completed"}], "coverage":{}}),
                    deadline()
                )
                .await
                .unwrap_err()
                .code,
            "not_allowed"
        );
        assert_eq!(store.lock().await.high_water().unwrap(), before);

        let mut wrong_agent = agent.clone();
        wrong_agent.native_scope = Some("wrong-scope".to_owned());
        let mut params = handoff.clone();
        params.as_object_mut().unwrap().remove("credential");
        let error = source_handoff::record_source_handoff(
            &store,
            &bus,
            params,
            source_handoff::SourceBinding {
                agent_id: &agent_id,
                generation: 1,
                observation: Some(&wrong_agent),
            },
            &[credential.to_owned()],
            deadline(),
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, "source_integrity");
        assert_eq!(store.lock().await.high_water().unwrap(), before);
        let ack = client
            .call("source.handoff", handoff, deadline())
            .await
            .unwrap();
        let state = client
            .call("source.state", readback, deadline())
            .await
            .unwrap();
        assert_eq!(state["last_handoff"], ack);
        assert_eq!(state["state"], json!("open"));
        assert_eq!(state["observer_ready"], json!(true));

        let before_facts = store.lock().await.high_water().unwrap();
        let original_ts = 1_700_000_000_123_i64;
        client.call("events.put", json!({"credential":credential, "agent_id":agent_id, "generation":1,
            "batch_id":"observed-live", "source":"transcript", "cursor":"tail-1", "events":[
                {"kind":"turn.started", "native_id":"live-start", "ts":original_ts, "data":{"turn_id":"native-turn"}},
                {"kind":"message.completed", "native_id":"live-message", "ts":original_ts+1, "data":{"turn_id":"native-turn", "text":"live result"}},
                {"kind":"turn.completed", "native_id":"live-end", "ts":original_ts+2, "data":{"turn_id":"native-turn", "status":"completed"}}
            ]}), deadline()).await.unwrap();
        let progress = {
            let store = store.lock().await;
            let current = store.get_agent(&agent_id).unwrap().unwrap();
            assert_eq!(current.state, eliot_core::AgentState::Unknown);
            assert!(!current.own_identity);
            assert!(current.active_turn.is_none());
            assert_eq!(store.get_agent_token(&agent_id, 1).unwrap(), None);
            assert_eq!(
                store.get_command(&attach_id).unwrap().unwrap().state,
                eliot_core::CommandState::Applied
            );
            let events = store.read_events(before_facts, 1000, None).unwrap();
            assert!(
                events
                    .iter()
                    .all(|event| !event.kind.starts_with("command."))
            );
            assert_eq!(
                events
                    .iter()
                    .find(|event| event.native_id.as_deref() == Some("live-start"))
                    .unwrap()
                    .ts,
                original_ts
            );
            assert_eq!(
                store
                    .get_turn(&agent_id, 1, "native-turn")
                    .unwrap()
                    .unwrap()
                    .state,
                eliot_core::TurnState::Completed
            );
            store.last_model_progress(&agent_id, 1).unwrap()
        };
        let gap_ack = client.call("source.gap", json!({"credential":credential, "agent_id":agent_id,
            "generation":1, "source":"transcript", "families":["message","tool","turn"],
            "gap_id":"append-gap", "reason":"append coverage changed", "from":"tail-1", "to":"tail-2"}), deadline()).await.unwrap();
        assert_eq!(gap_ack["gap_id"], json!("append-gap"));
        let source = store
            .lock()
            .await
            .read_source_state(&agent_id, 1, "transcript")
            .unwrap();
        assert_eq!(source["health"], json!("ok"));
        assert_eq!(source["gap_id"], json!("append-gap"));
        assert_eq!(
            store
                .lock()
                .await
                .last_model_progress(&agent_id, 1)
                .unwrap(),
            progress
        );
        let before_health = store.lock().await.high_water().unwrap();
        let binding = source_handoff::SourceBinding {
            agent_id: &agent_id,
            generation: 1,
            observation: Some(&agent),
        };
        let error = source_handoff::record_source_health(
            &store,
            &bus,
            json!({"agent_id":agent_id, "generation":1, "source":"transcript", "health":"stale"}),
            binding,
            &["transcript".to_owned()],
            deadline(),
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, "secret_in_payload");
        assert_eq!(store.lock().await.high_water().unwrap(), before_health);
        let error = source_handoff::record_source_health(
            &store,
            &bus,
            json!({}),
            source_handoff::SourceBinding {
                observation: None,
                ..binding
            },
            &[],
            deadline(),
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, "invalid");
        assert_eq!(store.lock().await.high_water().unwrap(), before_health);
        let error = source_handoff::record_source_health(
            &store,
            &bus,
            json!({"agent_id":agent_id, "generation":1, "source":"transcript", "health":"stale"}),
            source_handoff::SourceBinding {
                observation: None,
                ..binding
            },
            &[],
            deadline(),
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, "not_allowed");
        assert_eq!(store.lock().await.high_water().unwrap(), before_health);
        let health = json!({"credential":credential, "agent_id":agent_id, "generation":1, "source":"transcript", "health":"stale"});
        let health_ack = client
            .call("source.health", health.clone(), deadline())
            .await
            .unwrap();
        assert_eq!(
            health_ack,
            json!({"agent_id":agent_id, "generation":1, "source":"transcript", "health":"stale"})
        );
        let after_health = store.lock().await.high_water().unwrap();
        assert_eq!(
            client
                .call("source.health", health, deadline())
                .await
                .unwrap(),
            health_ack
        );
        assert_eq!(store.lock().await.high_water().unwrap(), after_health);
        assert_eq!(
            store
                .lock()
                .await
                .last_model_progress(&agent_id, 1)
                .unwrap(),
            progress
        );
        assert_eq!(
            store
                .lock()
                .await
                .read_source_state(&agent_id, 1, "transcript")
                .unwrap()["cursor"],
            json!("tail-1")
        );
        client.close_input(deadline()).await.unwrap();
        timeout_at(deadline(), task).await.unwrap().unwrap();
    }

    fn source_exit_fixture(harness: &str) -> (PluginHost, AgentKey, String) {
        let mut store = eliot_store::Store::open(Path::new(":memory:")).unwrap();
        let start_id = Ulid::new().to_string();
        let profile = "fixture";
        let agent = store
            .submit(
                "owner",
                SubmitCommand {
                    id: start_id.clone(),
                    target: None,
                    selector: None,
                    verb: "launch".to_owned(),
                    args: json!({"project":"source-exit", "workspace":"synthetic-workspace",
                "harness":harness, "profile":profile, "profile_snapshot":{}}),
                    expected_generation: Some(0),
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                },
                now_ms(),
            )
            .unwrap()
            .command
            .target
            .unwrap();
        store.dispatch(&start_id, now_ms()).unwrap();
        crate::test_support::install_fixture_sources(&mut store, &agent, &start_id, 1, now_ms());
        let store = Arc::new(AsyncMutex::new(store));
        let bus = Bus::new(32);
        let secrets = Arc::new(Mutex::new(HashSet::new()));
        let monitoring = Monitoring::new(store.clone(), bus.clone(), secrets.clone());
        let (progress, _) = watch::channel(0);
        let host = PluginHost::new(
            PluginHostOptions::new(
                PathBuf::from("synthetic-data-root"),
                "owner-test".to_owned(),
            ),
            store,
            bus,
            secrets,
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashSet::new())),
            progress,
            monitoring,
        );
        (
            host,
            AgentKey {
                id: agent,
                generation: 1,
            },
            start_id,
        )
    }

    async fn force_stop_fixture() -> (PluginHost, AgentKey, String) {
        let (host, key, start_id) = source_exit_fixture("codex");
        {
            let mut store = host.state.store.lock().await;
            store
                .put_batch(
                    SourceBatch {
                        batch_id: Ulid::new().to_string(),
                        agent_id: key.id.clone(),
                        generation: 1,
                        source: "stream".to_owned(),
                        cursor: None,
                        events: vec![NativeEvent {
                            ts: None,
                            kind: "session.ready".to_owned(),
                            native_id: Ulid::new().to_string(),
                            data: json!({"command_id":start_id}),
                        }],
                    },
                    now_ms(),
                )
                .unwrap();
            store
                .settle(&start_id, CommandState::Applied, None, now_ms())
                .unwrap();
            let send_id = Ulid::new().to_string();
            let mut send = SubmitCommand::new(&send_id, "send");
            send.target = Some(key.id.clone());
            send.expected_generation = Some(1);
            send.args = json!({"text":"bounded regression task"});
            store.submit("owner", send, now_ms()).unwrap();
            store.dispatch(&send_id, now_ms()).unwrap();
            store
                .put_batch(
                    SourceBatch {
                        batch_id: Ulid::new().to_string(),
                        agent_id: key.id.clone(),
                        generation: 1,
                        source: "stream".to_owned(),
                        cursor: None,
                        events: vec![NativeEvent {
                            ts: None,
                            kind: "turn.started".to_owned(),
                            native_id: Ulid::new().to_string(),
                            data: json!({"command_id":send_id, "turn_id":"force-turn"}),
                        }],
                    },
                    now_ms(),
                )
                .unwrap();
            assert_eq!(
                store.get_agent(&key.id).unwrap().unwrap().state,
                AgentState::Busy
            );
        }
        let stop_id = Ulid::new().to_string();
        {
            let mut store = host.state.store.lock().await;
            let mut stop = SubmitCommand::new(&stop_id, "stop");
            stop.target = Some(key.id.clone());
            stop.expected_generation = Some(1);
            stop.args = json!({"force":true});
            store.submit("owner", stop, now_ms()).unwrap();
            store.dispatch(&stop_id, now_ms()).unwrap();
        }
        (host, key, stop_id)
    }

    async fn assert_force_loss(host: &PluginHost, key: &AgentKey, stop_id: &str) {
        let store = host.state.store.lock().await;
        let agent = store.get_agent(&key.id).unwrap().unwrap();
        assert_eq!(agent.state, AgentState::Lost);
        assert!(agent.active_turn.is_none());
        assert!(agent.pending.is_empty());
        assert_eq!(
            store
                .get_turn(&key.id, 1, "force-turn")
                .unwrap()
                .unwrap()
                .state,
            eliot_core::TurnState::Unknown
        );
        assert_eq!(
            store.get_command(stop_id).unwrap().unwrap().state,
            CommandState::Unknown
        );
        assert_eq!(
            store
                .read_events(0, 1000, None)
                .unwrap()
                .iter()
                .filter(|event| event.kind == "agent.lost")
                .count(),
            1
        );
    }

    #[test]
    fn force_guard_drop_outside_runtime_does_not_panic() {
        assert!(tokio::runtime::Handle::try_current().is_err());
        let (host, key, _) = source_exit_fixture("codex");
        let guard = {
            let _store = host.state.store.try_lock().unwrap();
            ForceStopGuard::new(&host.state, key.clone()).unwrap()
        };
        guard.deferred_loss.store(true, Ordering::Release);
        let deferred_loss = Arc::clone(&guard.deferred_loss);

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(guard)));

        assert!(result.is_ok(), "dropping without a runtime must not panic");
        assert!(!host.state.planned_stops.lock_recover().contains_key(&key));
        assert!(deferred_loss.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn force_verification_timeout_then_late_proven_exit_settles_lost() {
        let (host, key, stop_id) = force_stop_fixture().await;
        let result = async {
            let _guard = ForceStopGuard::new(&host.state, key.clone()).unwrap();
            timeout_at(Instant::now(), std::future::pending::<()>()).await
        }
        .await;
        assert!(result.is_err());
        assert!(!host.state.planned_stops.lock_recover().contains_key(&key));
        host.state
            .store
            .lock()
            .await
            .settle(&stop_id, CommandState::Unknown, None, now_ms())
            .unwrap();
        settle_drained_source_exit(&Arc::downgrade(&host.state), &key, true, true, "late_exit")
            .await
            .unwrap();
        mark_lost_state(&Arc::downgrade(&host.state), &key).await;
        cleanup_actor(&Arc::downgrade(&host.state), &key, "", false).await;
        assert_force_loss(&host, &key, &stop_id).await;
    }

    #[tokio::test]
    async fn force_failure_reconciles_loss_deferred_before_actor_teardown() {
        let (host, key, stop_id) = force_stop_fixture().await;
        let guard = ForceStopGuard::new(&host.state, key.clone()).unwrap();
        settle_drained_source_exit(&Arc::downgrade(&host.state), &key, true, true, "early_exit")
            .await
            .unwrap();
        mark_lost_state(&Arc::downgrade(&host.state), &key).await;
        cleanup_actor(&Arc::downgrade(&host.state), &key, "", false).await;
        assert!(host.state.planned_stops.lock_recover().contains_key(&key));
        assert_eq!(
            host.state
                .store
                .lock()
                .await
                .get_agent(&key.id)
                .unwrap()
                .unwrap()
                .state,
            AgentState::Busy
        );
        let mut progress = host.state.progress.subscribe();
        drop(guard);
        assert!(!host.state.planned_stops.lock_recover().contains_key(&key));
        timeout_at(deadline(), progress.changed())
            .await
            .unwrap()
            .unwrap();
        assert_force_loss(&host, &key, &stop_id).await;
    }

    #[tokio::test]
    async fn force_outer_cancellation_clears_marker() {
        let (host, key, stop_id) = force_stop_fixture().await;
        let mut force = Box::pin(async {
            let _guard = ForceStopGuard::new(&host.state, key.clone()).unwrap();
            std::future::pending::<()>().await;
        });
        assert!(futures_util::poll!(&mut force).is_pending());
        assert!(host.state.planned_stops.lock_recover().contains_key(&key));
        // Drop the enclosing future, as the outer force deadline does.
        drop(force);
        assert!(!host.state.planned_stops.lock_recover().contains_key(&key));
        assert_eq!(
            host.state
                .store
                .lock()
                .await
                .get_agent(&key.id)
                .unwrap()
                .unwrap()
                .state,
            AgentState::Busy
        );
        settle_drained_source_exit(
            &Arc::downgrade(&host.state),
            &key,
            true,
            true,
            "cancelled_exit",
        )
        .await
        .unwrap();
        mark_lost_state(&Arc::downgrade(&host.state), &key).await;
        assert_force_loss(&host, &key, &stop_id).await;
    }

    #[tokio::test]
    async fn force_success_suppresses_loss_through_stop_commit() {
        let (host, key, stop_id) = force_stop_fixture().await;
        let guard = ForceStopGuard::new(&host.state, key.clone()).unwrap();
        settle_drained_source_exit(&Arc::downgrade(&host.state), &key, true, true, "force_exit")
            .await
            .unwrap();
        mark_lost_state(&Arc::downgrade(&host.state), &key).await;
        cleanup_actor(&Arc::downgrade(&host.state), &key, "", false).await;
        assert!(host.state.planned_stops.lock_recover().contains_key(&key));
        host.state
            .store
            .lock()
            .await
            .settle(&stop_id, CommandState::Applied, None, now_ms())
            .unwrap();
        guard.finish();
        mark_lost_state(&Arc::downgrade(&host.state), &key).await;
        assert!(!host.state.planned_stops.lock_recover().contains_key(&key));
        let store = host.state.store.lock().await;
        assert_eq!(
            store.get_agent(&key.id).unwrap().unwrap().state,
            AgentState::Stopped
        );
        assert!(
            store
                .read_events(0, 1000, None)
                .unwrap()
                .iter()
                .all(|event| event.kind != "agent.lost")
        );
    }

    #[tokio::test]
    async fn owned_source_exit_tracks_reader_independently_of_custody() {
        let (host, key, start_id) = source_exit_fixture("codex");
        let before = host.state.store.lock().await.high_water().unwrap();
        let error = settle_drained_source_exit(
            &Arc::downgrade(&host.state),
            &key,
            false,
            false,
            "test_exit",
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, "unknown");
        {
            let store = host.state.store.lock().await;
            let source = store.read_source_state(&key.id, 1, "stream").unwrap();
            assert_eq!(source["state"], json!("open"));
            assert_eq!(source["health"], json!("stale"));
        }
        let mut drained = tokio::spawn(async {});
        let error = settle_owned_source_exit(
            &Arc::downgrade(&host.state),
            &key,
            Some(&mut drained),
            false,
            "test_exit",
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, "unknown");
        {
            let store = host.state.store.lock().await;
            let source = store.read_source_state(&key.id, 1, "stream").unwrap();
            assert_eq!(source["state"], json!("closed"));
            assert_eq!(source["health"], json!("stale"));
            assert_eq!(
                store.get_agent(&key.id).unwrap().unwrap().state,
                eliot_core::AgentState::Starting
            );
            assert_eq!(
                store.get_command(&start_id).unwrap().unwrap().state,
                eliot_core::CommandState::Dispatched
            );
            assert!(
                store
                    .read_events(before, 1000, None)
                    .unwrap()
                    .iter()
                    .all(|event| !matches!(
                        event.kind.as_str(),
                        "agent.lost" | "session.lost" | "session.ended" | "command.applied"
                    ))
            );
        }
        let agent = host
            .state
            .store
            .lock()
            .await
            .get_agent(&key.id)
            .unwrap()
            .unwrap();
        match host.state.monitoring.view(&agent).await.unwrap() {
            eliot_core::api::ObservationView::Data(view) => {
                assert_eq!(view.liveness.source_healthy, Some(false));
            }
            _ => panic!("expected the ordinary observation view"),
        }
        let mut drained = tokio::spawn(async {});
        settle_owned_source_exit(
            &Arc::downgrade(&host.state),
            &key,
            Some(&mut drained),
            true,
            "test_exit",
        )
        .await
        .unwrap();
        {
            let store = host.state.store.lock().await;
            assert_eq!(
                store.read_source_state(&key.id, 1, "stream").unwrap()["state"],
                json!("closed")
            );
            assert_eq!(
                store.get_agent(&key.id).unwrap().unwrap().state,
                eliot_core::AgentState::Starting
            );
            assert!(
                store
                    .read_events(before, 1000, None)
                    .unwrap()
                    .iter()
                    .all(|event| event.kind != "capacity.released")
            );
        }
        // Proven control loss can advance the Agent while the cap remains
        // reserved until Store has the independent OS-source-close evidence.
        mark_lost_state(&Arc::downgrade(&host.state), &key).await;
        let store = host.state.store.lock().await;
        assert_eq!(
            store.get_agent(&key.id).unwrap().unwrap().state,
            eliot_core::AgentState::Lost
        );
        assert!(
            store
                .read_events(before, 1000, None)
                .unwrap()
                .iter()
                .all(|event| event.kind != "capacity.released")
        );
    }

    #[tokio::test]
    async fn owned_source_exit_retiring_generation_does_not_touch_current() {
        let (host, key, start_id) = source_exit_fixture("codex");
        let (current, sources, token, before) = {
            let mut store = host.state.store.lock().await;
            crate::test_support::install_live_owned_process_source(
                &mut store,
                &key.id,
                key.generation,
                now_ms(),
            );
            for (kind, data) in [
                (
                    "session.ready",
                    json!({"native_scope":"fixture-scope", "native_ref":"fixture-ref"}),
                ),
                ("session.ended", json!({})),
            ] {
                store
                    .put_batch(
                        SourceBatch {
                            batch_id: Ulid::new().to_string(),
                            agent_id: key.id.clone(),
                            generation: 1,
                            source: "stream".to_owned(),
                            cursor: None,
                            events: vec![NativeEvent {
                                ts: None,
                                kind: kind.to_owned(),
                                native_id: Ulid::new().to_string(),
                                data,
                            }],
                        },
                        now_ms(),
                    )
                    .unwrap();
                if kind == "session.ready" {
                    store
                        .settle(&start_id, eliot_core::CommandState::Applied, None, now_ms())
                        .unwrap();
                }
                if kind == "session.ended" {
                    crate::test_support::simulate_owned_process_exit_proof(
                        &mut store,
                        &key.id,
                        key.generation,
                        now_ms(),
                    );
                }
            }
            let resume_id = Ulid::new().to_string();
            store
                .submit(
                    "owner",
                    SubmitCommand {
                        id: resume_id.clone(),
                        target: Some(key.id.clone()),
                        selector: None,
                        verb: "resume".to_owned(),
                        args: json!({}),
                        expected_generation: Some(1),
                        expected_turn_id: None,
                        request_id: None,
                        probe: false,
                    },
                    now_ms(),
                )
                .unwrap();
            store.dispatch(&resume_id, now_ms()).unwrap();
            (
                store.get_agent(&key.id).unwrap().unwrap(),
                store.list_source_states(&key.id, 2).unwrap(),
                store.get_agent_token(&key.id, 2).unwrap(),
                store.high_water().unwrap(),
            )
        };
        assert_eq!(current.generation, 2);
        let mut drained = tokio::spawn(async {});
        settle_owned_source_exit(
            &Arc::downgrade(&host.state),
            &key,
            Some(&mut drained),
            true,
            "retiring_exit",
        )
        .await
        .unwrap();
        let store = host.state.store.lock().await;
        assert_eq!(store.get_agent(&key.id).unwrap().unwrap(), current);
        assert_eq!(store.list_source_states(&key.id, 2).unwrap(), sources);
        assert_eq!(store.get_agent_token(&key.id, 2).unwrap(), token);
        assert_eq!(store.high_water().unwrap(), before);
        drop(store);
        assert_eq!(
            settle_drained_source_exit(
                &Arc::downgrade(&host.state),
                &key,
                false,
                false,
                "retiring_reader_unavailable",
            )
            .await
            .unwrap_err()
            .code,
            "unknown"
        );
        let store = host.state.store.lock().await;
        assert_eq!(store.get_agent(&key.id).unwrap().unwrap(), current);
        assert_eq!(store.list_source_states(&key.id, 2).unwrap(), sources);
        assert_eq!(store.get_agent_token(&key.id, 2).unwrap(), token);
        // Reader unavailability marks only the native sources stale; the
        // reserved OS process source stays ok and closed after the exit
        // proof, exactly as the fixture helper records it.
        assert!(
            store
                .list_source_states(&key.id, 1)
                .unwrap()
                .iter()
                .filter(|source| !["daemon", "os", "git", "workspace_process"]
                    .contains(&source["source"].as_str().unwrap_or("")))
                .all(|source| source["health"] == "stale")
        );
        assert!(
            store
                .read_events(before, 1000, None)
                .unwrap()
                .iter()
                .all(|event| event.generation == Some(1))
        );
    }

    #[test]
    fn owned_source_exit_stop_deadlines_cover_queue_two_rpcs_custody_drain_and_store() {
        let now = Instant::now();
        let deadlines = StopDeadlines::new(now);
        assert_eq!(deadlines.queued_until - now, RPC_DEADLINE);
        let second_rpc_starts = deadlines.queued_until
            + RPC_DEADLINE
            + eliot_plugin::native_lifecycle::STOP_INTERRUPT_TIMEOUT;
        assert_eq!(
            deadlines.rpc_deadline_at(second_rpc_starts, RPC_DEADLINE),
            deadlines.rpc_until
        );
        assert_eq!(
            deadlines.custody_until - deadlines.rpc_until,
            STOP_GRACE + (STOP_GRACE + STOP_GRACE) * 2
        );
        assert_eq!(
            deadlines.source_until - deadlines.custody_until,
            STOP_GRACE + RPC_GRACE + RPC_DEADLINE
        );
        assert_eq!(deadlines.reply_until - deadlines.source_until, RPC_GRACE);
        assert!(deadlines.reply_until > now + RPC_DEADLINE + STOP_GRACE + STOP_GRACE + RPC_GRACE);
        assert!(
            deadlines.queued_until < deadlines.rpc_until
                && deadlines.rpc_until < deadlines.custody_until
        );
    }

    #[tokio::test]
    async fn stop_pre_io_full_and_closed_queues_are_rejected_without_admission() {
        let (host, key, start_id) = source_exit_fixture("codex");
        let command = {
            let mut store = host.state.store.lock().await;
            store
                .put_batch(
                    SourceBatch {
                        batch_id: "queue-test-ready".to_owned(),
                        agent_id: key.id.clone(),
                        generation: 1,
                        source: "stream".to_owned(),
                        cursor: None,
                        events: vec![NativeEvent {
                            ts: None,
                            kind: "session.ready".to_owned(),
                            native_id: "queue-test-ready".to_owned(),
                            data: json!({"command_id":start_id}),
                        }],
                    },
                    now_ms(),
                )
                .unwrap();
            store
                .settle(&start_id, eliot_core::CommandState::Applied, None, now_ms())
                .unwrap();
            let id = Ulid::new().to_string();
            store
                .submit(
                    "owner",
                    SubmitCommand {
                        id: id.clone(),
                        target: Some(key.id.clone()),
                        selector: None,
                        verb: "stop".to_owned(),
                        args: json!({}),
                        expected_generation: Some(1),
                        expected_turn_id: None,
                        request_id: None,
                        probe: false,
                    },
                    now_ms(),
                )
                .unwrap();
            store.dispatch(&id, now_ms()).unwrap().value
        };
        let (sender, mut receiver) = mpsc::channel(1);
        let (blocker, _blocked_reply) = oneshot::channel();
        sender
            .send(RuntimeMessage::DaemonForce { reply: blocker })
            .await
            .unwrap_or_else(|_| panic!("queue fixture closed"));
        let deadlines =
            StopDeadlines::new(Instant::now() - RPC_DEADLINE - Duration::from_millis(1));
        let (reply, response) = oneshot::channel();
        let error = enqueue_stop(
            &sender,
            RuntimeMessage::Stop {
                command: command.clone(),
                turn_id: None,
                force: true,
                deadlines,
                reply,
            },
            deadlines.queued_until,
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, "native_not_dispatched");
        assert_eq!(
            eliot_core::classify_dispatched_error(&error.code),
            eliot_core::CommandState::Rejected
        );
        assert!(matches!(
            receiver.try_recv(),
            Ok(RuntimeMessage::DaemonForce { .. })
        ));
        // The rejected stop never reached the actor, so no native work can follow it.
        assert!(matches!(
            receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert!(response.await.is_err());

        drop(receiver);
        let (reply, _response) = oneshot::channel();
        let error = enqueue_stop(
            &sender,
            RuntimeMessage::Stop {
                command,
                turn_id: None,
                force: true,
                deadlines,
                reply,
            },
            Instant::now() + RPC_DEADLINE,
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, "native_not_dispatched");
        assert_eq!(
            eliot_core::classify_dispatched_error(&error.code),
            eliot_core::CommandState::Rejected
        );
    }

    #[tokio::test]
    async fn stop_pre_io_expired_actor_deadline_is_rejected_without_rpc_bytes() {
        use tokio::io::AsyncReadExt;

        let (host_io, mut native_io) = tokio::io::duplex(256);
        let (reader, writer) = tokio::io::split(host_io);
        let connection = PluginConnection::new(reader, writer);
        let client = connection.client();
        let deadlines =
            StopDeadlines::new(Instant::now() - RPC_DEADLINE - Duration::from_millis(1));
        let error = match prepare_queued_stop_call(
            &client,
            json!({
                "agent_id":"a1", "generation":1, "command_id":Ulid::new().to_string(), "force":true,
            }),
            deadlines,
        ) {
            Err(error) => error,
            Ok(_) => panic!("expired actor must reject before constructing native RPC"),
        };
        assert_eq!(error.code, "native_not_dispatched");
        assert_eq!(
            eliot_core::classify_dispatched_error(&error.code),
            eliot_core::CommandState::Rejected
        );
        tokio::task::yield_now().await;
        let mut bytes = [0_u8; 1];
        let native_read = native_io.read(&mut bytes);
        tokio::pin!(native_read);
        assert!(futures_util::poll!(&mut native_read).is_pending());
    }

    fn instance_test_host(data_root: PathBuf, store: StoreHandle) -> PluginHost {
        let bus = Bus::new(64);
        let secrets = Arc::new(Mutex::new(HashSet::new()));
        let monitoring = Monitoring::new(store.clone(), bus.clone(), secrets.clone());
        let (progress, _) = watch::channel(0);
        PluginHost::new(
            PluginHostOptions::new(data_root, "owner-test".to_owned()),
            store,
            bus,
            secrets,
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashSet::new())),
            progress,
            monitoring,
        )
    }

    fn launch_command_for(agent_id: &str, generation: i64) -> Command {
        Command {
            id: Ulid::new().to_string(),
            principal: "owner".to_owned(),
            method: "command.submit".to_owned(),
            verb: "launch".to_owned(),
            target: Some(agent_id.to_owned()),
            selector: None,
            expected_generation: None,
            generation: Some(generation),
            args: json!({}),
            state: CommandState::Dispatched,
            turn_id: None,
            request_id: None,
            outcome: None,
            created_at: 0,
            updated_at: 0,
            probe: false,
            config_revision: 0,
            config: None,
        }
    }

    fn plugin_dir_entries(data_root: &Path) -> Vec<PathBuf> {
        let plugins = data_root.join("plugins");
        if !plugins.exists() {
            return Vec::new();
        }
        std::fs::read_dir(&plugins)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect()
    }

    fn submit_launch(store: &mut eliot_store::Store, project: &str, workspace: &str) -> Command {
        store
            .submit(
                "owner",
                SubmitCommand {
                    id: Ulid::new().to_string(),
                    target: None,
                    selector: None,
                    verb: "launch".to_owned(),
                    args: json!({"project": project, "workspace": workspace, "harness": "codex", "profile": "fixture", "profile_snapshot": {}}),
                    expected_generation: Some(0),
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                },
                now_ms(),
            )
            .unwrap()
            .command
            .clone()
    }

    #[tokio::test]
    async fn snapshot_turn_without_an_owned_plugin_fails_fast() {
        let data_root =
            std::env::temp_dir().join(format!("eliot-snapshot-no-runtime-{}", Ulid::new()));
        let _temp_1 = crate::test_support::TempDir::own(data_root.clone());
        let mut store = eliot_store::Store::open(Path::new(":memory:")).unwrap();
        let submitted = submit_launch(&mut store, "snapshot-absent", "synthetic-workspace");
        let agent_id = submitted.target.clone().unwrap();
        let store = Arc::new(AsyncMutex::new(store));
        let host = instance_test_host(data_root.clone(), store);
        let mut agent = Agent::new(&agent_id, "snapshot-absent", "codex", "fixture", 0);
        agent.generation = 1;

        let error = host
            .snapshot_turn(&agent, "send-1", "turn-1")
            .await
            .unwrap_err();
        assert_eq!(error.code, "not_found");
    }

    #[tokio::test]
    async fn start_validates_the_generation_credential_before_creating_an_instance_dir() {
        let data_root =
            std::env::temp_dir().join(format!("eliot-plugin-cleanup-precreate-{}", Ulid::new()));
        let _temp_2 = crate::test_support::TempDir::own(data_root.clone());
        let workspace =
            std::env::temp_dir().join(format!("eliot-plugin-cleanup-workspace-{}", Ulid::new()));
        let _temp_3 = crate::test_support::TempDir::own(workspace.clone());
        std::fs::create_dir_all(&workspace).unwrap();
        let workspace = workspace.to_string_lossy().into_owned();
        let mut store = eliot_store::Store::open(Path::new(":memory:")).unwrap();
        // Admit the Agent but never dispatch, so no generation credential exists.
        let submitted = submit_launch(&mut store, "precreate", &workspace);
        let agent_id = submitted.target.clone().unwrap();
        let store = Arc::new(AsyncMutex::new(store));
        let host = instance_test_host(data_root.clone(), store);
        let mut agent = Agent::new(&agent_id, "precreate", "codex", "fixture", 0);
        agent.generation = 1;
        agent.workspace = Some(workspace.clone());

        let error = host
            .start(agent, launch_command_for(&agent_id, 1), None)
            .await
            .unwrap_err();
        let StartError::NotSpawned(error) = error else {
            panic!("expected definite pre-spawn failure")
        };
        assert_eq!(error.code, "bad_state");
        assert!(
            plugin_dir_entries(&data_root).is_empty(),
            "no instance directory may exist before the credential check"
        );
    }

    #[tokio::test]
    async fn start_removes_only_the_exact_empty_instance_dir_on_a_pre_spawn_error() {
        let data_root =
            std::env::temp_dir().join(format!("eliot-plugin-cleanup-prespawn-{}", Ulid::new()));
        let _temp_4 = crate::test_support::TempDir::own(data_root.clone());
        let workspace =
            std::env::temp_dir().join(format!("eliot-plugin-cleanup-workspace-{}", Ulid::new()));
        let _temp_5 = crate::test_support::TempDir::own(workspace.clone());
        std::fs::create_dir_all(&workspace).unwrap();
        let workspace = workspace.to_string_lossy().into_owned();
        // Admit a different workspace than the one start() observes, so
        // prepare_generation fails deterministically before any spawn.
        let admitted = format!("{workspace}-admitted-elsewhere");
        let mut store = eliot_store::Store::open(Path::new(":memory:")).unwrap();
        let submitted = submit_launch(&mut store, "prespawn", &admitted);
        let agent_id = submitted.target.clone().unwrap();
        store.dispatch(&submitted.id, now_ms()).unwrap();
        let store = Arc::new(AsyncMutex::new(store));
        let host = instance_test_host(data_root.clone(), store);
        let mut agent = Agent::new(&agent_id, "prespawn", "codex", "fixture", 0);
        agent.generation = 1;
        agent.workspace = Some(workspace.clone());
        agent.profile_snapshot = json!({
            "harness":"codex",
            "pass_env":[],
            "pass_env_values":{"CODEX_HOME":null, "CODEX_SQLITE_HOME":null},
        });

        let error = host
            .start(agent, launch_command_for(&agent_id, 1), None)
            .await
            .unwrap_err();
        let StartError::NotSpawned(error) = error else {
            panic!("expected definite pre-spawn failure")
        };
        assert_eq!(error.code, "source_integrity");
        assert!(
            error.message.contains("does not match the Agent"),
            "unexpected pre-spawn failure: {error:?}"
        );
        // The plugins root was created, but the generated child is gone. A
        // post-spawn path would have left it behind.
        assert!(data_root.join("plugins").is_dir());
        assert!(
            plugin_dir_entries(&data_root).is_empty(),
            "a pre-spawn error must remove the exact generated empty directory"
        );
    }

    #[test]
    fn validate_configure_result_requires_verified_native_proof() {
        // OpenCode shape: model plus its GET proof, no effort knob.
        assert!(
            validate_configure_result(json!({
                "state": "applied",
                "model": "p/m1",
                "model_verified": true,
            }))
            .is_ok()
        );
        // Antigravity shape: recombined model plus thought level, both proven.
        assert!(
            validate_configure_result(json!({
                "state": "applied",
                "model": "base-high",
                "effort": "high",
                "model_verified": true,
                "effort_verified": true,
            }))
            .is_ok()
        );
        // Antigravity without a level: unverified effort is honest.
        assert!(
            validate_configure_result(json!({
                "state": "applied",
                "model": "base",
                "model_verified": true,
                "effort_verified": false,
            }))
            .is_ok()
        );
        // A claimed effort without its proof, a missing model, or an
        // unverified model is not an application.
        assert!(
            validate_configure_result(json!({
                "state": "applied",
                "model": "base-high",
                "effort": "high",
                "model_verified": true,
                "effort_verified": false,
            }))
            .is_err()
        );
        assert!(
            validate_configure_result(json!({
                "state": "applied",
                "model_verified": true,
            }))
            .is_err()
        );
        assert!(
            validate_configure_result(json!({
                "state": "applied",
                "model": "p/m1",
                "model_verified": false,
            }))
            .is_err()
        );
        assert!(
            validate_configure_result(json!({
                "state": "applied",
                "model": "   ",
                "model_verified": true,
            }))
            .is_err()
        );
    }

    #[test]
    fn validate_start_result_accepts_bound_ready_or_failed() {
        let agent_id = "agent-start-validate";
        let mut agent = Agent::new(agent_id, "project", "muse", "fixture", 0);
        agent.generation = 1;
        let command = launch_command_for(agent_id, 1);
        let bound = |state: &str| {
            json!({
                "agent_id": agent_id,
                "generation": 1,
                "command_id": command.id,
                "state": state,
                "reason": "Muse test check failed",
            })
        };
        assert!(validate_start_result(&bound("ready"), &agent, &command).is_ok());
        assert!(validate_start_result(&bound("failed"), &agent, &command).is_ok());
        assert!(!is_failed_start_result(&bound("ready")));
        assert!(is_failed_start_result(&bound("failed")));
        assert!(!is_failed_start_result(&json!({"state": "unknown"})));
        assert!(validate_start_result(&json!({"state": "ready"}), &agent, &command).is_err());
        assert!(validate_start_result(&bound("unknown"), &agent, &command).is_err());
        let mut wrong_command = bound("failed");
        wrong_command["command_id"] = json!("other-command");
        assert!(validate_start_result(&wrong_command, &agent, &command).is_err());
    }

    #[tokio::test]
    async fn create_instance_dir_reports_a_blocked_parent() {
        let data_root =
            std::env::temp_dir().join(format!("eliot-plugin-cleanup-blocked-{}", Ulid::new()));
        let _temp_6 = crate::test_support::TempDir::own(data_root.clone());
        std::fs::create_dir_all(&data_root).unwrap();
        std::fs::write(data_root.join("plugins"), b"not a directory").unwrap();
        let store = Arc::new(AsyncMutex::new(
            eliot_store::Store::open(Path::new(":memory:")).unwrap(),
        ));
        let host = instance_test_host(data_root.clone(), store);
        let key = AgentKey {
            id: "a1".to_owned(),
            generation: 1,
        };

        let error = host.create_instance_dir(&key).unwrap_err();
        assert_eq!(error.code, "platform_io");
        assert!(data_root.join("plugins").is_file());
    }

    #[test]
    fn create_instance_dir_removes_the_partial_directory_when_restriction_fails() {
        let data_root =
            std::env::temp_dir().join(format!("eliot-plugin-cleanup-restrict-{}", Ulid::new()));
        let _temp_7 = crate::test_support::TempDir::own(data_root.clone());
        let store = Arc::new(AsyncMutex::new(
            eliot_store::Store::open(Path::new(":memory:")).unwrap(),
        ));
        let host = instance_test_host(data_root.clone(), store);
        let key = AgentKey {
            id: "a1".to_owned(),
            generation: 1,
        };

        // Force the post-mkdir restrict step (unix chmod 0o700 in production)
        // to fail through the seam; the generated directory must be gone and
        // plugins/ left empty.
        let mut generated: Option<PathBuf> = None;
        let error = host
            .create_instance_dir_with(&key, |path| {
                generated = Some(path.to_owned());
                Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "forced restrict failure",
                ))
            })
            .unwrap_err();
        assert_eq!(error.code, "platform_io");
        let generated = generated.expect("the restrict step must see the generated directory");
        assert!(
            !generated.exists(),
            "a failed restrict must remove the partial directory"
        );
        assert!(data_root.join("plugins").is_dir());
        assert!(
            plugin_dir_entries(&data_root).is_empty(),
            "a failed restrict must leave plugins/ empty"
        );
    }

    #[test]
    fn empty_instance_dir_removal_never_touches_nonempty_or_missing_paths() {
        let root =
            std::env::temp_dir().join(format!("eliot-plugin-cleanup-removal-{}", Ulid::new()));
        let _temp_8 = crate::test_support::TempDir::own(root.clone());
        let empty = root.join("empty");
        let nonempty = root.join("nonempty");
        std::fs::create_dir_all(&empty).unwrap();
        std::fs::create_dir_all(&nonempty).unwrap();
        std::fs::write(nonempty.join("plugin-data"), b"must survive").unwrap();

        remove_empty_instance_dir(&empty);
        assert!(!empty.exists());
        remove_empty_instance_dir(&nonempty);
        assert!(nonempty.join("plugin-data").is_file());
        remove_empty_instance_dir(&root.join("missing"));
    }
}

#[cfg(all(test, target_os = "linux"))]
mod linux_family_tests {
    use super::*;

    /// The leader exits immediately while a same-group grandchild lingers:
    /// the shared family stop path must terminate the owned group before
    /// reaping, so no owned member outlives the stop. Reaping first (the old
    /// order) would clear the birth-checked identity and leave the
    /// grandchild running with no group signal authority.
    #[tokio::test]
    async fn lingering_grandchild_does_not_outlive_linux_family_stop() {
        let dir = std::env::temp_dir().join(format!("eliot-linux-family-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&dir).unwrap();
        let _temp = crate::test_support::TempDir::own(dir.clone());
        let pidfile = dir.join("grandchild.pid");
        let mut command = tokio::process::Command::new("sh");
        // The pidfile travels as a separate argument: interpolating it into
        // the program would break on directories containing an apostrophe.
        command
            .arg("-c")
            .arg("sleep 120 & echo $! > \"$1\"; exec sleep 0")
            .arg("sh")
            .arg(&pidfile)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        let mut child = OwnedChild::spawn(command).unwrap();
        // Capture the grandchild identity before the stop: the shell writes
        // the pidfile right after the background sleep forks.
        let grandchild: u32 = {
            let mut found = None;
            for _ in 0..100 {
                if let Ok(body) = std::fs::read_to_string(&pidfile)
                    && let Ok(pid) = body.trim().parse::<u32>()
                {
                    found = Some(pid);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            found.expect("the shell records its background grandchild")
        };
        let start_ticks = crate::platform::unix::linux_start_ticks(grandchild);
        let store = Arc::new(AsyncMutex::new(
            eliot_store::Store::open(Path::new(":memory:")).expect("open store"),
        ));
        let monitoring = Monitoring::new(store, Bus::new(16), Arc::new(Mutex::new(HashSet::new())));
        let agent = Agent::new("a1", "proj", "opencode", "fake", 0);
        // The actual stop path: observe the leader exit without reaping,
        // drain the owned group while the birth identity still anchors
        // group signals, then reap.
        assert!(
            wait_child_family(&mut child, STOP_GRACE, FAMILY_GRACE, &monitoring, &agent).await,
            "owned group drains before reap"
        );
        // Assert process exit, not /proc disappearance: a container init that
        // never reaps leaves the killed grandchild as a zombie with its
        // /proc entry still present.
        let mut exited = false;
        for _ in 0..100 {
            match crate::platform::unix::recorded_process_alive(grandchild, start_ticks) {
                Ok(alive) if !alive => {
                    exited = true;
                    break;
                }
                _ => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        }
        assert!(exited, "owned grandchild outlived the family stop");
    }

    /// A failed exit observation must not hang the reap: the bounded reap
    /// times out on a live child (routing to unknown-custody settlement) and
    /// still reaps an exited child.
    #[tokio::test]
    async fn bounded_reap_times_out_on_live_child() {
        fn sleep_command(secs: &str) -> tokio::process::Command {
            let mut command = tokio::process::Command::new("sleep");
            command
                .arg(secs)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null());
            command
        }

        let mut live = OwnedChild::spawn(sleep_command("30")).expect("spawn live child");
        let start = tokio::time::Instant::now();
        let error = live
            .wait_bounded(Duration::from_millis(100))
            .await
            .expect_err("a live child must not reap within the bound");
        assert_eq!(error.code, "unknown");
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "the bounded reap must not wait indefinitely"
        );
        let _ = live.terminate();
        let _ = tokio::time::timeout(Duration::from_secs(10), live.wait()).await;

        let mut exited = OwnedChild::spawn(sleep_command("0")).expect("spawn exiting child");
        assert!(
            exited.wait_bounded(Duration::from_secs(10)).await.is_ok(),
            "the bounded reap still reaps an exited child"
        );
    }
}
