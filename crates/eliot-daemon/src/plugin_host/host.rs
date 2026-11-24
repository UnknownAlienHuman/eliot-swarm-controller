use super::{PluginClient, PluginConnection};
use eliot_core::is_start_verb;
mod actor;
mod antigravity_mcp;
mod discovery;
mod native_children;
mod observation;
mod reader;
mod source_handoff;
mod source_intake;
use crate::bus::Bus;
use crate::command_guard::CommandGuardPolicy;
use crate::common::{now_ms, valid_environment_name};
use crate::harness_capabilities::HarnessCapabilities;
use crate::monitor::Monitoring;
use crate::platform::OwnedChild;
use crate::platform::custody::{AgentCustodies, AgentSpawnError, LocalPeer};
use actor::run_agent_inner;
use eliot_core::{
    Agent, Command, ContextItem, Error, InjectPoint, Result, SecretPolicy, SourceBatch, SourceGap,
    SourceGapAck, StderrSource,
    api::{ArtifactKind, ArtifactView},
};
use eliot_plugin::{
    AgentCommandParams, PluginManifest, StderrCapture, StderrView, spawn_plugin_stderr,
    sync::LockExt,
};
use eliot_store::{AgentLossEvidence, OwnedExitProof};

use serde_json::{Map, Value, json};
use source_intake::serve_plugin_requests;
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
#[expect(
    clippy::disallowed_types,
    reason = "The plugin host launches native adapter workers under owned process custody."
)]
use tokio::process::Command as ProcessCommand;
use tokio::{
    sync::{Mutex as AsyncMutex, Semaphore, mpsc, oneshot, watch},
    time::{Instant, timeout_at},
};
use ulid::Ulid;

const RPC_DEADLINE: Duration = Duration::from_secs(30);
pub(crate) const OWNER_INTERRUPT_TIMEOUT: Duration = Duration::from_secs(10);
const COMPACT_RPC_DEADLINE: Duration = Duration::from_secs(180);
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
        // agent.stop + shutdown. The router has already waited for the turn.
        let rpc_until = queued_until + RPC_DEADLINE + RPC_DEADLINE;
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

/// Read back the independent process-custody and native-source facts that
/// make a stop applied. Sampling the exact generation is bounded by one
/// absolute deadline; neither fact substitutes for the other.
async fn wait_for_stop_exit_proof(
    state: &Weak<HostState>,
    agent: &Agent,
    until: Instant,
) -> Result<OwnedExitProof> {
    let state = state
        .upgrade()
        .ok_or_else(|| Error::new("unknown", "plugin host is unavailable for stop proof"))?;
    let last = loop {
        let proof = if Instant::now() >= until {
            let store = state.store.try_lock().map_err(|_| {
                Error::new(
                    "unknown",
                    "stop proof deadline elapsed while Store was busy",
                )
            })?;
            store.owned_exit_proof(&agent.id, agent.generation)
        } else {
            timeout_at(until, state.store.lock())
                .await
                .map_err(|_| Error::new("unknown", "stop proof Store readback timed out"))?
                .owned_exit_proof(&agent.id, agent.generation)
        }
        .map_err(|error| {
            Error::new(
                "unknown",
                format!("stop proof readback failed: {}", error.message),
            )
        })?;
        if proof.is_complete() {
            return Ok(proof);
        }
        if Instant::now() >= until {
            break proof;
        }
        if !proof.process_custody_exited {
            // A single sampling failure does not invalidate independent
            // proof from the OS observer or the host's final sample.
            let _ = timeout_at(until, state.monitoring.sample_generation(agent)).await;
        }
        tokio::time::sleep_until((Instant::now() + Duration::from_millis(50)).min(until)).await;
    };
    Err(Error::new(
        "unknown",
        format!(
            "stop proof deadline elapsed: {}",
            last.missing_reason()
                .unwrap_or("process/source proof is incomplete"),
        ),
    ))
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

/// The observed loss evidence deferred while a force command settles: the
/// truthful cause with its process exit status and stderr tail together.
/// Retention is first-writer-wins so an independently observed root exit is
/// never overwritten by a later planned force-stop label.
struct DeferredLoss {
    reason: &'static str,
    evidence: AgentLossEvidence,
}

/// Own loss suppression until the force command's Store settlement commits.
/// Actor teardown may finish first, so retain its proven loss for cancellation.
pub(crate) struct ForceStopGuard {
    state: Weak<HostState>,
    key: AgentKey,
    deferred_loss: Arc<Mutex<Option<DeferredLoss>>>,
    committed: bool,
}

impl ForceStopGuard {
    // Caller holds the Store lock, also used by mark_lost_state.
    #[cfg(windows)]
    fn new(state: &Arc<HostState>, key: AgentKey, _reason: &'static str) -> Result<Self> {
        let deferred_loss = Arc::new(Mutex::new(None));
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
        let deferred = self.deferred_loss.lock_recover().take();
        // Replay the retained observation, never the planned force-stop label:
        // a force-stop that fails or is cancelled without any observed exit
        // leaves no loss behind, and an independently observed root exit keeps
        // its own cause, exit status and stderr tail.
        if !self.committed
            && let Some(deferred) = deferred
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            let key = self.key.clone();
            runtime.spawn(async move {
                mark_lost_state_with_exit(
                    &Arc::downgrade(&state),
                    &key,
                    deferred.reason,
                    deferred.evidence,
                )
                .await;
            });
        }
        // Without a runtime, the persisted exit/drain facts remain available
        // to restart reconciliation; Drop must not panic trying to spawn.
    }
}

struct RuntimeEntry {
    sender: mpsc::Sender<RuntimeMessage>,
    client: PluginClient,
    start_command_id: String,
    verbs: HashSet<String>,
    stderr: StderrView,
    source_credentials: Arc<[String]>,
}

enum RuntimeMessage {
    Command {
        command: Command,
        context: Vec<ContextItem>,
        interrupt_deadline: Option<Instant>,
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
        force: bool,
        deadlines: StopDeadlines,
        reply: oneshot::Sender<Result<Value>>,
    },
    DaemonForce {
        restart: bool,
        reply: oneshot::Sender<Result<()>>,
    },
}

pub(crate) struct PendingCommand {
    response: oneshot::Receiver<Result<Value>>,
    stderr: StderrView,
    credentials: Arc<[String]>,
    response_timeout: Option<Duration>,
    pub(crate) interrupt_deadline: Option<Instant>,
}

impl PendingCommand {
    pub(crate) async fn wait(self) -> Result<Value> {
        let result = if let Some(timeout) = self.response_timeout {
            match timeout_at(Instant::now() + timeout + RPC_GRACE, self.response).await {
                Ok(result) => result,
                Err(_) => {
                    return Err(Error::new(
                        "unknown",
                        "Agent plugin command exceeded its absolute deadline",
                    ));
                }
            }
        } else {
            self.response.await
        };
        result
            .unwrap_or_else(|_| {
                Err(Error::new(
                    "unknown",
                    "Agent plugin ended before the command outcome was returned",
                ))
            })
            .map_err(|error| diagnose_error(error, &self.stderr, &self.credentials))
    }
}

struct HostState {
    acl_notices: Arc<crate::acl_notice::AclNotices>,
    data_root: PathBuf,
    owner_token: String,
    store: StoreHandle,
    bus: Bus,
    monitoring: Monitoring,
    completion_interests: Mutex<HashMap<String, Vec<crate::automation::CompletionRuleInterest>>>,
    agent_custodies: AgentCustodies,
    secrets: SecretSet,
    agent_credentials: AgentCredentials,
    unknown_commands: UnknownCommands,
    progress: watch::Sender<u64>,
    late_batch_causes: Mutex<HashMap<AgentKey, String>>,
    running: Mutex<HashMap<AgentKey, RuntimeEntry>>,
    planned_stops: Mutex<HashMap<AgentKey, Arc<Mutex<Option<DeferredLoss>>>>>,
    startup_gate: AsyncMutex<()>,
    startup_closed: AtomicBool,
    observers: Mutex<HashMap<AgentKey, Arc<observation::ObserverEntry>>>,
    readers: Mutex<HashMap<String, discovery::Reader>>,
    discovery_closed: AtomicBool,
    reader_slots: Arc<Semaphore>,
    launch_reader_slots: Arc<Semaphore>,
}

struct AgentActorConfig {
    state: Weak<HostState>,
    monitoring: Monitoring,
    key: AgentKey,
    agent: Agent,
    start_command: Command,
    owner_rules: Option<String>,
    instance_dir: String,
    store_root: PathBuf,
    plugin_credential: String,
    _prepared_mcp: Option<antigravity_mcp::PreparedConfig>,
    target_environment: Value,
    mcp_environment: Option<Value>,
    pass_agent_identity_to_shell: bool,
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
    command_guard_policy: CommandGuardPolicy,
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
    #[allow(
        clippy::too_many_arguments,
        reason = "The operation's distinct inputs remain explicit at its established call boundary."
    )]
    pub(crate) fn new(
        options: PluginHostOptions,
        store: StoreHandle,
        bus: Bus,
        secrets: SecretSet,
        agent_credentials: AgentCredentials,
        unknown_commands: UnknownCommands,
        progress: watch::Sender<u64>,
        monitoring: Monitoring,
        acl_notices: Arc<crate::acl_notice::AclNotices>,
    ) -> Self {
        let PluginHostOptions {
            data_root,
            owner_token,
        } = options;
        Self {
            state: Arc::new(HostState {
                acl_notices,
                data_root,
                owner_token,
                store,
                bus,
                monitoring,
                completion_interests: Mutex::default(),
                agent_custodies: AgentCustodies::default(),
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
                readers: Mutex::new(HashMap::new()),
                discovery_closed: AtomicBool::new(false),
                reader_slots: Arc::new(Semaphore::new(discovery::MAX_READERS)),
                launch_reader_slots: Arc::new(Semaphore::new(discovery::MAX_LAUNCH_READERS)),
            }),
        }
    }

    pub(crate) fn install_completion_interests(
        &self,
        principal: &str,
        interests: Vec<crate::automation::CompletionRuleInterest>,
    ) {
        let mut stored = self.state.completion_interests.lock_recover();
        if interests.is_empty() {
            stored.remove(principal);
        } else {
            stored.insert(principal.to_owned(), interests);
        }
    }

    pub(crate) fn remove_completion_interest(&self, principal: &str, id: &str) {
        let mut stored = self.state.completion_interests.lock_recover();
        if let Some(rules) = stored.get_mut(principal) {
            rules.retain(|rule| rule.id != id);
            if rules.is_empty() {
                stored.remove(principal);
            }
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
        if !is_start_verb(command.verb.as_str())
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
        if HarnessCapabilities::for_harness(&agent.harness).is_none() || agent.generation <= 0 {
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
        // Only Store dispatch can mark the new generation as a fresh native
        // start after proving the initial launch input never dispatched.
        let restart_fresh_native = command
            .outcome
            .as_ref()
            .and_then(|outcome| outcome.get("restart_fresh_native"))
            .and_then(Value::as_bool)
            == Some(true);
        if restart_fresh_native {
            if command.verb != "resume"
                || agent.native_scope.is_some()
                || agent.native_ref.is_some()
            {
                return Err(Error::new(
                    "source_integrity",
                    "fresh restart has a conflicting native subject",
                ));
            }
            source_state = None;
        }
        let resume_from = if command.verb == "resume" && !restart_fresh_native {
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
        if HarnessCapabilities::of(&agent.harness).reader_executable_discovery {
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
        let (agent_token, owner_rules_receipt) = {
            let store = self.state.store.lock().await;
            let token = store
                .get_agent_token(&agent.id, agent.generation)?
                .ok_or_else(|| {
                    Error::new(
                        "bad_state",
                        "Store did not commit the generation credential before plugin start",
                    )
                })?;
            let receipt = if HarnessCapabilities::of(&agent.harness).owner_rules_delivery
                != eliot_core::api::OwnerRulesDeliveryMethod::FirstInput
            {
                store.get_agent_owner_rules_delivery(&agent.id)?
            } else {
                None
            };
            (token, receipt)
        };
        let owner_rules = owner_rules_receipt
            .map(|receipt| {
                let policy = SecretPolicy::new(self.state.secrets.lock_recover().iter());
                crate::config::settings::owner_rules_for_delivery(
                    &self.state.data_root,
                    &receipt.digest,
                    &policy,
                )
            })
            .transpose()?;
        let (mut target_environment, mcp_environment) =
            build_target_environment(&self.state.data_root, &agent, &agent_token)?;
        // A harness self-check Agent runs real tool turns, so unlike a
        // provider probe it keeps the ordinary MCP identity environment.
        let mcp_environment = if agent.is_automatic_provider_probe()
            || HarnessCapabilities::of(&agent.harness).data_root_only_mcp
        {
            Some(data_root_only_mcp_environment(&mcp_environment)?)
        } else {
            Some(mcp_environment)
        };
        let artifact_root = self
            .state
            .store
            .lock()
            .await
            .agent_artifact_root(&agent.id, agent.generation)?;
        if let Some(root) = &artifact_root {
            target_environment
                .as_object_mut()
                .ok_or_else(|| Error::new("invalid", "target environment is not an object"))?
                .insert(
                    "ELIOT_ARTIFACT_ROOT".to_owned(),
                    Value::String(root.path.clone()),
                );
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
        if let Some(root) = artifact_root
            && !agent.is_automatic_provider_probe()
            && !agent.is_harness_check()
            && target_environment
                .get(crate::daemon::AGENT_DATA_SCOPE_ENV)
                .is_some()
        {
            crate::platform::data_dir::write_agent_root_locator(
                Path::new(&root.path),
                &self.state.data_root,
            )?;
        }
        let (instance_dir, credential) = self.create_instance_dir(&key)?;
        let instance_dir = match self
            .register_plugin_instance_dir(&agent, &instance_dir)
            .await
        {
            Ok(path) => path,
            Err(error) => {
                remove_empty_instance_dir(&instance_dir);
                return Err(error);
            }
        };
        if let Err(error) = self.repair_plugin_instance_dir(&agent, &instance_dir).await {
            remove_empty_instance_dir(&instance_dir);
            return Err(error);
        }
        let prepared_mcp = if HarnessCapabilities::of(&agent.harness).prepared_mcp_config {
            match antigravity_mcp::prepare(&agent, &target_environment, &instance_dir) {
                Ok(config) => config,
                Err(error) => {
                    remove_empty_instance_dir(&instance_dir);
                    return Err(error.with_code("native_not_dispatched"));
                }
            }
        } else {
            None
        };
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
            drop(prepared_mcp);
            remove_empty_instance_dir(&instance_dir);
            return Err(error);
        }
        let spawn_started = Instant::now();
        // Per-spawn crash nonce, written as the plugin's first stdin line
        // (never argv or environment): only reports carrying it and the owned
        // PID count as plugin crashes.
        let crash_nonce = format!("{}{}", Ulid::new(), Ulid::new());
        let spawned = self.spawn_plugin(&agent, &instance_dir).await;
        let mut child = match spawned {
            Ok(child) => {
                *may_have_spawned = true;
                child
            }
            Err(error) => {
                drop(prepared_mcp);
                remove_empty_instance_dir(&instance_dir);
                return Err(match error {
                    AgentSpawnError::NotSpawned(error)
                    | AgentSpawnError::ProgramUnrunnable(error) => error,
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
                return Err(self
                    .cleanup_startup_setup_failure(
                        child,
                        &agent,
                        &key,
                        error,
                        "swarm.start.process_observer",
                    )
                    .await);
            }
        };
        if let Err(error) = self.state.monitoring.attach(&agent, observer).await {
            return Err(self
                .cleanup_startup_setup_failure(
                    child,
                    &agent,
                    &key,
                    error,
                    "swarm.start.monitor_attach",
                )
                .await);
        }
        self.state.secrets.lock_recover().insert(credential.clone());
        let stdin = child.take_stdin();
        let stdout = child.take_stdout();
        let stderr = child.take_stderr();
        let (Some(stdin), Some(stdout), Some(stderr)) = (stdin, stdout, stderr) else {
            let error = self
                .cleanup_startup_setup_failure(
                    child,
                    &agent,
                    &key,
                    Error::new(
                        "process_pipes",
                        "Agent plugin did not expose all three owned pipes",
                    ),
                    "swarm.start.process_pipes",
                )
                .await;
            self.remove_plugin_secret(&credential);
            return Err(error);
        };

        // The plugin reads this line before anything else. A write failure
        // means the plugin already exited; the owned exit path settles it.
        let mut stdin = stdin;
        {
            use tokio::io::AsyncWriteExt;
            let _ = stdin.write_all(format!("{crash_nonce}\n").as_bytes()).await;
            let _ = stdin.flush().await;
        }
        let telemetry_policy = Arc::new(SecretPolicy::new(source_credentials.iter()));
        let debug_policy = Arc::clone(&telemetry_policy);
        let debug_agent = agent.id.clone();
        let debug_generation = agent.generation as u64;
        let crash_line_prefix = format!(
            "{} {crash_nonce} {} ",
            crate::crash::PLUGIN_CRASH_LINE,
            child.pid().unwrap_or_default()
        );
        let stderr_capture = spawn_plugin_stderr(
            stderr,
            telemetry_policy,
            move |line| {
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
            },
            crash_line_prefix,
        );
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
                command_guard_policy: CommandGuardPolicy::from_profile(&agent.profile_snapshot)?,
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
                    client: client.clone(),
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
            owner_rules,
            instance_dir: instance_dir.to_string_lossy().into_owned(),
            store_root: self.state.data_root.join("plugins"),
            plugin_credential: credential.clone(),
            _prepared_mcp: prepared_mcp,
            target_environment,
            mcp_environment,
            pass_agent_identity_to_shell: agent
                .profile_snapshot
                .get("pass_agent_identity_to_shell")
                .and_then(Value::as_bool)
                .unwrap_or(true),
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

    /// Enqueue under the router's Store writer lock, immediately after commit.
    /// The actor performs all native I/O after this nonblocking handoff.
    pub(crate) fn enqueue_command(
        &self,
        agent: &Agent,
        command: Command,
        context: Vec<ContextItem>,
    ) -> Result<PendingCommand> {
        let key = AgentKey {
            id: agent.id.clone(),
            generation: agent.generation,
        };
        if command.target.as_deref() != Some(agent.id.as_str())
            || command.generation != Some(agent.generation)
        {
            return Err(Error::new(
                "native_not_dispatched",
                "command does not match the owned Agent generation",
            ));
        }
        match command.verb.as_str() {
            "steer" | "interrupt" if command.turn_id.is_none() && !command.is_owner_interrupt() => {
                return Err(Error::new(
                    "native_not_dispatched",
                    "turn control requires the captured active turn",
                ));
            }
            "approve" | "deny" | "answer"
                if command.request_id.is_none() || command.turn_id.is_none() =>
            {
                return Err(Error::new(
                    "native_not_dispatched",
                    "request reply requires its captured request and turn ids",
                ));
            }
            _ => {}
        }
        let (sender, stderr, credentials) = self.sender(&key, &command.verb)?;
        let response_timeout = match command.verb.as_str() {
            "steer" => None,
            "compact" => Some(COMPACT_RPC_DEADLINE),
            _ => Some(RPC_DEADLINE),
        };
        let (reply, response) = oneshot::channel();
        let interrupt_deadline = command
            .is_owner_interrupt()
            .then(|| Instant::now() + OWNER_INTERRUPT_TIMEOUT);
        sender
            .try_send(RuntimeMessage::Command {
                command,
                context,
                interrupt_deadline,
                reply,
            })
            .map_err(|error| {
                let message = match error {
                    mpsc::error::TrySendError::Full(_) => "Agent plugin command queue is full",
                    mpsc::error::TrySendError::Closed(_) => "Agent plugin command queue is closed",
                };
                Error::new("native_not_dispatched", message)
            })?;
        Ok(PendingCommand {
            response,
            stderr,
            credentials,
            response_timeout,
            interrupt_deadline,
        })
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
        let deadlines = StopDeadlines::new(Instant::now());
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
            Ok(sender) => sender,
            Err(error) => {
                let value = self
                    .reconcile_stop_error(agent, error, deadlines.reply_until)
                    .await?;
                return Ok((value, None));
            }
        };
        let (reply, response) = oneshot::channel();
        if let Err(error) = enqueue_stop(
            &sender,
            RuntimeMessage::Stop {
                command,
                force,
                deadlines,
                reply,
            },
            deadlines.queued_until,
        )
        .await
        {
            let error = diagnose_error(error, &stderr, &credentials);
            let value = self
                .reconcile_stop_error(agent, error, deadlines.reply_until)
                .await?;
            return Ok((value, None));
        }
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
        match result {
            Ok(mut value) => {
                let proof = wait_for_stop_exit_proof(
                    &Arc::downgrade(&self.state),
                    agent,
                    deadlines.reply_until,
                )
                .await?;
                if let Some(fields) = value.as_object_mut() {
                    fields.insert(
                        "process_custody_exit_proven".to_owned(),
                        Value::Bool(proof.process_custody_exited),
                    );
                    fields.insert(
                        "native_source_drain_proven".to_owned(),
                        Value::Bool(proof.native_source_drained),
                    );
                }
                Ok((value, None))
            }
            Err(error) => {
                let error = diagnose_error(error, &stderr, &credentials);
                self.reconcile_stop_error(agent, error, deadlines.reply_until)
                    .await
                    .map(|value| (value, None))
            }
        }
    }

    async fn reconcile_stop_error(
        &self,
        agent: &Agent,
        native_error: Error,
        until: Instant,
    ) -> Result<Value> {
        // A fenced actor may still be waiting on its family and draining its
        // event task. Keep that work within the original stop deadline.
        let runtime_gone = !self.controls_generation(&agent.id, agent.generation);
        let grace =
            if runtime_gone || matches!(native_error.code.as_str(), "unknown" | "unavailable") {
                STOP_GRACE * 3 + RPC_GRACE
            } else {
                RPC_GRACE
            };
        let proof_until = (Instant::now() + grace).min(until);
        match wait_for_stop_exit_proof(&Arc::downgrade(&self.state), agent, proof_until).await {
            Ok(proof) => Ok(json!({
                "state": "applied",
                "process_custody_exit_proven": proof.process_custody_exited,
                "native_source_drain_proven": proof.native_source_drained,
                "native_session_end_observed": false,
                "native_stop_error_code": native_error.code.as_str(),
                "native_stop_reason": "native_session_end_unobserved",
                "graceful": false,
            })),
            Err(proof_error) => {
                let message = format!("{}; {}", native_error.message, proof_error.message);
                let error = native_error.with_message(message);
                if runtime_gone || !self.controls_generation(&agent.id, agent.generation) {
                    Err(error.with_code("unknown"))
                } else {
                    Err(error)
                }
            }
        }
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

    /// The start command of the owned generation. A self-started native turn
    /// has no send, so its exact-turn readback binds to this command.
    pub(crate) fn session_start_command(&self, agent: &Agent) -> Option<String> {
        let key = AgentKey {
            id: agent.id.clone(),
            generation: agent.generation,
        };
        let running = self.state.running.lock_recover();
        running
            .get(&key)
            .map(|entry| entry.start_command_id.clone())
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

    /// Whether this Agent generation's own terminal write for the exact turn
    /// reached the host and intake has not finished it. Shared recovery
    /// defers while it is, for every adapter.
    pub(crate) fn terminal_write_in_flight(&self, agent: &Agent, turn_id: &str) -> bool {
        let key = AgentKey {
            id: agent.id.clone(),
            generation: agent.generation,
        };
        self.state
            .running
            .lock_recover()
            .get(&key)
            .is_some_and(|entry| {
                entry
                    .client
                    .terminal_write_in_flight(&agent.id, agent.generation, turn_id)
            })
    }

    pub(crate) fn close_startup_admission(&self) {
        self.state.startup_closed.store(true, Ordering::SeqCst);
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn restart_groups(&self) -> Vec<crate::platform::unix::ProcessGroupIdentity> {
        self.state.agent_custodies.restart_groups()
    }

    pub(crate) async fn force_all(&self, restart: bool) {
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
                .send(RuntimeMessage::DaemonForce { restart, reply })
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
        self.state.running.lock_recover().contains_key(&key)
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

    pub(crate) async fn agent_models(&self, agent_id: &str, generation: i64) -> Result<Value> {
        let key = AgentKey {
            id: agent_id.to_owned(),
            generation,
        };
        let (client, credentials) = {
            let running = self.state.running.lock_recover();
            let entry = running.get(&key).ok_or_else(|| {
                Error::new(
                    "unavailable",
                    "OpenCode Agent has no live plugin for this generation",
                )
            })?;
            if !entry.verbs.contains("models") {
                return Err(Error::new(
                    "unavailable",
                    "owned plugin has not advertised model reads",
                ));
            }
            (entry.client.clone(), Arc::clone(&entry.source_credentials))
        };
        let params = serde_json::to_value(eliot_plugin::protocol::AgentModelsParams {
            agent_id: key.id.clone(),
            generation,
        })
        .map_err(|_| Error::new("internal", "agent.models params encoding failed"))?;
        let result = client
            .call(
                "agent.models",
                params,
                Instant::now() + Duration::from_secs(8),
            )
            .await
            .map_err(|error| redact_native_error(error, &credentials))?;
        if !self.state.running.lock_recover().contains_key(&key) {
            return Err(Error::new(
                "unavailable",
                "OpenCode Agent stopped during its model read",
            ));
        }
        if SecretPolicy::new(credentials.iter()).contains_value(&result) {
            return Err(Error::new(
                "source_integrity",
                "agent.models returned reserved credentials",
            ));
        }
        validate_models_catalog_result(result, &key)
    }

    pub(crate) fn check_steer_target(&self, agent: &Agent) -> Result<()> {
        if !agent.steers_at_turn_safe_point() {
            return Ok(());
        }
        let key = AgentKey {
            id: agent.id.clone(),
            generation: agent.generation,
        };
        let running = self.state.running.lock_recover();
        let Some(entry) = running.get(&key) else {
            return Err(Error::new(
                "bad_state",
                "Agent has no live runtime to receive steer",
            ));
        };
        if !entry.verbs.contains("steer") {
            return Err(Error::new(
                "unsupported",
                format!("{} Agent does not support steer", agent.harness),
            ));
        }
        Ok(())
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
                    "native_not_dispatched",
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

    async fn register_plugin_instance_dir(&self, agent: &Agent, path: &Path) -> Result<PathBuf> {
        let path = self.canonical_plugin_instance_dir(path)?;
        let path_text = path.to_string_lossy().into_owned();
        let identity = crate::platform::script_file::directory_identity(&path)?;
        let mut store = self.state.store.lock().await;
        let current = store
            .get_agent(&agent.id)?
            .ok_or_else(|| Error::new("stale_generation", "plugin Agent generation absent"))?;
        if current.generation != agent.generation {
            return Err(Error::new(
                "stale_generation",
                "plugin Agent generation changed before instance registration",
            ));
        }
        let artifact = ArtifactView {
            id: Ulid::new().to_string(),
            agent: agent.id.clone(),
            generation: agent.generation,
            project: agent.project.clone(),
            tree: Vec::new(),
            path: path_text.clone(),
            kind: ArtifactKind::Other,
            purpose: "Swarm plugin instance directory".to_owned(),
            identity: identity.clone(),
            created_at: now_ms(),
            expires_at: None,
            state: "registered".to_owned(),
            git: None,
        };
        store.record_known_artifact(artifact)?;
        let Some(readback) = store.known_artifact_path(&path_text)? else {
            return Err(Error::new(
                "source_integrity",
                "plugin instance ownership was not retained",
            ));
        };
        if readback.agent != agent.id
            || readback.generation != agent.generation
            || readback.kind != ArtifactKind::Other
            || readback.purpose != "Swarm plugin instance directory"
            || readback.identity != identity
        {
            return Err(Error::new(
                "source_integrity",
                "plugin instance ownership readback changed",
            ));
        }
        Ok(path)
    }

    async fn repair_plugin_instance_dir(&self, agent: &Agent, path: &Path) -> Result<()> {
        let path = self.canonical_plugin_instance_dir(path)?;
        let path_text = path.to_string_lossy().into_owned();
        let identity = crate::platform::script_file::directory_identity(&path)?;
        {
            let store = self.state.store.lock().await;
            let Some(readback) = store.known_artifact_path(&path_text)? else {
                return Err(Error::new(
                    "source_integrity",
                    "plugin instance ownership is absent before ACL repair",
                ));
            };
            if readback.agent != agent.id
                || readback.generation != agent.generation
                || readback.kind != ArtifactKind::Other
                || readback.purpose != "Swarm plugin instance directory"
                || readback.identity != identity
            {
                return Err(Error::new(
                    "source_integrity",
                    "plugin instance ownership changed before ACL repair",
                ));
            }
        }

        let foreign_aces = crate::platform::repair_managed_dir_acl(&path)?;
        if foreign_aces.is_empty() {
            return Ok(());
        }
        let secrets: Vec<_> = self.state.secrets.lock_recover().iter().cloned().collect();
        let policy = SecretPolicy::new(secrets);
        self.state
            .acl_notices
            .report(&self.state.store, &self.state.bus, &policy, foreign_aces)
            .await;
        Ok(())
    }

    fn plugin_instance_root(&self) -> Result<PathBuf> {
        let data_root = std::fs::canonicalize(&self.state.data_root)
            .map_err(|_| Error::new("unavailable", "daemon data root is unavailable"))?;
        let path = self.state.data_root.join("plugins");
        crate::git_monitor::reject_links(&path)?;
        let plugins = std::fs::canonicalize(&path)
            .map_err(|_| Error::new("unavailable", "plugin instance root is unavailable"))?;
        crate::git_monitor::reject_links(&plugins)?;
        if plugins.parent() != Some(data_root.as_path()) {
            return Err(Error::new(
                "not_allowed",
                "plugin instance root is outside the daemon data directory",
            ));
        }
        Ok(plugins)
    }

    fn canonical_plugin_instance_dir(&self, path: &Path) -> Result<PathBuf> {
        crate::git_monitor::reject_links(path)?;
        let plugins = self.plugin_instance_root()?;
        let path = std::fs::canonicalize(path)
            .map_err(|_| Error::new("unavailable", "plugin instance directory unavailable"))?;
        crate::git_monitor::reject_links(&path)?;
        if path.parent() != Some(plugins.as_path()) {
            return Err(Error::new(
                "not_allowed",
                "plugin instance directory is outside the daemon-managed plugin root",
            ));
        }
        Ok(path)
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
            self.plugin_instance_root()?;
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
            self.plugin_instance_root()?;
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
            .plugin_command(&agent.harness, Path::new(workspace), true)
            .map_err(AgentSpawnError::NotSpawned)?;
        self.state.agent_custodies.spawn(&agent.id, || {
            OwnedChild::spawn_agent(command, &namespace, &agent.id, agent.generation)
        })
    }

    pub(crate) async fn authorize_local_owner(
        &self,
        peer: Option<LocalPeer>,
    ) -> Result<Option<LocalPeer>> {
        self.state.agent_custodies.authorize_owner(peer).await
    }

    fn spawn_plugin_process(
        &self,
        harness: &str,
        workspace: &Path,
        observation: Option<&Agent>,
    ) -> Result<OwnedChild> {
        let command = self.plugin_command(harness, workspace, false)?;
        if let Some(agent) = observation {
            self.state
                .agent_custodies
                .spawn(&agent.id, || {
                    OwnedChild::spawn(command).map_err(AgentSpawnError::NotSpawned)
                })
                .map_err(AgentSpawnError::into_error)
        } else {
            OwnedChild::spawn(command)
        }
    }

    #[expect(
        clippy::disallowed_types,
        reason = "This builds the configured process command for one harness plugin."
    )]
    fn plugin_command(
        &self,
        harness: &str,
        workspace: &Path,
        crash_nonce_stdin: bool,
    ) -> Result<ProcessCommand> {
        let executable = std::env::current_exe().map_err(|error| {
            Error::new("platform_io", format!("locate Eliot executable: {error}"))
        })?;
        #[expect(
            clippy::disallowed_methods,
            reason = "This statement constructs a process owned by this module."
        )]
        let mut command = ProcessCommand::new(executable);
        command
            .arg("--data-dir")
            .arg(&self.state.data_root)
            .arg("plugin");
        if crash_nonce_stdin {
            command.arg("--crash-nonce-stdin");
        }
        command
            .arg(harness)
            .current_dir(workspace)
            .env_clear()
            .envs(minimal_plugin_environment())
            .kill_on_drop(true)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        // Debug-only owner fault hook: the plugin entry point panics when
        // `ELIOT_DEBUG_PLUGIN_PANIC=1`. The child environment is rebuilt from
        // a fixed base list, so forward this one flag explicitly or the hook
        // is unreachable through the daemon. Compiled out of release builds
        // with the trigger itself.
        #[cfg(debug_assertions)]
        if let Some(value) = std::env::var_os("ELIOT_DEBUG_PLUGIN_PANIC") {
            command.env("ELIOT_DEBUG_PLUGIN_PANIC", value);
        }
        #[cfg(debug_assertions)]
        if crash_nonce_stdin
            && self
                .state
                .data_root
                .join(crate::stand_cleanup::STAND_MARKER)
                .is_file()
            && let Some(value) = std::env::var_os(eliot_core::limits::DEBUG_NATIVE_INACTIVITY_ENV)
        {
            command.env(eliot_core::limits::DEBUG_NATIVE_INACTIVITY_ENV, value);
        }
        Ok(command)
    }

    fn remove_plugin_secret(&self, credential: &str) {
        self.state.secrets.lock_recover().remove(credential);
    }

    async fn cleanup_startup_setup_failure(
        &self,
        child: OwnedChild,
        agent: &Agent,
        key: &AgentKey,
        error: Error,
        location: &'static str,
    ) -> Error {
        let exited = terminate_child(child, &self.state.monitoring, agent).await;
        let settled = settle_owned_source_exit(
            &Arc::downgrade(&self.state),
            key,
            None,
            exited,
            "plugin_startup_setup_failed",
        )
        .await;
        let error = match settled {
            Ok(()) => error,
            Err(cleanup) => {
                let message = format!("{}; {}", error.message, cleanup.message);
                error.with_message(message)
            }
        };
        error.with_where(location).with_origin(
            eliot_core::FailureOrigin::Swarm,
            "Swarm could not set up the owned plugin before its handshake",
        )
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
            let guard = ForceStopGuard::new(
                &self.state,
                key,
                "daemon_force_exit_without_session_terminal",
            )?;
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
                        "process_custody_exit_proven": true,
                        "native_source_drain_proven": true,
                        "native_session_end_observed": false,
                        "native_stop_reason": "native_session_end_unobserved",
                    }),
                    Some(guard),
                ));
            }
            #[expect(
                clippy::disallowed_methods,
                reason = "This bounded wait is owned by the surrounding operation."
            )]
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

/// Remove one registered plugin instance only after its exact generation is
/// terminal and quiescent.
/// The filesystem operation remains empty-only and follows both path and inode
/// custody checks so plugin data and substituted directories are preserved.
async fn remove_empty_instance_dir_after_generation_end(
    state: &Weak<HostState>,
    key: &AgentKey,
    path: &Path,
    start_command_id: &str,
    progress: &mut watch::Receiver<u64>,
) {
    let Some(host_state) = state.upgrade() else {
        return;
    };
    let Ok(data_root) = std::fs::canonicalize(&host_state.data_root) else {
        return;
    };
    let plugin_root_path = host_state.data_root.join("plugins");
    if crate::git_monitor::reject_links(&plugin_root_path).is_err() {
        return;
    }
    let Ok(plugin_root) = std::fs::canonicalize(&plugin_root_path) else {
        return;
    };
    if crate::git_monitor::reject_links(&plugin_root).is_err()
        || plugin_root.parent() != Some(data_root.as_path())
        || plugin_root.file_name().and_then(|name| name.to_str()) != Some("plugins")
    {
        return;
    }
    let Ok(path) = std::fs::canonicalize(path) else {
        return;
    };
    if crate::git_monitor::reject_links(&path).is_err()
        || path.parent() != Some(plugin_root.as_path())
    {
        return;
    }
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return;
    };
    let prefix = format!("{}-g{}-", key.id, key.generation);
    let Some(instance_id_text) = name.strip_prefix(&prefix) else {
        return;
    };
    let Ok(instance_id) = Ulid::from_string(instance_id_text) else {
        return;
    };
    if instance_id.to_string() != instance_id_text {
        return;
    }
    let path_text = path.to_string_lossy().into_owned();
    let Ok(identity) = crate::platform::script_file::directory_identity(&path) else {
        return;
    };
    let settle_deadline = Instant::now() + STOP_GRACE;
    loop {
        let Some(host_state) = state.upgrade() else {
            return;
        };
        let mut store = host_state.store.lock().await;
        let Ok(Some(agent)) = store.get_agent(&key.id) else {
            return;
        };
        if agent.generation != key.generation {
            return;
        }
        let start_pending = store
            .get_command(start_command_id)
            .ok()
            .flatten()
            .is_some_and(|command| !command.state.is_terminal());
        let stop_pending = store
            .generation_has_open_stop(&key.id, key.generation)
            .unwrap_or(false);
        let cleanup_terminal = agent.state == eliot_core::AgentState::Stopped
            || agent.state == eliot_core::AgentState::Failed
            || (agent.is_automatic_provider_probe() && agent.state == eliot_core::AgentState::Lost);
        if !cleanup_terminal {
            if !start_pending && !stop_pending {
                return;
            }
            drop(store);
            if !matches!(
                timeout_at(settle_deadline, progress.changed()).await,
                Ok(Ok(()))
            ) {
                return;
            }
            continue;
        }
        let Ok(Some(artifact)) = store.known_artifact_path(&path_text) else {
            return;
        };
        if artifact.agent != key.id
            || artifact.generation != key.generation
            || artifact.kind != ArtifactKind::Other
            || artifact.purpose != "Swarm plugin instance directory"
            || artifact.path != path_text
            || artifact.identity != identity
            || artifact.state != "registered"
        {
            return;
        }
        if !store.cleanup_agent_quiescent(&artifact).unwrap_or(false) {
            drop(store);
            if !matches!(
                timeout_at(settle_deadline, progress.changed()).await,
                Ok(Ok(()))
            ) {
                return;
            }
            continue;
        }
        if crate::git_monitor::reject_links(&path).is_err()
            || std::fs::canonicalize(&path).ok().as_deref() != Some(path.as_path())
            || crate::platform::script_file::directory_identity(&path)
                .ok()
                .as_deref()
                != Some(artifact.identity.as_str())
        {
            return;
        }
        match std::fs::remove_dir(&path) {
            Ok(()) => {
                if let Err(error) = store.settle_empty_plugin_instance_removal(&artifact) {
                    record_plugin_instance_cleanup_error(&host_state, key, &error);
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if let Err(error) = store.settle_empty_plugin_instance_removal(&artifact) {
                    record_plugin_instance_cleanup_error(&host_state, key, &error);
                }
            }
            Err(error) => {
                let nonempty = std::fs::read_dir(&path)
                    .ok()
                    .is_some_and(|mut entries| entries.next().is_some());
                if !nonempty {
                    let error = Error::new(
                        "platform_io",
                        format!("remove empty plugin instance directory: {error}"),
                    );
                    record_plugin_instance_cleanup_error(&host_state, key, &error);
                }
            }
        }
        return;
    }
}

fn record_plugin_instance_cleanup_error(state: &HostState, key: &AgentKey, error: &Error) {
    let secrets: Vec<_> = state.secrets.lock_recover().iter().cloned().collect();
    let policy = eliot_core::SecretPolicy::new(secrets);
    let generation = key.generation.to_string();
    crate::logging::record(
        &policy,
        "daemon.plugin_instance_cleanup_failed",
        &[
            ("agent", key.id.as_str()),
            ("generation", generation.as_str()),
        ],
        Some(error),
    );
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
    let state = actor.state.clone();
    let instance_dir = PathBuf::from(actor.instance_dir.clone());
    let start_command_id = actor.start_command.id.clone();
    let mut progress = state
        .upgrade()
        .map(|host_state| host_state.progress.subscribe());
    let stderr = actor.stderr.clone();
    let mut stderr_capture = Some(stderr_capture);
    let mut failure = None;
    run_agent_inner(
        actor,
        Some(child),
        client,
        event_task,
        messages,
        started,
        &mut stderr_capture,
        &mut failure,
    )
    .await;
    // All existing child/custody cleanup paths have completed or dropped their
    // owning handle. Finish the sole stderr drain before the final error view.
    let snapshot = match stderr_capture {
        Some(capture) => capture.finish(Instant::now() + STOP_GRACE).await,
        None => stderr.snapshot(),
    };
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
    if let Some(progress) = progress.as_mut() {
        remove_empty_instance_dir_after_generation_end(
            &state,
            &key,
            &instance_dir,
            &start_command_id,
            progress,
        )
        .await;
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
    stderr_capture: &mut Option<StderrCapture>,
    pending_failure: &mut Option<PendingFailure>,
    proven_no_dispatch: bool,
    origin: Option<eliot_core::FailureOrigin>,
) {
    let (exited, exit) = terminate_child_with_exit(child, &actor.monitoring, &actor.agent).await;
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
        // The stderr reader is independent of the source reader. Join it
        // before inspecting the final bytes of a plugin that panicked.
        let capture = stderr_capture
            .take()
            .expect("startup owns the stderr capture until cleanup");
        let crash_line_prefix = capture.view().crash_line_prefix().map(str::to_owned);
        let stderr = capture.finish(Instant::now() + STOP_GRACE).await;
        let marker = plugin_crash_marker(&stderr.text, crash_line_prefix.as_deref());
        let (origin, origin_evidence) = match marker {
            Some(marker) => {
                record_plugin_crash(&actor.state, &marker);
                (
                    Some(eliot_core::FailureOrigin::Swarm),
                    Some(marker.summary()),
                )
            }
            None => (origin, None),
        };
        mark_lost_state_with_exit(
            &actor.state,
            &actor.key,
            "plugin_startup_exit",
            AgentLossEvidence {
                exit,
                stderr_tail: Some(stderr.text),
                origin,
                origin_evidence,
            },
        )
        .await;
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
    let outcome = match error.code.as_str() {
        "unknown" | "busy" | "not_allowed" | "stale_generation" => error.code.as_str(),
        _ => "rejected",
    };
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
            ("outcome", outcome),
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
    let boundary: Option<eliot_core::CompactionGapBoundary> = params
        .get("compaction_boundary")
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .map_err(|_| Error::new("invalid", "source.gap compaction boundary is malformed"))?;
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
    let (result, write_faulted, can_continue) = {
        let mut store = timeout_at(until, store.lock())
            .await
            .map_err(|_| Error::new("busy", "Store lock deadline elapsed for source.gap"))?;
        let can_continue = subject.is_none()
            && match &boundary {
                Some(boundary) => allow_compaction_gap(&store, &gap, boundary)?,
                None => false,
            };
        let result = match subject {
            Some(subject) => store.record_observed_source_gap(gap, subject, now_ms()),
            None => store.record_source_gap(gap, now_ms()),
        };
        if let Ok(change) = &result {
            bus.publish(&change.events);
        }
        (result, store.is_write_faulted(), can_continue)
    };
    match result {
        Ok(change) => {
            let ack: SourceGapAck = change.value;
            let mut encoded = serde_json::to_value(ack).map_err(|_| {
                Error::new(
                    "internal",
                    "source.gap acknowledgement could not be encoded",
                )
            })?;
            if boundary.is_some() {
                encoded["can_continue"] = json!(can_continue);
            }
            Ok(encoded)
        }
        Err(_) if write_faulted => Err(Error::new("unknown", "source.gap outcome is unknown")),
        Err(error) => Err(redact_native_error(error, credentials)),
    }
}

fn allow_compaction_gap(
    store: &eliot_store::Store,
    gap: &SourceGap,
    boundary: &eliot_core::CompactionGapBoundary,
) -> Result<bool> {
    let from = gap
        .from
        .as_deref()
        .and_then(|value| value.parse::<u64>().ok());
    let to = gap
        .to
        .as_deref()
        .and_then(|value| value.parse::<u64>().ok());
    if gap.source != "stream"
        || to.is_none()
        || from.and_then(|seq| seq.checked_add(1)) != to
        || !boundary
            .start_seq
            .zip(from)
            .is_some_and(|(start, missing)| start < missing)
        || !boundary.usage.as_ref().is_some_and(|usage| {
            [
                usage.cost,
                usage.input_tokens,
                usage.output_tokens,
                usage.reasoning_tokens,
                usage.cache_read_tokens,
                usage.cache_write_tokens,
            ]
            .into_iter()
            .all(|value| value.is_finite() && value >= 0.0)
        })
    {
        return Ok(false);
    }
    let Some(agent) = store.get_agent(&gap.agent_id)?.filter(|agent| {
        agent.harness == "opencode"
            && agent.generation == gap.generation
            && agent.native_ref.as_deref() == Some(boundary.native_ref.as_str())
    }) else {
        return Ok(false);
    };
    match &boundary.activity {
        Some(eliot_core::CompactionActivity::Turn { turn_id }) => {
            Ok(agent.active_turn.as_deref() == Some(turn_id.as_str()))
        }
        Some(eliot_core::CompactionActivity::Command { command_id })
            if agent.state == eliot_core::AgentState::Idle && agent.active_turn.is_none() =>
        {
            Ok(store.get_command(command_id)?.is_some_and(|command| {
                command.verb == "compact"
                    && command.target.as_deref() == Some(gap.agent_id.as_str())
                    && command.generation == Some(gap.generation)
                    && command.state == eliot_core::CommandState::Dispatched
            }))
        }
        _ => Ok(false),
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
    // The boundary behind every persisted outcome, event and log record:
    // redacted message, origin evidence and stderr tails, normalized
    // `where`, every bound re-applied.
    error.sanitized(&SecretPolicy::new(credentials))
}

fn diagnose_error(error: Error, stderr: &StderrView, credentials: &[String]) -> Error {
    // This view is obtained from running[AgentKey] and drains exactly one
    // owned process pipe. A disk marker from another process/generation can
    // never enter this path. Marker evidence never replaces the effect code.
    let snapshot = stderr.snapshot();
    let mut error = error;
    if let Some(marker) = plugin_crash_marker(&snapshot.text, stderr.crash_line_prefix()) {
        let summary = marker.summary();
        error.origin = Some(eliot_core::FailureOrigin::Swarm);
        if error.failure_where.is_none() {
            error.failure_where = Some(eliot_core::PLUGIN_EXIT_WHERE.into());
        }
        error.origin_evidence = Some(
            match error.origin_evidence.as_deref() {
                Some(evidence) => format!("{evidence}; {summary}"),
                None => summary,
            }
            .into(),
        );
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
            args.remove("inline_digest");
        }
        "configure" => {
            // The model/effort overlay rides to the plugin verbatim: the
            // plugin performs the harness-native switch (OpenCode,
            // Antigravity) and proves it before the Router stores anything.
            // A configure carries no turn or request binding.
        }
        "compact" => {
            if !args.is_empty() || command.turn_id.is_some() || command.request_id.is_some() {
                return Err(Error::new(
                    "native_not_dispatched",
                    "compact requires an idle session binding",
                ));
            }
        }
        "steer" | "interrupt" => {
            if !command.is_owner_interrupt() && command.turn_id.is_none() {
                return Err(Error::new(
                    "stale_generation",
                    "turn control has no captured turn id",
                ));
            }
        }
        "inject" => {
            args.remove("_trusted_review_labels");
            // Direct active_turn inject: keep the original typed payload and the
            // captured turn. Any builder failure here is known before plugin
            // I/O, so it classifies native_not_dispatched (Rejected), never
            // a generic unsupported Unknown.
            let typed = eliot_core::decode_retained_inject_args(&command.args)
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
/// Git roots for a sandboxed harness on a managed worktree, including the
/// managed branch's ref and reflog parents needed by native Git.
async fn resolve_commit_roots(actor: &AgentActorConfig) -> Vec<String> {
    if !crate::work_truth::needs_commit_roots(&actor.agent.harness, &actor.agent.profile_snapshot) {
        return Vec::new();
    }
    let (Some(workspace), Some(state)) = (actor.agent.workspace.as_deref(), actor.state.upgrade())
    else {
        return Vec::new();
    };
    let managed = state.store.lock().await.worktree_branch_base(workspace);
    let Ok(Some((branch, _))) = managed else {
        return Vec::new();
    };
    crate::work_truth::commit_roots(workspace, &branch)
        .await
        .unwrap_or_default()
}

async fn resolve_resume_config(
    actor: &AgentActorConfig,
) -> Result<Option<(Option<String>, Option<String>, i64)>> {
    if actor.start_command.verb != "resume"
        || !HarnessCapabilities::of(&actor.agent.harness).resume_config
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
                let effort = if HarnessCapabilities::of(&actor.agent.harness).effort {
                    config.effort
                } else {
                    None
                };
                (config.model, effort, config.revision)
            })
        })
}

fn agent_command_params(
    agent: &Agent,
    command: &Command,
    context: Vec<ContextItem>,
    config: Option<&eliot_store::AgentConfig>,
) -> Result<Value> {
    let mut args = plugin_command_args(command)?;
    if let Some(args) = args.as_object_mut() {
        args.remove("context_items");
    }
    let config = config.filter(|_| command.verb == "send");
    serde_json::to_value(AgentCommandParams {
        agent_id: agent.id.clone(),
        generation: agent.generation,
        command_id: command.id.clone(),
        verb: command.verb.clone(),
        owner_control: command.is_owner_interrupt(),
        turn_id: command.turn_id.clone(),
        request_id: command.request_id.clone(),
        args,
        context,
        model_override: config.and_then(|config| config.model.clone()),
        effort_override: config.and_then(|config| config.effort.clone()),
        config_revision: config.map(|config| config.revision),
    })
    .map_err(|_| {
        Error::new(
            "native_not_dispatched",
            "plugin command could not be encoded",
        )
    })
}

fn validate_agent_command_result(value: Value, harness: &str, command: &Command) -> Result<Value> {
    let verb = command.verb.as_str();
    match value.get("state").and_then(Value::as_str) {
        Some("applied") if verb == "send" => validate_send_result(value),
        Some("applied")
            if verb == "configure"
                && HarnessCapabilities::of(harness).configure_reports_accepted_model =>
        {
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
fn validate_models_catalog_result(value: Value, key: &AgentKey) -> Result<Value> {
    if value.get("provenance").and_then(Value::as_str) != Some("session")
        || value.get("agent_id").and_then(Value::as_str) != Some(key.id.as_str())
        || value.get("generation").and_then(Value::as_i64) != Some(key.generation)
    {
        return Err(Error::new(
            "source_integrity",
            "plugin session catalogue did not bind to the requested live Agent generation",
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
        "compact",
    ];
    if verbs.len() != manifest.verbs.len()
        || verbs.len() < 3
        || !["launch", "send", "stop"]
            .iter()
            .all(|required| verbs.contains(required))
        || verbs.iter().any(|verb| {
            !implemented_verbs.contains(verb)
                && !HarnessCapabilities::of(harness)
                    .manifest_extra_verbs
                    .contains(verb)
        })
        || !manifest.child_verbs.is_empty()
        || manifest.features.start_requires_input
        || (manifest.features.native_children
            && eliot_core::native_child_families_for_harness(harness).is_none())
    {
        return Err(unsupported_manifest());
    }

    if HarnessCapabilities::for_harness(harness).is_none() {
        return Err(unsupported_manifest());
    }
    if verbs.contains("compact") && !HarnessCapabilities::of(harness).compact {
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
        for name in names
            .iter()
            .filter_map(Value::as_str)
            .chain(profile["api_key_env"].as_str())
        {
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
                    && !crate::config::claude_model_selector(profile, name)
                    && !environment
                        .as_object()
                        .into_iter()
                        .flatten()
                        .any(|(key, value)| {
                            (key == name || (cfg!(windows) && key.eq_ignore_ascii_case(name)))
                                && crate::config::claude_provider_mode_flag(profile, name, value)
                        })
            })
        });
    }
    source_secret_snapshot(&policy_profile, environment, known)
}

pub(crate) fn build_process_environment(profile: &Value) -> Result<Map<String, Value>> {
    build_process_environment_with(profile, |name| std::env::var_os(name))
}

/// A prepared metadata reader uses the same private environment that was
/// verified before cache lookup. Readers that require a verified route can
/// refuse legacy snapshots before spawning.
pub(crate) struct MetadataRoute {
    pub(crate) environment: Value,
    pub(crate) identity: Option<String>,
}

pub(crate) fn metadata_route(profile: &Value) -> Result<MetadataRoute> {
    metadata_route_with(profile, |name| std::env::var_os(name))
}

fn metadata_route_with(
    profile: &Value,
    lookup: impl FnMut(&str) -> Option<OsString>,
) -> Result<MetadataRoute> {
    let (environment, identity) = prepare_process_environment(profile, lookup)?;
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
    if captured.is_none()
        && HarnessCapabilities::of(profile["harness"].as_str().unwrap_or_default())
            .requires_frozen_store_inputs
    {
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
            if crate::config::reserved_eliot_environment_name(name) {
                return Err(Error::new(
                    "invalid",
                    "frozen profile pass_env cannot include reserved ELIOT_* names",
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

/// Scope only the daemon data root to a probe or a plugin-owned skill lookup.
fn data_root_only_mcp_environment(environment: &Value) -> Result<Value> {
    let root = environment
        .as_object()
        .and_then(|values| values.get("ELIOT_HOME"))
        .and_then(Value::as_str)
        .filter(|path| Path::new(path).is_absolute() && Path::new(path).is_dir())
        .ok_or_else(|| Error::new("invalid", "managed MCP data root is unavailable"))?;
    let mut scoped = Map::new();
    insert_environment(&mut scoped, "ELIOT_HOME", root.to_owned());
    Ok(Value::Object(scoped))
}

fn build_target_environment(
    data_root: &Path,
    agent: &Agent,
    agent_token: &str,
) -> Result<(Value, Value)> {
    if HarnessCapabilities::of(&agent.harness).requires_pass_env
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
    let mut mcp_environment = Map::new();
    let home = data_root
        .to_str()
        .ok_or_else(|| Error::new("invalid", "data directory path is not UTF-8"))?;
    insert_environment(&mut mcp_environment, "ELIOT_HOME", home.to_owned());
    let scope_dir = crate::daemon::normalize_identity_data_dir(data_root);
    let scope_dir = scope_dir
        .to_str()
        .ok_or_else(|| Error::new("invalid", "data directory path is not UTF-8"))?;
    insert_environment(
        &mut mcp_environment,
        crate::daemon::AGENT_DATA_DIR_ENV,
        scope_dir.to_owned(),
    );
    insert_environment(&mut mcp_environment, "ELIOT_AGENT_ID", agent.id.clone());
    insert_environment(
        &mut mcp_environment,
        "ELIOT_AGENT_GENERATION",
        agent.generation.to_string(),
    );
    insert_environment(
        &mut mcp_environment,
        "ELIOT_AGENT_TOKEN",
        agent_token.to_owned(),
    );
    let pass_identity = agent
        .profile_snapshot
        .get("pass_agent_identity_to_shell")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    // Codex's app-server supplies its MCP identity from named environment
    // variables and filters those variables from its shell tools. Data-root
    // values stay in the separate MCP environment for every harness.
    if pass_identity || HarnessCapabilities::of(&agent.harness).agent_identity_env {
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
    }
    if pass_identity {
        insert_environment(
            &mut environment,
            crate::daemon::AGENT_DATA_SCOPE_ENV,
            crate::daemon::identity_data_scope(data_root),
        );
    }
    if !pass_identity {
        insert_environment(
            &mut environment,
            crate::daemon::SHELL_IDENTITY_DISABLED_ENV,
            "1".to_owned(),
        );
    }
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
    Ok((Value::Object(environment), Value::Object(mcp_environment)))
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
    Ok(client.call("agent.stop", params, deadlines.rpc_deadline(RPC_DEADLINE)))
}

async fn wait_child(
    child: &mut OwnedChild,
    duration: Duration,
    monitoring: &Monitoring,
    agent: &Agent,
) -> bool {
    wait_child_family_with_exit(child, duration, duration, monitoring, agent)
        .await
        .0
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
    wait_child_family_with_exit(child, leader, family, monitoring, agent)
        .await
        .0
}

async fn wait_child_family_with_exit(
    child: &mut OwnedChild,
    leader: Duration,
    family: Duration,
    monitoring: &Monitoring,
    agent: &Agent,
) -> (bool, Option<eliot_core::ProcessExit>) {
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
) -> (bool, Option<eliot_core::ProcessExit>) {
    if !matches!(
        timeout_at(Instant::now() + leader, child.wait_exit_unreaped()).await,
        Ok(Ok(()))
    ) {
        return (false, None);
    }
    let _ = monitoring.sample_generation(agent).await;
    if !linux_drain_owned_group(child, family).await {
        return (false, None);
    }
    let _ = monitoring.sample_generation(agent).await;
    // Reap only after the group is proven empty except the leader: the
    // numeric PGID cannot be reused between the final observation and this
    // reap because the exited leader is still unreaped.
    match timeout_at(Instant::now() + family, child.wait()).await {
        Ok(Ok(status)) => (
            true,
            Some(eliot_core::ProcessExit::from_code(status.code())),
        ),
        _ => (false, None),
    }
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
) -> (bool, Option<eliot_core::ProcessExit>) {
    let Ok(Ok(status)) = timeout_at(Instant::now() + leader, child.wait()).await else {
        return (false, None);
    };
    let exit = eliot_core::ProcessExit::from_code(status.code());
    let _ = monitoring.sample_generation(agent).await;
    #[cfg(windows)]
    {
        if matches!(child.wait_family_empty(family).await, Ok(true)) {
            // Consume the OS observer's verified-empty certificate before the
            // owning Job is dropped; exited database rows alone are not proof.
            let _ = monitoring.sample_generation(agent).await;
            (true, Some(exit))
        } else {
            (false, Some(exit))
        }
    }
    #[cfg(not(windows))]
    {
        let _ = family;
        (true, Some(exit))
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

async fn terminate_child_with_exit(
    mut child: OwnedChild,
    monitoring: &Monitoring,
    agent: &Agent,
) -> (bool, Option<eliot_core::ProcessExit>) {
    #[cfg(not(target_os = "linux"))]
    let early_exit = child.try_root_exit_if_finished();
    #[cfg(target_os = "linux")]
    let early_exit = None;
    let terminated = child.terminate().is_ok();
    let (exited, waited_exit) = if terminated {
        wait_child_family_with_exit(&mut child, STOP_GRACE, STOP_GRACE, monitoring, agent).await
    } else {
        (false, None)
    };
    drop(child);
    (exited, early_exit.or(waited_exit))
}

/// Private shutdown, input close, then graceful wait with one
/// terminate-and-wait fallback. Returns (graceful, exited); callers settle
/// sources, fence, and reply.
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
        if reader_closed {
            record_root_exit_gap(state, key, reason).await?;
            let state = state
                .upgrade()
                .ok_or_else(|| Error::new("unavailable", "plugin host is unavailable"))?;
            let mut store = timeout_at(deadline(), state.store.lock())
                .await
                .map_err(|_| Error::new("unknown", "native reader drain persistence timed out"))?;
            // The reader fact can arrive before the OS observer proves custody
            // exit. Store releases capacity only once both are committed.
            let events = store.record_owned_reader_drained(&key.id, key.generation, now_ms())?;
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

/// Mark an agent generation lost with its truthful cause: a spontaneous
/// plugin root exit records `root_exited`, never `owner_marked_lost`, which
/// is reserved for an explicit Owner/host assertion of loss.
async fn mark_lost_state(state: &Weak<HostState>, key: &AgentKey, reason: &'static str) {
    mark_lost_state_with_exit(state, key, reason, AgentLossEvidence::default()).await;
}

/// The one owned plugin crash marker in this stderr snapshot, if any. Only
/// the exact owned process pipe is read, and only a line carrying that
/// spawn's prefix (argv nonce and owned PID) counts, so forwarded native
/// output, other processes and disk files cannot override a failure origin.
fn plugin_crash_marker(
    stderr: &str,
    crash_line_prefix: Option<&str>,
) -> Option<eliot_core::api::CrashMarker> {
    let prefix = crash_line_prefix?;
    stderr
        .lines()
        .filter_map(|line| line.strip_prefix(prefix))
        .filter_map(|json| serde_json::from_str::<eliot_core::api::CrashMarker>(json).ok())
        .find(|marker| marker.code == "plugin.panicked" && marker.validate().is_ok())
}

/// Record one verified plugin crash in this daemon's own crash directory so
/// `daemon.status` reports it; plugins never write data-directory files.
fn record_plugin_crash(state: &Weak<HostState>, marker: &eliot_core::api::CrashMarker) {
    if let Some(state) = state.upgrade() {
        crate::crash::write_marker(&crate::crash::crash_dir(&state.data_root), marker);
    }
}

/// Mark an agent generation lost with its process exit evidence: the
/// numeric exit code, the Windows NTSTATUS hex for crash statuses, and the
/// bounded redacted stderr tail recorded on the `agent.lost` event. Exit
/// evidence comes from the observed plugin process wait; the tail is the
/// already-redacted capture snapshot.
async fn mark_lost_state_with_exit(
    state: &Weak<HostState>,
    key: &AgentKey,
    reason: &'static str,
    evidence: AgentLossEvidence,
) {
    let Some(state) = state.upgrade() else {
        return;
    };
    fence_runtime_generation(&state, key);
    {
        let mut store = state.store.lock().await;
        // A force command owns this generation's settlement: retain the
        // observed cause with its exit status and stderr tail together for
        // the guard to replay. First-writer-wins keeps an independently
        // observed root exit ahead of the planned force-stop label.
        if let Some(deferred) = state.planned_stops.lock_recover().get(key) {
            let mut slot = deferred.lock_recover();
            if slot.is_none() {
                *slot = Some(DeferredLoss { reason, evidence });
            }
            return;
        }
        if let Ok(events) =
            store.mark_lost_with_exit(&key.id, key.generation, now_ms(), reason, &evidence)
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
