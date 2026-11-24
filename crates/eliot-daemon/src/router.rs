mod attention;
mod attribution;
mod auth;
mod background_wake;
mod catalog;
mod checkpoint_restore;
mod child_run;
mod cleanup;
mod command;
mod command_verbs;
mod config_ops;
mod context_notes;
mod coordination;
mod diagnose;
mod digests;
pub(crate) mod discovery;
mod goal_scope;
mod goals;
mod handoff;
mod harness_check;
mod hierarchy;
mod inference;
mod insertion_facts;
mod instructions;
pub(crate) mod launch_queue;
mod launch_refusal;
mod launch_result;
pub(crate) use launch_result::RULE_WORKTREE_GUARD_ARG;
mod loss_recovery;
mod organization;
mod overview;
pub(crate) mod owner_inbox;
mod owner_inbox_bulk;
mod owner_rules_doctor;
mod patch_digest;
mod project_assignment;
mod project_broadcast;
pub(crate) mod project_checks;
mod provider_resilience;
mod repeat_failures;
mod resolution;
mod routing;
mod rules;
mod session_growth;
mod sources;
mod status;
mod swarm_usage;
mod switch;
mod task_card;
mod tool_incident;
mod wake;
mod work_truth;

#[path = "plugin_host/mod.rs"]
pub(in crate::router) mod plugin_host;

pub(crate) use cleanup::remove_dir_all;
pub(crate) use plugin_host::{SecretSet, build_process_environment, minimal_plugin_environment};

use crate::harness_capabilities::HarnessCapabilities;
pub(crate) use attention::run_attention;
use auth::{authorize_command, authorize_rpc_method};
pub(crate) use context_notes::run_context_notes;
pub(crate) use coordination::run_mail_reoffer;
pub(crate) use digests::run_completion_digest;
use eliot_core::{is_attachment_verb, is_start_verb, is_user_event_verb};
pub(crate) use goals::{real_goal_check, run_goal_driver};
pub(crate) use project_checks::run_project_checks;
pub(crate) use tool_incident::run_tool_failure_attention;
pub(crate) use wake::run_agent_wakes;
pub(crate) use work_truth::run_work_truth;

use self::plugin_host::{AgentCredentials, PluginHost, PluginHostOptions, UnknownCommands};
use crate::{
    automation::RuleDefinition,
    bus::Bus,
    common::{now_ms, valid_environment_name},
    config::OnRestart,
    monitor::{Monitoring, ObserveThresholds},
    scripts::auth::{RunCredentialGuard, RunCredentialRegistry, SavedRunPrincipal},
    workspaces::WorkspaceManager,
};
use eliot_core::{
    Agent, AgentOrigin, AgentState, Command, CommandState, Error, Event, FrozenScriptContext,
    FrozenScriptRun, NativeStoreBinding, Result, RunDefinitionArgs, ScriptProcessResult,
    SecretPolicy, SessionUsage, SubmitCommand, Turn, TurnOutcome, TurnState, WorkspacePhase,
    WorkspacePreparationInput,
    api::{
        AgentConfigView, AgentDiscoverRequest, AgentEditsRequest, AgentEditsResponse,
        AgentGetRequest, AgentGetResponse, AgentGitRequest, AgentGitResponse, AgentHistoryRequest,
        AgentListRequest, AgentListResponse, AgentListRow, AgentProcessesRequest,
        AgentProcessesResponse, AgentResultRequest, AgentStatsRequest, AgentStatsResponse,
        AgentSummary, CommandChildrenRequest, CommandChildrenResponse, CommandGetRequest,
        CommandSubmitRequest, CommandSubmitResponse, CrashMarker, DaemonShutdownRequest,
        DaemonShutdownResponse, DaemonStatusRequest, DiscoverPage, DoctorHarnessEntry,
        EmitDefinitionArgs, EventsBoundsRequest, EventsFilter, EventsReadRequest,
        EventsReadResponse, EventsSubscribeRequest, EventsSubscribeResponse, FrozenGitPublish,
        GitFetchDefinitionArgs, GitPublishTrigger, GitView, HarnessCheckOutcome,
        HarnessDoctorRequest, HarnessDoctorResponse, HeavyAgentTotals, HeavyEpisodeView,
        HeavySummaryView, MAX_OBSERVATION_PAGE_BYTES, MAX_SWARM_WAIT_MS, MAX_SWARM_WAIT_TARGETS,
        MAX_WAIT_FINAL_TEXT_BYTES, MAX_WAIT_FRAME_BYTES, MaintenancePurgeLegacyAttentionRequest,
        MaintenancePurgeLegacyAttentionResponse, MaintenanceRunRequest, MaintenanceRunResponse,
        NotifyDefinitionArgs, ObservationPage, ObservationQuery, ObservationView,
        ObserveHeavyRequest, ObserveHeavyResponse, ProcessPageCursor, ProjectDiscoverRequest,
        ProjectRegistrationView, ProviderClearHoldRequest, ProviderHoldsRequest, RuleGetRequest,
        RuleStateView, SWITCH_BLANK_SCOPE_ARG, SWITCH_PROJECT_REVISION_ARG, SessionGrowthView,
        ShutdownMode, SourceState, SwarmWaitRequest, SwarmWaitResponse, TimerGetRequest,
        TimerStateView, TurnGetRequest, UsageScopeView, UsageSummaryRequest, WaitEntryResult,
        WorkSummaryView, WorkerTaskHealth, WorkerTaskStatus, fair_excerpt_cap,
        is_supported_harness,
    },
};
use eliot_plugin::{MAX_EVENT_BYTES, sync::LockExt};
use eliot_store::{
    Admission, AgentListFilter, OwnerProcess, ProjectRegistrationInput, RestartResumeCandidate,
    RuleProcessingState, SubmissionContext, TimerDefinitionInput, TimerSlotAdmission,
    TimerSlotCommit,
};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{Mutex as AsyncMutex, RwLock, broadcast, watch},
    time::{Instant, timeout_at},
};

const PAGE_MAX: usize = 1_000;
const EVENT_SCAN_MAX: usize = 1_000;
const EVENT_RESPONSE_MAX_BYTES: usize = 900 * 1024;
const EVENT_RESPONSE_WRAPPER_RESERVE_BYTES: usize = 4 * 1024;
const EVENT_RESPONSE_ENTRIES_MAX_BYTES: usize =
    EVENT_RESPONSE_MAX_BYTES - EVENT_RESPONSE_WRAPPER_RESERVE_BYTES;
const LIVE_WAIT_MAX_MS: u64 = 30_000;
const STATUS_BLOCKER_MAX: usize = 64;
const RUN_WORKER_FORCE_GRACE: Duration = Duration::from_secs(15);
/// Keep startup recovery bounded by actual resume-command settlement rather
/// than admitting the entire retained fleet at once.
const RESTART_RESUME_BATCH_SIZE: usize = 4;

struct BootResumeWorker {
    id: String,
    task: tokio::task::JoinHandle<()>,
    target: Option<(String, i64)>,
}

type StoreHandle = Arc<AsyncMutex<eliot_store::Store>>;

const MODE_OPEN: u8 = 0;
const MODE_QUIESCE: u8 = 1;
const MODE_RESTART: u8 = 2;
const MODE_DRAIN: u8 = 3;
const MODE_FORCE: u8 = 4;
const RESTART_LEASE: Duration = Duration::from_secs(120);
const DAEMON_UPDATE_IN_PROGRESS: &str = "daemon_update_in_progress";

fn update_launch_refusal() -> Error {
    Error::new(
        DAEMON_UPDATE_IN_PROGRESS,
        "daemon update in progress, retry after the update finishes",
    )
}

#[derive(Clone)]
pub(crate) struct Router {
    inner: Arc<RouterInner>,
}

struct RouterInner {
    acl_notices: Arc<crate::acl_notice::AclNotices>,
    data_root: PathBuf,
    store: StoreHandle,
    autonomy_reader: Arc<tokio::sync::Semaphore>,
    agent_list_readers: Arc<tokio::sync::Semaphore>,
    confirmed_stuck: Mutex<Arc<eliot_store::StuckStates>>,
    integration_cleanup: AsyncMutex<()>,
    bus: Bus,
    mail: crate::config::MailConfig,
    attention: crate::config::AttentionConfig,
    background_wake_max_wait_ms: i64,
    /// Automatic handoff covers only sessions lost since this daemon started.
    started_at_ms: i64,
    owner_token: String,
    secrets: SecretSet,
    agent_credentials: AgentCredentials,
    run_credentials: RunCredentialRegistry,
    unknown_commands: UnknownCommands,
    unknown_causes: Mutex<HashMap<String, String>>,
    repeat_failures: Mutex<repeat_failures::RepeatFailures>,
    rule_definitions: Mutex<HashMap<(String, String), (i64, RuleDefinition)>>,
    assignment_observations: Mutex<crate::automation::assignment::Observations>,
    rule_source_files: Mutex<HashMap<String, HashMap<String, String>>>,
    catalog_cache: Mutex<catalog::CatalogCache>,
    profile_cache: Mutex<crate::config::ProfileCache>,
    catalog_aggregate_lock: AsyncMutex<()>,
    var_writes: Mutex<coordination::VarWriteQueue>,
    var_worker: AsyncMutex<Option<tokio::task::JoinHandle<()>>>,
    plugin_host: PluginHost,
    monitoring: Monitoring,
    observe: Mutex<ObserveThresholds>,
    workspaces: WorkspaceManager,
    workspace_recovery_error: Mutex<Option<String>>,
    plugin_crashes: Mutex<PluginCrashStatus>,
    previous_crash: Mutex<Option<CrashMarker>>,
    worker_tasks: Mutex<Vec<WorkerTaskStatus>>,
    automatic_cleanup: Mutex<cleanup::AutomaticCleanupState>,
    mode: AtomicU8,
    restart_lease: Mutex<Option<String>>,
    restart_writes: RwLock<()>,
    shutdown_ready: AtomicBool,
    progress: watch::Sender<u64>,
    run_cancellation: watch::Sender<bool>,
    workspace_check_locks: Mutex<HashMap<PathBuf, std::sync::Weak<AsyncMutex<()>>>>,
    run_workers: Mutex<HashMap<String, RunWorkerHandle>>,
    inference_ops: AsyncMutex<()>,
    inference_runner: AsyncMutex<()>,
    inference_workers: Mutex<HashMap<String, tokio::task::JoinHandle<()>>>,
    /// Memory inference calls whose native process family exit is unproven
    /// (`Unknown { quiescent: false }`). Cleanup and native dispatch stay
    /// fenced for these ids; the fence lifts only on daemon restart.
    inference_unquiescent: Mutex<HashSet<String>>,
    /// Progressing keyset cursor for the completion-digest sweep: the last
    /// examined `(created_at, id)`, or `None` at the oldest row. The driver
    /// advances it past every examined page and wraps when it passes the
    /// sweep high-water below, so pending or mailbox-blocked registrations
    /// at the front never starve later eligible ones even while new
    /// registrations keep arriving. In-memory only: a restart resumes at the
    /// oldest row, and restart deduplication still holds through the digest
    /// mail id.
    digest_poll_cursor: Mutex<Option<(i64, String)>>,
    /// Sweep high-water: the newest pending `(created_at, id)` observed when
    /// the current sweep started (`None` before the first sweep). The sweep
    /// wraps once the cursor passes this bound, so a continuously growing
    /// tail cannot trap older registrations behind the cursor forever.
    digest_sweep_end: Mutex<Option<(i64, String)>>,
    /// Bumped by `goal.set`/`goal.resume` when the Goal ends up active and
    /// idle-keyed. The goal driver sweeps the retained idle subjects on
    /// every change; reads never bump it. Startup performs no sweep: the
    /// restart marks every owned session lost, so nothing is eligible.
    goal_reconcile: watch::Sender<u64>,
    /// Bumped when queued launch arbitration inputs change (enqueued intent,
    /// budget, settings, assignment, hold clear, or admitted slot) so `stop`
    /// acceptance never wakes arbitration ahead of committed `capacity.released`.
    launch_arbitration: watch::Sender<u64>,
}

#[derive(Default)]
struct PluginCrashStatus {
    markers: Vec<CrashMarker>,
    truncated: bool,
}

struct RunWorkerHandle {
    task: tokio::task::JoinHandle<()>,
    completed: watch::Receiver<bool>,
}

#[derive(Clone)]
enum Principal {
    Owner,
    Swarm(&'static str),
    MemoryInference {
        client_id: String,
        token_version: i64,
    },
    Agent {
        id: String,
        generation: i64,
    },
    ProjectRule {
        registration: Box<ProjectRegistrationView>,
    },
    Run {
        run_command_id: String,
    },
}

pub(crate) struct RpcLogContext {
    pub(crate) principal: String,
    pub(crate) method: String,
}

struct EventFilter {
    agents: HashSet<String>,
    kinds: HashSet<String>,
    sources: HashSet<String>,
    /// Encoded variable scopes whose Agent-less `var.changed` facts ride
    /// along with an Agent filter: the scopes that Agent can read.
    var_scopes: HashSet<String>,
}

pub(crate) struct PreparedSubmission {
    pub(crate) request: SubmitCommand,
    pub(crate) workspace: Option<WorkspacePreparationInput>,
}

use eliot_core::api::discovery::{ProjectGetRequest, ProjectListRequest};

/// One locked snapshot of a frozen wait set: per-entry results plus the
/// wake filter (watched Agents and commands, and the caller's mailbox for a
/// `mailbox` wait) and that mailbox's delivery watermark. All Store reads
/// happen under a single lock so entries never mix generations across a
/// concurrent commit.
struct WaitSnapshot {
    results: Vec<WaitEntryResult>,
    watch_agents: HashSet<String>,
    watch_commands: HashSet<String>,
    mailbox: Option<String>,
    mail_current: Option<i64>,
    mail_arrived: bool,
}

impl WaitSnapshot {
    /// Wake only on facts that can move one of our entries (command
    /// settlement, or a watched Agent's command/turn/session/request/agent
    /// frame) or on a delivery to our own mailbox; streaming progress and
    /// every other Agent's traffic keep waiting without a re-snapshot.
    fn watches(&self, event: &Event) -> bool {
        if event.kind == "mail.delivered" {
            return event.agent.is_some() && event.agent == self.mailbox;
        }
        if event
            .data
            .get("command_id")
            .and_then(Value::as_str)
            .is_some_and(|id| self.watch_commands.contains(id))
        {
            return true;
        }
        ["command.", "turn.", "session.", "request.", "agent."]
            .iter()
            .any(|prefix| event.kind.starts_with(prefix))
            && event
                .agent
                .as_deref()
                .is_some_and(|agent| self.watch_agents.contains(agent))
    }
}

/// Subscribe-then-snapshot long-poll shared by `swarm.wait`, `mail.inbox`
/// and `var.get` (see `Router::long_poll`).
pub(super) struct LongPoll {
    receiver: broadcast::Receiver<Event>,
    deadline: Instant,
}

impl LongPoll {
    /// Wait for the next bus event `wakes` accepts (a lag also wakes):
    /// true means snapshot again, false means the deadline passed and the
    /// last snapshot is the answer.
    pub(super) async fn wake(&mut self, wakes: impl Fn(&Event) -> bool) -> Result<bool> {
        loop {
            if Instant::now() >= self.deadline {
                return Ok(false);
            }
            match timeout_at(self.deadline, self.receiver.recv()).await {
                Ok(Ok(event)) if !wakes(&event) => continue,
                Ok(Ok(_)) | Ok(Err(broadcast::error::RecvError::Lagged(_))) => return Ok(true),
                Ok(Err(broadcast::error::RecvError::Closed)) => {
                    return Err(Error::new("unavailable", "event bus is closed"));
                }
                Err(_) => return Ok(false),
            }
        }
    }
}

/// A resolved bound turn carried through unknown-masking to encoding.
struct BoundTurn {
    agent: String,
    generation: i64,
    turn_id: String,
    turn: Turn,
}

/// One wait entry pending its turn data: either a leaf status or an
/// index into the shared bound-turn list.
struct PendingEntry {
    kind: &'static str,
    id: String,
    leaf: String,
    bound: Option<usize>,
    command_unknown: bool,
    excerpt_allowed: bool,
    branch: Result<Option<String>>,
}

/// Digest options for one wait snapshot; the default is a plain wait.
#[derive(Clone, Copy, Default)]
struct WaitScope<'a> {
    /// Registration-time mask: masked entries read `not_visible`.
    hidden: Option<&'a [bool]>,
    /// Mailbox whose live scope gates each turn identity and excerpt.
    excerpt_recipient: Option<&'a str>,
    /// Fair water-fill excerpts instead of the first-come frame budget.
    fair_budget: bool,
}

/// In-memory unknown marks captured before a snapshot's Store guard.
struct UnknownMarks {
    causes: HashMap<String, String>,
    memory: Vec<String>,
}

impl Router {
    /// Build the Router with the Owner settings captured at daemon startup.
    pub(crate) fn new(
        data_root: std::path::PathBuf,
        owner_token: String,
        store: StoreHandle,
        bus: Bus,
        mail: crate::config::MailConfig,
        attention: crate::config::AttentionConfig,
        background_wake_max_wait_ms: i64,
    ) -> Self {
        let secrets: SecretSet = Arc::new(Mutex::new(HashSet::from([owner_token.clone()])));
        let agent_credentials: AgentCredentials = Arc::new(Mutex::new(Default::default()));
        let run_credentials = RunCredentialRegistry::default();
        let unknown_commands: UnknownCommands = Arc::new(Mutex::new(Default::default()));
        let (progress, _) = watch::channel(0_u64);
        let (run_cancellation, _) = watch::channel(false);
        let (goal_reconcile, _) = watch::channel(0_u64);
        let (launch_arbitration, _) = watch::channel(0_u64);
        let monitoring = Monitoring::new(Arc::clone(&store), bus.clone(), Arc::clone(&secrets));
        let acl_notices = Arc::new(crate::acl_notice::AclNotices::default());
        let workspaces = WorkspaceManager::new(
            data_root.clone(),
            Arc::clone(&store),
            bus.clone(),
            Arc::clone(&secrets),
            progress.clone(),
            monitoring.clone(),
            Arc::clone(&acl_notices),
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
            Arc::clone(&acl_notices),
        );
        Self {
            inner: Arc::new(RouterInner {
                acl_notices,
                data_root,
                store,
                autonomy_reader: Arc::new(tokio::sync::Semaphore::new(1)),
                agent_list_readers: Arc::new(tokio::sync::Semaphore::new(4)),
                confirmed_stuck: Mutex::new(Arc::default()),
                integration_cleanup: AsyncMutex::new(()),
                bus,
                mail,
                attention,
                background_wake_max_wait_ms,
                started_at_ms: now_ms(),
                owner_token,
                secrets,
                agent_credentials,
                run_credentials,
                unknown_commands,
                unknown_causes: Mutex::new(HashMap::new()),
                repeat_failures: Mutex::default(),
                rule_definitions: Mutex::new(HashMap::new()),
                assignment_observations: Mutex::default(),
                rule_source_files: Mutex::new(HashMap::new()),
                catalog_cache: Mutex::new(catalog::CatalogCache::default()),
                profile_cache: Mutex::new(crate::config::ProfileCache::default()),
                catalog_aggregate_lock: AsyncMutex::new(()),
                var_writes: Mutex::new(coordination::VarWriteQueue::default()),
                var_worker: AsyncMutex::new(None),
                plugin_host,
                monitoring,
                observe: Mutex::new(ObserveThresholds::default()),
                workspaces,
                workspace_recovery_error: Mutex::new(None),
                plugin_crashes: Mutex::new(PluginCrashStatus::default()),
                previous_crash: Mutex::new(None),
                worker_tasks: Mutex::new(
                    crate::daemon::workers::WORKER_NAMES
                        .iter()
                        .map(|name| WorkerTaskStatus {
                            name: (*name).to_owned(),
                            health: WorkerTaskHealth::Starting,
                            failure_code: None,
                            last_failure: None,
                            restart_count: 0,
                        })
                        .collect(),
                ),
                automatic_cleanup: Mutex::new(cleanup::AutomaticCleanupState::default()),
                mode: AtomicU8::new(MODE_OPEN),
                restart_lease: Mutex::new(None),
                restart_writes: RwLock::new(()),
                shutdown_ready: AtomicBool::new(false),
                progress,
                run_cancellation,
                run_workers: Mutex::new(HashMap::new()),
                workspace_check_locks: Mutex::new(HashMap::new()),
                inference_ops: AsyncMutex::new(()),
                inference_runner: AsyncMutex::new(()),
                inference_workers: Mutex::new(HashMap::new()),
                inference_unquiescent: Mutex::new(HashSet::new()),
                digest_poll_cursor: Mutex::new(None),
                digest_sweep_end: Mutex::new(None),
                goal_reconcile,
                launch_arbitration,
            }),
        }
    }

    async fn handle(
        &self,
        method: &str,
        mut wire_params: Value,
        peer: Option<crate::platform::custody::LocalPeer>,
        log_principal: &mut String,
    ) -> Result<Value> {
        let remote = wire_params
            .get("_auth")
            .and_then(|a| a.get("remote_call"))
            .cloned();
        let mcp_call_id = wire_params
            .get_mut("_auth")
            .and_then(Value::as_object_mut)
            .and_then(|auth| auth.remove("swarm_mcp_call_id"));
        let (principal, params, owner_peer) = self.authenticate(wire_params, peer).await?;
        *log_principal = diagnostic_principal(&principal);
        let owner_process = (matches!(principal, Principal::Owner)
            && discovery::is_mutation(method))
        .then(|| self.owner_process(owner_peer.as_ref()));
        let remote: Option<eliot_core::RemoteCallContext> = remote
            .map(serde_json::from_value)
            .transpose()
            .map_err(|_| Error::new("not_allowed", "invalid remote context"))?;
        let mcp_trace = if let Some(call_id) = mcp_call_id.as_ref() {
            let call_id = call_id
                .as_str()
                .filter(|id| ulid::Ulid::from_string(id).is_ok())
                .ok_or_else(|| Error::new("invalid", "Swarm MCP call id is invalid"))?;
            self.begin_swarm_mcp_tool_call(&principal, remote.as_ref(), call_id, method)
                .await
        } else {
            None
        };
        let context = SubmissionContext {
            remote: remote.as_ref(),
            owner_process: owner_process.as_ref(),
            owner_automation: false,
        };
        let result = self
            .handle_authorized(method, params, principal.clone(), context)
            .await;
        if method == "command.submit" {
            self.note_command_submission(&principal, remote.as_ref(), "command.submit", &result);
        } else if let Err(error) = &result {
            self.note_rpc_failure(
                log_principal,
                discovery::canonical_method(method).unwrap_or("unsupported"),
                remote.as_ref(),
                error,
            );
        }
        if let Some(trace) = mcp_trace {
            self.finish_swarm_mcp_tool_call(&trace, result.is_err())
                .await;
        }
        let result = result?;
        Ok(match remote {
            Some(remote) => json!({"result": result, "remote_call": remote}),
            None => result,
        })
    }

    fn owner_process(&self, peer: Option<&crate::platform::custody::LocalPeer>) -> OwnerProcess {
        #[cfg(any(windows, target_os = "linux"))]
        let (pid, image_path) = (
            peer.map(|peer| peer.process_id()),
            peer.and_then(|peer| peer.image_path()).map(|path| {
                const MAX_IMAGE_PATH_BYTES: usize = 4096;
                const TRUNCATED: &str = " [truncated]";
                let mut path = self.secret_policy_snapshot().redact_text(&path);
                if path.len() > MAX_IMAGE_PATH_BYTES {
                    path.truncate(path.floor_char_boundary(MAX_IMAGE_PATH_BYTES - TRUNCATED.len()));
                    path.push_str(TRUNCATED);
                }
                path
            }),
        );
        #[cfg(not(any(windows, target_os = "linux")))]
        let (pid, image_path) = {
            let _ = peer;
            (None, None)
        };
        OwnerProcess { pid, image_path }
    }

    async fn handle_authorized(
        &self,
        method: &str,
        params: Value,
        principal: Principal,
        context: SubmissionContext<'_>,
    ) -> Result<Value> {
        authorize_rpc_method(&principal, method)?;
        let _write = if method != "daemon.shutdown" && discovery::is_mutation(method) {
            let guard = self.inner.restart_writes.read().await;
            if self.is_restart_requested() {
                if matches!(method, "launch.enqueue" | "project.assignment.launch")
                    || (method == "child.run" && params.get("text").is_some())
                    || (method == "command.submit" && params["verb"] == "launch")
                {
                    return Err(update_launch_refusal());
                }
                return Err(Error::new(
                    "unavailable",
                    "the Store is sealed for replacement",
                ));
            }
            Some(guard)
        } else {
            None
        };
        if let Some(process) = context.owner_process {
            let mutation_id = params
                .get("id")
                .and_then(Value::as_str)
                .filter(|id| ulid::Ulid::from_string(id).is_ok());
            let mut store = self.inner.store.lock().await;
            match store.record_owner_submission(method, mutation_id, process, now_ms()) {
                Ok(event) => self.publish_locked(&[event]),
                Err(error) => {
                    let pid = process.pid.map(|pid| pid.to_string()).unwrap_or_default();
                    crate::logging::record(
                        &self.secret_policy_snapshot(),
                        "owner.submission_record_failed",
                        &[
                            ("method", method),
                            ("command_id", mutation_id.unwrap_or_default()),
                            ("pid", &pid),
                            ("path", process.image_path.as_deref().unwrap_or_default()),
                        ],
                        Some(&error),
                    );
                }
            }
        }
        // Debug mode times every RPC at this single choke point. Standard
        // mode pays one Instant read and a branch; no parameters, prompts or
        // results ever enter the record.
        let started = std::time::Instant::now();
        let result = match method {
            "client.identity" => self.inference_identity(&params, &principal).await,
            "inference.client.install" => {
                self.inference_credential(method, &params, &principal).await
            }
            "inference.client.rotate" => {
                self.inference_credential(method, &params, &principal).await
            }
            "inference.client.promote" => {
                self.inference_credential(method, &params, &principal).await
            }
            "inference.client.retire" => {
                self.inference_credential(method, &params, &principal).await
            }
            "inference.text" => self.inference_text(&params, &principal).await,
            "inference.get" => self.inference_get(&params, &principal).await,
            "inference.write.get" => self.inference_write_get(&params, &principal).await,
            "inference.high_water" => self.inference_high_water(&params, &principal).await,
            "inference.list" => self.inference_list(&params, &principal).await,
            "inference.retention.advance" => self.inference_retention(&params, &principal).await,
            "inference.result.read" => self.inference_result_read(&params, &principal).await,
            "inference.cancel" => self.inference_cancel(&params, &principal).await,
            "inference.release" => self.inference_release(&params, &principal).await,
            "api.methods" => self.methods(&params, &principal).await,
            "api.help" => self.method_help(&params, &principal).await,
            "api.identity" => self.caller_identity(&params, &principal).await,
            "daemon.status" => {
                require_owner(&principal)?;
                let request: DaemonStatusRequest =
                    eliot_core::api::discovery::decode_params(params.clone())?;
                request.validate()?;
                self.status().await
            }
            "autonomy.get" => {
                require_owner(&principal)?;
                self.autonomy(eliot_core::api::discovery::decode_params(params)?)
                    .await
            }
            "daemon.shutdown" => {
                require_owner(&principal)?;
                self.shutdown(&params, &principal).await
            }
            #[cfg(debug_assertions)]
            crate::crash::DAEMON_PANIC_TEST_METHOD => {
                use eliot_core::api::{DaemonPanicTestRequest, DaemonPanicTestResponse};

                require_owner(&principal)?;
                let request: DaemonPanicTestRequest =
                    eliot_core::api::discovery::decode_params(params.clone())?;
                request.validate()?;
                // IPC triggers the panic after sending this acknowledgement.
                Ok(json!(DaemonPanicTestResponse {
                    state: "panicking".to_owned()
                }))
            }
            "maintenance.run" => self.maintenance_run(&params, &principal).await,
            "maintenance.purge_legacy_attention_advisories" => {
                self.maintenance_purge_legacy_attention_advisories(&params, &principal)
                    .await
            }
            "artifact.register" => self.artifact_register(&params, &principal).await,
            "cleanup.plan" => self.cleanup_plan(&params, &principal).await,
            "cleanup.apply" => self.cleanup_apply(&params, &principal).await,
            "command.submit" => {
                let result = match self.submit_bound(&params, &principal, context).await {
                    // Admission preflight call site: a typed refusal is
                    // marked only when no command was admitted for this ID.
                    Err(error) => Err(self.mark_admission_preflight(&params, error).await),
                    ok => ok,
                };
                // An explicit admission preflight refusal (capacity, a missing
                // profile executable) happens before a command exists. Retain
                // diagnostic metadata under the caller's exact ID in the
                // existing bounded log; this grants no replay authority.
                if let Err(error) = &result
                    && eliot_core::AdmissionRefusal::of(error).is_some()
                    && let Some(id) = params.get("id").and_then(Value::as_str)
                    && ulid::Ulid::from_string(id).is_ok()
                {
                    let log_principal = diagnostic_principal(&principal);
                    crate::logging::record(
                        &self.secret_policy_snapshot(),
                        crate::logging::ADMISSION_REFUSAL_CODE,
                        &[
                            ("principal", log_principal.as_str()),
                            ("method", "command.submit"),
                            ("command", id),
                        ],
                        Some(error),
                    );
                }
                result
            }
            "launch.enqueue" => self.enqueue_launch(&params, &principal, context).await,
            "launch.intent.get" => self.read_launch_intent(&params, &principal).await,
            "launch.queue" => self.read_launch_queue(&params, &principal).await,
            "project.budget.set" => self.set_manager_budget(&params, &principal).await,
            "command.get" => self.command_get(&params, &principal).await,
            "inject.wait" => self.inject_wait(&params, &principal).await,
            "command.children" => self.command_children(&params, &principal).await,
            "command.resolve" => self.command_resolve(&params, &principal).await,
            "command.settle_stopped" => self.command_settle_stopped(&params, &principal).await,
            "agent.get" => self.agent_get(&params, &principal).await,
            "agent.discover" => self.agent_discover(&params, &principal).await,
            "harness.list" => self.harness_list(&params, &principal).await,
            "harness.models" => self.harness_models(&params, &principal).await,
            "harness.quota" => self.harness_quota(&params, &principal).await,
            "harness.doctor" => self.harness_doctor(&params, &principal).await,
            "models.catalog" => self.models_catalog(&params, &principal).await,
            "agent.history" => self.agent_history(&params, &principal).await,
            "agent.archive" => {
                require_owner(&principal)?;
                let request: eliot_core::api::AgentArchiveRequest =
                    eliot_core::api::discovery::decode_params(params.clone())?;
                request.validate()?;
                self.inner.store.lock().await.change_agent_tree(
                    &request.id,
                    &request.agent,
                    Some(request.archived),
                    now_ms(),
                )
            }
            "agent.delete" => self.agent_delete(&params, &principal).await,
            "agent.list" => self.agent_list(&params, &principal).await,
            "agent.result" => self.agent_result(&params, &principal).await,
            "agent.processes" => self.agent_processes(&params, &principal).await,
            "agent.stats" => self.agent_stats(&params, &principal).await,
            "usage.summary" => self.usage_summary(&params, &principal).await,
            "observe.heavy" => self.observe_heavy(&params, &principal).await,
            "agent.git" => self.agent_git(&params, &principal).await,
            "checkpoint.restore" => self.checkpoint_restore(&params, &principal).await,
            "agent.patch_digest" => self.agent_patch_digest(&params, &principal).await,
            "agent.edits" => self.agent_edits(&params, &principal).await,
            "turn.get" => self.turn_get(&params, &principal).await,
            "turn.resolve" => self.turn_resolve(&params, &principal).await,
            "diagnose" => self.diagnose(&params, &principal).await,
            "events.bounds" => self.events_bounds(&params, &principal).await,
            "events.read" => self.events_read(&params, &principal).await,
            "events.subscribe" => self.events_subscribe(&params, &principal).await,
            "child.run" => self.child_run(&params, &principal).await,
            "agent.wake" => self.agent_wake(&params, &principal).await,
            "swarm.wait" => self.swarm_wait(&params, &principal).await,
            "swarm.digest" => self.swarm_digest(&params, &principal).await,
            "swarm.reparent" => self.swarm_reparent(&params, &principal).await,
            "swarm.handoff" => {
                self.swarm_handoff(&params, &principal, context.owner_process)
                    .await
            }
            "swarm.switch" => {
                self.swarm_switch(&params, &principal, context.owner_process, None)
                    .await
            }
            "chat.organization" => self.chat_organization(&params, &principal).await,
            "chat.attach" => self.chat_attach(&params, &principal).await,
            "chat.detach" => self.chat_detach(&params, &principal).await,
            "chat.tag" => self.chat_tag(&params, &principal).await,
            "command.cancel" => self.command_cancel(&params, &principal).await,
            "project.transfer" => {
                self.project_transfer(&params, &principal, context.owner_process)
                    .await
            }
            "project.assignment.get" => self.project_assignment_get(&params, &principal).await,
            "project.assignment.claim" => self.project_assignment_claim(&params, &principal).await,
            "project.assignment.launch" => {
                self.project_assignment_launch(&params, &principal, context.owner_process)
                    .await
            }
            "project.assignment.release" => {
                self.project_assignment_release(&params, &principal).await
            }
            "project.broadcast" => {
                self.project_broadcast(&params, &principal, context.owner_process)
                    .await
            }
            "project.get" => self.project_get(&params, &principal).await,
            "project.overview" => self.project_overview(&params, &principal).await,
            "project.list" => self.project_list(&params, &principal).await,
            "workspace.list" => self.workspace_list(&params, &principal).await,
            "project.discover" => self.project_discover(&params, &principal).await,
            "profile.list" => self.profile_list(&params, &principal).await,
            "settings.read" => self.settings_read(&params, &principal).await,
            "settings.save" => self.settings_save(&params, &principal).await,
            "routing.preview" => self.routing_preview(&params, &principal).await,
            "rule.list" => self.rule_list(&params, &principal).await,
            "rule.read" => self.rule_read(&params, &principal).await,
            "rule.save" => self.rule_save(&params, &principal).await,
            "rule.delete" => self.rule_delete(&params, &principal).await,
            "script.list" => self.script_list(&params, &principal).await,
            "script.read" => self.script_read(&params, &principal).await,
            "script.save" => self.script_save(&params, &principal).await,
            "rule.get" => self.rule_get(&params, &principal).await,
            "rule.catch_up" => self.rule_catch_up(&params, &principal).await,
            "timer.get" => self.timer_get(&params, &principal).await,
            "timer.run" => self.timer_run(&params, &principal).await,
            "provider.holds" => self.provider_holds(&params, &principal).await,
            "provider.clear_hold" => self.provider_clear_hold(&params, &principal).await,
            "goal.set" => self.goal_set(&params, &principal).await,
            "goal.get" => self.goal_get(&params, &principal).await,
            "goal.pause" => self.goal_pause(&params, &principal).await,
            "goal.clear" => self.goal_clear(&params, &principal).await,
            "goal.resume" => self.goal_resume(&params, &principal).await,
            "goal.round" => self.goal_round(&params, &principal).await,
            "owner.inbox" => self.owner_inbox(&params, &principal).await,
            "owner.decide" => self.owner_decide(&params, &principal, context).await,
            "owner.decide.bulk" => self.owner_decide_bulk(&params, &principal, context).await,
            "mail.send" => self.mail_send(&params, &principal).await,
            "mail.inbox" => self.mail_inbox(&params, &principal).await,
            "mail.read" => self.mail_read(&params, &principal).await,
            "mail.ack" => self.mail_ack(&params, &principal).await,
            "var.set" => self.var_set(&params, &principal).await,
            "var.get" => self.var_get(&params, &principal).await,
            "who" => self.who(&params, &principal).await,
            "claim.set" => self.claim_set(&params, &principal).await,
            "claim.list" => self.claim_list(&params, &principal).await,
            "claim.release" => self.claim_release(&params, &principal).await,
            _ => Err(discovery::unavailable_method(method, &principal)),
        };
        let result = result.map_err(|mut error| {
            if error.code == "invalid" && method == "agent.get" {
                let id = match &principal {
                    Principal::Agent { id, .. } => id.as_str(),
                    _ => "a1",
                };
                let example = json!({"method":"agent.get","params":{"id":id}});
                error.message = format!("{}. Try {example}.", error.message).into();
            } else if error.code == "not_allowed"
                && matches!(principal, Principal::Agent { .. })
                && discovery::owner_only_read(method)
            {
                error.message = "Owner-only read. For Agent-scoped information, use api.identity, who, or agent.get on yourself.".to_owned().into();
            }
            error
        });
        // Every RPC is timed here for debug mode: method, elapsed
        // milliseconds and ok/error. The method vocabulary is ours; params
        // and results never enter the record.
        if crate::logging::is_debug() {
            let elapsed_ms = started.elapsed().as_millis().min(u64::MAX as u128) as u64;
            let mut fields: [(&str, u64); 2] = [("elapsed_ms", elapsed_ms), ("ok", 0)];
            if result.is_ok() {
                fields[1].1 = 1;
            }
            let policy = SecretPolicy::new(self.daemon_secrets());
            let method = discovery::canonical_method(method).unwrap_or("unsupported");
            let log_principal = diagnostic_principal(&principal);
            let ids = [("principal", log_principal.as_str()), ("method", method)];
            match &result {
                Ok(_) => crate::logging::record_debug(&policy, "rpc.handled", &ids, &fields),
                Err(error) => {
                    crate::logging::record_debug_error(&policy, "rpc.failed", &ids, &fields, error)
                }
            }
        }
        result
    }

    pub(crate) async fn handle_rpc_with_log_context(
        &self,
        request: &eliot_plugin::RpcRequest,
        peer: Option<crate::platform::custody::LocalPeer>,
    ) -> (Result<Value>, RpcLogContext) {
        let method = discovery::canonical_method(&request.method)
            .unwrap_or("unsupported")
            .to_owned();
        let mut principal = "unauthenticated".to_owned();
        let result = if let Some(remote) = request
            .params
            .get("_auth")
            .and_then(|a| a.get("remote_call"))
            && remote.get("call_id") != Some(&request.id)
        {
            Err(Error::new(
                "not_allowed",
                "remote call does not match its RPC request",
            ))
        } else {
            self.handle(
                &request.method,
                request.params.clone(),
                peer,
                &mut principal,
            )
            .await
        };
        (result, RpcLogContext { principal, method })
    }
    pub(crate) fn progress_receiver(&self) -> watch::Receiver<u64> {
        self.inner.progress.subscribe()
    }

    pub(crate) fn launch_arbitration_receiver(&self) -> watch::Receiver<u64> {
        self.inner.launch_arbitration.subscribe()
    }

    pub(crate) fn signal_launch_arbitration(&self) {
        self.inner.launch_arbitration.send_modify(|value| {
            *value = value.wrapping_add(1);
        });
    }

    pub(crate) fn attention_config(&self) -> crate::config::AttentionConfig {
        self.inner.attention
    }

    pub(crate) fn monitoring(&self) -> Monitoring {
        self.inner.monitoring.clone()
    }

    /// Install the [daemon] observe thresholds once at startup, on the
    /// Router readbacks and the monitor sweeps together.
    pub(crate) fn set_observe_thresholds(&self, thresholds: ObserveThresholds) {
        *self.inner.observe.lock_recover() = thresholds;
        self.inner.monitoring.set_observe_thresholds(thresholds);
    }

    pub(crate) fn response_finished(&self, shutdown_requested: bool) {
        if shutdown_requested && self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN {
            self.inner.shutdown_ready.store(true, Ordering::SeqCst);
        }
        self.signal_progress();
    }

    pub(crate) async fn request_force(&self) {
        self.inner.plugin_host.close_discovery_admission();
        let _store = self.inner.store.lock().await;
        let _run_workers = self.inner.run_workers.lock_recover();
        self.inner.mode.store(MODE_FORCE, Ordering::SeqCst);
        self.inner.workspaces.close_startup_admission();
        self.inner.plugin_host.close_startup_admission();
        self.inner.run_cancellation.send_replace(true);
        self.inner.shutdown_ready.store(true, Ordering::SeqCst);
        drop(_store);
        self.signal_progress();
    }

    pub(crate) fn is_force_requested(&self) -> bool {
        self.inner.mode.load(Ordering::SeqCst) == MODE_FORCE
    }

    pub(crate) fn is_restart_requested(&self) -> bool {
        self.inner.mode.load(Ordering::SeqCst) == MODE_RESTART
    }

    pub(crate) async fn stop_for_restart(&self) {
        self.inner.workspaces.stop_all(true).await;
        self.inner.plugin_host.force_all(true).await;
    }

    /// Arm only after the restart receipt commits. An independent thread
    /// can enforce the exit bound even if worker or reply shutdown stalls.
    fn prepare_restart_exit_watchdog(&self) -> Result<std::sync::mpsc::Sender<()>> {
        let (commit, armed) = std::sync::mpsc::channel();
        #[cfg(target_os = "linux")]
        let groups = self.inner.plugin_host.restart_groups();
        std::thread::Builder::new()
            .name("swarm-restart-exit".to_owned())
            .spawn(move || {
                if armed.recv().is_err() {
                    return;
                }
                // Leave time for the update client's independent 120s exit check.
                std::thread::sleep(Duration::from_secs(110));
                #[cfg(target_os = "linux")]
                for group in groups {
                    let _ = crate::platform::unix::terminate_process_group(&group);
                }
                // Windows closes the daemon's kill-on-close Job handles on exit.
                // The sealed Store is reopened only by the replacement process.
                std::process::exit(1);
            })
            .map_err(|error| {
                Error::new(
                    "restart_watchdog",
                    format!("start restart exit watchdog: {error}"),
                )
            })?;
        Ok(commit)
    }

    pub(crate) async fn should_exit(&self) -> Result<bool> {
        match self.inner.mode.load(Ordering::SeqCst) {
            MODE_FORCE | MODE_RESTART => Ok(self.inner.shutdown_ready.load(Ordering::SeqCst)),
            MODE_DRAIN if self.inner.shutdown_ready.load(Ordering::SeqCst) => {
                // The blocker list already names live Run workers, but the
                // exit gate repeats the cheap registry check explicitly so a
                // worker the bounded receipt omits can never end the drain.
                Ok(self.status_blockers().await?.is_empty()
                    && !self.inference_workers_active()
                    && !self.run_workers_active())
            }
            MODE_DRAIN => Ok(false),
            _ => Ok(false),
        }
    }

    pub(crate) async fn force_owned_processes(&self) {
        self.stop_inference_workers().await;
        let workers = {
            let _store = self.inner.store.lock().await;
            let mut workers = self.inner.run_workers.lock_recover();
            self.inner.mode.fetch_max(MODE_FORCE, Ordering::SeqCst);
            self.inner.workspaces.close_startup_admission();
            self.inner.plugin_host.close_startup_admission();
            self.inner.run_cancellation.send_replace(true);
            workers.drain().collect::<Vec<_>>()
        };
        let deadline = Instant::now() + RUN_WORKER_FORCE_GRACE;
        for (run_id, mut worker) in workers {
            match timeout_at(deadline, &mut worker.task).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => {
                    self.settle_run_worker_unknown(&run_id, "script_worker_failed")
                        .await;
                }
                Err(_) => {
                    self.settle_run_worker_unknown(&run_id, "script_force_shutdown_unsettled")
                        .await;
                    let mut active = self.inner.run_workers.lock_recover();
                    if worker.task.is_finished() {
                        drop(active);
                        let _ = worker.task.await;
                    } else {
                        active.insert(run_id, worker);
                    }
                }
            }
        }
        self.inner.workspaces.stop_all(true).await;
        self.inner.plugin_host.force_all(false).await;
    }
    pub(crate) async fn recover_workspaces(&self) -> Result<()> {
        for id in self.inner.workspaces.recover().await? {
            let linked = {
                let store = self.inner.store.lock().await;
                let Some(command) = store.get_command(&id)? else {
                    continue;
                };
                if command.state != CommandState::Accepted
                    || !is_start_verb(command.verb.as_str())
                    || !store.recoverable_workspace_start(&id)?
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
            self.schedule_command(id, linked);
        }
        self.signal_progress();
        Ok(())
    }

    pub(crate) async fn recover_accepted_commands(&self) -> Result<()> {
        let mut after = None;
        loop {
            let keys = {
                let store = self.inner.store.lock().await;
                store.recover_accepted_command_keys(after, STATUS_BLOCKER_MAX)?
            };
            if keys.is_empty() {
                return Ok(());
            }
            let mut resumes = Vec::with_capacity(RESTART_RESUME_BATCH_SIZE);
            for (id, created_at, rowid) in keys {
                after = Some((created_at, rowid));
                let admission = {
                    let store = self.inner.store.lock().await;
                    store.recover_accepted_admission(&id)?
                };
                if let Some(admission) = admission
                    && command_needs_worker(&admission.command)
                {
                    if admission.command.verb == "resume" {
                        let target = admission
                            .command
                            .target
                            .clone()
                            .zip(admission.command.generation);
                        self.queue_boot_resume(&mut resumes, &admission, target)
                            .await?;
                    } else {
                        self.schedule_admission(&admission);
                    }
                }
            }
            self.wait_restart_resume_workers(resumes).await?;
            tokio::task::yield_now().await;
        }
    }

    /// Reopen eligible retained sessions in bounded, depth-ordered batches.
    /// Wait for each shallower batch before attempting deeper Agents. Store
    /// admission also holds descendants of an unresolved restart Resume.
    pub(crate) async fn recover_restart_agents(&self, on_restart: OnRestart) -> Result<()> {
        if on_restart == OnRestart::Hold {
            return Ok(());
        }
        let mut after = None;
        loop {
            let candidates = {
                let store = self.inner.store.lock().await;
                store.restart_resume_candidates(after.as_ref())?
            };
            let Some(last) = candidates.last() else {
                break;
            };
            after = Some(last.clone());
            let mut workers = Vec::with_capacity(RESTART_RESUME_BATCH_SIZE);
            let mut depth = candidates[0].depth;
            for candidate in candidates {
                if candidate.depth != depth {
                    self.wait_restart_resume_workers(std::mem::take(&mut workers))
                        .await?;
                    depth = candidate.depth;
                }
                if let Some(admission) = self.prepare_restart_resume(&candidate).await
                    && admission.is_new
                    && command_needs_worker(&admission.command)
                {
                    let target = Some((candidate.agent, candidate.generation));
                    self.queue_boot_resume(&mut workers, &admission, target)
                        .await?;
                }
            }
            self.wait_restart_resume_workers(workers).await?;
            tokio::task::yield_now().await;
        }
        self.signal_progress();
        Ok(())
    }

    async fn queue_boot_resume(
        &self,
        workers: &mut Vec<BootResumeWorker>,
        admission: &Admission,
        target: Option<(String, i64)>,
    ) -> Result<()> {
        let id = admission.command.id.clone();
        let linked = admission
            .linked_command
            .as_ref()
            .map(|command| command.id.clone());
        let task = tokio::spawn(self.clone().process_command(id.clone(), linked));
        workers.push(BootResumeWorker { id, task, target });
        if workers.len() == RESTART_RESUME_BATCH_SIZE {
            self.wait_restart_resume_workers(std::mem::take(workers))
                .await?;
        }
        Ok(())
    }

    /// Keep the boot batch tied to the actual command tasks. A task that
    /// returns without durably settling its command stops further recovery.
    async fn wait_restart_resume_workers(&self, workers: Vec<BootResumeWorker>) -> Result<()> {
        let mut first_error = None;
        for BootResumeWorker { id, task, target } in workers {
            if task.await.is_err() {
                first_error.get_or_insert_with(|| {
                    Error::new(
                        "unknown",
                        "restart Resume command worker ended unexpectedly",
                    )
                });
                continue;
            }
            let result = self.inner.store.lock().await.get_command(&id);
            match result {
                Ok(Some(command))
                    if matches!(command.state, CommandState::Rejected | CommandState::Failed) =>
                {
                    if let Some((agent, source_generation)) = target
                        && let Some(outcome) = command.outcome
                    {
                        let code = outcome
                            .get("code")
                            .or_else(|| outcome.get("reason"))
                            .and_then(Value::as_str)
                            .unwrap_or("resume_rejected");
                        let message = outcome
                            .get("message")
                            .and_then(Value::as_str)
                            .unwrap_or("The native session could not be reopened.");
                        self.report_restart_resume_blocked(
                            &agent,
                            command.generation.unwrap_or(source_generation),
                            Error::new(code, message),
                        )
                        .await;
                    }
                }
                Ok(Some(command))
                    if command.state.is_terminal() || command.state == CommandState::Unknown => {}
                Ok(_) => {
                    first_error.get_or_insert_with(|| {
                        Error::new(
                            "unknown",
                            "restart Resume command worker ended before settlement",
                        )
                    });
                }
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    async fn prepare_restart_resume(
        &self,
        candidate: &RestartResumeCandidate,
    ) -> Option<Admission> {
        if candidate.interrupted && candidate.undelivered_send_text.is_some() {
            self.report_restart_resume_blocked(
                &candidate.agent,
                candidate.generation,
                Error::new(
                    "bad_state",
                    "restart has both an interrupted turn and an undelivered launch input",
                ),
            )
            .await;
            return None;
        }
        let args = if candidate.interrupted {
            let body = eliot_core::format_restart_interrupted_turn_note_body(
                &candidate.agent,
                candidate.generation,
                candidate.turn_id.as_deref(),
            );
            json!({"text": eliot_core::swarm_note(&body, eliot_core::RESTART_PRINCIPAL)})
        } else if let Some(text) = candidate.undelivered_send_text.as_deref() {
            json!({"text": text})
        } else {
            json!({})
        };
        let request = SubmitCommand {
            id: ulid::Ulid::new().to_string(),
            target: Some(candidate.agent.clone()),
            selector: None,
            verb: "resume".to_owned(),
            args,
            expected_generation: Some(candidate.generation),
            expected_turn_id: None,
            request_id: None,
            probe: false,
            trusted_review_labels: None,
        };
        let prepared = match self.prepare_submission(request, &Principal::Owner).await {
            Ok(prepared) => prepared,
            Err(error) => {
                self.report_restart_resume_blocked(&candidate.agent, candidate.generation, error)
                    .await;
                return None;
            }
        };
        let PreparedSubmission { request, workspace } = prepared;
        if workspace.is_some() {
            let error = Error::new(
                "unsupported",
                "automatic restart Resume cannot prepare a workspace",
            );
            self.report_restart_resume_blocked(&candidate.agent, candidate.generation, error)
                .await;
            return None;
        }
        let result = {
            let mut store = self.inner.store.lock().await;
            match self.check_command_admission_mode("resume") {
                Err(error) => Err(error),
                Ok(()) => match store.submit_restart_resume(request, now_ms()) {
                    Ok(admission) => {
                        if !admission.events.is_empty() {
                            self.publish_locked(&admission.events);
                        }
                        Ok(admission)
                    }
                    Err(error) => Err(error),
                },
            }
        };
        match result {
            Ok(admission) => Some(admission),
            Err(error) => {
                self.report_restart_resume_blocked(&candidate.agent, candidate.generation, error)
                    .await;
                None
            }
        }
    }

    async fn report_restart_resume_blocked(&self, agent: &str, generation: i64, error: Error) {
        let error = self.sanitized_failure(&error, "daemon.restart.resume");
        let policy = self.secret_policy_snapshot();
        let generation_field = generation.to_string();
        let fields = [("agent", agent), ("generation", generation_field.as_str())];
        let message = format!(
            "Automatic resume for {} generation {} was blocked ({}). {}. When resolved, run swarm resume {}.",
            agent,
            generation,
            eliot_core::bound_chars(&error.code, 128),
            eliot_core::bound_chars(&error.message, 360).trim_end_matches('.'),
            agent
        );
        let data = json!({"message": &message});
        let record: Result<bool> = {
            let mut store = self.inner.store.lock().await;
            (|| {
                let Some(current) = store.get_agent(agent)? else {
                    return Ok(false);
                };
                if current.generation != generation
                    || !current.is_terminal()
                    || current.archived
                    || store.restart_resume_blocked(agent, generation)?.as_ref() == Some(&data)
                {
                    return Ok(false);
                }
                let event = store.commit_observe_event(
                    Some(agent),
                    Some(generation),
                    "agent.restart_resume_blocked",
                    data,
                    now_ms(),
                )?;
                self.publish_locked(&[event]);
                Ok(true)
            })()
        };
        match record {
            Ok(false) => return,
            Ok(true) => crate::logging::record(
                &policy,
                "daemon.restart_resume_skipped",
                &fields,
                Some(&error),
            ),
            Err(record_error) => crate::logging::record(
                &policy,
                "daemon.restart_resume_record_failed",
                &fields,
                Some(&record_error),
            ),
        }
        if let Err(error) = self.report_daemon_attention(message).await {
            crate::logging::record(
                &policy,
                "daemon.attention_delivery_failed",
                &fields,
                Some(&error),
            );
        }
    }

    pub(crate) fn note_workspace_recovery_failure(&self, code: &str) {
        *self.inner.workspace_recovery_error.lock_recover() = Some(code.to_owned());
        self.signal_progress();
    }

    pub(crate) fn note_worker_task_starting(&self, name: &str) {
        self.update_worker_task(name, WorkerTaskHealth::Starting, None);
    }

    pub(crate) fn note_worker_task_running(&self, name: &str) {
        self.update_worker_task(name, WorkerTaskHealth::Running, None);
    }

    pub(crate) fn note_worker_task_failed(
        &self,
        name: &str,
        failure_code: String,
        cause: String,
        restart_count: u32,
    ) {
        self.update_worker_task(
            name,
            WorkerTaskHealth::Failed,
            Some((failure_code, cause, restart_count)),
        );
    }

    pub(crate) fn note_worker_task_stopped(&self, name: &str) {
        self.update_worker_task(name, WorkerTaskHealth::Stopped, None);
    }

    fn update_worker_task(
        &self,
        name: &str,
        health: WorkerTaskHealth,
        failure: Option<(String, String, u32)>,
    ) {
        if let Some(task) = self
            .inner
            .worker_tasks
            .lock_recover()
            .iter_mut()
            .find(|task| task.name == name)
        {
            task.health = health;
            task.failure_code = None;
            if let Some((code, cause, restart_count)) = failure {
                task.failure_code = Some(code);
                task.last_failure = Some(cause);
                task.restart_count = restart_count;
            }
        }
        self.signal_progress();
    }

    pub(crate) fn note_events(&self, events: &[Event]) {
        self.note_unknown(events);
        self.signal_progress();
    }

    pub(crate) async fn report_acl_repairs(
        &self,
        policy: &SecretPolicy,
        removed: Vec<crate::platform::ForeignDataDirAce>,
    ) -> bool {
        self.inner
            .acl_notices
            .report(&self.inner.store, &self.inner.bus, policy, removed)
            .await
    }

    fn signal_progress(&self) {
        self.inner.progress.send_modify(|value| {
            *value = value.wrapping_add(1);
        });
    }

    /// Route a committed refusal for native requests whose reply cannot carry
    /// text. The synchronous request intake already holds the tool and blocks
    /// approval. Only this live receipt drives the ordinary deny and context.
    pub(crate) async fn deliver_project_guard(&self, event: &Event) -> Result<()> {
        use eliot_core::{
            InjectArgs,
            requests::{ApprovalArgs, ApprovalDecision, ApprovalRequest},
        };

        if event.kind != "request.opened" || event.source != "stream" {
            return Ok(());
        }
        let Some(subject) = &event.subject else {
            return Ok(());
        };
        let Some(point) = HarnessCapabilities::of(&subject.harness).guard_refusal_point else {
            return Ok(());
        };
        let details = &event.data["details"];
        if details.get("guard_refusal").is_none() {
            return Ok(());
        }
        let approval: ApprovalRequest = serde_json::from_value(details.clone())
            .map_err(|_| Error::new("source_integrity", "guarded approval details are invalid"))?;
        let Some(message) = approval.guard_refusal else {
            return Ok(());
        };
        let Some(agent) = &event.agent else {
            return Err(Error::new(
                "source_integrity",
                "guarded request has no Agent",
            ));
        };
        let Some(generation) = event.generation else {
            return Err(Error::new(
                "source_integrity",
                "guarded request has no generation",
            ));
        };
        let turn_id = required_text(&event.data, "turn_id")?.to_owned();
        let request_id = required_text(&event.data, "request_id")?.to_owned();
        let mut approvals = approval
            .choices
            .into_iter()
            .filter(|choice| choice.decision == ApprovalDecision::Deny);
        let first = approvals
            .next()
            .ok_or_else(|| Error::new("unsupported", "guarded request offers no refusal option"))?;
        let choice_id = match subject.harness.as_str() {
            "muse" if approvals.next().is_none() => Some(first.id),
            "muse" => {
                return Err(Error::new(
                    "unsupported",
                    "Muse guarded request offers multiple refusal options",
                ));
            }
            _ => None,
        };
        let mut deny = SubmitCommand::new(ulid::Ulid::new().to_string(), "deny");
        deny.target = Some(agent.clone());
        deny.expected_generation = Some(generation);
        deny.expected_turn_id = Some(turn_id.clone());
        deny.request_id = Some(request_id);
        deny.args = serde_json::to_value(ApprovalArgs {
            choice_id,
            feedback: None,
        })
        .map_err(|_| Error::new("internal", "guard reply could not be encoded"))?;
        self.submit(&json!(deny), &Principal::Owner, "command.submit")
            .await?;

        let mut inject = SubmitCommand::new(ulid::Ulid::new().to_string(), "inject");
        inject.target = Some(agent.clone());
        inject.expected_generation = Some(generation);
        inject.expected_turn_id = Some(turn_id);
        inject.args = json!(InjectArgs {
            text: message,
            point,
            expires_at_ms: Some(now_ms().saturating_add(60_000)),
        });
        self.submit(&json!(inject), &Principal::Owner, "command.submit")
            .await?;
        Ok(())
    }

    async fn submit(
        &self,
        params: &Value,
        principal: &Principal,
        method: &'static str,
    ) -> Result<Value> {
        let result = self
            .submit_bound(params, principal, SubmissionContext::default())
            .await;
        self.note_command_submission(principal, None, method, &result);
        result
    }

    /// Mark a refused submission as an admission preflight refusal only when
    /// the Store proves no command exists under the caller's exact ID.
    async fn mark_admission_preflight(&self, params: &Value, error: Error) -> Error {
        let Some(id) = params
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| ulid::Ulid::from_string(id).is_ok())
        else {
            return error;
        };
        let admitted = !matches!(self.inner.store.lock().await.get_command(id), Ok(None));
        if admitted {
            error
        } else {
            eliot_core::AdmissionRefusal::at_preflight(error)
        }
    }

    async fn submit_bound(
        &self,
        params: &Value,
        principal: &Principal,
        context: SubmissionContext<'_>,
    ) -> Result<Value> {
        let mut request: CommandSubmitRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        if request.verb == "git.integrate" {
            return self.submit_git_integrate(request, principal, context).await;
        }
        if request.verb == "git.fetch" {
            return self.submit_git_fetch(request, principal, context).await;
        }
        let remote = context.remote;
        // The internal attention verb is admitted only through
        // `submit_attention_event`; the public path never carries it.
        if request.verb == "attention.emit" {
            return Err(Error::new(
                "invalid",
                "attention.emit is reserved for the internal attention producer",
            ));
        }

        if let Some(flag) = request.args.get("owner_override") {
            require_owner(principal)?;
            if flag != &Value::Bool(true) || request.verb != "launch" {
                return Err(Error::new(
                    "invalid",
                    "owner_override is an explicit Owner launch flag",
                ));
            }
        }
        authorize_command(principal, &mut request)?;
        self.authorize_submission_scope(&mut request, principal)
            .await?;
        let replay_request = request.clone();
        if let Some(result) = self
            .replay_command_submission(&mut request, principal, remote)
            .await?
        {
            return Ok(result);
        }
        if is_user_event_verb(request.verb.as_str())
            && let Err(error) = Self::validate_user_event_request(&request)
        {
            let mut retry = replay_request.clone();
            if let Some(result) = self
                .replay_command_submission(&mut retry, principal, remote)
                .await?
            {
                return Ok(result);
            }
            return Err(error);
        }

        let prepared = match self
            .prepare_submission_after_authorization(request, principal, false, true)
            .await
        {
            Ok(prepared) => prepared,
            Err(error) => {
                let mut retry = replay_request.clone();
                if let Some(result) = self
                    .replay_command_submission(&mut retry, principal, remote)
                    .await?
                {
                    return Ok(result);
                }
                return Err(error);
            }
        };
        let submitted = {
            let mut store = self.inner.store.lock().await;
            let principal = principal_name(principal);
            let submitted =
                self.admit_prepared_submission(&mut store, &principal, prepared, context);
            match submitted {
                Ok(admission) => {
                    if !admission.events.is_empty() {
                        self.publish_locked(&admission.events);
                    }
                    Ok(admission)
                }
                Err(error) => Err(error),
            }
        };
        let admission = match submitted {
            Ok(admission) => admission,
            Err(error) => {
                let mut retry = replay_request;
                if let Some(result) = self
                    .replay_command_submission(&mut retry, principal, remote)
                    .await?
                {
                    return Ok(result);
                }
                return Err(error);
            }
        };
        if admission.command.state == CommandState::Unknown {
            self.inner
                .unknown_commands
                .lock_recover()
                .insert(admission.command.id.clone());
        }

        if admission.is_new && command_needs_worker(&admission.command) {
            self.schedule_admission(&admission);
        }
        self.signal_progress();
        crate::logging::record_debug(
            &self.secret_policy_snapshot(),
            "command.admitted",
            &[
                ("command_id", &admission.command.id),
                ("verb", &admission.command.verb),
            ],
            &[],
        );
        serde_json::to_value(submit_result(
            &admission.command,
            admission.linked_command.as_ref(),
        ))
        .map_err(|_| Error::new("internal", "command admission result could not be encoded"))
    }

    // Caller holds the writer used by shutdown to change mode. Retained reads
    // precede both the shutdown gate and Store's writer-health check, including
    // when an identical admission won during asynchronous preparation.
    fn admit_prepared_submission(
        &self,
        store: &mut eliot_store::Store,
        principal: &str,
        prepared: PreparedSubmission,
        context: SubmissionContext<'_>,
    ) -> Result<Admission> {
        let PreparedSubmission { request, workspace } = prepared;
        if let Some(admission) =
            store.replay_command_submission(principal, &request, context.remote)?
        {
            return Ok(admission);
        }
        owner_inbox::check_binding(store)?;
        self.check_command_admission_mode(&request.verb)?;
        if request.verb == "answer" {
            validate_answer_request(store, &request)?;
        }
        store.submit_with_context(principal, request, workspace, context, now_ms())
    }

    fn check_command_admission_mode(&self, verb: &str) -> Result<()> {
        let mode = self.inner.mode.load(Ordering::SeqCst);
        if verb == "launch" && matches!(mode, MODE_QUIESCE | MODE_RESTART) {
            return Err(update_launch_refusal());
        }
        if matches!(mode, MODE_FORCE | MODE_RESTART) {
            return Err(Error::new("unavailable", "daemon is shutting down"));
        }
        if matches!(verb, "launch" | "resume" | "send" | "attach") && mode != MODE_OPEN {
            return Err(Error::new(
                "unavailable",
                "daemon is draining and rejects new launch, resume, send, or attach commands",
            ));
        }
        Ok(())
    }

    async fn replay_command_submission(
        &self,
        request: &mut SubmitCommand,
        principal: &Principal,
        remote: Option<&eliot_core::RemoteCallContext>,
    ) -> Result<Option<Value>> {
        let store = self.inner.store.lock().await;
        store.ensure_committed_reads()?;
        // A same-ID retry of a launch or send with `sources` replays the text
        // its first admission delivered, when that text is this request's
        // prompt plus the delivered block.
        if matches!(request.verb.as_str(), "launch" | "send")
            && request.args.get("sources").is_some()
            && let Some(retained) = store.get_command(&request.id)?
            && let Some(stored) = retained.args.get("text").and_then(Value::as_str)
            && sources::is_delivery_of(
                stored,
                request
                    .args
                    .get("text")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
            )
        {
            request.args["text"] = json!(stored);
            if let Some(args) = request.args.as_object_mut() {
                args.remove("sources");
            }
        }
        match request.verb.as_str() {
            "launch" => {
                reject_launch_derived_keys(&request.args)?;
                if let Some(retained) = retained_launch_profile(&store, request)? {
                    if request.args.get("chat") == Some(&Value::Bool(true))
                        && request.args.get("workspace").is_none()
                    {
                        if let Some(workspace) = retained.workspace.clone() {
                            request.args["workspace"] = workspace;
                        }
                        request.args["project"] = json!("");
                    }
                    align_retained_workspace_alias(request, retained.workspace.as_ref());
                    install_retained_profile(request, retained)?;
                }
            }
            "attach" => {
                normalize_attach(request)?;
                install_retained_attach_profile(&store, request)?;
            }
            "detach" => Self::hydrate_detach_generation_from_store(&store, request)?,
            "resume" => normalize_resume(request)?,
            _ => {}
        }

        let admission =
            store.replay_command_submission(&principal_name(principal), request, remote)?;
        admission
            .map(|admission| self.retained_command_result(admission))
            .transpose()
    }

    fn retained_command_result(&self, admission: Admission) -> Result<Value> {
        let Admission {
            mut command,
            linked_command,
            ..
        } = admission;
        if command.state == CommandState::Unknown {
            self.inner
                .unknown_commands
                .lock_recover()
                .insert(command.id.clone());
        }
        self.signal_progress();
        self.adjust_command_for_read(&mut command);
        serde_json::to_value(submit_result(&command, linked_command.as_ref()))
            .map_err(|_| Error::new("internal", "retained command result could not be encoded"))
    }

    async fn submit_git_integrate(
        &self,
        mut request: CommandSubmitRequest,
        principal: &Principal,
        context: SubmissionContext<'_>,
    ) -> Result<Value> {
        request.validate()?;
        if request.verb != "git.integrate" {
            return Err(Error::new(
                "invalid",
                "Git integration requires git.integrate",
            ));
        }
        let definition = eliot_core::api::discovery::decode_params_at::<
            eliot_core::api::GitIntegrateDefinitionArgs,
        >(request.args.clone(), "params.args")?;
        definition.validate()?;
        let timeout_ms =
            crate::automation::git_integrate_timeout_ms(definition.timeout.as_deref())?;
        authorize_command(principal, &mut request)?;
        self.authorize_submission_scope(&mut request, principal)
            .await?;
        if let Some(response) = self
            .retained_git_integrate_submit(principal, &request, timeout_ms)
            .await?
        {
            return Ok(response);
        }
        let prepared = match self
            .prepare_git_integrate_submission(request.clone(), principal, None)
            .await
        {
            Ok(prepared) => prepared,
            Err(error) => {
                if let Some(response) = self
                    .retained_git_integrate_submit(principal, &request, timeout_ms)
                    .await?
                {
                    return Ok(response);
                }
                return Err(error);
            }
        };
        let saved_principal = principal_name(principal);
        let admission = {
            let mut store = self.inner.store.lock().await;
            self.admit_prepared_submission(&mut store, &saved_principal, prepared, context)
                .inspect(|admission| self.publish_locked(&admission.events))
        };
        let admission = match admission {
            Ok(admission) => admission,
            Err(error) => {
                if let Some(response) = self
                    .retained_git_integrate_submit(principal, &request, timeout_ms)
                    .await?
                {
                    return Ok(response);
                }
                return Err(error);
            }
        };
        if admission.command.state == CommandState::Unknown {
            self.inner
                .unknown_commands
                .lock_recover()
                .insert(admission.command.id.clone());
        }
        if admission.is_new {
            self.schedule_command(admission.command.id.clone(), None);
        }
        self.signal_progress();
        serde_json::to_value(submit_result(
            &admission.command,
            admission.linked_command.as_ref(),
        ))
        .map_err(|_| {
            Error::new(
                "internal",
                "git.integrate admission result could not be encoded",
            )
        })
    }

    async fn retained_git_integrate_submit(
        &self,
        principal: &Principal,
        request: &CommandSubmitRequest,
        timeout_ms: i64,
    ) -> Result<Option<Value>> {
        let admission = self
            .inner
            .store
            .lock()
            .await
            .replay_git_integrate_submission(&principal_name(principal), request, timeout_ms)?;
        let Some(admission) = admission else {
            return Ok(None);
        };
        self.note_unknown(&admission.events);
        if admission.command.state == CommandState::Unknown {
            self.inner
                .unknown_commands
                .lock_recover()
                .insert(admission.command.id.clone());
        }
        self.signal_progress();
        serde_json::to_value(submit_result(
            &admission.command,
            admission.linked_command.as_ref(),
        ))
        .map(Some)
        .map_err(|_| {
            Error::new(
                "internal",
                "retained git.integrate result could not be encoded",
            )
        })
    }

    async fn prepare_git_integrate_submission(
        &self,
        mut request: SubmitCommand,
        principal: &Principal,
        expected_registration: Option<&ProjectRegistrationView>,
    ) -> Result<PreparedSubmission> {
        if request.verb != "git.integrate"
            || request.selector.is_some()
            || request.target.is_some()
            || request.expected_generation.is_some()
            || request.expected_turn_id.is_some()
            || request.request_id.is_some()
            || request.probe
        {
            return Err(Error::new(
                "invalid",
                "git.integrate must be targetless and have no Agent command bindings",
            ));
        }
        authorize_command(principal, &mut request)?;
        if contains_known_secret(&request.args, &self.inner.secrets) {
            return Err(Error::new(
                "secret_in_payload",
                "git.integrate request contains a known credential",
            ));
        }
        let definition = eliot_core::api::discovery::decode_params_at::<
            eliot_core::api::GitIntegrateDefinitionArgs,
        >(request.args.clone(), "params.args")?;
        definition.validate()?;
        let timeout_ms =
            crate::automation::git_integrate_timeout_ms(definition.timeout.as_deref())?;
        let caller = match principal {
            Principal::Owner => None,
            Principal::ProjectRule {
                registration: saved,
            } => {
                if definition.repository != saved.id
                    || expected_registration.is_some_and(|expected| expected != saved.as_ref())
                {
                    return Err(Error::new(
                        "not_allowed",
                        "project git.integrate repository differs from its saved principal",
                    ));
                }
                let registration = self
                    .inner
                    .store
                    .lock()
                    .await
                    .get_project_registration("owner", &saved.id)?
                    .ok_or_else(|| Error::new("not_allowed", "project registration was removed"))?;
                if registration != **saved {
                    return Err(Error::new(
                        "not_allowed",
                        "project git.integrate registration changed before preparation",
                    ));
                }
                None
            }
            Principal::Agent { id, generation } => {
                let (agent, registration) = {
                    let store = self.inner.store.lock().await;
                    let agent = store
                        .get_agent(id)?
                        .ok_or_else(|| Error::new("stale_generation", "manager Agent is absent"))?;
                    let registration = store
                        .get_project_registration("owner", &definition.repository)?
                        .ok_or_else(|| {
                            Error::new("not_allowed", "git.integrate repository is not registered")
                        })?;
                    (agent, registration)
                };
                if agent.generation != *generation
                    || agent.is_terminal()
                    || agent.observe_only
                    || !matches!(
                        agent.control,
                        eliot_core::AgentControl::Tree | eliot_core::AgentControl::Project
                    )
                    || expected_registration.is_some_and(|expected| expected != &registration)
                    || !registration
                        .actions
                        .iter()
                        .any(|action| action == "git.integrate")
                {
                    return Err(Error::new(
                        "not_allowed",
                        "git.integrate requires a current manager in its approved registered project",
                    ));
                }
                let retained_workspace = agent.workspace.as_deref().ok_or_else(|| {
                    Error::new("not_allowed", "manager has no retained workspace")
                })?;
                let (canonical, common_dir) = crate::workspace_git::WorkspaceGit::new()?
                    .repository_identity(retained_workspace)
                    .await?;
                if canonical != retained_workspace || common_dir != registration.repo_common_dir {
                    return Err(Error::new(
                        "not_allowed",
                        "manager workspace is outside the registered Git repository",
                    ));
                }
                if let Some(target) = &definition.fast_forward_to
                    && agent.branch.as_deref() != Some(target.as_str())
                {
                    return Err(Error::new(
                        "not_allowed",
                        "git.integrate fast-forward target must be the manager's own branch",
                    ));
                }
                Some(eliot_core::api::GitIntegrateCaller {
                    agent: id.clone(),
                    generation: *generation,
                    workspace: retained_workspace.to_owned(),
                    project_scope: agent.project_scope().to_owned(),
                    control: agent.control,
                })
            }
            _ => {
                return Err(Error::new(
                    "not_allowed",
                    "git.integrate requires Owner, project-rule, or manager authority",
                ));
            }
        };
        self.check_command_admission_mode(&request.verb)?;
        let registration = match principal {
            Principal::Owner => self
                .inner
                .store
                .lock()
                .await
                .get_project_registration("owner", &definition.repository)?
                .ok_or_else(|| {
                    Error::new("not_found", "Git integration repository is not registered")
                })?,
            Principal::ProjectRule { registration } => registration.as_ref().clone(),
            Principal::Agent { .. } => self
                .inner
                .store
                .lock()
                .await
                .get_project_registration("owner", &definition.repository)?
                .ok_or_else(|| Error::new("not_allowed", "git.integrate repository was removed"))?,
            _ => unreachable!(),
        };
        if expected_registration.is_some_and(|expected| expected != &registration) {
            return Err(Error::new(
                "not_allowed",
                "git.integrate registration differs from its saved project principal",
            ));
        }
        registration.validate()?;
        crate::automation::verify_project_registration(&self.inner.data_root, &registration)
            .await?;
        self.reconcile_git_integrations(Some(&registration.repo_common_dir))
            .await?;
        let frozen =
            crate::git_integration::freeze(&registration, &definition, caller, timeout_ms).await?;
        request.args = serde_json::to_value(frozen)
            .map_err(|_| Error::new("internal", "frozen git.integrate intent cannot be encoded"))?;
        self.prepare_submission_after_authorization(request, principal, true, false)
            .await
    }
    async fn submit_git_fetch(
        &self,
        request: CommandSubmitRequest,
        principal: &Principal,
        context: SubmissionContext<'_>,
    ) -> Result<Value> {
        request.validate()?;
        let definition = eliot_core::api::discovery::decode_params_at::<GitFetchDefinitionArgs>(
            request.args.clone(),
            "params.args",
        )?;
        definition.validate()?;
        let timeout_ms = crate::automation::git_fetch_timeout_ms(definition.timeout.as_deref())?;

        if let Some(response) = self
            .retained_git_fetch_submit(principal, &request, timeout_ms)
            .await?
        {
            return Ok(response);
        }

        let prepared = match self
            .prepare_git_fetch_submission(request.clone(), principal, None)
            .await
        {
            Ok(prepared) => prepared,
            Err(error) => {
                if let Some(response) = self
                    .retained_git_fetch_submit(principal, &request, timeout_ms)
                    .await?
                {
                    return Ok(response);
                }
                return Err(error);
            }
        };
        let principal_name = principal_name(principal);
        let admission = {
            let mut store = self.inner.store.lock().await;
            let result =
                self.admit_prepared_submission(&mut store, &principal_name, prepared, context);
            match result {
                Ok(admission) => {
                    self.publish_locked(&admission.events);
                    Ok(admission)
                }
                Err(error) => Err(error),
            }
        };
        let admission = match admission {
            Ok(admission) => admission,
            Err(error) => {
                if let Some(response) = self
                    .retained_git_fetch_submit(principal, &request, timeout_ms)
                    .await?
                {
                    return Ok(response);
                }
                return Err(error);
            }
        };
        if admission.command.state == CommandState::Unknown {
            self.inner
                .unknown_commands
                .lock_recover()
                .insert(admission.command.id.clone());
        }
        if admission.is_new {
            self.schedule_command(admission.command.id.clone(), None);
        }
        self.signal_progress();
        serde_json::to_value(submit_result(
            &admission.command,
            admission.linked_command.as_ref(),
        ))
        .map_err(|_| {
            Error::new(
                "internal",
                "git.fetch admission result could not be encoded",
            )
        })
    }

    /// Validate one targetless local `notify`/`emit` submission. Admission
    /// secret checks run in the shared preparation path; execution commits
    /// exactly this validated DTO as one persisted `x.user.*` event. The
    /// reserved attention topic is rejected here: only the internal
    /// attention producer (verb `attention.emit`) may use the bound commit.
    fn validate_user_event_request(request: &SubmitCommand) -> Result<()> {
        if request.target.is_some()
            || request.expected_generation.is_some()
            || request.expected_turn_id.is_some()
            || request.request_id.is_some()
            || request.probe
        {
            return Err(Error::new(
                "invalid",
                "notify/emit must be targetless and have no Agent or request bindings",
            ));
        }
        let (kind, _) = user_event_kind_data(&request.verb, &request.args)?;
        if matches!(
            kind.as_str(),
            ATTENTION_USER_EVENT_KIND | eliot_core::ATTENTION_STEP_KIND
        ) {
            return Err(Error::new(
                "invalid",
                "monitoring topics are reserved for the internal attention producer",
            ));
        }
        Ok(())
    }

    /// Submit one internal attention event command (verb `attention.emit`).
    /// Only the attention producer calls this; the public `submit` path
    /// rejects the verb so a public emit can never acquire the bound commit.
    pub(crate) async fn submit_attention_event(&self, params: &Value) -> Result<Value> {
        let mut request: CommandSubmitRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        if request.verb != "attention.emit" {
            return Err(Error::new(
                "invalid",
                "attention submission requires the attention.emit verb",
            ));
        }
        authorize_command(&Principal::Owner, &mut request)?;
        self.authorize_submission_scope(&mut request, &Principal::Owner)
            .await?;
        let prepared = self
            .prepare_submission_after_authorization(request, &Principal::Owner, false, true)
            .await?;
        let submitted = {
            let mut store = self.inner.store.lock().await;
            let submitted = self.admit_prepared_submission(
                &mut store,
                &principal_name(&Principal::Owner),
                prepared,
                SubmissionContext::default(),
            );
            match submitted {
                Ok(admission) => {
                    if !admission.events.is_empty() {
                        self.publish_locked(&admission.events);
                    }
                    Ok(admission)
                }
                Err(error) => Err(error),
            }
        };
        let admission = submitted?;
        if admission.command.state == CommandState::Unknown {
            self.inner
                .unknown_commands
                .lock_recover()
                .insert(admission.command.id.clone());
        }
        if admission.is_new && command_needs_worker(&admission.command) {
            self.schedule_admission(&admission);
        }
        self.signal_progress();
        serde_json::to_value(submit_result(
            &admission.command,
            admission.linked_command.as_ref(),
        ))
        .map_err(|_| Error::new("internal", "command admission result could not be encoded"))
    }

    async fn retained_git_fetch_submit(
        &self,
        principal: &Principal,
        request: &CommandSubmitRequest,
        timeout_ms: i64,
    ) -> Result<Option<Value>> {
        let admission = self.inner.store.lock().await.replay_git_fetch_submission(
            &principal_name(principal),
            request,
            timeout_ms,
        )?;
        let Some(admission) = admission else {
            return Ok(None);
        };
        self.note_unknown(&admission.events);
        if admission.command.state == CommandState::Unknown {
            self.inner
                .unknown_commands
                .lock_recover()
                .insert(admission.command.id.clone());
        }
        self.signal_progress();
        serde_json::to_value(submit_result(
            &admission.command,
            admission.linked_command.as_ref(),
        ))
        .map(Some)
        .map_err(|_| Error::new("internal", "retained git.fetch result could not be encoded"))
    }

    pub(crate) async fn issue_run_credential(
        &self,
        command: &Command,
    ) -> Result<(RunCredentialGuard, SavedRunPrincipal)> {
        if command.verb != "run"
            || command.state != CommandState::Dispatched
            || command.target.is_some()
            || command.generation.is_some()
            || command.request_id.is_some()
        {
            return Err(Error::new(
                "bad_state",
                "Run credentials require a dispatched targetless run command",
            ));
        }
        let saved_principal = if let Some(project_id) = command.principal.strip_prefix("project:") {
            if project_id.trim().is_empty() {
                return Err(Error::new("not_allowed", "stored Run principal is invalid"));
            }
            let registration = self
                .inner
                .store
                .lock()
                .await
                .get_project_registration("owner", project_id)?
                .ok_or_else(|| Error::new("not_allowed", "Run project registration is absent"))?;
            if registration.id != project_id {
                return Err(Error::new("source_integrity", "Run project scope changed"));
            }
            registration.validate()?;
            SavedRunPrincipal::ProjectRule {
                registration: Box::new(registration),
            }
        } else if command.principal == "owner" {
            SavedRunPrincipal::Owner
        } else {
            return Err(Error::new(
                "not_allowed",
                "stored Run principal is unsupported",
            ));
        };
        let guard = self
            .inner
            .run_credentials
            .issue(command.id.clone(), saved_principal.clone())?;
        Ok((guard, saved_principal))
    }

    async fn prepare_git_fetch_submission(
        &self,
        mut request: SubmitCommand,
        principal: &Principal,
        expected_registration: Option<&ProjectRegistrationView>,
    ) -> Result<PreparedSubmission> {
        if request.verb != "git.fetch"
            || request.target.is_some()
            || request.expected_generation.is_some()
            || request.expected_turn_id.is_some()
            || request.request_id.is_some()
            || request.probe
        {
            return Err(Error::new(
                "invalid",
                "git.fetch must be targetless and have no Agent or request bindings",
            ));
        }
        authorize_command(principal, &mut request)?;
        if contains_known_secret(&request.args, &self.inner.secrets) {
            return Err(Error::new(
                "secret_in_payload",
                "git.fetch request contains a known credential",
            ));
        }
        let definition = eliot_core::api::discovery::decode_params_at::<GitFetchDefinitionArgs>(
            request.args.clone(),
            "params.args",
        )?;
        definition.validate()?;
        let timeout_ms = crate::automation::git_fetch_timeout_ms(definition.timeout.as_deref())?;
        let _binding_args = definition.binding_args(timeout_ms)?;
        self.check_command_admission_mode(&request.verb)?;

        let registration = match principal {
            Principal::Owner => self
                .inner
                .store
                .lock()
                .await
                .get_project_registration("owner", &definition.repository)?
                .ok_or_else(|| Error::new("not_found", "Git fetch repository is not registered"))?,
            Principal::ProjectRule {
                registration: saved,
            } => {
                let saved = saved.as_ref();
                if definition.repository != saved.id {
                    return Err(Error::new(
                        "not_allowed",
                        "project git.fetch repository differs from its saved principal",
                    ));
                }
                if expected_registration.is_some_and(|expected| expected != saved) {
                    return Err(Error::new(
                        "not_allowed",
                        "project git.fetch registration differs from its saved principal",
                    ));
                }
                let current = self
                    .inner
                    .store
                    .lock()
                    .await
                    .get_project_registration("owner", &saved.id)?
                    .ok_or_else(|| Error::new("not_allowed", "project registration was removed"))?;
                if current != *saved {
                    return Err(Error::new(
                        "not_allowed",
                        "project registration changed before git.fetch preparation",
                    ));
                }
                saved.clone()
            }
            _ => {
                return Err(Error::new(
                    "not_allowed",
                    "git.fetch requires the Owner or saved project principal",
                ));
            }
        };
        if expected_registration.is_some_and(|expected| expected != &registration) {
            return Err(Error::new(
                "not_allowed",
                "git.fetch registration changed before preparation",
            ));
        }
        let frozen =
            crate::git_actions::prepare_fetch(&self.inner.data_root, &registration, &definition)
                .await?;
        request.args = serde_json::to_value(frozen)
            .map_err(|_| Error::new("internal", "frozen git.fetch intent could not be encoded"))?;
        self.prepare_submission_after_authorization(request, principal, true, false)
            .await
    }

    async fn prepare_git_publish_submission(
        &self,
        mut request: SubmitCommand,
        principal: &Principal,
        expected_registration: Option<&ProjectRegistrationView>,
    ) -> Result<PreparedSubmission> {
        if request.verb != "git.publish"
            || request.target.is_some()
            || request.expected_generation.is_some()
            || request.expected_turn_id.is_some()
            || request.request_id.is_some()
            || request.probe
        {
            return Err(Error::new(
                "invalid",
                "git.publish must be a targetless project-check action",
            ));
        }
        authorize_command(principal, &mut request)?;
        if contains_known_secret(&request.args, &self.inner.secrets) {
            return Err(Error::new(
                "secret_in_payload",
                "git.publish request contains a known credential",
            ));
        }
        let trigger: GitPublishTrigger =
            eliot_core::api::discovery::decode_params_at(request.args.clone(), "params.args")?;
        trigger.validate()?;
        let Principal::ProjectRule {
            registration: saved,
        } = principal
        else {
            return Err(Error::new(
                "not_allowed",
                "git.publish requires a project rule principal",
            ));
        };
        if trigger.repository != saved.id
            || expected_registration.is_some_and(|expected| expected != saved.as_ref())
        {
            return Err(Error::new(
                "not_allowed",
                "git.publish project differs from its saved rule principal",
            ));
        }
        let (registration, agent, workspace, event) = {
            let store = self.inner.store.lock().await;
            let registration = store
                .get_project_registration("owner", &trigger.repository)?
                .ok_or_else(|| {
                    Error::new("not_allowed", "git.publish project is not registered")
                })?;
            let agent = store.get_agent(&trigger.agent)?.ok_or_else(|| {
                Error::new("not_allowed", "git.publish Agent is no longer retained")
            })?;
            let workspace = store.get_workspace(&trigger.agent)?.ok_or_else(|| {
                Error::new("not_allowed", "git.publish managed workspace is absent")
            })?;
            let mut events = store.read_events(
                trigger.source_event_seq - 1,
                1,
                Some(trigger.source_event_seq),
            )?;
            let event = events
                .pop()
                .filter(|event| event.seq == trigger.source_event_seq)
                .ok_or_else(|| {
                    Error::new("not_found", "git.publish check event is not retained")
                })?;
            (registration, agent, workspace, event)
        };
        if &registration != saved.as_ref()
            || expected_registration.is_some_and(|expected| expected != &registration)
            || event.kind != "project.check.passed"
            || event.agent.as_deref() != Some(trigger.agent.as_str())
            || event.generation != Some(trigger.generation)
            || event
                .subject
                .as_ref()
                .and_then(|subject| subject.workspace.as_deref())
                != Some(trigger.workspace.as_str())
        {
            return Err(Error::new(
                "not_allowed",
                "git.publish no longer matches its successful project check event",
            ));
        }
        let check = serde_json::from_value::<eliot_core::api::ProjectCheckPassed>(event.data)
            .map_err(|_| Error::new("source_integrity", "project check event data is invalid"))?;
        if check != trigger.check {
            return Err(Error::new(
                "not_allowed",
                "git.publish check evidence differs from its retained event",
            ));
        }
        let frozen: FrozenGitPublish =
            crate::publication::freeze(&registration, &agent, &workspace, trigger).await?;
        request.args = serde_json::to_value(frozen)
            .map_err(|_| Error::new("internal", "frozen git.publish intent cannot be encoded"))?;
        self.prepare_submission_after_authorization(request, principal, true, false)
            .await
    }

    pub(crate) async fn verify_git_publish_state(&self, frozen: &FrozenGitPublish) -> Result<()> {
        frozen.validate()?;
        let (registration, agent, workspace, event) = {
            let store = self.inner.store.lock().await;
            let registration = store
                .get_project_registration("owner", &frozen.repository)?
                .ok_or_else(|| {
                    Error::new("not_allowed", "git.publish project is not registered")
                })?;
            let agent = store
                .get_agent(&frozen.agent)?
                .ok_or_else(|| Error::new("not_allowed", "git.publish Agent is not retained"))?;
            let workspace = store.get_workspace(&frozen.agent)?.ok_or_else(|| {
                Error::new("not_allowed", "git.publish managed workspace is absent")
            })?;
            let mut events = store.read_events(
                frozen.source_event_seq - 1,
                1,
                Some(frozen.source_event_seq),
            )?;
            let event = events
                .pop()
                .filter(|event| event.seq == frozen.source_event_seq)
                .ok_or_else(|| {
                    Error::new("not_found", "git.publish check event is not retained")
                })?;
            (registration, agent, workspace, event)
        };
        if registration.id != frozen.repository
            || registration.revision != frozen.registration_revision
            || registration.checkout != frozen.canonical_checkout
            || registration.repo_common_dir != frozen.common_dir
            || !registration
                .actions
                .iter()
                .any(|action| action == "git.publish")
            || agent.id != frozen.agent
            || agent.generation != frozen.generation
            || agent.active_turn.is_some()
            || agent.workspace.as_deref() != Some(frozen.workspace.as_str())
            || workspace.agent != frozen.agent
            || workspace.target_generation != frozen.generation
            || workspace.phase != WorkspacePhase::Ready
            || workspace.worktree_path != frozen.workspace
            || workspace.worktree_path == registration.checkout
            || workspace.project != registration.checkout
            || workspace.repo_path != registration.checkout
            || workspace.repo_common_dir != registration.repo_common_dir
            || workspace.base_oid != frozen.base_oid
            || workspace.requested_branch != frozen.branch
            || event.kind != "project.check.passed"
            || event.agent.as_deref() != Some(frozen.agent.as_str())
            || event.generation != Some(frozen.generation)
            || event
                .subject
                .as_ref()
                .and_then(|subject| subject.workspace.as_deref())
                != Some(frozen.workspace.as_str())
        {
            return Err(Error::new(
                "not_allowed",
                "git.publish project-check state changed before its effect",
            ));
        }
        let check = serde_json::from_value::<eliot_core::api::ProjectCheckPassed>(event.data)
            .map_err(|_| Error::new("source_integrity", "project check event data is invalid"))?;
        if check != frozen.check {
            return Err(Error::new(
                "not_allowed",
                "git.publish check evidence differs from its retained event",
            ));
        }
        Ok(())
    }

    pub(crate) async fn prepare_owner_submission(
        &self,
        mut request: SubmitCommand,
    ) -> Result<PreparedSubmission> {
        if request.verb == "git.integrate" {
            return self
                .prepare_git_integrate_submission(request, &Principal::Owner, None)
                .await;
        }
        if request.verb == "git.fetch" {
            return self
                .prepare_git_fetch_submission(request, &Principal::Owner, None)
                .await;
        }
        validate_rule_launch_action(&request, None)?;
        validate_rule_resume_action(&request, None)?;
        authorize_command(&Principal::Owner, &mut request)?;
        let prepared = self
            .prepare_submission_with_routing(
                request,
                &Principal::Owner,
                false,
                false,
                routing::LaunchRouting {
                    owner_automation: true,
                    ..Default::default()
                },
                false,
            )
            .await?;
        validate_rule_launch_preparation(&prepared)?;
        validate_rule_resume_preparation(&prepared, None)?;
        Ok(prepared)
    }

    /// Prepare a targetless global rule Run under the current Owner Run grant.
    /// Ordinary Owner command submission still rejects Run in `authorize_command`.
    pub(crate) async fn prepare_owner_rule_run_submission(
        &self,
        mut request: SubmitCommand,
        context: FrozenScriptContext,
    ) -> Result<PreparedSubmission> {
        if request.verb != "run"
            || !request.id.is_empty()
            || request.target.is_some()
            || request.expected_generation.is_some()
            || request.expected_turn_id.is_some()
            || request.request_id.is_some()
            || request.probe
        {
            return Err(Error::new(
                "invalid",
                "Owner event Run must be targetless and unbound",
            ));
        }
        let definition: RunDefinitionArgs =
            eliot_core::api::discovery::decode_params_at(request.args.clone(), "params.args")?;
        let automation = crate::config::load_automation_config(&self.inner.data_root)?;
        let policy = self.secret_policy_snapshot();
        let frozen = crate::scripts::freeze::freeze_global_run(
            definition,
            &automation.run,
            &self.inner.data_root,
            context,
            policy.as_ref(),
        )?;
        request.args = serde_json::to_value(frozen)
            .map_err(|_| Error::new("internal", "frozen Owner Run could not be encoded"))?;
        self.prepare_submission_after_authorization(request, &Principal::Owner, false, false)
            .await
    }

    /// Freeze a timer Run under its live Owner or project grant before admission.
    pub(crate) async fn prepare_timer_rule_run_submission(
        &self,
        mut request: SubmitCommand,
        project: Option<&eliot_core::api::ProjectRegistrationView>,
        context: FrozenScriptContext,
    ) -> Result<PreparedSubmission> {
        if request.verb != "run"
            || !request.id.is_empty()
            || request.target.is_some()
            || request.expected_generation.is_some()
            || request.expected_turn_id.is_some()
            || request.request_id.is_some()
            || request.probe
            || context.timer().is_none()
        {
            return Err(Error::new(
                "invalid",
                "timer Run must be targetless, unbound, and carry timer context",
            ));
        }
        context.validate()?;
        match project {
            Some(registration) => {
                let definition: RunDefinitionArgs = eliot_core::api::discovery::decode_params_at(
                    request.args.clone(),
                    "params.args",
                )?;
                let policy = self.secret_policy_snapshot();
                let frozen = crate::scripts::freeze_project_run(
                    definition,
                    registration,
                    context,
                    policy.as_ref(),
                )?;
                request.args = serde_json::to_value(frozen).map_err(|_| {
                    Error::new("internal", "frozen project Run could not be encoded")
                })?;
                self.prepare_project_rule_submission(request, registration)
                    .await
            }
            None => {
                self.prepare_owner_rule_run_submission(request, context)
                    .await
            }
        }
    }

    pub(crate) async fn prepare_project_rule_submission(
        &self,
        request: SubmitCommand,
        registration: &ProjectRegistrationView,
    ) -> Result<PreparedSubmission> {
        self.prepare_project_rule_submissions(vec![request], registration)
            .await?
            .pop()
            .ok_or_else(|| Error::new("internal", "project preparation omitted its command"))
    }

    pub(crate) async fn prepare_project_rule_submissions(
        &self,
        requests: Vec<SubmitCommand>,
        registration: &ProjectRegistrationView,
    ) -> Result<Vec<PreparedSubmission>> {
        registration.validate()?;
        crate::automation::verify_project_registration(&self.inner.data_root, registration).await?;
        let principal = Principal::ProjectRule {
            registration: Box::new(registration.clone()),
        };
        let mut prepared = Vec::with_capacity(requests.len());
        for request in requests {
            let prepared_request = if request.verb == "git.integrate" {
                self.prepare_git_integrate_submission(request, &principal, Some(registration))
                    .await?
            } else if request.verb == "git.fetch" {
                self.prepare_git_fetch_submission(request, &principal, Some(registration))
                    .await?
            } else if request.verb == "git.publish" {
                self.prepare_git_publish_submission(request, &principal, Some(registration))
                    .await?
            } else {
                self.prepare_submission(request, &principal).await?
            };
            validate_rule_launch_preparation(&prepared_request)?;
            validate_rule_resume_preparation(&prepared_request, Some(registration))?;
            if let (Some(target), Some(generation)) = (
                prepared_request.request.target.as_deref(),
                prepared_request.request.expected_generation,
            ) {
                if prepared_request.request.verb == "resume" {
                    self.verify_project_rule_resume_target(registration, target, generation)
                        .await?;
                } else {
                    self.verify_project_rule_target(registration, target, generation)
                        .await?;
                }
            }
            prepared.push(prepared_request);
        }
        Ok(prepared)
    }

    pub(crate) async fn verify_project_rule_submission_authority(
        &self,
        requests: &[SubmitCommand],
        registration: &ProjectRegistrationView,
    ) -> Result<()> {
        registration.validate()?;
        crate::automation::verify_project_registration(&self.inner.data_root, registration).await?;
        let principal = Principal::ProjectRule {
            registration: Box::new(registration.clone()),
        };
        for request in requests {
            let mut authorized = request.clone();
            authorize_command(&principal, &mut authorized)?;
        }
        Ok(())
    }

    /// Prepare ordinary action payload after catch-up has already preflighted every
    /// project authority and frozen workspace binding in the request.
    pub(crate) async fn prepare_project_rule_submission_after_authority(
        &self,
        request: SubmitCommand,
        registration: &ProjectRegistrationView,
    ) -> Result<PreparedSubmission> {
        if request.verb == "git.integrate" {
            let principal = Principal::ProjectRule {
                registration: Box::new(registration.clone()),
            };
            return self
                .prepare_git_integrate_submission(request, &principal, Some(registration))
                .await;
        }
        if request.verb == "git.fetch" {
            let principal = Principal::ProjectRule {
                registration: Box::new(registration.clone()),
            };
            return self
                .prepare_git_fetch_submission(request, &principal, Some(registration))
                .await;
        }
        if request.verb == "git.publish" {
            let principal = Principal::ProjectRule {
                registration: Box::new(registration.clone()),
            };
            return self
                .prepare_git_publish_submission(request, &principal, Some(registration))
                .await;
        }
        validate_rule_launch_action(&request, Some(registration))?;
        validate_rule_resume_action(&request, Some(registration))?;
        let principal = Principal::ProjectRule {
            registration: Box::new(registration.clone()),
        };
        let prepared = self
            .prepare_submission_after_authorization(request, &principal, true, false)
            .await?;
        validate_rule_launch_preparation(&prepared)?;
        validate_rule_resume_preparation(&prepared, Some(registration))?;
        if prepared.request.verb == "resume"
            && let (Some(target), Some(generation)) = (
                prepared.request.target.as_deref(),
                prepared.request.expected_generation,
            )
        {
            self.verify_project_rule_resume_target(registration, target, generation)
                .await?;
        }
        Ok(prepared)
    }

    async fn verify_project_rule_target(
        &self,
        registration: &ProjectRegistrationView,
        target: &str,
        generation: i64,
    ) -> Result<()> {
        let Some((agent, workspace)) = self
            .project_rule_target_snapshot(target, generation)
            .await?
        else {
            return Err(Error::new(
                "stale_generation",
                "project action generation is stale",
            ));
        };
        self.verify_project_rule_target_workspace(
            registration,
            &agent,
            workspace.as_ref(),
            generation,
        )
        .await
    }

    async fn verify_project_rule_resume_target(
        &self,
        registration: &ProjectRegistrationView,
        target: &str,
        generation: i64,
    ) -> Result<()> {
        let Some((agent, workspace)) = self
            .project_rule_target_snapshot(target, generation)
            .await?
        else {
            return Err(Error::new(
                "stale_generation",
                "project resume generation is stale",
            ));
        };
        self.verify_project_rule_target_workspace(
            registration,
            &agent,
            workspace.as_ref(),
            generation,
        )
        .await?;
        validate_project_rule_resume_profile(registration, &agent)
    }

    async fn verify_project_rule_resume_after_admission(
        &self,
        registration: &ProjectRegistrationView,
        command: &Command,
    ) -> Result<()> {
        registration.validate()?;
        let target = command
            .target
            .as_deref()
            .ok_or_else(|| Error::new("invalid", "project resume has no Agent target"))?;
        let retained_generation = command
            .generation
            .ok_or_else(|| Error::new("invalid", "project resume has no retained generation"))?;
        let Some((agent, workspace)) = self
            .project_rule_target_snapshot(target, retained_generation)
            .await?
        else {
            return Err(Error::new(
                "stale_generation",
                "project resume no longer targets its retained Agent generation",
            ));
        };
        validate_project_rule_resume_profile(registration, &agent)?;
        match workspace {
            Some(workspace) => {
                crate::automation::verify_managed_project_resume_workspace(
                    registration,
                    &workspace,
                    &agent,
                    command,
                )
                .await
            }
            None => {
                self.verify_project_rule_target_workspace(
                    registration,
                    &agent,
                    None,
                    retained_generation,
                )
                .await
            }
        }
    }

    pub(crate) async fn verify_project_rule_target_scope(
        &self,
        registration: &ProjectRegistrationView,
        target: &str,
        generation: i64,
    ) -> Result<bool> {
        let Some((agent, workspace)) = self
            .project_rule_target_snapshot(target, generation)
            .await?
        else {
            return Ok(false);
        };
        self.verify_project_rule_target_workspace(
            registration,
            &agent,
            workspace.as_ref(),
            generation,
        )
        .await?;
        Ok(true)
    }

    async fn project_rule_target_snapshot(
        &self,
        target: &str,
        expected_generation: i64,
    ) -> Result<Option<(eliot_core::Agent, Option<eliot_core::WorkspacePreparation>)>> {
        let store = self.inner.store.lock().await;
        let agent = store
            .get_agent(target)?
            .ok_or_else(|| Error::new("not_found", "project action Agent was not found"))?;
        if expected_generation != agent.generation {
            return Ok(None);
        }
        let workspace = store.get_workspace(target)?;
        Ok(Some((agent, workspace)))
    }

    async fn verify_project_rule_target_workspace(
        &self,
        registration: &ProjectRegistrationView,
        agent: &eliot_core::Agent,
        workspace: Option<&eliot_core::WorkspacePreparation>,
        generation: i64,
    ) -> Result<()> {
        let target_workspace = agent
            .workspace
            .as_deref()
            .ok_or_else(|| Error::new("not_allowed", "project action Agent has no workspace"))?;
        if let Some(workspace) = workspace {
            crate::automation::verify_managed_project_workspace(
                registration,
                workspace,
                target_workspace,
                generation,
            )
            .await
        } else if target_workspace == registration.checkout {
            crate::automation::verify_project_registration(&self.inner.data_root, registration)
                .await
        } else {
            Err(Error::new(
                "not_allowed",
                "project action workspace is not the registered checkout",
            ))
        }
    }

    pub(crate) async fn verify_project_rule_submission_target_generation(
        &self,
        request: &SubmitCommand,
    ) -> Result<()> {
        let target = request
            .target
            .as_deref()
            .ok_or_else(|| Error::new("invalid", "project action has no Agent target"))?;
        let expected_generation = request
            .expected_generation
            .ok_or_else(|| Error::new("invalid", "project action has no expected generation"))?;
        let store = self.inner.store.lock().await;
        let agent = store
            .get_agent(target)?
            .ok_or_else(|| Error::new("not_found", "project action Agent was not found"))?;
        if agent.generation != expected_generation {
            return Err(Error::new(
                "stale_generation",
                "project action generation is stale",
            ));
        }
        if agent.observe_only {
            return Err(Error::new(
                "not_allowed",
                "native control is unavailable for an observe-only Agent",
            ));
        }
        Ok(())
    }

    async fn prepare_submission(
        &self,
        mut request: SubmitCommand,
        principal: &Principal,
    ) -> Result<PreparedSubmission> {
        authorize_command(principal, &mut request)?;
        self.prepare_submission_after_authorization(request, principal, false, false)
            .await
    }

    async fn authorize_submission_scope(
        &self,
        request: &mut SubmitCommand,
        principal: &Principal,
    ) -> Result<()> {
        if is_attachment_verb(request.verb.as_str()) {
            require_owner(principal)?;
        }
        if request.verb == "worktree.remove" {
            require_owner(principal)?;
        }
        let store = self.inner.store.lock().await;
        store.ensure_committed_reads()?;
        hierarchy::authorize_agent_submit_scope(&store, principal, request)
    }

    async fn prepare_submission_after_authorization(
        &self,
        request: SubmitCommand,
        principal: &Principal,
        project_authority_preverified: bool,
        submission_scope_preverified: bool,
    ) -> Result<PreparedSubmission> {
        self.prepare_submission_with_routing(
            request,
            principal,
            project_authority_preverified,
            submission_scope_preverified,
            routing::LaunchRouting::default(),
            false,
        )
        .await
    }

    async fn prepare_submission_with_routing(
        &self,
        mut request: SubmitCommand,
        principal: &Principal,
        project_authority_preverified: bool,
        submission_scope_preverified: bool,
        routing: routing::LaunchRouting<'_>,
        defer_launch_goal: bool,
    ) -> Result<PreparedSubmission> {
        if request
            .args
            .get(eliot_core::PROVIDER_PROBE_LAUNCH_ARG)
            .is_some()
        {
            return Err(Error::new(
                "not_allowed",
                "provider probe launch metadata is daemon-owned",
            ));
        }
        if request.probe && request.verb != "send" {
            return Err(Error::new(
                "unsupported",
                "only an explicit send may probe a provider hold",
            ));
        }
        if !submission_scope_preverified {
            self.authorize_submission_scope(&mut request, principal)
                .await?;
        }
        if !project_authority_preverified {
            self.verify_native_control_target(&request).await?;
        }
        self.check_safe_point_steer_target(&request).await?;
        self.check_command_admission_mode(&request.verb)?;
        if request.verb == "detach" {
            self.hydrate_detach_generation(&mut request).await?;
        }
        let mut workspace = None;
        if request.verb == "launch" {
            reject_launch_derived_keys(&request.args)?;
            if self.hydrate_retained_launch(&mut request).await? {
                workspace = retained_preparation(&request.args)?;
            } else {
                let frozen = async {
                    {
                        let store = self.inner.store.lock().await;
                        hierarchy::authorize_agent_submit(&store, principal)?;
                        hierarchy::default_child_workspace(&store, &mut request)?;
                    }
                    let chat = match request.args.get("chat") {
                        None => false,
                        Some(Value::Bool(value)) => *value,
                        _ => return Err(Error::new("invalid", "launch chat must be a boolean")),
                    };
                    if chat {
                        require_owner(principal)?;
                        if ["workspace", "worktree", "base", "parent", "project"]
                            .iter()
                            .any(|key| request.args.get(*key).is_some())
                        {
                            return Err(Error::new(
                                "invalid",
                                "chat launch omits workspace, project, parent and worktree",
                            ));
                        }
                        let configured = crate::config::load_daemon_config(&self.inner.data_root)?;
                        let root = crate::config::validate_chats_location(
                            &self.inner.data_root,
                            configured.chats_root.as_deref(),
                        )?;
                        let root = cleanup::private_chat_root(&root)?;
                        let data = std::fs::canonicalize(&self.inner.data_root)
                            .map_err(|_| Error::new("unavailable", "data root unavailable"))?;
                        if root.starts_with(&data) || data.starts_with(&root) {
                            return Err(Error::new(
                                "invalid",
                                "chats_root must be outside the daemon data directory",
                            ));
                        }
                        request.args["workspace"] = json!(root.to_string_lossy());
                    }
                    normalize_launch(&mut request, principal)?;
                    let parent_workspace_shared = {
                        let store = self.inner.store.lock().await;
                        hierarchy::inherit_launch_project_scope(&store, &mut request)?
                    };
                    let resolve_project = !chat && request.args.get("parent").is_none();
                    let input = self
                        .freeze_launch_admission(
                            principal,
                            &mut request,
                            resolve_project,
                            parent_workspace_shared,
                            routing,
                        )
                        .await?;
                    if !defer_launch_goal {
                        project_checks::freeze_launch_goal(self, &mut request, principal).await?;
                    }
                    Ok(input)
                }
                .await;
                workspace = match frozen {
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
                && let Err(error) = freeze_attach_profile(
                    &self.inner.profile_cache,
                    &self.inner.data_root,
                    &mut request,
                )
                .await
                && !self.hydrate_retained_attach(&mut request).await?
            {
                return Err(error);
            }
        } else if request.verb == "resume" {
            normalize_resume(&mut request)?;
        } else if matches!(
            request.verb.as_str(),
            "send" | "steer" | "inject" | "interrupt" | "approve" | "deny" | "answer"
        ) {
            validate_plugin_command_args(&request)?;
        }
        // Required sources are read and delivered before the secret check, so
        // their redacted text is checked like any other input text.
        if matches!(request.verb.as_str(), "launch" | "send") {
            let delivered = self.deliver_sources(&mut request).await?;
            if request.verb == "launch" && !delivered.is_empty() {
                request.args["instruction_sources"] = json!(delivered);
            }
        }
        // Emit data and variable values are arbitrary JSON: their object keys
        // can smuggle a known credential past the values-only check. Block
        // keys before admission; never persist or log the offending key.
        let secret_hit = if matches!(
            request.verb.as_str(),
            "notify" | "emit" | "attention.emit" | "mail.send" | "var.set"
        ) {
            contains_known_secret_including_keys(&request.args, &self.inner.secrets)
        } else {
            contains_known_secret(&request.args, &self.inner.secrets)
        };
        if secret_hit {
            return Err(Error::new(
                "secret_in_payload",
                "durable command text contains a known credential",
            ));
        }
        if let Some(input) = &workspace {
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

        self.refresh_model_hold_for_submission(&request, principal)
            .await;
        Ok(PreparedSubmission { request, workspace })
    }

    async fn verify_native_control_target(&self, request: &SubmitCommand) -> Result<()> {
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
        Ok(())
    }

    /// A safe-point steer waits in Store for a hook, so a live runtime that
    /// cannot take it is refused before acceptance (#906). Stale generation
    /// and turn bindings keep their Store errors; direct-RPC steers settle
    /// from their plugin. A read failure here refuses nothing: Store admission
    /// still owns the decision.
    async fn check_safe_point_steer_target(&self, request: &SubmitCommand) -> Result<()> {
        if request.verb != "steer" {
            return Ok(());
        }
        let Some(target) = request.target.as_deref() else {
            return Ok(());
        };
        let agent = {
            let store = self.inner.store.lock().await;
            if !matches!(store.get_command(&request.id), Ok(None)) {
                return Ok(());
            }
            let Ok(Some(agent)) = store.get_agent(target) else {
                return Ok(());
            };
            agent
        };
        if request
            .expected_generation
            .is_none_or(|expected| expected == agent.generation)
            && matches!(agent.state, AgentState::Busy | AgentState::Waiting)
            && agent.active_turn.as_deref().is_some_and(|turn| {
                request
                    .expected_turn_id
                    .as_deref()
                    .is_none_or(|expected| expected == turn)
            })
        {
            self.inner.plugin_host.check_steer_target(&agent)?;
        }
        Ok(())
    }

    async fn hydrate_retained_launch(&self, request: &mut SubmitCommand) -> Result<bool> {
        let store = self.inner.store.lock().await;
        store.ensure_committed_reads()?;
        if let Some(retained) = retained_launch_profile(&store, request)? {
            if request.args.get("chat") == Some(&Value::Bool(true))
                && request.args.get("workspace").is_none()
            {
                if let Some(workspace) = retained.workspace.clone() {
                    request.args["workspace"] = workspace;
                }
                request.args["project"] = json!("");
            }
            align_retained_workspace_alias(request, retained.workspace.as_ref());
            install_retained_profile(request, retained)?;
            return Ok(true);
        }
        Ok(false)
    }

    async fn hydrate_retained_attach(&self, request: &mut SubmitCommand) -> Result<bool> {
        let store = self.inner.store.lock().await;
        store.ensure_committed_reads()?;
        install_retained_attach_profile(&store, request)
    }

    async fn hydrate_detach_generation(&self, request: &mut SubmitCommand) -> Result<()> {
        let store = self.inner.store.lock().await;
        store.ensure_committed_reads()?;
        Self::hydrate_detach_generation_from_store(&store, request)
    }

    fn hydrate_detach_generation_from_store(
        store: &eliot_store::Store,
        request: &mut SubmitCommand,
    ) -> Result<()> {
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

    /// Whether a top-level launch gets its own prepared worktree without any
    /// flag: Owner launches in a registered project whose `workspace` setting
    /// is not `shared` (the default for registered projects). Workers keep
    /// sharing their parent's worktree; chats never mint one.
    async fn auto_worktree_for_top_level(
        &self,
        principal: &Principal,
        request: &SubmitCommand,
    ) -> Result<bool> {
        if !matches!(principal, Principal::Owner) {
            return Ok(false);
        }
        if request.args.get("parent").is_some() {
            return Ok(false);
        }
        if request.args.get("chat") == Some(&Value::Bool(true)) {
            return Ok(false);
        }
        let Some(project) = request
            .args
            .get("registered_project")
            .and_then(Value::as_str)
        else {
            return Ok(false);
        };
        let mode = crate::config::load_project_registration_config(&self.inner.data_root)
            .map(|registrations| {
                registrations
                    .into_iter()
                    .find(|registration| registration.id == project)
                    .map(|registration| registration.workspace)
            })
            .unwrap_or(None);
        if matches!(mode, Some(crate::config::ProjectWorkspaceMode::Shared)) {
            return Ok(false);
        }
        // Auto preparation needs the exact repository root as its source: a
        // subdirectory launch keeps its shared workspace instead of failing
        // the freeze.
        let workspace = required_text(&request.args, "workspace")?;
        let identity = crate::workspace_git::WorkspaceGit::new()?
            .repository_identity(workspace)
            .await;
        Ok(identity.is_ok_and(|(root, _)| root == workspace))
    }

    async fn freeze_launch_admission(
        &self,
        principal: &Principal,
        request: &mut SubmitCommand,
        resolve_project: bool,
        parent_workspace_shared: bool,
        routing: routing::LaunchRouting<'_>,
    ) -> Result<Option<WorkspacePreparationInput>> {
        if matches!(principal, Principal::Agent { .. })
            && request.args.get("base").is_some()
            && request.args.get("worktree").is_none()
        {
            return Err(Error::new("invalid", "base requires worktree"));
        }
        if self
            .freeze_launch_profile(principal, request, routing)
            .await?
        {
            self.freeze_project_command_guards(request).await?;
            return retained_preparation(&request.args);
        }
        let profile = required_text(&request.args, "profile")?;
        self.refresh_model_hold(profile, &request.args["profile_snapshot"])
            .await;
        // A switch that requests neither a workspace nor a branch keeps its
        // source checkout: reuse was selected before preparation, so no
        // worktree is minted even under the automatic policy.
        if request
            .args
            .as_object_mut()
            .and_then(|args| args.remove("switch_reuse_source_checkout"))
            .is_some()
        {
            self.freeze_project_command_guards(request).await?;
            return Ok(None);
        }
        let mut registered_checkout = None;
        if resolve_project {
            let workspace = std::path::Path::new(required_text(&request.args, "workspace")?);
            let registration = if matches!(principal, Principal::Owner) {
                crate::bootstrap::discover_project(
                    &self.inner.data_root,
                    &self.inner.store,
                    workspace,
                    &self.secret_policy_snapshot(),
                )
                .await?
                .project
            } else {
                match crate::bootstrap::resolve_registered_project(&self.inner.data_root, workspace)
                    .await?
                {
                    Some(project) => {
                        let store = self.inner.store.lock().await;
                        store.get_project_registration("owner", &project)?
                    }
                    None => None,
                }
            };
            if let Some(project) = registration {
                request.args["registered_project"] = json!(project.id);
                registered_checkout = Some(project.checkout);
            }
        } else if let Some(project) = request
            .args
            .get("registered_project")
            .and_then(Value::as_str)
            .filter(|project| !project.trim().is_empty())
        {
            let store = self.inner.store.lock().await;
            registered_checkout = store
                .get_project_registration("owner", project)?
                .map(|registration| registration.checkout);
        }
        self.freeze_project_command_guards(request).await?;
        // A requested branch name always wins, but even an explicit branch
        // lands in the standard directory. A bare `--worktree` (an empty
        // value) or a top-level launch in a `worktree`-mode project generates
        // the standard branch instead. Agent children opt in explicitly.
        let requested = request
            .args
            .get("worktree")
            .and_then(Value::as_str)
            .filter(|branch| !branch.is_empty())
            .map(str::to_owned);
        // A bare `--worktree` carries an empty value and still counts as an
        // explicit preparation request: even a `shared` project prepares a
        // standard worktree when the flag is present.
        let explicit_worktree = request
            .args
            .get("worktree")
            .and_then(Value::as_str)
            .is_some();
        if !explicit_worktree
            && request.args.get("auto_worktree").is_none()
            && !self.auto_worktree_for_top_level(principal, request).await?
        {
            if !parent_workspace_shared
                && request
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
        }
        let (project_slug, task_slug) = generated_naming(&self.inner.data_root, request).await?;
        if contains_known_secret(&request.args, &self.inner.secrets) {
            return Err(Error::new(
                "secret_in_payload",
                "durable launch contains a known credential",
            ));
        }
        let snapshot = request
            .args
            .get("profile_snapshot")
            .ok_or_else(|| Error::new("internal", "launch profile was not frozen"))?;
        let base = request.args.get("base").and_then(Value::as_str);
        // When launching or switching from a retained linked worktree of a
        // registered project, resolve the base commit from that worktree while
        // freezing the new worktree against the project's registered checkout.
        let retained_workspace = required_text(&request.args, "workspace")?;
        let child_worktree = matches!(principal, Principal::Agent { .. }) && explicit_worktree;
        if child_worktree && registered_checkout.is_none() {
            registered_checkout = self
                .inner
                .store
                .lock()
                .await
                .get_workspace_by_path(retained_workspace)?
                .map(|workspace| workspace.repo_path);
        }
        let freeze_repo = registered_checkout
            .as_deref()
            .filter(|checkout| !checkout.trim().is_empty())
            .unwrap_or(retained_workspace);
        let base_repo = (freeze_repo != retained_workspace).then_some(retained_workspace);
        let input = self
            .inner
            .workspaces
            .freeze_launch(
                freeze_repo,
                requested.as_deref(),
                base,
                base_repo,
                child_worktree,
                &project_slug,
                &task_slug,
                snapshot,
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

    /// Freeze the daemon-owned launch default for a Codex or Muse snapshot
    /// that omits its model: resolve the native default through the
    /// existing catalogue reader (cache hit or live read), then retain it
    /// separately from the caller's omitted request, so the plugins
    /// translate an already-decided launch. `requested.model` stays null;
    /// the effective model is the frozen `daemon_model` (Codex) or the
    /// server default (Muse). Muse defers to the server default, so only
    /// an empty catalogue refuses there. A live-read failure leaves the
    /// snapshot untouched for the plugin's native read, so passive freeze
    /// work can never fail a launch its readers could serve. Explicit
    /// models, routed Codex profiles (the Responses route requires an
    /// explicit model id) and other harnesses pass through untouched.
    async fn resolve_launch_default(
        &self,
        harness: &str,
        cache_label: &str,
        snapshot: &mut Value,
        deadline: std::time::Instant,
    ) -> Result<()> {
        if !HarnessCapabilities::of(harness).launch_default_resolution {
            return Ok(());
        }
        // Routed Codex profiles keep their own rule: the Responses route
        // requires an explicit model id, enforced by the plugin at launch.
        if snapshot.get("route_binding").is_some_and(Value::is_object) {
            return Ok(());
        }
        let omitted = snapshot
            .get("model")
            .and_then(Value::as_str)
            .is_none_or(|model| model.trim().is_empty());
        if !omitted {
            return Ok(());
        }
        if snapshot
            .get("daemon_model")
            .and_then(Value::as_str)
            .is_some_and(|model| !model.trim().is_empty())
        {
            return Ok(());
        }
        // The existing reader decides (cache hit or live native read) under
        // the canonical executable/version/route key, so a changed `cmd`
        // never reuses a stale entry. Ad-hoc launches share the catalogue's
        // `adhoc` label, never `adhoc-<harness>`.
        let response = match self
            .models_for_snapshot(
                harness,
                Some(snapshot.clone()),
                cache_label,
                None,
                None,
                deadline,
            )
            .await
        {
            Ok(response) => response,
            Err(_) => return Ok(()),
        };
        if let Some(model) = catalog::launch_model_default(harness, &response)? {
            snapshot["daemon_model"] = Value::String(model);
        }
        Ok(())
    }

    async fn freeze_project_command_guards(&self, request: &mut SubmitCommand) -> Result<()> {
        if request
            .args
            .get("profile_snapshot")
            .and_then(|snapshot| snapshot.get(crate::command_guard::SNAPSHOT_FIELD))
            .is_some()
        {
            return Ok(());
        }
        let project_id = request
            .args
            .get("registered_project")
            .and_then(Value::as_str)
            .filter(|project| !project.trim().is_empty())
            .map(str::to_owned);
        let guards = if let Some(project_id) = project_id {
            let registrations =
                crate::config::load_project_registration_config(&self.inner.data_root)?;
            let registration = registrations
                .iter()
                .find(|registration| registration.id == project_id)
                .ok_or_else(|| {
                    Error::new(
                        "not_found",
                        "registered project command guard policy is unavailable",
                    )
                })?;
            serde_json::to_value(&registration.command_guards)
                .map_err(|_| Error::new("internal", "project command guards could not be frozen"))?
        } else if let Some(parent_id) = request.args.get("parent").and_then(Value::as_str) {
            let store = self.inner.store.lock().await;
            let parent = store
                .get_agent(parent_id)?
                .ok_or_else(|| Error::new("not_found", "launch parent was not found"))?;
            parent
                .profile_snapshot
                .get(crate::command_guard::SNAPSHOT_FIELD)
                .cloned()
                .unwrap_or_else(|| Value::Array(Vec::new()))
        } else {
            request
                .args
                .get("profile_snapshot")
                .and_then(|snapshot| snapshot.get(crate::command_guard::SNAPSHOT_FIELD))
                .cloned()
                .unwrap_or_else(|| Value::Array(Vec::new()))
        };
        let snapshot = request
            .args
            .get_mut("profile_snapshot")
            .and_then(Value::as_object_mut)
            .ok_or_else(|| Error::new("internal", "launch profile snapshot is not an object"))?;
        if guards.as_array().is_some_and(|guards| !guards.is_empty()) {
            snapshot.insert(crate::command_guard::SNAPSHOT_FIELD.to_owned(), guards);
        }
        Ok(())
    }

    async fn freeze_launch_profile(
        &self,
        principal: &Principal,
        request: &mut SubmitCommand,
        routing: routing::LaunchRouting<'_>,
    ) -> Result<bool> {
        if self.hydrate_retained_launch(request).await? {
            return Ok(true);
        }

        // Ad-hoc Owner launch: freeze a profile-equivalent snapshot from
        // built-in per-harness defaults plus flags. Agent principals keep
        // the profile and child_profiles rules; they cannot ad-hoc launch.
        if request.args.get("harness").is_some() {
            if !matches!(principal, Principal::Owner) {
                return Err(Error::new(
                    "not_allowed",
                    "ad-hoc launch requires the Owner principal",
                ));
            }
            let harness = request
                .args
                .get("harness")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    Error::new("invalid", "launch harness must be a non-empty string")
                })?;
            let harness = crate::adhoc::validate_adhoc_harness(harness)?;
            let model = request
                .args
                .get("model")
                .and_then(Value::as_str)
                .map(str::to_owned);
            let effort = request
                .args
                .get("effort")
                .and_then(Value::as_str)
                .map(str::to_owned);
            if let Some(auth) = request.args.get("auth")
                && !auth.is_string()
            {
                return Err(Error::new(
                    "invalid",
                    "launch auth must be an integration selector string",
                ));
            }
            let auth = request
                .args
                .get("auth")
                .and_then(Value::as_str)
                .map(str::to_owned);
            if let Some(cmd) = request.args.get("cmd")
                && !cmd.is_string()
            {
                return Err(Error::new(
                    "invalid",
                    "launch cmd must be an executable path string",
                ));
            }
            if let Some(access) = request.args.get("access")
                && !access.is_string()
            {
                return Err(Error::new(
                    "invalid",
                    "launch access must be ask, auto or full",
                ));
            }
            let access = request
                .args
                .get("access")
                .and_then(Value::as_str)
                .map(str::to_owned);
            let explicit_cmd = request
                .args
                .get("cmd")
                .and_then(Value::as_str)
                .map(str::to_owned);
            // An explicit `--cmd` pins the native executable (e.g. a Muse
            // binary version the launcher no longer marks current);
            // otherwise the installed executable is discovered on the
            // blocking pool. Shape is validated with the frozen snapshot
            // below.
            let profile_name = crate::adhoc::adhoc_profile_name(&harness);
            let (cmd, selected_version) = match explicit_cmd.as_deref() {
                Some(cmd) if harness == "muse" => {
                    let cmd = crate::adhoc::validate_native_cmd(&profile_name, cmd)?;
                    let deadline = std::time::Instant::now() + crate::adhoc::DISCOVERY_BUDGET;
                    let (cmd, version) =
                        crate::adhoc::reader_executable(&cmd, &harness, deadline).await?;
                    (Some(cmd), version)
                }
                Some(_) if harness != "claude" => (explicit_cmd, None),
                selector => {
                    let candidate =
                        crate::adhoc::resolve_profile_command(&profile_name, &harness, selector)
                            .await?;
                    (Some(candidate.cmd), candidate.version)
                }
            };
            let deadline = std::time::Instant::now() + crate::adhoc::DISCOVERY_BUDGET;
            let mut snapshot = crate::adhoc::blocking_discovery(deadline, move || {
                crate::adhoc::adhoc_snapshot(
                    &harness,
                    model.as_deref(),
                    effort.as_deref(),
                    cmd,
                    crate::adhoc::AdhocAuth::Resolve(auth.as_deref()),
                    access.as_deref(),
                )
            })
            .await??;
            crate::config::stamp_cloud_mcp_hostname(&mut snapshot, &self.inner.data_root);
            crate::config::capture_native_store_inputs(&mut snapshot, |name| {
                std::env::var_os(name)
            })?;
            crate::config::capture_build_inputs(&mut snapshot, |name| std::env::var_os(name))?;
            let harness = snapshot
                .get("harness")
                .and_then(Value::as_str)
                .filter(|harness| !harness.trim().is_empty())
                .ok_or_else(|| Error::new("invalid", "resolved profile must name its harness"))?
                .to_owned();
            if let Some(version) = selected_version {
                snapshot["cmd_version"] = Value::String(version);
            }
            let profile_name = crate::adhoc::adhoc_profile_name(&harness);
            // Keep the requested harness/model/effort/cmd/auth/access beside
            // the frozen snapshot: same-ID retries bind the exact requested
            // payload (a changed model is id_conflict), while the frozen
            // snapshot stays the replay-stable source for dispatch.
            if let Some(args) = request.args.as_object_mut() {
                args.insert("profile".to_owned(), Value::String(profile_name.clone()));
            }
            // The catalogue caches ad-hoc reads under `adhoc`, never
            // `adhoc-<harness>`: the freeze shares that key.
            self.resolve_launch_default(&harness, "adhoc", &mut snapshot, deadline)
                .await?;
            install_frozen_profile(request, &harness, snapshot, true)?;
            {
                let store = self.inner.store.lock().await;
                hierarchy::check_agent_launch_authority(&store, principal, request)?;
            }
            return Ok(false);
        }

        if request.args.get("profile").is_none() {
            let (name, harness, mut snapshot) = self
                .route_launch_profile(principal, request, routing)
                .await?;
            request.args["profile"] = Value::String(name.clone());
            let deadline = std::time::Instant::now() + crate::adhoc::DISCOVERY_BUDGET;
            self.resolve_launch_default(&harness, &name, &mut snapshot, deadline)
                .await?;
            install_frozen_profile(request, &harness, snapshot, true)?;
            let store = self.inner.store.lock().await;
            hierarchy::check_agent_launch_authority(&store, principal, request)?;
            return Ok(false);
        }

        // Agent-principal child launches default to the parent profile and
        // reuse the parent's frozen snapshot verbatim (including the
        // dispatch-derived `native_store_binding` the parent gained at its
        // own dispatch, which a fresh resolve would lack). A named profile
        // that differs from the parent's must be in the parent snapshot's
        // captured `child_profiles`; the captured list governs, so a later
        // config edit cannot widen an already-running manager. The allowed
        // child profile resolves now, before any native effect, and must use
        // its own harness policy. Same-harness children retain the existing
        // credential and model-only confinement; cross-harness children use
        // the Owner-approved profile's bindings and inherit capacity bounds.
        let agent_child_snapshot = if matches!(principal, Principal::Agent { .. })
            && request.args.get("parent").is_some()
        {
            let parent_id = request
                .args
                .get("parent")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::new("invalid", "launch parent must be an Agent id"))?;
            let parent = {
                let store = self.inner.store.lock().await;
                store.get_agent(parent_id)?.ok_or_else(|| {
                    Error::new("not_found", format!("Agent {parent_id} was not found"))
                })?
            };
            if !parent.profile_snapshot.is_object() {
                return Err(Error::new(
                    "bad_state",
                    "retained parent profile snapshot is malformed",
                ));
            }
            let requested = request.args.get("profile").and_then(Value::as_str);
            if requested.is_some_and(|name| name != parent.profile) {
                let name =
                    requested.ok_or_else(|| Error::new("internal", "launch profile vanished"))?;
                let allowed = parent
                    .profile_snapshot
                    .get("child_profiles")
                    .and_then(Value::as_array)
                    .is_some_and(|allowed| {
                        allowed.iter().any(|entry| entry.as_str() == Some(name))
                    });
                if !allowed {
                    return Err(Error::new(
                        "not_allowed",
                        format!(
                            "Agent children of {parent_id} may only use the parent profile or its frozen child_profiles"
                        ),
                    ));
                }
                let mut snapshot = self.resolve_profile_cached(name)?;
                crate::config::capture_native_store_inputs(&mut snapshot, |name| {
                    std::env::var_os(name)
                })?;
                crate::config::capture_build_inputs(&mut snapshot, |name| std::env::var_os(name))?;
                let harness = snapshot
                    .get("harness")
                    .and_then(Value::as_str)
                    .filter(|harness| !harness.trim().is_empty())
                    .ok_or_else(|| Error::new("invalid", "resolved profile must name its harness"))?
                    .to_owned();
                hierarchy::inherit_child_launch_bounds(&mut snapshot, &parent)?;
                let command_bound = harness == parent.harness;
                if command_bound {
                    crate::adhoc::inherit_profile_command(
                        &mut snapshot,
                        &parent.profile_snapshot,
                        &harness,
                    )?;
                }
                Some((name.to_owned(), harness, snapshot, command_bound))
            } else {
                Some((
                    parent.profile.clone(),
                    parent.harness.clone(),
                    parent.profile_snapshot.clone(),
                    true,
                ))
            }
        } else {
            None
        };
        let (profile_name, mut snapshot, harness, command_bound) = match agent_child_snapshot {
            Some((name, harness, snapshot, bound)) => (name, snapshot, harness, bound),
            None => {
                let profile_name = match request.args.get("profile") {
                    Some(Value::String(name)) => name.to_owned(),
                    Some(_) => required_text(&request.args, "profile")?.to_owned(),
                    None => required_text(&request.args, "profile")?.to_owned(),
                };
                let mut snapshot = self.resolve_profile_cached(&profile_name)?;
                crate::config::capture_native_store_inputs(&mut snapshot, |name| {
                    std::env::var_os(name)
                })?;
                crate::config::capture_build_inputs(&mut snapshot, |name| std::env::var_os(name))?;
                let harness = snapshot
                    .get("harness")
                    .and_then(Value::as_str)
                    .filter(|harness| !harness.trim().is_empty())
                    .ok_or_else(|| Error::new("invalid", "resolved profile must name its harness"))?
                    .to_owned();
                let command_bound = if let Some(source) = routing.provider_source {
                    crate::adhoc::inherit_profile_command(&mut snapshot, source, &harness)?;
                    if eliot_core::provider_switch_authority(&snapshot)?
                        != eliot_core::provider_switch_authority(source)?
                    {
                        return Err(Error::new(
                            "not_allowed",
                            "profile authority differs from the source generation",
                        ));
                    }
                    true
                } else {
                    false
                };
                (profile_name, snapshot, harness, command_bound)
            }
        };
        // The retained wire shape keeps the named profile so Store admission
        // can bind the child row to the parent's profile without guessing.
        if let Some(args) = request.args.as_object_mut() {
            args.insert("profile".to_owned(), Value::String(profile_name.clone()));
        }
        if matches!(principal, Principal::Agent { .. }) {
            install_frozen_profile(request, &harness, snapshot.clone(), true)?;
            let store = self.inner.store.lock().await;
            hierarchy::check_agent_launch_authority(&store, principal, request)?;
        }
        if command_bound {
            let cmd = crate::adhoc::explicit_profile_command(&snapshot, &harness);
            if harness == "claude" && cmd.is_none() {
                return Err(Error::new(
                    "bad_state",
                    "inherited Claude snapshot has no frozen executable path",
                ));
            }
            if let Some(cmd) = cmd
                && !crate::adhoc::profile_command_exists(cmd)
            {
                return Err(crate::adhoc::missing_profile_command_refusal(
                    &profile_name,
                    &harness,
                    cmd,
                    None,
                ));
            }
        } else {
            let candidate = if let Some(candidate) = routing.harness_check_candidate {
                let version = candidate
                    .version
                    .as_deref()
                    .ok_or_else(|| Error::new("unavailable", "installed version is unavailable"))?;
                if harness_check::check_model_rank(&snapshot).is_none() {
                    return Err(Error::new(
                        "unavailable",
                        "the check profile has no cheap model",
                    ));
                }
                harness_check::configure_check_snapshot(&mut snapshot, &harness, version);
                candidate.clone()
            } else {
                let selector = snapshot.get("cmd").and_then(Value::as_str);
                crate::adhoc::resolve_profile_command(&profile_name, &harness, selector).await?
            };
            crate::adhoc::write_profile_command_snapshot(&mut snapshot, candidate)?;
        }
        // Reused parent snapshots already carry their frozen default; only
        // freshly resolved child snapshots need a freeze. The label matches
        // the catalogue's named-profile key.
        if snapshot
            .get("daemon_model")
            .and_then(Value::as_str)
            .is_none_or(|model| model.trim().is_empty())
        {
            let deadline = std::time::Instant::now() + crate::adhoc::DISCOVERY_BUDGET;
            self.resolve_launch_default(&harness, &profile_name, &mut snapshot, deadline)
                .await?;
        }
        install_frozen_profile(request, &harness, snapshot, true)?;
        {
            let store = self.inner.store.lock().await;
            hierarchy::check_agent_launch_authority(&store, principal, request)?;
        }
        self.check_explicit_launch_quota(
            principal,
            routing,
            &profile_name,
            &harness,
            &request.args["profile_snapshot"],
        )
        .await?;
        Ok(false)
    }

    async fn provider_holds(&self, params: &Value, principal: &Principal) -> Result<Value> {
        require_owner(principal)?;
        let request: ProviderHoldsRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let response = self
            .inner
            .store
            .lock()
            .await
            .list_provider_holds(request.after.as_deref(), request.effective_limit())?;
        serde_json::to_value(response)
            .map_err(|_| Error::new("internal", "provider hold page could not be encoded"))
    }

    async fn provider_clear_hold(&self, params: &Value, principal: &Principal) -> Result<Value> {
        require_owner(principal)?;
        if contains_known_secret_including_keys(params, &self.inner.secrets) {
            return Err(Error::new(
                "secret_in_payload",
                "provider correction contains a known credential",
            ));
        }
        let request: ProviderClearHoldRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let change = {
            let mut store = self.inner.store.lock().await;
            owner_inbox::check_binding(&store)?;
            let change = store.clear_provider_hold("owner", &request, now_ms())?;
            self.publish_locked(&change.events);
            change
        };
        self.signal_progress();
        self.signal_launch_arbitration();
        serde_json::to_value(change.value)
            .map_err(|_| Error::new("internal", "provider correction could not be encoded"))
    }

    async fn project_get(&self, params: &Value, principal: &Principal) -> Result<Value> {
        require_owner(principal)?;
        let request: ProjectGetRequest = eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let registration = {
            let store = self.inner.store.lock().await;
            let mut registration = store
                .get_project_registration("owner", &request.id)?
                .ok_or_else(|| Error::new("not_found", "project registration was not found"))?;
            registration.policy_usage = store.project_policy_usage(&registration.id, now_ms())?;
            registration
        };
        // Git reads run after the Store lock is released.
        let mut registrations = [registration];
        crate::workspace_git::WorkspaceGit::fill_project_readbacks(&mut registrations).await;
        display_project_registration(&mut registrations[0]);
        let [registration] = registrations;
        registration.validate()?;
        serde_json::to_value(registration)
            .map_err(|_| Error::new("internal", "project registration could not be encoded"))
    }

    async fn project_list(&self, params: &Value, principal: &Principal) -> Result<Value> {
        require_owner(principal)?;
        let _: ProjectListRequest = eliot_core::api::discovery::decode_params(params.clone())?;
        let mut registrations = {
            let store = self.inner.store.lock().await;
            let mut registrations = store.list_project_registrations("owner", None, PAGE_MAX)?;
            let now = now_ms();
            let projects: Vec<_> = registrations
                .iter()
                .map(|registration| registration.id.as_str())
                .collect();
            let policy_usage = store.project_policy_usage_for_projects(&projects, now)?;
            for (registration, policy_usage) in registrations.iter_mut().zip(policy_usage) {
                registration.policy_usage = policy_usage;
            }
            registrations
        };
        // Git reads run after the Store lock is released.
        crate::workspace_git::WorkspaceGit::fill_project_readbacks(&mut registrations).await;
        for registration in &mut registrations {
            display_project_registration(registration);
        }
        for registration in &registrations {
            registration.validate()?;
        }
        serde_json::to_value(registrations)
            .map_err(|_| Error::new("internal", "project registrations could not be encoded"))
    }

    /// Owner page of the worktrees Swarm manages, from the retained
    /// workspace preparations (not from Agent project labels).
    async fn workspace_list(&self, params: &Value, principal: &Principal) -> Result<Value> {
        require_owner(principal)?;
        let request: eliot_core::api::WorkspaceListRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let (rows, total, truncated) = self
            .inner
            .store
            .lock()
            .await
            .list_managed_workspaces(request.after.as_deref(), request.limit)?;
        let workspaces: Vec<eliot_core::api::ManagedWorkspaceView> = rows
            .into_iter()
            .map(|row| eliot_core::api::ManagedWorkspaceView {
                agent: row.agent,
                project: row.project,
                repo_path: row.repo_path,
                branch: row.requested_branch,
                worktree_path: row.worktree_path,
                phase: row.phase,
                updated_at: row.updated_at,
            })
            .collect();
        let response = eliot_core::api::WorkspaceListResponse {
            next: workspaces.last().map(|row| row.agent.clone()),
            truncated: truncated && !workspaces.is_empty(),
            total,
            workspaces,
        };
        request.validate_response(&response)?;
        serde_json::to_value(response)
            .map_err(|_| Error::new("internal", "workspace.list could not be encoded"))
    }

    async fn project_discover(&self, params: &Value, principal: &Principal) -> Result<Value> {
        require_owner(principal)?;
        let request: ProjectDiscoverRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let policy = self.secret_policy_snapshot();
        let response = crate::bootstrap::discover_project(
            &self.inner.data_root,
            &self.inner.store,
            std::path::Path::new(&request.workspace),
            &policy,
        )
        .await?;
        request.validate_response(&response)?;
        serde_json::to_value(response)
            .map_err(|_| Error::new("internal", "project discovery could not be encoded"))
    }

    async fn rule_get(&self, params: &Value, principal: &Principal) -> Result<Value> {
        require_owner(principal)?;
        let request: RuleGetRequest = eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let rule_principal = rule_principal(request.project.as_deref());
        let show = {
            let store = self.inner.store.lock().await;
            match store.get_rule_show(&rule_principal, &request.id)? {
                Some(show) => show,
                None if request.project.is_none() && store.rule_project_exists(&request.id)? => {
                    return Err(Error::new(
                        "not_found",
                        "rule is defined in a project. Use --project PROJECT",
                    ));
                }
                None => return Err(Error::new("not_found", "rule was not found")),
            }
        };
        let state = &show.state;
        if state.validate().is_err() || state.id != request.id || state.project != request.project {
            return Err(Error::new(
                "source_integrity",
                "stored rule namespace differs from the request",
            ));
        }
        let mut result = serde_json::to_value(show)
            .map_err(|_| Error::new("internal", "rule state could not be encoded"))?;
        self.redact_native_value(&mut result);
        Ok(result)
    }

    async fn timer_get(&self, params: &Value, principal: &Principal) -> Result<Value> {
        require_owner(principal)?;
        let request: TimerGetRequest = eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let rule_principal = rule_principal(request.project.as_deref());
        let store = self.inner.store.lock().await;
        let state = rules::timer_readback(&store, &rule_principal, &request.id)?
            .ok_or_else(|| Error::new("not_found", "timer was not found"))?;
        request.validate_response(&state)?;
        serde_json::to_value(state)
            .map_err(|_| Error::new("internal", "timer state could not be encoded"))
    }

    pub(crate) fn install_rule_definitions(
        &self,
        principal: &str,
        definitions: &[(i64, RuleDefinition)],
        completion_interests: Vec<crate::automation::CompletionRuleInterest>,
    ) {
        if principal == "owner" {
            let active = definitions
                .iter()
                .filter_map(|(_, definition)| definition.id.strip_prefix("assignment-"))
                .collect();
            self.inner
                .assignment_observations
                .lock_recover()
                .retain_queues(&active);
        }
        let mut stored = self.inner.rule_definitions.lock_recover();
        stored.retain(|(stored_principal, _), _| stored_principal != principal);
        stored.extend(definitions.iter().map(|(revision, definition)| {
            (
                (principal.to_owned(), definition.id.clone()),
                (*revision, definition.clone()),
            )
        }));
        self.inner
            .plugin_host
            .install_completion_interests(principal, completion_interests);
    }

    pub(crate) fn remove_completion_interest(&self, principal: &str, id: &str) {
        self.inner
            .plugin_host
            .remove_completion_interest(principal, id);
    }

    fn rule_definition(&self, principal: &str, id: &str) -> Option<(i64, RuleDefinition)> {
        self.inner
            .rule_definitions
            .lock_recover()
            .get(&(principal.to_owned(), id.to_owned()))
            .cloned()
    }

    pub(crate) fn install_rule_source_files(
        &self,
        principal: &str,
        files: &HashMap<String, String>,
    ) {
        let mut stored = self.inner.rule_source_files.lock_recover();
        if files.is_empty() {
            stored.remove(principal);
        } else if stored.get(principal) != Some(files) {
            stored.insert(principal.to_owned(), files.clone());
        }
    }

    fn rule_source_file(&self, principal: &str, id: &str) -> Option<String> {
        self.inner
            .rule_source_files
            .lock_recover()
            .get(principal)?
            .get(id)
            .cloned()
    }

    pub(crate) async fn initialize_rules(
        &self,
        principal: &str,
        definitions: &[eliot_store::RuleDefinitionInput],
        timers: &[TimerDefinitionInput],
        registration_now: i64,
        absent_from_source: &[&str],
        source_files: &HashMap<String, String>,
    ) -> Result<Vec<RuleProcessingState>> {
        self.validate_rule_definitions(principal, definitions)
            .await?;

        let change = {
            let mut store = self.inner.store.lock().await;
            let change = store.reconcile_rule_definitions(
                principal,
                definitions,
                timers,
                registration_now,
                absent_from_source,
            )?;
            // rule.list holds this Store lock while reading the source map.
            self.install_rule_source_files(principal, source_files);
            self.publish_locked(&change.events);
            change
        };
        if !change.events.is_empty() {
            self.signal_progress();
        }
        self.rule_processing(principal).await
    }

    pub(crate) async fn validate_rule_definitions(
        &self,
        principal: &str,
        definitions: &[eliot_store::RuleDefinitionInput],
    ) -> Result<()> {
        // Check every canonical definition before the first durable seed. Rule
        // text is persisted by Store and must use the same known-secret policy
        // as command semantic data.
        for definition in definitions {
            let value = definition.validated_value()?;
            if contains_known_secret_including_keys(&value, &self.inner.secrets) {
                return Err(Error::new(
                    "secret_in_payload",
                    "rule definition contains a known credential",
                ));
            }
        }
        for state in self.rule_processing(principal).await? {
            for canonical in
                std::iter::once(&state.current_definition).chain(state.retiring_definition.iter())
            {
                let value: Value = serde_json::from_str(canonical).map_err(|_| {
                    Error::new("source_integrity", "stored rule definition is invalid")
                })?;
                if contains_known_secret_including_keys(&value, &self.inner.secrets) {
                    return Err(Error::new(
                        "secret_in_payload",
                        "stored rule definition contains a known credential",
                    ));
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn reconcile_configured_projects(
        &self,
        inputs: &[ProjectRegistrationInput],
        expected_revision: Option<&str>,
    ) -> Result<Vec<ProjectRegistrationView>> {
        let change = {
            let mut store = self.inner.store.lock().await;
            if crate::config::settings::current_revision(&self.inner.data_root)?.as_deref()
                != expected_revision
            {
                return Err(Error::new(
                    "conflict",
                    "project settings changed during reconciliation",
                ));
            }
            let change = store.reconcile_project_registrations("owner", inputs, now_ms())?;
            self.publish_locked(&change.events);
            change
        };
        if !change.events.is_empty() {
            self.signal_progress();
        }
        Ok(change.value)
    }

    pub(crate) async fn rule_processing(
        &self,
        principal: &str,
    ) -> Result<Vec<RuleProcessingState>> {
        let mut after_id = None;
        let mut states = Vec::new();
        loop {
            let page = self.inner.store.lock().await.list_rule_processing_states(
                principal,
                after_id.as_deref(),
                PAGE_MAX,
            )?;
            let page_len = page.len();
            if page_len == 0 {
                break;
            }
            for state in page {
                after_id = Some(state.view.id.clone());
                states.push(state);
            }
            if page_len < PAGE_MAX {
                break;
            }
        }
        Ok(states)
    }

    pub(crate) async fn settle_rule_backlog(
        &self,
        principal: &str,
        state: &RuleStateView,
        through: i64,
        max_events: i64,
        now: i64,
    ) -> Result<RuleProcessingState> {
        let change = {
            let mut store = self.inner.store.lock().await;
            let change = store.settle_rule_backlog(principal, state, through, max_events, now)?;
            self.publish_locked(&change.events);
            change
        };
        if !change.events.is_empty() {
            self.signal_progress();
        }
        Ok(change.value)
    }

    pub(crate) async fn get_rule_timer(
        &self,
        principal: &str,
        id: &str,
    ) -> Result<Option<TimerStateView>> {
        self.inner.store.lock().await.get_timer_state(principal, id)
    }

    pub(crate) async fn list_due_rule_timers(
        &self,
        principal: &str,
        after_id: Option<&str>,
        due_by_ms: i64,
        limit: usize,
    ) -> Result<Vec<TimerStateView>> {
        self.inner
            .store
            .lock()
            .await
            .list_due_timer_states(principal, after_id, due_by_ms, limit)
    }

    pub(crate) async fn replay_rule_timer_slot(
        &self,
        principal: &str,
        state: &TimerStateView,
        scheduled_at_ms: i64,
    ) -> Result<Option<TimerSlotAdmission>> {
        self.inner
            .store
            .lock()
            .await
            .replay_timer_slot(principal, state, scheduled_at_ms)
    }

    pub(crate) async fn commit_rule_timer_slot(
        &self,
        principal: &str,
        project: Option<&ProjectRegistrationView>,
        state: &TimerStateView,
        slot: TimerSlotCommit,
        arbitration: Option<eliot_store::ArbitratedTimer>,
        now: i64,
    ) -> Result<Option<TimerSlotAdmission>> {
        let scheduled_at_ms = slot.scheduled_at_ms;
        let was_arbitrated = arbitration.is_some();
        let admission = {
            let mut store = self.inner.store.lock().await;
            if let Some(admission) = store.replay_timer_slot(principal, state, scheduled_at_ms)? {
                admission
            } else {
                if let Some(action) = slot.action.as_ref() {
                    validate_prepared_rule_launch_command(action)?;
                    validate_rule_resume_action(action, project)?;
                }
                if self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN {
                    return Ok(None);
                }
                if let Some(expected) = project {
                    let current = store
                        .get_project_registration("owner", &expected.id)?
                        .ok_or_else(|| {
                            Error::new("not_allowed", "project registration was removed")
                        })?;
                    if &current != expected {
                        return Err(Error::new(
                            "not_allowed",
                            "project registration changed before timer admission",
                        ));
                    }
                }
                let admission =
                    store.commit_timer_slot_arbitrated(principal, state, slot, arbitration, now)?;
                self.publish_locked(&admission.events);
                admission
            }
        };
        if admission.is_new
            && let Some(child) = &admission.child
        {
            self.schedule_rule_admission(child);
        }
        if admission.is_new {
            self.signal_progress();
            if was_arbitrated {
                self.signal_launch_arbitration();
            }
        }
        Ok(Some(admission))
    }

    pub(crate) fn subscribe_rule_events(&self) -> broadcast::Receiver<Event> {
        self.inner.bus.subscribe()
    }

    pub(crate) async fn rule_high_water(&self) -> Result<i64> {
        self.inner.store.lock().await.high_water()
    }

    pub(crate) async fn read_rule_events(
        &self,
        after: i64,
        through: i64,
        limit: usize,
    ) -> Result<Vec<Event>> {
        self.inner
            .store
            .lock()
            .await
            .read_events(after, limit, Some(through))
    }

    pub(crate) async fn replay_rule_event_action(
        &self,
        principal: &str,
        state: &RuleStateView,
        revision: i64,
        event_seq: i64,
    ) -> Result<Option<eliot_store::RuleEventAdmission>> {
        self.inner
            .store
            .lock()
            .await
            .replay_rule_event_action(principal, state, revision, event_seq)
    }

    pub(crate) fn rule_firings_open(&self) -> bool {
        self.inner.mode.load(Ordering::SeqCst) == MODE_OPEN
    }

    pub(crate) async fn advance_rule_cursor(
        &self,
        principal: &str,
        state: &RuleStateView,
        through: i64,
        outcome: eliot_store::RuleCursorOutcome,
    ) -> Result<RuleStateView> {
        let change = {
            let mut store = self.inner.store.lock().await;
            let change = store.advance_rule_cursor(principal, state, through, outcome, now_ms())?;
            self.publish_locked(&change.events);
            change
        };
        if !change.events.is_empty() {
            self.note_rule_refusals(principal, &change.events);
            self.signal_progress();
        }
        Ok(change.value)
    }

    pub(crate) async fn recover_rule_children(&self, limit: usize) -> Result<()> {
        let mut after = None;
        loop {
            let children = {
                let store = self.inner.store.lock().await;
                store.recover_rule_children(after.as_deref(), limit)?
            };
            if children.is_empty() {
                break;
            }
            for admission in children {
                after = Some(admission.command.id.clone());
                self.schedule_rule_admission(&admission);
            }
        }
        Ok(())
    }

    async fn command_get(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: CommandGetRequest = eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        if let Principal::Run { run_command_id, .. } = principal
            && request.id.as_str() != run_command_id.as_str()
        {
            return Err(Error::new(
                "not_allowed",
                "Run credentials are scoped to their own command",
            ));
        }
        let id = request.id.as_str();
        let mut command = {
            let store = self.inner.store.lock().await;
            let mut command = store
                .get_command(id)?
                .ok_or_else(|| Error::new("not_found", format!("command {id} was not found")))?;
            hierarchy::authorize_command_read(&store, principal, &command)?;
            // Historical configuration from the command's exact retained
            // generation and revision, never the current overlay.
            command.config = store.command_config_view(&command)?;
            command
        };
        self.adjust_command_for_read(&mut command);
        serde_json::to_value(command)
            .map_err(|error| Error::new("internal", format!("encode command: {error}")))
    }

    fn adjust_command_for_read(&self, command: &mut Command) {
        if command.state.is_terminal() {
            self.clear_unknown_id(&command.id);
        } else if command.state == CommandState::Unknown {
            command.turn_id = None;
        } else if let Some(cause) = self.unknown_cause(&command.id) {
            command.state = CommandState::Unknown;
            command.outcome = Some(json!({"code": "unknown", "cause": cause}));
            command.turn_id = None;
        }
        if command.verb == "send"
            && let Some(args) = command.args.as_object_mut()
            && (args.get("inline_digest").and_then(Value::as_bool) == Some(true)
                || (args.get("notice_kind").and_then(Value::as_str) == Some("mail")
                    && args
                        .get("text")
                        .and_then(Value::as_str)
                        .is_some_and(|text| text.starts_with("Eliot completion digest "))))
        {
            args.remove("inline_digest");
            args.insert(
                "text".to_owned(),
                Value::String("[redacted: inline mail]".to_owned()),
            );
            args.insert("redacted".to_owned(), Value::Bool(true));
        }
    }

    async fn agent_discover(&self, params: &Value, principal: &Principal) -> Result<Value> {
        require_owner(principal)?;
        let request: AgentDiscoverRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        if self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN {
            return Err(Error::new(
                "unavailable",
                "daemon is shutting down and rejects new discovery",
            ));
        }
        let mut profile = self.resolve_profile_cached(&request.profile)?;
        let harness = profile
            .get("harness")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::new("profile_error", "resolved profile has no harness"))?;
        if request
            .harness
            .as_deref()
            .is_some_and(|name| name != harness)
        {
            return Err(Error::new(
                "invalid",
                "harness does not match the selected profile",
            ));
        }
        let candidate = crate::adhoc::resolve_profile_command(
            &request.profile,
            harness,
            profile.get("cmd").and_then(Value::as_str),
        )
        .await?;
        crate::adhoc::write_profile_command_snapshot(&mut profile, candidate)?;
        crate::config::capture_native_store_inputs(&mut profile, |name| std::env::var_os(name))?;
        if self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN {
            return Err(Error::new(
                "unavailable",
                "daemon is shutting down and rejects new discovery",
            ));
        }
        let raw = self
            .inner
            .plugin_host
            .discover(
                profile,
                request.native_scope,
                request.cursor,
                request.archived,
                request.limit,
            )
            .await?;
        let page: DiscoverPage = serde_json::from_value(raw).map_err(|_| {
            Error::new(
                "source_integrity",
                "plugin returned an invalid discovery page",
            )
        })?;
        serde_json::to_value(page)
            .map_err(|_| Error::new("internal", "discovery page could not be encoded"))
    }

    /// Fleet heavy-work readback over a window: closed `activity.heavy`
    /// episodes per Agent with totals, newest episodes first. The Owner reads
    /// every Agent; an Agent principal reads itself or one scope-authorized
    /// Agent, exactly like the other Agent reads.
    async fn observe_heavy(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: ObserveHeavyRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let filter: Option<Vec<String>> = match principal {
            Principal::Owner => request.agent.clone().map(|agent| vec![agent]),
            Principal::Agent { id, .. } => {
                if let Some(target) = request.agent.as_deref() {
                    let store = self.inner.store.lock().await;
                    hierarchy::authorize_agent_read(&store, principal, target, request.generation)?;
                    Some(vec![target.to_owned()])
                } else {
                    Some(vec![id.clone()])
                }
            }
            Principal::Run { .. }
            | Principal::ProjectRule { .. }
            | Principal::MemoryInference { .. }
            | Principal::Swarm(_) => {
                return Err(Error::new(
                    "not_allowed",
                    "heavy work needs the owner or an Agent principal",
                ));
            }
        };
        let window_secs = request.effective_window_secs();
        let since =
            now_ms().saturating_sub(i64::try_from(window_secs).unwrap_or(0).saturating_mul(1000));
        let (rows, truncated) = {
            let store = self.inner.store.lock().await;
            store.heavy_episodes(
                filter.as_deref(),
                request.generation,
                since,
                eliot_core::api::MAX_HEAVY_EPISODES,
            )?
        };
        let mut by_agent: HashMap<String, Vec<HeavyEpisodeView>> = HashMap::new();
        for row in rows {
            match HeavyEpisodeView::from_record(&row.agent, row.generation, &row.data) {
                Ok(view) => by_agent.entry(row.agent).or_default().push(view),
                Err(_) => {
                    return Err(Error::new("internal", "heavy episode journal is malformed"));
                }
            }
        }
        let mut agents = Vec::new();
        for (agent, mut episodes) in by_agent {
            episodes.sort_by(|left, right| {
                right
                    .ended_at
                    .cmp(&left.ended_at)
                    .then(right.started_at.cmp(&left.started_at))
            });
            let mut total_duration_ms: u64 = 0;
            let mut total_cpu_ms: u64 = 0;
            let mut peak_rss_bytes: u64 = 0;
            for episode in &episodes {
                total_duration_ms = total_duration_ms
                    .saturating_add(u64::try_from(episode.duration_ms).unwrap_or(0));
                total_cpu_ms =
                    total_cpu_ms.saturating_add(u64::try_from(episode.cpu_ms).unwrap_or(0));
                peak_rss_bytes = peak_rss_bytes.max(episode.peak_rss_bytes);
            }
            agents.push(HeavyAgentTotals {
                agent,
                episodes,
                total_duration_ms,
                total_cpu_ms,
                peak_rss_bytes,
            });
        }
        agents.sort_by(|left, right| left.agent.cmp(&right.agent));
        let response = ObserveHeavyResponse {
            window_secs,
            since,
            agents,
            truncated,
        };
        request
            .validate_response(&response)
            .map_err(|_| Error::new("internal", "heavy work is invalid"))?;
        serde_json::to_value(response)
            .map_err(|_| Error::new("internal", "heavy work could not be encoded"))
    }

    /// Read-only harness installation and self-check report per harness: the
    /// installed probe facts plus the single durable self-check fact (the
    /// installed version and when that version last passed, #932 item 3),
    /// with the doc section and the last `harness_contract_mismatch` seen.
    /// Recording a changed installed version marks it "not checked"; a new
    /// version is never refused. Owner only, like `harness.list`; starts no
    /// model turn and performs no inference.
    async fn harness_doctor(&self, params: &Value, principal: &Principal) -> Result<Value> {
        require_owner(principal)?;
        let request: HarnessDoctorRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let mut probes = Vec::new();
        for harness in eliot_core::api::harness::SUPPORTED_HARNESSES {
            probes.push((
                *harness,
                crate::adhoc::harness_installation_async(harness).await,
            ));
        }
        let missing_commands = crate::config::missing_profile_commands(&self.inner.data_root);
        let profile_command_issues = self.profile_command_issues(missing_commands, &probes);
        let store = self.inner.store.lock().await;
        let mut harnesses = Vec::new();
        for (harness, entry) in &probes {
            let installed = entry
                .get("installed")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let version = entry
                .get("version")
                .and_then(Value::as_str)
                .map(str::to_owned);
            let row = store.get_harness_check(harness)?.filter(|row| {
                version
                    .as_deref()
                    .zip(row.installed_version.as_deref())
                    .is_some_and(|(actual, recorded)| {
                        eliot_core::same_harness_version(harness, actual, recorded)
                    })
            });
            let checked = row.as_ref().filter(|row| row.version_checked());
            let check_outcome = row
                .as_ref()
                .map_or(HarnessCheckOutcome::NotChecked, |row| row.checked_outcome());
            let check_agent = version
                .as_deref()
                .map(|version| store.active_harness_check_agent(harness, version))
                .transpose()?
                .flatten();
            let contract_mismatch = store
                .latest_event_data("harness_contract_mismatch")?
                .and_then(|data| doctor_mismatch_text(&data));
            harnesses.push(DoctorHarnessEntry {
                harness: (*harness).to_owned(),
                installed,
                version,
                check_outcome,
                checked_version: checked.and_then(|row| row.checked_version.clone()),
                checked_at: checked.map(|row| row.checked_at),
                check_profile: checked
                    .and_then(|row| row.check_profile.clone())
                    .filter(|profile| !profile.is_empty()),
                check_agent,
                not_checked_reason: if checked.is_none() && installed {
                    Some(
                        row.as_ref()
                            .and_then(|row| row.not_checked_reason.clone())
                            .unwrap_or_else(|| eliot_core::NOT_CHECKED_NEW_VERSION.to_owned()),
                    )
                } else {
                    None
                },
                failure_step: checked.and_then(|row| row.failure_step.clone()),
                failure_error: checked.and_then(|row| row.failure_error.clone()),
                doc_section: eliot_core::harness_doc_section(harness).to_owned(),
                contract_mismatch,
            });
        }
        let (profile_first_calls, profile_first_calls_truncated) = store.profile_first_calls()?;
        let (profile_native_child_usage, profile_native_child_usage_truncated) =
            store.profile_native_child_usage()?;
        drop(store);
        let data_root = self.inner.data_root.clone();
        let (owner_rules_drift, owner_rules_check_unavailable) = match tokio::task::spawn_blocking(
            move || owner_rules_doctor::check(&data_root),
        )
        .await
        {
            Ok(Ok(files)) => (files, false),
            _ => (Vec::new(), true),
        };
        let response = HarnessDoctorResponse {
            harnesses,
            profile_command_issues,
            profile_first_calls,
            profile_first_calls_truncated,
            profile_native_child_usage,
            profile_native_child_usage_truncated,
            goal_check_deadlines: self.goal_check_deadlines().await?,
            attention_thresholds: Some(self.attention_thresholds().await?),
            owner_rules_drift,
            owner_rules_check_unavailable,
        };
        request
            .validate_response(&response)
            .map_err(|_| Error::new("internal", "harness doctor is invalid"))?;
        serde_json::to_value(response)
            .map_err(|_| Error::new("internal", "harness doctor could not be encoded"))
    }

    async fn agent_history(&self, params: &Value, principal: &Principal) -> Result<Value> {
        require_owner(principal)?;
        let request: AgentHistoryRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let id = request.id.as_str();
        let generation = request.generation;
        let query = ObservationQuery {
            source: request.source,
            view: request.view,
            turn_id: request.turn_id,
            cursor: request.cursor,
            limit: request.limit,
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

        if agent.origin == AgentOrigin::Native && agent.harness == "muse" {
            return Err(Error::new(
                "unsupported",
                "native history is unsupported for Muse child observations",
            ));
        }

        // This is a private native read. It is deliberately not reduced, stored,
        // published, or used to settle a command or input acknowledgement.
        //
        // The native subject projection was already read at attach and is
        // retained with the applied attach outcome. When the live observer
        // reader has lapsed, `source=native, view=subject` is still served from
        // that retained metadata; turns/items keep requiring the live reader,
        // and a later detach still fences every read.
        let raw = match self
            .inner
            .plugin_host
            .snapshot_observer(
                &agent,
                serde_json::to_value(&query).map_err(|_| {
                    Error::new("internal", "observation query could not be encoded")
                })?,
            )
            .await
        {
            Ok(raw) => raw,
            Err(error)
                if query.source == "native"
                    && query.view == "subject"
                    && query.cursor.is_none()
                    && query.turn_id.is_none() =>
            {
                match self.retained_observe_subject_page(&agent).await? {
                    Some(raw) => raw,
                    None => return Err(error),
                }
            }
            Err(error) => return Err(error),
        };
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

    /// Native subject page from the retained applied attach outcome.
    ///
    /// Returns `None` when the current generation has no applied attach with
    /// subject metadata (including after a later detach), so the caller keeps
    /// the original live-reader error and the detach fence stands.
    async fn retained_observe_subject_page(&self, agent: &Agent) -> Result<Option<Value>> {
        let subject = {
            let store = self.inner.store.lock().await;
            store.retained_observe_subject(&agent.id, agent.generation)?
        };
        let Some(subject) = subject else {
            return Ok(None);
        };
        let page = ObservationPage {
            agent_id: agent.id.clone(),
            generation: agent.generation,
            native_scope: agent.native_scope.clone().unwrap_or_default(),
            native_ref: agent.native_ref.clone().unwrap_or_default(),
            source: "native".to_owned(),
            view: "subject".to_owned(),
            entries: vec![subject],
            next_cursor: None,
            coverage: json!({"kind": "native_metadata", "complete": true, "live": false}),
        };
        page.validate()?;
        serde_json::to_value(page)
            .map_err(|_| Error::new("internal", "retained observation page could not be encoded"))
            .map(Some)
    }

    async fn agent_get(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: AgentGetRequest = eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let id = request.id.as_str();
        let (
            agent,
            source_states,
            observer_active,
            provider_hold,
            native_notice,
            instruction_delivery,
            owner_rules_delivery,
            last_control_failure,
        ) = {
            let store = self.inner.store.lock().await;
            hierarchy::authorize_agent_read(&store, principal, id, None)?;
            let agent = store
                .get_agent(id)?
                .ok_or_else(|| Error::new("not_found", format!("Agent {id} was not found")))?;
            let last_control_failure = match store.latest_owner_control_failure(&agent)? {
                Some(command) => {
                    match hierarchy::authorize_command_read(&store, principal, &command) {
                        Ok(()) => Some(eliot_core::api::AgentControlFailure::from_command(
                            &command,
                        )?),
                        Err(error) if error.code == "not_allowed" => None,
                        Err(error) => return Err(error),
                    }
                }
                None => None,
            };
            let (provider_hold, native_notice) = store.agent_status_projection(&agent)?;
            let instruction_delivery = store.get_agent_instruction_delivery(id)?;
            let owner_rules_delivery = store.get_agent_owner_rules_delivery(id)?;
            let mut source_states = store.list_source_states(id, agent.generation)?;
            if let Some(workspace) = store.get_workspace(id)?
                && workspace.target_generation != agent.generation
            {
                source_states.extend(store.list_source_states(id, workspace.target_generation)?);
            }
            let observer_active =
                agent.observe_only && store.observer_active(&agent.id, agent.generation)?;
            (
                agent,
                source_states,
                observer_active,
                provider_hold,
                native_notice,
                instruction_delivery,
                owner_rules_delivery,
                last_control_failure,
            )
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
        let source_states = source_states
            .into_iter()
            .map(|value| {
                serde_json::from_value::<SourceState>(value)
                    .map_err(|_| Error::new("internal", "source state view is invalid"))
            })
            .collect::<Result<Vec<_>>>()?;
        let session_usage = self.session_usage(&agent).await?;
        let (heavy, session_growth, work) = self.advisory_agent_readbacks(&agent).await;
        // Current model/effort overlay for `swarm show`: the latest
        // retained revision with its effective values plus the original
        // frozen profile values and provenance. `profile` repeats the
        // retained requested name, `profile_inherited` repeats the
        // launch-time freeze decision (verbatim parent-snapshot reuse),
        // and `provenance` distinguishes a direct `configure` from a
        // `resume` generation's `inherited_from` row. Absent when no
        // overlay ever applied to this generation.
        let (config, pending_background_wake) = {
            let store = self.inner.store.lock().await;
            let pending_background_wake =
                store.pending_background_wake(&agent.id, agent.generation)?;
            let overlay = store.get_agent_config_current(&agent.id, agent.generation)?;
            let profile_inherited = agent.profile_inherited;
            let config = overlay.map(|overlay| AgentConfigView {
                revision: overlay.revision,
                application: overlay.application.clone(),
                provenance: overlay.provenance.clone(),
                profile: agent.profile.clone(),
                profile_inherited,
                requested_model: overlay.requested_model.clone(),
                requested_effort: overlay.requested_effort.clone(),
                model: overlay.model.clone(),
                effort: overlay.effort.clone(),
                original_model: agent
                    .profile_snapshot
                    .get("model")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
                original_effort: agent
                    .profile_snapshot
                    .get("effort")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
            });
            (config, pending_background_wake)
        };
        let work_truth = {
            let store = self.inner.store.lock().await;
            store
                .agent_work_truth(&agent.id, agent.generation)
                .unwrap_or(None)
        };
        let mut response = AgentGetResponse {
            agent,
            observation,
            source_states,
            session_usage,
            config,
            heavy,
            session_growth,
            work,
            provider_hold,
            native_notice,
            instruction_delivery,
            owner_rules_delivery,
            pending_background_wake,
            last_control_failure,
            work_truth,
        };
        display_agent_get_response(&mut response);
        serde_json::to_value(response)
            .map_err(|_| Error::new("internal", "Agent view could not be encoded"))
    }

    async fn session_usage(&self, agent: &Agent) -> Result<Option<SessionUsage>> {
        if let Some(usage) = self
            .inner
            .monitoring
            .live_session_usage(&agent.id, agent.generation)
        {
            return Ok(Some(usage));
        }
        let store = self.inner.store.lock().await;
        store.get_session_usage(&agent.id, agent.generation)
    }

    /// Heavy-work, session-growth and work readbacks are advisory: a degraded
    /// journal yields `None` instead of failing the Agent view.
    async fn advisory_agent_readbacks(
        &self,
        agent: &Agent,
    ) -> (
        Option<HeavySummaryView>,
        Option<SessionGrowthView>,
        Option<WorkSummaryView>,
    ) {
        let growth = self.inner.observe.lock_recover().growth;
        let store = self.inner.store.lock().await;
        (
            store
                .heavy_rollup(&agent.id, agent.generation)
                .unwrap_or(None),
            store
                .session_growth_view(&agent.harness, &agent.id, agent.generation, &growth)
                .unwrap_or(None),
            store
                .work_summary(
                    &agent.id,
                    agent.generation,
                    agent.workspace.as_deref(),
                    agent.active_turn.as_deref(),
                )
                .ok(),
        )
    }

    async fn observation_view(&self, agent: &Agent) -> ObservationView {
        match self.inner.monitoring.view(agent).await {
            Ok(value) => value,
            Err(error) => ObservationView::unavailable(error.code),
        }
    }

    /// Join one observation to its captured summary generation. A resume
    /// between the summary read and the observation fetch must not attach the
    /// new generation's observations to the old summary.
    fn join_observation_for_summary(
        summary_generation: i64,
        agent: &Agent,
        observation: ObservationView,
    ) -> ObservationView {
        if agent.generation != summary_generation {
            ObservationView::unavailable("stale_generation")
        } else {
            observation
        }
    }

    async fn agent_processes(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: AgentProcessesRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let id = request.id.as_str();
        let generation = request.generation;
        let scope = request.effective_scope();
        let after = request.cursor()?;
        let limit = request.limit;
        let (generation, mut processes, child_counts) = {
            let store = self.inner.store.lock().await;
            hierarchy::authorize_agent_read(&store, principal, id, generation)?;
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
                hierarchy::authorize_agent_read(&store, principal, id, Some(generation))?;
            }
            if let Some(cursor) = &after {
                cursor.validate_binding(id, generation, scope)?;
            }
            let owner = format!("{scope}:{id}#{generation}");
            (
                generation,
                store.list_processes(
                    &owner,
                    after.as_ref().map(|cursor| &cursor.identity),
                    limit,
                )?,
                store.child_process_counts(&owner)?,
            )
        };
        self.inner
            .monitoring
            .refresh_process_timestamps(id, generation, scope, &mut processes);
        let next = processes
            .last()
            .map(|process| {
                ProcessPageCursor {
                    agent: id.to_owned(),
                    generation,
                    scope: scope.to_owned(),
                    identity: process.identity.clone(),
                }
                .encode()
            })
            .transpose()?;
        serde_json::to_value(AgentProcessesResponse {
            agent: id.to_owned(),
            generation,
            processes,
            next,
            scope: (scope == "workspace").then(|| scope.to_owned()),
            child_counts: Some(child_counts),
        })
        .map_err(|_| Error::new("internal", "process page could not be encoded"))
    }

    async fn agent_git(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: AgentGitRequest = eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let id = request.id.as_str();
        let expected = request.generation;
        let agent = {
            let store = self.inner.store.lock().await;
            hierarchy::authorize_agent_read(&store, principal, id, expected)?;
            store.get_agent(id)?
        }
        .ok_or_else(|| Error::new("not_found", format!("Agent {id} was not found")))?;
        if expected.is_some_and(|generation| generation != agent.generation) {
            return Err(Error::new(
                "stale_generation",
                "Git view requires the current Agent generation",
            ));
        }
        let git = match self.inner.monitoring.git_view(&agent).await {
            Ok(value) => value,
            Err(error) => GitView::unavailable(error.code),
        };
        serde_json::to_value(AgentGitResponse {
            agent: agent.id,
            generation: agent.generation,
            git,
        })
        .map_err(|_| Error::new("internal", "Git view could not be encoded"))
    }

    async fn agent_edits(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: AgentEditsRequest = eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let id = request.id.as_str();
        let generation = request.generation;
        let after = request.after.as_deref();
        let limit = request.limit;
        let (generation, edits) = {
            let store = self.inner.store.lock().await;
            hierarchy::authorize_agent_read(&store, principal, id, generation)?;
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
        serde_json::to_value(AgentEditsResponse {
            agent: id.to_owned(),
            generation,
            edits,
            next,
        })
        .map_err(|_| Error::new("internal", "edit page could not be encoded"))
    }

    async fn agent_stats(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: AgentStatsRequest = eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let id = request.id.as_str();
        let expected = request.generation;
        let agent = {
            let store = self.inner.store.lock().await;
            hierarchy::authorize_agent_read(&store, principal, id, expected)?;
            store.get_agent(id)?
        }
        .ok_or_else(|| Error::new("not_found", format!("Agent {id} was not found")))?;
        if expected.is_some_and(|generation| generation != agent.generation) {
            return Err(Error::new(
                "stale_generation",
                "stats require the current Agent generation",
            ));
        }
        let observation = self.observation_view(&agent).await;
        let session_usage = self.session_usage(&agent).await?;
        let (heavy, session_growth, work) = self.advisory_agent_readbacks(&agent).await;
        serde_json::to_value(AgentStatsResponse {
            agent: agent.id,
            generation: agent.generation,
            observation,
            session_usage,
            heavy,
            session_growth,
            work,
        })
        .map_err(|_| Error::new("internal", "stats view could not be encoded"))
    }

    /// Bounded usage totals with explicit coverage over one Agent, project
    /// or tree scope. The Owner reads every retained generation in scope;
    /// an Agent principal reads only the pairs its frozen control scope
    /// allows, exactly like the other Agent reads. Scope expansion reuses
    /// the Store hierarchy bound; truncation and retention coverage stay
    /// explicit so a partial subtotal never presents as complete.
    async fn usage_summary(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: UsageSummaryRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let response = {
            let store = self.inner.store.lock().await;
            let resolved = store.resolve_usage_scope(&request)?;
            let (pairs, agents_included) = match principal {
                Principal::Owner => (resolved.pairs, resolved.agents_included),
                Principal::Agent { id, .. } => {
                    if let Some(target) = request.agent.as_deref() {
                        hierarchy::authorize_agent_read(
                            &store,
                            principal,
                            target,
                            request.generation,
                        )?;
                    } else if let Some(root) = request.root.as_deref() {
                        hierarchy::authorize_agent_read(&store, principal, root, None)?;
                    }
                    let caller = store.get_agent(id)?.ok_or_else(|| {
                        Error::new("not_found", format!("Agent {id} was not found"))
                    })?;
                    let mut pairs = Vec::new();
                    let mut agents = HashSet::new();
                    for (num, generation) in resolved.pairs {
                        let target = format!("a{num}");
                        if target == *id || store.agent_in_scope(&caller, &target)? {
                            agents.insert(num);
                            pairs.push((num, generation));
                        }
                    }
                    (pairs, agents.len())
                }
                Principal::Run { .. }
                | Principal::ProjectRule { .. }
                | Principal::MemoryInference { .. }
                | Principal::Swarm(_) => {
                    return Err(Error::new(
                        "not_allowed",
                        "usage summary needs the owner or an Agent principal",
                    ));
                }
            };
            let truncated = resolved.truncated;
            store.summarize_usage(
                UsageScopeView {
                    agent: request.agent.clone(),
                    generation: request.generation,
                    project: request.project.clone(),
                    root: request.root.clone(),
                },
                &pairs,
                agents_included,
                truncated,
            )?
        };
        request
            .validate_response(&response)
            .map_err(|_| Error::new("internal", "usage summary is invalid"))?;
        serde_json::to_value(response)
            .map_err(|_| Error::new("internal", "usage summary could not be encoded"))
    }

    fn read_agent_list_page_from_store(
        store: &eliot_store::Store,
        agent_caller: Option<&str>,
        filter: &AgentListFilter,
        after: Option<&str>,
        limit: usize,
        now: i64,
        growth: &eliot_core::SessionGrowthThresholds,
    ) -> Result<eliot_store::AgentListPage> {
        match agent_caller {
            None => store.list_agent_summaries(filter, after, limit, now, growth),
            Some(id) => {
                let caller = store
                    .get_agent(id)?
                    .ok_or_else(|| Error::new("not_found", format!("Agent {id} was not found")))?;
                store.list_agent_summaries_scoped(&caller, filter, after, limit, now, growth)
            }
        }
    }

    async fn agent_list(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: AgentListRequest = eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let agent_caller = match principal {
            Principal::Owner => None,
            Principal::Agent { id, .. } => Some(id.clone()),
            Principal::Run { .. } | Principal::MemoryInference { .. } | Principal::Swarm(_) => {
                return Err(Error::new(
                    "not_allowed",
                    "Run credentials cannot list Agents",
                ));
            }
            Principal::ProjectRule { .. } => {
                return Err(Error::new(
                    "not_allowed",
                    "project rule authority cannot list Agents",
                ));
            }
        };
        let filter = AgentListFilter {
            archived: request.archived,
            order: request.order,
            state: request.state,
            states: request.states.clone(),
            project: request.project.clone(),
            project_scope: request.project_scope.clone(),
            harness: request.harness.clone(),
            parent: request.parent.clone(),
            root: request.root.clone(),
            query: request.query.clone(),
        };
        let limit = request.limit;
        let observations = request.observations;
        let after = request.after.clone();
        let growth = self.inner.observe.lock_recover().growth;
        let page = if !observations {
            let permit = self
                .inner
                .agent_list_readers
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| Error::new("unavailable", "agent list readers are closed"))?;
            let db_path = self.inner.data_root.join("eliot.db");
            let filter_clone = filter.clone();
            tokio::task::spawn_blocking(move || {
                let _permit = permit;
                let store = eliot_store::Store::open_read_only(&db_path)?;
                Self::read_agent_list_page_from_store(
                    &store,
                    agent_caller.as_deref(),
                    &filter_clone,
                    after.as_deref(),
                    limit,
                    now_ms(),
                    &growth,
                )
            })
            .await
            .map_err(|_| Error::new("internal", "agent list reader failed"))??
        } else {
            let store = self.inner.store.lock().await;
            Self::read_agent_list_page_from_store(
                &store,
                agent_caller.as_deref(),
                &filter,
                after.as_deref(),
                limit,
                now_ms(),
                &growth,
            )?
        };
        if !observations {
            return serde_json::to_value(AgentListResponse {
                state_totals: page.state_totals,
                agents: page
                    .summaries
                    .into_iter()
                    .map(display_agent_summary)
                    .map(|summary| AgentListRow {
                        summary,
                        observation: None,
                    })
                    .collect(),
                next: page.next,
                total: page.total,
                truncated: page.truncated,
            })
            .map_err(|_| Error::new("internal", "Agent list could not be encoded"));
        }
        // Observations join one full row per summary for the captured
        // Agent/generation; a resume between the summary read and this fetch
        // must not attach the new generation's observations to the old
        // summary. The byte bound trims the page the same way it trims the
        // compact rows, and truncation stays explicit so polling managers can
        // tell a quiet page from a cut one.
        let mut rows = Vec::new();
        let mut encoded_bytes = 0_usize;
        let mut truncated = page.truncated;
        for summary in page.summaries {
            let summary_generation = summary.generation;
            let agent = {
                let store = self.inner.store.lock().await;
                store.get_agent(&summary.id)?.ok_or_else(|| {
                    Error::new("not_found", format!("Agent {} was not found", summary.id))
                })?
            };
            let observation = if agent.generation != summary_generation {
                // Drop the new generation's observations; the old summary
                // keeps its generation without inheriting them.
                ObservationView::unavailable("stale_generation")
            } else {
                let fetched = self.observation_view(&agent).await;
                Self::join_observation_for_summary(summary_generation, &agent, fetched)
            };
            let mut observation = observation;
            display_observation(&mut observation);
            let row = AgentListRow {
                summary: display_agent_summary(summary),
                observation: Some(observation),
            };
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
                truncated = true;
                break;
            }
            encoded_bytes += row_bytes + 1;
            rows.push(row);
        }
        let next = rows.last().map(|row| match request.order {
            eliot_core::api::AgentListOrder::Id => row.summary.id.clone(),
            eliot_core::api::AgentListOrder::Recent => {
                eliot_core::api::encode_recent_cursor(&row.summary.id, row.summary.updated_at)
            }
        });
        serde_json::to_value(AgentListResponse {
            state_totals: page.state_totals,
            agents: rows,
            next,
            total: page.total,
            truncated,
        })
        .map_err(|_| Error::new("internal", "Agent list could not be encoded"))
    }

    async fn agent_result(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: AgentResultRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let id = request.id.as_str();
        let mut response = {
            let store = self.inner.store.lock().await;
            hierarchy::authorize_agent_read(&store, principal, id, request.generation)?;
            let agent = store
                .get_agent(id)?
                .ok_or_else(|| Error::new("not_found", format!("Agent {id} was not found")))?;
            let self_generation = match principal {
                Principal::Agent {
                    id: caller_id,
                    generation,
                } if caller_id == id => Some(*generation),
                _ => None,
            };
            let generation_filter = request.generation.or(self_generation);
            let generation = generation_filter.unwrap_or(agent.generation);
            if let Some(before) = &request.before
                && (before.generation > generation
                    || generation_filter.is_some_and(|filter| before.generation != filter))
            {
                return Err(Error::new(
                    "invalid",
                    "result cursor generation is outside the requested generation scope",
                ));
            }
            store.agent_result_page(&agent, generation_filter, &request)?
        };
        self.mask_unknown_turns(id, &mut response.turns).await?;
        request
            .validate_response(&response)
            .map_err(|_| Error::new("internal", "agent.result response is invalid"))?;
        serde_json::to_value(response)
            .map_err(|_| Error::new("internal", "agent.result response could not be encoded"))
    }

    async fn turn_get(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: TurnGetRequest = eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let agent = request.agent.as_str();
        let generation = request.generation;
        let turn_id = request.turn_id.as_str();
        let mut turn = {
            let store = self.inner.store.lock().await;
            hierarchy::authorize_agent_read(&store, principal, agent, Some(generation))?;
            let agent_row = store
                .get_agent(agent)?
                .ok_or_else(|| Error::new("not_found", format!("Agent {agent} was not found")))?;
            let mut turn = store
                .get_turn(agent, generation, turn_id)?
                .ok_or_else(|| Error::new("not_found", format!("turn {turn_id} was not found")))?;
            // Historical configuration from the turn's exact retained
            // generation and revision, never the current overlay.
            store.fill_turn_readback(&agent_row, &mut turn)?;
            turn
        };
        self.mask_unknown_turns(agent, std::slice::from_mut(&mut turn))
            .await?;
        serde_json::to_value(turn)
            .map_err(|error| Error::new("internal", format!("encode turn: {error}")))
    }

    /// Variable scopes an Agent can read (global plus its own project), so
    /// its event filter sees those Agent-less `var.changed` facts. Other
    /// principals keep the plain filter: the owner reads every event
    /// unfiltered.
    async fn readable_var_scopes(&self, principal: &Principal) -> Result<HashSet<String>> {
        let Principal::Agent { id, .. } = principal else {
            return Ok(HashSet::new());
        };
        let mut scopes = HashSet::from([eliot_core::api::VarScope::Global.encoded()]);
        if let Some(agent) = self.inner.store.lock().await.get_agent(id)? {
            scopes.insert(
                eliot_core::api::VarScope::Project(agent.project_scope().to_owned()).encoded(),
            );
        }
        Ok(scopes)
    }

    async fn events_bounds(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: EventsBoundsRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        authorize_event_filter(request.filter, principal)?;
        let bounds = self.inner.store.lock().await.event_bounds()?;
        bounds.validate()?;
        serde_json::to_value(bounds)
            .map_err(|_| Error::new("internal", "event bounds could not be encoded"))
    }

    async fn events_read(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: EventsReadRequest = eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let mut filter = authorize_event_filter(request.filter, principal)?;
        filter.var_scopes = self.readable_var_scopes(principal).await?;
        let (events, next, high_water, truncated, _) = self
            .read_filtered(
                request.after,
                request.limit,
                None,
                EVENT_RESPONSE_ENTRIES_MAX_BYTES,
                &filter,
            )
            .await?;
        serde_json::to_value(EventsReadResponse {
            events,
            next,
            high_water,
            truncated,
        })
        .map_err(|_| Error::new("internal", "events.read response could not be encoded"))
    }

    async fn events_subscribe(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: EventsSubscribeRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let limit = request.limit;
        let wait_ms = request.wait_ms.min(LIVE_WAIT_MAX_MS);
        let mut filter = authorize_event_filter(request.filter, principal)?;
        filter.var_scopes = self.readable_var_scopes(principal).await?;

        // Subscribe before reading high-water so commits are either replayed or buffered.
        let mut receiver = self.inner.bus.subscribe();
        let (mut events, mut cursor, mut high_water, mut truncated, mut encoded_bytes) = self
            .read_filtered(
                request.after,
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
        serde_json::to_value(EventsSubscribeResponse {
            events,
            next: cursor,
            high_water,
            lagged,
            truncated,
        })
        .map_err(|_| Error::new("internal", "events.subscribe response could not be encoded"))
    }

    async fn swarm_wait(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: SwarmWaitRequest = eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        match principal {
            Principal::Owner | Principal::Agent { .. } => {}
            Principal::Run { .. } | Principal::MemoryInference { .. } | Principal::Swarm(_) => {
                return Err(Error::new(
                    "not_allowed",
                    "Run credentials may call only command.get",
                ));
            }
            Principal::ProjectRule { .. } => {
                return Err(Error::new(
                    "not_allowed",
                    "project rule authority cannot wait on tasks",
                ));
            }
        }
        if request.total() > MAX_SWARM_WAIT_TARGETS {
            return Err(Error::new(
                "invalid",
                "wait requires 1..=256 captured entries",
            ));
        }
        // `mailbox` also ends the wait on new mail for the calling Agent; the
        // owner has no mailbox to watch.
        let mailbox = match (request.mailbox, principal) {
            (false, _) => None,
            (true, Principal::Agent { id, .. }) => Some(id.as_str()),
            (true, _) => {
                return Err(Error::new("invalid", "mailbox wait needs an Agent caller"));
            }
        };
        let mode = request.mode.as_str();
        let mut poll = self.long_poll(request.wait_ms);
        // Only deliveries admitted after the first snapshot end a mailbox
        // wait: the watermark never drops on acknowledgement.
        let mut mail_base: Option<i64> = None;
        loop {
            let mut snapshot = self.wait_snapshot(&request, principal, mailbox).await?;
            if let Some(current) = snapshot.mail_current {
                snapshot.mail_arrived = current > *mail_base.get_or_insert(current);
            }
            if snapshot.mail_arrived
                || Self::wait_mode_completed(mode, &snapshot.results)
                || !poll.wake(|event| snapshot.watches(event)).await?
            {
                let mut results = snapshot.results;
                self.attach_wait_facts(&mut results, principal).await;
                return Self::encode_wait(
                    &request,
                    results,
                    !snapshot.mail_arrived,
                    snapshot.mail_arrived,
                );
            }
        }
    }

    /// Start a long-poll for `swarm.wait`, `mail.inbox` or `var.get`. It
    /// subscribes before the caller's first snapshot, so a commit racing
    /// that snapshot is either in it or buffered on the bus. The wait is
    /// clamped to `MAX_SWARM_WAIT_MS`, like `events.subscribe`.
    fn long_poll(&self, wait_ms: u64) -> LongPoll {
        LongPoll {
            receiver: self.inner.bus.subscribe(),
            deadline: Instant::now() + Duration::from_millis(wait_ms.min(MAX_SWARM_WAIT_MS)),
        }
    }

    fn wait_mode_completed(mode: &str, results: &[WaitEntryResult]) -> bool {
        match mode {
            "all" => results.iter().all(WaitEntryResult::is_terminal),
            _ => results.iter().any(WaitEntryResult::is_terminal),
        }
    }

    fn encode_wait(
        request: &SwarmWaitRequest,
        results: Vec<WaitEntryResult>,
        timed_out: bool,
        mail_wake: bool,
    ) -> Result<Value> {
        let completed = Self::wait_mode_completed(request.mode.as_str(), &results);
        let response = SwarmWaitResponse {
            mode: request.mode.clone(),
            completed,
            timed_out: timed_out && !completed,
            mail_wake,
            results,
        };
        request
            .validate_response(&response)
            .map_err(|_| Error::new("internal", "swarm.wait response is invalid"))?;
        serde_json::to_value(response)
            .map_err(|_| Error::new("internal", "swarm.wait response could not be encoded"))
    }

    /// Snapshot every frozen entry under one Store lock: authorize and read
    /// each command and bound turn, mask unknown turns, then encode bounded
    /// retained `final_text` for completed turns within the frame budget.
    /// `mailbox` names the calling Agent whose delivery watermark is read;
    /// the caller keeps the baseline across snapshots.
    async fn wait_snapshot(
        &self,
        request: &SwarmWaitRequest,
        principal: &Principal,
        mailbox: Option<&str>,
    ) -> Result<WaitSnapshot> {
        let marks = self.wait_unknown_marks().await;
        let store = self.inner.store.lock().await;
        Self::wait_snapshot_locked(
            &store,
            request,
            principal,
            mailbox,
            WaitScope::default(),
            &marks,
        )
    }

    /// Capture the in-memory unknown marks before terminal-cache eviction:
    /// an uncertainty check must still see a marker whose command already
    /// settled, mirroring the old per-entry order (check first, evict while
    /// masking). Masking uses the post-clear ids, also like before. Taken
    /// before the snapshot's Store guard, so the guarded part never awaits.
    async fn wait_unknown_marks(&self) -> UnknownMarks {
        let causes = self.inner.unknown_causes.lock_recover().clone();
        self.clear_terminal_unknown_cache().await;
        UnknownMarks {
            causes,
            memory: self.unknown_ids(),
        }
    }

    /// Snapshot every frozen entry inside the caller's Store guard with
    /// optional registration-time masking, delivery-time recipient scope
    /// checking and fair water-fill excerpt budgeting ([`WaitScope`]).
    /// Out-of-recipient-scope entries keep their status only, with no turn
    /// identity or `final_text` excerpt. Synchronous, so a caller can keep
    /// the same guard through a write that depends on the authorization.
    fn wait_snapshot_locked(
        store: &eliot_store::Store,
        request: &SwarmWaitRequest,
        principal: &Principal,
        mailbox: Option<&str>,
        scope: WaitScope<'_>,
        marks: &UnknownMarks,
    ) -> Result<WaitSnapshot> {
        let WaitScope {
            hidden,
            excerpt_recipient,
            fair_budget,
        } = scope;
        let unknown_causes = &marks.causes;
        let recipient_principal = match excerpt_recipient {
            Some(recipient_id) => store
                .get_agent(recipient_id)?
                .map(|agent| Principal::Agent {
                    id: agent.id,
                    generation: agent.generation,
                }),
            None => None,
        };
        let mut pending: Vec<PendingEntry> = Vec::with_capacity(request.total());
        let mut bounds: Vec<BoundTurn> = Vec::new();
        let mut watch_agents: HashSet<String> = HashSet::new();
        let watch_commands: HashSet<String> = request.commands.iter().cloned().collect();
        let leaf_entry = |kind: &'static str, id: String, leaf: String| PendingEntry {
            kind,
            id,
            leaf,
            bound: None,
            command_unknown: false,
            excerpt_allowed: false,
            branch: Ok(None),
        };
        for (idx, id) in request.commands.iter().enumerate() {
            if hidden.is_some_and(|mask| mask.get(idx).copied().unwrap_or(false)) {
                pending.push(leaf_entry("command", id.clone(), "not_visible".to_owned()));
                continue;
            }
            let command = match store.get_command(id)? {
                Some(command) => command,
                None => {
                    pending.push(leaf_entry("command", id.clone(), "not_found".to_owned()));
                    continue;
                }
            };
            if let Err(error) =
                crate::router::hierarchy::authorize_command_read(store, principal, &command)
            {
                if error.code == "not_allowed" {
                    pending.push(leaf_entry("command", id.clone(), "not_visible".to_owned()));
                    continue;
                }
                return Err(error);
            }
            if let Some(target) = command.target.as_deref() {
                watch_agents.insert(target.to_owned());
            }
            let mut excerpt_allowed = match excerpt_recipient {
                None => true,
                Some(_) => recipient_principal.as_ref().is_some_and(|rp| {
                    crate::router::hierarchy::authorize_command_read(store, rp, &command).is_ok()
                }),
            };
            // Uncertain commands are terminal unknown, never still running.
            // This mirrors the command readback masking without mutating the
            // row.
            let command_unknown =
                command.state == CommandState::Unknown || unknown_causes.contains_key(&command.id);
            if let (Some(target), Some(generation), Some(turn_id)) = (
                command.target.as_deref(),
                command.generation,
                command.turn_id.as_deref(),
            ) {
                if let Err(error) = crate::router::hierarchy::authorize_agent_read(
                    store,
                    principal,
                    target,
                    Some(generation),
                ) {
                    if error.code == "not_allowed" {
                        pending.push(leaf_entry(
                            "command",
                            id.clone(),
                            if command_unknown {
                                "unknown".to_owned()
                            } else {
                                "not_visible".to_owned()
                            },
                        ));
                        continue;
                    }
                    return Err(error);
                }
                if excerpt_recipient.is_some() {
                    excerpt_allowed = excerpt_allowed
                        && recipient_principal.as_ref().is_some_and(|rp| {
                            crate::router::hierarchy::authorize_agent_read(
                                store,
                                rp,
                                target,
                                Some(generation),
                            )
                            .is_ok()
                        });
                }
                match store.get_turn(target, generation, turn_id)? {
                    Some(turn) => {
                        watch_agents.insert(target.to_owned());
                        bounds.push(BoundTurn {
                            agent: target.to_owned(),
                            generation,
                            turn_id: turn_id.to_owned(),
                            turn,
                        });
                        pending.push(PendingEntry {
                            kind: "command",
                            id: id.clone(),
                            leaf: String::new(),
                            bound: Some(bounds.len() - 1),
                            command_unknown,
                            excerpt_allowed,
                            branch: Ok(None),
                        });
                    }
                    // A command bound to a missing turn reads unknown, never
                    // still running.
                    None => pending.push(leaf_entry("command", id.clone(), "unknown".to_owned())),
                }
                continue;
            }
            let mut entry = leaf_entry(
                "command",
                id.clone(),
                if command_unknown {
                    "unknown".to_owned()
                } else {
                    match command.state {
                        CommandState::Applied => "succeeded",
                        CommandState::Rejected | CommandState::Failed => "failed",
                        CommandState::Cancelled => "interrupted",
                        CommandState::Unknown => "unknown",
                        CommandState::Accepted
                        | CommandState::Queued
                        | CommandState::Dispatched => "still_running",
                    }
                    .to_owned()
                },
            );
            let generation = match command.verb.as_str() {
                "launch" => Some(1),
                "send" => command.generation.or(command.expected_generation),
                _ => None,
            };
            if excerpt_allowed
                && let (Some(target), Some(generation)) = (command.target.as_deref(), generation)
                && hierarchy::authorize_agent_read(store, principal, target, Some(generation))
                    .is_ok()
                && (excerpt_recipient.is_none()
                    || recipient_principal.as_ref().is_some_and(|rp| {
                        hierarchy::authorize_agent_read(store, rp, target, Some(generation)).is_ok()
                    }))
            {
                entry.branch = store.child_worktree_branch(target, generation);
            }
            pending.push(entry);
        }
        for (t_idx, turn_ref) in request.turns.iter().enumerate() {
            let id = turn_ref.canonical_id();
            let idx = request.commands.len() + t_idx;
            if hidden.is_some_and(|mask| mask.get(idx).copied().unwrap_or(false)) {
                pending.push(leaf_entry("turn", id, "not_visible".to_owned()));
                continue;
            }
            if let Err(error) = crate::router::hierarchy::authorize_agent_read(
                store,
                principal,
                &turn_ref.agent,
                Some(turn_ref.generation),
            ) {
                if error.code == "not_allowed" {
                    pending.push(leaf_entry("turn", id, "not_visible".to_owned()));
                    continue;
                }
                return Err(error);
            }
            let excerpt_allowed = match excerpt_recipient {
                None => true,
                Some(_) => recipient_principal.as_ref().is_some_and(|rp| {
                    crate::router::hierarchy::authorize_agent_read(
                        store,
                        rp,
                        &turn_ref.agent,
                        Some(turn_ref.generation),
                    )
                    .is_ok()
                }),
            };
            match store.get_turn(&turn_ref.agent, turn_ref.generation, &turn_ref.turn_id)? {
                Some(turn) => {
                    watch_agents.insert(turn_ref.agent.clone());
                    bounds.push(BoundTurn {
                        agent: turn_ref.agent.clone(),
                        generation: turn_ref.generation,
                        turn_id: turn_ref.turn_id.clone(),
                        turn,
                    });
                    pending.push(PendingEntry {
                        kind: "turn",
                        id,
                        leaf: String::new(),
                        bound: Some(bounds.len() - 1),
                        command_unknown: false,
                        excerpt_allowed,
                        branch: Ok(None),
                    });
                }
                None => pending.push(leaf_entry("turn", id, "not_found".to_owned())),
            }
        }
        // Mask turns whose commands never proved native completion, in the
        // same lock as the reads above.
        let mut groups: HashMap<(String, i64), Vec<String>> = HashMap::new();
        for bound in &bounds {
            groups
                .entry((bound.agent.clone(), bound.generation))
                .or_default()
                .push(bound.turn_id.clone());
        }
        for ((agent, generation), turn_ids) in &groups {
            let masked =
                store.turns_with_unknown_commands(agent, *generation, turn_ids, &marks.memory)?;
            for bound in bounds.iter_mut() {
                if bound.agent == *agent
                    && bound.generation == *generation
                    && masked.contains(&bound.turn_id)
                {
                    bound.turn.state = TurnState::Unknown;
                    bound.turn.status = None;
                }
            }
        }
        let mail_current = match mailbox {
            Some(_) => Some(store.mail_mailbox_high_water(&principal_name(principal), None)?),
            None => None,
        };
        let fair_cap = if fair_budget {
            let text_lens: Vec<usize> = pending
                .iter()
                .filter_map(|entry| {
                    if !entry.excerpt_allowed {
                        return None;
                    }
                    let bound = &bounds[entry.bound?];
                    if bound.turn.state != TurnState::Completed {
                        return None;
                    }
                    let text = bound.turn.final_text.as_deref()?;
                    (!text.is_empty()).then_some(text.len().min(MAX_WAIT_FINAL_TEXT_BYTES))
                })
                .collect();
            if text_lens.is_empty() {
                usize::MAX
            } else {
                fair_excerpt_cap(&text_lens, MAX_WAIT_FRAME_BYTES)
            }
        } else {
            0
        };
        // Bounded retained `final_text` for completed bound turns within one
        // response frame budget. A withheld tail marks `truncated` without its
        // body. Entries outside recipient scope emit status-only results.
        let mut remaining = MAX_WAIT_FRAME_BYTES;
        let mut results = Vec::with_capacity(pending.len());
        for entry in pending {
            let Some(index) = entry.bound else {
                results.push(
                    WaitEntryResult::new(entry.kind, entry.id, entry.leaf)
                        .with_branch(entry.branch),
                );
                continue;
            };
            let bound = &bounds[index];
            let status = if entry.command_unknown {
                "unknown".to_owned()
            } else {
                Self::turn_entry_status(&bound.turn).to_owned()
            };
            if !entry.excerpt_allowed {
                results.push(WaitEntryResult::new(entry.kind, entry.id, status));
                continue;
            }
            let mut result = WaitEntryResult::new(entry.kind, entry.id, status)
                .with_turn(bound.agent.clone(), bound.generation, bound.turn_id.clone())
                .with_branch(store.child_worktree_branch(&bound.agent, bound.generation));
            result.resolution_source = bound.turn.resolution_source;
            if bound.turn.state == TurnState::Completed
                && let Some(text) = bound.turn.final_text.as_deref()
            {
                let budget = if fair_budget { fair_cap } else { remaining };
                result = result.with_final_text(text, budget);
                if !fair_budget {
                    remaining -= result.final_text.as_ref().map_or(0, String::len);
                }
            }
            results.push(result);
        }
        Ok(WaitSnapshot {
            results,
            watch_agents,
            watch_commands,
            mailbox: mailbox.map(str::to_owned),
            mail_current,
            mail_arrived: false,
        })
    }
    fn turn_entry_status(turn: &Turn) -> &'static str {
        match turn.state {
            TurnState::Running => "still_running",
            TurnState::Unknown => "unknown",
            TurnState::Completed => {
                let outcome = turn
                    .outcome
                    .unwrap_or_else(|| TurnOutcome::classify_legacy_status(turn.status.as_deref()));
                match turn
                    .resolution_source
                    .map_or(outcome, |source| source.reported_outcome(outcome))
                {
                    TurnOutcome::Success => "succeeded",
                    TurnOutcome::Failure => "failed",
                    TurnOutcome::Interrupted => "interrupted",
                    TurnOutcome::Unknown => "unknown",
                }
            }
        }
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
            if !filter.agents.is_empty() {
                // Named Agents read through the `(agent, seq)` index, so the
                // per-call bound counts matched rows and other Agents' traffic
                // costs nothing. A noncanonical id matches no row.
                let agents: Vec<i64> = filter
                    .agents
                    .iter()
                    .filter_map(|agent| eliot_core::parse_agent_id(agent).ok())
                    .collect();
                let kinds: Vec<&str> = filter.kinds.iter().map(String::as_str).collect();
                let sources: Vec<&str> = filter.sources.iter().map(String::as_str).collect();
                let var_scopes: Vec<&str> = filter.var_scopes.iter().map(String::as_str).collect();
                while cursor < high_water && events.len() < limit {
                    let page = store.read_agent_events(
                        &agents,
                        &kinds,
                        &sources,
                        &var_scopes,
                        cursor,
                        high_water,
                        limit - events.len(),
                    )?;
                    if page.is_empty() {
                        cursor = high_water;
                        break;
                    }
                    for event in page {
                        let cost = encoded_event_cost(&event)?;
                        if encoded_bytes.saturating_add(cost) > byte_budget {
                            byte_limited = true;
                            break;
                        }
                        encoded_bytes += cost;
                        cursor = event.seq;
                        events.push(event);
                    }
                    if byte_limited {
                        break;
                    }
                }
            }
            while filter.agents.is_empty()
                && cursor < high_water
                && events.len() < limit
                && scanned < EVENT_SCAN_MAX
            {
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

    /// Owner-triggered retention pass: reload the daemon retention settings
    /// and run the same bounded epochs as the hourly tick, returning the
    /// pruned counts and the retained-event watermark.
    async fn maintenance_run(&self, params: &Value, principal: &Principal) -> Result<Value> {
        require_owner(principal)?;
        let request: MaintenanceRunRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let policy = self.secret_policy_snapshot();
        let report = crate::maintenance::run_now(
            &self.inner.store,
            &self.inner.data_root,
            &policy,
            now_ms(),
        )
        .await?;
        let response = MaintenanceRunResponse {
            events_deleted: report.events_deleted,
            pruned_through: report.pruned_through,
            commands_deleted: report.commands_deleted,
            writes_deleted: report.writes_deleted,
            mail_deleted: report.mail_deleted,
            digests_deleted: report.digests_deleted,
            processes_deleted: report.processes_deleted,
        };
        request.validate_response(&response)?;
        serde_json::to_value(response)
            .map_err(|_| Error::new("internal", "maintenance.run result could not be encoded"))
    }

    /// Preview or explicitly purge owner-sent legacy advisory-attention mail.
    async fn maintenance_purge_legacy_attention_advisories(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        require_owner(principal)?;
        let request: MaintenancePurgeLegacyAttentionRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let (matching_count, deleted_count) = self
            .inner
            .store
            .lock()
            .await
            .purge_legacy_attention_advisories(request.expected_message_count)?;
        let response = MaintenancePurgeLegacyAttentionResponse {
            matching_count,
            deleted_count,
            applied: request.expected_message_count.is_some(),
        };
        request.validate_response(&response)?;
        serde_json::to_value(response).map_err(|_| {
            Error::new(
                "internal",
                "legacy attention advisory purge result could not be encoded",
            )
        })
    }

    pub(super) fn set_previous_crash(&self, crash: Option<CrashMarker>) {
        *self.inner.previous_crash.lock_recover() = crash;
    }

    async fn shutdown(&self, params: &Value, principal: &Principal) -> Result<Value> {
        require_owner(principal)?;
        let request: DaemonShutdownRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let mode = request.mode;
        if matches!(
            mode,
            ShutdownMode::PrepareRestart | ShutdownMode::CancelRestart | ShutdownMode::Restart
        ) {
            return self.restart_shutdown(request, principal).await;
        }
        let id = request.id.to_owned();
        // Drain is a pure read-side report: no effects run before the
        // shutdown-ID claim, so an exact retry replays the retained reply
        // with no duplicate effects to guard.
        let result = if mode == ShutdownMode::Force {
            DaemonShutdownResponse {
                state: CommandState::Dispatched,
                mode,
                blockers: None,
                drain: None,
                store_write_faulted: None,
            }
        } else {
            let blockers = self.status_blockers().await?;
            let drain = self.drain_report().await?;
            DaemonShutdownResponse {
                state: if blockers.is_empty() {
                    CommandState::Applied
                } else {
                    CommandState::Dispatched
                },
                mode,
                blockers: Some(blockers),
                drain: Some(drain),
                store_write_faulted: None,
            }
        };
        let result = serde_json::to_value(result)
            .map_err(|_| Error::new("internal", "shutdown result could not be encoded"))?;
        let write = {
            let mut store = self.inner.store.lock().await;
            if matches!(
                self.inner.mode.load(Ordering::SeqCst),
                MODE_QUIESCE | MODE_RESTART
            ) {
                return Err(Error::new(
                    "operator_busy",
                    "a planned restart owns daemon admission",
                ));
            }
            let payload = json!({"mode": mode.as_str()});
            let write = match store.record_write(
                &id,
                &principal_name(principal),
                "daemon.shutdown",
                &payload,
                &result,
                now_ms(),
            ) {
                Ok(write) => Some(write.result),
                Err(_error) if mode == ShutdownMode::Force && store.is_write_faulted() => None,
                Err(error) => return Err(error),
            };
            // Command and rule admissions serialize their final mode check
            // under this same writer guard.
            let _run_workers = self.inner.run_workers.lock_recover();
            self.inner.mode.fetch_max(
                if mode == ShutdownMode::Force {
                    MODE_FORCE
                } else {
                    MODE_DRAIN
                },
                Ordering::SeqCst,
            );
            self.inner.shutdown_ready.store(false, Ordering::SeqCst);
            // The Owner asked: a desktop must not start this daemon again
            // on its own (a crash leaves no marker and is restarted).
            if let Err(error) = crate::platform::data_dir::mark_owner_stop(&self.inner.data_root) {
                crate::logging::record(
                    &self.secret_policy_snapshot(),
                    "daemon.owner_stop_mark_failed",
                    &[
                        ("principal", diagnostic_principal(principal).as_str()),
                        ("method", "daemon.shutdown"),
                    ],
                    Some(&error),
                );
            }
            if mode == ShutdownMode::Force {
                self.inner.workspaces.close_startup_admission();
                self.inner.plugin_host.close_startup_admission();
                self.inner.run_cancellation.send_replace(true);
            }
            write
        };
        // Decode the retained receipt, never replace it with the newly computed
        // candidate. A bad receipt must not undo an already admitted shutdown.
        let response = match write {
            Some(value) => serde_json::from_value::<DaemonShutdownResponse>(value)
                .map_err(|_| Error::new("internal", "retained shutdown result is invalid")),
            None => Ok(DaemonShutdownResponse {
                state: CommandState::Dispatched,
                mode: ShutdownMode::Force,
                blockers: None,
                drain: None,
                store_write_faulted: Some(true),
            }),
        };
        self.inner.plugin_host.close_discovery_admission();
        self.finish_var_writes().await;
        self.signal_progress();
        serde_json::to_value(response?)
            .map_err(|_| Error::new("internal", "shutdown result could not be encoded"))
    }

    async fn restart_shutdown(
        &self,
        request: DaemonShutdownRequest,
        principal: &Principal,
    ) -> Result<Value> {
        // In-flight RPC mutations may write settings as well as the Store.
        // Keep the backup behind their completion boundary.
        let _write_barrier =
            if request.backup {
                Some(self.inner.restart_writes.try_write().map_err(|_| {
                    Error::new("restart_busy", "an admitted mutation is still running")
                })?)
            } else {
                None
            };
        let mut store = self.inner.store.lock().await;
        let mut lease = self.inner.restart_lease.lock_recover();
        let mode = self.inner.mode.load(Ordering::SeqCst);
        let action = request.mode;
        if action == ShutdownMode::PrepareRestart {
            if mode != MODE_OPEN && !(mode == MODE_QUIESCE && lease.as_ref() == Some(&request.id)) {
                return Err(Error::new(
                    "bad_state",
                    "daemon admission is already closed",
                ));
            }
        } else if mode != MODE_QUIESCE || lease.as_ref() != request.restart_id.as_ref() {
            return Err(Error::new(
                "bad_state",
                "the update admission fence is no longer held",
            ));
        }
        if action == ShutdownMode::Restart
            && (store.has_restart_work()?
                || self.inner.workspaces.has_owned_processes()
                || self.inference_workers_active()
                || self.run_workers_active())
        {
            return Err(Error::new("restart_busy", "admitted work is still running"));
        }
        let watchdog = if action == ShutdownMode::Restart {
            Some(self.prepare_restart_exit_watchdog()?)
        } else {
            None
        };
        let result = json!({"state": "applied", "mode": action});
        if request.backup {
            crate::update_backup::create(&self.inner.data_root, &request.id, &store)?;
        }
        let owner = principal_name(principal);
        let payload =
            json!({"mode": action, "restart_id": request.restart_id, "backup": request.backup});
        let admission = if request.backup {
            store.record_restart_and_seal(&request.id, &owner, &payload, &result, now_ms())
        } else {
            store.record_write(
                &request.id,
                &owner,
                "daemon.shutdown",
                &payload,
                &result,
                now_ms(),
            )
        };
        let admission = match admission {
            Ok(admission) => admission,
            Err(error) => {
                if request.backup
                    && !store.is_write_faulted()
                    && let Err(cleanup) = crate::update_backup::discard_update_backup(
                        &self.inner.data_root,
                        &request.id,
                    )
                {
                    return Err(Error::new(
                        error.code,
                        format!("{}; Store backup cleanup failed: {cleanup}", error.message),
                    ));
                }
                return Err(error);
            }
        };
        if admission.is_new {
            match action {
                ShutdownMode::PrepareRestart => {
                    *lease = Some(request.id.clone());
                    self.inner.mode.store(MODE_QUIESCE, Ordering::SeqCst);
                    let router = self.clone();
                    tokio::spawn(async move {
                        #[expect(
                            clippy::disallowed_methods,
                            reason = "This bounded wait is owned by the surrounding operation."
                        )]
                        tokio::time::sleep(RESTART_LEASE).await;
                        let _store = router.inner.store.lock().await;
                        let mut lease = router.inner.restart_lease.lock_recover();
                        if router.inner.mode.load(Ordering::SeqCst) == MODE_QUIESCE
                            && lease.as_ref() == Some(&request.id)
                        {
                            *lease = None;
                            router.inner.mode.store(MODE_OPEN, Ordering::SeqCst);
                            router.signal_progress();
                        }
                    });
                }
                ShutdownMode::CancelRestart => {
                    *lease = None;
                    self.inner.mode.store(MODE_OPEN, Ordering::SeqCst);
                }
                ShutdownMode::Restart => {
                    self.inner.shutdown_ready.store(false, Ordering::SeqCst);
                    self.inner.mode.store(MODE_RESTART, Ordering::SeqCst);
                    *lease = None;
                    if let Some(watchdog) = watchdog {
                        let _ = watchdog.send(());
                    }
                }
                _ => unreachable!("ordinary shutdown handled separately"),
            }
        }
        self.signal_progress();
        Ok(admission.result)
    }

    async fn settle_run_worker_unknown(&self, run_id: &str, cause: &str) {
        let process = crate::scripts::unobserved_process_result(false);
        let outcome = json!({"code":"unknown", "cause":cause, "process":process});
        if self
            .settle(run_id, CommandState::Unknown, Some(outcome))
            .await
            .is_none()
        {
            self.note_unknown_if_uncertain(run_id, cause).await;
        }
    }
    pub(crate) fn schedule_command(&self, id: String, linked_id: Option<String>) {
        tokio::spawn(self.clone().process_command(id, linked_id));
    }

    fn schedule_rule_admission(&self, admission: &eliot_store::Admission) {
        self.note_rule_command_admission(admission);
        if let Some((id, linked_id)) = rule_admission_dispatch_plan(admission) {
            self.schedule_command(id, linked_id);
        }
    }

    pub(crate) fn run_cancellation_receiver(&self) -> watch::Receiver<bool> {
        self.inner.run_cancellation.subscribe()
    }

    pub(crate) fn secret_policy_snapshot(&self) -> Arc<SecretPolicy> {
        let secrets = self.inner.secrets.lock_recover();
        Arc::new(SecretPolicy::new(secrets.iter()))
    }
    pub(crate) fn script_secret_policy(&self, token: &str) -> Arc<SecretPolicy> {
        let mut secrets = self.inner.secrets.lock_recover().clone();
        secrets.insert(token.to_owned());
        Arc::new(SecretPolicy::new(secrets.iter()))
    }

    pub(crate) async fn verify_run_registration(
        &self,
        registration: &ProjectRegistrationView,
    ) -> Result<()> {
        crate::automation::verify_project_registration(&self.inner.data_root, registration).await
    }

    #[expect(
        clippy::disallowed_types,
        reason = "This Router method verifies and returns the frozen owner script command."
    )]
    pub(crate) async fn verify_global_run(
        &self,
        frozen: &FrozenScriptRun,
    ) -> Result<tokio::process::Command> {
        let grant = crate::config::load_automation_config(&self.inner.data_root)?.run;
        let policy = self.secret_policy_snapshot();
        crate::scripts::freeze::verify_global_run(
            frozen,
            &grant,
            &self.inner.data_root,
            policy.as_ref(),
        )
    }

    pub(crate) async fn admit_script_output(
        &self,
        run_id: &str,
        actions: Vec<SubmitCommand>,
        process: ScriptProcessResult,
    ) -> Result<eliot_store::ScriptRunAdmission> {
        let (result, write_faulted) = {
            let mut store = self.inner.store.lock().await;
            let result = if self.inner.mode.load(Ordering::SeqCst) == MODE_FORCE {
                Err(Error::new(
                    "not_allowed",
                    "force shutdown prevents script output admission",
                ))
            } else {
                store.admit_script_output(run_id, actions, process, now_ms())
            };
            (result, store.is_write_faulted())
        };
        match result {
            Ok(admission) => {
                self.publish_locked(&admission.events);
                if admission.is_new {
                    for child in &admission.children {
                        if matches!(child.state, CommandState::Accepted | CommandState::Queued) {
                            self.schedule_command(child.id.clone(), None);
                        }
                    }
                }
                self.signal_progress();
                Ok(admission)
            }
            Err(error) if write_faulted || error.code == "unknown" => {
                self.note_unknown_if_uncertain(run_id, "script_output_commit_unknown")
                    .await;
                Err(Error::new(
                    "unknown",
                    "script output admission outcome is unknown",
                ))
            }
            Err(error) => Err(error),
        }
    }
    pub(crate) async fn prepare_script_output(
        &self,
        actions: Vec<SubmitCommand>,
        saved_principal: &SavedRunPrincipal,
        frozen: &FrozenScriptRun,
    ) -> Result<Vec<SubmitCommand>> {
        let SavedRunPrincipal::ProjectRule { registration } = saved_principal else {
            let _ = self.verify_global_run(frozen).await?;
            let mut prepared = Vec::with_capacity(actions.len());
            for action in actions {
                prepared.push(self.prepare_owner_submission(action).await?.request);
            }
            return Ok(prepared);
        };
        let current = self
            .inner
            .store
            .lock()
            .await
            .get_project_registration("owner", &registration.id)?
            .ok_or_else(|| Error::new("not_allowed", "Run project registration is absent"))?;
        if &current != registration.as_ref() {
            return Err(Error::new(
                "not_allowed",
                "project registration changed during script execution",
            ));
        }
        let prepared = self
            .prepare_project_rule_submissions(actions, registration)
            .await?;
        Ok(prepared
            .into_iter()
            .map(|submission| submission.request)
            .collect())
    }
    fn redact_native_value(&self, value: &mut Value) {
        let secrets = self
            .inner
            .secrets
            .lock_recover()
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        crate::redaction::redact_outcome(value, &secrets);
    }

    pub(crate) async fn settle(
        &self,
        id: &str,
        state: CommandState,
        mut outcome: Option<Value>,
    ) -> Option<Command> {
        if let Some(outcome) = outcome.as_mut() {
            let envelope = json!({"command_id": id, "state": state, "outcome": null});
            let overhead = serde_json::to_vec(&envelope)
                .map_or(usize::MAX, |bytes| bytes.len().saturating_sub(4));
            let outcome_budget = MAX_EVENT_BYTES.saturating_sub(overhead);
            crate::redaction::bound_outcome_diagnostics(outcome, outcome_budget);
            eliot_core::bound_script_outcome_stderr(outcome, outcome_budget);
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
        if let Ok(change) = &result
            && !change.events.is_empty()
        {
            self.note_script_failure(&change.value);
            self.note_command_rejection(&change.value, &change.events);
        }
        self.finish_settlement(id, result, write_faulted, &persistence_cause, true)
            .await
    }

    /// Settle a dispatched or already-unknown stop only after Store-owned
    /// process-custody exit and native-source drain proof both read back true.
    pub(crate) async fn settle_stop_with_proof(
        &self,
        id: &str,
        mut outcome: Value,
    ) -> Option<Command> {
        let envelope = json!({"command_id": id, "state": CommandState::Applied, "outcome": null});
        let overhead =
            serde_json::to_vec(&envelope).map_or(usize::MAX, |bytes| bytes.len().saturating_sub(4));
        let outcome_budget = MAX_EVENT_BYTES.saturating_sub(overhead);
        crate::redaction::bound_outcome_diagnostics(&mut outcome, outcome_budget);
        eliot_core::bound_script_outcome_stderr(&mut outcome, outcome_budget);
        let (result, write_faulted) = {
            let mut store = self.inner.store.lock().await;
            let result = store.settle_stop_after_proven_exit(id, outcome, now_ms());
            if let Ok(change) = &result {
                self.publish_locked(&change.events);
            }
            (result, store.is_write_faulted())
        };
        self.finish_settlement(
            id,
            result,
            write_faulted,
            "stop_process_source_proof_missing",
            true,
        )
        .await
    }

    async fn finish_settlement(
        &self,
        id: &str,
        result: Result<eliot_store::Change<Command>>,
        write_faulted: bool,
        persistence_cause: &str,
        external_effect_may_be_uncertain: bool,
    ) -> Option<Command> {
        match result {
            Ok(change) => {
                if change.value.state.is_terminal() {
                    self.clear_unknown_id(id);
                    if let (Some(agent), Some(generation)) =
                        (change.value.target.as_deref(), change.value.generation)
                    {
                        self.forget_token_if_revoked(agent, generation).await;
                    }
                }
                let command = change.value;
                self.signal_progress();
                Some(command)
            }
            Err(_) => {
                if write_faulted && external_effect_may_be_uncertain {
                    self.note_unknown_if_uncertain(id, persistence_cause).await;
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
            plugin_host::forget_revoked_agent_token(
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

    pub(crate) fn note_unknown(&self, events: &[Event]) {
        let mut unknown = self.inner.unknown_commands.lock_recover();
        let mut causes = self.inner.unknown_causes.lock_recover();
        for event in events {
            let Some(id) = event.data.get("command_id").and_then(Value::as_str) else {
                continue;
            };
            if event.kind == "command.unknown" {
                unknown.insert(id.to_owned());
            } else if matches!(
                event.kind.as_str(),
                "command.applied"
                    | "command.failed"
                    | "command.rejected"
                    | "command.cancelled"
                    | "command.resolved"
            ) {
                unknown.remove(id);
                causes.remove(id);
            }
        }
    }

    fn note_unknown_id(&self, id: &str, cause: &str) {
        let mut unknown = self.inner.unknown_commands.lock_recover();
        let mut causes = self.inner.unknown_causes.lock_recover();
        unknown.insert(id.to_owned());
        causes.insert(id.to_owned(), cause.to_owned());
    }

    pub(crate) async fn note_unknown_if_uncertain(&self, id: &str, cause: &str) {
        let store = self.inner.store.lock().await;
        match store.get_command(id) {
            Ok(Some(command))
                if matches!(
                    command.state,
                    CommandState::Dispatched | CommandState::Unknown
                ) =>
            {
                self.note_unknown_id(id, cause);
            }
            Err(_) => self.note_unknown_id(id, cause),
            Ok(Some(_)) | Ok(None) => {}
        }
    }

    fn clear_unknown_id(&self, id: &str) {
        let mut unknown = self.inner.unknown_commands.lock_recover();
        let mut causes = self.inner.unknown_causes.lock_recover();
        unknown.remove(id);
        causes.remove(id);
    }

    pub(crate) async fn clear_terminal_unknown_cache(&self) {
        let ids = self.unknown_ids();
        if ids.is_empty() {
            return;
        }
        let terminal = {
            let store = self.inner.store.lock().await;
            ids.into_iter()
                .filter(|id| {
                    store
                        .get_command(id)
                        .ok()
                        .flatten()
                        .is_some_and(|command| command.state.is_terminal())
                })
                .collect::<Vec<_>>()
        };
        for id in terminal {
            self.clear_unknown_id(&id);
        }
    }

    fn unknown_cause(&self, id: &str) -> Option<String> {
        self.inner.unknown_causes.lock_recover().get(id).cloned()
    }

    async fn mask_unknown_turns(&self, agent: &str, turns: &mut [Turn]) -> Result<()> {
        self.clear_terminal_unknown_cache().await;
        if turns.is_empty() {
            return Ok(());
        }
        let memory_unknown = self.unknown_ids();
        let turn_keys = turns
            .iter()
            .map(|turn| (turn.generation, turn.turn_id.clone()))
            .collect::<Vec<_>>();
        let unknown_turns = {
            let store = self.inner.store.lock().await;
            store.turns_with_unknown_commands_for_page(agent, &turn_keys, &memory_unknown)?
        };
        for turn in turns {
            if unknown_turns.contains(&(turn.generation, turn.turn_id.clone())) {
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
            .lock_recover()
            .iter()
            .cloned()
            .collect();
        ids.sort();
        ids
    }

    fn mode_name(&self) -> &'static str {
        match self.inner.mode.load(Ordering::SeqCst) {
            MODE_QUIESCE => "quiescing",
            MODE_RESTART => "restarting",
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
                .is_some_and(|agent| self.agents.contains(agent))
            || (event.agent.is_none()
                && event.kind == "var.changed"
                && event
                    .data
                    .get("scope")
                    .and_then(Value::as_str)
                    .is_some_and(|scope| self.var_scopes.contains(scope))))
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

impl Router {
    fn resolve_profile_cached(&self, name: &str) -> Result<Value> {
        crate::config::resolve_profile_cached(
            &self.inner.profile_cache,
            &self.inner.data_root,
            name,
        )
    }
}

async fn freeze_attach_profile(
    cache: &Mutex<crate::config::ProfileCache>,
    data_root: &std::path::Path,
    request: &mut SubmitCommand,
) -> Result<()> {
    let profile_name = required_text(&request.args, "profile")?;
    let harness = required_text(&request.args, "harness")?.to_owned();
    let mut snapshot = crate::config::resolve_profile_cached(cache, data_root, profile_name)?;
    if snapshot.get("harness").and_then(Value::as_str) != Some(harness.as_str()) {
        return Err(Error::new(
            "invalid",
            "attach harness does not match the selected profile",
        ));
    }
    let candidate = crate::adhoc::resolve_profile_command(
        profile_name,
        &harness,
        snapshot.get("cmd").and_then(Value::as_str),
    )
    .await?;
    crate::adhoc::write_profile_command_snapshot(&mut snapshot, candidate)?;
    crate::config::capture_native_store_inputs(&mut snapshot, |name| std::env::var_os(name))?;
    install_frozen_profile(request, &harness, snapshot, true)
}

fn command_needs_worker(command: &Command) -> bool {
    command.verb != "inject"
        || eliot_core::decode_retained_inject_args(&command.args)
            .is_ok_and(|args| args.point == eliot_core::InjectPoint::ActiveTurn)
}

fn attention_inject_point(agent: &Agent) -> Option<eliot_core::InjectPoint> {
    let points = agent
        .effective
        .as_ref()?
        .get("context_points")?
        .as_array()?;
    [
        ("turn_safe_point", eliot_core::InjectPoint::TurnSafePoint),
        ("active_turn", eliot_core::InjectPoint::ActiveTurn),
    ]
    .into_iter()
    .find_map(|(name, point)| points.iter().any(|value| value == name).then_some(point))
}

fn rule_admission_dispatch_plan(
    admission: &eliot_store::Admission,
) -> Option<(String, Option<String>)> {
    let command = &admission.command;
    if matches!(command.state, CommandState::Accepted | CommandState::Queued) {
        let linked_id = admission
            .linked_command
            .as_ref()
            .filter(|linked| matches!(linked.state, CommandState::Accepted | CommandState::Queued))
            .map(|linked| linked.id.clone());
        return Some((command.id.clone(), linked_id));
    }
    if is_start_verb(command.verb.as_str())
        && command.state == CommandState::Applied
        && let Some(linked) = &admission.linked_command
        && matches!(linked.state, CommandState::Accepted | CommandState::Queued)
    {
        return Some((linked.id.clone(), None));
    }
    None
}

fn encoded_event_cost(event: &Event) -> Result<usize> {
    let encoded = serde_json::to_vec(event)
        .map_err(|error| Error::new("internal", format!("encode event page entry: {error}")))?;
    Ok(encoded.len().saturating_add(1))
}

/// Project-rule verbs that address a live Agent: authorization and
/// catch-up preparation both require their frozen target and generation.
/// Every other project verb (`run`, `launch`, `git.fetch`, `notify`,
/// `emit`, `mail.send`, `var.set`) is targetless on both paths. A newly
/// authorized Agent-bound verb must extend this one list, never a second
/// copy.
pub(crate) fn project_rule_action_binds_agent_target(verb: &str) -> bool {
    matches!(
        verb,
        "resume"
            | "stop"
            | "send"
            | "steer"
            | "inject"
            | "interrupt"
            | "approve"
            | "deny"
            | "answer"
    )
}

fn validate_rule_launch_action(
    request: &SubmitCommand,
    project: Option<&ProjectRegistrationView>,
) -> Result<()> {
    if request.verb != "launch" {
        return Ok(());
    }
    if !request.id.is_empty()
        || request.target.is_some()
        || request.expected_generation != Some(0)
        || request.expected_turn_id.is_some()
        || request.request_id.is_some()
        || request.probe
    {
        return Err(Error::new(
            "invalid",
            "rule launch must be targetless, generation 0, and unbound",
        ));
    }
    let args = request
        .args
        .as_object()
        .ok_or_else(|| Error::new("invalid", "rule launch args must be an object"))?;
    if args.keys().any(|key| {
        !matches!(
            key.as_str(),
            "profile" | "role" | "workspace" | "text" | "assignment" | "goal" | "no_policy"
        )
    }) {
        return Err(Error::new(
            "invalid",
            "rule launch accepts profile or role, workspace, text, assignment, goal and no_policy",
        ));
    }
    project_checks::validate_launch_inputs(&request.args, project.is_none())?;
    let profile = args
        .get("profile")
        .map(|value| {
            value
                .as_str()
                .filter(|name| {
                    !name.trim().is_empty()
                        && name.trim() == *name
                        && !name.contains(concat!("$", "{"))
                })
                .ok_or_else(|| Error::new("invalid", "rule launch profile must be a literal name"))
        })
        .transpose()?;
    if let Some(role) = args.get("role") {
        crate::config::validate_routing_role(
            role.as_str()
                .ok_or_else(|| Error::new("invalid", "rule launch role must be a literal name"))?,
        )?;
        if profile.is_some() {
            return Err(Error::new("invalid", "rule launch uses profile or role"));
        }
    }
    if args.contains_key("assignment") && project.is_some() {
        return Err(Error::new(
            "not_allowed",
            "folder assignment requires Owner automation",
        ));
    }
    let workspace = args
        .get("workspace")
        .and_then(Value::as_str)
        .filter(|workspace| {
            !workspace.trim().is_empty()
                && workspace.trim() == *workspace
                && !workspace.contains(concat!("$", "{"))
        })
        .ok_or_else(|| Error::new("invalid", "rule launch workspace must be a literal path"))?;
    let text = match args.get("text") {
        None => None,
        Some(value) => Some(
            value
                .as_str()
                .filter(|text| !text.trim().is_empty())
                .ok_or_else(|| Error::new("invalid", "rule launch text must be nonempty"))?,
        ),
    };
    if let Some(registration) = project {
        registration.validate()?;
        if !registration.actions.iter().any(|action| action == "launch") {
            return Err(Error::new(
                "not_allowed",
                "project rule is not approved for launch",
            ));
        }
        if profile.is_some_and(|profile| {
            !registration
                .profiles
                .iter()
                .any(|approved| approved == profile)
        }) {
            return Err(Error::new(
                "not_allowed",
                "project registration does not approve the launch profile",
            ));
        }
        // The project API displays Windows paths without the verbatim prefix.
        // Compare the same canonical checkout that Store will fence at admission.
        let canonical = crate::setup::canonical_directory(std::path::Path::new(workspace))
            .map_err(|_| {
                Error::new(
                    "not_allowed",
                    "project launch workspace is unavailable or unsafe",
                )
            })?;
        if canonical != std::path::Path::new(&registration.checkout) {
            return Err(Error::new(
                "not_allowed",
                "project launch must use the registered checkout",
            ));
        }
        if text.is_some() && !registration.actions.iter().any(|action| action == "send") {
            return Err(Error::new(
                "not_allowed",
                "project launch text requires the send action grant",
            ));
        }
    }
    Ok(())
}

/// A prepared rule launch: the daemon may have frozen the standard managed
/// worktree of a worktree-mode project (an Owner rule cannot name one), and
/// Store prepares it exactly as for an interactive launch.
fn validate_rule_launch_preparation(prepared: &PreparedSubmission) -> Result<()> {
    if prepared.request.verb != "launch" {
        return Ok(());
    }
    validate_prepared_rule_launch_command(&prepared.request)
}

fn validate_prepared_rule_launch_command(request: &SubmitCommand) -> Result<()> {
    if request.verb != "launch" {
        return Ok(());
    }
    if !request.id.is_empty()
        || request.target.is_some()
        || request.expected_generation != Some(0)
        || request.expected_turn_id.is_some()
        || request.request_id.is_some()
        || request.probe
    {
        return Err(Error::new(
            "invalid",
            "rule launch must be targetless, generation 0, and unbound",
        ));
    }
    let args = request
        .args
        .as_object()
        .ok_or_else(|| Error::new("invalid", "prepared rule launch args must be an object"))?;
    if args.contains_key("worktree")
        || args.contains_key("base")
        || args
            .get("profile_snapshot")
            .and_then(|snapshot| snapshot.get("setup"))
            .and_then(Value::as_array)
            .is_some_and(|setup| !setup.is_empty())
    {
        return Err(Error::new(
            "unsupported",
            "rule launch cannot name a worktree or base or run profile setup",
        ));
    }
    let workspace = args
        .get("workspace")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::new("invalid", "prepared rule launch has no workspace"))?;
    if args.get("project").and_then(Value::as_str) != Some(workspace) {
        return Err(Error::new(
            "source_integrity",
            "prepared rule launch project and workspace differ",
        ));
    }
    Ok(())
}

fn validate_project_rule_launch_command(
    registration: &ProjectRegistrationView,
    command: &Command,
) -> Result<()> {
    if command.verb != "launch" {
        return Ok(());
    }
    registration.validate()?;
    if !registration.actions.iter().any(|action| action == "launch") {
        return Err(Error::new(
            "not_allowed",
            "project registration no longer approves launch",
        ));
    }
    let profile = command
        .args
        .get("profile")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::new("source_integrity", "project launch has no frozen profile"))?;
    if !registration
        .profiles
        .iter()
        .any(|approved| approved == profile)
    {
        return Err(Error::new(
            "not_allowed",
            "project registration no longer approves the launch profile",
        ));
    }
    let workspace = command
        .args
        .get("workspace")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::new("source_integrity", "project launch has no workspace"))?;
    if workspace != registration.checkout.as_str()
        || command.args.get("project").and_then(Value::as_str) != Some(workspace)
    {
        return Err(Error::new(
            "not_allowed",
            "project launch workspace differs from the registered checkout",
        ));
    }
    if command.args.get("workspace_preparation").is_some()
        || command.args.get("worktree").is_some()
        || command.args.get("base").is_some()
        || command
            .args
            .get("profile_snapshot")
            .and_then(|snapshot| snapshot.get("setup"))
            .and_then(Value::as_array)
            .is_some_and(|setup| !setup.is_empty())
    {
        return Err(Error::new(
            "unsupported",
            "project rule launch cannot prepare a worktree or run profile setup",
        ));
    }
    if command.args.get("text").is_some()
        && !registration.actions.iter().any(|action| action == "send")
    {
        return Err(Error::new(
            "not_allowed",
            "project launch text requires the send action grant",
        ));
    }
    Ok(())
}

fn validate_rule_resume_action(
    request: &SubmitCommand,
    project: Option<&ProjectRegistrationView>,
) -> Result<()> {
    if request.verb != "resume" {
        return Ok(());
    }
    if !request.id.is_empty()
        || !request
            .target
            .as_deref()
            .is_some_and(|target| eliot_core::parse_agent_id(target).is_ok())
        || request
            .expected_generation
            .is_none_or(|generation| generation < 0)
        || request.expected_turn_id.is_some()
        || request.request_id.is_some()
        || request.probe
    {
        return Err(Error::new(
            "invalid",
            "rule resume must bind the captured Agent and generation without turn or request fields",
        ));
    }
    let args = request
        .args
        .as_object()
        .ok_or_else(|| Error::new("invalid", "rule resume args must be an object"))?;
    if args.keys().any(|key| key != "text") {
        return Err(Error::new(
            "unsupported",
            "rule resume accepts only optional new text; profile, subject and workspace are retained",
        ));
    }
    let text = match args.get("text") {
        None => None,
        Some(Value::String(text)) if !text.trim().is_empty() => Some(text),
        Some(_) => {
            return Err(Error::new(
                "invalid",
                "rule resume text must be a nonempty string when supplied",
            ));
        }
    };
    if text.is_some_and(|text| text.len() > MAX_EVENT_BYTES) {
        return Err(Error::new(
            "frame_too_large",
            "rule resume text exceeds the 64 KiB persisted-text limit",
        ));
    }
    if let Some(registration) = project {
        registration.validate()?;
        if !registration.actions.iter().any(|action| action == "resume") {
            return Err(Error::new(
                "not_allowed",
                "project rule is not approved for resume",
            ));
        }
        if text.is_some() && !registration.actions.iter().any(|action| action == "send") {
            return Err(Error::new(
                "not_allowed",
                "project resume text requires the send action grant",
            ));
        }
    }
    Ok(())
}

fn validate_rule_resume_preparation(
    prepared: &PreparedSubmission,
    project: Option<&ProjectRegistrationView>,
) -> Result<()> {
    if prepared.request.verb != "resume" {
        return Ok(());
    }
    validate_rule_resume_action(&prepared.request, project)?;
    if prepared.workspace.is_some() {
        return Err(Error::new(
            "unsupported",
            "rule resume cannot create or prepare a workspace",
        ));
    }
    Ok(())
}

fn validate_retained_resume_agent(agent: &Agent, command: &Command) -> Result<i64> {
    if command.verb != "resume" {
        return Err(Error::new("bad_state", "command is not a resume"));
    }
    if !matches!(command.state, CommandState::Accepted | CommandState::Queued) {
        return Err(Error::new(
            "bad_state",
            "resume source state must be captured before dispatch",
        ));
    }
    let target = command
        .target
        .as_deref()
        .filter(|target| eliot_core::parse_agent_id(target).is_ok())
        .ok_or_else(|| Error::new("not_found", "resume target Agent is missing"))?;
    if target != agent.id {
        return Err(Error::new(
            "source_integrity",
            "resume command target differs from its retained Agent",
        ));
    }
    if agent.observe_only || agent.state == eliot_core::AgentState::Unknown {
        return Err(Error::new(
            "not_allowed",
            "observe-only or unknown Agents cannot resume",
        ));
    }
    let generation = command
        .expected_generation
        .or(command.generation)
        .ok_or_else(|| {
            Error::new(
                "stale_generation",
                "resume did not capture its old generation",
            )
        })?;
    if agent.generation != generation || command.generation != Some(generation) {
        return Err(Error::new(
            "stale_generation",
            "resume target no longer matches its retained generation",
        ));
    }
    if !matches!(
        agent.state,
        eliot_core::AgentState::Stopped
            | eliot_core::AgentState::Failed
            | eliot_core::AgentState::Lost
    ) || agent.active_turn.is_some()
    {
        return Err(Error::new(
            "bad_state",
            "resume requires a terminal Agent without an active turn",
        ));
    }
    let native_scope = agent
        .native_scope
        .as_deref()
        .filter(|scope| !scope.trim().is_empty())
        .ok_or_else(|| Error::new("unsupported", "resume has no retained native scope"))?;
    if agent
        .native_ref
        .as_deref()
        .is_none_or(|native_ref| native_ref.trim().is_empty())
    {
        return Err(Error::new(
            "unsupported",
            "resume has no retained native subject",
        ));
    }
    let capabilities = HarnessCapabilities::for_harness(&agent.harness).ok_or_else(|| {
        Error::new(
            "unsupported",
            format!("resume is not supported for the {} harness", agent.harness),
        )
    })?;
    if capabilities.requires_frozen_store_inputs {
        let binding: NativeStoreBinding = agent
            .profile_snapshot
            .get("native_store_binding")
            .cloned()
            .ok_or_else(|| {
                Error::new(
                    "unsupported",
                    "Codex resume requires a retained native store binding",
                )
            })
            .and_then(|value| {
                serde_json::from_value(value).map_err(|_| {
                    Error::new(
                        "source_integrity",
                        "retained native store binding is malformed",
                    )
                })
            })?;
        if native_scope != binding.native_scope {
            return Err(Error::new(
                "source_integrity",
                "resume native scope differs from its retained store binding",
            ));
        }
    } else if agent.profile_snapshot.get("native_store_binding").is_some() {
        return Err(Error::new(
            "source_integrity",
            "non-Codex resume cannot use a Codex native store binding",
        ));
    }
    Ok(generation)
}

fn validate_project_rule_resume_profile(
    registration: &ProjectRegistrationView,
    agent: &Agent,
) -> Result<()> {
    if !registration
        .profiles
        .iter()
        .any(|approved| approved == &agent.profile)
    {
        return Err(Error::new(
            "not_allowed",
            "project registration does not approve the retained Agent profile",
        ));
    }
    Ok(())
}

fn validate_project_rule_resume_command(
    registration: &ProjectRegistrationView,
    command: &Command,
    linked_input_pending: bool,
) -> Result<()> {
    if command.verb != "resume" {
        return Ok(());
    }
    registration.validate()?;
    if !registration.actions.iter().any(|action| action == "resume") {
        return Err(Error::new(
            "not_allowed",
            "project registration no longer approves resume",
        ));
    }
    if linked_input_pending && !registration.actions.iter().any(|action| action == "send") {
        return Err(Error::new(
            "not_allowed",
            "project registration no longer approves the linked resume input",
        ));
    }
    if !command.args.as_object().is_some_and(|args| args.is_empty()) {
        return Err(Error::new(
            "source_integrity",
            "project resume command must retain its input in the linked send",
        ));
    }
    Ok(())
}

/// Whether a registration id was minted by project auto-discovery
/// (`project-<sha256>`): such ids are opaque and never name the standard
/// directory.
fn is_auto_project_id(id: &str) -> bool {
    id.len() == "project-".len() + 64
        && id.starts_with("project-")
        && id["project-".len()..]
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
}

/// The checkout folder name for a launch workspace path.
fn workspace_folder_name(workspace: &str) -> Option<String> {
    std::path::Path::new(workspace)
        .file_name()
        .and_then(|name| name.to_str())
        .map(str::to_owned)
}

/// Format display paths in an `agent.list` row. Project and scope are exact
/// filter keys, including when a legacy Agent uses a path as its project.
pub(crate) fn display_agent_summary(mut summary: AgentSummary) -> AgentSummary {
    if let Some(workspace) = summary.workspace.as_deref() {
        summary.workspace = Some(eliot_core::display_path(workspace));
    }
    if let Some(repo_common_dir) = summary.repo_common_dir.as_deref() {
        summary.repo_common_dir = Some(eliot_core::display_path(repo_common_dir));
    }
    summary
}

/// The joined Git observation names the workspace it read; it renders like
/// the Agent's own workspace.
fn display_observation(observation: &mut ObservationView) {
    if let ObservationView::Data(data) = observation
        && let GitView::Data(git) = &mut data.git
    {
        git.workspace = eliot_core::display_path(&git.workspace);
    }
}

/// Format display paths in an `agent.get` view using the same spelling as the
/// summary. Keep its project and scope usable as exact `agent.list` filters.
fn display_agent_get_response(response: &mut AgentGetResponse) {
    if let Some(workspace) = response.agent.workspace.as_deref() {
        response.agent.workspace = Some(eliot_core::display_path(workspace));
    }
    display_observation(&mut response.observation);
    if let Some(facts) = &mut response.instruction_delivery
        && let Some(workspace) = facts.source_workspace.as_deref()
    {
        facts.source_workspace = Some(eliot_core::display_path(workspace));
    }
}

/// Owner-facing spelling of one project registration view: `checkout`,
/// `repo_common_dir` and every live checkout path lose the verbatim prefix.
/// Stored registration values and Git identity comparisons stay untouched.
fn display_project_registration(registration: &mut ProjectRegistrationView) {
    registration.checkout = eliot_core::display_path(&registration.checkout);
    registration.repo_common_dir = eliot_core::display_path(&registration.repo_common_dir);
    for checkout in &mut registration.checkouts {
        checkout.path = eliot_core::display_path(&checkout.path);
    }
}

/// A path as the daemon's Git identity compares it: no verbatim prefix, one
/// separator, no trailing separator, case-insensitive on Windows. This is
/// the same spelling the desktop sidebar uses for `project:repo:*` keys.
fn display_path_key(path: &str) -> String {
    let path = if path.starts_with(r"\\?\UNC\") {
        path.to_owned()
    } else {
        path.strip_prefix(r"\\?\").unwrap_or(path).to_owned()
    };
    let unified = path.replace('\\', "/");
    let trimmed = unified.trim_end_matches('/');
    if cfg!(windows) {
        trimmed.to_lowercase()
    } else {
        trimmed.to_owned()
    }
}

/// The repository folder implied by a Git common directory:
/// `<folder>/.git` is checked out at `folder`; any other common directory
/// names itself without a trailing `.git`. Mirrors the desktop sidebar.
fn repository_folder(common_dir: &str) -> Option<String> {
    let shown = if common_dir.starts_with(r"\\?\UNC\") {
        common_dir.to_owned()
    } else {
        common_dir
            .strip_prefix(r"\\?\")
            .unwrap_or(common_dir)
            .to_owned()
    };
    let shown = shown.trim_end_matches(['\\', '/']);
    let mut parts = shown.rsplitn(2, ['\\', '/']);
    let last = parts.next().unwrap_or(shown);
    if last.eq_ignore_ascii_case(".git")
        && let Some(main) = parts.next().filter(|main| !main.is_empty())
    {
        return workspace_folder_name(main);
    }
    let name = last
        .strip_suffix(".git")
        .filter(|name| !name.is_empty())
        .unwrap_or(last);
    (!name.is_empty()).then(|| name.to_owned())
}

/// The project's logical name for a standard worktree directory
/// (docs/spec/configuration.md): the owner's `[desktop] display_names`
/// entry first, else the `origin` repository name read live from Git without
/// credentials, else the main checkout's folder. A registration ID names
/// nothing a person reads and is never used.
async fn project_logical_name(
    data_root: &std::path::Path,
    registration_id: Option<&str>,
    workspace: &str,
) -> Result<String> {
    let git = crate::workspace_git::WorkspaceGit::new().ok();
    let mut common_dir: Option<String> = None;
    if let Some(git) = git.as_ref()
        && let Ok((_, common)) = git.repository_identity(workspace).await
    {
        common_dir = Some(common);
    }
    // The owner's display name wins when one names this project.
    {
        let mut keys = Vec::new();
        if let Some(common) = common_dir.as_deref() {
            keys.push(format!("project:repo:{}", display_path_key(common)));
        }
        if let Some(id) = registration_id.filter(|id| !id.trim().is_empty()) {
            keys.push(format!("project:{id}"));
        }
        keys.push(format!("project:folder:{}", display_path_key(workspace)));
        if let Some(name) = crate::config::load_project_display_name(data_root, &keys)
            && !name.trim().is_empty()
        {
            return Ok(name);
        }
    }
    // Else the repository name of the `origin` remote.
    if let (Some(git), Some(common)) = (git.as_ref(), common_dir.as_deref()) {
        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(30);
        if let Some(origin) = git.origin_name(common, deadline).await
            && !origin.trim().is_empty()
        {
            return Ok(origin);
        }
    }
    // Else the main checkout's folder: registration IDs (including
    // configured ones) never name the directory.
    if let Some(common) = common_dir.as_deref()
        && let Some(folder) = repository_folder(common)
        && !folder.trim().is_empty()
    {
        return Ok(folder);
    }
    workspace_folder_name(workspace)
        .filter(|folder| !folder.trim().is_empty())
        .ok_or_else(|| Error::new("invalid", "launch workspace has no project name"))
}

/// Naming parts for a standard worktree directory: the project's logical
/// name (display name, else origin, else main checkout folder) and the
/// prompt text the Store slugs into the worktree path leaf
/// `<agent>-<task-slug>`.
/// A daemon-set `auto_worktree` hint (manager launches) wins when present.
async fn generated_naming(
    data_root: &std::path::Path,
    request: &SubmitCommand,
) -> Result<(String, String)> {
    if let Some(hint) = request.args.get("auto_worktree") {
        let project = hint
            .get("project")
            .and_then(Value::as_str)
            .filter(|project| !project.trim().is_empty())
            .ok_or_else(|| Error::new("internal", "auto worktree hint has no project"))?;
        let task = hint
            .get("task")
            .and_then(Value::as_str)
            .unwrap_or("manager");
        // A hint already carries the resolved logical name (manager
        // launches resolve it before hinting). An opaque auto-discovered id
        // that reaches here anyway falls back to the checkout folder.
        if is_auto_project_id(project)
            && let Some(folder) = request
                .args
                .get("workspace")
                .and_then(Value::as_str)
                .and_then(workspace_folder_name)
        {
            return Ok((folder, task.to_owned()));
        }
        return Ok((project.to_owned(), task.to_owned()));
    }
    let workspace = request
        .args
        .get("workspace")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let registered = request
        .args
        .get("registered_project")
        .and_then(Value::as_str);
    let project = project_logical_name(data_root, registered, workspace).await?;
    let task = request
        .args
        .get("text")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    Ok((project, task))
}

fn normalize_launch(request: &mut SubmitCommand, principal: &Principal) -> Result<()> {
    if request.verb != "launch" {
        return Ok(());
    }
    reject_launch_derived_keys(&request.args)?;
    project_checks::validate_launch_inputs(&request.args, matches!(principal, Principal::Owner))?;
    if let Some(role) = request.args.get("role") {
        let role = role
            .as_str()
            .ok_or_else(|| Error::new("invalid", "launch role must be a name"))?;
        crate::config::validate_routing_role(role)?;
        if request.args.get("profile").is_some() || request.args.get("harness").is_some() {
            return Err(Error::new(
                "invalid",
                "launch role requires an omitted profile and harness",
            ));
        }
    }
    if let Some(worktree) = request.args.get("worktree") {
        let Some(branch) = worktree.as_str() else {
            return Err(Error::new("invalid", "worktree must name a new branch"));
        };
        // A bare `--worktree` carries an empty value and means "generate the
        // standard branch". Any other blank value is rejected.
        if !branch.is_empty() && branch.trim().is_empty() {
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
    // Owner launches without a harness may omit the profile; the freeze step
    // resolves their single configured routing.default candidate. Agent
    // children may also omit it to inherit the parent's frozen profile.
    // Ad-hoc Owner launch names a harness instead of a profile.
    if let Some(harness) = args.get("harness") {
        let Some(name) = harness.as_str().filter(|name| !name.trim().is_empty()) else {
            return Err(Error::new(
                "invalid",
                "launch harness must be a non-empty string",
            ));
        };
        crate::adhoc::validate_adhoc_harness(name)?;
        if args.get("profile").is_some() {
            return Err(Error::new(
                "invalid",
                "launch profile and harness are mutually exclusive",
            ));
        }
        if args.get("parent").is_some() {
            return Err(Error::new("invalid", "ad-hoc launch cannot have a parent"));
        }
        for field in ["model", "effort", "auth", "access"] {
            if let Some(value) = args.get(field)
                && value.as_str().is_none_or(|text| text.trim().is_empty())
            {
                return Err(Error::new(
                    "invalid",
                    format!("launch {field} must be a non-empty string"),
                ));
            }
        }
        if let Some(access) = args.get("access").and_then(Value::as_str) {
            crate::adhoc::validate_adhoc_access(access)?;
        }
    } else {
        for field in ["model", "effort", "auth", "access"] {
            if args.get(field).is_some() {
                return Err(Error::new(
                    "invalid",
                    format!("launch {field} requires a harness for ad-hoc launch"),
                ));
            }
        }
        if !args.contains_key("profile")
            && let Some(field) = eliot_core::PROVIDER_LAUNCH_OVERRIDE_FIELDS
                .iter()
                .find(|field| args.contains_key(**field))
        {
            return Err(Error::new(
                "invalid",
                format!("launch {field} requires an ad-hoc harness or saved role profile"),
            ));
        }
        if let Some(profile) = args.get("profile") {
            let Some(name) = profile.as_str().filter(|name| !name.trim().is_empty()) else {
                return Err(Error::new(
                    "invalid",
                    "launch profile must be a non-empty string",
                ));
            };
            let _ = name;
        }
    }
    args.insert("workspace".to_owned(), Value::String(workspace.clone()));
    args.insert(
        "project".to_owned(),
        Value::String(if args.get("chat") == Some(&Value::Bool(true)) {
            String::new()
        } else {
            workspace
        }),
    );
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

/// Persisted kind of the internal attention event.
pub(crate) const ATTENTION_USER_EVENT_KIND: &str = "x.user.agent.attention";

/// Map one validated local `notify`/`emit`/`attention.emit` DTO to its
/// persisted event kind and bounded payload. Shared by admission and
/// execution so both read the same DTO exactly once. The internal
/// `attention.emit` verb carries the same DTO as `emit`; only the producer
/// (not the kind) differs, and admission keeps them apart.
fn user_event_kind_data(verb: &str, args: &Value) -> Result<(String, Value)> {
    match verb {
        "notify" => {
            let definition = eliot_core::api::discovery::decode_params_at::<NotifyDefinitionArgs>(
                args.clone(),
                "params.args",
            )?;
            definition.validate()?;
            let mut data = json!({"message": definition.message});
            if let Some(title) = definition.title {
                data["title"] = Value::String(title);
            }
            Ok(("x.user.notify".to_owned(), data))
        }
        "emit" | "attention.emit" => {
            let definition = eliot_core::api::discovery::decode_params_at::<EmitDefinitionArgs>(
                args.clone(),
                "params.args",
            )?;
            definition.validate()?;
            let mut data = json!({"data": definition.data});
            if let Some(key) = definition.key {
                data["key"] = Value::String(key);
            }
            Ok((format!("x.user.{}", definition.topic), data))
        }
        _ => Err(Error::new("invalid", "not a local user-event verb")),
    }
}

fn validate_plugin_command_args(request: &SubmitCommand) -> Result<()> {
    let args = request
        .args
        .as_object()
        .ok_or_else(|| Error::new("invalid", "command args must be an object"))?;
    for reserved in ["expected_turn_id", "turn_id", "request_id", "context_items"] {
        if args.contains_key(reserved) {
            return Err(Error::new(
                "invalid",
                format!("command field {reserved} is captured by the daemon or Store"),
            ));
        }
    }
    Ok(())
}

/// Bounded mismatch text for the doctor: the payload's message when present,
/// else the compact payload truncated to one short line. Shapes belong to
/// k68; the doctor never assumes them.
fn doctor_mismatch_text(data: &Value) -> Option<String> {
    if let Some(message) = data
        .get("message")
        .and_then(Value::as_str)
        .filter(|message| !message.trim().is_empty())
    {
        return Some(message.chars().take(256).collect());
    }
    let compact = serde_json::to_string(data).ok()?;
    if compact.is_empty() {
        return None;
    }
    Some(compact.chars().take(256).collect())
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
    profile: String,
    profile_choice: Option<Value>,
    workspace: Option<Value>,
    project: Option<Value>,
    registered_project: Option<Value>,
    preparation: Option<Value>,
    agent_child: bool,
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
    let stored_profile = command
        .args
        .get("profile")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let profile_choice = command
        .args
        .get("profile_choice")
        .filter(|choice| choice.is_object())
        .cloned();
    let workspace = command.args.get("workspace").cloned();
    let project = command.args.get("project").cloned();
    let registered_project = command.args.get("registered_project").cloned();
    let preparation = command.args.get("workspace_preparation").cloned();
    let agent_child =
        command.principal.starts_with("agent:") && command.args.get("parent").is_some();
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
        // Profile was required on every admitted launch, so a missing or
        // non-string field on a retained command is malformed, never a
        // legacy shape to repair from the Agent row.
        let profile = stored_profile.ok_or_else(|| {
            Error::new(
                "bad_state",
                "retained launch profile field is missing or malformed",
            )
        })?;
        return Ok(Some(RetainedLaunchProfile {
            harness,
            snapshot,
            command_has_harness,
            command_has_snapshot,
            profile,
            profile_choice,
            workspace,
            project,
            registered_project,
            preparation,
            agent_child,
        }));
    }

    // Older L1 admissions did not keep the resolved route in command.args. Their
    // Agent row is already the retained source of truth, so reuse it without a
    // config read and preserve the original command payload for Store id binding.
    if let Some(target) = command.target.as_deref()
        && let Some(agent) = store.get_agent(target)?
    {
        let profile = stored_profile.ok_or_else(|| {
            Error::new(
                "bad_state",
                "retained launch profile field is missing or malformed",
            )
        })?;
        return Ok(Some(RetainedLaunchProfile {
            harness: agent.harness,
            snapshot: agent.profile_snapshot,
            command_has_harness,
            command_has_snapshot,
            profile,
            profile_choice,
            workspace,
            project,
            registered_project,
            preparation,
            agent_child,
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
    if !is_supported_harness(harness) {
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
        profile,
        profile_choice,
        workspace,
        project,
        registered_project,
        preparation,
        agent_child,
    } = retained;
    let same_workspace = match (
        request.args.get("workspace").and_then(Value::as_str),
        workspace.as_ref().and_then(Value::as_str),
    ) {
        (Some(requested), Some(stored)) => same_retained_workspace(requested, stored),
        (None, Some(_))
            if agent_child && matches!(request.args.get("workspace"), None | Some(Value::Null)) =>
        {
            true
        }
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
    // Restore the admitted profile only when the retry omits it, so a
    // no-profile retry hashes to the persisted digest and an explicitly
    // changed profile stays untouched for the canonical id_conflict check.
    if !args.contains_key("profile") {
        args.insert("profile".to_owned(), Value::String(profile));
        if let Some(choice) = profile_choice {
            args.insert("profile_choice".to_owned(), choice);
        }
    }
    if let Some(preparation) = preparation {
        args.insert("workspace_preparation".to_owned(), preparation);
    } else {
        args.remove("workspace_preparation");
    }
    // An Agent child's omitted workspace inherits the retained workspace,
    // without consulting a changed parent or the filesystem on exact replay.
    // Normalize only the same caller workspace without touching the filesystem.
    // A changed path/branch/base/text stays in the payload for Store's id_conflict.
    if same_workspace {
        if let Some(workspace) = workspace {
            args.insert("workspace".to_owned(), workspace);
        }
        if let Some(project) = project {
            args.insert("project".to_owned(), project);
        }
        if let Some(project) = registered_project {
            args.insert("registered_project".to_owned(), project);
        } else {
            args.remove("registered_project");
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

// Retained-ID hydration precedes normalize_launch. Resolve only mismatched
// spellings of an existing workspace; the lexical fast path still works when
// a previously admitted workspace has since been removed.
fn align_retained_workspace_alias(request: &mut SubmitCommand, stored: Option<&Value>) {
    let Some(stored) = stored.and_then(Value::as_str).map(str::to_owned) else {
        return;
    };
    let Some(requested) = request
        .args
        .get("workspace")
        .and_then(Value::as_str)
        .map(str::to_owned)
    else {
        return;
    };
    if same_retained_workspace(&requested, &stored) {
        return;
    }

    let (Ok(requested_path), Ok(stored_path)) = (
        std::fs::canonicalize(&requested),
        std::fs::canonicalize(&stored),
    ) else {
        return;
    };
    let (Some(requested_path), Some(stored_path)) = (requested_path.to_str(), stored_path.to_str())
    else {
        return;
    };
    if !same_retained_workspace(requested_path, stored_path) {
        return;
    }

    // normalize_launch stores the canonical path. Align an existing alias to
    // that retained spelling so Store sees the original same-ID payload.
    if let Some(args) = request.args.as_object_mut() {
        args.insert("workspace".to_owned(), Value::String(stored));
    }
}

fn reject_launch_derived_keys(args: &Value) -> Result<()> {
    let args = args
        .as_object()
        .ok_or_else(|| Error::new("invalid", "launch args must be an object"))?;
    if args.contains_key("profile_snapshot") {
        return Err(Error::new(
            "invalid",
            "omit profile_snapshot for launch; the daemon freezes it from the named profile (Agent children reuse the parent snapshot or name a child_profiles entry)",
        ));
    }
    for reserved in [
        "registered_project",
        RULE_WORKTREE_GUARD_ARG,
        eliot_core::PROJECT_CHECK_GOAL_ARG,
        eliot_core::PROJECT_POLICY_APPLIED_ARG,
        "admission_mode",
        "auto_worktree",
        "project_assignment_launch",
        "project_transfer_launch",
        "profile_choice",
        "instruction_sources",
        "source_plan",
        "target_environment",
        "workspace_preparation",
        "repo_common_dir",
        "base_oid",
        "worktree_path",
        "frozen_setup",
        "setup",
        "target_generation",
        // Switch-only markers the worker sets after preparation; a launch
        // caller can never carry a verified target revision or a retained
        // blank scope past admission.
        SWITCH_PROJECT_REVISION_ARG,
        SWITCH_BLANK_SCOPE_ARG,
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
        if crate::config::reserved_eliot_environment_name(name) {
            return Err(Error::new(
                "invalid",
                "profile pass_env cannot include reserved ELIOT_* names",
            ));
        }
    }
    Ok(())
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

fn diagnostic_principal(principal: &Principal) -> String {
    match principal {
        Principal::Owner => "owner".to_owned(),
        Principal::Swarm(name) => (*name).to_owned(),
        Principal::MemoryInference { client_id, .. } => format!("memory_inference:{client_id}"),
        Principal::Agent { id, generation } => format!("{id}#{generation}"),
        Principal::ProjectRule { registration } => format!("project:{}", registration.id),
        Principal::Run { run_command_id } => format!("run:{run_command_id}"),
    }
}
fn principal_name(principal: &Principal) -> String {
    match principal {
        Principal::Owner => "owner".to_owned(),
        Principal::Swarm(name) => (*name).to_owned(),
        Principal::MemoryInference { client_id, .. } => format!("memory_inference:{client_id}"),
        Principal::Agent { id, generation } => format!("agent:{id}:{generation}"),
        Principal::ProjectRule { registration } => format!("project:{}", registration.id),
        Principal::Run { run_command_id, .. } => format!("run:{run_command_id}"),
    }
}

fn rule_principal(project: Option<&str>) -> String {
    project.map_or_else(
        || "owner".to_owned(),
        |project| format!("project:{project}"),
    )
}

fn submit_result(command: &Command, linked: Option<&Command>) -> CommandSubmitResponse {
    CommandSubmitResponse {
        command_id: command.id.clone(),
        agent: command.target.clone(),
        generation: command.generation,
        state: command.state,
        outcome: command.outcome.clone(),
        linked_command: linked.map(|command| command.id.clone()),
    }
}

fn authorize_event_filter(raw: EventsFilter, principal: &Principal) -> Result<EventFilter> {
    raw.validate()?;
    let mut filter = EventFilter {
        agents: raw.agents.into_iter().collect(),
        kinds: raw.kinds.into_iter().collect(),
        sources: raw.sources.into_iter().collect(),
        var_scopes: HashSet::new(),
    };
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
    let secrets = secrets.lock_recover();
    contains_secret_value(value, &secrets)
}

fn contains_known_secret_including_keys(value: &Value, secrets: &SecretSet) -> bool {
    let secrets = secrets.lock_recover();
    SecretPolicy::new(secrets.iter()).contains_value(value)
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
