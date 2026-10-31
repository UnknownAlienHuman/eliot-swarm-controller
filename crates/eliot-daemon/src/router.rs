use crate::{
    automation::RuleDefinition,
    bus::Bus,
    common::{now_ms, secure_eq, valid_environment_name},
    monitor::Monitoring,
    plugin_host::{AgentCredentials, PluginHost, PluginHostOptions, SecretSet, UnknownCommands},
    scripts::auth::{RunCredentialGuard, RunCredentialRegistry, SavedRunPrincipal},
    workspaces::WorkspaceManager,
};
use eliot_core::{
    Agent, AgentState, Command, CommandState, Error, Event, FrozenScriptContext, FrozenScriptRun,
    NativeStoreBinding, Result, RunDefinitionArgs, ScriptProcessResult, ScriptTimerInvocation,
    SecretPolicy, SubmitCommand, Turn, TurnState, WorkspacePhase, WorkspacePreparationInput,
    api::{
        AgentDiscoverRequest, AgentEditsRequest, AgentEditsResponse, AgentGetRequest,
        AgentGetResponse, AgentGitRequest, AgentGitResponse, AgentHistoryRequest, AgentListRequest,
        AgentListResponse, AgentListRow, AgentProcessesRequest, AgentProcessesResponse,
        AgentResultRequest, AgentResultResponse, AgentStatsRequest, AgentStatsResponse,
        CommandGetRequest, CommandSubmitRequest, CommandSubmitResponse, DaemonShutdownRequest,
        DaemonShutdownResponse, DaemonStatusRequest, DaemonStatusResponse, DiscoverPage,
        EventsFilter, EventsReadRequest, EventsReadResponse, EventsSubscribeRequest,
        EventsSubscribeResponse, GitView, MAX_OBSERVATION_PAGE_BYTES, ObservationPage,
        ObservationQuery, ObservationView, ProcessPageCursor, ProjectRegistrationView,
        ProviderClearHoldRequest, ProviderHoldsRequest, RuleCatchUpRequest, RuleGetRequest,
        RuleStateView, ShutdownMode, SourceState, StatusBlocker, TimerGetRequest, TimerRunRequest,
        TimerStateView, TurnGetRequest, UnsettledCommandSummary, WorkspaceCustodyView,
    },
    classify_dispatched_error, is_deterministic_pre_admission_rejection,
};
use eliot_plugin::{MAX_EVENT_BYTES, sync::LockExt};
use eliot_store::{
    ProjectRegistrationInput, RuleCatchUpAction, RuleCatchUpAdmission, RuleProcessingState,
    TimerDefinitionInput, TimerSlotAdmission, TimerSlotCommit,
};
use futures_util::future::BoxFuture;
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{Mutex as AsyncMutex, broadcast, oneshot, watch},
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
const CONTROLLED_PLUGIN_SOURCE: &str = "stream";
const RUN_WORKER_FORCE_GRACE: Duration = Duration::from_secs(15);

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
    run_credentials: RunCredentialRegistry,
    unknown_commands: UnknownCommands,
    unknown_causes: Mutex<HashMap<String, String>>,
    rule_definitions: Mutex<HashMap<(String, String), (i64, RuleDefinition)>>,
    plugin_host: PluginHost,
    monitoring: Monitoring,
    workspaces: WorkspaceManager,
    workspace_recovery_error: Mutex<Option<String>>,
    mode: AtomicU8,
    shutdown_ready: AtomicBool,
    progress: watch::Sender<u64>,
    run_cancellation: watch::Sender<bool>,
    run_workers: Mutex<HashMap<String, RunWorkerHandle>>,
}

struct RunWorkerHandle {
    task: tokio::task::JoinHandle<()>,
    completed: watch::Receiver<bool>,
}

struct RunWorkerCompletion {
    router: Weak<RouterInner>,
    run_id: String,
    completed: watch::Sender<bool>,
    finished: bool,
}

impl RunWorkerCompletion {
    fn finish(&mut self) {
        if self.finished {
            return;
        }
        self.completed.send_replace(true);
        if let Some(router) = self.router.upgrade() {
            router.run_workers.lock_recover().remove(&self.run_id);
        }
        self.finished = true;
    }
}

impl Drop for RunWorkerCompletion {
    fn drop(&mut self) {
        if !self.finished {
            self.completed.send_replace(true);
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RunWorkerStart {
    Started,
    AlreadyActive,
    ForceRequested,
}
#[derive(Clone)]
enum Principal {
    Owner,
    Agent {
        id: String,
        generation: i64,
    },
    ProjectRule {
        registration: ProjectRegistrationView,
    },
    Run {
        run_command_id: String,
    },
}
struct EventFilter {
    agents: HashSet<String>,
    kinds: HashSet<String>,
    sources: HashSet<String>,
}

pub(crate) struct PreparedSubmission {
    pub(crate) request: SubmitCommand,
    pub(crate) workspace: Option<WorkspacePreparationInput>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectGetRequest {
    id: String,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectListRequest {}

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

impl Router {
    pub(crate) fn new(
        data_root: std::path::PathBuf,
        owner_token: String,
        store: StoreHandle,
        bus: Bus,
    ) -> Self {
        let secrets: SecretSet = Arc::new(Mutex::new(HashSet::from([owner_token.clone()])));
        let agent_credentials: AgentCredentials = Arc::new(Mutex::new(Default::default()));
        let run_credentials = RunCredentialRegistry::default();
        let unknown_commands: UnknownCommands = Arc::new(Mutex::new(Default::default()));
        let (progress, _) = watch::channel(0_u64);
        let (run_cancellation, _) = watch::channel(false);
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
                run_credentials,
                unknown_commands,
                unknown_causes: Mutex::new(HashMap::new()),
                rule_definitions: Mutex::new(HashMap::new()),
                plugin_host,
                monitoring,
                workspaces,
                workspace_recovery_error: Mutex::new(None),
                mode: AtomicU8::new(MODE_OPEN),
                shutdown_ready: AtomicBool::new(false),
                progress,
                run_cancellation,
                run_workers: Mutex::new(HashMap::new()),
            }),
        }
    }

    pub(crate) async fn handle(&self, method: &str, wire_params: Value) -> Result<Value> {
        let (principal, params) = self.authenticate(wire_params).await?;
        authorize_rpc_method(&principal, method)?;
        match method {
            "daemon.status" => {
                require_owner(&principal)?;
                let request: DaemonStatusRequest = serde_json::from_value(params)
                    .map_err(|_| Error::new("invalid", "daemon.status params are invalid"))?;
                request.validate()?;
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
            "project.get" => self.project_get(&params, &principal).await,
            "project.list" => self.project_list(&params, &principal).await,
            "rule.get" => self.rule_get(&params, &principal).await,
            "rule.catch_up" => self.rule_catch_up(&params, &principal).await,
            "timer.get" => self.timer_get(&params, &principal).await,
            "timer.run" => self.timer_run(&params, &principal).await,
            "provider.holds" => self.provider_holds(&params, &principal).await,
            "provider.clear_hold" => self.provider_clear_hold(&params, &principal).await,
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

    pub(crate) async fn request_force(&self) {
        self.inner.plugin_host.close_discovery_admission();
        let _store = self.inner.store.lock().await;
        let _run_workers = self.inner.run_workers.lock_recover();
        self.inner.mode.store(MODE_FORCE, Ordering::SeqCst);
        self.inner.run_cancellation.send_replace(true);
        self.inner.shutdown_ready.store(true, Ordering::SeqCst);
        drop(_store);
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
        let workers = {
            let _store = self.inner.store.lock().await;
            let mut workers = self.inner.run_workers.lock_recover();
            self.inner.mode.fetch_max(MODE_FORCE, Ordering::SeqCst);
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
            self.schedule_command(id, linked);
        }
        self.signal_progress();
        Ok(())
    }

    pub(crate) fn note_workspace_recovery_failure(&self, code: &str) {
        *self.inner.workspace_recovery_error.lock_recover() = Some(code.to_owned());
        self.signal_progress();
    }

    pub(crate) fn note_events(&self, events: &[Event]) {
        self.note_committed_events(events);
        self.signal_progress();
    }

    pub(crate) fn note_committed_events(&self, events: &[Event]) {
        self.note_unknown(events);
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
        if !auth_fields_are_exact(auth, kind) {
            return Err(Error::new(
                "not_allowed",
                "daemon credential fields are incomplete or conflicting",
            ));
        }
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
                        .lock_recover()
                        .insert((id.to_owned(), generation), credential.to_owned());
                    self.inner
                        .secrets
                        .lock_recover()
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
            "run" => {
                let run_command_id = auth
                    .get("run_command_id")
                    .and_then(Value::as_str)
                    .filter(|id| !id.trim().is_empty() && !id.contains('\0'))
                    .ok_or_else(|| Error::new("not_allowed", "Run command id is invalid"))?;
                let _saved_principal = self
                    .inner
                    .run_credentials
                    .authenticate(run_command_id, credential)
                    .ok_or_else(|| {
                        Error::new("not_allowed", "Run credential is invalid or revoked")
                    })?;
                Ok((
                    Principal::Run {
                        run_command_id: run_command_id.to_owned(),
                    },
                    payload,
                ))
            }
            _ => Err(Error::new("not_allowed", "daemon principal is unsupported")),
        }
    }
    async fn submit(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: CommandSubmitRequest = serde_json::from_value(params.clone())
            .map_err(|_| Error::new("invalid", "command.submit params are malformed"))?;
        let prepared = self.prepare_submission(request, principal).await?;
        let PreparedSubmission {
            mut request,
            workspace: mut preparation,
        } = prepared;

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
        self.note_unknown(&admission.events);
        if admission.command.state == CommandState::Unknown {
            self.inner
                .unknown_commands
                .lock_recover()
                .insert(admission.command.id.clone());
        }

        if admission.is_new {
            let command_id = admission.command.id.clone();
            let linked_id = admission
                .linked_command
                .as_ref()
                .map(|command| command.id.clone());
            self.schedule_command(command_id, linked_id);
        }
        self.signal_progress();
        serde_json::to_value(submit_result(
            &admission.command,
            admission.linked_command.as_ref(),
        ))
        .map_err(|_| Error::new("internal", "command admission result could not be encoded"))
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
            SavedRunPrincipal::ProjectRule { registration }
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
    pub(crate) async fn prepare_owner_submission(
        &self,
        request: SubmitCommand,
    ) -> Result<PreparedSubmission> {
        validate_rule_launch_action(&request, None)?;
        validate_rule_resume_action(&request, None)?;
        let prepared = self.prepare_submission(request, &Principal::Owner).await?;
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
        let definition: RunDefinitionArgs = serde_json::from_value(request.args.clone())
            .map_err(|_| Error::new("invalid", "Owner Run args are malformed"))?;
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
        self.prepare_submission_after_authorization(request, &Principal::Owner, false)
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
                let definition: RunDefinitionArgs = serde_json::from_value(request.args.clone())
                    .map_err(|_| Error::new("invalid", "project Run args are malformed"))?;
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
            registration: registration.clone(),
        };
        let mut prepared = Vec::with_capacity(requests.len());
        for request in requests {
            let prepared_request = self.prepare_submission(request, &principal).await?;
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
            registration: registration.clone(),
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
        validate_rule_launch_action(&request, Some(registration))?;
        validate_rule_resume_action(&request, Some(registration))?;
        let principal = Principal::ProjectRule {
            registration: registration.clone(),
        };
        let prepared = self
            .prepare_submission_after_authorization(request, &principal, true)
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
        self.prepare_submission_after_authorization(request, principal, false)
            .await
    }

    async fn prepare_submission_after_authorization(
        &self,
        mut request: SubmitCommand,
        principal: &Principal,
        project_authority_preverified: bool,
    ) -> Result<PreparedSubmission> {
        if request.probe && request.verb != "send" {
            return Err(Error::new(
                "unsupported",
                "only an explicit send may probe a provider hold",
            ));
        }
        if matches!(request.verb.as_str(), "attach" | "detach") {
            require_owner(principal)?;
        }
        if request.verb == "worktree.remove" {
            require_owner(principal)?;
        }
        if !project_authority_preverified {
            self.verify_native_control_target(&request).await?;
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
        let mut workspace = None;
        if request.verb == "launch" {
            reject_launch_derived_keys(&request.args)?;
            if self.hydrate_retained_launch(&mut request).await? {
                workspace = retained_preparation(&request.args)?;
            } else {
                let frozen = async {
                    normalize_launch(&mut request)?;
                    self.freeze_launch_admission(&mut request).await
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

    async fn hydrate_retained_launch(&self, request: &mut SubmitCommand) -> Result<bool> {
        if let Some(retained) = {
            let store = self.inner.store.lock().await;
            retained_launch_profile(&store, request)?
        } {
            align_retained_workspace_alias(request, retained.workspace.as_ref());
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
        let mut snapshot = crate::config::resolve_profile(&self.inner.data_root, profile_name)?;
        crate::config::capture_native_store_inputs(&mut snapshot, |name| std::env::var_os(name))?;
        let harness = snapshot
            .get("harness")
            .and_then(Value::as_str)
            .filter(|harness| !harness.trim().is_empty())
            .ok_or_else(|| Error::new("invalid", "resolved profile must name its harness"))?
            .to_owned();
        install_frozen_profile(request, &harness, snapshot, true)?;
        Ok(false)
    }

    async fn provider_holds(&self, params: &Value, principal: &Principal) -> Result<Value> {
        require_owner(principal)?;
        let request: ProviderHoldsRequest = serde_json::from_value(params.clone())
            .map_err(|_| Error::new("invalid", "provider.holds params are invalid"))?;
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
        let request: ProviderClearHoldRequest = serde_json::from_value(params.clone())
            .map_err(|_| Error::new("invalid", "provider.clear_hold params are invalid"))?;
        request.validate()?;
        let change = {
            let mut store = self.inner.store.lock().await;
            let change = store.clear_provider_hold("owner", &request, now_ms())?;
            self.publish_locked(&change.events);
            change
        };
        self.signal_progress();
        serde_json::to_value(change.value)
            .map_err(|_| Error::new("internal", "provider correction could not be encoded"))
    }

    async fn project_get(&self, params: &Value, principal: &Principal) -> Result<Value> {
        require_owner(principal)?;
        let request: ProjectGetRequest = serde_json::from_value(params.clone())
            .map_err(|_| Error::new("invalid", "project.get params are invalid"))?;
        if request.id.trim().is_empty() {
            return Err(Error::new("invalid", "project id must be nonempty"));
        }
        let registration = self
            .inner
            .store
            .lock()
            .await
            .get_project_registration("owner", &request.id)?
            .ok_or_else(|| Error::new("not_found", "project registration was not found"))?;
        registration.validate()?;
        serde_json::to_value(registration)
            .map_err(|_| Error::new("internal", "project registration could not be encoded"))
    }

    async fn project_list(&self, params: &Value, principal: &Principal) -> Result<Value> {
        require_owner(principal)?;
        let _: ProjectListRequest = serde_json::from_value(params.clone())
            .map_err(|_| Error::new("invalid", "project.list params are invalid"))?;
        let registrations = self
            .inner
            .store
            .lock()
            .await
            .list_project_registrations("owner", None, PAGE_MAX)?;
        for registration in &registrations {
            registration.validate()?;
        }
        serde_json::to_value(registrations)
            .map_err(|_| Error::new("internal", "project registrations could not be encoded"))
    }

    async fn rule_catch_up(&self, params: &Value, principal: &Principal) -> Result<Value> {
        require_owner(principal)?;
        let request: RuleCatchUpRequest = serde_json::from_value(params.clone())
            .map_err(|_| Error::new("invalid", "rule.catch_up params are invalid"))?;
        request.validate()?;
        let rule_principal = rule_principal(request.project.as_deref());
        if let Some(parent) = self
            .retained_rule_catch_up(&rule_principal, &request)
            .await?
        {
            return serde_json::to_value(parent)
                .map_err(|_| Error::new("internal", "rule.catch_up result could not be encoded"));
        }
        if contains_known_secret_including_keys(params, &self.inner.secrets) {
            return Err(Error::new(
                "secret_in_payload",
                "rule.catch_up request contains a known credential",
            ));
        }
        if !self.rule_firings_open() {
            if let Some(parent) = self
                .retained_rule_catch_up(&rule_principal, &request)
                .await?
            {
                return serde_json::to_value(parent).map_err(|_| {
                    Error::new("internal", "rule.catch_up result could not be encoded")
                });
            }
            return Err(Error::new("unavailable", "automation is draining"));
        }

        let project = match request.project.as_deref() {
            Some(id) => Some(
                self.inner
                    .store
                    .lock()
                    .await
                    .get_project_registration("owner", id)?
                    .ok_or_else(|| {
                        Error::new("not_allowed", "project registration is not active")
                    })?,
            ),
            None => None,
        };
        if let Some(project) = &project {
            crate::automation::verify_project_registration(&self.inner.data_root, project).await?;
        }
        let (expected_revision, definition) = self
            .rule_definition(&rule_principal, &request.rule)
            .ok_or_else(|| Error::new("not_found", "rule was not found"))?;
        if definition.schedule().is_some() {
            return Err(Error::new(
                "unsupported",
                "timer rules have no event history",
            ));
        }
        let state = self
            .inner
            .store
            .lock()
            .await
            .get_rule_state(&rule_principal, &request.rule)?
            .ok_or_else(|| Error::new("not_found", "rule was not found"))?;
        if state.project != request.project || state.revision != expected_revision {
            return Err(Error::new(
                "bad_state",
                "rule definition changed before catch-up preparation",
            ));
        }
        if !state.enabled || state.paused_reason.is_some() || state.retiring_revision.is_some() {
            return Err(Error::new("bad_state", "rule is not eligible for catch-up"));
        }

        let high_water = self.rule_high_water().await?;
        let through = request.through_seq.unwrap_or(high_water);
        if through > high_water || through > state.current_cursor {
            return Err(Error::new(
                "bad_state",
                "catch-up boundary must be at or behind the live rule cursor",
            ));
        }
        if through < request.after_seq {
            return Err(Error::new(
                "invalid",
                "catch-up boundary precedes after_seq",
            ));
        }
        let events = self
            .read_rule_catch_up_events(request.after_seq, through)
            .await?;
        let actions = crate::automation::prepare_rule_catch_up_actions(
            self,
            &definition,
            state.revision,
            &events,
            project.as_ref(),
        )
        .await?;
        let Some(admission) = self
            .admit_rule_catch_up(
                &rule_principal,
                project.as_ref(),
                &request,
                &state,
                through,
                &actions,
            )
            .await?
        else {
            if let Some(parent) = self
                .retained_rule_catch_up(&rule_principal, &request)
                .await?
            {
                return serde_json::to_value(parent).map_err(|_| {
                    Error::new("internal", "rule.catch_up result could not be encoded")
                });
            }
            return Err(Error::new("unavailable", "automation is draining"));
        };
        request.validate_response(&admission.parent)?;
        serde_json::to_value(admission.parent)
            .map_err(|_| Error::new("internal", "rule.catch_up result could not be encoded"))
    }

    async fn retained_rule_catch_up(
        &self,
        principal: &str,
        request: &RuleCatchUpRequest,
    ) -> Result<Option<Command>> {
        Ok(self
            .inner
            .store
            .lock()
            .await
            .replay_rule_catch_up(principal, request)?
            .map(|admission| admission.parent))
    }

    async fn read_rule_catch_up_events(&self, after: i64, through: i64) -> Result<Vec<Event>> {
        let mut cursor = after;
        let mut events = Vec::new();
        while cursor < through {
            let page = self
                .read_rule_events(cursor, through, crate::automation::RULE_EVENT_PAGE_SIZE)
                .await?;
            if page.is_empty() {
                break;
            }
            let page_len = page.len();
            for event in page {
                if event.seq <= cursor {
                    return Err(Error::new(
                        "source_integrity",
                        "rule history page did not advance",
                    ));
                }
                cursor = event.seq;
                events.push(event);
                if events.len() > EVENT_SCAN_MAX {
                    return Err(Error::new(
                        "frame_too_large",
                        "rule.catch_up range exceeds the bounded event scan",
                    ));
                }
            }
            if page_len < crate::automation::RULE_EVENT_PAGE_SIZE {
                break;
            }
        }
        Ok(events)
    }

    async fn rule_get(&self, params: &Value, principal: &Principal) -> Result<Value> {
        require_owner(principal)?;
        let request: RuleGetRequest = serde_json::from_value(params.clone())
            .map_err(|_| Error::new("invalid", "rule.get params are invalid"))?;
        request.validate()?;
        let rule_principal = rule_principal(request.project.as_deref());
        let state = self
            .inner
            .store
            .lock()
            .await
            .get_rule_state(&rule_principal, &request.id)?
            .ok_or_else(|| Error::new("not_found", "rule was not found"))?;
        if state.validate().is_err() || state.id != request.id || state.project != request.project {
            return Err(Error::new(
                "source_integrity",
                "stored rule namespace differs from the request",
            ));
        }
        serde_json::to_value(state)
            .map_err(|_| Error::new("internal", "rule state could not be encoded"))
    }

    async fn timer_get(&self, params: &Value, principal: &Principal) -> Result<Value> {
        require_owner(principal)?;
        let request: TimerGetRequest = serde_json::from_value(params.clone())
            .map_err(|_| Error::new("invalid", "timer.get params are invalid"))?;
        request.validate()?;
        let rule_principal = rule_principal(request.project.as_deref());
        let state = self
            .inner
            .store
            .lock()
            .await
            .get_timer_state(&rule_principal, &request.id)?
            .ok_or_else(|| Error::new("not_found", "timer was not found"))?;
        request.validate_response(&state)?;
        serde_json::to_value(state)
            .map_err(|_| Error::new("internal", "timer state could not be encoded"))
    }

    async fn timer_run(&self, params: &Value, principal: &Principal) -> Result<Value> {
        require_owner(principal)?;
        let request: TimerRunRequest = serde_json::from_value(params.clone())
            .map_err(|_| Error::new("invalid", "timer.run params are invalid"))?;
        request.validate()?;
        let rule_principal = rule_principal(request.project.as_deref());

        // An exact external-ID replay is returned before current timer, mode,
        // or target-generation gates are evaluated again.
        if let Some(parent) = self.replay_timer_run(&rule_principal, &request).await? {
            request.validate_response(&parent)?;
            return serde_json::to_value(parent)
                .map_err(|_| Error::new("internal", "timer.run result could not be encoded"));
        }
        if contains_known_secret_including_keys(params, &self.inner.secrets) {
            return Err(Error::new(
                "secret_in_payload",
                "timer.run request contains a known credential",
            ));
        }

        if !self.rule_firings_open() {
            return Err(Error::new("unavailable", "automation is draining"));
        }
        let project_view = match request.project.as_deref() {
            Some(project) => Some(
                self.inner
                    .store
                    .lock()
                    .await
                    .get_project_registration("owner", project)?
                    .ok_or_else(|| {
                        Error::new("not_allowed", "project registration is not active")
                    })?,
            ),
            None => None,
        };
        let (expected_rule_revision, definition) = self
            .rule_definition(&rule_principal, &request.rule)
            .ok_or_else(|| Error::new("not_found", "timer rule was not found"))?;
        if definition.schedule().is_none() {
            return Err(Error::new("unsupported", "rule is not a timer"));
        }
        let timer = self
            .inner
            .store
            .lock()
            .await
            .get_timer_state(&rule_principal, &request.rule)?
            .ok_or_else(|| Error::new("not_found", "timer was not found"))?;
        if !timer.enabled {
            return Err(Error::new("bad_state", "timer is disabled or paused"));
        }
        if !self.rule_firings_open() {
            return Err(Error::new("unavailable", "automation is draining"));
        }

        let command = definition.evaluate_timer()?;
        let prepared_result = if command.verb == "run" {
            self.prepare_timer_rule_run_submission(
                command,
                project_view.as_ref(),
                crate::automation::timer_script_context(
                    &definition.id,
                    expected_rule_revision,
                    &timer,
                    ScriptTimerInvocation::Manual {
                        parent_command_id: request.id.clone(),
                    },
                ),
            )
            .await
        } else {
            match project_view.as_ref() {
                Some(project) => self.prepare_project_rule_submission(command, project).await,
                None => self.prepare_owner_submission(command).await,
            }
        };
        let prepared = match prepared_result {
            Ok(prepared) => prepared,
            Err(error) => {
                if let Some(parent) = self.replay_timer_run(&rule_principal, &request).await? {
                    request.validate_response(&parent)?;
                    return serde_json::to_value(parent).map_err(|_| {
                        Error::new("internal", "timer.run result could not be encoded")
                    });
                }
                return Err(error);
            }
        };
        let Some(admission) = self
            .admit_manual_timer_run(
                &rule_principal,
                project_view.as_ref(),
                &request,
                &timer,
                expected_rule_revision,
                prepared,
            )
            .await?
        else {
            if let Some(parent) = self.replay_timer_run(&rule_principal, &request).await? {
                request.validate_response(&parent)?;
                return serde_json::to_value(parent)
                    .map_err(|_| Error::new("internal", "timer.run result could not be encoded"));
            }
            return Err(Error::new("unavailable", "automation is draining"));
        };
        if admission.blocked {
            return Err(Error::new(
                "busy",
                "previous timer action is still open or unknown",
            ));
        }
        let parent = admission
            .parent
            .ok_or_else(|| Error::new("internal", "timer.run admission omitted its parent"))?;
        request.validate_response(&parent)?;
        serde_json::to_value(parent)
            .map_err(|_| Error::new("internal", "timer.run result could not be encoded"))
    }

    async fn replay_timer_run(
        &self,
        principal: &str,
        request: &TimerRunRequest,
    ) -> Result<Option<Command>> {
        self.inner
            .store
            .lock()
            .await
            .replay_timer_run(principal, request)
    }

    async fn admit_manual_timer_run(
        &self,
        principal: &str,
        project: Option<&ProjectRegistrationView>,
        request: &TimerRunRequest,
        state: &TimerStateView,
        expected_rule_revision: i64,
        prepared: PreparedSubmission,
    ) -> Result<Option<TimerSlotAdmission>> {
        validate_rule_launch_preparation(&prepared)?;
        let PreparedSubmission {
            request: action,
            workspace,
        } = prepared;
        if !matches!(action.verb.as_str(), "stop" | "send" | "launch" | "run")
            || workspace.is_some()
        {
            return Err(Error::new(
                "invalid",
                "timer firing must contain one prepared stop, send, launch, or Run command",
            ));
        }
        if action.verb == "run" {
            if !action.id.is_empty()
                || action.target.is_some()
                || action.expected_generation.is_some()
                || action.expected_turn_id.is_some()
                || action.request_id.is_some()
                || action.probe
            {
                return Err(Error::new(
                    "invalid",
                    "timer Run must be targetless and unbound",
                ));
            }
            let frozen: FrozenScriptRun = serde_json::from_value(action.args.clone())
                .map_err(|_| Error::new("invalid", "manual timer Run args are not frozen"))?;
            frozen.validate()?;
            let expected_context = crate::automation::timer_script_context(
                &request.rule,
                expected_rule_revision,
                state,
                ScriptTimerInvocation::Manual {
                    parent_command_id: request.id.clone(),
                },
            );
            if frozen.context != expected_context {
                return Err(Error::new(
                    "invalid",
                    "manual timer Run context does not match its admission",
                ));
            }
        }
        let admission = {
            let mut store = self.inner.store.lock().await;
            let replay = store.replay_timer_run(principal, request)?.is_some();
            if !replay && self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN {
                return Ok(None);
            }
            if !replay && let Some(expected) = project {
                let current = store
                    .get_project_registration("owner", &expected.id)?
                    .ok_or_else(|| Error::new("not_allowed", "project registration was removed"))?;
                if &current != expected {
                    return Err(Error::new(
                        "not_allowed",
                        "project registration changed before timer admission",
                    ));
                }
            }
            let admission = store.admit_manual_timer_run(
                principal,
                request,
                state,
                expected_rule_revision,
                action,
                now_ms(),
            )?;
            self.publish_locked(&admission.events);
            admission
        };
        if admission.is_new
            && let Some(child) = &admission.child
        {
            self.schedule_rule_admission(child);
        }
        if admission.is_new {
            self.signal_progress();
        }
        Ok(Some(admission))
    }

    pub(crate) fn install_rule_definitions(
        &self,
        principal: &str,
        definitions: &[(i64, RuleDefinition)],
    ) {
        let mut stored = self.inner.rule_definitions.lock_recover();
        stored.retain(|(stored_principal, _), _| stored_principal != principal);
        stored.extend(definitions.iter().map(|(revision, definition)| {
            (
                (principal.to_owned(), definition.id.clone()),
                (*revision, definition.clone()),
            )
        }));
    }

    fn rule_definition(&self, principal: &str, id: &str) -> Option<(i64, RuleDefinition)> {
        self.inner
            .rule_definitions
            .lock_recover()
            .get(&(principal.to_owned(), id.to_owned()))
            .cloned()
    }

    pub(crate) async fn initialize_rules(
        &self,
        principal: &str,
        definitions: &[eliot_store::RuleDefinitionInput],
        timers: &[TimerDefinitionInput],
        registration_now: i64,
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
            )?;
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

    pub(crate) async fn reconcile_project_registrations(
        &self,
        inputs: &[ProjectRegistrationInput],
    ) -> Result<Vec<ProjectRegistrationView>> {
        let change = {
            let mut store = self.inner.store.lock().await;
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
        now: i64,
    ) -> Result<Option<TimerSlotAdmission>> {
        let scheduled_at_ms = slot.scheduled_at_ms;
        let admission = {
            let mut store = self.inner.store.lock().await;
            if let Some(admission) = store.replay_timer_slot(principal, state, scheduled_at_ms)? {
                admission
            } else {
                if let Some(action) = slot.action.as_ref() {
                    validate_prepared_rule_launch_command(action)?;
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
                let admission = store.commit_timer_slot(principal, state, slot, now)?;
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
        let report = matches!(
            &outcome,
            eliot_store::RuleCursorOutcome::Rejected { .. }
                | eliot_store::RuleCursorOutcome::Disabled
        );
        let change = {
            let mut store = self.inner.store.lock().await;
            let change = store.advance_rule_cursor(principal, state, through, outcome, now_ms())?;
            if report {
                self.publish_locked(&change.events);
            }
            change
        };
        if report && !change.events.is_empty() {
            self.signal_progress();
        }
        Ok(change.value)
    }

    pub(crate) async fn admit_rule_action(
        &self,
        principal: &str,
        project: Option<&ProjectRegistrationView>,
        state: &RuleStateView,
        revision: i64,
        event_seq: i64,
        prepared: PreparedSubmission,
    ) -> Result<Option<eliot_store::RuleEventAdmission>> {
        if prepared.request.verb == "resume"
            && let Some(admission) = self
                .replay_rule_event_action(principal, state, revision, event_seq)
                .await?
        {
            return Ok(Some(admission));
        }
        validate_rule_launch_preparation(&prepared)?;
        validate_rule_resume_preparation(&prepared, project)?;
        let PreparedSubmission { request, workspace } = prepared;
        if workspace.is_some() {
            return Err(Error::new(
                "invalid",
                "rule firing cannot contain workspace preparation",
            ));
        }
        if request.verb == "run" {
            if !request.id.is_empty()
                || request.target.is_some()
                || request.expected_generation.is_some()
                || request.expected_turn_id.is_some()
                || request.request_id.is_some()
                || request.probe
            {
                return Err(Error::new(
                    "invalid",
                    "event Run must be targetless and unbound",
                ));
            }
            let frozen: eliot_core::FrozenScriptRun = serde_json::from_value(request.args.clone())
                .map_err(|_| Error::new("invalid", "project Run args are not frozen"))?;
            frozen.validate()?;
            match project {
                Some(registration) => {
                    let expected_principal = format!("project:{}", registration.id);
                    if principal != expected_principal {
                        return Err(Error::new(
                            "not_allowed",
                            "project Run principal does not match its registration",
                        ));
                    }
                    if !registration.actions.iter().any(|action| action == "run") {
                        return Err(Error::new(
                            "not_allowed",
                            "project registration does not approve Run",
                        ));
                    }
                }
                None if principal == "owner" => {
                    let _ = self.verify_global_run(&frozen).await?;
                }
                None => {
                    return Err(Error::new(
                        "not_allowed",
                        "global Run firing requires the Owner rule principal",
                    ));
                }
            }
        } else if request.verb != "launch"
            && request.verb != "resume"
            && !matches!(
                request.verb.as_str(),
                "stop" | "send" | "steer" | "interrupt" | "approve" | "deny" | "answer"
            )
        {
            return Err(Error::new(
                "invalid",
                "rule firing must contain a prepared stop, send, launch, resume, steer, interrupt, reply, or Run action",
            ));
        }
        let admission = {
            let mut store = self.inner.store.lock().await;
            let retained_resume = if request.verb == "resume" {
                store.replay_rule_event_action(principal, state, revision, event_seq)?
            } else {
                None
            };
            let admission = if let Some(admission) = retained_resume {
                admission
            } else {
                if self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN {
                    return Ok(None);
                }
                let replay_first_action = matches!(
                    request.verb.as_str(),
                    "launch" | "approve" | "deny" | "answer"
                );
                // Store resolves retained launch/reply parents before current gates.
                if !replay_first_action && let Some(expected) = project {
                    let current = store
                        .get_project_registration("owner", &expected.id)?
                        .ok_or_else(|| {
                            Error::new("not_allowed", "project registration was removed")
                        })?;
                    if &current != expected {
                        return Err(Error::new(
                            "not_allowed",
                            "project registration changed before rule admission",
                        ));
                    }
                }
                store.admit_rule_action(principal, state, event_seq, request, now_ms())?
            };
            self.publish_locked(&admission.events);
            admission
        };
        if admission.is_new
            && let Some(child) = &admission.child
        {
            self.schedule_rule_admission(child);
        }
        self.signal_progress();
        Ok(Some(admission))
    }

    async fn admit_rule_catch_up(
        &self,
        principal: &str,
        project: Option<&ProjectRegistrationView>,
        request: &RuleCatchUpRequest,
        state: &RuleStateView,
        through: i64,
        actions: &[RuleCatchUpAction],
    ) -> Result<Option<RuleCatchUpAdmission>> {
        let retained = {
            let store = self.inner.store.lock().await;
            store.get_command(&request.id)?.is_some()
        };
        let owner_actions = if !retained && principal == "owner" && project.is_none() {
            let mut checked = Vec::with_capacity(actions.len());
            for action in actions {
                if let eliot_store::RuleCatchUpActionOutcome::Command(command) = &action.outcome
                    && command.verb == "run"
                {
                    let frozen: FrozenScriptRun = match serde_json::from_value(command.args.clone())
                    {
                        Ok(frozen) => frozen,
                        Err(_) => {
                            checked.push(RuleCatchUpAction::rejected(action.event_seq, "invalid"));
                            continue;
                        }
                    };
                    match self.verify_global_run(&frozen).await {
                        Ok(_) => checked.push(action.clone()),
                        Err(error) if is_deterministic_pre_admission_rejection(&error.code) => {
                            checked.push(RuleCatchUpAction::rejected(action.event_seq, error.code));
                        }
                        Err(error) => return Err(error),
                    }
                } else {
                    checked.push(action.clone());
                }
            }
            Some(checked)
        } else {
            None
        };
        let actions = owner_actions.as_deref().unwrap_or(actions);
        let admission = {
            let mut store = self.inner.store.lock().await;
            let retained = store.get_command(&request.id)?.is_some();
            if !retained && self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN {
                return Ok(None);
            }
            if !retained && let Some(expected) = project {
                let current = store
                    .get_project_registration("owner", &expected.id)?
                    .ok_or_else(|| Error::new("not_allowed", "project registration was removed"))?;
                if &current != expected {
                    return Err(Error::new(
                        "not_allowed",
                        "project registration changed before catch-up admission",
                    ));
                }
            }
            let admission =
                store.admit_rule_catch_up(principal, request, state, through, actions, now_ms())?;
            self.publish_locked(&admission.events);
            admission
        };
        if admission.is_new {
            if let Some(child) = admission.children.first() {
                self.schedule_rule_admission(child);
            }
            self.signal_progress();
        }
        Ok(Some(admission))
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
        let request: CommandGetRequest = serde_json::from_value(params.clone())
            .map_err(|_| Error::new("invalid", "command.get params are invalid"))?;
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
            store.get_command(id)?
        }
        .ok_or_else(|| Error::new("not_found", format!("command {id} was not found")))?;
        authorize_command_read(principal, &command)?;
        if command.state.is_terminal() {
            self.clear_unknown_id(id);
        } else if command.state == CommandState::Unknown {
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
        let request: AgentDiscoverRequest = serde_json::from_value(params.clone())
            .map_err(|_| Error::new("invalid", "agent.discover params are invalid"))?;
        request.validate()?;
        if self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN {
            return Err(Error::new(
                "unavailable",
                "daemon is shutting down and rejects new discovery",
            ));
        }
        let profile = crate::config::resolve_profile(&self.inner.data_root, &request.profile)?;
        if request
            .harness
            .as_deref()
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
        let raw = self
            .inner
            .plugin_host
            .discover(profile, request.native_scope, request.cursor, request.limit)
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

    async fn agent_history(&self, params: &Value, principal: &Principal) -> Result<Value> {
        require_owner(principal)?;
        let request: AgentHistoryRequest = serde_json::from_value(params.clone())
            .map_err(|_| Error::new("invalid", "agent.history params are invalid"))?;
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
        let request: AgentGetRequest = serde_json::from_value(params.clone())
            .map_err(|_| Error::new("invalid", "agent.get params are invalid"))?;
        request.validate()?;
        let id = request.id.as_str();
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
        let source_states = source_states
            .into_iter()
            .map(|value| {
                serde_json::from_value::<SourceState>(value)
                    .map_err(|_| Error::new("internal", "source state view is invalid"))
            })
            .collect::<Result<Vec<_>>>()?;
        serde_json::to_value(AgentGetResponse {
            agent,
            observation,
            source_states,
        })
        .map_err(|_| Error::new("internal", "Agent view could not be encoded"))
    }

    async fn observation_view(&self, agent: &Agent) -> ObservationView {
        match self.inner.monitoring.view(agent).await {
            Ok(value) => value,
            Err(error) => ObservationView::unavailable(error.code),
        }
    }

    async fn agent_processes(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: AgentProcessesRequest = serde_json::from_value(params.clone())
            .map_err(|_| Error::new("invalid", "agent.processes params are invalid"))?;
        request.validate()?;
        let id = request.id.as_str();
        let generation = request.generation;
        authorize_agent_read(principal, id, generation)?;
        let scope = request.effective_scope();
        let after = request.cursor()?;
        let limit = request.limit;
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
            )
        };
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
        })
        .map_err(|_| Error::new("internal", "process page could not be encoded"))
    }

    async fn agent_git(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: AgentGitRequest = serde_json::from_value(params.clone())
            .map_err(|_| Error::new("invalid", "agent.git params are invalid"))?;
        request.validate()?;
        let id = request.id.as_str();
        let expected = request.generation;
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
        let request: AgentEditsRequest = serde_json::from_value(params.clone())
            .map_err(|_| Error::new("invalid", "agent.edits params are invalid"))?;
        request.validate()?;
        let id = request.id.as_str();
        let generation = request.generation;
        authorize_agent_read(principal, id, generation)?;
        let after = request.after.as_deref();
        let limit = request.limit;
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
        serde_json::to_value(AgentEditsResponse {
            agent: id.to_owned(),
            generation,
            edits,
            next,
        })
        .map_err(|_| Error::new("internal", "edit page could not be encoded"))
    }

    async fn agent_stats(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: AgentStatsRequest = serde_json::from_value(params.clone())
            .map_err(|_| Error::new("invalid", "agent.stats params are invalid"))?;
        request.validate()?;
        let id = request.id.as_str();
        let expected = request.generation;
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
        let observation = self.observation_view(&agent).await;
        serde_json::to_value(AgentStatsResponse {
            agent: agent.id,
            generation: agent.generation,
            observation,
        })
        .map_err(|_| Error::new("internal", "stats view could not be encoded"))
    }

    async fn agent_list(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: AgentListRequest = serde_json::from_value(params.clone())
            .map_err(|_| Error::new("invalid", "agent.list params are invalid"))?;
        request.validate()?;
        let after = request.after.as_deref();
        let limit = request.limit;
        let observations = request.observations;
        let agents = {
            let store = self.inner.store.lock().await;
            match principal {
                Principal::Owner => store.list_agents(after, limit)?,
                Principal::Agent { id, .. } => {
                    store.get_agent(id)?.into_iter().collect::<Vec<Agent>>()
                }
                Principal::Run { .. } => {
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
            }
        };
        if observations {
            let mut rows = Vec::new();
            let mut encoded_bytes = 0_usize;
            for agent in agents {
                let observation = self.observation_view(&agent).await;
                let row = AgentListRow {
                    agent,
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
                    break;
                }
                encoded_bytes += row_bytes + 1;
                rows.push(row);
            }
            let next = match principal {
                Principal::Owner => rows.last().map(|row| row.agent.id.clone()),
                Principal::Agent { .. } => None,
                Principal::Run { .. } => None,
                Principal::ProjectRule { .. } => None,
            };
            return serde_json::to_value(AgentListResponse { agents: rows, next })
                .map_err(|_| Error::new("internal", "Agent list could not be encoded"));
        }
        let next = match principal {
            Principal::Owner => agents.last().map(|agent| agent.id.clone()),
            Principal::Agent { .. } => None,
            Principal::Run { .. } => None,
            Principal::ProjectRule { .. } => None,
        };
        serde_json::to_value(AgentListResponse {
            agents: agents
                .into_iter()
                .map(|agent| AgentListRow {
                    agent,
                    observation: None,
                })
                .collect(),
            next,
        })
        .map_err(|_| Error::new("internal", "Agent list could not be encoded"))
    }

    async fn agent_result(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: AgentResultRequest = serde_json::from_value(params.clone())
            .map_err(|_| Error::new("invalid", "agent.result params are invalid"))?;
        request.validate()?;
        let id = request.id.as_str();
        authorize_agent_read(principal, id, request.generation)?;
        let (agent_metadata, generation, mut turns) = {
            let store = self.inner.store.lock().await;
            let agent = store
                .get_agent(id)?
                .ok_or_else(|| Error::new("not_found", format!("Agent {id} was not found")))?;
            let generation = request.generation.unwrap_or(agent.generation);
            let turns = if let Some(turn_id) = request.turn_id.as_deref() {
                vec![store.get_turn(id, generation, turn_id)?.ok_or_else(|| {
                    Error::new("not_found", format!("turn {turn_id} was not found"))
                })?]
            } else {
                store.list_turns(id, generation, request.limit)?
            };
            (agent, generation, turns)
        };
        self.mask_unknown_turns(id, generation, &mut turns).await?;
        serde_json::to_value(AgentResultResponse {
            agent: request.id,
            generation,
            turns,
            agent_metadata,
        })
        .map_err(|_| Error::new("internal", "agent.result response could not be encoded"))
    }

    async fn turn_get(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: TurnGetRequest = serde_json::from_value(params.clone())
            .map_err(|_| Error::new("invalid", "turn.get params are invalid"))?;
        request.validate()?;
        let agent = request.agent.as_str();
        let generation = request.generation;
        authorize_agent_read(principal, agent, Some(generation))?;
        let turn_id = request.turn_id.as_str();
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
        let request: EventsReadRequest = serde_json::from_value(params.clone())
            .map_err(|_| Error::new("invalid", "events.read params are invalid"))?;
        request.validate()?;
        let filter = authorize_event_filter(request.filter, principal)?;
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
        let request: EventsSubscribeRequest = serde_json::from_value(params.clone())
            .map_err(|_| Error::new("invalid", "events.subscribe params are invalid"))?;
        request.validate()?;
        let limit = request.limit;
        let wait_ms = request.wait_ms.min(LIVE_WAIT_MAX_MS);
        let filter = authorize_event_filter(request.filter, principal)?;

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
            .map(|command| UnsettledCommandSummary {
                id: command.id.clone(),
                agent: command.target.clone(),
                generation: command.generation,
                verb: command.verb.clone(),
                state: command.state,
            })
            .collect::<Vec<_>>();
        let workspace_custody: WorkspaceCustodyView =
            serde_json::from_value(crate::workspaces::custody_capability())
                .map_err(|_| Error::new("internal", "workspace custody view is invalid"))?;
        serde_json::to_value(DaemonStatusResponse {
            state: if mode == "open" { "running" } else { mode }.to_owned(),
            admission: mode.to_owned(),
            agents: usize::try_from(agent_count)
                .map_err(|_| Error::new("internal", "Agent count is invalid"))?,
            store_write_faulted: write_faulted,
            owned_plugins: self.inner.plugin_host.live_count(),
            observer_readers: self.inner.plugin_host.observer_count(),
            discovery_readers: self.inner.plugin_host.reader_count(),
            workspace_custody,
            workspace_recovery_error: self.inner.workspace_recovery_error.lock_recover().clone(),
            unknown_commands_count,
            unknown_commands,
            unknown_commands_truncated,
            unsettled_commands_count: unsettled_commands.len(),
            unsettled_commands: unsettled_command_details,
            unsettled_commands_truncated,
            blockers,
        })
        .map_err(|_| Error::new("internal", "daemon status could not be encoded"))
    }

    async fn status_blockers(&self) -> Result<Vec<StatusBlocker>> {
        self.clear_terminal_unknown_cache().await;
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
            blockers.push(StatusBlocker::WorkspaceProcesses);
        }
        let discovery_readers = self.inner.plugin_host.reader_count();
        if discovery_readers > 0 {
            blockers.push(StatusBlocker::DiscoveryReaders {
                count: discovery_readers,
            });
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
            blockers.push(StatusBlocker::AgentLive {
                agent: agent.id,
                generation: agent.generation,
                state: agent.state,
            });
        }
        let mut unsettled_ids = HashSet::new();
        for command in unsettled_commands {
            unsettled_ids.insert(command.id.clone());
            blockers.push(StatusBlocker::CommandUnsettled {
                command_id: command.id,
                state: command.state,
            });
        }
        if write_faulted {
            blockers.push(StatusBlocker::StoreWriteFaulted);
        }
        let unknown_ids = self.unknown_ids();
        for id in unknown_ids.iter().take(STATUS_BLOCKER_MAX) {
            if unsettled_ids.contains(id) {
                continue;
            }
            blockers.push(StatusBlocker::CommandUnknown {
                command_id: id.clone(),
            });
        }
        if unknown_ids.len() > STATUS_BLOCKER_MAX {
            blockers.push(StatusBlocker::CommandUnknownMore {
                count: unknown_ids.len() - STATUS_BLOCKER_MAX,
            });
        }
        Ok(blockers)
    }

    async fn shutdown(&self, params: &Value, principal: &Principal) -> Result<Value> {
        require_owner(principal)?;
        let request: DaemonShutdownRequest = serde_json::from_value(params.clone())
            .map_err(|_| Error::new("invalid", "daemon.shutdown params are malformed"))?;
        request.validate()?;
        let mode = request.mode;
        let id = request.id.as_str();
        let result = if mode == ShutdownMode::Force {
            DaemonShutdownResponse {
                state: CommandState::Dispatched,
                mode,
                blockers: None,
                store_write_faulted: None,
            }
        } else {
            let blockers = self.status_blockers().await?;
            DaemonShutdownResponse {
                state: if blockers.is_empty() {
                    CommandState::Applied
                } else {
                    CommandState::Dispatched
                },
                mode,
                blockers: Some(blockers),
                store_write_faulted: None,
            }
        };
        let result = serde_json::to_value(result)
            .map_err(|_| Error::new("internal", "shutdown result could not be encoded"))?;
        let write = {
            let mut store = self.inner.store.lock().await;
            let write = match store.record_write(
                id,
                &principal_name(principal),
                "daemon.shutdown",
                &json!({"mode": mode.as_str()}),
                &result,
                now_ms(),
            ) {
                Ok(write) => Some(write.result),
                Err(_error) if mode == ShutdownMode::Force && store.is_write_faulted() => None,
                Err(error) => return Err(error),
            };
            // Rule admissions serialize their final mode check under this same
            // writer guard, so no firing crosses the drain/force boundary.
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
            if mode == ShutdownMode::Force {
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
                store_write_faulted: Some(true),
            }),
        };
        self.inner.plugin_host.close_discovery_admission();
        self.signal_progress();
        serde_json::to_value(response?)
            .map_err(|_| Error::new("internal", "shutdown result could not be encoded"))
    }

    fn start_run_worker(&self, command: Command) -> RunWorkerStart {
        let run_id = command.id.clone();
        let mut workers = self.inner.run_workers.lock_recover();
        if workers.contains_key(&run_id) {
            return RunWorkerStart::AlreadyActive;
        }
        if self.inner.mode.load(Ordering::SeqCst) == MODE_FORCE {
            return RunWorkerStart::ForceRequested;
        }

        let (start_tx, start_rx) = oneshot::channel();
        let (completed_tx, completed_rx) = watch::channel(false);
        let worker_id = run_id.clone();
        let worker_router = self.clone();
        let registry = Arc::downgrade(&self.inner);
        let task = tokio::spawn(async move {
            let mut completion = RunWorkerCompletion {
                router: registry,
                run_id: worker_id,
                completed: completed_tx,
                finished: false,
            };
            if start_rx.await.is_ok() {
                crate::scripts::execute_dispatched(worker_router, command).await;
            }
            completion.finish();
        });
        workers.insert(
            run_id,
            RunWorkerHandle {
                task,
                completed: completed_rx,
            },
        );
        drop(workers);
        let _ = start_tx.send(());
        RunWorkerStart::Started
    }

    async fn wait_run_worker(&self, run_id: &str) {
        let completed = self
            .inner
            .run_workers
            .lock_recover()
            .get(run_id)
            .map(|worker| worker.completed.clone());
        if let Some(mut completed) = completed {
            loop {
                if *completed.borrow_and_update() {
                    break;
                }
                if completed.changed().await.is_err() {
                    break;
                }
            }
        }
        let worker = self.inner.run_workers.lock_recover().remove(run_id);
        if let Some(worker) = worker
            && worker.task.await.is_err()
        {
            self.settle_run_worker_unknown(run_id, "script_worker_failed")
                .await;
        }
    }

    async fn settle_run_cancelled_before_start(&self, run_id: &str) {
        let process = crate::scripts::unobserved_process_result(true);
        let outcome = json!({
            "code": "failed",
            "cause": "script_cancelled_before_start",
            "process": process,
        });
        let _ = self
            .settle(run_id, CommandState::Failed, Some(outcome))
            .await;
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
        if let Some((id, linked_id)) = rule_admission_dispatch_plan(admission) {
            self.schedule_command(id, linked_id);
        }
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
        if command.verb == "run"
            && command.principal != "owner"
            && !command.principal.starts_with("project:")
        {
            self.reject_undispatched(&id, "unsupported").await;
            return;
        }
        if command.verb == "run" && command.principal == "owner" {
            let frozen: FrozenScriptRun = match serde_json::from_value(command.args.clone()) {
                Ok(frozen) => frozen,
                Err(_) => {
                    self.reject_undispatched(&id, "invalid").await;
                    return;
                }
            };
            if let Err(error) = self.verify_global_run(&frozen).await {
                self.reject_undispatched(&id, &error.code).await;
                return;
            }
        }
        if let Some(project_id) = command.principal.strip_prefix("project:") {
            let registration = {
                let store = self.inner.store.lock().await;
                store.get_project_registration("owner", project_id)
            };
            let registration = match registration {
                Ok(Some(registration)) => registration,
                Ok(None) => {
                    self.reject_undispatched(&id, "not_allowed").await;
                    return;
                }
                Err(error) => {
                    self.reject_undispatched(&id, &error.code).await;
                    return;
                }
            };
            if let Err(error) =
                crate::automation::verify_project_registration(&self.inner.data_root, &registration)
                    .await
            {
                self.reject_undispatched(&id, &error.code).await;
                return;
            }
            if command.verb == "launch" {
                if let Err(error) = validate_project_rule_launch_command(&registration, &command) {
                    self.reject_undispatched(&id, &error.code).await;
                    return;
                }
            } else if command.verb == "resume" {
                if let Err(error) = validate_project_rule_resume_command(
                    &registration,
                    &command,
                    linked_id.is_some(),
                ) {
                    self.reject_undispatched(&id, &error.code).await;
                    return;
                }
                if let Err(error) = self
                    .verify_project_rule_resume_after_admission(&registration, &command)
                    .await
                {
                    self.reject_undispatched(&id, &error.code).await;
                    return;
                }
            } else if matches!(
                command.verb.as_str(),
                "stop" | "send" | "steer" | "interrupt" | "approve" | "deny" | "answer"
            ) {
                let (Some(target), Some(generation)) =
                    (command.target.as_deref(), command.generation)
                else {
                    self.reject_undispatched(&id, "invalid").await;
                    return;
                };
                if let Err(error) = self
                    .verify_project_rule_target(&registration, target, generation)
                    .await
                {
                    self.reject_undispatched(&id, &error.code).await;
                    return;
                }
            }
        }
        if matches!(command.verb.as_str(), "launch" | "resume")
            && let Err(error) = self.inner.workspaces.prepare(&id).await
        {
            self.settle_plugin_error(&id, &error).await;
            self.signal_progress();
            return;
        }
        let launch_agent = if command.verb == "launch" {
            match self.accepted_launch_agent(&id).await {
                Ok(Some(agent)) => Some(agent),
                Ok(None) => return, // A retained/settled command cannot acquire new I/O.
                Err(error) => {
                    self.reject_undispatched(&id, &error.code).await;
                    return;
                }
            }
        } else {
            None
        };
        let (resume_profile, resume_source_state) = if command.verb == "resume" {
            match self.capture_resume_source_state(&id).await {
                Ok((profile, source_state)) => (Some(profile), Some(source_state)),
                Err(error) => {
                    self.reject_undispatched(&id, &error.code).await;
                    return;
                }
            }
        } else {
            (None, None)
        };
        let controlled_profile = launch_agent
            .as_ref()
            .map(|agent| &agent.profile_snapshot)
            .or(resume_profile.as_ref());
        if let Some(profile) = controlled_profile
            && profile.get("route_binding").is_some()
            && let Err(error) = crate::proxy::check_profile_ready(profile).await
        {
            self.reject_undispatched(&id, &error.code).await;
            return;
        }
        let launch_binding = if let Some(agent) = launch_agent {
            if agent.harness == "codex" {
                let workspace = match agent.workspace.as_deref() {
                    Some(workspace) => workspace,
                    None => {
                        self.reject_undispatched(&id, "invalid").await;
                        return;
                    }
                };
                let binding = match self
                    .inner
                    .plugin_host
                    .profile_store(agent.profile_snapshot.clone(), workspace)
                    .await
                {
                    Ok(binding) => binding,
                    Err(error) => {
                        self.reject_undispatched(&id, &error.code).await;
                        return;
                    }
                };
                Some((agent, binding))
            } else {
                None // Fake retains its ordinary dispatch; no metadata filesystem I/O.
            }
        } else {
            None
        };
        let dispatched = loop {
            let change = {
                let mut store = self.inner.store.lock().await;
                let dispatched = match &launch_binding {
                    Some((expected_agent, binding)) => store.dispatch_with_native_store_binding(
                        &id,
                        now_ms(),
                        expected_agent,
                        binding,
                    ),
                    None if command.verb == "run"
                        && self.inner.mode.load(Ordering::SeqCst) == MODE_FORCE =>
                    {
                        Err(Error::new(
                            "bad_state",
                            "force shutdown prevents script dispatch",
                        ))
                    }
                    None => store.dispatch(&id, now_ms()),
                };
                let result = match dispatched {
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
                                if event.kind.starts_with("provider.")
                                    || matches!(
                                        event.kind.as_str(),
                                        "command.applied"
                                            | "command.failed"
                                            | "command.rejected"
                                            | "command.cancelled"
                                    )
                                    || (event.agent == target
                                        && event.generation == generation
                                        && (event.kind.starts_with("turn.")
                                            || event.kind.starts_with("session.")
                                            || event.kind.starts_with("request."))) =>
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
                Ok(change) if change.value.state.is_terminal() => {
                    // Store may durably cancel a terminal predecessor's
                    // successor at this dispatch gate; never pass it to a native
                    // plugin after that result.
                    self.signal_progress();
                    return;
                }
                Ok(change) => break change.value,
                Err(error) => {
                    self.settle_dispatch_error(
                        &id,
                        &error,
                        change.1,
                        launch_binding.is_some(),
                        command.verb == "run"
                            && (command.principal == "owner"
                                || command.principal.starts_with("project:")),
                    )
                    .await;
                    return;
                }
            }
        };

        // Only a Store-confirmed dispatch may reach a plugin. Queued/held,
        // terminal, unknown, and any future non-dispatched state are not effects.
        if dispatched.state != CommandState::Dispatched {
            self.signal_progress();
            return;
        }

        if dispatched.verb == "run" {
            let run_id = dispatched.id.clone();
            match self.start_run_worker(dispatched) {
                RunWorkerStart::Started | RunWorkerStart::AlreadyActive => {
                    self.wait_run_worker(&run_id).await;
                }
                RunWorkerStart::ForceRequested => {
                    self.settle_run_cancelled_before_start(&run_id).await;
                }
            }
            self.signal_progress();
            return;
        }
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
                            self.schedule_command(linked_id, None);
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

    async fn settle_dispatch_error(
        &self,
        id: &str,
        error: &Error,
        write_faulted: bool,
        native_binding: bool,
        script_run: bool,
    ) {
        // This is only the local Store dispatch result, before any control RPC.
        // A plugin error with the same code still follows post-I/O C1 handling.
        let revoked_run_authority = script_run
            && error.code == "not_allowed"
            && is_deterministic_pre_admission_rejection(&error.code);
        if write_faulted {
            self.note_unknown_if_uncertain(id, "store_dispatch_outcome_unknown")
                .await;
        } else if matches!(
            error.code.as_str(),
            "bad_state" | "stale_generation" | "not_found" | "provider_held"
        ) || native_binding
            && matches!(
                error.code.as_str(),
                "source_integrity" | "frame_too_large" | "invalid"
            )
            || revoked_run_authority
        {
            self.reject_undispatched(id, &error.code).await;
        }
        self.signal_progress();
    }

    async fn accepted_launch_agent(&self, command_id: &str) -> Result<Option<Agent>> {
        let agent = {
            let store = self.inner.store.lock().await;
            let command = store
                .get_command(command_id)?
                .ok_or_else(|| Error::new("not_found", "launch command was not found"))?;
            if command.verb != "launch" || command.state != CommandState::Accepted {
                return Ok(None);
            }
            let target = command
                .target
                .as_deref()
                .ok_or_else(|| Error::new("bad_state", "launch target is missing"))?;
            let agent = store
                .get_agent(target)?
                .ok_or_else(|| Error::new("not_found", "launch Agent was not found"))?;
            if command.generation != Some(agent.generation)
                || agent.generation != 0
                || agent.state != AgentState::Created
                || agent.observe_only
            {
                return Err(Error::new(
                    "stale_generation",
                    "launch no longer targets its admitted Agent",
                ));
            }
            agent
        };
        Ok(Some(agent))
    }

    async fn capture_resume_source_state(&self, command_id: &str) -> Result<(Value, Value)> {
        let (profile, generation, source, latest_turn) = {
            let store = self.inner.store.lock().await;
            let command = store
                .get_command(command_id)?
                .ok_or_else(|| Error::new("not_found", "resume command was not found"))?;
            let agent_id = command
                .target
                .as_deref()
                .filter(|target| eliot_core::parse_agent_id(target).is_ok())
                .ok_or_else(|| Error::new("not_found", "resume target Agent is missing"))?
                .to_owned();
            let agent = store
                .get_agent(&agent_id)?
                .ok_or_else(|| Error::new("not_found", "resume Agent was not found"))?;
            let generation = validate_retained_resume_agent(&agent, &command)?;
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
            (agent.profile_snapshot, generation, source, latest_turn)
        };
        let mut source_state = json!({
            "retained_generation": generation,
            "retained_source": source,
            "current_turn": latest_turn,
        });
        self.redact_native_value(&mut source_state);
        Ok((profile, source_state))
    }

    async fn reject_undispatched(&self, id: &str, reason: &str) {
        let outcome = json!({"reason": reason});
        let (result, write_faulted) = {
            let mut store = self.inner.store.lock().await;
            let result = match store.get_command(id) {
                Ok(Some(command))
                    if matches!(command.state, CommandState::Accepted | CommandState::Queued) =>
                {
                    let workspace_resume = if command.verb == "resume" {
                        match (
                            command.target.as_deref(),
                            command
                                .generation
                                .and_then(|generation| generation.checked_add(1)),
                        ) {
                            (Some(target), Some(next_generation)) => {
                                store.get_workspace(target).map(|workspace| {
                                    workspace.is_some_and(|workspace| {
                                        workspace.start_command_id == id
                                            && workspace.target_generation == next_generation
                                    })
                                })
                            }
                            _ => Ok(false),
                        }
                    } else {
                        Ok(false)
                    };
                    match workspace_resume {
                        Ok(true) => store.reject_workspace_resume(id, reason, now_ms()),
                        Ok(false) => {
                            store.settle(id, CommandState::Rejected, Some(outcome), now_ms())
                        }
                        Err(error) => Err(error),
                    }
                }
                Ok(_) => return,
                Err(error) => Err(error),
            };
            if let Ok(change) = &result {
                self.publish_locked(&change.events);
            }
            (result, store.is_write_faulted())
        };
        self.finish_settlement(id, result, write_faulted, "store_settlement_failed", false)
            .await;
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
                self.note_unknown_if_uncertain(id, cause).await;
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
                self.note_unknown_if_uncertain(id, cause).await;
                self.signal_progress();
                None
            }
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
        if &current != registration {
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
            .lock_recover()
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
        self.finish_settlement(id, result, write_faulted, &persistence_cause, true)
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
                self.note_unknown(&change.events);
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
                "command.applied" | "command.failed" | "command.rejected" | "command.cancelled"
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

    async fn mask_unknown_turns(
        &self,
        agent: &str,
        generation: i64,
        turns: &mut [Turn],
    ) -> Result<()> {
        self.clear_terminal_unknown_cache().await;
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
            .lock_recover()
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
    let mut snapshot = crate::config::resolve_profile(data_root, profile_name)?;
    if snapshot.get("harness").and_then(Value::as_str) != Some(harness.as_str()) {
        return Err(Error::new(
            "invalid",
            "attach harness does not match the selected profile",
        ));
    }
    crate::config::capture_native_store_inputs(&mut snapshot, |name| std::env::var_os(name))?;
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
    if matches!(command.verb.as_str(), "launch" | "resume")
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
    if args
        .keys()
        .any(|key| !matches!(key.as_str(), "profile" | "workspace" | "text"))
    {
        return Err(Error::new(
            "invalid",
            "rule launch accepts only profile, workspace, and optional text",
        ));
    }
    let profile = args
        .get("profile")
        .and_then(Value::as_str)
        .filter(|profile| {
            !profile.trim().is_empty()
                && profile.trim() == *profile
                && !profile.contains(concat!("$", "{"))
        })
        .ok_or_else(|| Error::new("invalid", "rule launch profile must be a literal name"))?;
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
        if !registration
            .profiles
            .iter()
            .any(|approved| approved == profile)
        {
            return Err(Error::new(
                "not_allowed",
                "project registration does not approve the launch profile",
            ));
        }
        if workspace != registration.checkout.as_str() {
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

fn validate_rule_launch_preparation(prepared: &PreparedSubmission) -> Result<()> {
    if prepared.request.verb != "launch" {
        return Ok(());
    }
    validate_prepared_rule_launch_command(&prepared.request)?;
    if prepared.workspace.is_some() {
        return Err(Error::new(
            "unsupported",
            "rule launch requires an existing workspace without worktree preparation",
        ));
    }
    Ok(())
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
    if args.contains_key("workspace_preparation")
        || args.contains_key("worktree")
        || args.contains_key("base")
        || args
            .get("profile_snapshot")
            .and_then(|snapshot| snapshot.get("setup"))
            .and_then(Value::as_array)
            .is_some_and(|setup| !setup.is_empty())
    {
        return Err(Error::new(
            "unsupported",
            "rule launch cannot prepare a worktree or run profile setup",
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
    match agent.harness.as_str() {
        "codex" => {
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
        }
        "fake" => {
            if agent.profile_snapshot.get("native_store_binding").is_some() {
                return Err(Error::new(
                    "source_integrity",
                    "Fake resume cannot use a Codex native store binding",
                ));
            }
        }
        harness => {
            return Err(Error::new(
                "unsupported",
                format!("resume is not supported for the {harness} harness"),
            ));
        }
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

fn authorize_rpc_method(principal: &Principal, method: &str) -> Result<()> {
    if matches!(principal, Principal::Run { .. }) && method != "command.get" {
        return Err(Error::new(
            "not_allowed",
            "Run credentials may call only command.get",
        ));
    }
    Ok(())
}

fn auth_fields_are_exact(auth: &serde_json::Map<String, Value>, kind: &str) -> bool {
    let fields: &[&str] = match kind {
        "owner" => &["kind", "credential"],
        "agent" => &["kind", "agent_id", "generation", "credential"],
        "run" => &["kind", "run_command_id", "credential"],
        _ => return false,
    };
    auth.len() == fields.len() && fields.iter().all(|field| auth.contains_key(*field))
}

fn authorize_command(principal: &Principal, request: &mut SubmitCommand) -> Result<()> {
    if let Principal::ProjectRule { registration } = principal {
        if request.verb == "run" {
            if !registration.actions.iter().any(|action| action == "run") {
                return Err(Error::new(
                    "not_allowed",
                    "project rule is not approved for run",
                ));
            }
            if request.target.is_some()
                || request.expected_generation.is_some()
                || request.expected_turn_id.is_some()
                || request.request_id.is_some()
                || request.probe
            {
                return Err(Error::new(
                    "invalid",
                    "Run command must be targetless and unbound",
                ));
            }
            return Ok(());
        }
        if request.verb == "launch" {
            return validate_rule_launch_action(request, Some(registration));
        }
        if request.verb == "resume" {
            return validate_rule_resume_action(request, Some(registration));
        }
        if !matches!(
            request.verb.as_str(),
            "stop" | "send" | "steer" | "interrupt" | "approve" | "deny" | "answer"
        ) || !registration
            .actions
            .iter()
            .any(|action| action == &request.verb)
        {
            return Err(Error::new(
                "not_allowed",
                "project rule is not approved for this action",
            ));
        }
        if request.target.is_none() || request.expected_generation.is_none() {
            return Err(Error::new(
                "invalid",
                "project action requires its frozen Agent and generation",
            ));
        }
        if matches!(request.verb.as_str(), "approve" | "deny" | "answer")
            && (request
                .expected_turn_id
                .as_deref()
                .is_none_or(str::is_empty)
                || request.request_id.as_deref().is_none_or(str::is_empty))
        {
            return Err(Error::new(
                "invalid",
                "project request reply requires its captured turn and request",
            ));
        }
        return Ok(());
    }
    if matches!(principal, Principal::Owner) && request.verb == "run" {
        return Err(Error::new(
            "unsupported",
            "standalone Owner Run submission is unsupported",
        ));
    }
    if matches!(principal, Principal::Run { .. }) {
        return Err(Error::new(
            "not_allowed",
            "Run credentials cannot submit commands",
        ));
    }
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
    if matches!(principal, Principal::ProjectRule { .. }) {
        return Err(Error::new(
            "not_allowed",
            "project rule authority cannot read commands",
        ));
    }
    if let Principal::Run { run_command_id, .. } = principal
        && command.id.as_str() != run_command_id.as_str()
    {
        return Err(Error::new(
            "not_allowed",
            "Run credentials are scoped to their own command",
        ));
    }
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
    if matches!(principal, Principal::Run { .. }) {
        return Err(Error::new(
            "not_allowed",
            "Run credentials cannot read Agents",
        ));
    }
    if matches!(principal, Principal::ProjectRule { .. }) {
        return Err(Error::new(
            "not_allowed",
            "project rule authority cannot read Agents",
        ));
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn router() -> Router {
        router_with_data_root(PathBuf::from("synthetic-data-root"))
    }

    fn router_with_data_root(data_root: PathBuf) -> Router {
        let store = eliot_store::Store::open(std::path::Path::new(":memory:")).unwrap();
        Router::new(
            data_root,
            "owner-credential".to_owned(),
            Arc::new(AsyncMutex::new(store)),
            Bus::new(16),
        )
    }

    fn command_with_state(id: &str, verb: &str, state: CommandState) -> Command {
        Command {
            id: id.to_owned(),
            principal: "owner".to_owned(),
            method: "command.submit".to_owned(),
            verb: verb.to_owned(),
            target: None,
            expected_generation: (verb == "launch").then_some(0),
            generation: None,
            args: json!({}),
            state,
            turn_id: None,
            request_id: None,
            outcome: None,
            created_at: 0,
            updated_at: 0,
            probe: false,
        }
    }

    fn rule_admission(command: Command, linked_command: Option<Command>) -> eliot_store::Admission {
        eliot_store::Admission {
            command,
            linked_command,
            events: Vec::new(),
            is_new: true,
        }
    }

    #[test]
    fn rule_admission_dispatch_plan_keeps_linked_send_and_never_replays_uncertain_work() {
        let accepted_launch = rule_admission(
            command_with_state("launch-accepted", "launch", CommandState::Accepted),
            Some(command_with_state(
                "send-queued",
                "send",
                CommandState::Queued,
            )),
        );
        assert_eq!(
            rule_admission_dispatch_plan(&accepted_launch),
            Some(("launch-accepted".to_owned(), Some("send-queued".to_owned())))
        );

        let applied_launch = rule_admission(
            command_with_state("launch-applied", "launch", CommandState::Applied),
            Some(command_with_state(
                "send-accepted",
                "send",
                CommandState::Accepted,
            )),
        );
        assert_eq!(
            rule_admission_dispatch_plan(&applied_launch),
            Some(("send-accepted".to_owned(), None))
        );

        let accepted_resume = rule_admission(
            command_with_state("resume-accepted", "resume", CommandState::Accepted),
            Some(command_with_state(
                "resume-send-queued",
                "send",
                CommandState::Queued,
            )),
        );
        assert_eq!(
            rule_admission_dispatch_plan(&accepted_resume),
            Some((
                "resume-accepted".to_owned(),
                Some("resume-send-queued".to_owned())
            ))
        );

        let queued_resume = rule_admission(
            command_with_state("resume-queued", "resume", CommandState::Queued),
            Some(command_with_state(
                "resume-send-held",
                "send",
                CommandState::Queued,
            )),
        );
        assert_eq!(
            rule_admission_dispatch_plan(&queued_resume),
            Some((
                "resume-queued".to_owned(),
                Some("resume-send-held".to_owned())
            ))
        );

        let applied_resume = rule_admission(
            command_with_state("resume-applied", "resume", CommandState::Applied),
            Some(command_with_state(
                "resume-send-accepted",
                "send",
                CommandState::Accepted,
            )),
        );
        assert_eq!(
            rule_admission_dispatch_plan(&applied_resume),
            Some(("resume-send-accepted".to_owned(), None))
        );

        for launch_state in [CommandState::Dispatched, CommandState::Unknown] {
            let uncertain_launch = rule_admission(
                command_with_state("launch-uncertain", "launch", launch_state),
                Some(command_with_state(
                    "send-pending",
                    "send",
                    CommandState::Accepted,
                )),
            );
            assert_eq!(rule_admission_dispatch_plan(&uncertain_launch), None);

            let uncertain_resume = rule_admission(
                command_with_state("resume-uncertain", "resume", launch_state),
                Some(command_with_state(
                    "resume-send-pending",
                    "send",
                    CommandState::Accepted,
                )),
            );
            assert_eq!(rule_admission_dispatch_plan(&uncertain_resume), None);
        }

        for send_state in [CommandState::Dispatched, CommandState::Unknown] {
            let uncertain_input = rule_admission(
                command_with_state("launch-applied", "launch", CommandState::Applied),
                Some(command_with_state("send-uncertain", "send", send_state)),
            );
            assert_eq!(rule_admission_dispatch_plan(&uncertain_input), None);
        }
    }

    #[test]
    fn resume_capture_preserves_codex_binding_and_accepts_only_owned_fake_subjects() {
        let mut fake = Agent::new("a7", "descriptive-project", "fake", "fake", 10);
        fake.generation = 2;
        fake.state = eliot_core::AgentState::Stopped;
        fake.native_scope = Some("fake-scope-a7".to_owned());
        fake.native_ref = Some("fake-subject-a7".to_owned());
        let mut resume = command_with_state("resume-fixture", "resume", CommandState::Accepted);
        resume.target = Some(fake.id.clone());
        resume.expected_generation = Some(2);
        resume.generation = Some(2);
        assert_eq!(validate_retained_resume_agent(&fake, &resume).unwrap(), 2);

        let mut ordinary = resume.clone();
        ordinary.expected_generation = None;
        assert_eq!(validate_retained_resume_agent(&fake, &ordinary).unwrap(), 2);
        let mut queued = ordinary.clone();
        queued.state = CommandState::Queued;
        assert_eq!(validate_retained_resume_agent(&fake, &queued).unwrap(), 2);
        let mut missing_stored_generation = resume.clone();
        missing_stored_generation.generation = None;
        assert_eq!(
            validate_retained_resume_agent(&fake, &missing_stored_generation)
                .unwrap_err()
                .code,
            "stale_generation"
        );

        let mut observed = fake.clone();
        observed.observe_only = true;
        assert_eq!(
            validate_retained_resume_agent(&observed, &resume)
                .unwrap_err()
                .code,
            "not_allowed"
        );
        let mut unknown_agent = fake.clone();
        unknown_agent.state = AgentState::Unknown;
        assert_eq!(
            validate_retained_resume_agent(&unknown_agent, &resume)
                .unwrap_err()
                .code,
            "not_allowed"
        );
        let mut unknown_resume = resume.clone();
        unknown_resume.state = CommandState::Unknown;
        assert_eq!(
            validate_retained_resume_agent(&fake, &unknown_resume)
                .unwrap_err()
                .code,
            "bad_state"
        );
        let mut nonterminal = fake.clone();
        nonterminal.state = eliot_core::AgentState::Busy;
        assert_eq!(
            validate_retained_resume_agent(&nonterminal, &resume)
                .unwrap_err()
                .code,
            "bad_state"
        );

        let mut codex = fake.clone();
        codex.harness = "codex".to_owned();
        codex.profile = "codex-luna".to_owned();
        codex.native_scope = Some("codex-scope-a7".to_owned());
        codex.native_ref = Some("codex-thread-a7".to_owned());
        let binding = NativeStoreBinding {
            home: "fixture-home".to_owned(),
            root: "fixture-root".to_owned(),
            native_scope: "codex-scope-a7".to_owned(),
        };
        codex.profile_snapshot = json!({
            "native_store_binding": serde_json::to_value(&binding).unwrap()
        });
        assert_eq!(validate_retained_resume_agent(&codex, &resume).unwrap(), 2);

        codex.native_scope = Some("different-scope".to_owned());
        assert_eq!(
            validate_retained_resume_agent(&codex, &resume)
                .unwrap_err()
                .code,
            "source_integrity"
        );
        codex.profile_snapshot = json!({});
        assert_eq!(
            validate_retained_resume_agent(&codex, &resume)
                .unwrap_err()
                .code,
            "unsupported"
        );
    }

    #[tokio::test]
    async fn global_rule_known_secret_is_rejected_before_any_rule_seed() {
        const SECRET: &str = "fixture-known-rule-secret";

        let router = router();
        router
            .inner
            .secrets
            .lock_recover()
            .insert(SECRET.to_owned());
        let (before_high_water, before_events) = {
            let store = router.inner.store.lock().await;
            (
                store.high_water().unwrap(),
                store.read_events(0, 100, None).unwrap(),
            )
        };
        let make_rule = |id: &str, matches: serde_json::Map<String, Value>| {
            let value = json!({
                "id":id,
                "enabled":true,
                "on":{"event":"turn.completed", "match":matches},
                "do":[{"action":"stop", "target":"${event.agent}",
                    "expected_generation":"${event.generation}"}],
            });
            eliot_store::RuleDefinitionInput {
                id: id.to_owned(),
                canonical_definition: serde_json::to_string(&value).unwrap(),
                definition_digest: eliot_core::canonical_json_sha256(&value).unwrap(),
                enabled: true,
            }
        };
        let mut ordinary_match = serde_json::Map::new();
        ordinary_match.insert("subject.harness".to_owned(), json!("fake"));
        let ordinary_definition = make_rule("ordinary-first", ordinary_match);
        let mut secret_value_match = serde_json::Map::new();
        secret_value_match.insert("subject.harness".to_owned(), json!(SECRET));
        let secret_value_definition = make_rule("secret-match", secret_value_match);
        let mut dynamic_match = serde_json::Map::new();
        dynamic_match.insert(format!("data.{SECRET}"), json!("ordinary-value"));
        let secret_key_definition = make_rule("secret-match-key", dynamic_match);

        for definitions in [
            vec![ordinary_definition, secret_value_definition],
            vec![secret_key_definition],
        ] {
            let error = router
                .initialize_rules("owner", &definitions, &[], now_ms())
                .await
                .unwrap_err();
            assert_eq!(error.code, "secret_in_payload");
            assert!(error.diagnostics.is_empty());
            assert!(!error.message.contains(SECRET));
            assert!(!format!("{error:?}").contains(SECRET));

            let store = router.inner.store.lock().await;
            for definition in &definitions {
                assert!(
                    store
                        .get_rule_state("owner", &definition.id)
                        .unwrap()
                        .is_none()
                );
            }
            assert_eq!(store.high_water().unwrap(), before_high_water);
            assert_eq!(store.read_events(0, 100, None).unwrap(), before_events);
        }
    }

    #[tokio::test]
    async fn rule_catch_up_commits_empty_range_once_without_advancing_live_cursor() {
        let root =
            std::env::temp_dir().join(format!("eliot-rule-catch-up-empty-{}", ulid::Ulid::new()));
        let rules_dir = root.join("rules");
        std::fs::create_dir_all(&rules_dir).unwrap();
        std::fs::write(
            rules_dir.join("catch-up.toml"),
            r#"
[[rule]]
id = "catch-up-empty"
on = { event = "turn.completed" }
do = [{ action = "stop", target = "a1", expected_generation = 1 }]
"#,
        )
        .unwrap();
        let router = router_with_data_root(root.clone());
        let policy = SecretPolicy::default();
        crate::automation::initialize_global_rules(&router, &root, &policy)
            .await
            .unwrap();
        let request_id = ulid::Ulid::new().to_string();
        let request = json!({
            "id":request_id,
            "rule":"catch-up-empty",
            "after_seq":0,
        });

        let first: Command = serde_json::from_value(
            router
                .rule_catch_up(&request, &Principal::Owner)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(first.verb, "rule.catch_up");
        assert_eq!(first.state, CommandState::Applied);
        assert_eq!(first.args["through_seq"], 0);
        assert_eq!(first.args["revision"], 1);
        assert_eq!(first.outcome.as_ref().unwrap()["children"], json!([]));

        let explicit_bound = json!({
            "id":request_id,
            "rule":"catch-up-empty",
            "after_seq":0,
            "through_seq":0,
        });
        assert_eq!(
            router
                .rule_catch_up(&explicit_bound, &Principal::Owner)
                .await
                .unwrap_err()
                .code,
            "id_conflict"
        );

        let explicit_id = ulid::Ulid::new().to_string();
        let explicit_request = json!({
            "id":explicit_id,
            "rule":"catch-up-empty",
            "after_seq":0,
            "through_seq":0,
        });
        let explicit_parent: Command = serde_json::from_value(
            router
                .rule_catch_up(&explicit_request, &Principal::Owner)
                .await
                .unwrap(),
        )
        .unwrap();
        let omitted_bound = json!({
            "id":explicit_id,
            "rule":"catch-up-empty",
            "after_seq":0,
        });
        assert_eq!(
            router
                .rule_catch_up(&omitted_bound, &Principal::Owner)
                .await
                .unwrap_err()
                .code,
            "id_conflict"
        );

        std::fs::write(
            rules_dir.join("catch-up.toml"),
            r#"
[[rule]]
id = "catch-up-empty"
on = { event = "turn.started" }
do = [{ action = "stop", target = "a1", expected_generation = 1 }]
"#,
        )
        .unwrap();
        crate::automation::initialize_global_rules(&router, &root, &policy)
            .await
            .unwrap();
        let replay: Command = serde_json::from_value(
            router
                .rule_catch_up(&request, &Principal::Owner)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(replay, first);
        assert_eq!(replay.args["revision"], 1);
        assert_eq!(explicit_parent.args["revision"], 1);
        let state = router
            .inner
            .store
            .lock()
            .await
            .get_rule_state("owner", "catch-up-empty")
            .unwrap()
            .unwrap();
        assert_eq!(state.revision, 2);
        assert!(state.retiring_revision.is_some());
        assert_eq!(state.current_cursor, 2);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn rule_catch_up_marks_deterministic_interrupt_refusal_and_replays_once() {
        let root = std::env::temp_dir().join(format!(
            "eliot-rule-catch-up-interrupt-{}",
            ulid::Ulid::new()
        ));
        let rules_dir = root.join("rules");
        std::fs::create_dir_all(&rules_dir).unwrap();
        let router = router_with_data_root(root.clone());

        let event_seq = {
            let mut store = router.inner.store.lock().await;
            let (agent, send_id) = result_fixture(&mut store);
            read_fixture_event(
                &mut store,
                &agent,
                "turn.completed",
                json!({
                    "command_id":send_id,
                    "turn_id":"turn-1",
                    "status":"completed",
                }),
            );
            if store
                .get_command(&send_id)
                .unwrap()
                .is_some_and(|command| command.state == CommandState::Dispatched)
            {
                store
                    .settle(
                        &send_id,
                        CommandState::Applied,
                        Some(json!({"turn_id":"turn-1"})),
                        now_ms(),
                    )
                    .unwrap();
            }
            store
                .read_events(0, 1_000, None)
                .unwrap()
                .into_iter()
                .find(|event| {
                    event.kind == "turn.completed"
                        && event.data.get("command_id").and_then(Value::as_str)
                            == Some(send_id.as_str())
                })
                .expect("committed historical turn completion")
                .seq
        };

        std::fs::write(
            rules_dir.join("interrupt.toml"),
            r#"
[[rule]]
id = 'catch-up-interrupt-refusal'
on = { event = 'turn.completed' }
do = [{ action = 'interrupt', target = '${event.agent}', expected_generation = '${event.generation}', expected_turn_id = 'wrong-turn' }]
"#,
        )
        .unwrap();
        crate::automation::initialize_global_rules(&router, &root, &SecretPolicy::default())
            .await
            .unwrap();
        let state = router
            .inner
            .store
            .lock()
            .await
            .get_rule_state("owner", "catch-up-interrupt-refusal")
            .unwrap()
            .unwrap();
        assert!(state.current_cursor >= event_seq);

        let request = json!({
            "id":ulid::Ulid::new().to_string(),
            "rule":"catch-up-interrupt-refusal",
            "after_seq":event_seq - 1,
            "through_seq":event_seq,
        });
        let first: Command = serde_json::from_value(
            router
                .rule_catch_up(&request, &Principal::Owner)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(first.verb, "rule.catch_up");
        assert_eq!(first.state, CommandState::Applied);
        assert_eq!(first.outcome.as_ref().unwrap()["children"], json!([]));

        let events_after_first = router
            .inner
            .store
            .lock()
            .await
            .read_events(0, 1_000, None)
            .unwrap();
        let rejection_markers = events_after_first
            .iter()
            .filter(|event| {
                event.kind == "rule.rejected"
                    && event.data["parent_command"] == first.id
                    && event.data["event_seq"] == event_seq
            })
            .collect::<Vec<_>>();
        assert_eq!(rejection_markers.len(), 1);
        assert_eq!(rejection_markers[0].data["code"], "invalid");
        assert!(!events_after_first.iter().any(|event| {
            event.kind == "rule.fired"
                && event.data["parent_command"] == first.id
                && event.data["event_seq"] == event_seq
        }));

        let replay: Command = serde_json::from_value(
            router
                .rule_catch_up(&request, &Principal::Owner)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(replay, first);
        assert_eq!(
            router
                .inner
                .store
                .lock()
                .await
                .read_events(0, 1_000, None)
                .unwrap(),
            events_after_first
        );

        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn project_rule_catch_up_marks_only_stale_generation_and_replays_valid_child_once() {
        const PROJECT_ID: &str = "stale-catch-up-project";
        const RULE_ID: &str = "stale-catch-up-rule";
        const NOT_READY_RULE_ID: &str = "stale-catch-up-not-ready-workspace";

        let root = std::env::temp_dir().join(format!(
            "eliot-project-rule-stale-catch-up-{}",
            ulid::Ulid::new()
        ));
        let checkout_path = root.join("checkout");
        let data_root = root.join("owner-data");
        std::fs::create_dir_all(&checkout_path).unwrap();
        std::fs::create_dir_all(&data_root).unwrap();
        let git_init = std::process::Command::new("git")
            .args(["init", "--quiet"])
            .current_dir(&checkout_path)
            .status()
            .unwrap();
        assert!(git_init.success(), "initialize registered checkout");

        let checkout_input = checkout_path.to_string_lossy().into_owned();
        let (checkout, repo_common_dir) = crate::workspace_git::WorkspaceGit::new()
            .unwrap()
            .repository_identity(&checkout_input)
            .await
            .unwrap();
        std::fs::write(
            data_root.join("config.toml"),
            format!(
                concat!(
                    "[profiles.codex]\n",
                    "harness = \"codex\"\n",
                    "model = \"gpt-6-luna\"\n\n",
                    "[projects.{PROJECT_ID}]\n",
                    "path = {checkout:?}\n",
                    "profiles = [\"codex\"]\n",
                    "actions = [\"stop\"]\n",
                ),
                PROJECT_ID = PROJECT_ID,
                checkout = checkout
            ),
        )
        .unwrap();

        let project_rules_dir = checkout_path.join(".eliot").join("rules");
        std::fs::create_dir_all(&project_rules_dir).unwrap();
        std::fs::write(
            project_rules_dir.join("catch-up.toml"),
            format!(
                concat!(
                    "[[rule]]\n",
                    "id = \"{RULE_ID}\"\n",
                    "on = {{ event = \"session.ready\" }}\n",
                    "do = [{{ action = \"stop\", target = \"${{event.agent}}\", expected_generation = \"${{event.generation}}\" }}]\n\n",
                    "[[rule]]\n",
                    "id = \"{NOT_READY_RULE_ID}\"\n",
                    "on = {{ event = \"command.accepted\" }}\n",
                    "do = [{{ action = \"stop\", target = \"${{event.agent}}\", expected_generation = \"${{event.generation}}\" }}]\n",
                ),
                RULE_ID = RULE_ID,
                NOT_READY_RULE_ID = NOT_READY_RULE_ID,
            ),
        )
        .unwrap();

        let router = router_with_data_root(data_root.clone());

        let (agent, stale_event_seq, valid_event_seq) = {
            let mut store = router.inner.store.lock().await;
            let launch_id = ulid::Ulid::new().to_string();
            let launch = store
                .submit(
                    "owner",
                    SubmitCommand {
                        id: launch_id.clone(),
                        target: None,
                        verb: "launch".to_owned(),
                        args: json!({
                            "project":"stale-catch-up-fixture",
                            "workspace":checkout,
                            "harness":"codex",
                            "profile":"codex",
                            "profile_snapshot":{},
                        }),
                        expected_generation: Some(0),
                        expected_turn_id: None,
                        request_id: None,
                        probe: false,
                    },
                    now_ms(),
                )
                .unwrap();
            let agent = launch.command.target.unwrap();
            store.dispatch(&launch_id, now_ms()).unwrap();
            read_fixture_event(
                &mut store,
                &agent,
                "session.ready",
                json!({
                    "command_id":launch_id,
                    "native_scope":"stale-catch-up-generation-1",
                    "native_ref":"stale-catch-up-thread-1",
                }),
            );
            store
                .settle(&launch_id, CommandState::Applied, None, now_ms())
                .unwrap();
            let stale_event_seq = store
                .read_events(0, 1_000, None)
                .unwrap()
                .into_iter()
                .find(|event| {
                    event.kind == "session.ready"
                        && event.agent.as_deref() == Some(agent.as_str())
                        && event.generation == Some(1)
                })
                .expect("generation-1 readiness event")
                .seq;

            read_fixture_event(&mut store, &agent, "session.ended", json!({}));
            let resume_id = ulid::Ulid::new().to_string();
            let mut resume = SubmitCommand::new(resume_id.clone(), "resume");
            resume.target = Some(agent.clone());
            resume.expected_generation = Some(1);
            store.submit("owner", resume, now_ms()).unwrap();
            store.dispatch(&resume_id, now_ms()).unwrap();

            let batch_id = ulid::Ulid::new().to_string();
            store
                .put_batch(
                    eliot_core::SourceBatch {
                        batch_id: batch_id.clone(),
                        agent_id: agent.clone(),
                        generation: 2,
                        source: "stream".to_owned(),
                        events: vec![eliot_core::NativeEvent {
                            ts: None,
                            kind: "session.ready".to_owned(),
                            native_id: batch_id,
                            data: json!({
                                "command_id":resume_id.clone(),
                                "native_scope":"stale-catch-up-generation-1",
                                "native_ref":"stale-catch-up-thread-1",
                            }),
                        }],
                        cursor: None,
                    },
                    now_ms(),
                )
                .unwrap();
            store
                .settle(&resume_id, CommandState::Applied, None, now_ms())
                .unwrap();
            let valid_event_seq = store
                .read_events(0, 1_000, None)
                .unwrap()
                .into_iter()
                .find(|event| {
                    event.kind == "session.ready"
                        && event.agent.as_deref() == Some(agent.as_str())
                        && event.generation == Some(2)
                })
                .expect("generation-2 readiness event")
                .seq;
            (agent, stale_event_seq, valid_event_seq)
        };

        // Seed the later stale-generation event before rule initialization so the
        // captured rule cursor covers both explicit catch-up boundaries. Keep the
        // next managed workspace absent until after the first valid child is checked.
        let (pending_agent, pending_event_seq, pending_workspace_input) = {
            let mut store = router.inner.store.lock().await;
            let launch_id = ulid::Ulid::new().to_string();
            let initial_branch = format!(
                "codex/stale-catch-up-initial-{}",
                ulid::Ulid::new().to_string().to_ascii_lowercase()
            );
            let initial_workspace = eliot_core::WorkspacePreparationInput {
                project: checkout.clone(),
                repo_path: checkout.clone(),
                repo_common_dir: repo_common_dir.clone(),
                requested_branch: initial_branch.clone(),
                base_ref: Some("HEAD".to_owned()),
                base_oid: "d".repeat(40),
                worktree_path: root
                    .join("pending-ready-worktree")
                    .to_string_lossy()
                    .into_owned(),
                frozen_setup: Vec::new(),
            };
            let launch = store
                .submit_with_workspace(
                    "owner",
                    SubmitCommand {
                        id: launch_id.clone(),
                        target: None,
                        verb: "launch".to_owned(),
                        args: json!({
                            "project":checkout,
                            "workspace":checkout,
                            "worktree":initial_branch,
                            "harness":"codex",
                            "profile":"codex",
                            "profile_snapshot":{},
                        }),
                        expected_generation: Some(0),
                        expected_turn_id: None,
                        request_id: None,
                        probe: false,
                    },
                    initial_workspace.clone(),
                    now_ms(),
                )
                .unwrap();
            let pending_agent = launch.command.target.unwrap();
            let pending_event_seq = store
                .read_events(0, 1_000, None)
                .unwrap()
                .into_iter()
                .find(|event| {
                    event.kind == "command.accepted"
                        && event.agent.as_deref() == Some(pending_agent.as_str())
                        && event.generation == Some(0)
                        && event.data["command_id"] == launch_id
                })
                .expect("generation-0 launch acceptance event")
                .seq;

            store
                .settle_workspace_creation(
                    &launch_id,
                    1,
                    eliot_core::WorkspaceCreationOutcome::Verified {
                        repo_path: initial_workspace.repo_path.clone(),
                        repo_common_dir: initial_workspace.repo_common_dir.clone(),
                        worktree_path: initial_workspace.worktree_path.clone(),
                        branch: initial_workspace.requested_branch.clone(),
                        head: initial_workspace.base_oid.clone(),
                    },
                    now_ms(),
                )
                .unwrap();
            store.dispatch(&launch_id, now_ms()).unwrap();
            read_fixture_event(
                &mut store,
                &pending_agent,
                "session.ready",
                json!({
                    "command_id":launch_id,
                    "native_scope":"stale-catch-up-pending-generation-1",
                    "native_ref":"stale-catch-up-pending-thread-1",
                }),
            );
            store
                .settle(&launch_id, CommandState::Applied, None, now_ms())
                .unwrap();
            read_fixture_event(&mut store, &pending_agent, "session.ended", json!({}));
            (pending_agent, pending_event_seq, initial_workspace)
        };

        let project_principal = format!("project:{PROJECT_ID}");
        let policy = SecretPolicy::default();
        crate::automation::initialize_global_rules(&router, &data_root, &policy)
            .await
            .unwrap();
        let namespaces = crate::automation::initialize_project_rules(&router, &data_root, &policy)
            .await
            .unwrap();
        assert_eq!(namespaces.len(), 1);
        assert!(
            router
                .rule_definition(&project_principal, RULE_ID)
                .is_some()
        );
        assert!(
            router
                .rule_definition(&project_principal, NOT_READY_RULE_ID)
                .is_some()
        );

        let (registration, state, not_ready_state) = {
            let store = router.inner.store.lock().await;
            let registration = store
                .get_project_registration("owner", PROJECT_ID)
                .unwrap()
                .expect("owner-configured project registration readback");
            let state = store
                .get_rule_state(&project_principal, RULE_ID)
                .unwrap()
                .expect("project rule state readback");
            let not_ready_state = store
                .get_rule_state(&project_principal, NOT_READY_RULE_ID)
                .unwrap()
                .expect("not-ready project rule state readback");
            (registration, state, not_ready_state)
        };
        assert_eq!(registration.id, PROJECT_ID);
        assert_eq!(registration.checkout, checkout);
        assert_eq!(registration.repo_common_dir, repo_common_dir);
        assert!(state.current_cursor >= valid_event_seq);
        assert!(state.current_cursor >= pending_event_seq);
        assert!(not_ready_state.current_cursor >= pending_event_seq);

        let request = json!({
            "id":ulid::Ulid::new().to_string(),
            "rule":RULE_ID,
            "project":PROJECT_ID,
            "after_seq":stale_event_seq - 1,
            "through_seq":valid_event_seq,
        });
        let first: Command = serde_json::from_value(
            router
                .rule_catch_up(&request, &Principal::Owner)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(first.state, CommandState::Applied);
        let child_ids = first.outcome.as_ref().unwrap()["children"]
            .as_array()
            .unwrap();
        assert_eq!(child_ids.len(), 1);
        let child_id = child_ids[0].as_str().unwrap().to_owned();
        let child = router
            .inner
            .store
            .lock()
            .await
            .get_command(&child_id)
            .unwrap()
            .unwrap();
        assert_eq!(child.verb, "stop");
        assert_eq!(child.target.as_deref(), Some(agent.as_str()));
        assert_eq!(child.generation, Some(2));

        let events_after_first = router
            .inner
            .store
            .lock()
            .await
            .read_events(0, 1_000, None)
            .unwrap();
        let stale_markers = events_after_first
            .iter()
            .filter(|event| {
                event.kind == "rule.rejected"
                    && event.data["parent_command"] == first.id
                    && event.data["event_seq"] == stale_event_seq
            })
            .collect::<Vec<_>>();
        assert_eq!(stale_markers.len(), 1);
        assert_eq!(stale_markers[0].data["code"], "stale_generation");
        let valid_firings = events_after_first
            .iter()
            .filter(|event| {
                event.kind == "rule.fired"
                    && event.data["parent_command"] == first.id
                    && event.data["event_seq"] == valid_event_seq
            })
            .collect::<Vec<_>>();
        assert_eq!(valid_firings.len(), 1);

        let replay: Command = serde_json::from_value(
            router
                .rule_catch_up(&request, &Principal::Owner)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(replay, first);
        let events_after_replay = router
            .inner
            .store
            .lock()
            .await
            .read_events(0, 1_000, None)
            .unwrap();
        assert_eq!(
            events_after_replay
                .iter()
                .filter(|event| {
                    event.kind == "rule.rejected"
                        && event.data["parent_command"] == first.id
                        && event.data["event_seq"] == stale_event_seq
                })
                .count(),
            1
        );
        assert_eq!(
            events_after_replay
                .iter()
                .filter(|event| {
                    event.kind == "rule.fired"
                        && event.data["parent_command"] == first.id
                        && event.data["event_seq"] == valid_event_seq
                })
                .count(),
            1
        );

        {
            let mut store = router.inner.store.lock().await;
            // Leave the next managed generation in Creating. Catch-up for the old
            // launch event must stop at the generation mismatch before reading it.
            let resume_id = ulid::Ulid::new().to_string();
            let mut resume = SubmitCommand::new(resume_id, "resume");
            resume.target = Some(pending_agent.clone());
            resume.expected_generation = Some(1);
            store
                .submit_with_workspace("owner", resume, pending_workspace_input.clone(), now_ms())
                .unwrap();
            let workspace = store
                .get_workspace(&pending_agent)
                .unwrap()
                .expect("pending generation-2 workspace");
            assert_eq!(workspace.target_generation, 2);
            assert_eq!(workspace.phase, eliot_core::WorkspacePhase::Creating);
            assert_eq!(
                store.get_agent(&pending_agent).unwrap().unwrap().generation,
                1
            );
        }

        let not_ready_request = json!({
            "id":ulid::Ulid::new().to_string(),
            "rule":NOT_READY_RULE_ID,
            "project":PROJECT_ID,
            "after_seq":pending_event_seq - 1,
            "through_seq":pending_event_seq,
        });
        let not_ready_parent: Command = serde_json::from_value(
            router
                .rule_catch_up(&not_ready_request, &Principal::Owner)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(not_ready_parent.state, CommandState::Applied);
        assert_eq!(
            not_ready_parent.outcome.as_ref().unwrap()["children"],
            json!([])
        );
        assert_eq!(
            router
                .inner
                .store
                .lock()
                .await
                .read_events(0, 1_000, None)
                .unwrap()
                .iter()
                .filter(|event| {
                    event.kind == "rule.rejected"
                        && event.data["parent_command"] == not_ready_parent.id
                        && event.data["event_seq"] == pending_event_seq
                        && event.data["code"] == "stale_generation"
                })
                .count(),
            1
        );

        let mut progress = router.progress_receiver();
        loop {
            let command = router
                .inner
                .store
                .lock()
                .await
                .get_command(&child_id)
                .unwrap()
                .expect("admitted child command remains retained");
            if matches!(
                command.state,
                CommandState::Applied
                    | CommandState::Rejected
                    | CommandState::Failed
                    | CommandState::Unknown
                    | CommandState::Cancelled
            ) {
                break;
            }
            progress
                .changed()
                .await
                .expect("router remains available until child settlement");
        }
        drop(router);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn invalid_initial_timer_slot_leaves_rules_unseeded_and_same_id_registers_after_fix() {
        let root =
            std::env::temp_dir().join(format!("eliot-timer-preflight-{}", ulid::Ulid::new()));
        let rules_dir = root.join("rules");
        std::fs::create_dir_all(&rules_dir).unwrap();
        let rule_path = rules_dir.join("timers.toml");
        let router = router_with_data_root(root.clone());
        let invalid = r#"
[[rule]]
id = "preflight-existing"
on = { event = "turn.completed" }
do = [{ action = "stop", target = "a1", expected_generation = 1 }]

[[rule]]
id = "same-timer"
on = { every = "1s", anchor = 9223372036854775807 }
do = [{ action = "stop", target = "a1", expected_generation = 1 }]
"#;
        std::fs::write(&rule_path, invalid).unwrap();
        let high_water = router.inner.store.lock().await.high_water().unwrap();
        let policy = SecretPolicy::default();

        let error = match crate::automation::initialize_global_rules(&router, &root, &policy).await
        {
            Err(error) => error,
            Ok(_) => panic!("overflowing first timer slot should fail preflight"),
        };
        assert_eq!(error.code, "invalid");
        {
            let store = router.inner.store.lock().await;
            assert!(
                store
                    .get_rule_state("owner", "preflight-existing")
                    .unwrap()
                    .is_none()
            );
            assert!(
                store
                    .get_rule_state("owner", "same-timer")
                    .unwrap()
                    .is_none()
            );
            assert!(
                store
                    .get_timer_state("owner", "same-timer")
                    .unwrap()
                    .is_none()
            );
            assert_eq!(store.high_water().unwrap(), high_water);
        }

        let corrected = invalid.replace(
            "on = { every = \"1s\", anchor = 9223372036854775807 }",
            "on = { every = \"1s\" }",
        );
        std::fs::write(&rule_path, corrected).unwrap();
        let (initialized, _) = crate::automation::initialize_global_rules(&router, &root, &policy)
            .await
            .unwrap();
        assert_eq!(initialized.len(), 2);
        let store = router.inner.store.lock().await;
        assert!(
            store
                .get_rule_state("owner", "same-timer")
                .unwrap()
                .is_some()
        );
        let timer = store
            .get_timer_state("owner", "same-timer")
            .unwrap()
            .unwrap();
        assert_eq!(timer.next_due_at, Some(timer.anchor + 1_000));
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[cfg(windows)]
    #[test]
    fn retained_launch_hydration_aligns_existing_8dot3_workspace_alias() {
        let root = std::env::temp_dir().join(format!("eliot-retained-alias-{}", ulid::Ulid::new()));
        let workspace = root.join("workspace with spaces");
        std::fs::create_dir_all(&workspace).unwrap();
        // cmd's %~sI asks Windows for the existing path's 8.3 spelling.
        let short_path = std::process::Command::new("cmd.exe")
            .args(["/d", "/c"])
            .arg(format!(
                "for %I in (\"{}\") do @echo %~sI",
                workspace.display()
            ))
            .output();
        let Ok(short_path) = short_path else {
            std::fs::remove_dir_all(&root).unwrap();
            return;
        };
        if !short_path.status.success() {
            std::fs::remove_dir_all(&root).unwrap();
            return;
        }
        let requested = String::from_utf8_lossy(&short_path.stdout)
            .trim()
            .to_owned();
        if requested.is_empty()
            || !requested
                .split(['\\', '/'])
                .any(|component| component.contains('~'))
        {
            std::fs::remove_dir_all(&root).unwrap();
            return;
        }
        if !std::path::Path::new(&requested).is_dir() {
            std::fs::remove_dir_all(&root).unwrap();
            return;
        }
        let stored = std::fs::canonicalize(&workspace)
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();

        if same_retained_workspace(&requested, &stored) {
            std::fs::remove_dir_all(&root).unwrap();
            return; // This Windows volume did not expose a distinct short spelling.
        }

        let mut request = SubmitCommand::new(ulid::Ulid::new().to_string(), "launch");
        request.args = json!({"workspace":requested});
        let retained_workspace = Value::String(stored.clone());
        align_retained_workspace_alias(&mut request, Some(&retained_workspace));

        assert_eq!(request.args["workspace"], stored);
        assert!(same_retained_workspace(
            request.args["workspace"].as_str().unwrap(),
            &stored
        ));
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[tokio::test]
    async fn unready_frozen_route_rejects_before_profile_store_and_dispatch() {
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buffer = [0_u8; 512];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let read = stream.read(&mut buffer).await.unwrap();
                if read == 0 || request.len().saturating_add(read) > 8 * 1024 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
            }
            let body = b"{\"status\":\"starting\"}";
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(headers.as_bytes()).await.unwrap();
            stream.write_all(body).await.unwrap();
            String::from_utf8_lossy(&request).to_ascii_lowercase()
        });

        let router = router();
        let id = ulid::Ulid::new().to_string();
        let mut request = SubmitCommand::new(&id, "launch");
        request.args = json!({
            "project":"route-preflight",
            "workspace":"synthetic-workspace",
            "profile":"codex-opencodex",
            "harness":"codex",
            "profile_snapshot":{
                "harness":"codex",
                "pass_env":[],
                "pass_env_values":{"CODEX_HOME":null,"CODEX_SQLITE_HOME":null},
                "route_binding":{
                    "name":"opencodex",
                    "endpoint":endpoint,
                    "readiness_path":"/readyz",
                    "provider_base_url":format!("{endpoint}/v1"),
                    "client_key_env_name":null
                },
                "capacity_scopes":[
                    {"scope":"profile:codex-opencodex","max_live":1},
                    {"scope":"proxy:opencodex","max_live":1}
                ]
            }
        });
        {
            let mut store = router.inner.store.lock().await;
            assert!(
                store
                    .submit("owner", request.clone(), now_ms())
                    .unwrap()
                    .is_new
            );
        }

        router.clone().process_command_inner(id.clone(), None).await;
        let request_line = tokio::time::timeout(std::time::Duration::from_secs(3), server)
            .await
            .expect("readiness GET deadline")
            .unwrap();
        assert!(request_line.starts_with("get /readyz http/1.1\r\n"));

        let store = router.inner.store.lock().await;
        let rejected = store.get_command(&id).unwrap().unwrap();
        assert_eq!(rejected.state, CommandState::Rejected);
        assert_eq!(rejected.outcome.as_ref().unwrap()["reason"], "unavailable");
        let failed = store
            .get_agent(rejected.target.as_deref().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(failed.generation, 0);
        assert_eq!(failed.state, AgentState::Failed);
        assert_eq!(router.inner.plugin_host.live_count(), 0);
        assert!(
            store
                .read_events(0, 1000, None)
                .unwrap()
                .iter()
                .all(|event| event.kind != "command.dispatched")
        );
        drop(store);

        let status = router.status().await.unwrap();
        assert_eq!(status["unsettled_commands_count"], 0);
        assert!(router.status_blockers().await.unwrap().is_empty());

        request.id = ulid::Ulid::new().to_string();
        request.args["project"] = json!("route-preflight-capacity-release");
        request.args["workspace"] = json!("synthetic-workspace-after-reject");
        let second_id = request.id.clone();
        assert!(
            router
                .inner
                .store
                .lock()
                .await
                .submit("owner", request, now_ms())
                .unwrap()
                .is_new
        );
        router.reject_undispatched(&second_id, "test_cleanup").await;
        let drained = router
            .shutdown(
                &json!({"id":ulid::Ulid::new().to_string(), "mode":"drain"}),
                &Principal::Owner,
            )
            .await
            .unwrap();
        assert_eq!(drained["state"], "applied");
        assert_eq!(drained["blockers"], json!([]));
    }

    #[tokio::test]
    async fn native_store_binding_dispatch_keeps_caller_payload_and_settles_validation_failure() {
        let router = router();
        let mut request = SubmitCommand::new(ulid::Ulid::new().to_string(), "launch");
        request.args = json!({"project":"binding-test", "workspace":"synthetic-workspace", "profile":"fixture", "harness":"codex",
            "profile_snapshot":{"harness":"codex", "pass_env":[], "pass_env_values":{"CODEX_HOME":null,"CODEX_SQLITE_HOME":null}}});
        {
            let mut store = router.inner.store.lock().await;
            store.submit("owner", request.clone(), now_ms()).unwrap();
        }
        let expected = router
            .accepted_launch_agent(&request.id)
            .await
            .unwrap()
            .unwrap();
        let directory = std::env::temp_dir().to_str().unwrap().to_owned();
        let binding = NativeStoreBinding {
            home: directory.clone(),
            root: directory,
            native_scope: "verified-test-scope".to_owned(),
        };
        {
            let mut store = router.inner.store.lock().await;
            let change = store
                .dispatch_with_native_store_binding(&request.id, now_ms(), &expected, &binding)
                .unwrap();
            router.publish_locked(&change.events);
            assert_eq!(change.value.state, CommandState::Dispatched);
            assert_eq!(change.value.args, request.args);
            let agent = store.get_agent(&expected.id).unwrap().unwrap();
            assert_eq!(
                agent.profile_snapshot["native_store_binding"],
                serde_json::to_value(&binding).unwrap()
            );
            assert!(
                !store
                    .submit("owner", request.clone(), now_ms())
                    .unwrap()
                    .is_new
            );
        }
        assert!(
            router
                .accepted_launch_agent(&request.id)
                .await
                .unwrap()
                .is_none()
        );

        let mut invalid = request.clone();
        invalid.id = ulid::Ulid::new().to_string();
        {
            let mut store = router.inner.store.lock().await;
            store.submit("owner", invalid.clone(), now_ms()).unwrap();
        }
        let expected = router
            .accepted_launch_agent(&invalid.id)
            .await
            .unwrap()
            .unwrap();
        let mut oversized = binding;
        oversized
            .root
            .push_str(&"x".repeat(eliot_plugin::MAX_FRAME_BYTES));
        let (error, faulted) = {
            let mut store = router.inner.store.lock().await;
            let error = store
                .dispatch_with_native_store_binding(&invalid.id, now_ms(), &expected, &oversized)
                .unwrap_err();
            assert_eq!(error.code, "frame_too_large");
            assert_eq!(
                store.get_command(&invalid.id).unwrap().unwrap().state,
                CommandState::Accepted
            );
            (error, store.is_write_faulted())
        };
        assert!(!faulted);
        router
            .settle_dispatch_error(&invalid.id, &error, faulted, true, false)
            .await;
        let store = router.inner.store.lock().await;
        let rejected = store.get_command(&invalid.id).unwrap().unwrap();
        assert_eq!(rejected.state, CommandState::Rejected);
        assert_eq!(rejected.args, invalid.args);
        assert!(store.get_agent_token(&expected.id, 0).unwrap().is_none());
        assert!(
            !store
                .read_events(0, 1000, None)
                .unwrap()
                .iter()
                .any(|event| event.kind == "command.dispatched"
                    && event.data["command_id"] == invalid.id)
        );
    }

    #[tokio::test]
    async fn native_store_binding_fake_launch_skips_metadata_resolution() {
        let router = router();
        let mut request = SubmitCommand::new(ulid::Ulid::new().to_string(), "launch");
        request.args = json!({"project":"legacy-fake", "workspace":"missing-synthetic-workspace"});
        {
            let mut store = router.inner.store.lock().await;
            store.submit("owner", request.clone(), now_ms()).unwrap();
        }
        router
            .clone()
            .process_command_inner(request.id.clone(), None)
            .await;
        let store = router.inner.store.lock().await;
        let command = store.get_command(&request.id).unwrap().unwrap();
        // Fake reaches its existing start validation; no metadata process is
        // spawned for a synthetic/nonexistent workspace or legacy snapshot.
        assert_eq!(command.state, CommandState::Unknown);
        assert_eq!(command.outcome.unwrap()["code"], "invalid");
        assert!(
            store
                .read_events(0, 1000, None)
                .unwrap()
                .iter()
                .any(|event| event.kind == "command.dispatched"
                    && event.data["command_id"] == request.id)
        );
        let agent = store
            .get_agent(command.target.as_deref().unwrap())
            .unwrap()
            .unwrap();
        assert!(agent.profile_snapshot.get("native_store_binding").is_none());
    }

    #[tokio::test]
    async fn native_store_inputs_retained_hydration_survives_store_reopen() {
        let root = std::env::temp_dir().join(format!("eliot-store-inputs-{}", ulid::Ulid::new()));
        std::fs::create_dir(&root).unwrap();
        let db = root.join("state.sqlite");
        let original_cwd = root.join("original-parent").join("workspace");
        let replacement_cwd = root.join("replacement-parent").join("workspace");
        let mut snapshot = json!({"harness":"codex", "pass_env":["CODEX_HOME", "CODEX_SQLITE_HOME", "EPHEMERAL_TOKEN"]});
        let credential = "retained-test-credential";
        crate::config::capture_native_store_inputs_with_lookups(
            &mut snapshot,
            |name| match name {
                "CODEX_HOME" => Some("../original-home".into()),
                "CODEX_SQLITE_HOME" => None,
                _ => panic!("unexpected selector lookup: {name}"),
            },
            |name| match name {
                "EPHEMERAL_TOKEN" => Some(credential.into()),
                _ => panic!("unexpected credential lookup: {name}"),
            },
        )
        .unwrap();
        assert_eq!(
            snapshot["pass_env_values"],
            json!({"CODEX_HOME":"../original-home", "CODEX_SQLITE_HOME":null})
        );
        assert!(!snapshot.to_string().contains(credential));
        let mut replacement = snapshot.clone();
        crate::config::capture_native_store_inputs_with_lookups(
            &mut replacement,
            |name| Some(replacement_cwd.join(name).into_os_string()),
            |name| match name {
                "EPHEMERAL_TOKEN" => Some(credential.into()),
                _ => panic!("unexpected credential lookup: {name}"),
            },
        )
        .unwrap();
        assert_eq!(replacement["pass_env_values"].as_object().unwrap().len(), 2);
        assert!(!replacement.to_string().contains(credential));
        assert_ne!(replacement["pass_env_values"], snapshot["pass_env_values"]);
        let launch_id = ulid::Ulid::new().to_string();
        let attach_id = ulid::Ulid::new().to_string();
        let legacy_id = ulid::Ulid::new().to_string();
        let mut launch = SubmitCommand::new(&launch_id, "launch");
        launch.args = json!({"project":original_cwd.to_str().unwrap(), "workspace":original_cwd.to_str().unwrap(),
            "profile":"missing-current-config", "harness":"codex", "profile_snapshot":snapshot});
        let mut attach = SubmitCommand::new(&attach_id, "attach");
        attach.args = json!({"profile":"missing-current-config", "harness":"codex", "mode":"observe",
            "native_scope":"retained-scope", "native_ref":"retained-reference", "profile_snapshot":snapshot});
        let mut legacy = launch.clone();
        legacy.id = legacy_id;
        legacy.args["profile_snapshot"]
            .as_object_mut()
            .unwrap()
            .remove("pass_env_values");
        // Supply a typed metadata result at the Store boundary; no native probe
        // is run by this persistence/hydration regression.
        let binding = NativeStoreBinding {
            home: root.to_str().unwrap().to_owned(),
            root: root.to_str().unwrap().to_owned(),
            native_scope: "retained-launch-store".to_owned(),
        };
        let launch_agent = {
            let mut store = eliot_store::Store::open(&db).unwrap();
            for request in [&launch, &attach, &legacy] {
                assert!(
                    store
                        .submit("owner", request.clone(), now_ms())
                        .unwrap()
                        .is_new
                );
            }
            let target = store
                .get_command(&launch_id)
                .unwrap()
                .unwrap()
                .target
                .unwrap();
            let expected = store.get_agent(&target).unwrap().unwrap();
            store
                .dispatch_with_native_store_binding(&launch_id, now_ms(), &expected, &binding)
                .unwrap();
            target
        };
        let store = eliot_store::Store::open(&db).unwrap();
        let router = Router::new(
            root.join("no-current-config"),
            "owner-credential".to_owned(),
            Arc::new(AsyncMutex::new(store)),
            Bus::new(16),
        );
        let before = router.inner.store.lock().await.high_water().unwrap();
        for original in [&launch, &legacy] {
            let mut retry = original.clone();
            retry.args.as_object_mut().unwrap().remove("harness");
            retry
                .args
                .as_object_mut()
                .unwrap()
                .remove("profile_snapshot");
            // A retained ID must return before current config or selector capture.
            assert!(router.freeze_launch_profile(&mut retry).await.unwrap());
            assert_eq!(retry.args, original.args);
            assert!(
                retry.args["profile_snapshot"]
                    .get("native_store_binding")
                    .is_none()
            );
            let mut store = router.inner.store.lock().await;
            assert!(!store.submit("owner", retry, now_ms()).unwrap().is_new);
        }
        let mut retry = attach.clone();
        retry
            .args
            .as_object_mut()
            .unwrap()
            .remove("profile_snapshot");
        assert!(router.hydrate_retained_attach(&mut retry).await.unwrap());
        assert_eq!(retry.args, attach.args);
        {
            let mut store = router.inner.store.lock().await;
            assert!(!store.submit("owner", retry, now_ms()).unwrap().is_new);
            assert_eq!(store.high_water().unwrap(), before);
            assert_eq!(
                store.get_command(&launch_id).unwrap().unwrap().args,
                launch.args
            );
            assert_eq!(
                store.get_command(&launch_id).unwrap().unwrap().args["profile_snapshot"],
                snapshot
            );
            let mut derived_snapshot = snapshot.clone();
            derived_snapshot["native_store_binding"] = serde_json::to_value(&binding).unwrap();
            assert_eq!(
                store
                    .get_agent(&launch_agent)
                    .unwrap()
                    .unwrap()
                    .profile_snapshot,
                derived_snapshot
            );
            assert!(
                store.get_command(&legacy.id).unwrap().unwrap().args["profile_snapshot"]
                    .get("pass_env_values")
                    .is_none()
            );
        }
        assert_eq!(
            snapshot["pass_env_values"]["CODEX_HOME"],
            "../original-home"
        );
        assert!(
            std::path::Path::new(snapshot["pass_env_values"]["CODEX_HOME"].as_str().unwrap())
                .is_relative()
        );
        drop(router);
        std::fs::remove_file(db).unwrap();
        std::fs::remove_dir(root).unwrap();
    }

    #[test]
    fn native_store_inputs_augmented_snapshot_uses_existing_admission_bound() {
        let mut snapshot = json!({"harness":"codex", "pass_env":["CODEX_HOME"]});
        crate::config::capture_native_store_inputs(&mut snapshot, |_| {
            Some("\u{1}".repeat(eliot_plugin::MAX_FRAME_BYTES / 6 + 1).into())
        })
        .unwrap();
        let mut request = SubmitCommand::new(ulid::Ulid::new().to_string(), "launch");
        request.args = json!({"project":"bounded", "workspace":"synthetic-workspace", "harness":"codex",
            "profile":"fixture", "profile_snapshot":snapshot});
        let mut store = eliot_store::Store::open(std::path::Path::new(":memory:")).unwrap();
        assert_eq!(
            store.submit("owner", request, now_ms()).unwrap_err().code,
            "frame_too_large"
        );
        assert_eq!(store.high_water().unwrap(), 0);
        assert_eq!(store.agent_count().unwrap(), 0);
    }

    fn admit(store: &mut eliot_store::Store) -> Vec<Event> {
        store
            .submit(
                "owner",
                SubmitCommand {
                    id: ulid::Ulid::new().to_string(),
                    target: None,
                    verb: "launch".to_owned(),
                    args: json!({"project":"typed-events", "workspace":"synthetic-workspace"}),
                    expected_generation: Some(0),
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                },
                now_ms(),
            )
            .unwrap()
            .events
    }

    #[tokio::test]
    async fn typed_mutation_submit_retains_unknown_without_new_dispatch() {
        let router = router();
        let outcome = json!({"code":"unknown", "opaque":{"nested":[false, null, "retained"]},
            "diagnostics":[{"source":"plugin", "snapshot":{"text":"safe diagnostic", "truncated":false, "eof":true, "read_error":false}}]});
        let (agent, request, retained, before_events) = {
            let mut store = router.inner.store.lock().await;
            let (agent, _) = result_fixture(&mut store);
            let request = SubmitCommand {
                id: ulid::Ulid::new().to_string().to_ascii_lowercase(),
                target: Some(agent.clone()),
                verb: "interrupt".to_owned(),
                args: json!({}),
                expected_generation: Some(1),
                expected_turn_id: Some("turn-1".to_owned()),
                request_id: None,
                probe: false,
            };
            let principal = format!("agent:{agent}:1");
            store.submit(&principal, request.clone(), now_ms()).unwrap();
            store.dispatch(&request.id, now_ms()).unwrap();
            store
                .settle(
                    &request.id,
                    CommandState::Unknown,
                    Some(outcome.clone()),
                    now_ms(),
                )
                .unwrap();
            let duplicate = store.submit(&principal, request.clone(), now_ms()).unwrap();
            assert!(!duplicate.is_new);
            assert!(duplicate.events.is_empty());
            let before_events = store.read_events(0, 1000, None).unwrap();
            (agent, request, duplicate.command, before_events)
        };
        let principal = Principal::Agent {
            id: agent.clone(),
            generation: 1,
        };
        let mut wire = serde_json::to_value(&request).unwrap();
        // These values must be hydrated from the managed caller before Store id binding.
        wire.as_object_mut().unwrap().remove("target");
        wire.as_object_mut().unwrap().remove("expected_generation");
        let progress = router.progress_receiver();
        let before_progress = *progress.borrow();
        let first = router.submit(&wire, &principal).await.unwrap();
        assert_eq!(first.as_object().unwrap().len(), 6);
        let typed: CommandSubmitResponse = serde_json::from_value(first.clone()).unwrap();
        assert_eq!(typed.command_id, request.id);
        assert_eq!(typed.agent.as_deref(), Some(agent.as_str()));
        assert_eq!(typed.generation, Some(1));
        assert_eq!(typed.state, CommandState::Unknown);
        assert_eq!(typed.outcome, Some(outcome));
        assert_eq!(typed.linked_command, None);
        assert_eq!(router.submit(&wire, &principal).await.unwrap(), first);
        tokio::task::yield_now().await;
        // Each retained submit signals once; no processing actor signals another dispatch.
        assert_eq!(*progress.borrow(), before_progress.wrapping_add(2));
        {
            let store = router.inner.store.lock().await;
            assert_eq!(store.get_command(&request.id).unwrap().unwrap(), retained);
            assert_eq!(store.read_events(0, 1000, None).unwrap(), before_events);
        }
        wire["target"] = json!("a999");
        assert_eq!(
            router.submit(&wire, &principal).await.unwrap_err().code,
            "not_allowed"
        );
    }

    #[tokio::test]
    async fn drain_worker_releases_ordered_successor_after_predecessor_applies() {
        let data_root =
            std::env::temp_dir().join(format!("eliot-drain-worker-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&data_root).unwrap();
        let router = router_with_data_root(data_root.clone());
        let store = Arc::clone(&router.inner.store);

        let (first_child_id, second_child_id, admitted_events, first_dispatch_events) = {
            let mut store = store.lock().await;
            let create_agent_fixture =
                |store: &mut eliot_store::Store, native_scope: &str, native_ref: &str| {
                    let launch_id = ulid::Ulid::new().to_string();
                    let agent = store
                        .submit(
                            "owner",
                            SubmitCommand {
                                id: launch_id.clone(),
                                target: None,
                                verb: "launch".to_owned(),
                                args: json!({
                                    "project":"typed-results", "workspace":"synthetic-workspace",
                                    "harness":"codex", "profile":"synthetic", "profile_snapshot":{},
                                }),
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
                    store.dispatch(&launch_id, now_ms()).unwrap();
                    read_fixture_event(
                        store,
                        &agent,
                        "session.ready",
                        json!({"native_scope":native_scope, "native_ref":native_ref}),
                    );
                    store
                        .settle(&launch_id, CommandState::Applied, None, now_ms())
                        .unwrap();

                    let send_id = ulid::Ulid::new().to_string();
                    store
                        .submit(
                            "owner",
                            SubmitCommand {
                                id: send_id.clone(),
                                target: Some(agent.clone()),
                                verb: "send".to_owned(),
                                args: json!({"text":"fixture input"}),
                                expected_generation: Some(1),
                                expected_turn_id: None,
                                request_id: None,
                                probe: false,
                            },
                            now_ms(),
                        )
                        .unwrap();
                    store.dispatch(&send_id, now_ms()).unwrap();
                    read_fixture_event(
                        store,
                        &agent,
                        "turn.started",
                        json!({"command_id":send_id, "turn_id":"turn-1"}),
                    );
                    (agent, send_id)
                };
            let (first_agent, first_send) =
                create_agent_fixture(&mut store, "drain-first-scope", "drain-first-ref");
            let (second_agent, second_send) =
                create_agent_fixture(&mut store, "drain-second-scope", "drain-second-ref");
            for (agent, send_id) in [(&first_agent, &first_send), (&second_agent, &second_send)] {
                read_fixture_event(
                    &mut store,
                    agent,
                    "turn.completed",
                    json!({"command_id":send_id, "turn_id":"turn-1", "status":"completed"}),
                );
                if store.get_command(send_id).unwrap().unwrap().state == CommandState::Dispatched {
                    store
                        .settle(
                            send_id,
                            CommandState::Applied,
                            Some(json!({"turn_id":"turn-1"})),
                            now_ms(),
                        )
                        .unwrap();
                }
            }

            let history = store.read_events(0, 1_000, None).unwrap();
            let first_event = history
                .iter()
                .find(|event| {
                    event.kind == "turn.completed"
                        && event.agent.as_deref() == Some(first_agent.as_str())
                })
                .unwrap()
                .seq;
            let second_event = history
                .iter()
                .find(|event| {
                    event.kind == "turn.completed"
                        && event.agent.as_deref() == Some(second_agent.as_str())
                })
                .unwrap()
                .seq;
            let definition_value = json!({"id":"drain-order", "enabled":true});
            let definition = eliot_store::RuleDefinitionInput {
                id: "drain-order".to_owned(),
                canonical_definition: serde_json::to_string(&definition_value).unwrap(),
                definition_digest: eliot_core::canonical_json_sha256(&definition_value).unwrap(),
                enabled: true,
            };
            let state = store
                .ensure_rule_state("owner", &definition, now_ms())
                .unwrap();
            let request_id = ulid::Ulid::new();
            let now = i64::try_from(request_id.timestamp_ms()).unwrap() + 1;
            let request = RuleCatchUpRequest::new(
                request_id.to_string(),
                "drain-order",
                first_event - 1,
                Some(state.current_cursor),
            );
            let actions = [
                RuleCatchUpAction::command(
                    first_event,
                    SubmitCommand {
                        id: ulid::Ulid::new().to_string(),
                        target: Some(first_agent.clone()),
                        verb: "stop".to_owned(),
                        args: json!({}),
                        expected_generation: Some(1),
                        expected_turn_id: None,
                        request_id: None,
                        probe: false,
                    },
                ),
                RuleCatchUpAction::command(
                    second_event,
                    SubmitCommand {
                        id: ulid::Ulid::new().to_string(),
                        target: Some(second_agent.clone()),
                        verb: "stop".to_owned(),
                        args: json!({}),
                        expected_generation: Some(1),
                        expected_turn_id: None,
                        request_id: None,
                        probe: false,
                    },
                ),
            ];
            let admission = store
                .admit_rule_catch_up(
                    "owner",
                    &request,
                    &state,
                    state.current_cursor,
                    &actions,
                    now,
                )
                .unwrap();
            assert_eq!(admission.children.len(), 2);
            assert_ne!(
                admission.children[0].command.target,
                admission.children[1].command.target
            );
            let first_child_id = admission.children[0].command.id.clone();
            let second_child_id = admission.children[1].command.id.clone();
            let first_dispatch = store.dispatch(&first_child_id, now + 1).unwrap();
            assert_eq!(first_dispatch.value.state, CommandState::Dispatched);
            (
                first_child_id,
                second_child_id,
                admission.events,
                first_dispatch.events,
            )
        };
        router.note_events(&admitted_events);
        router.note_events(&first_dispatch_events);

        let (worker_shutdown, worker_shutdown_rx) = tokio::sync::watch::channel(false);
        let mut worker = tokio::spawn(crate::automation::run_global_rules(
            router.clone(),
            crate::automation::RuleWorkerStart {
                global_rules: Vec::new(),
                project_rules: Vec::new(),
                max_event_backlog: 100_000,
                data_root: data_root.clone(),
                policy: Arc::new(SecretPolicy::default()),
                initial_notices: crate::automation::ReloadNoticeContext::default(),
            },
            worker_shutdown_rx,
        ));
        tokio::task::yield_now().await;
        let mut worker_events = router.inner.bus.subscribe();

        let scenario: Result<_> = async {
            let drain = router
                .shutdown(
                    &json!({"id":ulid::Ulid::new().to_string(), "mode":"drain"}),
                    &Principal::Owner,
                )
                .await?;
            router.response_finished(true);
            let firing_closed = !router.rule_firings_open();
            let initial_blockers = router.status_blockers().await?;
            let initial_first_blocker = initial_blockers.iter().any(|blocker| match blocker {
                StatusBlocker::CommandUnsettled { command_id, .. }
                | StatusBlocker::CommandUnknown { command_id } => command_id == &first_child_id,
                _ => false,
            });
            let initial_second_blocker = initial_blockers.iter().any(|blocker| match blocker {
                StatusBlocker::CommandUnsettled { command_id, .. }
                | StatusBlocker::CommandUnknown { command_id } => command_id == &second_child_id,
                _ => false,
            });

            let first_applied = router
                .settle(
                    &first_child_id,
                    CommandState::Applied,
                    Some(json!({"state":"stopped"})),
                )
                .await
                .ok_or_else(|| Error::new("internal", "predecessor settlement was not retained"))?;
            let dispatched_event = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    match worker_events.recv().await {
                        Ok(event)
                            if event.kind == "command.dispatched"
                                && event.data.get("command_id").and_then(Value::as_str)
                                    == Some(second_child_id.as_str()) =>
                        {
                            break Ok(event);
                        }
                        Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {}
                        Err(broadcast::error::RecvError::Closed) => {
                            break Err(Error::new(
                                "unavailable",
                                "router event bus closed before successor dispatch",
                            ));
                        }
                    }
                }
            })
            .await
            .map_err(|_| {
                Error::new(
                    "timeout",
                    "rule worker did not dispatch the stored successor",
                )
            })??;
            let second_before_settle = {
                let store = store.lock().await;
                store
                    .get_command(&second_child_id)?
                    .ok_or_else(|| Error::new("not_found", "successor command disappeared"))?
                    .state
            };
            let blockers_before_second_settle = router.status_blockers().await?;
            let second_blocker_before_settle =
                blockers_before_second_settle
                    .iter()
                    .any(|blocker| match blocker {
                        StatusBlocker::CommandUnsettled { command_id, .. }
                        | StatusBlocker::CommandUnknown { command_id } => {
                            command_id == &second_child_id
                        }
                        _ => false,
                    });
            let first_blocker_after_applied =
                blockers_before_second_settle
                    .iter()
                    .any(|blocker| match blocker {
                        StatusBlocker::CommandUnsettled { command_id, .. }
                        | StatusBlocker::CommandUnknown { command_id } => {
                            command_id == &first_child_id
                        }
                        _ => false,
                    });
            let should_exit_before_second_settle = router.should_exit().await?;

            let second_applied = router
                .settle(
                    &second_child_id,
                    CommandState::Applied,
                    Some(json!({"state":"stopped"})),
                )
                .await
                .ok_or_else(|| Error::new("internal", "successor settlement was not retained"))?;
            let blockers_after_second_settle = router.status_blockers().await?;
            let second_blocker_after_settle =
                blockers_after_second_settle
                    .iter()
                    .any(|blocker| match blocker {
                        StatusBlocker::CommandUnsettled { command_id, .. }
                        | StatusBlocker::CommandUnknown { command_id } => {
                            command_id == &second_child_id
                        }
                        _ => false,
                    });
            Ok::<_, Error>((
                drain,
                firing_closed,
                initial_first_blocker,
                initial_second_blocker,
                first_applied.state,
                dispatched_event,
                second_before_settle,
                second_blocker_before_settle,
                first_blocker_after_applied,
                should_exit_before_second_settle,
                second_applied.state,
                second_blocker_after_settle,
            ))
        }
        .await;

        let _ = worker_shutdown.send(true);
        let worker_join = match tokio::time::timeout(Duration::from_secs(5), &mut worker).await {
            Ok(result) => result,
            Err(_) => {
                worker.abort();
                worker.await
            }
        };
        let worker_result = worker_join;
        std::fs::remove_dir_all(&data_root).unwrap();
        assert!(
            worker_result.is_ok(),
            "global rule worker failed: {worker_result:?}"
        );
        let worker_result = worker_result.unwrap();
        assert!(
            worker_result.is_ok(),
            "global rule worker failed: {worker_result:?}"
        );
        let (
            drain,
            firing_closed,
            initial_first_blocker,
            initial_second_blocker,
            first_state,
            dispatched_event,
            second_before_settle,
            second_blocker_before_settle,
            first_blocker_after_applied,
            should_exit_before_second_settle,
            second_state,
            second_blocker_after_settle,
        ) = scenario.expect("drain worker ordering scenario completed");
        assert_eq!(drain["state"], json!("dispatched"));
        assert!(firing_closed);
        assert!(initial_first_blocker);
        assert!(initial_second_blocker);
        assert_eq!(first_state, CommandState::Applied);
        assert_eq!(dispatched_event.data["command_id"], second_child_id);
        assert!(matches!(
            second_before_settle,
            CommandState::Dispatched | CommandState::Unknown
        ));
        assert!(second_blocker_before_settle);
        assert!(!first_blocker_after_applied);
        assert!(!should_exit_before_second_settle);
        assert_eq!(second_state, CommandState::Applied);
        assert!(!second_blocker_after_settle);
    }

    #[tokio::test]
    async fn typed_mutation_shutdown_returns_first_retained_drain_reply() {
        let router = router();
        let id = ulid::Ulid::new().to_string().to_ascii_lowercase();
        let request = json!({"id":id, "mode":"drain"});
        let first = router.shutdown(&request, &Principal::Owner).await.unwrap();
        assert_eq!(
            first,
            json!({"state":"applied", "mode":"drain", "blockers":[]})
        );
        {
            let mut store = router.inner.store.lock().await;
            admit(&mut store);
        }
        assert!(!router.status_blockers().await.unwrap().is_empty());
        router.inner.shutdown_ready.store(true, Ordering::SeqCst);
        let retained = router.shutdown(&request, &Principal::Owner).await.unwrap();
        assert_eq!(retained, first);
        assert_eq!(router.inner.mode.load(Ordering::SeqCst), MODE_DRAIN);
        assert!(!router.inner.shutdown_ready.load(Ordering::SeqCst));
        assert_eq!(
            router
                .shutdown(&json!({"id":id, "mode":"force"}), &Principal::Owner)
                .await
                .unwrap_err()
                .code,
            "id_conflict"
        );
        assert_eq!(router.inner.mode.load(Ordering::SeqCst), MODE_DRAIN);
    }

    #[tokio::test]
    async fn typed_mutation_retained_drain_cannot_downgrade_force() {
        let router = router();
        let agent = {
            let mut store = router.inner.store.lock().await;
            admit(&mut store)[0].agent.clone().unwrap()
        };
        let progress = router.progress_receiver();
        let drain = json!({"id":ulid::Ulid::new().to_string(), "mode":"drain"});
        let first = router.shutdown(&drain, &Principal::Owner).await.unwrap();
        router.response_finished(true);
        assert!(router.inner.shutdown_ready.load(Ordering::SeqCst));

        let force = router
            .shutdown(
                &json!({"id":ulid::Ulid::new().to_string(), "mode":"force"}),
                &Principal::Owner,
            )
            .await
            .unwrap();
        assert_eq!(force, json!({"state":"dispatched", "mode":"force"}));
        assert!(router.is_force_requested());
        assert!(!router.inner.shutdown_ready.load(Ordering::SeqCst));
        router.response_finished(true);
        let before_retry = *progress.borrow();

        assert_eq!(
            router.shutdown(&drain, &Principal::Owner).await.unwrap(),
            first
        );
        assert_eq!(router.inner.mode.load(Ordering::SeqCst), MODE_FORCE);
        assert_eq!(router.mode_name(), "forcing");
        assert!(!router.inner.shutdown_ready.load(Ordering::SeqCst));
        assert_eq!(*progress.borrow(), before_retry.wrapping_add(1));

        let stop_id = ulid::Ulid::new().to_string();
        let before_events = router
            .inner
            .store
            .lock()
            .await
            .read_events(0, 1000, None)
            .unwrap();
        let before_submit = *progress.borrow();
        let error = router.submit(
            &json!({"id":stop_id, "target":agent, "verb":"stop", "args":{}, "expected_generation":0}),
            &Principal::Owner,
        ).await.unwrap_err();
        assert_eq!(error.code, "unavailable");
        {
            let store = router.inner.store.lock().await;
            assert!(store.get_command(&stop_id).unwrap().is_none());
            assert_eq!(store.read_events(0, 1000, None).unwrap(), before_events);
        }
        assert_eq!(*progress.borrow(), before_submit);
        router.response_finished(true);
        assert!(router.inner.shutdown_ready.load(Ordering::SeqCst));
        assert_eq!(*progress.borrow(), before_submit.wrapping_add(1));
        assert!(router.should_exit().await.unwrap());
    }

    #[tokio::test]
    async fn typed_mutation_bad_retained_shutdown_receipt_does_not_withdraw_admission() {
        let router = router();
        let id = ulid::Ulid::new().to_string();
        router
            .inner
            .store
            .lock()
            .await
            .record_write(
                &id,
                "owner",
                "daemon.shutdown",
                &json!({"mode":"drain"}),
                &json!({"private-receipt-detail":"not a shutdown response"}),
                now_ms(),
            )
            .unwrap();
        let error = router
            .shutdown(&json!({"id":id, "mode":"drain"}), &Principal::Owner)
            .await
            .unwrap_err();
        assert_eq!(error.code, "internal");
        assert_eq!(error.message, "retained shutdown result is invalid");
        assert!(
            !serde_json::to_string(&error)
                .unwrap()
                .contains("private-receipt-detail")
        );
        assert_eq!(router.inner.mode.load(Ordering::SeqCst), MODE_DRAIN);
        assert!(!router.inner.shutdown_ready.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn typed_mutation_grammar_keeps_owner_first_and_downstream_validation() {
        let router = router();
        let principal = Principal::Agent {
            id: "a1".to_owned(),
            generation: 1,
        };
        assert_eq!(
            router
                .shutdown(&Value::Null, &principal)
                .await
                .unwrap_err()
                .code,
            "not_allowed"
        );
        for method in ["command.submit", "daemon.shutdown"] {
            let payload = if method == "command.submit" {
                json!({"id":ulid::Ulid::new().to_string(), "verb":"unknown", "extra":0})
            } else {
                json!({"id":ulid::Ulid::new().to_string(), "mode":"force", "extra":0})
            };
            assert_eq!(router.handle(method, json!({
                "_auth":{"kind":"owner", "credential":"owner-credential"}, "_payload":payload
            })).await.unwrap_err().code, "invalid");
        }
        // Grammar does not preempt the managed-principal policy with an args check.
        assert_eq!(
            router
                .submit(
                    &json!({"id":"literal-id", "verb":"launch", "args":null}),
                    &principal
                )
                .await
                .unwrap_err()
                .code,
            "not_allowed"
        );
        // The unchanged Store boundary rejects supplied null args rather than defaulting them.
        let error = router
            .submit(
                &json!({"id":ulid::Ulid::new().to_string(), "verb":"unknown", "args":null}),
                &Principal::Owner,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, "invalid");
        assert_eq!(error.message, "command args must be a JSON object");
        assert_eq!(
            router
                .submit(
                    &json!({"id":ulid::Ulid::new().to_string(), "verb":"unknown"}),
                    &Principal::Owner
                )
                .await
                .unwrap_err()
                .code,
            "unsupported"
        );
    }

    #[tokio::test]
    async fn provider_hold_rpcs_are_owner_only_and_probe_uses_send_capability() {
        let router = router();
        let agent = Principal::Agent {
            id: "a1".to_owned(),
            generation: 1,
        };
        assert_eq!(
            router
                .provider_holds(&Value::Null, &agent)
                .await
                .unwrap_err()
                .code,
            "not_allowed"
        );
        assert_eq!(
            router
                .provider_clear_hold(&Value::Null, &agent)
                .await
                .unwrap_err()
                .code,
            "not_allowed"
        );
        router
            .inner
            .secrets
            .lock_recover()
            .insert("router-test-known-secret".to_owned());
        assert_eq!(
            router
                .provider_clear_hold(
                    &json!({"router-test-known-secret":"unknown field"}),
                    &Principal::Owner,
                )
                .await
                .unwrap_err()
                .code,
            "secret_in_payload"
        );
        assert_eq!(
            router
                .provider_clear_hold(
                    &json!({"id":"clear-id", "scope":"profile:fixture",
                        "corrective_evidence":"owner_verified",
                        "expected_probe_command":"router-test-known-secret"}),
                    &Principal::Owner,
                )
                .await
                .unwrap_err()
                .code,
            "secret_in_payload"
        );

        let invalid_probe_id = ulid::Ulid::new().to_string();
        assert_eq!(
            router
                .submit(
                    &json!({"id":invalid_probe_id.clone(), "verb":"launch", "args":{}, "probe":true}),
                    &Principal::Owner,
                )
                .await
                .unwrap_err()
                .code,
            "unsupported"
        );

        let own_probe_id = ulid::Ulid::new().to_string();
        let mut own_probe = CommandSubmitRequest::new(own_probe_id.clone(), "send");
        own_probe.probe = true;
        authorize_command(&agent, &mut own_probe).unwrap();
        assert_eq!(own_probe.target.as_deref(), Some("a1"));
        assert_eq!(own_probe.expected_generation, Some(1));

        let unrelated_probe_id = ulid::Ulid::new().to_string();
        assert_eq!(
            router
                .submit(
                    &json!({"id":unrelated_probe_id.clone(), "target":"a2", "verb":"send",
                        "args":{"text":"probe"}, "expected_generation":1, "probe":true}),
                    &agent,
                )
                .await
                .unwrap_err()
                .code,
            "not_allowed"
        );
        let stale_probe_id = ulid::Ulid::new().to_string();
        assert_eq!(
            router
                .submit(
                    &json!({"id":stale_probe_id.clone(), "target":"a1", "verb":"send",
                        "args":{"text":"probe"}, "expected_generation":2, "probe":true}),
                    &agent,
                )
                .await
                .unwrap_err()
                .code,
            "stale_generation"
        );

        let store = router.inner.store.lock().await;
        assert_eq!(store.high_water().unwrap(), 0);
        assert!(store.get_command(&invalid_probe_id).unwrap().is_none());
        assert!(store.get_command(&own_probe_id).unwrap().is_none());
        assert!(store.get_command(&unrelated_probe_id).unwrap().is_none());
        assert!(store.get_command(&stale_probe_id).unwrap().is_none());
    }

    #[tokio::test]
    async fn local_provider_held_dispatch_failure_rejects_before_native_dispatch() {
        let router = router();
        let id = ulid::Ulid::new().to_string();
        router
            .inner
            .store
            .lock()
            .await
            .submit(
                "owner",
                SubmitCommand {
                    id: id.clone(),
                    target: None,
                    verb: "launch".to_owned(),
                    args: json!({"project":"provider-held", "workspace":"synthetic-workspace"}),
                    expected_generation: Some(0),
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                },
                now_ms(),
            )
            .unwrap();

        router
            .settle_dispatch_error(
                &id,
                &Error::new("provider_held", "provider scope is held"),
                false,
                false,
                false,
            )
            .await;

        let store = router.inner.store.lock().await;
        let command = store.get_command(&id).unwrap().unwrap();
        assert_eq!(command.state, CommandState::Rejected);
        assert_eq!(command.outcome, Some(json!({"reason":"provider_held"})));
        let events = store.read_events(0, 100, None).unwrap();
        assert!(events.iter().any(|event| event.kind == "command.rejected"));
        assert!(
            !events
                .iter()
                .any(|event| event.kind == "command.dispatched")
        );
    }

    #[tokio::test]
    async fn project_run_registration_revocation_rejects_before_dispatch_and_recovery_is_quiet() {
        const PROJECT_ID: &str = "run-revocation";

        let base = std::fs::canonicalize(std::env::temp_dir()).unwrap();
        let root = base.join(format!("eliot-router-run-revocation-{}", ulid::Ulid::new()));
        let checkout_path = root.join("checkout");
        let data_root = root.join("owner-data");
        std::fs::create_dir_all(&checkout_path).unwrap();
        std::fs::create_dir_all(&data_root).unwrap();
        let git_init = std::process::Command::new("git")
            .args(["init", "--quiet"])
            .current_dir(&checkout_path)
            .status()
            .unwrap();
        assert!(git_init.success(), "initialize registered checkout");

        let checkout_input = checkout_path.to_string_lossy().into_owned();
        let (checkout, repo_common_dir) = crate::workspace_git::WorkspaceGit::new()
            .unwrap()
            .repository_identity(&checkout_input)
            .await
            .unwrap();
        let tools_dir = root.join("tools");
        std::fs::create_dir_all(&tools_dir).unwrap();
        let interpreter_path = tools_dir.join(if cfg!(windows) { "pwsh.exe" } else { "pwsh" });
        std::fs::write(&interpreter_path, b"fixture interpreter").unwrap();
        let interpreter_program = std::fs::canonicalize(interpreter_path)
            .unwrap()
            .to_string_lossy()
            .into_owned();
        let write_registration = |actions: &[&str]| {
            let actions = actions
                .iter()
                .map(|action| serde_json::to_string(action).unwrap())
                .collect::<Vec<_>>()
                .join(", ");
            let source = format!(
                concat!(
                    "[projects.{project_id}]\n",
                    "path = {checkout}\n",
                    "actions = [{actions}]\n\n",
                    "[projects.{project_id}.interpreters.fixture]\n",
                    "program = {interpreter}\n",
                    "argv_prefix = []\n",
                ),
                project_id = PROJECT_ID,
                checkout = serde_json::to_string(&checkout).unwrap(),
                actions = actions,
                interpreter = serde_json::to_string(&interpreter_program).unwrap(),
            );
            std::fs::write(data_root.join("config.toml"), source).unwrap();
        };
        write_registration(&["run"]);

        let scripts_dir = std::path::Path::new(&checkout)
            .join(".eliot")
            .join("scripts");
        std::fs::create_dir_all(&scripts_dir).unwrap();
        std::fs::write(scripts_dir.join("not-executed.sh"), "exit 0\n").unwrap();

        let router = router_with_data_root(data_root.clone());
        let registration_input = ProjectRegistrationInput {
            id: PROJECT_ID.to_owned(),
            checkout: checkout.clone(),
            repo_common_dir,
            profiles: Vec::new(),
            actions: vec!["run".to_owned()],
            interpreters: std::collections::BTreeMap::from([(
                "fixture".to_owned(),
                eliot_core::api::ScriptInterpreter {
                    program: interpreter_program.clone(),
                    argv_prefix: Vec::new(),
                },
            )]),
            run_env: Vec::new(),
        };
        let registration = router
            .reconcile_project_registrations(std::slice::from_ref(&registration_input))
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();

        let project_principal = format!("project:{PROJECT_ID}");
        let rule_id = "revocation-rule";
        let rule_value = json!({
            "id":rule_id,
            "enabled":true,
            "on":{"event":"turn.completed"},
            "do":[{"action":"run", "cmd":["fixture", ".eliot/scripts/not-executed.sh"], "timeout":"5s"}],
        });
        let rule_input = eliot_store::RuleDefinitionInput {
            id: rule_id.to_owned(),
            canonical_definition: serde_json::to_string(&rule_value).unwrap(),
            definition_digest: eliot_core::canonical_json_sha256(&rule_value).unwrap(),
            enabled: true,
        };
        let (agent, trigger_id) = {
            let mut store = router.inner.store.lock().await;
            result_fixture(&mut store)
        };
        let rule_state = router
            .initialize_rules(
                &project_principal,
                std::slice::from_ref(&rule_input),
                &[],
                now_ms(),
            )
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
            .view;

        let event = {
            let mut store = router.inner.store.lock().await;
            read_fixture_event(
                &mut store,
                &agent,
                "turn.completed",
                json!({
                    "command_id":trigger_id,
                    "turn_id":"turn-1",
                    "status":"completed",
                }),
            );
            if store
                .get_command(&trigger_id)
                .unwrap()
                .is_some_and(|command| command.state == CommandState::Dispatched)
            {
                store
                    .settle(
                        &trigger_id,
                        CommandState::Applied,
                        Some(json!({"turn_id":"turn-1"})),
                        now_ms(),
                    )
                    .unwrap();
            }
            store
                .read_events(0, 1_000, None)
                .unwrap()
                .into_iter()
                .find(|event| {
                    event.kind == "turn.completed"
                        && event.data.get("command_id").and_then(Value::as_str)
                            == Some(trigger_id.as_str())
                })
                .unwrap()
        };

        let frozen = crate::scripts::freeze_project_run(
            eliot_core::RunDefinitionArgs {
                cmd: vec![
                    "fixture".to_owned(),
                    ".eliot/scripts/not-executed.sh".to_owned(),
                ],
                cwd: None,
                timeout: Some("5s".to_owned()),
                env: std::collections::BTreeMap::new(),
            },
            &registration,
            eliot_core::FrozenScriptContext::Event {
                event: Box::new(event.clone()),
                subject: event.subject.clone(),
                rule: eliot_core::ScriptRuleContext {
                    id: rule_id.to_owned(),
                    revision: rule_state.revision,
                },
            },
            &SecretPolicy::default(),
        )
        .unwrap();
        let mut request = SubmitCommand::new(String::new(), "run");
        request.args = serde_json::to_value(frozen).unwrap();
        let prepared = router
            .prepare_project_rule_submission(request, &registration)
            .await
            .unwrap();

        // Use Store's real rule-firing transaction without scheduling the
        // child, so this test can commit authority revocation before Router
        // attempts the Run dispatch.
        let run_id = {
            let mut store = router.inner.store.lock().await;
            let firing = store
                .admit_rule_action(
                    &project_principal,
                    &rule_state,
                    event.seq,
                    prepared.request,
                    now_ms(),
                )
                .unwrap();
            assert!(firing.is_new);
            let parent = firing
                .parent
                .as_ref()
                .expect("committed rule firing parent");
            let run_admission = firing
                .child
                .as_ref()
                .expect("committed targetless Run child");
            let run = &run_admission.command;
            assert_eq!(parent.verb, "rule");
            assert_eq!(parent.state, CommandState::Applied);
            assert_eq!(run.verb, "run");
            assert_eq!(run.state, CommandState::Accepted);
            assert_eq!(run.principal, project_principal);
            assert!(firing.events.iter().any(|fired_event| {
                fired_event.kind == "rule.fired"
                    && fired_event.data["event_seq"] == event.seq
                    && fired_event.data["child_command"] == run.id
            }));
            router.publish_locked(&firing.events);
            run.id.clone()
        };

        let mut revoked_registration = registration_input;
        revoked_registration.actions.clear();
        write_registration(&[]);
        let updated = router
            .reconcile_project_registrations(&[revoked_registration])
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        assert!(updated.revision > registration.revision);

        router
            .clone()
            .process_command_inner(run_id.clone(), None)
            .await;
        let (rejected, events_after_rejection) = {
            let store = router.inner.store.lock().await;
            let command = store.get_command(&run_id).unwrap().unwrap();
            let events = store.read_events(0, 1_000, None).unwrap();
            (command, events)
        };
        assert_eq!(rejected.state, CommandState::Rejected);
        assert_eq!(rejected.outcome, Some(json!({"reason":"not_allowed"})));
        assert_eq!(
            events_after_rejection
                .iter()
                .filter(|event| {
                    event.kind == "command.rejected"
                        && event.data.get("command_id").and_then(Value::as_str)
                            == Some(run_id.as_str())
                })
                .count(),
            1
        );
        assert!(!events_after_rejection.iter().any(|event| {
            event.kind == "command.dispatched"
                && event.data.get("command_id").and_then(Value::as_str) == Some(run_id.as_str())
        }));
        assert!(router.inner.run_workers.lock_recover().is_empty());

        router
            .clone()
            .process_command_inner(run_id.clone(), None)
            .await;
        let store = router.inner.store.lock().await;
        assert_eq!(
            store.get_command(&run_id).unwrap().unwrap().state,
            CommandState::Rejected
        );
        assert_eq!(
            store.read_events(0, 1_000, None).unwrap(),
            events_after_rejection,
            "recovery does not retry or rewrite a terminally rejected Run"
        );
        drop(store);
        assert!(router.inner.run_workers.lock_recover().is_empty());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn owner_global_run_grant_revocation_rejects_before_dispatch_and_recovery_is_quiet() {
        const RULE_ID: &str = "owner-run-revocation";

        let base = std::fs::canonicalize(std::env::temp_dir()).unwrap();
        let root = base.join(format!(
            "eliot-router-owner-run-revocation-{}",
            ulid::Ulid::new()
        ));
        let data_root = root.join("owner-data");
        let scripts_dir = data_root.join("scripts");
        let tools_dir = root.join("tools");
        std::fs::create_dir_all(&scripts_dir).unwrap();
        std::fs::create_dir_all(&tools_dir).unwrap();
        std::fs::write(scripts_dir.join("not-executed.sh"), "exit 0\n").unwrap();
        let interpreter_path = tools_dir.join(if cfg!(windows) { "pwsh.exe" } else { "pwsh" });
        std::fs::write(&interpreter_path, b"tiny fixture interpreter").unwrap();
        let interpreter_program = std::fs::canonicalize(interpreter_path)
            .unwrap()
            .to_string_lossy()
            .into_owned();
        let write_global_grant = |interpreter_approved: bool| {
            let source = if interpreter_approved {
                format!(
                    concat!(
                        "[automation.run]\n",
                        "run_env = [\"CI\"]\n\n",
                        "[automation.run.interpreters.fixture]\n",
                        "program = {}\n",
                        "argv_prefix = []\n",
                    ),
                    serde_json::to_string(&interpreter_program).unwrap(),
                )
            } else {
                "[automation.run]\nrun_env = [\"CI\"]\n".to_owned()
            };
            std::fs::write(data_root.join("config.toml"), source).unwrap();
        };
        write_global_grant(true);

        let router = router_with_data_root(data_root.clone());
        let (agent, trigger_id) = {
            let mut store = router.inner.store.lock().await;
            result_fixture(&mut store)
        };
        let run_definition = eliot_core::RunDefinitionArgs {
            cmd: vec!["fixture".to_owned(), "not-executed.sh".to_owned()],
            cwd: Some(data_root.to_string_lossy().into_owned()),
            timeout: Some("5s".to_owned()),
            env: std::collections::BTreeMap::from([(
                "CI".to_owned(),
                "script-approved".to_owned(),
            )]),
        };
        let run_value = serde_json::to_value(&run_definition).unwrap();
        let rule_value = json!({
            "id":RULE_ID,
            "enabled":true,
            "on":{"event":"turn.completed"},
            "do":[{
                "action":"run",
                "cmd":run_value["cmd"].clone(),
                "cwd":run_value["cwd"].clone(),
                "timeout":run_value["timeout"].clone(),
                "env":run_value["env"].clone(),
            }],
        });
        let rule_input = eliot_store::RuleDefinitionInput {
            id: RULE_ID.to_owned(),
            canonical_definition: serde_json::to_string(&rule_value).unwrap(),
            definition_digest: eliot_core::canonical_json_sha256(&rule_value).unwrap(),
            enabled: true,
        };
        let rule_state = router
            .initialize_rules("owner", std::slice::from_ref(&rule_input), &[], now_ms())
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
            .view;

        let event = {
            let mut store = router.inner.store.lock().await;
            read_fixture_event(
                &mut store,
                &agent,
                "turn.completed",
                json!({
                    "command_id":trigger_id,
                    "turn_id":"turn-1",
                    "status":"completed",
                }),
            );
            if store
                .get_command(&trigger_id)
                .unwrap()
                .is_some_and(|command| command.state == CommandState::Dispatched)
            {
                store
                    .settle(
                        &trigger_id,
                        CommandState::Applied,
                        Some(json!({"turn_id":"turn-1"})),
                        now_ms(),
                    )
                    .unwrap();
            }
            store
                .read_events(0, 1_000, None)
                .unwrap()
                .into_iter()
                .find(|event| {
                    event.kind == "turn.completed"
                        && event.data.get("command_id").and_then(Value::as_str)
                            == Some(trigger_id.as_str())
                })
                .unwrap()
        };
        let mut request = SubmitCommand::new(String::new(), "run");
        request.args = run_value;
        let prepared = router
            .prepare_owner_rule_run_submission(
                request,
                FrozenScriptContext::Event {
                    event: Box::new(event.clone()),
                    subject: event.subject.clone(),
                    rule: eliot_core::ScriptRuleContext {
                        id: RULE_ID.to_owned(),
                        revision: rule_state.revision,
                    },
                },
            )
            .await
            .unwrap();
        let run_id = {
            let mut store = router.inner.store.lock().await;
            let firing = store
                .admit_rule_action("owner", &rule_state, event.seq, prepared.request, now_ms())
                .unwrap();
            assert!(firing.is_new);
            let parent = firing.parent.as_ref().expect("committed Owner rule firing");
            let run = &firing
                .child
                .as_ref()
                .expect("committed Owner Run child")
                .command;
            assert_eq!(parent.verb, "rule");
            assert_eq!(parent.state, CommandState::Applied);
            assert_eq!(run.verb, "run");
            assert_eq!(run.state, CommandState::Accepted);
            assert_eq!(run.principal, "owner");
            assert!(firing.events.iter().any(|fired_event| {
                fired_event.kind == "rule.fired"
                    && fired_event.data["event_seq"] == event.seq
                    && fired_event.data["child_command"] == run.id
            }));
            router.publish_locked(&firing.events);
            run.id.clone()
        };

        write_global_grant(false);
        router
            .clone()
            .process_command_inner(run_id.clone(), None)
            .await;
        let (rejected, events_after_rejection) = {
            let store = router.inner.store.lock().await;
            let command = store.get_command(&run_id).unwrap().unwrap();
            let events = store.read_events(0, 1_000, None).unwrap();
            (command, events)
        };
        assert_eq!(rejected.state, CommandState::Rejected);
        assert_eq!(rejected.outcome, Some(json!({"reason":"not_allowed"})));
        assert_eq!(
            events_after_rejection
                .iter()
                .filter(|event| {
                    event.kind == "command.rejected"
                        && event.data.get("command_id").and_then(Value::as_str)
                            == Some(run_id.as_str())
                })
                .count(),
            1
        );
        assert!(!events_after_rejection.iter().any(|event| {
            event.kind == "command.dispatched"
                && event.data.get("command_id").and_then(Value::as_str) == Some(run_id.as_str())
        }));
        assert!(!events_after_rejection.iter().any(|event| {
            event.data.get("parent_command").and_then(Value::as_str) == Some(run_id.as_str())
        }));
        assert!(router.inner.run_workers.lock_recover().is_empty());

        router
            .clone()
            .process_command_inner(run_id.clone(), None)
            .await;
        let store = router.inner.store.lock().await;
        assert_eq!(
            store.get_command(&run_id).unwrap().unwrap().state,
            CommandState::Rejected
        );
        assert_eq!(
            store.read_events(0, 1_000, None).unwrap(),
            events_after_rejection,
            "recovery does not rewrite or retry a terminally rejected Owner Run"
        );
        drop(store);
        assert!(router.inner.run_workers.lock_recover().is_empty());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn stale_pre_dispatch_rejection_preserves_dispatched_winner() {
        let router = router();
        let id = ulid::Ulid::new().to_string();
        {
            let mut store = router.inner.store.lock().await;
            let (agent, _) = result_fixture(&mut store);
            store
                .submit(
                    "owner",
                    SubmitCommand {
                        id: id.clone(),
                        target: Some(agent),
                        verb: "interrupt".to_owned(),
                        args: json!({}),
                        expected_generation: Some(1),
                        expected_turn_id: Some("turn-1".to_owned()),
                        request_id: None,
                        probe: false,
                    },
                    now_ms(),
                )
                .unwrap();
            let dispatched = store.dispatch(&id, now_ms()).unwrap();
            assert_eq!(dispatched.value.state, CommandState::Dispatched);
            router.publish_locked(&dispatched.events);
        }

        // A losing preflight must not overwrite the command after the winning
        // processor has committed its dispatch decision.
        router.reject_undispatched(&id, "preflight_failed").await;
        {
            let store = router.inner.store.lock().await;
            assert_eq!(
                store.get_command(&id).unwrap().unwrap().state,
                CommandState::Dispatched
            );
            assert!(
                !store
                    .read_events(0, 100, None)
                    .unwrap()
                    .iter()
                    .any(|event| {
                        event.kind == "command.rejected"
                            && event.data.get("command_id").and_then(Value::as_str)
                                == Some(id.as_str())
                    })
            );
        }

        let applied = router
            .settle(
                &id,
                CommandState::Applied,
                Some(json!({"command_id":id.clone(), "turn_id":"turn-1"})),
            )
            .await
            .expect("the dispatched winner settles");
        assert_eq!(applied.state, CommandState::Applied);
    }

    #[tokio::test]
    async fn resume_stop_dispatch_race_settlement_cancels_linked_send() {
        let router = router();
        let (agent, stop_id, resume_id, linked_id, dispatch_error) = {
            let mut store = router.inner.store.lock().await;
            let agent = stopped_fake_agent(&mut store, "resume-stop-dispatch-race");

            let stop_id = ulid::Ulid::new().to_string();
            let mut stop = SubmitCommand::new(stop_id.clone(), "stop");
            stop.target = Some(agent.clone());
            stop.expected_generation = Some(1);
            store.submit("owner", stop, now_ms()).unwrap();

            let resume_id = ulid::Ulid::new().to_string();
            let mut resume = SubmitCommand::new(resume_id.clone(), "resume");
            resume.target = Some(agent.clone());
            resume.expected_generation = Some(1);
            resume.args = json!({"text":"resume after the stop gate"});
            let admission = store.submit("owner", resume, now_ms()).unwrap();
            let linked = admission
                .linked_command
                .expect("Resume text is persisted as linked SEND");
            let linked_id = linked.id;
            assert_eq!(admission.command.state, CommandState::Accepted);
            assert_eq!(linked.state, CommandState::Queued);

            assert_eq!(
                store.dispatch(&stop_id, now_ms()).unwrap().value.state,
                CommandState::Dispatched
            );
            let dispatch_error = store.dispatch(&resume_id, now_ms()).unwrap_err();
            assert_eq!(dispatch_error.code, "bad_state");
            (agent, stop_id, resume_id, linked_id, dispatch_error)
        };

        router
            .settle_dispatch_error(&resume_id, &dispatch_error, false, false, false)
            .await;

        let store = router.inner.store.lock().await;
        assert_eq!(
            store.get_command(&resume_id).unwrap().unwrap().state,
            CommandState::Rejected
        );
        assert_eq!(
            store.get_command(&linked_id).unwrap().unwrap().state,
            CommandState::Cancelled
        );
        assert!(
            store
                .read_events(0, 1_000, None)
                .unwrap()
                .iter()
                .any(|event| {
                    event.kind == "command.cancelled"
                        && event.data.get("command_id").and_then(Value::as_str)
                            == Some(linked_id.as_str())
                })
        );
        assert_eq!(store.get_agent(&agent).unwrap().unwrap().generation, 1);
        assert_eq!(
            store.get_command(&stop_id).unwrap().unwrap().state,
            CommandState::Dispatched
        );
    }

    #[tokio::test]
    async fn managed_resume_rejection_restores_ready_workspace_and_cancels_linked_send() {
        let router = router();
        let (agent, launch_id, resume_id, linked_id, previous_workspace) = {
            let mut store = router.inner.store.lock().await;
            let root = std::env::temp_dir().canonicalize().unwrap().join(format!(
                "eliot-managed-resume-rejection-{}",
                ulid::Ulid::new()
            ));
            let checkout = root.join("checkout").to_string_lossy().into_owned();
            let worktree = root.join("worktree").to_string_lossy().into_owned();
            let branch = format!("codex/resume-rejection-{}", ulid::Ulid::new());
            let base_oid = "d".repeat(40);
            let input = WorkspacePreparationInput {
                project: checkout.clone(),
                repo_path: checkout.clone(),
                repo_common_dir: root.join("checkout/.git").to_string_lossy().into_owned(),
                requested_branch: branch.clone(),
                base_ref: Some("HEAD".to_owned()),
                base_oid: base_oid.clone(),
                worktree_path: worktree.clone(),
                frozen_setup: Vec::new(),
            };

            let launch_id = ulid::Ulid::new().to_string();
            let mut launch = SubmitCommand::new(launch_id.clone(), "launch");
            launch.expected_generation = Some(0);
            launch.args = json!({
                "project":checkout,
                "workspace":worktree,
                "worktree":branch,
                "harness":"fake",
                "profile":"fake",
                "profile_snapshot":{"harness":"fake"},
            });
            let accepted = store
                .submit_with_workspace("owner", launch, input.clone(), now_ms())
                .unwrap();
            let agent = accepted.command.target.unwrap();
            store
                .settle_workspace_creation(
                    &launch_id,
                    1,
                    eliot_core::WorkspaceCreationOutcome::Verified {
                        repo_path: input.repo_path.clone(),
                        repo_common_dir: input.repo_common_dir.clone(),
                        worktree_path: input.worktree_path.clone(),
                        branch: input.requested_branch.clone(),
                        head: base_oid.clone(),
                    },
                    now_ms(),
                )
                .unwrap();
            store.dispatch(&launch_id, now_ms()).unwrap();
            read_fixture_event(
                &mut store,
                &agent,
                "session.ready",
                json!({
                    "command_id":launch_id,
                    "native_scope":format!("fake:synthetic:scope:{agent}"),
                    "native_ref":format!("fake:synthetic:session:{agent}"),
                }),
            );
            store
                .settle(&launch_id, CommandState::Applied, None, now_ms())
                .unwrap();
            read_fixture_event(&mut store, &agent, "session.ended", json!({}));
            let previous_workspace = store
                .get_workspace(&agent)
                .unwrap()
                .expect("retained managed workspace");

            let resume_id = ulid::Ulid::new().to_string();
            let mut resume = SubmitCommand::new(resume_id.clone(), "resume");
            resume.target = Some(agent.clone());
            resume.expected_generation = Some(1);
            resume.args = json!({"text":"new input only"});
            let admission = store.submit("owner", resume, now_ms()).unwrap();
            let linked_id = admission
                .linked_command
                .expect("Resume text is a linked SEND")
                .id;

            // Exercise the already-prepared marker boundary: routing must use its
            // exact command and generation ownership even after it reaches Ready.
            store
                .settle_workspace_creation(
                    &resume_id,
                    2,
                    eliot_core::WorkspaceCreationOutcome::Verified {
                        repo_path: input.repo_path.clone(),
                        repo_common_dir: input.repo_common_dir.clone(),
                        worktree_path: input.worktree_path.clone(),
                        branch: input.requested_branch.clone(),
                        head: base_oid,
                    },
                    now_ms(),
                )
                .unwrap();
            let ready = store
                .get_workspace(&agent)
                .unwrap()
                .expect("prepared next-generation marker");
            assert_eq!(ready.phase, WorkspacePhase::Ready);
            assert_eq!(ready.start_command_id, resume_id);
            assert_eq!(ready.target_generation, 2);
            (agent, launch_id, resume_id, linked_id, previous_workspace)
        };

        router.reject_undispatched(&resume_id, "not_allowed").await;

        let store = router.inner.store.lock().await;
        assert_eq!(
            store.get_command(&resume_id).unwrap().unwrap().state,
            CommandState::Rejected
        );
        assert_eq!(
            store.get_command(&linked_id).unwrap().unwrap().state,
            CommandState::Cancelled
        );
        let restored = store
            .get_workspace(&agent)
            .unwrap()
            .expect("prior managed workspace restored");
        assert_eq!(restored.start_command_id, launch_id);
        assert_eq!(restored.target_generation, 1);
        assert_eq!(restored.phase, WorkspacePhase::Ready);
        assert_eq!(restored.setup_index, previous_workspace.setup_index);
        assert_eq!(restored.outcome, previous_workspace.outcome);
        assert!(
            store
                .read_events(0, 1_000, None)
                .unwrap()
                .iter()
                .any(|event| {
                    event.kind == "command.cancelled"
                        && event.data.get("command_id").and_then(Value::as_str)
                            == Some(linked_id.as_str())
                })
        );
    }

    #[tokio::test]
    async fn store_write_fault_overlays_unknown_only_for_uncertain_dispatch() {
        let router = router();
        let accepted_id = ulid::Ulid::new().to_string();
        let (dispatched_id, queued_id, terminal_id) = {
            let mut store = router.inner.store.lock().await;
            let mut launch = SubmitCommand::new(accepted_id.clone(), "launch");
            launch.args = json!({
                "project":"fault-accepted",
                "workspace":"workspace-fault-accepted",
            });
            launch.expected_generation = Some(0);
            store.submit("owner", launch, now_ms()).unwrap();
            let (agent, _) = result_fixture(&mut store);
            let dispatched_id = ulid::Ulid::new().to_string();
            store
                .submit(
                    "owner",
                    SubmitCommand {
                        id: dispatched_id.clone(),
                        target: Some(agent.clone()),
                        verb: "interrupt".to_owned(),
                        args: json!({}),
                        expected_generation: Some(1),
                        expected_turn_id: Some("turn-1".to_owned()),
                        request_id: None,
                        probe: false,
                    },
                    now_ms(),
                )
                .unwrap();
            let dispatched = store.dispatch(&dispatched_id, now_ms()).unwrap();
            assert_eq!(dispatched.value.state, CommandState::Dispatched);
            let definition = json!({
                "id":"fault-queued",
                "enabled":true,
                "on":{"event":"turn.completed"},
                "do":[{"action":"stop", "target":"${event.agent}",
                    "expected_generation":"${event.generation}"}],
            });
            let input = eliot_store::RuleDefinitionInput {
                id: "fault-queued".to_owned(),
                canonical_definition: serde_json::to_string(&definition).unwrap(),
                definition_digest: eliot_core::canonical_json_sha256(&definition).unwrap(),
                enabled: true,
            };
            let rule_state = store.ensure_rule_state("owner", &input, now_ms()).unwrap();
            let event_seqs = store
                .read_events(0, 1_000, None)
                .unwrap()
                .into_iter()
                .filter(|event| {
                    event.agent.as_deref() == Some(agent.as_str())
                        && event.generation == Some(1)
                        && matches!(event.kind.as_str(), "session.ready" | "turn.started")
                })
                .map(|event| event.seq)
                .collect::<Vec<_>>();
            assert_eq!(event_seqs.len(), 2);
            let through = rule_state.current_cursor;
            let request: RuleCatchUpRequest = serde_json::from_value(json!({
                "id":ulid::Ulid::new().to_string(),
                "rule":"fault-queued",
                "after_seq":0,
                "through_seq":through,
            }))
            .unwrap();
            let actions = event_seqs
                .into_iter()
                .map(|event_seq| {
                    RuleCatchUpAction::command(
                        event_seq,
                        SubmitCommand {
                            id: ulid::Ulid::new().to_string(),
                            target: Some(agent.clone()),
                            verb: "stop".to_owned(),
                            args: json!({}),
                            expected_generation: Some(1),
                            expected_turn_id: None,
                            request_id: None,
                            probe: false,
                        },
                    )
                })
                .collect::<Vec<_>>();
            let admission = store
                .admit_rule_catch_up("owner", &request, &rule_state, through, &actions, now_ms())
                .unwrap();
            let terminal_id = admission.parent.id.clone();
            assert_eq!(admission.parent.state, CommandState::Applied);
            let first_id = admission.children[0].command.id.clone();
            let queued_id = admission.children[1].command.id.clone();
            let dispatched = store.dispatch(&first_id, now_ms()).unwrap();
            assert_eq!(dispatched.value.state, CommandState::Dispatched);
            let queued = store.dispatch(&queued_id, now_ms()).unwrap();
            assert_eq!(queued.value.state, CommandState::Queued);
            (dispatched_id, queued_id, terminal_id)
        };

        for (id, state) in [
            (accepted_id.as_str(), CommandState::Accepted),
            (terminal_id.as_str(), CommandState::Applied),
            (queued_id.as_str(), CommandState::Queued),
        ] {
            let persisted = router
                .inner
                .store
                .lock()
                .await
                .get_command(id)
                .unwrap()
                .unwrap();
            assert_eq!(persisted.state, state);
            router
                .finish_settlement(
                    id,
                    Err(Error::new("internal", "injected Store write failure")),
                    true,
                    "store_settlement_failed",
                    true,
                )
                .await;
            let dispatch_error = Error::new("internal", "injected Store dispatch failure");
            router
                .settle_dispatch_error(id, &dispatch_error, true, false, false)
                .await;
            let command: Command = serde_json::from_value(
                router
                    .command_get(&json!({"id":id}), &Principal::Owner)
                    .await
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(command.state, state);
        }

        let persisted = router
            .inner
            .store
            .lock()
            .await
            .get_command(&dispatched_id)
            .unwrap()
            .unwrap();
        assert_eq!(persisted.state, CommandState::Dispatched);
        router
            .finish_settlement(
                &dispatched_id,
                Err(Error::new("internal", "injected Store write failure")),
                true,
                "store_settlement_failed",
                true,
            )
            .await;
        let dispatch_error = Error::new("internal", "injected Store dispatch failure");
        router
            .settle_dispatch_error(&dispatched_id, &dispatch_error, true, false, false)
            .await;
        let persisted = router
            .inner
            .store
            .lock()
            .await
            .get_command(&dispatched_id)
            .unwrap()
            .unwrap();
        assert_eq!(persisted.state, CommandState::Dispatched);
        let readback: Command = serde_json::from_value(
            router
                .command_get(&json!({"id":dispatched_id}), &Principal::Owner)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(readback.state, CommandState::Unknown);
        assert!(router.unknown_ids().contains(&dispatched_id));
        assert_eq!(
            router.unknown_cause(&dispatched_id).as_deref(),
            Some("store_dispatch_outcome_unknown")
        );

        let applied = router
            .settle(
                &dispatched_id,
                CommandState::Applied,
                Some(json!({"command_id":dispatched_id.clone(), "turn_id":"turn-1"})),
            )
            .await
            .expect("the exact committed result settles the dispatched command");
        assert_eq!(applied.state, CommandState::Applied);
        let readback: Command = serde_json::from_value(
            router
                .command_get(&json!({"id":dispatched_id}), &Principal::Owner)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(readback.state, CommandState::Applied);
        assert!(!router.unknown_ids().contains(&dispatched_id));
        assert!(router.unknown_cause(&dispatched_id).is_none());
        let blockers = router.status_blockers().await.unwrap();
        assert!(!blockers.iter().any(|blocker| matches!(
            blocker,
            StatusBlocker::CommandUnknown { command_id } if command_id == &dispatched_id
        )));
        assert!(!blockers.iter().any(|blocker| matches!(
            blocker,
            StatusBlocker::CommandUnsettled { command_id, .. } if command_id == &dispatched_id
        )));
    }

    #[tokio::test]
    async fn rule_send_uses_normal_router_preparation_and_dispatch() {
        let router = router();
        let mut committed = router.subscribe_rule_events();
        let rule_id = "send-after-turn";
        let definition = json!({
            "id":rule_id,
            "enabled":true,
            "allow_self":true,
            "on":{"event":"turn.completed"},
            "do":[{"action":"send", "target":"${event.agent}",
                "expected_generation":"${event.generation}", "text":"follow-up"}],
        });
        let input = eliot_store::RuleDefinitionInput {
            id: rule_id.to_owned(),
            canonical_definition: serde_json::to_string(&definition).unwrap(),
            definition_digest: eliot_core::canonical_json_sha256(&definition).unwrap(),
            enabled: true,
        };

        let (agent, state) = {
            let mut store = router.inner.store.lock().await;
            let launch_id = ulid::Ulid::new().to_string();
            let mut launch = SubmitCommand::new(launch_id.clone(), "launch");
            launch.args = json!({
                "project":"rule-send",
                "workspace":"synthetic-rule-send-workspace",
                "harness":"codex",
                "profile":"synthetic",
                "profile_snapshot":{},
            });
            launch.expected_generation = Some(0);
            let agent = store
                .submit("owner", launch, now_ms())
                .unwrap()
                .command
                .target
                .unwrap();
            store.dispatch(&launch_id, now_ms()).unwrap();
            read_fixture_event(
                &mut store,
                &agent,
                "session.ready",
                json!({"native_scope":"rule-send-scope", "native_ref":"rule-send-ref"}),
            );
            store
                .settle(&launch_id, CommandState::Applied, None, now_ms())
                .unwrap();
            let state = store.ensure_rule_state("owner", &input, now_ms()).unwrap();
            (agent, state)
        };

        let (trigger_id, event) = {
            let mut store = router.inner.store.lock().await;
            let trigger_id = ulid::Ulid::new().to_string();
            store
                .submit(
                    "owner",
                    SubmitCommand {
                        id: trigger_id.clone(),
                        target: Some(agent.clone()),
                        verb: "send".to_owned(),
                        args: json!({"text":"source input"}),
                        expected_generation: Some(1),
                        expected_turn_id: None,
                        request_id: None,
                        probe: false,
                    },
                    now_ms(),
                )
                .unwrap();
            store.dispatch(&trigger_id, now_ms()).unwrap();
            read_fixture_event(
                &mut store,
                &agent,
                "turn.started",
                json!({"command_id":trigger_id, "turn_id":"turn-rule-send"}),
            );
            read_fixture_event(
                &mut store,
                &agent,
                "turn.completed",
                json!({
                    "command_id":trigger_id,
                    "turn_id":"turn-rule-send",
                    "status":"completed",
                }),
            );
            if store
                .get_command(&trigger_id)
                .unwrap()
                .is_some_and(|command| command.state == CommandState::Dispatched)
            {
                store
                    .settle(
                        &trigger_id,
                        CommandState::Applied,
                        Some(json!({"turn_id":"turn-rule-send"})),
                        now_ms(),
                    )
                    .unwrap();
            }
            let event = store
                .read_events(0, 1_000, None)
                .unwrap()
                .into_iter()
                .find(|event| {
                    event.kind == "turn.completed"
                        && event.data.get("command_id").and_then(Value::as_str)
                            == Some(trigger_id.as_str())
                })
                .unwrap();
            (trigger_id, event)
        };
        let action = SubmitCommand {
            id: String::new(),
            target: event.agent.clone(),
            verb: "send".to_owned(),
            args: json!({"text":"follow-up"}),
            expected_generation: event.generation,
            expected_turn_id: None,
            request_id: None,
            probe: false,
        };
        assert_eq!(action.verb, "send");
        assert_eq!(action.target.as_deref(), Some(agent.as_str()));
        assert_eq!(action.expected_generation, Some(1));
        assert_eq!(action.args, json!({"text":"follow-up"}));

        let prepared = router.prepare_owner_submission(action).await.unwrap();
        let admission = router
            .admit_rule_action("owner", None, &state, state.revision, event.seq, prepared)
            .await
            .unwrap()
            .expect("the rule SEND is admitted");
        assert!(admission.is_new);
        let child = admission
            .child
            .expect("the admitted firing has one child")
            .command;
        assert_eq!(child.verb, "send");
        assert_eq!(child.target.as_deref(), Some(agent.as_str()));
        assert_eq!(child.generation, Some(1));
        assert_eq!(child.args, json!({"text":"follow-up"}));

        let mut saw_dispatch = false;
        loop {
            let event = committed.recv().await.expect("router event bus stays open");
            if event.data.get("command_id").and_then(Value::as_str) != Some(child.id.as_str()) {
                continue;
            }
            if event.kind == "command.dispatched" {
                saw_dispatch = true;
            }
            if event.kind == "command.unknown" {
                break;
            }
        }
        assert!(saw_dispatch);
        let stored = router
            .inner
            .store
            .lock()
            .await
            .get_command(&child.id)
            .unwrap()
            .unwrap();
        assert_eq!(stored.state, CommandState::Unknown);
        assert_eq!(stored.verb, "send");
        assert_eq!(stored.args, json!({"text":"follow-up"}));
        assert_eq!(
            router
                .inner
                .store
                .lock()
                .await
                .get_command(&trigger_id)
                .unwrap()
                .unwrap()
                .state,
            CommandState::Applied
        );
    }

    #[tokio::test]
    async fn owner_event_rule_steer_admits_the_exact_trigger_turn() {
        let router = router();
        let rule_id = "owner-steer-exact-turn";
        let definition = json!({
            "id":rule_id,
            "enabled":true,
            "on":{"event":"turn.started"},
            "do":[{"action":"steer", "target":"a1",
                "expected_generation":1, "expected_turn_id":"turn-1", "text":"follow-up"}],
        });
        let input = eliot_store::RuleDefinitionInput {
            id: rule_id.to_owned(),
            canonical_definition: serde_json::to_string(&definition).unwrap(),
            definition_digest: eliot_core::canonical_json_sha256(&definition).unwrap(),
            enabled: true,
        };

        let (agent, state, trigger_id) = {
            let mut store = router.inner.store.lock().await;
            let state = store.ensure_rule_state("owner", &input, now_ms()).unwrap();
            let (agent, trigger_id) = result_fixture(&mut store);
            (agent, state, trigger_id)
        };
        let event = {
            let store = router.inner.store.lock().await;
            store
                .read_events(0, 1_000, None)
                .unwrap()
                .into_iter()
                .find(|event| {
                    event.kind == "turn.started"
                        && event.data.get("command_id").and_then(Value::as_str)
                            == Some(trigger_id.as_str())
                })
                .expect("exact started-turn event")
        };
        let turn_id = event.data["turn_id"].as_str().unwrap().to_owned();
        let mut action = SubmitCommand::new(String::new(), "steer");
        action.target = event.agent.clone();
        action.args = json!({"text":"follow-up"});
        action.expected_generation = event.generation;
        action.expected_turn_id = Some(turn_id.clone());

        let prepared = router.prepare_owner_submission(action).await.unwrap();
        let admission = router
            .admit_rule_action("owner", None, &state, state.revision, event.seq, prepared)
            .await
            .unwrap()
            .expect("Owner event STEER is admitted");
        assert!(admission.is_new);
        let parent = admission.parent.expect("retained rule parent");
        let child = admission.child.expect("admitted STEER child").command;
        assert_eq!(child.verb, "steer");
        assert_eq!(child.target.as_deref(), Some(agent.as_str()));
        assert_eq!(child.generation, Some(1));
        assert_eq!(child.args, json!({"text":"follow-up"}));
        assert_eq!(
            parent.outcome.as_ref().unwrap()["children"],
            json!([child.id])
        );
        assert_eq!(
            router
                .inner
                .store
                .lock()
                .await
                .get_command(&child.id)
                .unwrap()
                .unwrap()
                .generation,
            Some(1)
        );
        assert_eq!(turn_id, "turn-1");
    }

    #[tokio::test]
    async fn owner_event_resume_uses_post_stop_ack_and_keeps_unknown_input_queued() {
        let router = router();
        let workspace = std::fs::canonicalize(std::env::temp_dir())
            .unwrap()
            .to_string_lossy()
            .into_owned();
        let launch_id = ulid::Ulid::new().to_string();
        let stop_id = ulid::Ulid::new().to_string();
        let rule_value = json!({
            "id":"resume-after-exact-stop",
            "enabled":true,
            "on":{
                "event":"command.applied",
                "match":{"event.data.command_id":stop_id},
            },
            "do":[{"action":"resume", "target":"${event.agent}",
                "expected_generation":"${event.generation}", "text":"continue with new input"}],
        });
        let input = eliot_store::RuleDefinitionInput {
            id: "resume-after-exact-stop".to_owned(),
            canonical_definition: serde_json::to_string(&rule_value).unwrap(),
            definition_digest: eliot_core::canonical_json_sha256(&rule_value).unwrap(),
            enabled: true,
        };

        let (agent, state, event, binding) = {
            let mut store = router.inner.store.lock().await;
            let mut launch = SubmitCommand::new(&launch_id, "launch");
            launch.expected_generation = Some(0);
            launch.args = json!({
                "project":"resume-event-fixture",
                "workspace":workspace,
                "harness":"codex",
                "profile":"codex-luna",
                "profile_snapshot":{
                    "harness":"codex",
                    "pass_env":[],
                    "pass_env_values":{"CODEX_HOME":null,"CODEX_SQLITE_HOME":null},
                },
            });
            let agent = store
                .submit("owner", launch, now_ms())
                .unwrap()
                .command
                .target
                .unwrap();
            let expected_agent = store.get_agent(&agent).unwrap().unwrap();
            let binding = NativeStoreBinding {
                home: workspace.clone(),
                root: workspace,
                native_scope: "resume-codex-scope".to_owned(),
            };
            store
                .dispatch_with_native_store_binding(&launch_id, now_ms(), &expected_agent, &binding)
                .unwrap();
            read_fixture_event(
                &mut store,
                &agent,
                "session.ready",
                json!({
                    "native_scope":"resume-codex-scope",
                    "native_ref":"retained-resume-thread",
                }),
            );
            store
                .settle(&launch_id, CommandState::Applied, None, now_ms())
                .unwrap();
            let state = store.ensure_rule_state("owner", &input, now_ms()).unwrap();

            let mut stop = SubmitCommand::new(&stop_id, "stop");
            stop.target = Some(agent.clone());
            stop.expected_generation = Some(1);
            store.submit("owner", stop, now_ms()).unwrap();
            store.dispatch(&stop_id, now_ms()).unwrap();
            read_fixture_event(
                &mut store,
                &agent,
                "session.ended",
                json!({"command_id":stop_id}),
            );
            store
                .settle(&stop_id, CommandState::Applied, None, now_ms())
                .unwrap();
            let event = store
                .read_events(0, 1_000, None)
                .unwrap()
                .into_iter()
                .find(|event| {
                    event.kind == "command.applied"
                        && event.data.get("command_id").and_then(Value::as_str)
                            == Some(stop_id.as_str())
                })
                .expect("the exact stop command applied after session end");
            (agent, state, event, binding)
        };

        assert_eq!(event.agent.as_deref(), Some(agent.as_str()));
        assert_eq!(event.generation, Some(1));
        let mut action = SubmitCommand::new(String::new(), "resume");
        action.target = event.agent.clone();
        action.expected_generation = event.generation;
        action.args = json!({"text":"continue with new input"});
        let prepared = router.prepare_owner_submission(action).await.unwrap();
        assert!(prepared.workspace.is_none());
        assert_eq!(prepared.request.target.as_deref(), Some(agent.as_str()));
        assert_eq!(prepared.request.expected_generation, Some(1));

        let firing = router
            .inner
            .store
            .lock()
            .await
            .admit_rule_action("owner", &state, event.seq, prepared.request, now_ms())
            .unwrap();
        assert!(firing.is_new);
        let child = firing.child.expect("the rule admitted its Resume");
        let resume_id = child.command.id.clone();
        let linked_id = child
            .linked_command
            .as_ref()
            .expect("Resume text becomes a linked SEND")
            .id
            .clone();
        assert_eq!(child.command.verb, "resume");
        assert_eq!(child.command.target.as_deref(), Some(agent.as_str()));
        assert_eq!(child.command.expected_generation, Some(1));
        assert_eq!(child.command.args, json!({}));
        let linked = child.linked_command.as_ref().unwrap();
        assert_eq!(linked.verb, "send");
        assert_eq!(linked.target.as_deref(), Some(agent.as_str()));
        assert_eq!(linked.expected_generation, Some(2));
        assert_eq!(linked.generation, Some(2));
        assert_eq!(linked.state, CommandState::Queued);
        assert_eq!(linked.args, json!({"text":"continue with new input"}));

        let (retained_profile, source_state) = router
            .capture_resume_source_state(&resume_id)
            .await
            .unwrap();
        assert_eq!(
            retained_profile["native_store_binding"],
            serde_json::to_value(&binding).unwrap()
        );
        assert_eq!(
            retained_profile["native_store_binding"]["native_scope"],
            "resume-codex-scope"
        );
        assert_eq!(source_state["retained_generation"], 1);
        assert_eq!(
            source_state["retained_source"]["source"],
            CONTROLLED_PLUGIN_SOURCE
        );

        let dispatched = router
            .inner
            .store
            .lock()
            .await
            .dispatch(&resume_id, now_ms())
            .unwrap()
            .value;
        assert_eq!(dispatched.state, CommandState::Dispatched);
        assert_eq!(dispatched.generation, Some(2));
        assert_eq!(
            router
                .inner
                .store
                .lock()
                .await
                .get_agent(&agent)
                .unwrap()
                .unwrap()
                .generation,
            2
        );
        router
            .inner
            .store
            .lock()
            .await
            .settle(
                &resume_id,
                CommandState::Unknown,
                Some(json!({"code":"unknown"})),
                now_ms(),
            )
            .unwrap();

        let replay = router
            .replay_rule_event_action("owner", &state, state.revision, event.seq)
            .await
            .unwrap()
            .expect("replay resolves the exact event before current generation checks");
        assert!(!replay.is_new);
        let replayed_child = replay.child.expect("replay retains the Resume admission");
        assert_eq!(replayed_child.command.id, resume_id);
        assert_eq!(replayed_child.command.state, CommandState::Unknown);
        assert_eq!(
            replayed_child.command.target.as_deref(),
            Some(agent.as_str())
        );
        assert_eq!(replayed_child.command.generation, Some(2));
        let replayed_linked = replayed_child.linked_command.as_ref().unwrap();
        assert_eq!(replayed_linked.id, linked_id);
        assert_eq!(replayed_linked.target.as_deref(), Some(agent.as_str()));
        assert_eq!(replayed_linked.generation, Some(2));
        assert_eq!(replayed_linked.state, CommandState::Queued);
        assert_eq!(rule_admission_dispatch_plan(&replayed_child), None);
        assert_eq!(
            router
                .inner
                .store
                .lock()
                .await
                .recover_rule_children(None, 100)
                .unwrap(),
            Vec::<eliot_store::Admission>::new()
        );
        assert_eq!(
            router
                .inner
                .store
                .lock()
                .await
                .dispatch(&resume_id, now_ms())
                .unwrap_err()
                .code,
            "bad_state"
        );
    }

    #[tokio::test]
    async fn owner_request_opened_rule_admits_the_captured_request_reply() {
        let router = router();
        let rule_id = "owner-approve-request";
        let definition = json!({
            "id":rule_id,
            "enabled":true,
            "on":{"event":"request.opened"},
            "do":[{"action":"approve", "target":"a1",
                "expected_generation":1,
                "expected_turn_id":"turn-1",
                "request_id":"request-approve-1"}],
        });
        let input = eliot_store::RuleDefinitionInput {
            id: rule_id.to_owned(),
            canonical_definition: serde_json::to_string(&definition).unwrap(),
            definition_digest: eliot_core::canonical_json_sha256(&definition).unwrap(),
            enabled: true,
        };
        let (state, agent, send_id) = {
            let mut store = router.inner.store.lock().await;
            let state = store.ensure_rule_state("owner", &input, now_ms()).unwrap();
            let (agent, send_id) = result_fixture(&mut store);
            (state, agent, send_id)
        };
        let request_id = "request-approve-1";
        {
            let mut store = router.inner.store.lock().await;
            store
                .settle(
                    &send_id,
                    CommandState::Applied,
                    Some(json!({"turn_id":"turn-1"})),
                    now_ms(),
                )
                .unwrap();
            read_fixture_event(
                &mut store,
                &agent,
                "request.opened",
                json!({
                    "command_id":send_id,
                    "request_id":request_id,
                    "turn_id":"turn-1",
                    "kind":"command_execution_approval",
                    "details":{"item_id":"item-1"},
                }),
            );
        }
        let event = router
            .inner
            .store
            .lock()
            .await
            .read_events(0, 1_000, None)
            .unwrap()
            .into_iter()
            .find(|event| {
                event.kind == "request.opened"
                    && event.data.get("request_id").and_then(Value::as_str) == Some(request_id)
            })
            .expect("the exact request.opened event");
        let mut action = SubmitCommand::new(String::new(), "approve");
        action.target = event.agent.clone();
        action.expected_generation = event.generation;
        action.expected_turn_id = Some("turn-1".to_owned());
        action.request_id = Some(request_id.to_owned());

        let prepared = router.prepare_owner_submission(action).await.unwrap();
        let admission = router
            .admit_rule_action("owner", None, &state, state.revision, event.seq, prepared)
            .await
            .unwrap()
            .expect("Owner request reply is admitted");
        let child = admission
            .child
            .expect("the firing has one reply command")
            .command;
        assert_eq!(child.verb, "approve");
        assert_eq!(child.target.as_deref(), Some(agent.as_str()));
        assert_eq!(child.generation, Some(1));
        assert_eq!(child.turn_id.as_deref(), Some("turn-1"));
        assert_eq!(child.request_id.as_deref(), Some(request_id));
        assert_eq!(child.args, json!({}));
    }

    #[tokio::test]
    async fn owner_secret_answer_rejects_once_and_replays_after_request_closes() {
        let router = router();
        let rule_id = "owner-secret-answer";
        let definition = json!({
            "id":rule_id,
            "enabled":true,
            "on":{"event":"request.opened"},
            "do":[{"action":"answer", "target":"a1",
                "expected_generation":1,
                "expected_turn_id":"turn-1",
                "request_id":"request-secret-1"}],
        });
        let input = eliot_store::RuleDefinitionInput {
            id: rule_id.to_owned(),
            canonical_definition: serde_json::to_string(&definition).unwrap(),
            definition_digest: eliot_core::canonical_json_sha256(&definition).unwrap(),
            enabled: true,
        };
        let (state, agent, send_id) = {
            let mut store = router.inner.store.lock().await;
            let state = store.ensure_rule_state("owner", &input, now_ms()).unwrap();
            let (agent, send_id) = result_fixture(&mut store);
            (state, agent, send_id)
        };
        let request_id = "request-secret-1";
        {
            let mut store = router.inner.store.lock().await;
            store
                .settle(
                    &send_id,
                    CommandState::Applied,
                    Some(json!({"turn_id":"turn-1"})),
                    now_ms(),
                )
                .unwrap();
            read_fixture_event(
                &mut store,
                &agent,
                "request.opened",
                json!({
                    "command_id":send_id,
                    "request_id":request_id,
                    "turn_id":"turn-1",
                    "kind":"user_input",
                    "details":{"questions":[{"id":"q1","is_secret":true}]},
                }),
            );
        }
        let event = router
            .inner
            .store
            .lock()
            .await
            .read_events(0, 1_000, None)
            .unwrap()
            .into_iter()
            .find(|event| {
                event.kind == "request.opened"
                    && event.data.get("request_id").and_then(Value::as_str) == Some(request_id)
            })
            .expect("the exact secret request.opened event");
        let mut action = SubmitCommand::new(String::new(), "answer");
        action.target = event.agent.clone();
        action.args = json!({"answers":{"q1":{"answers":["secret-answer-marker"]}}});
        action.expected_generation = event.generation;
        action.expected_turn_id = Some("turn-1".to_owned());
        action.request_id = Some(request_id.to_owned());
        let prepared = router
            .prepare_owner_submission(action.clone())
            .await
            .unwrap();

        let rejected = router
            .admit_rule_action("owner", None, &state, state.revision, event.seq, prepared)
            .await
            .unwrap()
            .expect("the secret answer refusal advances this firing");
        assert!(rejected.is_new);
        assert!(rejected.child.is_none());
        let parent = rejected.parent.expect("metadata-only rejected parent");
        assert_eq!(parent.state, CommandState::Rejected);
        assert_eq!(
            parent.outcome.as_ref().unwrap()["code"],
            "secret_in_payload"
        );
        assert!(
            !serde_json::to_string(&parent)
                .unwrap()
                .contains("secret-answer-marker")
        );
        assert_eq!(rejected.state.current_cursor, event.seq);
        assert_eq!(
            rejected
                .events
                .iter()
                .filter(|event| event.kind == "rule.rejected")
                .count(),
            1
        );

        {
            let mut store = router.inner.store.lock().await;
            read_fixture_event(
                &mut store,
                &agent,
                "request.closed",
                json!({"request_id":request_id,"turn_id":"turn-1"}),
            );
        }
        let before_replay = router.inner.store.lock().await.high_water().unwrap();
        let replay = router
            .admit_rule_action(
                "owner",
                None,
                &state,
                state.revision,
                event.seq,
                PreparedSubmission {
                    request: action,
                    workspace: None,
                },
            )
            .await
            .unwrap()
            .expect("Store returns the retained firing before current pending checks");
        assert!(!replay.is_new);
        assert!(replay.child.is_none());
        assert!(replay.events.is_empty());
        assert_eq!(replay.parent.as_ref().unwrap().id, parent.id);
        assert_eq!(
            router.inner.store.lock().await.high_water().unwrap(),
            before_replay
        );
    }

    #[test]
    fn project_rule_resume_requires_resume_send_and_retained_profile_grants() {
        let checkout = std::fs::canonicalize(std::env::temp_dir())
            .unwrap()
            .to_string_lossy()
            .into_owned();
        let registration = eliot_core::api::ProjectRegistrationView {
            id: "resume-project".to_owned(),
            checkout: checkout.clone(),
            repo_common_dir: checkout,
            profiles: vec!["fake".to_owned()],
            actions: vec!["resume".to_owned(), "send".to_owned()],
            interpreters: std::collections::BTreeMap::new(),
            run_env: Vec::new(),
            revision: 1,
        };
        let mut request = SubmitCommand::new(String::new(), "resume");
        request.target = Some("a7".to_owned());
        request.expected_generation = Some(7);
        request.args = json!({"text":"resume with new input"});
        let mut authorized = request.clone();
        authorize_command(
            &Principal::ProjectRule {
                registration: registration.clone(),
            },
            &mut authorized,
        )
        .expect("Project resume and its linked SEND are explicitly approved");

        let mut without_captured_generation = request.clone();
        without_captured_generation.expected_generation = None;
        assert_eq!(
            validate_rule_resume_action(&without_captured_generation, Some(&registration))
                .unwrap_err()
                .code,
            "invalid",
            "EventRule Resume must keep its captured generation",
        );

        let mut without_send = registration.clone();
        without_send.actions = vec!["resume".to_owned()];
        assert_eq!(
            validate_rule_resume_action(&request, Some(&without_send))
                .unwrap_err()
                .code,
            "not_allowed"
        );
        let mut without_resume = registration.clone();
        without_resume.actions = vec!["send".to_owned()];
        assert_eq!(
            authorize_command(
                &Principal::ProjectRule {
                    registration: without_resume,
                },
                &mut request.clone(),
            )
            .unwrap_err()
            .code,
            "not_allowed"
        );

        let mut no_input = request.clone();
        no_input.args = json!({});
        validate_rule_resume_action(&no_input, Some(&without_send))
            .expect("resume without new text does not need a SEND grant");
        let prepared = PreparedSubmission {
            request: no_input.clone(),
            workspace: None,
        };
        validate_rule_resume_preparation(&prepared, Some(&without_send))
            .expect("existing-workspace resume has no preparation payload");
        let mut managed = prepared;
        managed.workspace = Some(eliot_core::WorkspacePreparationInput {
            project: "project".to_owned(),
            repo_path: "repo".to_owned(),
            repo_common_dir: "repo".to_owned(),
            requested_branch: "codex/resume".to_owned(),
            base_ref: None,
            base_oid: "0".repeat(40),
            worktree_path: "worktree".to_owned(),
            frozen_setup: Vec::new(),
        });
        assert_eq!(
            validate_rule_resume_preparation(&managed, Some(&without_send))
                .unwrap_err()
                .code,
            "unsupported"
        );

        let mut with_override = request.clone();
        with_override.args["profile"] = Value::String("other".to_owned());
        assert_eq!(
            validate_rule_resume_action(&with_override, Some(&registration))
                .unwrap_err()
                .code,
            "unsupported"
        );

        let agent = Agent::new("a7", "project-label", "fake", "fake", 1);
        validate_project_rule_resume_profile(&registration, &agent)
            .expect("the retained Agent profile is on the registration allowlist");
        let mut unapproved_profile = agent;
        unapproved_profile.profile = "other".to_owned();
        assert_eq!(
            validate_project_rule_resume_profile(&registration, &unapproved_profile)
                .unwrap_err()
                .code,
            "not_allowed"
        );

        let mut frozen_resume =
            command_with_state("resume-parent", "resume", CommandState::Accepted);
        frozen_resume.target = Some("a7".to_owned());
        frozen_resume.expected_generation = Some(7);
        frozen_resume.args = json!({});
        assert!(validate_project_rule_resume_command(&registration, &frozen_resume, true).is_ok());
        assert_eq!(
            validate_project_rule_resume_command(&without_send, &frozen_resume, true)
                .unwrap_err()
                .code,
            "not_allowed"
        );
    }

    #[test]
    fn project_rule_launch_requires_launch_profile_checkout_and_send_grants() {
        let checkout = std::fs::canonicalize(std::env::temp_dir())
            .unwrap()
            .to_string_lossy()
            .into_owned();
        let registration = eliot_core::api::ProjectRegistrationView {
            id: "launch-project".to_owned(),
            checkout: checkout.clone(),
            repo_common_dir: checkout.clone(),
            profiles: vec!["fake".to_owned()],
            actions: vec!["launch".to_owned(), "send".to_owned()],
            interpreters: std::collections::BTreeMap::new(),
            run_env: Vec::new(),
            revision: 1,
        };
        let mut request = SubmitCommand::new(String::new(), "launch");
        request.expected_generation = Some(0);
        request.args = json!({
            "profile":"fake",
            "workspace":checkout,
            "text":"review this result",
        });
        validate_rule_launch_action(&request, Some(&registration))
            .expect("the registered project grants launch, profile and linked send");

        let mut without_send = registration.clone();
        without_send.actions = vec!["launch".to_owned()];
        assert_eq!(
            validate_rule_launch_action(&request, Some(&without_send))
                .unwrap_err()
                .code,
            "not_allowed"
        );

        let mut without_launch = registration.clone();
        without_launch.actions = vec!["send".to_owned()];
        assert_eq!(
            validate_rule_launch_action(&request, Some(&without_launch))
                .unwrap_err()
                .code,
            "not_allowed"
        );

        let mut unapproved_profile = request.clone();
        unapproved_profile.args["profile"] = Value::String("other".to_owned());
        assert_eq!(
            validate_rule_launch_action(&unapproved_profile, Some(&registration))
                .unwrap_err()
                .code,
            "not_allowed"
        );

        let mut other_checkout = request.clone();
        other_checkout.args["workspace"] = Value::String("other-checkout".to_owned());
        assert_eq!(
            validate_rule_launch_action(&other_checkout, Some(&registration))
                .unwrap_err()
                .code,
            "not_allowed"
        );
    }

    #[test]
    fn project_rule_launch_dispatch_rejects_present_null_preparation_fields() {
        let checkout = std::fs::canonicalize(std::env::temp_dir())
            .unwrap()
            .to_string_lossy()
            .into_owned();
        let registration = eliot_core::api::ProjectRegistrationView {
            id: "launch-project".to_owned(),
            checkout: checkout.clone(),
            repo_common_dir: checkout.clone(),
            profiles: vec!["fake".to_owned()],
            actions: vec!["launch".to_owned(), "send".to_owned()],
            interpreters: std::collections::BTreeMap::new(),
            run_env: Vec::new(),
            revision: 1,
        };
        let command = Command {
            id: "launch-command".to_owned(),
            principal: "project:launch-project".to_owned(),
            method: "command.submit".to_owned(),
            verb: "launch".to_owned(),
            target: None,
            expected_generation: Some(0),
            generation: None,
            args: json!({
                "profile":"fake",
                "workspace":checkout,
                "project":checkout,
                "profile_snapshot":{"harness":"fake", "setup":[]},
                "text":"review this result",
            }),
            state: CommandState::Accepted,
            turn_id: None,
            request_id: None,
            outcome: None,
            created_at: 0,
            updated_at: 0,
            probe: false,
        };
        validate_project_rule_launch_command(&registration, &command)
            .expect("the frozen existing-checkout launch is permitted");

        for field in ["workspace_preparation", "worktree", "base"] {
            let mut malformed = command.clone();
            malformed.args[field] = Value::Null;
            assert_eq!(
                validate_project_rule_launch_command(&registration, &malformed)
                    .unwrap_err()
                    .code,
                "unsupported",
                "present null field {field} must remain rejected"
            );
        }
    }

    #[test]
    fn rule_launch_preparation_rejects_workspace_preparation_and_profile_setup() {
        let workspace = std::env::current_dir()
            .unwrap()
            .canonicalize()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        let mut request = SubmitCommand::new(String::new(), "launch");
        request.expected_generation = Some(0);
        request.args = json!({
            "profile":"fake",
            "workspace":workspace,
            "project":workspace,
            "profile_snapshot":{"harness":"fake", "setup":[]},
        });
        let mut prepared = PreparedSubmission {
            request,
            workspace: None,
        };
        validate_rule_launch_preparation(&prepared)
            .expect("a prepared existing-workspace launch is allowed");

        prepared.request.args["profile_snapshot"]["setup"] = json!([{}]);
        assert_eq!(
            validate_rule_launch_preparation(&prepared)
                .unwrap_err()
                .code,
            "unsupported"
        );
        prepared.request.args["profile_snapshot"]["setup"] = json!([]);
        prepared.workspace = Some(eliot_core::WorkspacePreparationInput {
            project: "project".to_owned(),
            repo_path: "repo".to_owned(),
            repo_common_dir: "repo".to_owned(),
            requested_branch: "codex/test".to_owned(),
            base_ref: None,
            base_oid: "0".repeat(40),
            worktree_path: "worktree".to_owned(),
            frozen_setup: Vec::new(),
        });
        assert_eq!(
            validate_rule_launch_preparation(&prepared)
                .unwrap_err()
                .code,
            "unsupported"
        );
    }

    #[test]
    fn project_request_replies_require_per_verb_grants_and_keep_exact_bindings() {
        let checkout = std::fs::canonicalize(std::env::temp_dir())
            .unwrap()
            .to_string_lossy()
            .into_owned();
        let base = eliot_core::api::ProjectRegistrationView {
            id: "reply-project".to_owned(),
            checkout: checkout.clone(),
            repo_common_dir: checkout,
            profiles: vec!["fake".to_owned()],
            actions: Vec::new(),
            interpreters: std::collections::BTreeMap::new(),
            run_env: Vec::new(),
            revision: 1,
        };
        let verbs = ["approve", "deny", "answer"];
        for granted in verbs {
            let mut registration = base.clone();
            registration.actions = vec![granted.to_owned()];
            for verb in verbs {
                let mut request = SubmitCommand::new(String::new(), verb);
                request.target = Some("a7".to_owned());
                request.args = if verb == "answer" {
                    json!({"answers":{"q1":{"answers":["Continue"]}}})
                } else {
                    json!({})
                };
                request.expected_generation = Some(7);
                request.expected_turn_id = Some("turn-captured".to_owned());
                request.request_id = Some("request-captured".to_owned());
                let result = authorize_command(
                    &Principal::ProjectRule {
                        registration: registration.clone(),
                    },
                    &mut request,
                );
                if granted == verb {
                    result.expect("each reply verb has its own explicit grant");
                    assert_eq!(request.target.as_deref(), Some("a7"));
                    assert_eq!(request.expected_generation, Some(7));
                    assert_eq!(request.expected_turn_id.as_deref(), Some("turn-captured"));
                    assert_eq!(request.request_id.as_deref(), Some("request-captured"));
                } else {
                    assert_eq!(result.unwrap_err().code, "not_allowed");
                }
            }
        }
    }

    #[test]
    fn project_rule_steer_requires_an_explicit_grant_and_keeps_its_fences() {
        let checkout = std::fs::canonicalize(std::env::temp_dir())
            .unwrap()
            .to_string_lossy()
            .into_owned();
        let registration = eliot_core::api::ProjectRegistrationView {
            id: "steer-project".to_owned(),
            checkout: checkout.clone(),
            repo_common_dir: checkout,
            profiles: vec!["fake".to_owned()],
            actions: vec!["steer".to_owned()],
            interpreters: std::collections::BTreeMap::new(),
            run_env: Vec::new(),
            revision: 1,
        };
        let mut request = SubmitCommand::new(String::new(), "steer");
        request.target = Some("a1".to_owned());
        request.args = json!({"text":"follow-up"});
        request.expected_generation = Some(7);
        request.expected_turn_id = Some("turn-captured".to_owned());

        authorize_command(
            &Principal::ProjectRule {
                registration: registration.clone(),
            },
            &mut request,
        )
        .expect("explicit project STEER grant");
        assert_eq!(request.target.as_deref(), Some("a1"));
        assert_eq!(request.expected_generation, Some(7));
        assert_eq!(request.expected_turn_id.as_deref(), Some("turn-captured"));

        let mut revoked = registration;
        revoked.actions = vec!["send".to_owned()];
        let error = authorize_command(
            &Principal::ProjectRule {
                registration: revoked,
            },
            &mut request,
        )
        .unwrap_err();
        assert_eq!(error.code, "not_allowed");
    }

    #[tokio::test]
    async fn project_rule_steer_rejects_a_stale_registration_before_admission() {
        let router = router();
        let project_id = "steer-race";
        let checkout = std::fs::canonicalize(std::env::temp_dir())
            .unwrap()
            .to_string_lossy()
            .into_owned();
        let current = {
            let mut store = router.inner.store.lock().await;
            store
                .upsert_project_registration(
                    "owner",
                    &eliot_store::ProjectRegistrationInput {
                        id: project_id.to_owned(),
                        checkout: checkout.clone(),
                        repo_common_dir: checkout,
                        profiles: vec!["fake".to_owned()],
                        actions: vec!["send".to_owned()],
                        interpreters: std::collections::BTreeMap::new(),
                        run_env: Vec::new(),
                    },
                    now_ms(),
                )
                .unwrap()
                .value
        };
        let mut captured = current;
        captured.actions = vec!["steer".to_owned()];
        let state = eliot_core::api::RuleStateView {
            id: "steer-race-rule".to_owned(),
            project: Some(project_id.to_owned()),
            revision: 1,
            current_cursor: 0,
            retiring_revision: None,
            retiring_cursor: None,
            retire_through: None,
            enabled: true,
            paused_reason: None,
        };
        let mut action = SubmitCommand::new(String::new(), "steer");
        action.target = Some("a1".to_owned());
        action.args = json!({"text":"follow-up"});
        action.expected_generation = Some(1);
        action.expected_turn_id = Some("turn-captured".to_owned());
        authorize_command(
            &Principal::ProjectRule {
                registration: captured.clone(),
            },
            &mut action,
        )
        .expect("captured project snapshot grants STEER");

        let before = router.inner.store.lock().await.high_water().unwrap();
        let error = router
            .admit_rule_action(
                &format!("project:{project_id}"),
                Some(&captured),
                &state,
                state.revision,
                1,
                PreparedSubmission {
                    request: action,
                    workspace: None,
                },
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, "not_allowed");
        assert_eq!(
            router.inner.store.lock().await.high_water().unwrap(),
            before
        );
    }

    #[tokio::test]
    async fn project_rule_reply_replays_after_the_grant_is_revoked() {
        let router = router();
        let project_id = "typed-results";
        let principal = format!("project:{project_id}");
        let rule_id = "project-reply-replay";
        let definition = json!({
            "id":rule_id,
            "enabled":true,
            "on":{"event":"request.opened"},
            "do":[{"action":"approve", "target":"a1",
                "expected_generation":1,
                "expected_turn_id":"turn-1",
                "request_id":"request-project-replay"}],
        });
        let input = eliot_store::RuleDefinitionInput {
            id: rule_id.to_owned(),
            canonical_definition: serde_json::to_string(&definition).unwrap(),
            definition_digest: eliot_core::canonical_json_sha256(&definition).unwrap(),
            enabled: true,
        };
        let registration_input = eliot_store::ProjectRegistrationInput {
            id: project_id.to_owned(),
            checkout: "synthetic-workspace".to_owned(),
            repo_common_dir: "synthetic-workspace/.git".to_owned(),
            profiles: vec!["synthetic".to_owned()],
            actions: vec!["approve".to_owned()],
            interpreters: std::collections::BTreeMap::new(),
            run_env: Vec::new(),
        };
        let (state, registration, agent, send_id) = {
            let mut store = router.inner.store.lock().await;
            let registration = store
                .upsert_project_registration("owner", &registration_input, now_ms())
                .unwrap()
                .value;
            let state = store
                .ensure_rule_state(&principal, &input, now_ms())
                .unwrap();
            let (agent, send_id) = result_fixture(&mut store);
            (state, registration, agent, send_id)
        };
        let request_id = "request-project-replay";
        {
            let mut store = router.inner.store.lock().await;
            store
                .settle(
                    &send_id,
                    CommandState::Applied,
                    Some(json!({"turn_id":"turn-1"})),
                    now_ms(),
                )
                .unwrap();
            read_fixture_event(
                &mut store,
                &agent,
                "request.opened",
                json!({
                    "command_id":send_id,
                    "request_id":request_id,
                    "turn_id":"turn-1",
                    "kind":"command_execution_approval",
                    "details":{"item_id":"item-1"},
                }),
            );
        }
        let event = router
            .inner
            .store
            .lock()
            .await
            .read_events(0, 1_000, None)
            .unwrap()
            .into_iter()
            .find(|event| {
                event.kind == "request.opened"
                    && event.data.get("request_id").and_then(Value::as_str) == Some(request_id)
            })
            .expect("the exact project request.opened event");
        let mut action = SubmitCommand::new(String::new(), "approve");
        action.target = event.agent.clone();
        action.expected_generation = event.generation;
        action.expected_turn_id = Some("turn-1".to_owned());
        action.request_id = Some(request_id.to_owned());
        authorize_command(
            &Principal::ProjectRule {
                registration: registration.clone(),
            },
            &mut action,
        )
        .expect("the captured project snapshot grants APPROVE");
        let first = router
            .inner
            .store
            .lock()
            .await
            .admit_rule_action(&principal, &state, event.seq, action.clone(), now_ms())
            .unwrap();
        let first_parent = first.parent.expect("the project firing parent");
        let first_child = first.child.expect("the admitted project reply").command;
        assert_eq!(first_child.verb, "approve");

        let mut revoked = registration_input;
        revoked.actions = vec!["send".to_owned()];
        {
            let mut store = router.inner.store.lock().await;
            store
                .upsert_project_registration("owner", &revoked, now_ms())
                .unwrap();
        }
        let before_replay = router.inner.store.lock().await.high_water().unwrap();
        let replay = router
            .admit_rule_action(
                &principal,
                Some(&registration),
                &state,
                state.revision,
                event.seq,
                PreparedSubmission {
                    request: action,
                    workspace: None,
                },
            )
            .await
            .unwrap()
            .expect("Store returns the retained firing before the current grant gate");
        assert!(!replay.is_new);
        assert!(replay.events.is_empty());
        assert_eq!(replay.parent.as_ref().unwrap().id, first_parent.id);
        assert_eq!(replay.child.as_ref().unwrap().command.id, first_child.id);
        assert_eq!(
            router.inner.store.lock().await.high_water().unwrap(),
            before_replay
        );
    }

    #[tokio::test]
    async fn typed_agent_views_reject_malformed_requests_and_keep_null_defaults() {
        let router = router();
        for method in [
            "agent.get",
            "agent.list",
            "agent.processes",
            "agent.stats",
            "agent.git",
            "agent.edits",
            "daemon.status",
        ] {
            let params = if matches!(method, "agent.list" | "daemon.status") {
                json!({"extra":0})
            } else {
                json!({"id":"a1", "extra":0})
            };
            assert_eq!(router.handle(method, json!({
                "_auth":{"kind":"owner", "credential":"owner-credential"}, "_payload":params
            })).await.unwrap_err().code, "invalid", "{method}");
        }
        for method in ["agent.processes", "agent.stats", "agent.git", "agent.edits"] {
            for generation in [json!(-1), json!(false), json!(1.0), json!("1")] {
                assert_eq!(
                    router
                        .handle(
                            method,
                            json!({
                                "_auth":{"kind":"owner", "credential":"owner-credential"},
                                "_payload":{"id":"a1", "generation":generation}
                            })
                        )
                        .await
                        .unwrap_err()
                        .code,
                    "invalid",
                    "{method}"
                );
            }
            assert_eq!(
                router
                    .handle(
                        method,
                        json!({
                            "_auth":{"kind":"owner", "credential":"owner-credential"},
                            "_payload":{"id":"a1", "generation":null}
                        })
                    )
                    .await
                    .unwrap_err()
                    .code,
                "not_found",
                "{method}"
            );
        }
        for method in ["agent.list", "agent.processes", "agent.edits"] {
            for limit in [
                json!(null),
                json!(0),
                json!(1001),
                json!(1.0),
                json!(false),
                json!("1"),
            ] {
                let mut params = json!({"limit":limit});
                if method != "agent.list" {
                    params["id"] = json!("a1");
                }
                assert_eq!(router.handle(method, json!({
                    "_auth":{"kind":"owner", "credential":"owner-credential"}, "_payload":params
                })).await.unwrap_err().code, "invalid", "{method}");
            }
        }
        assert_eq!(
            router
                .agent_list(&json!({"observations":null}), &Principal::Owner)
                .await
                .unwrap_err()
                .code,
            "invalid"
        );
        let list = router
            .agent_list(&json!({"after":null}), &Principal::Owner)
            .await
            .unwrap();
        assert_eq!(list, json!({"agents":[], "next":null}));
        assert_eq!(
            router
                .agent_processes(&json!({"id":"a1", "scope":false}), &Principal::Owner)
                .await
                .unwrap_err()
                .code,
            "invalid"
        );
        assert_eq!(
            router
                .agent_processes(
                    &json!({"id":"a1", "scope":null, "after":null}),
                    &Principal::Owner
                )
                .await
                .unwrap_err()
                .code,
            "not_found"
        );
    }

    #[tokio::test]
    async fn typed_agent_views_preserve_self_list_and_live_presence_gate() {
        let router = router();
        let (id, other) = {
            let mut store = router.inner.store.lock().await;
            (
                admit(&mut store)[0].agent.clone().unwrap(),
                admit(&mut store)[0].agent.clone().unwrap(),
            )
        };
        let principal = Principal::Agent {
            id: id.clone(),
            generation: 0,
        };
        for observations in [false, true] {
            let reply = router
                .agent_list(
                    &json!({"after":"not-canonical", "limit":1, "observations":observations}),
                    &principal,
                )
                .await
                .unwrap();
            assert_eq!(reply["next"], Value::Null);
            assert_eq!(reply["agents"].as_array().unwrap().len(), 1);
            assert_eq!(reply["agents"][0]["id"], json!(id));
            assert_eq!(
                reply["agents"][0].get("observation").is_some(),
                observations
            );
            assert!(reply["agents"][0].get("agent").is_none());
        }
        let get = router
            .agent_get(&json!({"id":id}), &principal)
            .await
            .unwrap();
        assert_eq!(get["id"], json!(id));
        assert!(get.get("agent").is_none());
        assert_eq!(get["source_states"], json!([]));
        assert_eq!(
            router
                .agent_get(&json!({"id":other}), &principal)
                .await
                .unwrap_err()
                .code,
            "not_allowed"
        );
        assert_eq!(
            router
                .agent_stats(&json!({"id":id, "generation":1}), &Principal::Owner)
                .await
                .unwrap_err()
                .code,
            "stale_generation"
        );
        assert_eq!(
            router
                .agent_git(&json!({"id":id, "generation":1}), &Principal::Owner)
                .await
                .unwrap_err()
                .code,
            "stale_generation"
        );
        let edits = router
            .agent_edits(&json!({"id":id, "after":" \t"}), &principal)
            .await
            .unwrap();
        assert_eq!(edits["edits"], json!([]));
        assert_eq!(edits["next"], Value::Null);
        let busy = {
            let mut store = router.inner.store.lock().await;
            result_fixture(&mut store).0
        };
        assert_eq!(
            router
                .agent_get(&json!({"id":busy}), &Principal::Owner)
                .await
                .unwrap_err()
                .code,
            "unknown"
        );
        assert_eq!(
            router
                .inner
                .store
                .lock()
                .await
                .get_agent(&busy)
                .unwrap()
                .unwrap()
                .state,
            AgentState::Busy
        );
    }

    #[tokio::test]
    async fn typed_agent_views_workspace_cursor_binds_after_target_resolution() {
        let router = router();
        let id = {
            let mut store = router.inner.store.lock().await;
            let input = WorkspacePreparationInput {
                project: "typed-views".to_owned(),
                repo_path: "/synthetic-source".to_owned(),
                repo_common_dir: "/synthetic-source/.git".to_owned(),
                requested_branch: "codex/typed-views".to_owned(),
                base_ref: None,
                base_oid: "a".repeat(40),
                worktree_path: "/synthetic-worktree".to_owned(),
                frozen_setup: Vec::new(),
            };
            let id = store.submit_with_workspace("owner", SubmitCommand {
                id:ulid::Ulid::new().to_string(), target:None, verb:"launch".to_owned(),
                args:json!({"project":input.project, "workspace":input.repo_path, "worktree":input.requested_branch}),
                expected_generation:Some(0), expected_turn_id:None, request_id:None, probe:false,
            }, input, now_ms()).unwrap().command.target.unwrap();
            store
                .ensure_workspace_process_source(&id, 1, now_ms())
                .unwrap();
            store
                .observe_workspace_processes(
                    &id,
                    1,
                    &eliot_core::ProcessSnapshot {
                        samples: vec![eliot_core::ProcessSample {
                            record: eliot_core::ProcessRecord {
                                identity: eliot_core::ProcessIdentity {
                                    host_boot_id: "fixture-boot".to_owned(),
                                    pid: 42,
                                    start_time: 1,
                                },
                                owner: format!("workspace:{id}#1"),
                                role: "setup".to_owned(),
                                state: eliot_core::ProcessState::Exited,
                                exit_code: Some(0),
                                updated_at: now_ms(),
                            },
                            cpu_time_ns: None,
                            rss_bytes: None,
                        }],
                        complete: true,
                    },
                    now_ms(),
                )
                .unwrap();
            id
        };
        let first = router
            .agent_processes(&json!({"id":id, "scope":"workspace"}), &Principal::Owner)
            .await
            .unwrap();
        assert_eq!(first["generation"], json!(1));
        assert_eq!(first["scope"], json!("workspace"));
        assert_eq!(first["processes"].as_array().unwrap().len(), 1);
        let next = first["next"].as_str().unwrap();
        ProcessPageCursor::decode(next)
            .unwrap()
            .validate_binding(&id, 1, "workspace")
            .unwrap();
        let empty = router
            .agent_processes(
                &json!({"id":id, "scope":"workspace", "after":next}),
                &Principal::Owner,
            )
            .await
            .unwrap();
        assert_eq!(empty["processes"], json!([]));
        assert_eq!(empty["next"], Value::Null);
        assert_eq!(
            router
                .agent_processes(&json!({"id":id, "after":next}), &Principal::Owner)
                .await
                .unwrap_err()
                .code,
            "invalid"
        );
        assert_eq!(
            router
                .agent_processes(
                    &json!({"id":id, "scope":"workspace", "generation":2}),
                    &Principal::Owner
                )
                .await
                .unwrap_err()
                .code,
            "stale_generation"
        );
        assert_eq!(
            router
                .agent_processes(&json!({"id":id, "generation":1}), &Principal::Owner)
                .await
                .unwrap_err()
                .code,
            "stale_generation"
        );
        let principal = Principal::Agent {
            id: id.clone(),
            generation: 0,
        };
        assert_eq!(
            router
                .agent_processes(&json!({"id":id, "scope":"workspace"}), &principal)
                .await
                .unwrap_err()
                .code,
            "not_allowed"
        );
        let native = router
            .agent_processes(&json!({"id":id, "scope":null}), &Principal::Owner)
            .await
            .unwrap();
        assert_eq!(native["generation"], json!(0));
        assert!(native.get("scope").is_none());
        let get: AgentGetResponse = serde_json::from_value(
            router
                .agent_get(&json!({"id":id}), &Principal::Owner)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(get.agent.generation, 0);
        assert!(
            get.source_states
                .iter()
                .any(|state| state.source == "workspace_process" && state.generation == 1)
        );
    }

    #[tokio::test]
    async fn typed_agent_views_status_and_shutdown_keep_identical_blocker_wire() {
        let router = router();
        {
            let mut store = router.inner.store.lock().await;
            admit(&mut store);
        }
        let status = router
            .handle(
                "daemon.status",
                json!({
                    "_auth":{"kind":"owner", "credential":"owner-credential"}, "_payload":{}
                }),
            )
            .await
            .unwrap();
        let typed: DaemonStatusResponse = serde_json::from_value(status.clone()).unwrap();
        assert_eq!(typed.state, "running");
        assert_eq!(typed.admission, "open");
        assert_eq!(typed.agents, 1);
        assert!(
            typed
                .blockers
                .iter()
                .any(|blocker| matches!(blocker, StatusBlocker::AgentLive { .. }))
        );
        assert!(
            typed
                .blockers
                .iter()
                .any(|blocker| matches!(blocker, StatusBlocker::CommandUnsettled { .. }))
        );
        let shutdown = router
            .shutdown(
                &json!({"id":ulid::Ulid::new().to_string(), "mode":"drain"}),
                &Principal::Owner,
            )
            .await
            .unwrap();
        assert_eq!(shutdown["blockers"], status["blockers"]);
        assert_eq!(shutdown["state"], json!("dispatched"));
        assert_eq!(shutdown["mode"], json!("drain"));
        assert_eq!(
            router.status().await.unwrap()["admission"],
            json!("draining")
        );
        assert!(!router.should_exit().await.unwrap());
    }

    #[tokio::test]
    async fn typed_native_metadata_owner_first_and_malformed_params() {
        let router = router();
        let principal = Principal::Agent {
            id: "a1".to_owned(),
            generation: 1,
        };
        assert_eq!(
            router
                .agent_discover(&Value::Null, &principal)
                .await
                .unwrap_err()
                .code,
            "not_allowed"
        );
        assert_eq!(
            router
                .agent_history(&Value::Null, &principal)
                .await
                .unwrap_err()
                .code,
            "not_allowed"
        );
        for params in [
            json!({}),
            json!({"profile":null}),
            json!({"profile":" \t"}),
            json!({"profile":"fake", "extra":0}),
            json!({"profile":"fake", "harness":false}),
            json!({"profile":"fake", "native_scope":512}),
            json!({"profile":"fake", "native_scope":"x\ny"}),
            json!({"profile":"fake", "native_scope":"x".repeat(513)}),
            json!({"profile":"fake", "cursor":false}),
            json!({"profile":"fake", "cursor":"x\ny"}),
            json!({"profile":"fake", "cursor":"x".repeat(16 * 1024 + 1)}),
            json!({"profile":"fake", "limit":0}),
        ] {
            assert_eq!(
                router
                    .agent_discover(&params, &Principal::Owner)
                    .await
                    .unwrap_err()
                    .code,
                "invalid",
                "{params}"
            );
        }
        let base = json!({"id":"a1", "source":"native", "view":"turns"});
        for (field, value) in [
            ("id", json!(false)),
            ("source", json!(null)),
            ("source", json!("other")),
            ("view", json!("other")),
            ("view", json!(null)),
            ("view", json!("items")),
            ("generation", json!(-1)),
            ("generation", json!(1.0)),
            ("generation", json!("1")),
            ("turn_id", json!("turn-1")),
            ("turn_id", json!(false)),
            ("cursor", json!(false)),
            ("cursor", json!("x".repeat(16 * 1024 + 1))),
            ("limit", json!(null)),
            ("limit", json!(1001)),
            ("extra", json!(0)),
        ] {
            let mut params = base.clone();
            params[field] = value;
            assert_eq!(
                router
                    .agent_history(&params, &Principal::Owner)
                    .await
                    .unwrap_err()
                    .code,
                "invalid",
                "{field}"
            );
        }
        let mut missing_source = base;
        missing_source.as_object_mut().unwrap().remove("source");
        assert_eq!(
            router
                .agent_history(&missing_source, &Principal::Owner)
                .await
                .unwrap_err()
                .code,
            "invalid"
        );
        for turn_id in ["x\ny".to_owned(), "x".repeat(16 * 1024 + 1)] {
            assert_eq!(
                router
                    .agent_history(
                        &json!({"id":"a1", "source":"native", "view":"items", "turn_id":turn_id}),
                        &Principal::Owner,
                    )
                    .await
                    .unwrap_err()
                    .code,
                "invalid"
            );
        }
    }

    #[tokio::test]
    async fn typed_native_metadata_optional_nulls_keep_absent_semantics() {
        let router = router();
        router.inner.mode.store(MODE_DRAIN, Ordering::SeqCst);
        for params in [
            json!({"profile":"fake"}),
            json!({"profile":"fake", "harness":null, "native_scope":null, "cursor":null}),
        ] {
            // Valid grammar reaches the existing shutdown fence before profile or reader I/O.
            assert_eq!(
                router
                    .agent_discover(&params, &Principal::Owner)
                    .await
                    .unwrap_err()
                    .code,
                "unavailable"
            );
        }
        for params in [
            json!({"id":"a1", "source":"native", "view":"turns"}),
            json!({"id":"a1", "source":"native", "view":"turns", "generation":null, "turn_id":null, "cursor":null}),
            // The private history cursor has no discovery-style control-character ban.
            json!({"id":"a1", "source":"native", "view":"turns", "cursor":"opaque\nvalue"}),
        ] {
            assert_eq!(
                router
                    .agent_history(&params, &Principal::Owner)
                    .await
                    .unwrap_err()
                    .code,
                "not_found"
            );
        }
        let store = router.inner.store.lock().await;
        assert_eq!(store.agent_count().unwrap(), 0);
        assert_eq!(store.high_water().unwrap(), 0);
    }

    #[tokio::test]
    async fn typed_native_history_current_generation_fence_has_no_durable_effects() {
        let router = router();
        let attach_id = ulid::Ulid::new().to_string();
        let (agent, before_events, before_source, before_command) = {
            let mut store = router.inner.store.lock().await;
            let agent = store.submit("owner", SubmitCommand {
                id:attach_id.clone(), target:None, verb:"attach".to_owned(),
                args:json!({"profile":"fixture", "harness":"fake", "mode":"observe",
                    "native_scope":"fixture-scope", "native_ref":"fixture-ref", "profile_snapshot":{}}),
                expected_generation:Some(0), expected_turn_id:None, request_id:None, probe:false,
            }, now_ms()).unwrap().command.target.unwrap();
            // Commit only the source fence; no PluginHost actor or process is started.
            store.dispatch(&attach_id, now_ms()).unwrap();
            let events = store.read_events(0, 1000, None).unwrap();
            let source = store.read_source_state(&agent, 1, "observe").unwrap();
            let command = store.get_command(&attach_id).unwrap().unwrap();
            (agent, events, source, command)
        };
        let mut receiver = router.inner.bus.subscribe();
        assert_eq!(
            router
                .agent_history(
                    &json!({"id":agent, "generation":0, "source":"native", "view":"turns"}),
                    &Principal::Owner,
                )
                .await
                .unwrap_err()
                .code,
            "stale_generation"
        );
        for generation in [Value::Null, json!(1)] {
            assert_eq!(router.agent_history(
                &json!({"id":agent, "generation":generation, "source":"native", "view":"turns"}),
                &Principal::Owner,
            ).await.unwrap_err().code, "unavailable");
        }
        let store = router.inner.store.lock().await;
        assert_eq!(store.read_events(0, 1000, None).unwrap(), before_events);
        assert_eq!(
            store.read_source_state(&agent, 1, "observe").unwrap(),
            before_source
        );
        assert_eq!(
            store.get_command(&attach_id).unwrap().unwrap(),
            before_command
        );
        assert_eq!(store.last_model_progress(&agent, 1).unwrap(), None);
        assert_eq!(store.agent_count().unwrap(), 1);
        assert!(matches!(
            receiver.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
    }

    fn read_fixture_event(store: &mut eliot_store::Store, agent: &str, kind: &str, data: Value) {
        store
            .put_batch(
                eliot_core::SourceBatch {
                    batch_id: ulid::Ulid::new().to_string(),
                    agent_id: agent.to_owned(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![eliot_core::NativeEvent {
                        ts: None,
                        kind: kind.to_owned(),
                        native_id: ulid::Ulid::new().to_string(),
                        data,
                    }],
                    cursor: None,
                },
                now_ms(),
            )
            .unwrap();
    }

    fn stopped_fake_agent(store: &mut eliot_store::Store, project: &str) -> String {
        let workspace = std::env::temp_dir()
            .canonicalize()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        let launch_id = ulid::Ulid::new().to_string();
        let mut launch = SubmitCommand::new(launch_id.clone(), "launch");
        launch.expected_generation = Some(0);
        launch.args = json!({
            "project":project,
            "workspace":workspace,
            "harness":"fake",
            "profile":"fake",
            "profile_snapshot":{"harness":"fake"},
        });
        let agent = store
            .submit("owner", launch, now_ms())
            .unwrap()
            .command
            .target
            .unwrap();
        store.dispatch(&launch_id, now_ms()).unwrap();
        read_fixture_event(
            store,
            &agent,
            "session.ready",
            json!({
                "command_id":launch_id,
                "native_scope":format!("fake:synthetic:scope:{agent}"),
                "native_ref":format!("fake:synthetic:session:{agent}"),
            }),
        );
        store
            .settle(&launch_id, CommandState::Applied, None, now_ms())
            .unwrap();
        read_fixture_event(store, &agent, "session.ended", json!({}));
        agent
    }

    fn result_fixture(store: &mut eliot_store::Store) -> (String, String) {
        let launch_id = ulid::Ulid::new().to_string();
        let agent = store
            .submit(
                "owner",
                SubmitCommand {
                    id: launch_id.clone(),
                    target: None,
                    verb: "launch".to_owned(),
                    args: json!({
                        "project":"typed-results", "workspace":"synthetic-workspace",
                        "harness":"codex", "profile":"synthetic", "profile_snapshot":{},
                    }),
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
        store.dispatch(&launch_id, now_ms()).unwrap();
        read_fixture_event(
            store,
            &agent,
            "session.ready",
            json!({"native_scope":"fixture-scope", "native_ref":"fixture-ref"}),
        );
        store
            .settle(&launch_id, CommandState::Applied, None, now_ms())
            .unwrap();
        let send_id = ulid::Ulid::new().to_string();
        store
            .submit(
                "owner",
                SubmitCommand {
                    id: send_id.clone(),
                    target: Some(agent.clone()),
                    verb: "send".to_owned(),
                    args: json!({"text":"fixture input"}),
                    expected_generation: Some(1),
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                },
                now_ms(),
            )
            .unwrap();
        store.dispatch(&send_id, now_ms()).unwrap();
        read_fixture_event(
            store,
            &agent,
            "turn.started",
            json!({"command_id":send_id, "turn_id":"turn-1"}),
        );
        (agent, send_id)
    }

    #[tokio::test]
    async fn typed_result_handlers_reject_malformed_params_before_lookup() {
        let router = router();
        let cases = [
            ("command.get", json!({})),
            ("command.get", json!({"id":null})),
            ("command.get", json!({"id":false})),
            ("command.get", json!({"id":" \t"})),
            ("command.get", json!({"id":"missing", "extra":0})),
            ("turn.get", json!({"agent":"a1", "turn_id":"turn-1"})),
            (
                "turn.get",
                json!({"agent":"a1", "generation":1, "turn_id":null}),
            ),
            (
                "turn.get",
                json!({"agent":"a1", "generation":1, "turn_id":" \t"}),
            ),
            (
                "turn.get",
                json!({"agent":"a1", "generation":1, "turn_id":"turn-1", "extra":0}),
            ),
            ("agent.result", json!({"id":"a1", "turn_id":null})),
            ("agent.result", json!({"id":"a1", "turn_id":false})),
            ("agent.result", json!({"id":"a1", "turn_id":" \t"})),
            ("agent.result", json!({"id":"a1", "extra":0})),
        ];
        for (method, params) in cases {
            let error = router
                .handle(
                    method,
                    json!({
                        "_auth":{"kind":"owner", "credential":"owner-credential"},
                        "_payload":params,
                    }),
                )
                .await
                .unwrap_err();
            assert_eq!(error.code, "invalid", "{method}: {params}");
        }
        for generation in [
            json!(null),
            json!(-1),
            json!(1.0),
            json!("1"),
            json!(true),
            json!(u64::MAX),
        ] {
            for method in ["turn.get", "agent.result"] {
                let params = if method == "turn.get" {
                    json!({"agent":"a1", "generation":generation, "turn_id":"turn-1"})
                } else {
                    json!({"id":"a1", "generation":generation})
                };
                let result = if method == "turn.get" {
                    router.turn_get(&params, &Principal::Owner).await
                } else {
                    router.agent_result(&params, &Principal::Owner).await
                };
                assert_eq!(result.unwrap_err().code, "invalid");
            }
        }
        for limit in [
            json!(null),
            json!(0),
            json!(1001),
            json!(-1),
            json!(1.0),
            json!("1"),
            json!(false),
        ] {
            let error = router
                .agent_result(
                    &json!({"id":"a1", "turn_id":"turn-1", "limit":limit}),
                    &Principal::Owner,
                )
                .await
                .unwrap_err();
            assert_eq!(error.code, "invalid", "exact-turn limit: {limit}");
        }
    }

    #[tokio::test]
    async fn typed_result_reads_preserve_historical_generation_and_principal_scope() {
        let router = router();
        let (agent, send_id) = {
            let mut store = router.inner.store.lock().await;
            let (agent, send_id) = result_fixture(&mut store);
            read_fixture_event(
                &mut store,
                &agent,
                "turn.completed",
                json!({"command_id":send_id, "turn_id":"turn-1", "status":"completed"}),
            );
            read_fixture_event(&mut store, &agent, "session.ended", json!({}));
            let resume_id = ulid::Ulid::new().to_string();
            store
                .submit(
                    "owner",
                    SubmitCommand {
                        id: resume_id.clone(),
                        target: Some(agent.clone()),
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
            // Store-only generation transition; no Router dispatch or process I/O.
            store.dispatch(&resume_id, now_ms()).unwrap();
            (agent, send_id)
        };
        let historical = json!({"id":agent, "generation":1, "turn_id":"turn-1"});
        let result: AgentResultResponse = serde_json::from_value(
            router
                .agent_result(&historical, &Principal::Owner)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(result.agent, agent);
        assert_eq!(result.generation, 1);
        assert_eq!(result.agent_metadata.generation, 2);
        assert_eq!(result.turns[0].state, TurnState::Completed);
        let turn_params = json!({"agent":agent, "generation":1, "turn_id":"turn-1"});
        let turn: Turn = serde_json::from_value(
            router
                .turn_get(&turn_params, &Principal::Owner)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(turn, result.turns[0]);
        let current = router
            .agent_result(&json!({"id":agent}), &Principal::Owner)
            .await
            .unwrap();
        assert_eq!(current["generation"], json!(2));
        assert_eq!(current["agent_metadata"]["generation"], json!(2));
        assert_eq!(current["turns"], json!([]));

        let current_principal = Principal::Agent {
            id: agent.clone(),
            generation: 2,
        };
        assert_eq!(
            router
                .agent_result(&historical, &current_principal)
                .await
                .unwrap_err()
                .code,
            "not_allowed"
        );
        assert_eq!(
            router
                .turn_get(&turn_params, &current_principal)
                .await
                .unwrap_err()
                .code,
            "not_allowed"
        );
        assert_eq!(
            router
                .command_get(&json!({"id":send_id}), &current_principal)
                .await
                .unwrap_err()
                .code,
            "not_allowed"
        );
        let historical_principal = Principal::Agent {
            id: agent.clone(),
            generation: 1,
        };
        assert!(
            router
                .agent_result(&historical, &historical_principal)
                .await
                .is_ok()
        );
        assert!(
            router
                .turn_get(&turn_params, &historical_principal)
                .await
                .is_ok()
        );
        assert!(
            router
                .command_get(&json!({"id":send_id}), &historical_principal)
                .await
                .is_ok()
        );
        let other_principal = Principal::Agent {
            id: "a999".to_owned(),
            generation: 1,
        };
        assert_eq!(
            router
                .agent_result(&historical, &other_principal)
                .await
                .unwrap_err()
                .code,
            "not_allowed"
        );
    }

    #[tokio::test]
    async fn typed_result_reads_retain_unknown_masks_and_opaque_outcome() {
        let router = router();
        let (agent, interrupt_id) = {
            let mut store = router.inner.store.lock().await;
            let (agent, _) = result_fixture(&mut store);
            let interrupt_id = ulid::Ulid::new().to_string();
            store
                .submit(
                    "owner",
                    SubmitCommand {
                        id: interrupt_id.clone(),
                        target: Some(agent.clone()),
                        verb: "interrupt".to_owned(),
                        args: json!({}),
                        expected_generation: Some(1),
                        expected_turn_id: Some("turn-1".to_owned()),
                        request_id: None,
                        probe: false,
                    },
                    now_ms(),
                )
                .unwrap();
            store.dispatch(&interrupt_id, now_ms()).unwrap();
            (agent, interrupt_id)
        };
        router.note_unknown_id(&interrupt_id, "fixture_transport_loss");
        let command: Command = serde_json::from_value(
            router
                .command_get(&json!({"id":interrupt_id}), &Principal::Owner)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(command.state, CommandState::Unknown);
        assert_eq!(command.turn_id, None);
        assert_eq!(
            command.outcome.unwrap()["cause"],
            json!("fixture_transport_loss")
        );
        let turn: Turn = serde_json::from_value(
            router
                .turn_get(
                    &json!({"agent":agent, "generation":1, "turn_id":"turn-1"}),
                    &Principal::Owner,
                )
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(turn.state, TurnState::Unknown);
        assert_eq!(turn.status, None);
        let result: AgentResultResponse = serde_json::from_value(
            router
                .agent_result(&json!({"id":agent}), &Principal::Owner)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(result.turns, vec![turn]);

        let retained = {
            let mut store = router.inner.store.lock().await;
            assert_eq!(
                store.get_turn(&agent, 1, "turn-1").unwrap().unwrap().state,
                TurnState::Running
            );
            assert_eq!(
                store.get_command(&interrupt_id).unwrap().unwrap().state,
                CommandState::Dispatched
            );
            let outcome = json!({"opaque":{"nested":[1, false, null]}, "diagnostics":[{
                "source":"plugin", "snapshot":{"text":"safe tail", "truncated":false, "eof":true, "read_error":false}
            }]});
            store
                .settle(
                    &interrupt_id,
                    CommandState::Unknown,
                    Some(outcome),
                    now_ms(),
                )
                .unwrap()
                .value
        };
        let observed: Command = serde_json::from_value(
            router
                .command_get(&json!({"id":interrupt_id}), &Principal::Owner)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(observed.outcome, retained.outcome);
        assert_eq!(observed.state, CommandState::Unknown);
        assert_eq!(observed.turn_id, None);
    }

    #[tokio::test]
    async fn typed_event_handlers_reject_malformed_params() {
        let router = router();
        let malformed = [
            json!({"after":null}),
            json!({"after":1.0}),
            json!({"after":"0"}),
            json!({"after":false}),
            json!({"after":-1}),
            json!({"after":u64::MAX}),
            json!({"limit":null}),
            json!({"limit":1.0}),
            json!({"limit":false}),
            json!({"limit":0}),
            json!({"limit":1001}),
            json!({"filter":null}),
            json!({"filter":false}),
            json!({"filter":{"unknown":[]}}),
            json!({"filter":{"agents":null}}),
            json!({"filter":{"agents":"a1"}}),
            json!({"filter":{"kinds":[false]}}),
            json!({"filter":{"sources":[""]}}),
            json!({"filter":{"agents":vec!["a1"; 1001]}}),
            json!({"unknown":0}),
        ];
        for method in ["events.read", "events.subscribe"] {
            for params in &malformed {
                let error = router
                    .handle(
                        method,
                        json!({
                            "_auth":{"kind":"owner", "credential":"owner-credential"},
                            "_payload":params,
                        }),
                    )
                    .await
                    .unwrap_err();
                assert_eq!(error.code, "invalid", "{method}: {params}");
            }
        }
        for wait_ms in [json!(null), json!(-1), json!(1.0), json!("0"), json!(false)] {
            let error = router
                .events_subscribe(&json!({"wait_ms":wait_ms}), &Principal::Owner)
                .await
                .unwrap_err();
            assert_eq!(error.code, "invalid");
        }
    }

    #[tokio::test]
    async fn typed_events_read_preserves_owner_and_agent_filter_scope() {
        let router = router();
        let (first, second) = {
            let mut store = router.inner.store.lock().await;
            (admit(&mut store), admit(&mut store))
        };
        let agent = first[0].agent.as_ref().unwrap().clone();
        let other = second[0].agent.as_ref().unwrap().clone();
        let principal = Principal::Agent {
            id: agent.clone(),
            generation: 0,
        };
        let all = router
            .events_read(&json!({}), &Principal::Owner)
            .await
            .unwrap();
        assert_eq!(
            all["events"].as_array().unwrap().len(),
            first.len() + second.len()
        );
        assert_eq!(all["next"], all["high_water"]);
        assert_eq!(all["truncated"], json!(false));

        for filter in [json!({}), json!({"agents":[agent, other]})] {
            let response = router
                .events_read(&json!({"filter":filter}), &principal)
                .await
                .unwrap();
            let events: Vec<Event> = serde_json::from_value(response["events"].clone()).unwrap();
            assert_eq!(events, first);
            assert_eq!(response["next"], all["high_water"]);
        }
        let error = router
            .events_read(&json!({"filter":{"agents":[other]}}), &principal)
            .await
            .unwrap_err();
        assert_eq!(error.code, "not_allowed");

        // Noncanonical nonempty Agent filter strings keep their old no-match behavior.
        let response = router
            .events_read(
                &json!({"filter":{"agents":["not-a-canonical-agent"]}}),
                &Principal::Owner,
            )
            .await
            .unwrap();
        assert_eq!(response["events"], json!([]));
        assert_eq!(response["next"], all["high_water"]);

        let response = router
            .events_subscribe(&json!({"limit":1, "wait_ms":u64::MAX}), &Principal::Owner)
            .await
            .unwrap();
        assert_eq!(response["events"].as_array().unwrap().len(), 1);
        assert_eq!(response["lagged"], json!(false));
    }

    #[test]
    fn typed_event_filter_checks_raw_bounds_before_deduplication() {
        let error = authorize_event_filter(
            EventsFilter {
                agents: vec!["a1".to_owned(); 1001],
                kinds: Vec::new(),
                sources: Vec::new(),
            },
            &Principal::Owner,
        )
        .err()
        .unwrap();
        assert_eq!(error.code, "invalid");
    }

    #[tokio::test]
    async fn typed_events_subscribe_replays_and_buffers_late_commits_once() {
        let router = router();
        let params = json!({"limit":2, "wait_ms":1000, "filter":{"kinds":["command.accepted"]}});
        let mut store = router.inner.store.lock().await;
        let subscription = router.events_subscribe(&params, &Principal::Owner);
        tokio::pin!(subscription);
        // Registration happens before the initial Store/high-water read can proceed.
        assert!(futures_util::poll!(&mut subscription).is_pending());
        let first = admit(&mut store);
        router.inner.bus.publish(&first);
        drop(store);
        // Replay consumes this commit; the buffered copy must not duplicate it.
        assert!(futures_util::poll!(&mut subscription).is_pending());
        // A waiting subscription must release the Store lock for the next commit.
        let mut store = timeout_at(
            Instant::now() + Duration::from_secs(1),
            router.inner.store.lock(),
        )
        .await
        .unwrap();
        let second = admit(&mut store);
        drop(store);
        router.inner.bus.publish(&second);
        let response = timeout_at(Instant::now() + Duration::from_secs(1), subscription)
            .await
            .unwrap()
            .unwrap();
        let expected: Vec<_> = first
            .into_iter()
            .chain(second)
            .filter(|event| event.kind == "command.accepted")
            .collect();
        let events: Vec<Event> = serde_json::from_value(response["events"].clone()).unwrap();
        assert_eq!(events, expected);
        assert_eq!(response["next"], json!(expected.last().unwrap().seq));
        assert_eq!(response["high_water"], response["next"]);
        assert_eq!(response["lagged"], json!(false));
        assert_eq!(response["truncated"], json!(false));
    }
}

#[cfg(test)]
mod run_authorization_tests {
    use super::*;

    fn command(id: &str) -> Command {
        Command {
            id: id.to_owned(),
            principal: "project:sample".to_owned(),
            method: "command.submit".to_owned(),
            verb: "run".to_owned(),
            target: None,
            expected_generation: None,
            generation: None,
            args: json!({}),
            state: CommandState::Dispatched,
            turn_id: None,
            request_id: None,
            outcome: None,
            created_at: 0,
            updated_at: 0,
            probe: false,
        }
    }

    fn run_principal() -> Principal {
        Principal::Run {
            run_command_id: "run-1".to_owned(),
        }
    }

    #[test]
    fn run_auth_envelope_requires_exact_run_fields() {
        let exact = json!({
            "kind": "run",
            "run_command_id": "run-1",
            "credential": "opaque",
        })
        .as_object()
        .expect("object")
        .clone();
        assert!(auth_fields_are_exact(&exact, "run"));
        let mut conflicting = exact;
        conflicting.insert("agent_id".to_owned(), json!("a1"));
        assert!(!auth_fields_are_exact(&conflicting, "run"));
        assert!(!auth_fields_are_exact(&conflicting, "owner"));
    }

    #[test]
    fn targetless_run_is_project_only_and_requires_the_run_grant() {
        let registration = ProjectRegistrationView {
            id: "sample".to_owned(),
            checkout: "C:\\work\\sample".to_owned(),
            repo_common_dir: "C:\\work".to_owned(),
            profiles: Vec::new(),
            actions: vec!["run".to_owned()],
            interpreters: Default::default(),
            run_env: Vec::new(),
            revision: 1,
        };
        let project = Principal::ProjectRule { registration };
        let mut request = SubmitCommand::new("", "run");
        assert!(authorize_command(&project, &mut request).is_ok());
        request.target = Some("a1".to_owned());
        assert_eq!(
            authorize_command(&project, &mut request).unwrap_err().code,
            "invalid"
        );

        let mut owner_request = SubmitCommand::new("", "run");
        assert_eq!(
            authorize_command(&Principal::Owner, &mut owner_request)
                .unwrap_err()
                .code,
            "unsupported"
        );
        let mut run_request = SubmitCommand::new("", "send");
        assert!(authorize_command(&run_principal(), &mut run_request).is_err());
    }
    #[tokio::test]
    async fn force_joins_active_run_worker_and_rejects_post_force_output() {
        let store = Arc::new(AsyncMutex::new(
            eliot_store::Store::open(std::path::Path::new(":memory:")).expect("open test Store"),
        ));
        let router = Router::new(
            PathBuf::from("."),
            "owner-token".to_owned(),
            store,
            Bus::new(8),
        );
        let run_id = ulid::Ulid::new().to_string();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let (completed_tx, completed_rx) = watch::channel(false);
        let cleanup_completed = Arc::new(AtomicBool::new(false));
        let worker_cleanup_completed = Arc::clone(&cleanup_completed);
        let worker_router = Arc::downgrade(&router.inner);
        let worker_id = run_id.clone();
        let task = tokio::spawn(async move {
            let mut completion = RunWorkerCompletion {
                router: worker_router,
                run_id: worker_id,
                completed: completed_tx,
                finished: false,
            };
            let _ = release_rx.await;
            worker_cleanup_completed.store(true, Ordering::SeqCst);
            completion.finish();
        });
        router.inner.run_workers.lock_recover().insert(
            run_id.clone(),
            RunWorkerHandle {
                task,
                completed: completed_rx,
            },
        );

        let mut cancellation = router.run_cancellation_receiver();
        let force_router = router.clone();
        let mut force = tokio::spawn(async move {
            force_router.force_owned_processes().await;
        });
        timeout_at(
            Instant::now() + Duration::from_secs(2),
            cancellation.changed(),
        )
        .await
        .expect("Force requested")
        .expect("run cancellation channel remains open");
        assert!(*cancellation.borrow_and_update());
        assert_eq!(
            router.start_run_worker(command("run-late")),
            RunWorkerStart::ForceRequested
        );

        let before = router
            .inner
            .store
            .lock()
            .await
            .high_water()
            .expect("read pre-admission high-water");
        let rejected = router
            .admit_script_output(
                &run_id,
                Vec::new(),
                crate::scripts::unobserved_process_result(false),
            )
            .await
            .expect_err("Force must close script output admission");
        assert_eq!(rejected.code, "not_allowed");
        let after = router
            .inner
            .store
            .lock()
            .await
            .high_water()
            .expect("read post-admission high-water");
        assert_eq!(after, before, "force admitted no script child events");
        assert!(!cleanup_completed.load(Ordering::SeqCst));
        assert!(
            timeout_at(Instant::now() + Duration::from_millis(50), &mut force)
                .await
                .is_err(),
            "Force must wait for the registered Run worker"
        );

        release_tx.send(()).expect("release Run worker cleanup");
        timeout_at(Instant::now() + Duration::from_secs(2), &mut force)
            .await
            .expect("Force joins the completed Run worker")
            .expect("Force worker task completes");
        assert!(cleanup_completed.load(Ordering::SeqCst));
        assert_eq!(
            router.start_run_worker(command("run-late")),
            RunWorkerStart::ForceRequested
        );
        assert!(router.inner.run_workers.lock_recover().is_empty());
    }
    #[test]
    fn run_rpc_is_limited_to_own_command_get() {
        let principal = run_principal();
        assert!(authorize_rpc_method(&principal, "command.get").is_ok());
        assert!(authorize_rpc_method(&principal, "command.submit").is_err());
        assert!(authorize_command_read(&principal, &command("run-1")).is_ok());
        assert!(authorize_command_read(&principal, &command("run-2")).is_err());
    }
}
