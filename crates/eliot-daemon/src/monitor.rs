//! OS facts and ephemeral counters, independent of the native lifecycle source.
#[cfg(target_os = "linux")]
use crate::platform::unix::{LinuxProcessSweep, enumerate_process_sweep};
use crate::{
    bus::Bus,
    common::now_ms,
    fs_invalidation::WorkspaceInvalidation,
    git_monitor::{GitProbe, WorktreeCapture, normalize_hint, worktree_capture_from_snapshot},
    platform::CustodyObserver,
    plugin_host::SecretSet,
};
use eliot_core::{
    Agent, Error, FileChangeHint, GitBaseline, GitHintPage, GitSnapshot, ProcessIdentity,
    ProcessRecord, ProcessSample, ProcessSnapshot, ProcessState, Result, SourceGap,
    WorkspacePreparation,
    api::{
        ChildRunLineCounts, FilesystemInvalidationState, FilesystemInvalidationView,
        GitObservation, GitView, LivenessBasis, LivenessView, MonitoringSourceState,
        ObservationData, ObservationView, ProcessStatsView, SourceState, StatsCoverage,
        WorkspaceObservation, WorkspaceView,
    },
};
use eliot_plugin::sync::LockExt;
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{Mutex as AsyncMutex, watch},
    task::JoinSet,
    time::{Instant, MissedTickBehavior, interval, sleep_until, timeout_at},
};

const SAMPLE_INTERVAL: Duration = Duration::from_secs(2);
const SAMPLE_TTL_MS: i64 = 4_000;
const PAGE_LIMIT: usize = 1_000;
/// Maximum Git bindings snapshotted under one Store mutex acquisition.
/// Each binding costs a handful of small reads plus one bounded hint page
/// (at most `PAGE_LIMIT` entries); chunking keeps that work bounded while
/// filesystem and native probe work stays outside the lock.
const GIT_INPUTS_PER_SNAPSHOT: usize = 32;
const GIT_MAX_BACKOFF: Duration = Duration::from_secs(30);
const GIT_SHUTDOWN_GRACE: Duration = Duration::from_secs(20);
const WATCH_RETRY: Duration = Duration::from_secs(30);

type Key = (String, i64);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct WorkspaceProcessContext {
    pub agent: String,
    pub target_generation: i64,
    pub command_id: String,
    pub role: &'static str,
    pub setup_index: Option<usize>,
}

impl WorkspaceProcessContext {
    pub(crate) fn new(
        preparation: &WorkspacePreparation,
        command_id: &str,
        role: &'static str,
        setup_index: Option<usize>,
    ) -> Self {
        Self {
            agent: preparation.agent.clone(),
            target_generation: preparation.target_generation,
            command_id: command_id.to_owned(),
            role,
            setup_index,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum ProcessKey {
    Agent(String, i64),
    Workspace(WorkspaceProcessContext),
}

#[derive(Clone)]
pub(crate) struct WorkspaceProcessHandle {
    target: Arc<Target>,
}

#[derive(Clone)]
pub(crate) struct Monitoring {
    inner: Arc<State>,
}

struct State {
    store: Arc<AsyncMutex<eliot_store::Store>>,
    bus: Bus,
    secrets: SecretSet,
    targets: Mutex<HashMap<ProcessKey, Arc<Target>>>,
    unavailable: Mutex<Option<String>>,
    git_bindings: Mutex<HashMap<Key, String>>,
    git_workspaces: Mutex<HashMap<String, Arc<GitWorkspace>>>,
    git_unavailable: Mutex<Option<String>>,
    observe: Mutex<ObserveThresholds>,
}

/// Observe thresholds carried from `[daemon]` config and installed once at
/// startup. Defaults match the core constants; the daemon never re-reads
/// the config file per sweep.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ObserveThresholds {
    pub heavy: eliot_core::HeavyThresholds,
    pub growth: eliot_core::SessionGrowthThresholds,
}

/// In-memory observe state for one target: open heavy episodes, warned
/// growth bands, committed episode keys and the cached harness name.
/// Episodes are keyed by process identity (boot id, pid, start time) so PID
/// reuse never merges two episodes.
#[derive(Debug, Default)]
struct ObserveTracker {
    episodes: HashMap<String, OpenHeavyEpisode>,
    warned_growth: HashSet<&'static str>,
    emitted_heavy: HashSet<String>,
    harness: Option<String>,
}

#[derive(Debug, Default)]
struct OpenHeavyEpisode {
    category: String,
    category_source: String,
    start_ms: i64,
    last_ms: i64,
    first_cpu_ns: Option<u64>,
    last_cpu_ns: Option<u64>,
    peak_rss_bytes: u64,
    exit_code: Option<i32>,
    exited: bool,
    misses: u8,
}

struct Target {
    key: ProcessKey,
    agent: String,
    generation: i64,
    context_ceiling_tokens: Option<i64>,
    owner: String,
    observer: CustodyObserver,
    sampling: AsyncMutex<()>,
    cache: Mutex<Cache>,
    workspace: Option<WorkspaceProcessContext>,
    observe: Mutex<ObserveTracker>,
}

#[derive(Clone, Default)]
struct Cache {
    snapshot: Option<ProcessSnapshot>,
    sampled_at: Option<i64>,
    error: Option<String>,
}

#[derive(Default)]
struct GitWorkspace {
    probe: AsyncMutex<Option<GitProbe>>,
    cache: Mutex<GitCache>,
    watch: Mutex<WatchSlot>,
}

struct WatchSlot {
    watcher: Option<WorkspaceInvalidation>,
    pending: Option<std::sync::mpsc::Receiver<Result<WorkspaceInvalidation>>>,
    retry_at: Instant,
    active: bool,
}

impl Default for WatchSlot {
    fn default() -> Self {
        Self {
            watcher: None,
            pending: None,
            retry_at: Instant::now(),
            active: false,
        }
    }
}

impl WatchSlot {
    fn view(&self) -> FilesystemInvalidationView {
        let state = match (self.active, self.watcher.as_ref()) {
            (false, _) => FilesystemInvalidationState::Inactive,
            (true, Some(watcher)) if watcher.is_degraded() => FilesystemInvalidationState::Degraded,
            (true, Some(_)) => FilesystemInvalidationState::Healthy,
            (true, None) => FilesystemInvalidationState::Unavailable,
        };
        FilesystemInvalidationView {
            state,
            retry_after_ms: Some(
                (self.pending.is_none()
                    && matches!(
                        state,
                        FilesystemInvalidationState::Degraded
                            | FilesystemInvalidationState::Unavailable
                    ))
                .then(|| {
                    self.retry_at
                        .saturating_duration_since(Instant::now())
                        .as_millis()
                }),
            ),
        }
    }
}

#[derive(Clone)]
struct GitCache {
    snapshot: Option<GitSnapshot>,
    error: Option<String>,
    last_attempt: Option<Instant>,
    next_attempt: Instant,
    backoff: Duration,
}

impl Default for GitCache {
    fn default() -> Self {
        Self {
            snapshot: None,
            error: None,
            last_attempt: None,
            next_attempt: Instant::now(),
            backoff: SAMPLE_INTERVAL,
        }
    }
}

struct GitBindingInput {
    key: Key,
    terminal: bool,
    native_closed: bool,
    cursor: u64,
    page: GitHintPage,
    barrier: u64,
}

struct GitGroup {
    baseline: Option<GitBaseline>,
    base_oid: Option<String>,
    inputs: Vec<GitBindingInput>,
    edited_paths: Vec<String>,
    paths_complete: bool,
}

impl Monitoring {
    pub(crate) fn new(
        store: Arc<AsyncMutex<eliot_store::Store>>,
        bus: Bus,
        secrets: SecretSet,
    ) -> Self {
        Self {
            inner: Arc::new(State {
                store,
                bus,
                secrets,
                targets: Mutex::new(HashMap::new()),
                unavailable: Mutex::new(None),
                git_bindings: Mutex::new(HashMap::new()),
                git_workspaces: Mutex::new(HashMap::new()),
                git_unavailable: Mutex::new(None),
                observe: Mutex::new(ObserveThresholds::default()),
            }),
        }
    }

    pub(crate) fn mark_unavailable(&self, code: &str) {
        *self.inner.unavailable.lock_recover() = Some(code.to_owned());
        self.mark_git_unavailable(code);
    }

    fn mark_available(&self) {
        *self.inner.unavailable.lock_recover() = None;
    }

    fn mark_git_available(&self) {
        *self.inner.git_unavailable.lock_recover() = None;
    }

    /// Install the [daemon] observe thresholds once at startup. Sweeps read
    /// them from memory; the daemon never re-reads the config file per sweep.
    pub(crate) fn set_observe_thresholds(&self, thresholds: ObserveThresholds) {
        *self.inner.observe.lock_recover() = thresholds;
    }

    /// An owned observer reader has OS custody but no native control token or
    /// admitted workspace. Do not install Git observation for its external subject.
    pub(crate) async fn prepare_observer_generation(&self, agent: &Agent) -> Result<()> {
        let mut store = self.inner.store.lock().await;
        let events = store.ensure_process_source(&agent.id, agent.generation, now_ms())?;
        self.inner.bus.publish(&events);
        Ok(())
    }

    pub(crate) async fn prepare_generation(&self, agent: &Agent) -> Result<()> {
        // Router admission already binds the canonical workspace, including on resume.
        let workspace = agent.workspace.as_deref().ok_or_else(|| {
            Error::new("invalid", "Git observation requires an admitted workspace")
        })?;
        let mut store = self.inner.store.lock().await;
        let events = store.ensure_process_source(&agent.id, agent.generation, now_ms())?;
        self.inner.bus.publish(&events);
        let events = store.ensure_git_source(&agent.id, agent.generation, workspace, now_ms())?;
        self.inner.bus.publish(&events);
        drop(store);
        // Registration and retirement take these locks in the same order. Keep
        // the binding visible until its cache slot is installed as one operation.
        let mut bindings = self.inner.git_bindings.lock_recover();
        bindings.insert((agent.id.clone(), agent.generation), workspace.to_owned());
        self.inner
            .git_workspaces
            .lock_recover()
            .entry(workspace.to_owned())
            .or_insert_with(|| Arc::new(GitWorkspace::default()));
        Ok(())
    }

    pub(crate) async fn attach(&self, agent: &Agent, observer: CustodyObserver) -> Result<()> {
        let key = ProcessKey::Agent(agent.id.clone(), agent.generation);
        let target = Arc::new(Target {
            key: key.clone(),
            agent: agent.id.clone(),
            generation: agent.generation,
            context_ceiling_tokens: agent
                .profile_snapshot
                .get("context_ceiling_tokens")
                .and_then(Value::as_i64)
                .filter(|ceiling| *ceiling > 0),
            owner: owner(&agent.id, agent.generation),
            observer,
            sampling: AsyncMutex::new(()),
            cache: Mutex::new(Cache::default()),
            workspace: None,
            observe: Mutex::new(ObserveTracker::default()),
        });
        {
            let mut targets = self.inner.targets.lock_recover();
            if targets.contains_key(&key) {
                return Err(Error::new(
                    "bad_state",
                    "process observation is already attached",
                ));
            }
            targets.insert(key, Arc::clone(&target));
        }
        // A failed read-only probe leaves unknown observation; it does not fail native startup.
        // Controlled startup holds its cleanup gate here. Bound this initial
        // sample, including its Store wait, by the existing sampling cadence.
        // The target stays registered; a timed-out blocking read has no native
        // effect, and its dropped future cannot later commit a stale snapshot.
        let _ = timeout_at(
            Instant::now() + SAMPLE_INTERVAL,
            self.sample_target(&target),
        )
        .await;
        Ok(())
    }

    /// Bind the frozen target before its mutator spawns, without creating an Agent
    /// generation/token or installing a native lifecycle source.
    pub(crate) async fn prepare_workspace_process(
        &self,
        context: &WorkspaceProcessContext,
    ) -> Result<()> {
        let mut store = self.inner.store.lock().await;
        let events = store.ensure_workspace_process_source(
            &context.agent,
            context.target_generation,
            now_ms(),
        )?;
        self.inner.bus.publish(&events);
        Ok(())
    }

    pub(crate) async fn workspace_process_unavailable(
        &self,
        context: &WorkspaceProcessContext,
    ) -> Result<()> {
        let mut store = self.inner.store.lock().await;
        let events = store.set_workspace_process_source_status(
            &context.agent,
            context.target_generation,
            "error",
            false,
            now_ms(),
        )?;
        self.inner.bus.publish(&events);
        Ok(())
    }

    pub(crate) async fn attach_workspace_process(
        &self,
        context: WorkspaceProcessContext,
        observer: CustodyObserver,
    ) -> Result<WorkspaceProcessHandle> {
        let key = ProcessKey::Workspace(context.clone());
        let target = Arc::new(Target {
            key: key.clone(),
            agent: context.agent.clone(),
            generation: context.target_generation,
            context_ceiling_tokens: None,
            owner: workspace_owner(&context.agent, context.target_generation),
            observer,
            sampling: AsyncMutex::new(()),
            cache: Mutex::new(Cache::default()),
            workspace: Some(context),
            observe: Mutex::new(ObserveTracker::default()),
        });
        {
            let mut targets = self.inner.targets.lock_recover();
            if targets.contains_key(&key) {
                return Err(Error::new(
                    "bad_state",
                    "workspace process invocation is already observed",
                ));
            }
            targets.insert(key, Arc::clone(&target));
        }
        // Initial sampling is an edge, not just a timer: short setup processes
        // must remain visible even if they exit before the next periodic tick.
        let _ = self.sample_target(&target).await;
        Ok(WorkspaceProcessHandle { target })
    }

    pub(crate) async fn finish_workspace_process(
        &self,
        handle: &WorkspaceProcessHandle,
    ) -> Result<()> {
        {
            let mut targets = self.inner.targets.lock_recover();
            match targets.get(&handle.target.key) {
                Some(current) if !Arc::ptr_eq(current, &handle.target) => {
                    return Err(Error::new(
                        "bad_state",
                        "workspace observer invocation changed",
                    ));
                }
                Some(_) => {}
                None => {
                    // A timer may have seen departure before wait() supplied its
                    // exact leader exit code. Preserve that final same-identity update.
                    targets.insert(handle.target.key.clone(), Arc::clone(&handle.target));
                }
            }
        }
        self.sample_target(&handle.target).await
    }

    pub(crate) async fn process_put(
        &self,
        agent: &str,
        generation: i64,
        pid: u32,
        role: &str,
    ) -> Result<Value> {
        if pid == 0 || role != "harness" {
            return Err(Error::new(
                "invalid",
                "process.put requires a nonzero harness PID",
            ));
        }
        let target = self.target(agent, generation).ok_or_else(|| {
            Error::new(
                "stale_generation",
                "process observation is not attached for this generation",
            )
        })?;
        let _sampling = target.sampling.lock().await;
        let observer = target.observer.clone();
        let process_owner = target.owner.clone();
        let sampled_at = now_ms();
        let sample = tokio::task::spawn_blocking(move || {
            observer.register_pid(pid, "harness", &process_owner, sampled_at)
        })
        .await
        .map_err(|_| Error::new("process_observation", "process verification task failed"))??;
        if sample.record.owner != target.owner
            || sample.record.identity.pid != pid
            || sample.record.identity.host_boot_id.is_empty()
            || sample.record.identity.start_time == 0
            || sample.record.role != "harness"
            || sample.record.state != ProcessState::Running
            || sample.record.exit_code.is_some()
        {
            return Err(Error::new(
                "not_allowed",
                "OS observation did not verify a live harness process",
            ));
        }
        {
            let mut store = self.inner.store.lock().await;
            let current = store
                .get_agent(agent)?
                .ok_or_else(|| Error::new("not_found", "process.put Agent was not found"))?;
            if current.generation != generation
                || current.is_terminal()
                || store.get_agent_token(agent, generation)?.is_none()
            {
                return Err(Error::new(
                    "stale_generation",
                    "process.put generation is no longer controlled",
                ));
            }
            let events = store.observe_processes(
                agent,
                generation,
                std::slice::from_ref(&sample.record),
                now_ms(),
            )?;
            self.inner.bus.publish(&events);
        }
        serde_json::to_value(sample.record)
            .map_err(|_| Error::new("internal", "process acknowledgement could not be encoded"))
    }

    /// Sample an exiting root before its native stdout is drained. Only the OS
    /// observer determines process facts; failed probes never synthesize an exit.
    pub(crate) async fn sample_generation(&self, agent: &Agent) -> Result<()> {
        match self.target(&agent.id, agent.generation) {
            Some(target) => self.sample_target(&target).await,
            None => Ok(()),
        }
    }

    pub(crate) async fn view(&self, agent: &Agent) -> Result<ObservationView> {
        let cache = self
            .target(&agent.id, agent.generation)
            .map(|target| target.cache.lock_recover().clone())
            .unwrap_or_default();
        let unavailable = self.inner.unavailable.lock_recover().clone();
        let (mut processes, last_model_progress, source_healthy) = {
            let store = self.inner.store.lock().await;
            let processes =
                store.list_processes(&owner(&agent.id, agent.generation), None, PAGE_LIMIT)?;
            let progress = store.last_model_progress(&agent.id, agent.generation)?;
            let healthy = store
                .list_source_states(&agent.id, agent.generation)
                .ok()
                .and_then(native_sources_healthy);
            (processes, progress, healthy)
        };
        refresh_process_timestamps(&mut processes, &cache);
        let now = now_ms();
        let model_clock_regressed = last_model_progress.is_some_and(|at| at > now);
        let workspace = match self.workspace_view(agent).await {
            Ok(workspace) => workspace,
            Err(error) => Some(WorkspaceView::unavailable(error.code)),
        };
        let git = match self.git_view(agent).await {
            Ok(git) => git,
            Err(error) => GitView::error(error.code),
        };
        Ok(ObservationView::Data(Box::new(ObservationData {
            processes_truncated: processes.len() == PAGE_LIMIT,
            processes,
            stats: stats_view(&cache, unavailable, now),
            liveness: LivenessView {
                state: agent.state,
                last_model_progress,
                age_ms: last_model_progress.and_then(|at| age_ms(now, at)),
                clock_regressed: model_clock_regressed,
                basis: LivenessBasis::CommittedNativeFacts,
                source_healthy,
            },
            workspace,
            git,
        })))
    }

    /// Agent.generation remains native/control state. Workspace observation is
    /// explicitly named by its retained target, including target 1 while Agent is 0.
    pub(crate) async fn workspace_view(&self, agent: &Agent) -> Result<Option<WorkspaceView>> {
        let (preparation, mut processes, source) = {
            let store = self.inner.store.lock().await;
            let Some(preparation) = store.get_workspace(&agent.id)? else {
                return Ok(None);
            };
            let process_owner = workspace_owner(&agent.id, preparation.target_generation);
            let processes = store.list_processes(&process_owner, None, PAGE_LIMIT)?;
            let source = store
                .list_source_states(&agent.id, preparation.target_generation)?
                .into_iter()
                .find(|state| {
                    state.get("source").and_then(Value::as_str) == Some("workspace_process")
                })
                .map(serde_json::from_value::<SourceState>)
                .transpose()
                .map_err(|_| Error::new("internal", "invalid workspace source-state projection"))?;
            (preparation, processes, source)
        };
        let cache = self.workspace_cache(&agent.id, preparation.target_generation);
        refresh_process_timestamps(&mut processes, &cache);
        let unavailable = self.inner.unavailable.lock_recover().clone();
        Ok(Some(WorkspaceView::Data(Box::new(WorkspaceObservation {
            owner: workspace_owner(&agent.id, preparation.target_generation),
            target_generation: preparation.target_generation,
            phase: preparation.phase,
            setup_index: preparation.setup_index,
            processes_truncated: processes.len() == PAGE_LIMIT,
            processes,
            stats: stats_view(&cache, unavailable, now_ms()),
            source,
        }))))
    }

    fn workspace_cache(&self, agent: &str, generation: i64) -> Cache {
        let targets: Vec<_> = self
            .inner
            .targets
            .lock_recover()
            .values()
            .filter(|target| {
                target.workspace.is_some()
                    && target.agent == agent
                    && target.generation == generation
            })
            .cloned()
            .collect();
        if targets.is_empty() {
            return Cache::default();
        }
        let mut snapshot = ProcessSnapshot {
            samples: Vec::new(),
            complete: true,
        };
        let mut sampled_at: Option<i64> = None;
        let mut error = None;
        let mut identities = HashSet::new();
        for target in targets {
            let cache = target.cache.lock_recover();
            if let Some(at) = cache.sampled_at {
                sampled_at = Some(sampled_at.map_or(at, |previous| previous.min(at)));
            }
            error = error.or_else(|| cache.error.clone());
            if let Some(current) = &cache.snapshot {
                snapshot.complete &= current.complete;
                for sample in &current.samples {
                    if identities.insert(sample.record.identity.clone())
                        && snapshot.samples.len() < PAGE_LIMIT
                    {
                        snapshot.samples.push(sample.clone());
                    } else {
                        snapshot.complete = false;
                    }
                }
            } else {
                snapshot.complete = false;
                error.get_or_insert_with(|| "workspace_process_unsampled".to_owned());
            }
        }
        Cache {
            snapshot: Some(snapshot),
            sampled_at,
            error,
        }
    }

    pub(crate) fn refresh_process_timestamps(
        &self,
        agent: &str,
        generation: i64,
        scope: &str,
        processes: &mut [ProcessRecord],
    ) {
        let cache = if scope == "workspace" {
            self.workspace_cache(agent, generation)
        } else {
            self.target(agent, generation)
                .map(|target| target.cache.lock_recover().clone())
                .unwrap_or_default()
        };
        refresh_process_timestamps(processes, &cache);
    }

    /// Reuse existing bounded work capture for no-policy completion. The rule
    /// launch boundary must fresh-probe and compare this candidate fingerprint.
    pub(crate) fn completion_worktree_capture(&self, agent: &Agent) -> Option<WorktreeCapture> {
        let workspace = agent.workspace.as_deref()?;
        let target = self
            .inner
            .git_workspaces
            .lock_recover()
            .get(workspace)
            .cloned()?;
        let cache = target.cache.lock_recover().clone();
        if cache.error.is_some() || self.inner.git_unavailable.lock_recover().is_some() {
            return None;
        }
        let snapshot = cache.snapshot.as_ref()?;
        if snapshot.workspace != workspace
            || agent.base_oid.as_deref() != snapshot.base_oid.as_deref()
        {
            return None;
        }
        worktree_capture_from_snapshot(snapshot).ok()
    }

    pub(crate) async fn git_view(&self, agent: &Agent) -> Result<GitView> {
        let workspace = agent.workspace.as_deref().ok_or_else(|| {
            Error::new("invalid", "Git observation requires an admitted workspace")
        })?;
        let target = self
            .inner
            .git_workspaces
            .lock_recover()
            .get(workspace)
            .cloned();
        let cache = target
            .as_ref()
            .map(|target| target.cache.lock_recover().clone())
            .unwrap_or_default();
        let invalidation = target
            .as_ref()
            .map(|target| target.watch.lock_recover().view())
            .unwrap_or_else(FilesystemInvalidationView::inactive);
        let unavailable = self.inner.git_unavailable.lock_recover().clone();
        let (baseline, edits, source) = {
            let store = self.inner.store.lock().await;
            (
                store.git_baseline(workspace)?,
                store.list_agent_edits(&agent.id, agent.generation, None, PAGE_LIMIT)?,
                store
                    .read_source_state(&agent.id, agent.generation, "git")
                    .ok()
                    .map(serde_json::from_value::<MonitoringSourceState>)
                    .transpose()
                    .map_err(|_| Error::new("internal", "invalid Git source-state projection"))?,
            )
        };
        let snapshot = cache.snapshot.as_ref();
        let sampled_at = snapshot.map(|snapshot| snapshot.observed_at);
        let now = now_ms();
        let age = sampled_at.and_then(|at| age_ms(now, at));
        let clock_regressed = sampled_at.is_some_and(|at| at > now);
        let stale = unavailable.is_some()
            || cache.error.is_some()
            || clock_regressed
            || age.is_some_and(|age| age > SAMPLE_TTL_MS);
        let value = GitView::Data(Box::new(GitObservation {
            available: snapshot.is_some() && !stale,
            workspace: workspace.to_owned(),
            observed_at: sampled_at,
            age_ms: age,
            ttl_ms: SAMPLE_TTL_MS,
            clock_regressed,
            retry_after_ms: cache
                .next_attempt
                .saturating_duration_since(Instant::now())
                .as_millis(),
            filesystem_invalidation: invalidation,
            head: snapshot.and_then(|snapshot| snapshot.head.clone()),
            branch: snapshot.and_then(|snapshot| snapshot.branch.clone()),
            dirty: snapshot.map(|snapshot| snapshot.dirty.clone()),
            commits: snapshot.map(|snapshot| snapshot.commits.clone()),
            complete: snapshot.map(|snapshot| snapshot.complete),
            history_complete: snapshot.map(|snapshot| snapshot.history_complete),
            history_gap: snapshot.map(|snapshot| snapshot.history_gap),
            base_oid: snapshot.and_then(|snapshot| snapshot.base_oid.clone()),
            committed_paths: snapshot.map(|snapshot| snapshot.committed_paths.clone()),
            base_complete: snapshot.map(|snapshot| snapshot.base_complete),
            line_counts: snapshot.and_then(|snapshot| {
                let work = snapshot.work.as_ref()?;
                if !work.available {
                    return None;
                }
                Some(ChildRunLineCounts {
                    files_changed: work.files_changed?,
                    insertions: work.insertions?,
                    deletions: work.deletions?,
                })
            }),
            baseline,
            edits_truncated: edits.len() == PAGE_LIMIT,
            edits,
            source,
            error: unavailable.or(cache.error),
        }));
        self.check_git_value(
            serde_json::to_value(&value)
                .map_err(|_| Error::new("internal", "cannot encode Git observation"))?,
        )?;
        Ok(value)
    }

    fn check_git_value(&self, mut value: Value) -> Result<()> {
        let secrets: Vec<_> = self.inner.secrets.lock_recover().iter().cloned().collect();
        if crate::redaction::redact_value(&mut value, &secrets) {
            return Err(Error::new(
                "secret_in_payload",
                "Git observation contains a known credential",
            ));
        }
        Ok(())
    }

    pub(crate) async fn run(
        self,
        mut shutdown: watch::Receiver<bool>,
        reporter: crate::daemon::workers::WorkerReporter,
    ) -> Result<()> {
        let mut git_tasks = JoinSet::new();
        let git_monitoring = self.clone();
        let git_worker_reporter = reporter.clone();
        let git_factory = move |git_shutdown: watch::Receiver<bool>| {
            let monitoring = git_monitoring.clone();
            let git_reporter = git_worker_reporter.clone();
            async move {
                let result = monitoring.run_git(git_shutdown.clone(), git_reporter).await;
                crate::daemon::workers::from_result(result, &git_shutdown)
            }
        };
        let git_failure_monitor = self.clone();
        git_tasks.spawn(crate::daemon::workers::supervise_with(
            "git_sampler",
            reporter.clone(),
            shutdown.clone(),
            git_factory,
            move |error| git_failure_monitor.mark_git_unavailable(&error.code),
        ));
        let mut ticks = interval(SAMPLE_INTERVAL);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
        reporter.running("monitor");
        loop {
            if *shutdown.borrow() {
                break;
            }
            tokio::select! {
                _ = ticks.tick() => {
                    self.sample_all().await;
                    self.mark_available();
                }
                joined = git_tasks.join_next(), if !git_tasks.is_empty() => {
                    if !*shutdown.borrow() {
                        let code = match joined {
                            Some(Err(_)) => "git_supervisor_failed",
                            Some(Ok(())) => "git_supervisor_stopped",
                            None => "git_supervisor_missing",
                        };
                        self.mark_git_unavailable(code);
                        return Err(Error::new(
                            code,
                            "Git sampler supervisor stopped unexpectedly",
                        ));
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
            }
        }
        self.sample_all().await;
        let joined = timeout_at(Instant::now() + GIT_SHUTDOWN_GRACE, async {
            while let Some(joined) = git_tasks.join_next().await {
                if !matches!(joined, Ok(())) {
                    self.mark_git_unavailable("git_task_failed");
                }
            }
        })
        .await;
        if joined.is_err() {
            git_tasks.abort_all();
            while git_tasks.join_next().await.is_some() {}
            self.mark_git_unavailable("git_shutdown_incomplete");
        }
        // Finish cursor/gap bookkeeping even when the Git task failed or was cancelled.
        self.finish_git_unavailable().await;
        self.release_inactive_watchers(&HashSet::new(), true);
        Ok(())
    }

    fn mark_git_unavailable(&self, code: &str) {
        *self.inner.git_unavailable.lock_recover() = Some(code.to_owned());
        self.release_inactive_watchers(&HashSet::new(), true);
    }

    async fn run_git(
        self,
        mut shutdown: watch::Receiver<bool>,
        reporter: crate::daemon::workers::WorkerReporter,
    ) -> Result<()> {
        let mut ticks = interval(SAMPLE_INTERVAL);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
        // A bound Agent's native stream closing makes its binding final:
        // read it now instead of at the next tick, so a stopped Agent's Git
        // source closes (and `agent.delete` stops answering busy) promptly.
        let mut committed = Some(self.inner.bus.subscribe());
        self.mark_git_available();
        reporter.running("git_sampler");
        loop {
            if *shutdown.borrow() {
                break;
            }
            tokio::select! {
                _ = ticks.tick() => self.sample_git(false).await,
                event = async { committed.as_mut().expect("guarded receiver").recv().await },
                    if committed.is_some() =>
                {
                    match event {
                        Ok(event) if self.closes_git_binding(&event) => self.sample_git(false).await,
                        Ok(_) => {}
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            self.sample_git(false).await;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => committed = None,
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
            }
        }
        self.sample_git(true).await;
        Ok(())
    }

    /// The committed `source.closed` of a Git-bound Agent's native stream.
    fn closes_git_binding(&self, event: &eliot_core::Event) -> bool {
        event.kind == "source.closed"
            && event.data.get("source").and_then(Value::as_str) == Some("stream")
            && event.agent.as_deref().is_some_and(|agent| {
                self.inner
                    .git_bindings
                    .lock_recover()
                    .keys()
                    .any(|(bound, generation)| {
                        bound == agent && event.generation.is_none_or(|g| g == *generation)
                    })
            })
    }

    async fn git_inputs(&self) -> Result<HashMap<String, GitGroup>> {
        let bindings = self.inner.git_bindings.lock_recover().clone();
        // Deterministic chunk order so the per-acquisition bound does not
        // depend on `HashMap` iteration order.
        let mut ordered: Vec<(Key, String)> = bindings.into_iter().collect();
        ordered.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));
        let mut groups: HashMap<String, GitGroup> = HashMap::new();
        let mut retired = Vec::new();
        // Bounded snapshot per Store mutex acquisition: each chunk shares one
        // high-water barrier, keeping generation/source/cursor fields exact
        // while filesystem and native probe work stays outside the lock.
        for chunk in ordered.chunks(GIT_INPUTS_PER_SNAPSHOT) {
            let store = self.inner.store.lock().await;
            let barrier = u64::try_from(store.high_water()?)
                .map_err(|_| Error::new("internal", "Git high-water is invalid"))?;
            for (key, workspace) in chunk {
                let Some(current) = store.get_agent(&key.0)? else {
                    retired.push(key.clone());
                    continue;
                };
                if key.1 > current.generation {
                    return Err(Error::new(
                        "stale_generation",
                        "Git generation is not committed",
                    ));
                }
                let historical = key.1 < current.generation;
                // A committed generation advance fences and closes the old native
                // stream. Historical hints retain their original generation key.
                let native_closed = historical
                    || store
                        .read_source_state(&key.0, key.1, "stream")?
                        .get("state")
                        .and_then(Value::as_str)
                        == Some("closed");
                let group = match groups.entry(workspace.to_owned()) {
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        let (edited_paths, paths_complete) =
                            store.git_edit_paths(workspace, PAGE_LIMIT)?;
                        entry.insert(GitGroup {
                            baseline: store.git_baseline(workspace)?,
                            base_oid: store
                                .get_workspace_by_path(workspace)?
                                .map(|row| row.base_oid),
                            inputs: Vec::new(),
                            edited_paths,
                            paths_complete,
                        })
                    }
                    std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                };
                let cursor = store.git_cursor(&key.0, key.1)?;
                let page = store.git_hint_page(&key.0, key.1, cursor, PAGE_LIMIT)?;
                group.inputs.push(GitBindingInput {
                    key: key.clone(),
                    terminal: historical || current.is_terminal(),
                    native_closed,
                    cursor,
                    page,
                    barrier,
                });
            }
        }
        let mut bindings = self.inner.git_bindings.lock_recover();
        for key in retired {
            bindings.remove(&key);
        }
        Ok(groups)
    }

    async fn git_probe_paths(
        &self,
        workspace: &str,
        group: &GitGroup,
        final_flush: bool,
    ) -> Result<(Vec<String>, bool)> {
        let mut paths: HashSet<String> = group.edited_paths.iter().cloned().collect();
        let mut complete = group.paths_complete;
        for input in &group.inputs {
            let mut page = input.page.clone();
            let final_binding = (input.terminal || final_flush) && input.native_closed;
            let mut cursor = input.cursor;
            loop {
                let page_input = GitBindingInput {
                    key: input.key.clone(),
                    terminal: input.terminal,
                    native_closed: input.native_closed,
                    cursor,
                    page,
                    barrier: input.barrier,
                };
                let (hints, _) = verified_file_hints(workspace, &page_input);
                self.check_git_value(
                    serde_json::to_value(&hints)
                        .map_err(|_| Error::new("internal", "Git hints could not be encoded"))?,
                )?;
                for hint in hints {
                    if paths.contains(&hint.path) {
                        continue;
                    }
                    if paths.len() == PAGE_LIMIT {
                        complete = false;
                    } else {
                        paths.insert(hint.path);
                    }
                }
                if !final_binding || page_input.page.through >= input.barrier {
                    break;
                }
                if page_input.page.through <= cursor {
                    return Err(Error::new(
                        "source_integrity",
                        "Git hint cursor did not advance",
                    ));
                }
                cursor = page_input.page.through;
                page = {
                    let store = self.inner.store.lock().await;
                    store.git_hint_page(&input.key.0, input.key.1, cursor, PAGE_LIMIT)?
                };
            }
        }
        let mut paths: Vec<_> = paths.into_iter().collect();
        paths.sort_unstable();
        Ok((paths, complete))
    }

    fn release_inactive_watchers(&self, live: &HashSet<String>, discard_pending: bool) {
        let targets: Vec<_> = self
            .inner
            .git_workspaces
            .lock_recover()
            .iter()
            .filter(|(workspace, _)| !live.contains(*workspace))
            .map(|(_, target)| Arc::clone(target))
            .collect();
        for target in targets {
            let retired = {
                let mut slot = target.watch.lock_recover();
                slot.active = false;
                let result = slot.pending.as_ref().map(|pending| pending.try_recv());
                if matches!(
                    &result,
                    Some(Ok(_)) | Some(Err(std::sync::mpsc::TryRecvError::Disconnected))
                ) {
                    slot.pending = None;
                }
                let pending = if discard_pending {
                    slot.pending.take()
                } else {
                    None
                };
                (slot.watcher.take(), result, pending)
            };
            drop(retired);
        }
    }

    fn retire_git_workspaces(&self) {
        let retired = {
            let bindings = self.inner.git_bindings.lock_recover();
            let needed: HashSet<_> = bindings.values().collect();
            let mut workspaces = self.inner.git_workspaces.lock_recover();
            let mut retired = Vec::new();
            workspaces.retain(|workspace, target| {
                if needed.contains(workspace) {
                    return true;
                }
                // Retain the last Arc until outside both maps: dropping a watcher
                // or a completed pending registration can stop backend threads.
                retired.push(Arc::clone(target));
                false
            });
            retired
        };
        drop(retired);
    }

    fn update_watcher(target: &GitWorkspace, workspace: &str, live: bool) -> bool {
        let (dirty, registration, retired) = {
            let mut slot = target.watch.lock_recover();
            slot.active = live;
            let mut retired = None;
            match slot.pending.as_ref().map(|pending| pending.try_recv()) {
                Some(Ok(result)) => {
                    slot.pending = None;
                    if let Ok(watcher) = result {
                        if live {
                            slot.watcher = Some(watcher);
                        } else {
                            retired = Some(watcher);
                        }
                    }
                }
                Some(Err(std::sync::mpsc::TryRecvError::Disconnected)) => slot.pending = None,
                _ => {}
            }
            let dirty = live
                && slot
                    .watcher
                    .as_ref()
                    .is_some_and(WorkspaceInvalidation::take_dirty);
            let retry = live
                && slot.pending.is_none()
                && Instant::now() >= slot.retry_at
                && slot
                    .watcher
                    .as_ref()
                    .is_none_or(WorkspaceInvalidation::is_degraded);
            let registration = if retry {
                slot.retry_at = Instant::now() + WATCH_RETRY;
                let (sender, receiver) = std::sync::mpsc::sync_channel(1);
                slot.pending = Some(receiver);
                Some(sender)
            } else {
                None
            };
            if !live || retry {
                retired = slot.watcher.take().or(retired);
            }
            (dirty, registration, retired)
        };
        drop(retired);
        if let Some(sender) = registration {
            let workspace = workspace.to_owned();
            // Registration may block in recursive filesystem I/O. Its detached
            // thread never holds Tokio shutdown, Store, cache, or watcher locks.
            if std::thread::Builder::new()
                .name("eliot-fs-watch".to_owned())
                .spawn(move || {
                    let _ = sender.send(WorkspaceInvalidation::watch(&workspace));
                })
                .is_err()
            {
                target.watch.lock_recover().pending = None;
            }
        }
        dirty
    }

    async fn sample_git(&self, final_flush: bool) {
        let groups = match self.git_inputs().await {
            Ok(groups) => groups,
            Err(error) => {
                self.release_inactive_watchers(&HashSet::new(), false);
                self.note_git_read_error(&error.code);
                self.retire_git_workspaces();
                return;
            }
        };
        let live_workspaces: HashSet<_> = groups
            .iter()
            .filter(|(_, group)| group.inputs.iter().any(|input| !input.terminal))
            .map(|(workspace, _)| workspace.clone())
            .collect();
        self.release_inactive_watchers(&live_workspaces, false);
        for (workspace, group) in groups {
            let target = self
                .inner
                .git_workspaces
                .lock_recover()
                .entry(workspace.clone())
                .or_insert_with(|| Arc::new(GitWorkspace::default()))
                .clone();
            let dirty =
                Self::update_watcher(&target, &workspace, live_workspaces.contains(&workspace));
            let (due, earliest) = {
                let mut cache = target.cache.lock_recover();
                if dirty {
                    let earliest = cache
                        .last_attempt
                        .map(|at| at + SAMPLE_INTERVAL)
                        .unwrap_or_else(Instant::now);
                    cache.next_attempt = cache.next_attempt.min(earliest);
                }
                (
                    cache.next_attempt,
                    cache.last_attempt.map(|at| at + SAMPLE_INTERVAL),
                )
            };
            // A binding's final read (terminal generation, native stream
            // closed) is what closes its Git source. Like the shutdown flush
            // it bypasses error backoff: a workspace that is not a Git
            // repository (a chat directory) otherwise backs off to 30 s and
            // holds a stopped Agent's custody for that long.
            let final_read = final_flush
                || group
                    .inputs
                    .iter()
                    .any(|input| input.terminal && input.native_closed);
            if (!final_read && Instant::now() < due)
                || (!final_flush && earliest.is_some_and(|earliest| Instant::now() < earliest))
            {
                continue;
            }
            if final_flush && let Some(earliest) = earliest {
                // A final read may bypass error backoff, but never the minimum probe interval.
                sleep_until(earliest).await;
            }
            let started = Instant::now();
            target.cache.lock_recover().last_attempt = Some(started);
            let result = async {
                let (edited_paths, paths_complete) = self
                    .git_probe_paths(&workspace, &group, final_flush)
                    .await?;
                let mut probe = target.probe.lock().await;
                if probe.is_none() {
                    *probe = Some(GitProbe::new()?);
                }
                let mut snapshot = probe
                    .as_ref()
                    .expect("initialized Git probe")
                    .probe(
                        &workspace,
                        group.baseline.as_ref(),
                        group.base_oid.as_deref(),
                        &edited_paths,
                        now_ms(),
                    )
                    .await?;
                if !paths_complete {
                    snapshot.complete = false;
                    snapshot.history_complete = false;
                    snapshot.history_gap = true;
                }
                self.check_git_value(
                    serde_json::to_value(&snapshot)
                        .map_err(|_| Error::new("internal", "Git snapshot could not be encoded"))?,
                )?;
                Ok::<_, Error>(snapshot)
            }
            .await;
            let slow = started.elapsed() >= SAMPLE_INTERVAL;
            match result {
                Ok(snapshot) => {
                    let mut failed = None;
                    for input in group.inputs {
                        let final_binding = (input.terminal || final_flush) && input.native_closed;
                        let key = input.key.clone();
                        if let Err(error) = self
                            .commit_git_pages(&snapshot, input, final_binding, slow)
                            .await
                        {
                            let _ = self.git_source_status(&key, "error", false).await;
                            if failed.as_deref() != Some("secret_in_payload") {
                                failed = Some(error.code);
                            }
                        }
                    }
                    let cache_check = serde_json::to_value(&snapshot)
                        .map_err(|_| Error::new("internal", "Git snapshot could not be encoded"))
                        .and_then(|value| self.check_git_value(value));
                    let cache_rejected = cache_check.is_err();
                    if let Err(error) = cache_check
                        && failed.as_deref() != Some("secret_in_payload")
                    {
                        failed = Some(error.code);
                    }
                    let mut cache = target.cache.lock_recover();
                    cache.backoff = if slow || failed.is_some() {
                        cache.backoff.saturating_mul(2).min(GIT_MAX_BACKOFF)
                    } else {
                        SAMPLE_INTERVAL
                    };
                    cache.next_attempt = Instant::now() + cache.backoff;
                    if cache_rejected || failed.as_deref() == Some("secret_in_payload") {
                        cache.snapshot = None;
                    } else {
                        cache.snapshot = Some(snapshot);
                    }
                    cache.error = failed;
                }
                Err(error) => {
                    for input in group.inputs {
                        if (input.terminal || final_flush) && input.native_closed {
                            let _ = self
                                .commit_unavailable_git(
                                    &workspace,
                                    group.base_oid.as_deref(),
                                    input,
                                )
                                .await;
                        } else {
                            let _ = self.git_source_status(&input.key, "error", false).await;
                        }
                    }
                    let mut cache = target.cache.lock_recover();
                    if error.code == "secret_in_payload" {
                        cache.snapshot = None;
                    }
                    cache.error = Some(error.code);
                    cache.backoff = cache.backoff.saturating_mul(2).min(GIT_MAX_BACKOFF);
                    cache.next_attempt = Instant::now() + cache.backoff;
                }
            }
        }
        // Final bindings remain registered through hint attribution and source
        // closure. Consult the current registry, not this sweep's input snapshot,
        // so a concurrent new binding keeps its shared cache slot.
        self.retire_git_workspaces();
    }

    async fn commit_git_pages(
        &self,
        snapshot: &GitSnapshot,
        mut input: GitBindingInput,
        final_binding: bool,
        slow_probe: bool,
    ) -> Result<()> {
        loop {
            let (hints, metadata_gap) = verified_file_hints(&snapshot.workspace, &input);
            self.check_git_value(
                serde_json::to_value(&hints)
                    .map_err(|_| Error::new("internal", "Git hints could not be encoded"))?,
            )?;
            {
                let mut store = self.inner.store.lock().await;
                // Recheck at the persistence boundary against the current shared secret set.
                self.check_git_value(
                    serde_json::to_value(snapshot)
                        .map_err(|_| Error::new("internal", "Git snapshot could not be encoded"))?,
                )?;
                self.check_git_value(
                    serde_json::to_value(&hints)
                        .map_err(|_| Error::new("internal", "Git hints could not be encoded"))?,
                )?;
                if metadata_gap {
                    let change = store.record_source_gap(
                        SourceGap {
                            gap_id: format!(
                                "git-file-hints:{}:{}:{}",
                                input.key.0, input.key.1, input.page.through
                            ),
                            agent_id: input.key.0.clone(),
                            generation: input.key.1,
                            source: "git".to_owned(),
                            families: vec!["file".to_owned()],
                            reason: "native_file_change_metadata_incomplete".to_owned(),
                            from: Some(input.cursor.to_string()),
                            to: Some(input.page.through.to_string()),
                        },
                        now_ms(),
                    )?;
                    self.inner.bus.publish(&change.events);
                }
                let events = store.record_git_observation(
                    &input.key.0,
                    input.key.1,
                    snapshot,
                    &hints,
                    input.page.through,
                    now_ms(),
                )?;
                self.inner.bus.publish(&events);
            }
            if !final_binding || input.page.through >= input.barrier {
                break;
            }
            if input.page.through <= input.cursor {
                return Err(Error::new(
                    "source_integrity",
                    "Git hint cursor did not advance",
                ));
            }
            input.cursor = input.page.through;
            let store = self.inner.store.lock().await;
            input.page =
                store.git_hint_page(&input.key.0, input.key.1, input.cursor, PAGE_LIMIT)?;
        }
        let slow = slow_probe || !snapshot.complete || !snapshot.history_complete;
        self.git_source_status(&input.key, if slow { "slow" } else { "ok" }, final_binding)
            .await?;
        if final_binding {
            self.inner.git_bindings.lock_recover().remove(&input.key);
        }
        Ok(())
    }

    async fn commit_unavailable_git(
        &self,
        workspace: &str,
        base_oid: Option<&str>,
        mut input: GitBindingInput,
    ) -> Result<()> {
        // This is an explicit unknown readback: no paths, clean proofs, or HEAD
        // update. The durable gap precedes acknowledging the unobservable hint interval.
        let snapshot = GitSnapshot {
            workspace: workspace.to_owned(),
            head: None,
            branch: None,
            dirty: Vec::new(),
            commits: Vec::new(),
            clean_paths: Vec::new(),
            observed_at: now_ms(),
            complete: false,
            history_complete: false,
            history_gap: true,
            base_oid: base_oid.map(ToOwned::to_owned),
            committed_paths: Vec::new(),
            base_complete: false,
            work: None,
        };
        loop {
            let mut store = self.inner.store.lock().await;
            self.check_git_value(
                serde_json::to_value(&snapshot)
                    .map_err(|_| Error::new("internal", "Git observation could not be encoded"))?,
            )?;
            let change = store.record_source_gap(
                SourceGap {
                    gap_id: format!(
                        "git-unavailable:{}:{}:{}",
                        input.key.0, input.key.1, input.page.through
                    ),
                    agent_id: input.key.0.clone(),
                    generation: input.key.1,
                    source: "git".to_owned(),
                    families: vec!["git".to_owned(), "file".to_owned()],
                    reason: "git_observation_unavailable".to_owned(),
                    from: Some(input.cursor.to_string()),
                    to: Some(input.page.through.to_string()),
                },
                now_ms(),
            )?;
            self.inner.bus.publish(&change.events);
            let events = store.record_git_observation(
                &input.key.0,
                input.key.1,
                &snapshot,
                &[],
                input.page.through,
                now_ms(),
            )?;
            self.inner.bus.publish(&events);
            if input.page.through >= input.barrier {
                let events = store.set_git_source_status(
                    &input.key.0,
                    input.key.1,
                    "error",
                    true,
                    now_ms(),
                )?;
                self.inner.bus.publish(&events);
                drop(store);
                self.inner.git_bindings.lock_recover().remove(&input.key);
                return Ok(());
            }
            if input.page.through <= input.cursor {
                return Err(Error::new(
                    "source_integrity",
                    "Git hint cursor did not advance",
                ));
            }
            input.cursor = input.page.through;
            input.page =
                store.git_hint_page(&input.key.0, input.key.1, input.cursor, PAGE_LIMIT)?;
        }
    }

    async fn git_source_status(&self, key: &Key, health: &str, closed: bool) -> Result<()> {
        let mut store = self.inner.store.lock().await;
        let events = store.set_git_source_status(&key.0, key.1, health, closed, now_ms())?;
        self.inner.bus.publish(&events);
        Ok(())
    }

    async fn finish_git_unavailable(&self) {
        match self.git_inputs().await {
            Ok(groups) => {
                for (workspace, group) in groups {
                    for input in group.inputs {
                        if input.native_closed {
                            let _ = self
                                .commit_unavailable_git(
                                    &workspace,
                                    group.base_oid.as_deref(),
                                    input,
                                )
                                .await;
                        } else {
                            let _ = self.git_source_status(&input.key, "stale", false).await;
                        }
                    }
                }
            }
            Err(error) => self.mark_git_unavailable(&error.code),
        }
        self.retire_git_workspaces();
    }

    fn note_git_read_error(&self, code: &str) {
        let targets: Vec<_> = self
            .inner
            .git_workspaces
            .lock_recover()
            .values()
            .cloned()
            .collect();
        for target in targets {
            target.cache.lock_recover().error = Some(code.to_owned());
        }
    }

    fn target(&self, agent: &str, generation: i64) -> Option<Arc<Target>> {
        self.inner
            .targets
            .lock_recover()
            .get(&ProcessKey::Agent(agent.to_owned(), generation))
            .cloned()
    }

    async fn sample_all(&self) {
        let targets: Vec<_> = self
            .inner
            .targets
            .lock_recover()
            .values()
            .cloned()
            .collect();
        #[cfg(target_os = "linux")]
        self.sample_all_sweep(targets).await;
        #[cfg(not(target_os = "linux"))]
        for target in targets {
            let _ = self.sample_target(&target).await;
        }
    }

    /// Sample every captured target from one shared host enumeration. New
    /// targets attached mid-sweep wait for the next sweep. The scan is skipped
    /// when no captured leader is current; a failed scan is committed as
    /// unavailable to each captured target without rescanning per target.
    /// All captured sampling locks are held across the shared scan; each is
    /// released right after that target's own commit.
    /// Returns the host enumerations this sweep performed: 0 when skipped or
    /// failed, 1 when scanned. The count is scoped to the sweep's return
    /// value, never to a process-global counter.
    #[cfg(target_os = "linux")]
    async fn sample_all_sweep(&self, targets: Vec<Arc<Target>>) -> usize {
        self.sample_all_sweep_with(targets, enumerate_process_sweep)
            .await
    }

    /// Sweep with the host enumeration injected: production passes
    /// [`enumerate_process_sweep`], while tests inject a failing or refusing
    /// enumerator scoped to that sweep.
    #[cfg(target_os = "linux")]
    async fn sample_all_sweep_with<E>(&self, targets: Vec<Arc<Target>>, enumerate: E) -> usize
    where
        E: FnOnce() -> Result<LinuxProcessSweep> + Send + 'static,
    {
        if targets.is_empty() {
            return 0;
        }
        // Serialize with concurrent per-target samplers in capture order; a
        // concurrent sampler holds at most one target lock, so this cannot
        // deadlock.
        let mut guards = Vec::with_capacity(targets.len());
        for target in &targets {
            guards.push(target.sampling.lock().await);
        }
        // One timestamp for the whole sweep, taken before the shared scan.
        let sampled_at = now_ms();
        let observers: Vec<CustodyObserver> = targets
            .iter()
            .map(|target| target.observer.clone())
            .collect();
        let scan = tokio::task::spawn_blocking(move || {
            let leaders_before: Vec<bool> = observers
                .iter()
                .map(|observer| observer.group_is_current())
                .collect();
            if !leaders_before.iter().any(|current| *current) {
                return Ok((leaders_before, None));
            }
            enumerate().map(|sweep| (leaders_before, Some(sweep)))
        })
        .await
        .map_err(|_| Error::new("process_observation", "process sampling task failed"))
        .and_then(|result| result);
        let (leaders_before, sweep) = match scan {
            Ok((leaders, sweep)) => (leaders, sweep),
            Err(error) => {
                // All locks were held across the shared scan above; release
                // each target right after its own failure commit so a
                // single-target sampler waits only for earlier commits.
                let mut guards = guards.into_iter();
                for target in &targets {
                    let guard = guards.next();
                    let _ = self
                        .commit_snapshot(target, Err(error.clone()), sampled_at)
                        .await;
                    drop(guard);
                }
                return 0;
            }
        };
        // Enumeration was skipped only when every captured leader is gone; the
        // empty sweep is then unused because no member is attributed.
        let enumerations = usize::from(sweep.is_some());
        let sweep = sweep.unwrap_or(LinuxProcessSweep {
            entries: Vec::new(),
            complete: false,
        });
        let sweep = Arc::new(sweep);
        let mut built = Vec::with_capacity(targets.len());
        for (target, leader_before) in targets.iter().zip(leaders_before) {
            let observer = target.observer.clone();
            let process_owner = target.owner.clone();
            let sweep = Arc::clone(&sweep);
            let result = tokio::task::spawn_blocking(move || {
                observer.snapshot_with_sweep(&process_owner, sampled_at, &sweep, leader_before)
            })
            .await
            .map_err(|_| Error::new("process_observation", "process sampling task failed"))
            .and_then(|result| result);
            built.push((target, result));
        }
        // Release the shared sweep before touching the Store.
        drop(sweep);
        // All locks were held across the shared scan and snapshot builds
        // above; release each target right after its own commit.
        let mut guards = guards.into_iter();
        for (target, result) in built {
            let guard = guards.next();
            let _ = self.commit_snapshot(target, result, sampled_at).await;
            drop(guard);
        }
        enumerations
    }

    async fn sample_target(&self, target: &Arc<Target>) -> Result<()> {
        let _sampling = target.sampling.lock().await;
        let observer = target.observer.clone();
        let process_owner = target.owner.clone();
        let sampled_at = now_ms();
        let result =
            tokio::task::spawn_blocking(move || observer.snapshot(&process_owner, sampled_at))
                .await
                .map_err(|_| Error::new("process_observation", "process sampling task failed"))
                .and_then(|result| result);
        self.commit_snapshot(target, result, sampled_at).await
    }

    async fn commit_snapshot(
        &self,
        target: &Arc<Target>,
        result: Result<ProcessSnapshot>,
        sampled_at: i64,
    ) -> Result<()> {
        let mut snapshot = match result {
            Ok(snapshot) => snapshot,
            Err(error) => {
                target.cache.lock_recover().error = Some(error.code.clone());
                let _ = self.set_source_status(target, "error", false).await;
                return Err(error);
            }
        };
        if let Some(context) = &target.workspace {
            for sample in &mut snapshot.samples {
                // The platform's verified root role is presentation, not authority.
                if sample.record.role == "plugin" {
                    sample.record.role = context.role.to_owned();
                }
            }
        }
        let final_exit = snapshot.complete
            && snapshot
                .samples
                .iter()
                .all(|sample| sample.record.state == ProcessState::Exited);
        let observed = {
            let mut store = self.inner.store.lock().await;
            let mut result = Ok(());
            for samples in snapshot.samples.chunks(PAGE_LIMIT) {
                let events = if target.workspace.is_some() {
                    store.observe_workspace_processes(
                        &target.agent,
                        target.generation,
                        &ProcessSnapshot {
                            samples: samples.to_vec(),
                            complete: snapshot.complete,
                        },
                        sampled_at,
                    )
                } else {
                    let records: Vec<ProcessRecord> =
                        samples.iter().map(|sample| sample.record.clone()).collect();
                    store.observe_processes(&target.agent, target.generation, &records, sampled_at)
                };
                match events {
                    Ok(events) => self.inner.bus.publish(&events),
                    Err(error) => {
                        result = Err(error);
                        break;
                    }
                }
            }
            result
        };
        if let Err(error) = observed {
            target.cache.lock_recover().error = Some(error.code.clone());
            let _ = self.set_source_status(target, "error", false).await;
            return Err(error);
        }
        let health = if target.workspace.is_some() && !snapshot.complete {
            "slow"
        } else {
            "ok"
        };
        // Workspace source lifetime includes subsequent setup steps/removal;
        // set_source_status keeps it open even after this owned scope exits.
        if let Err(error) = self.set_source_status(target, health, final_exit).await {
            target.cache.lock_recover().error = Some(error.code.clone());
            return Err(error);
        }
        self.observe_sampled(target, &snapshot.samples, sampled_at)
            .await;
        *target.cache.lock_recover() = Cache {
            snapshot: Some(snapshot),
            sampled_at: Some(sampled_at),
            error: None,
        };
        if final_exit {
            let key = target.key.clone();
            let mut targets = self.inner.targets.lock_recover();
            if targets
                .get(&key)
                .is_some_and(|current| Arc::ptr_eq(current, target))
            {
                targets.remove(&key);
            }
        }
        Ok(())
    }

    /// Observe one sampled agent target: close heavy child-process episodes
    /// and evaluate session growth. Workspace targets are out of scope: their
    /// setup runner owns its own lifecycle. Failures here never fail the
    /// sample; the next sweep retries.
    async fn observe_sampled(&self, target: &Arc<Target>, samples: &[ProcessSample], now: i64) {
        if target.workspace.is_some() {
            return;
        }
        let thresholds = *self.inner.observe.lock_recover();
        let mut seen = HashSet::new();
        let mut probes = Vec::new();
        {
            let mut tracker = target.observe.lock_recover();
            for sample in samples {
                if sample.record.role != "child" {
                    continue;
                }
                let key = episode_key(&sample.record.identity);
                seen.insert(key.clone());
                if let Some(open) = tracker.episodes.get_mut(&key) {
                    apply_sample(open, sample, now);
                } else if !tracker.emitted_heavy.contains(&key)
                    && !probes.iter().any(|(known, _)| known == &key)
                {
                    probes.push((key, sample.record.identity.pid));
                }
            }
        }
        // Classify new identities outside the lock: one quick read-only
        // query per new child; failures fail closed (no episode opened).
        let mut classified = Vec::new();
        for (key, pid) in probes {
            if let Some(class) = command_class_for(pid) {
                classified.push((key, class));
            }
        }
        let mut closing = Vec::new();
        {
            let mut tracker = target.observe.lock_recover();
            for (key, class) in classified {
                tracker
                    .episodes
                    .entry(key)
                    .or_insert_with(|| OpenHeavyEpisode {
                        category: class.category,
                        category_source: class.source.to_owned(),
                        start_ms: now,
                        last_ms: now,
                        first_cpu_ns: None,
                        last_cpu_ns: None,
                        peak_rss_bytes: 0,
                        exit_code: None,
                        exited: false,
                        misses: 0,
                    });
            }
            // New episodes missed the first update pass; apply this sweep.
            for sample in samples {
                if sample.record.role != "child" {
                    continue;
                }
                let key = episode_key(&sample.record.identity);
                if let Some(open) = tracker.episodes.get_mut(&key)
                    && open.start_ms == now
                {
                    apply_sample(open, sample, now);
                }
            }
            tracker.episodes.retain(|key, open| {
                if seen.contains(key) {
                    return true;
                }
                open.misses = open.misses.saturating_add(1);
                if open.exited || open.misses >= 2 {
                    closing.push((key.clone(), std::mem::take(open)));
                    return false;
                }
                true
            });
        }
        // Hold back closing episodes whose category still has an open episode:
        // that command is still running, and this tail belongs to its single
        // episode. Held episodes rejoin the open map with their state intact.
        let mut ready: Vec<(String, OpenHeavyEpisode)> = Vec::new();
        {
            let mut tracker = target.observe.lock_recover();
            let open_categories: HashSet<String> = tracker
                .episodes
                .values()
                .map(|open| open.category.clone())
                .collect();
            for (key, open) in closing {
                if open_categories.contains(open.category.as_str()) {
                    tracker.episodes.insert(key, open);
                } else {
                    ready.push((key, open));
                }
            }
        }
        let mut episodes = Vec::with_capacity(ready.len());
        for (key, open) in ready {
            let end_source = if open.exited {
                "observed_exit"
            } else {
                "last_seen"
            };
            let episode = eliot_core::HeavyEpisode {
                category: open.category,
                category_source: open.category_source,
                started_at: open.start_ms,
                ended_at: open.last_ms.max(open.start_ms),
                end_source: end_source.to_owned(),
                duration_ms: open.last_ms.max(open.start_ms) - open.start_ms,
                cpu_ms: open
                    .first_cpu_ns
                    .and_then(|first| {
                        open.last_cpu_ns
                            .map(|last| last.saturating_sub(first) / 1_000_000)
                    })
                    .unwrap_or(0) as i64,
                peak_rss_bytes: open.peak_rss_bytes,
                exit_code: open.exit_code,
            };
            if episode.validate().is_ok() {
                target.observe.lock_recover().emitted_heavy.insert(key);
                episodes.push(episode);
            }
        }
        // Overlapping episodes from one command (wrapper, driver, compiler,
        // console host) merge into one episode per command.
        let mut heavy = Vec::new();
        for episode in eliot_core::merge_heavy_episodes(episodes) {
            if episode.crosses(&thresholds.heavy) {
                heavy.push(episode);
            }
        }
        // Session growth from the retained snapshot; warn once per basis per
        // generation until the signal clears, then record the recovery. A
        // profile ceiling uses the latest actual context count independently
        // of whether the native adapter reports its window capacity.
        let mut growth_events = Vec::new();
        {
            let (harness, usage) = {
                let store = self.inner.store.lock().await;
                let harness = match target.observe.lock_recover().harness.clone() {
                    Some(harness) => harness,
                    None => match store.get_agent(&target.agent).unwrap_or(None) {
                        Some(agent) => agent.harness.clone(),
                        None => return,
                    },
                };
                let usage = store
                    .get_session_usage(&target.agent, target.generation)
                    .unwrap_or(None);
                (harness, usage)
            };
            target.observe.lock_recover().harness = Some(harness.clone());
            let mut tracker = target.observe.lock_recover();
            // The retained session snapshot carries the native session file
            // size where the harness exposes a documented size source
            // (transcript/rollout scan); absent sizes never warn.
            let session_bytes = usage.as_ref().and_then(|usage| usage.session_file_bytes);
            let ceiling_growth = target.context_ceiling_tokens.and_then(|ceiling| {
                let used = usage
                    .as_ref()
                    .and_then(|usage| usage.context_used_tokens)
                    .filter(|used| *used >= 0)?;
                (used > ceiling).then(|| {
                    (
                        "context_ceiling",
                        serde_json::json!({
                            "basis": "context_ceiling",
                            "warn": true,
                            "context_used_tokens": used,
                            "context_ceiling_tokens": ceiling,
                            "usage_observed_at": usage.as_ref().and_then(|usage| usage.observed_at),
                            "message": format!("latest model context {used} tokens exceeds profile ceiling {ceiling} tokens"),
                            "level": eliot_core::OBSERVE_LEVEL_STANDARD,
                        }),
                    )
                })
            });
            let growth = ceiling_growth.or_else(|| {
                eliot_core::evaluate_session_growth(
                    &harness,
                    usage.as_ref(),
                    session_bytes,
                    &thresholds.growth,
                )
                .map(|warn| (warn.basis_name(), warn.payload()))
            });
            match growth {
                Some((basis, payload)) => {
                    if tracker.warned_growth.insert(basis) {
                        growth_events.push((eliot_core::SESSION_GROWTH_KIND, payload));
                    }
                }
                None => {
                    for basis in tracker.warned_growth.drain().collect::<Vec<_>>() {
                        growth_events.push((
                            eliot_core::SESSION_GROWTH_KIND,
                            serde_json::json!({
                                "basis": basis,
                                "warn": false,
                                "message": "session growth cleared; signal is back under threshold",
                                "level": eliot_core::OBSERVE_LEVEL_STANDARD,
                            }),
                        ));
                    }
                }
            }
        }
        if heavy.is_empty() && growth_events.is_empty() {
            return;
        }
        let mut published = Vec::new();
        {
            let mut store = self.inner.store.lock().await;
            for episode in heavy {
                if let Ok(event) = store.commit_observe_event(
                    Some(&target.agent),
                    Some(target.generation),
                    eliot_core::ACTIVITY_HEAVY_KIND,
                    episode.payload(),
                    now,
                ) {
                    published.push(event);
                }
            }
            let active_turn = if growth_events.is_empty() {
                None
            } else {
                store
                    .get_agent(&target.agent)
                    .ok()
                    .flatten()
                    .filter(|agent| agent.generation == target.generation)
                    .and_then(|agent| agent.active_turn)
            };
            for (kind, mut data) in growth_events {
                if let Some(turn_id) = &active_turn {
                    data["turn_id"] = serde_json::json!(turn_id);
                }
                if let Ok(event) = store.commit_observe_event(
                    Some(&target.agent),
                    Some(target.generation),
                    kind,
                    data,
                    now,
                ) {
                    published.push(event);
                }
            }
        }
        if !published.is_empty() {
            self.inner.bus.publish(&published);
        }
    }

    async fn set_source_status(&self, target: &Target, health: &str, closed: bool) -> Result<()> {
        let mut store = self.inner.store.lock().await;
        let events = if target.workspace.is_some() {
            store.set_workspace_process_source_status(
                &target.agent,
                target.generation,
                health,
                false,
                now_ms(),
            )?
        } else {
            store.set_process_source_status(
                &target.agent,
                target.generation,
                health,
                closed,
                now_ms(),
            )?
        };
        self.inner.bus.publish(&events);
        Ok(())
    }
}

fn verified_file_hints(workspace: &str, input: &GitBindingInput) -> (Vec<FileChangeHint>, bool) {
    let mut hints = Vec::new();
    let mut seen = HashSet::new();
    let mut gap = false;
    for event in &input.page.events {
        if event.agent.as_deref() != Some(input.key.0.as_str())
            || event.generation != Some(input.key.1)
            || event.source != "stream"
            || event.native_id.as_deref().is_none_or(str::is_empty)
            || event.kind != "tool.completed"
            || event.data.get("tool").and_then(Value::as_str) != Some("fileChange")
            || event.data.get("status").and_then(Value::as_str) != Some("completed")
        {
            continue;
        }
        gap |= event
            .data
            .get("changed_paths_truncated")
            .and_then(Value::as_bool)
            == Some(true);
        if event.data.get("redacted").and_then(Value::as_bool) == Some(true) {
            gap = true;
            continue;
        }
        let Some(paths) = event.data.get("changed_paths").and_then(Value::as_array) else {
            gap = true;
            continue;
        };
        let Some(event_seq) = u64::try_from(event.seq).ok().filter(|seq| *seq > 0) else {
            gap = true;
            continue;
        };
        for raw in paths {
            let Some(path) = raw
                .as_str()
                .and_then(|raw| normalize_hint(workspace, raw).ok())
            else {
                gap = true;
                continue;
            };
            if !seen.insert((path.to_owned(), event_seq)) {
                continue;
            }
            if hints.len() == PAGE_LIMIT {
                gap = true;
                continue;
            }
            hints.push(FileChangeHint {
                path: path.to_owned(),
                event_seq,
                ts: event.ts,
            });
        }
    }
    (hints, gap)
}

/// Only committed owners of native lifecycle families determine live-source health.
/// Retained gap boundaries and predecessors without those families are independent.
fn native_sources_healthy(states: Vec<Value>) -> Option<bool> {
    let mut selected = false;
    let mut unknown = false;
    let mut unhealthy = false;
    for state in states {
        let state: SourceState = serde_json::from_value(state).ok()?;
        if matches!(
            state.source.as_str(),
            "daemon" | "os" | "git" | "workspace_process"
        ) || !state.families.iter().any(|family| {
            matches!(
                family.as_str(),
                "session" | "turn" | "message" | "tool" | "request" | "child" | "goal" | "usage"
            )
        }) {
            continue;
        }
        selected = true;
        match (state.state.as_str(), state.health.as_str()) {
            ("open", "ok") => {}
            ("closed", _) | ("open", "slow" | "stale" | "error") => unhealthy = true,
            _ => unknown = true,
        }
    }
    if unhealthy {
        Some(false)
    } else if selected && !unknown {
        Some(true)
    } else {
        None
    }
}

fn age_ms(now: i64, timestamp: i64) -> Option<i64> {
    now.checked_sub(timestamp).filter(|age| *age >= 0)
}

fn stats_view(cache: &Cache, unavailable: Option<String>, now: i64) -> ProcessStatsView {
    let sample_age_ms = cache.sampled_at.and_then(|at| age_ms(now, at));
    let clock_regressed = cache.sampled_at.is_some_and(|at| at > now);
    let stale = unavailable.is_some()
        || cache.error.is_some()
        || clock_regressed
        || sample_age_ms.is_some_and(|age| age > SAMPLE_TTL_MS);
    let available = cache.snapshot.is_some() && !stale;
    let coverage = match cache.snapshot.as_ref() {
        None => StatsCoverage::Unknown,
        Some(_) if stale => StatsCoverage::Stale,
        Some(snapshot) if snapshot.complete => StatsCoverage::Complete,
        Some(_) => StatsCoverage::Partial,
    };
    let samples = cache
        .snapshot
        .as_ref()
        .filter(|_| available)
        .map(|snapshot| &snapshot.samples);
    let total_samples = samples.filter(|_| cache.snapshot.as_ref().is_some_and(|s| s.complete));
    let cpu_time_ns = total_samples.and_then(|samples| {
        if samples.is_empty() {
            return None;
        }
        samples.iter().try_fold(0_u64, |total, sample| {
            total.checked_add(sample.cpu_time_ns?)
        })
    });
    let rss_bytes = total_samples.and_then(|samples| {
        if samples.is_empty() {
            return None;
        }
        samples
            .iter()
            .try_fold(0_u64, |total, sample| total.checked_add(sample.rss_bytes?))
    });
    ProcessStatsView {
        available,
        sampled_at: cache.sampled_at,
        age_ms: sample_age_ms,
        ttl_ms: SAMPLE_TTL_MS,
        clock_regressed,
        coverage,
        complete: cache.snapshot.as_ref().map(|snapshot| snapshot.complete),
        cpu_time_ns,
        rss_bytes,
        samples: samples.cloned(),
        error: unavailable.or_else(|| cache.error.clone()),
    }
}

pub(crate) fn workspace_owner(agent: &str, target_generation: i64) -> String {
    format!("workspace:{agent}#{target_generation}")
}

fn episode_key(identity: &ProcessIdentity) -> String {
    format!(
        "{}/{}/{}",
        identity.host_boot_id, identity.pid, identity.start_time
    )
}

fn apply_sample(open: &mut OpenHeavyEpisode, sample: &ProcessSample, now: i64) {
    open.last_ms = now;
    open.misses = 0;
    if let Some(cpu) = sample.cpu_time_ns {
        if open.first_cpu_ns.is_none() {
            open.first_cpu_ns = Some(cpu);
        }
        open.last_cpu_ns = Some(cpu);
    }
    if let Some(rss) = sample.rss_bytes {
        open.peak_rss_bytes = open.peak_rss_bytes.max(rss);
    }
    if sample.record.state == ProcessState::Exited {
        open.exited = true;
        open.exit_code = sample.record.exit_code;
    }
}

/// Read-only program identity for one owned child process: its own argv plus
/// ancestors' argv resolve through harness plumbing (console hosts, the
/// shell wrapper) to the program that actually consumed the resources. The
/// Windows probe uses `QueryFullProcessImageNameW` plus best-effort
/// `NtQueryInformationProcess` command-line and parent reads; Linux reads
/// `/proc/<pid>/cmdline` and `/proc/<pid>/stat`. Plumbing alone fails closed
/// to `None` so no episode opens on it.
#[cfg(windows)]
fn command_class_for(pid: u32) -> Option<crate::platform::CommandClass> {
    crate::platform::windows::resolve_heavy_category(pid)
}

#[cfg(target_os = "linux")]
fn command_class_for(pid: u32) -> Option<crate::platform::CommandClass> {
    crate::platform::unix::resolve_heavy_category(pid)
}

#[cfg(not(any(windows, target_os = "linux")))]
fn command_class_for(_pid: u32) -> Option<crate::platform::CommandClass> {
    None
}

fn owner(agent: &str, generation: i64) -> String {
    format!("agent:{agent}#{generation}")
}

/// A cache is installed only after Store accepts the exact observation.
/// Never let an older cache or a different process fact refresh a durable row.
fn refresh_process_timestamps(processes: &mut [ProcessRecord], cache: &Cache) {
    let Some(snapshot) = &cache.snapshot else {
        return;
    };
    let samples: HashMap<_, _> = snapshot
        .samples
        .iter()
        .map(|sample| (&sample.record.identity, &sample.record))
        .collect();
    for process in processes {
        let Some(sample) = samples.get(&process.identity) else {
            continue;
        };
        if sample.updated_at > process.updated_at
            && sample.owner == process.owner
            && sample.role == process.role
            && sample.state == process.state
            && sample.exit_code == process.exit_code
        {
            process.updated_at = sample.updated_at;
        }
    }
}
