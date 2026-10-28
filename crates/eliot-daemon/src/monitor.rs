//! OS facts and ephemeral counters, independent of the native lifecycle source.
use crate::{
    bus::Bus,
    fs_invalidation::WorkspaceInvalidation,
    git_monitor::{GitProbe, normalize_hint},
    platform::CustodyObserver,
    plugin_host::SecretSet,
};
use eliot_core::{
    Agent, Error, FileChangeHint, GitBaseline, GitHintPage, GitSnapshot, ProcessRecord,
    ProcessSnapshot, ProcessState, Result, SourceGap, WorkspacePreparation,
};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{Mutex as AsyncMutex, watch},
    task::JoinSet,
    time::{Instant, MissedTickBehavior, interval, sleep_until, timeout_at},
};

const SAMPLE_INTERVAL: Duration = Duration::from_secs(2);
const SAMPLE_TTL_MS: i64 = 4_000;
const PAGE_LIMIT: usize = 1_000;
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
}

struct Target {
    key: ProcessKey,
    agent: String,
    generation: i64,
    owner: String,
    observer: CustodyObserver,
    sampling: AsyncMutex<()>,
    cache: Mutex<Cache>,
    workspace: Option<WorkspaceProcessContext>,
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
    fn view(&self) -> Value {
        let state = match (self.active, self.watcher.as_ref()) {
            (false, _) => "inactive",
            (true, Some(watcher)) if watcher.is_degraded() => "degraded",
            (true, Some(_)) => "healthy",
            (true, None) => "unavailable",
        };
        json!({
            "state": state,
            "retry_after_ms": (self.pending.is_none() && matches!(state, "degraded" | "unavailable"))
                .then(|| self.retry_at.saturating_duration_since(Instant::now()).as_millis()),
        })
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
            }),
        }
    }

    pub(crate) fn mark_unavailable(&self, code: &str) {
        *self
            .inner
            .unavailable
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(code.to_owned());
        self.mark_git_unavailable(code);
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
        self.inner
            .git_bindings
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert((agent.id.clone(), agent.generation), workspace.to_owned());
        self.inner
            .git_workspaces
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
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
            owner: owner(&agent.id, agent.generation),
            observer,
            sampling: AsyncMutex::new(()),
            cache: Mutex::new(Cache::default()),
            workspace: None,
        });
        {
            let mut targets = self
                .inner
                .targets
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if targets.contains_key(&key) {
                return Err(Error::new(
                    "bad_state",
                    "process observation is already attached",
                ));
            }
            targets.insert(key, Arc::clone(&target));
        }
        // A failed read-only probe leaves unknown observation; it does not fail native startup.
        let _ = self.sample_target(&target).await;
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
            owner: workspace_owner(&context.agent, context.target_generation),
            observer,
            sampling: AsyncMutex::new(()),
            cache: Mutex::new(Cache::default()),
            workspace: Some(context),
        });
        {
            let mut targets = self
                .inner
                .targets
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
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
            let mut targets = self
                .inner
                .targets
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
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

    pub(crate) async fn view(&self, agent: &Agent) -> Result<Value> {
        let cache = self
            .target(&agent.id, agent.generation)
            .map(|target| {
                target
                    .cache
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone()
            })
            .unwrap_or_default();
        let unavailable = self
            .inner
            .unavailable
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let (processes, last_model_progress, source_healthy) = {
            let store = self.inner.store.lock().await;
            let processes =
                store.list_processes(&owner(&agent.id, agent.generation), None, PAGE_LIMIT)?;
            let progress = store.last_model_progress(&agent.id, agent.generation)?;
            let healthy = store
                .read_source_state(&agent.id, agent.generation, "stream")
                .ok()
                .map(|state| {
                    state.get("state").and_then(Value::as_str) == Some("open")
                        && state.get("health").and_then(Value::as_str) == Some("ok")
                });
            (processes, progress, healthy)
        };
        let now = now_ms();
        let model_clock_regressed = last_model_progress.is_some_and(|at| at > now);
        let mut view = json!({
            "processes_truncated": processes.len() == PAGE_LIMIT,
            "processes": processes,
            "stats": stats_view(&cache, unavailable, now),
            "liveness": {
                "state": agent.state,
                "last_model_progress": last_model_progress,
                "age_ms": last_model_progress.and_then(|at| age_ms(now, at)),
                "clock_regressed": model_clock_regressed,
                "basis": "committed_native_facts",
                "source_healthy": source_healthy,
            },
        });
        view["workspace"] = match self.workspace_view(agent).await {
            Ok(workspace) => workspace,
            Err(error) => json!({"available": false, "error": error.code}),
        };
        view["git"] = match self.git_view(agent).await {
            Ok(git) => git,
            Err(error) => json!({"available": false, "error": error.code}),
        };
        Ok(view)
    }

    /// Agent.generation remains native/control state. Workspace observation is
    /// explicitly named by its retained target, including target 1 while Agent is 0.
    pub(crate) async fn workspace_view(&self, agent: &Agent) -> Result<Value> {
        let (preparation, processes, source) = {
            let store = self.inner.store.lock().await;
            let Some(preparation) = store.get_workspace(&agent.id)? else {
                return Ok(Value::Null);
            };
            let process_owner = workspace_owner(&agent.id, preparation.target_generation);
            let processes = store.list_processes(&process_owner, None, PAGE_LIMIT)?;
            let source = store
                .list_source_states(&agent.id, preparation.target_generation)?
                .into_iter()
                .find(|state| {
                    state.get("source").and_then(Value::as_str) == Some("workspace_process")
                });
            (preparation, processes, source)
        };
        let cache = self.workspace_cache(&agent.id, preparation.target_generation);
        let unavailable = self
            .inner
            .unavailable
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        Ok(json!({
            "owner": workspace_owner(&agent.id, preparation.target_generation),
            "target_generation": preparation.target_generation,
            "phase": preparation.phase,
            "setup_index": preparation.setup_index,
            "processes_truncated": processes.len() == PAGE_LIMIT,
            "processes": processes,
            "stats": stats_view(&cache, unavailable, now_ms()),
            "source": source,
        }))
    }

    fn workspace_cache(&self, agent: &str, generation: i64) -> Cache {
        let targets: Vec<_> = self
            .inner
            .targets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
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
            let cache = target
                .cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
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

    pub(crate) async fn git_view(&self, agent: &Agent) -> Result<Value> {
        let workspace = agent.workspace.as_deref().ok_or_else(|| {
            Error::new("invalid", "Git observation requires an admitted workspace")
        })?;
        let target = self
            .inner
            .git_workspaces
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(workspace)
            .cloned();
        let cache = target
            .as_ref()
            .map(|target| {
                target
                    .cache
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone()
            })
            .unwrap_or_default();
        let invalidation = target
            .as_ref()
            .map(|target| {
                target
                    .watch
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .view()
            })
            .unwrap_or_else(|| json!({"state": "inactive"}));
        let unavailable = self
            .inner
            .git_unavailable
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let (baseline, edits, source) = {
            let store = self.inner.store.lock().await;
            (
                store.git_baseline(workspace)?,
                store.list_agent_edits(&agent.id, agent.generation, None, PAGE_LIMIT)?,
                store
                    .read_source_state(&agent.id, agent.generation, "git")
                    .ok(),
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
        let value = json!({
            "available": snapshot.is_some() && !stale,
            "workspace": workspace,
            "observed_at": sampled_at,
            "age_ms": age,
            "ttl_ms": SAMPLE_TTL_MS,
            "clock_regressed": clock_regressed,
            "retry_after_ms": cache.next_attempt.saturating_duration_since(Instant::now()).as_millis(),
            "filesystem_invalidation": invalidation,
            "head": snapshot.and_then(|snapshot| snapshot.head.as_deref()),
            "branch": snapshot.and_then(|snapshot| snapshot.branch.as_deref()),
            "dirty": snapshot.map(|snapshot| &snapshot.dirty),
            "commits": snapshot.map(|snapshot| &snapshot.commits),
            "complete": snapshot.map(|snapshot| snapshot.complete),
            "history_complete": snapshot.map(|snapshot| snapshot.history_complete),
            "history_gap": snapshot.map(|snapshot| snapshot.history_gap),
            "base_oid": snapshot.and_then(|snapshot| snapshot.base_oid.as_deref()),
            "committed_paths": snapshot.map(|snapshot| &snapshot.committed_paths),
            "base_complete": snapshot.map(|snapshot| snapshot.base_complete),
            "baseline": baseline,
            "edits_truncated": edits.len() == PAGE_LIMIT,
            "edits": edits,
            "source": source,
            "error": unavailable.or(cache.error),
        });
        self.check_git_value(value.clone())?;
        Ok(value)
    }

    fn check_git_value(&self, mut value: Value) -> Result<()> {
        let secrets: Vec<_> = self
            .inner
            .secrets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .cloned()
            .collect();
        if crate::redaction::redact_value(&mut value, &secrets) {
            return Err(Error::new(
                "secret_in_payload",
                "Git observation contains a known credential",
            ));
        }
        Ok(())
    }

    pub(crate) async fn run(self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        let mut git_tasks = JoinSet::new();
        git_tasks.spawn(self.clone().run_git(shutdown.clone()));
        let mut ticks = interval(SAMPLE_INTERVAL);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            if *shutdown.borrow() {
                break;
            }
            tokio::select! {
                _ = ticks.tick() => self.sample_all().await,
                joined = git_tasks.join_next(), if !git_tasks.is_empty() => {
                    if !*shutdown.borrow() {
                        let code = match joined {
                            Some(Ok(Err(ref error))) => error.code.as_str(),
                            Some(Err(_)) => "git_task_failed",
                            _ => "git_task_stopped",
                        };
                        self.mark_git_unavailable(code);
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
                if !matches!(joined, Ok(Ok(()))) {
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
        *self
            .inner
            .git_unavailable
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(code.to_owned());
        self.release_inactive_watchers(&HashSet::new(), true);
    }

    async fn run_git(self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        let mut ticks = interval(SAMPLE_INTERVAL);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            if *shutdown.borrow() {
                break;
            }
            tokio::select! {
                _ = ticks.tick() => self.sample_git(false).await,
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

    async fn git_inputs(&self) -> Result<HashMap<String, GitGroup>> {
        let bindings = self
            .inner
            .git_bindings
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let mut groups: HashMap<String, GitGroup> = HashMap::new();
        let mut retired = Vec::new();
        {
            let store = self.inner.store.lock().await;
            let barrier = u64::try_from(store.high_water()?)
                .map_err(|_| Error::new("internal", "Git high-water is invalid"))?;
            for (key, workspace) in bindings {
                let Some(current) = store.get_agent(&key.0)? else {
                    retired.push(key);
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
                let group = match groups.entry(workspace.clone()) {
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        let (edited_paths, paths_complete) =
                            store.git_edit_paths(&workspace, PAGE_LIMIT)?;
                        entry.insert(GitGroup {
                            baseline: store.git_baseline(&workspace)?,
                            base_oid: store
                                .get_workspace_by_path(&workspace)?
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
                    key,
                    terminal: historical || current.is_terminal(),
                    native_closed,
                    cursor,
                    page,
                    barrier,
                });
            }
        }
        let mut bindings = self
            .inner
            .git_bindings
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|(workspace, _)| !live.contains(*workspace))
            .map(|(_, target)| Arc::clone(target))
            .collect();
        for target in targets {
            let retired = {
                let mut slot = target
                    .watch
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
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

    fn update_watcher(target: &GitWorkspace, workspace: &str, live: bool) -> bool {
        let (dirty, registration, retired) = {
            let mut slot = target
                .watch
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
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
                target
                    .watch
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .pending = None;
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
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entry(workspace.clone())
                .or_insert_with(|| Arc::new(GitWorkspace::default()))
                .clone();
            let dirty =
                Self::update_watcher(&target, &workspace, live_workspaces.contains(&workspace));
            let (due, earliest) = {
                let mut cache = target
                    .cache
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
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
            if !final_flush
                && (Instant::now() < due
                    || earliest.is_some_and(|earliest| Instant::now() < earliest))
            {
                continue;
            }
            if final_flush && let Some(earliest) = earliest {
                // A final read may bypass error backoff, but never the minimum probe interval.
                sleep_until(earliest).await;
            }
            let started = Instant::now();
            target
                .cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .last_attempt = Some(started);
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
                    let mut cache = target
                        .cache
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
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
                    let mut cache = target
                        .cache
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if error.code == "secret_in_payload" {
                        cache.snapshot = None;
                    }
                    cache.error = Some(error.code);
                    cache.backoff = cache.backoff.saturating_mul(2).min(GIT_MAX_BACKOFF);
                    cache.next_attempt = Instant::now() + cache.backoff;
                }
            }
        }
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
            self.inner
                .git_bindings
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&input.key);
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
                self.inner
                    .git_bindings
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&input.key);
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
    }

    fn note_git_read_error(&self, code: &str) {
        let targets: Vec<_> = self
            .inner
            .git_workspaces
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect();
        for target in targets {
            target
                .cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .error = Some(code.to_owned());
        }
    }

    fn target(&self, agent: &str, generation: i64) -> Option<Arc<Target>> {
        self.inner
            .targets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&ProcessKey::Agent(agent.to_owned(), generation))
            .cloned()
    }

    async fn sample_all(&self) {
        let targets: Vec<_> = self
            .inner
            .targets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect();
        for target in targets {
            let _ = self.sample_target(&target).await;
        }
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
        let mut snapshot = match result {
            Ok(snapshot) => snapshot,
            Err(error) => {
                target
                    .cache
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .error = Some(error.code.clone());
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
            target
                .cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .error = Some(error.code.clone());
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
            target
                .cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .error = Some(error.code.clone());
            return Err(error);
        }
        *target
            .cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Cache {
            snapshot: Some(snapshot),
            sampled_at: Some(sampled_at),
            error: None,
        };
        if final_exit {
            let key = target.key.clone();
            let mut targets = self
                .inner
                .targets
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if targets
                .get(&key)
                .is_some_and(|current| Arc::ptr_eq(current, target))
            {
                targets.remove(&key);
            }
        }
        Ok(())
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

fn age_ms(now: i64, timestamp: i64) -> Option<i64> {
    now.checked_sub(timestamp).filter(|age| *age >= 0)
}

fn stats_view(cache: &Cache, unavailable: Option<String>, now: i64) -> Value {
    let sample_age_ms = cache.sampled_at.and_then(|at| age_ms(now, at));
    let clock_regressed = cache.sampled_at.is_some_and(|at| at > now);
    let stale = unavailable.is_some()
        || cache.error.is_some()
        || clock_regressed
        || sample_age_ms.is_some_and(|age| age > SAMPLE_TTL_MS);
    let available = cache.snapshot.is_some() && !stale;
    let coverage = match cache.snapshot.as_ref() {
        None => "unknown",
        Some(_) if stale => "stale",
        Some(snapshot) if snapshot.complete => "complete",
        Some(_) => "partial",
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
    json!({
        "available": available,
        "sampled_at": cache.sampled_at,
        "age_ms": sample_age_ms,
        "ttl_ms": SAMPLE_TTL_MS,
        "clock_regressed": clock_regressed,
        "coverage": coverage,
        "complete": cache.snapshot.as_ref().map(|snapshot| snapshot.complete),
        "cpu_time_ns": cpu_time_ns,
        "rss_bytes": rss_bytes,
        "samples": samples,
        "error": unavailable.or_else(|| cache.error.clone()),
    })
}

pub(crate) fn workspace_owner(agent: &str, target_generation: i64) -> String {
    format!("workspace:{agent}#{target_generation}")
}

fn owner(agent: &str, generation: i64) -> String {
    format!("agent:{agent}#{generation}")
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}
