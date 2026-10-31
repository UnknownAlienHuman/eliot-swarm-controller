//! Managed-worktree orchestration. Store owns intent; setup owns verification;
//! workspace_git owns Git identity/readback. Only this module runs their mutators.
use crate::{
    bus::Bus,
    common::now_ms,
    git_monitor::{canonical_root, reject_links},
    monitor::{Monitoring, WorkspaceProcessContext},
    platform::{CustodyObserver, OwnedChild},
    plugin_host::SecretSet,
    setup,
    workspace_git::WorkspaceGit,
};
use eliot_core::{
    Agent, Command as CoreCommand, CommandState, Error, Result, SecretPolicy, SubmitCommand,
    WorkspaceCancelOutcome, WorkspaceCreationOutcome, WorkspacePhase, WorkspacePreparation,
    WorkspacePreparationInput, WorkspaceRemovalOutcome, WorkspaceSetupOutcome,
};
use eliot_plugin::sync::LockExt;
use eliot_store::WorkspaceGitReservation;
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Component, Path, PathBuf},
    process::Stdio,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::Command,
    sync::{Mutex as AsyncMutex, OwnedMutexGuard, watch},
    time::{Instant, sleep_until, timeout_at},
};
use ulid::Ulid;

const OUTPUT_BYTES: usize = 128 * 1024;
const CLEANUP_TIME: Duration = Duration::from_secs(5);
const CANCEL_TIME: Duration = Duration::from_secs(15);
const GIT_TIME: Duration = Duration::from_secs(60);
const PAGE_SIZE: usize = 100;

type StoreHandle = Arc<AsyncMutex<eliot_store::Store>>;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Key {
    agent: String,
    target_generation: i64,
}

#[derive(Default)]
struct Slot {
    gate: Arc<AsyncMutex<()>>,
    operation: AsyncMutex<()>,
    active: Mutex<Option<ActiveRun>>,
    last: Mutex<Option<RunResult>>,
}

#[derive(Clone)]
struct ActiveRun {
    cancel: watch::Sender<bool>,
    done: watch::Receiver<Option<RunResult>>,
}

#[derive(Clone, Copy)]
enum RunState {
    Exited(i32),
    Interrupted,
}

#[derive(Clone, Copy)]
struct RunResult {
    state: RunState,
    // Empty known owned scope, not a universal descendant-exit proof on Unix.
    quiescent: bool,
    code: &'static str,
}

struct State {
    data_root: PathBuf,
    store: StoreHandle,
    bus: Bus,
    secrets: SecretSet,
    progress: watch::Sender<u64>,
    monitoring: Monitoring,
    slots: Mutex<HashMap<Key, Arc<Slot>>>,
    #[cfg(test)]
    deferred_git_effect: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

#[derive(Clone)]
pub(crate) struct WorkspaceManager {
    inner: Arc<State>,
}

pub(crate) fn custody_capability() -> Value {
    let capability = if cfg!(windows) {
        "windows_job_exact"
    } else if cfg!(target_os = "linux") {
        "unix_process_group_degraded"
    } else {
        "unsupported"
    };
    json!({
        "scope": "current_host",
        "capability": capability,
        "exact": cfg!(windows),
        "orphan_possible": !cfg!(windows),
    })
}

impl WorkspaceManager {
    pub(crate) fn new(
        data_root: PathBuf,
        store: StoreHandle,
        bus: Bus,
        secrets: SecretSet,
        progress: watch::Sender<u64>,
        monitoring: Monitoring,
    ) -> Self {
        Self {
            inner: Arc::new(State {
                data_root,
                store,
                bus,
                secrets,
                progress,
                monitoring,
                slots: Mutex::new(HashMap::new()),
                #[cfg(test)]
                deferred_git_effect: Mutex::new(None),
            }),
        }
    }

    #[cfg(test)]
    fn signal_deferred_git_effect(&self) {
        if let Some(sender) = self.inner.deferred_git_effect.lock_recover().take() {
            let _ = sender.send(());
        }
    }

    pub(crate) async fn freeze_launch(
        &self,
        repo: &str,
        branch: &str,
        base_ref: Option<&str>,
        profile: &Value,
    ) -> Result<WorkspacePreparationInput> {
        let target = self
            .inner
            .data_root
            .join("worktrees")
            .join(Ulid::new().to_string());
        let target = target
            .to_str()
            .ok_or_else(|| problem("invalid", "managed workspace path is not UTF-8"))?;
        let frozen = WorkspaceGit::new()?
            .freeze(repo, branch, base_ref, target)
            .await?;
        let frozen_setup = setup::freeze(profile, Path::new(&frozen.repo_path))?;
        let input = WorkspacePreparationInput {
            project: frozen.repo_path.clone(),
            repo_path: frozen.repo_path,
            repo_common_dir: frozen.repo_common_dir,
            requested_branch: frozen.requested_branch,
            base_ref: base_ref.map(str::to_owned),
            base_oid: frozen.base_oid,
            worktree_path: frozen.worktree_path,
            frozen_setup,
        };
        self.check_nonsecret(
            serde_json::to_value(&input)
                .map_err(|_| problem("invalid", "workspace preparation cannot be encoded"))?,
        )?;
        Ok(input)
    }

    pub(crate) async fn prepare(&self, command_id: &str) -> Result<()> {
        let Some((_, preparation)) = self.start(command_id).await? else {
            return Ok(());
        };
        let slot = self.slot(&preparation);
        let _operation = slot.operation.lock().await;
        let git = WorkspaceGit::new()?;
        loop {
            let Some((agent, current)) = self.start(command_id).await? else {
                return Err(problem(
                    "stale_generation",
                    "workspace preparation disappeared",
                ));
            };
            require_same(&preparation, &current)?;
            if cancelled(&current) {
                return Err(problem("bad_state", "workspace cancellation is pending"));
            }
            match current.phase {
                WorkspacePhase::Ready => {
                    let readback = git.inspect_retained(&current).await;
                    if !matches!(readback, WorkspaceCreationOutcome::Verified { .. }) {
                        return Err(problem(
                            "unknown",
                            "ready workspace identity is unavailable",
                        ));
                    }
                    let _gate = slot.gate.clone().lock_owned().await;
                    self.require_current(&current, WorkspacePhase::Ready)
                        .await?;
                    return Ok(());
                }
                WorkspacePhase::Creating => {
                    let reuse = current
                        .outcome
                        .as_ref()
                        .and_then(|value| value.get("reuse"))
                        .and_then(Value::as_bool)
                        == Some(true);
                    let outcome = if reuse {
                        git.inspect_retained(&current).await
                    } else {
                        match git.prepare_create(&current).await {
                            Ok(command) => {
                                let gate = slot.gate.clone().lock_owned().await;
                                self.require_current(&current, WorkspacePhase::Creating)
                                    .await?;
                                let mut progress = self.inner.progress.subscribe();
                                let observed_progress = *progress.borrow_and_update();
                                let reservation = {
                                    let mut store = self.inner.store.lock().await;
                                    store.begin_workspace_git_effect(
                                        command_id,
                                        &current,
                                        now_ms(),
                                    )?
                                };
                                match reservation {
                                    WorkspaceGitReservation::Reserved => {
                                        let result = self
                                            .run_owned(
                                                &slot,
                                                command,
                                                Instant::now() + GIT_TIME,
                                                gate,
                                                WorkspaceProcessContext::new(
                                                    &current,
                                                    command_id,
                                                    "workspace_create",
                                                    None,
                                                ),
                                            )
                                            .await;
                                        if !result.quiescent {
                                            creation_unknown(result.code)
                                        } else {
                                            git.inspect(&current).await
                                        }
                                    }
                                    WorkspaceGitReservation::Deferred => {
                                        drop(gate);
                                        #[cfg(test)]
                                        self.signal_deferred_git_effect();
                                        wait_for_git_write_progress(
                                            &mut progress,
                                            observed_progress,
                                        )
                                        .await;
                                        continue;
                                    }
                                    WorkspaceGitReservation::MayHaveBegun => {
                                        drop(gate);
                                        if workspace_git_effect_quiescent(&slot) {
                                            git.inspect(&current).await
                                        } else {
                                            creation_unknown("workspace_git_custody_unknown")
                                        }
                                    }
                                }
                            }
                            Err(_) => git.inspect(&current).await,
                        }
                    };
                    let _gate = slot.gate.clone().lock_owned().await;
                    self.require_current(&current, WorkspacePhase::Creating)
                        .await?;
                    let mut store = self.inner.store.lock().await;
                    let change = store.settle_workspace_creation(
                        command_id,
                        current.target_generation,
                        safe_creation(outcome),
                        now_ms(),
                    )?;
                    self.publish(&change.events);
                    if matches!(
                        change.value.phase,
                        WorkspacePhase::Unknown | WorkspacePhase::Failed
                    ) {
                        return Err(problem(
                            if change.value.phase == WorkspacePhase::Unknown {
                                "unknown"
                            } else {
                                "failed"
                            },
                            "workspace creation did not establish readiness",
                        ));
                    }
                }
                WorkspacePhase::SetupReady => {
                    // Every persisted successful predecessor is identity-checked without
                    // requiring a pristine worktree or resetting the next setup index.
                    if !matches!(
                        git.inspect_retained(&current).await,
                        WorkspaceCreationOutcome::Verified { .. }
                    ) {
                        return Err(problem(
                            "unknown",
                            "workspace identity changed before setup",
                        ));
                    }
                    let gate = slot.gate.clone().lock_owned().await;
                    self.require_current(&current, WorkspacePhase::SetupReady)
                        .await?;
                    let entry = current
                        .frozen_setup
                        .get(current.setup_index)
                        .ok_or_else(|| {
                            problem("source_integrity", "workspace setup index is invalid")
                        })?;
                    {
                        let mut store = self.inner.store.lock().await;
                        let change = store.begin_workspace_setup(
                            command_id,
                            current.target_generation,
                            current.setup_index,
                            now_ms(),
                        )?;
                        self.publish(&change.events);
                    }
                    // No Store lock is held while reading/verifying frozen files.
                    let verified = setup::build_environment(
                        &agent.profile_snapshot,
                        &current.agent,
                        current.target_generation,
                    )
                    .and_then(|environment| {
                        setup::verify_command(
                            entry,
                            Path::new(&current.worktree_path),
                            &environment,
                        )
                    });
                    let command = match verified {
                        Ok(command) => command,
                        Err(_) => {
                            let mut store = self.inner.store.lock().await;
                            let change = store.settle_workspace_setup(
                                command_id,
                                current.target_generation,
                                current.setup_index,
                                WorkspaceSetupOutcome::NotStarted {
                                    code: "setup_verification_failed".to_owned(),
                                    message: "frozen setup verification failed before spawn"
                                        .to_owned(),
                                },
                                now_ms(),
                            )?;
                            self.publish(&change.events);
                            return Err(problem("failed", "frozen setup command was not started"));
                        }
                    };
                    let result = self
                        .run_owned(
                            &slot,
                            command,
                            Instant::now() + Duration::from_millis(entry.timeout_ms),
                            gate,
                            WorkspaceProcessContext::new(
                                &current,
                                command_id,
                                "setup",
                                Some(current.setup_index),
                            ),
                        )
                        .await;
                    let _gate = slot.gate.clone().lock_owned().await;
                    let (_, fresh) = self
                        .start(command_id)
                        .await?
                        .ok_or_else(|| problem("not_found", "workspace preparation disappeared"))?;
                    require_same(&current, &fresh)?;
                    if cancelled(&fresh) {
                        return Err(problem("bad_state", "workspace cancellation is pending"));
                    }
                    let outcome = match result.state {
                        RunState::Exited(0) if result.quiescent => WorkspaceSetupOutcome::Succeeded,
                        RunState::Exited(exit_code) if result.quiescent => {
                            WorkspaceSetupOutcome::Failed { exit_code }
                        }
                        _ => WorkspaceSetupOutcome::Unknown {
                            code: result.code.to_owned(),
                            message: "setup completion or child quiescence is unknown".to_owned(),
                        },
                    };
                    let mut store = self.inner.store.lock().await;
                    let change = store.settle_workspace_setup(
                        command_id,
                        current.target_generation,
                        current.setup_index,
                        outcome,
                        now_ms(),
                    )?;
                    self.publish(&change.events);
                    if matches!(
                        change.value.phase,
                        WorkspacePhase::Unknown | WorkspacePhase::Failed
                    ) {
                        return Err(problem(
                            if change.value.phase == WorkspacePhase::Unknown {
                                "unknown"
                            } else {
                                "failed"
                            },
                            "workspace setup did not establish readiness",
                        ));
                    }
                }
                _ => {
                    return Err(problem(
                        "unknown",
                        "workspace phase does not authorize preparation replay",
                    ));
                }
            }
        }
    }

    pub(crate) async fn remove(&self, command: &CoreCommand) -> Result<Value> {
        let agent = command
            .target
            .as_deref()
            .ok_or_else(|| problem("invalid", "workspace removal has no Agent"))?;
        let preparation = self
            .inner
            .store
            .lock()
            .await
            .get_workspace(agent)?
            .ok_or_else(|| problem("not_found", "managed workspace was not found"))?;
        let slot = self.slot(&preparation);
        let _operation = slot.operation.lock().await;
        let outcome = match WorkspaceGit::new() {
            Ok(git) => {
                self.remove_prepared(command, &preparation, &slot, &git)
                    .await
            }
            Err(_) => removal_refused(),
        };
        let _gate = slot.gate.clone().lock_owned().await;
        let outcome = if matches!(&outcome, WorkspaceRemovalOutcome::Failed { .. })
            && self.require_removing(&preparation, command).await.is_err()
        {
            removal_unknown("workspace_removal_marker_changed")
        } else {
            outcome
        };
        let mut store = self.inner.store.lock().await;
        let change = store.settle_workspace_removal(
            &command.id,
            preparation.target_generation,
            safe_removal(outcome),
            now_ms(),
        )?;
        self.publish(&change.events);
        let command = store
            .get_command(&command.id)?
            .ok_or_else(|| problem("not_found", "removal command disappeared"))?;
        Ok(json!({"command_id": command.id, "state": command.state, "outcome": command.outcome}))
    }

    async fn remove_prepared(
        &self,
        command: &CoreCommand,
        preparation: &WorkspacePreparation,
        slot: &Arc<Slot>,
        git: &WorkspaceGit,
    ) -> WorkspaceRemovalOutcome {
        loop {
            let deadline = Instant::now() + GIT_TIME;
            if self
                .wait_git_sources_closed(preparation, command, slot, deadline)
                .await
                .is_err()
            {
                return removal_unknown("workspace_git_drain_unknown");
            }
            let scratch = {
                let _gate = slot.gate.clone().lock_owned().await;
                if self.require_removing(preparation, command).await.is_err() {
                    return removal_unknown("workspace_removal_marker_changed");
                }
                let store = self.inner.store.lock().await;
                let profile = match store.get_agent(&preparation.agent) {
                    Ok(Some(agent)) => agent.profile_snapshot,
                    _ => return removal_refused(),
                };
                match frozen_scratch(&profile) {
                    Ok(scratch) => scratch,
                    Err(_) => return removal_refused(),
                }
            };
            // Complete Git classification belongs to the low-level reader. It rejects
            // tracked dirt and returns only ordinary untracked/ignored file candidates.
            let candidates =
                match timeout_at(deadline, git.scratch_candidates(preparation, &scratch)).await {
                    Ok(Ok(candidates)) => candidates,
                    Ok(Err(error)) => return removal_preflight_error(&error),
                    Err(_) => return removal_unknown("workspace_scratch_deadline"),
                };
            let mut files = Vec::with_capacity(candidates.len());
            let mut seen = HashSet::new();
            for candidate in candidates {
                if Instant::now() >= deadline {
                    return removal_unknown("workspace_scratch_deadline");
                }
                let file = match self.checked_scratch_file(preparation, &scratch, &candidate) {
                    Ok(file) => file,
                    Err(error) => return removal_preflight_error(&error),
                };
                if !seen.insert(file.clone()) {
                    return removal_refused();
                }
                files.push(file);
            }
            let mut deleted = false;
            for file in files {
                let _gate = slot.gate.clone().lock_owned().await;
                if self.require_removing(preparation, command).await.is_err() {
                    return removal_unknown("workspace_removal_marker_changed");
                }
                if Instant::now() >= deadline {
                    return removal_unknown("workspace_scratch_deadline");
                }
                // Recheck immediately before each individual unlink, under the same
                // gate as forced shutdown. Never hold Store while doing filesystem I/O.
                let file = match self.checked_scratch_file(preparation, &scratch, &file) {
                    Ok(file) => file,
                    Err(_) => return removal_unknown("workspace_scratch_path_changed"),
                };
                if fs::remove_file(&file).is_err() {
                    return removal_unknown("workspace_scratch_delete_unknown");
                }
                deleted = true;
                if Instant::now() >= deadline {
                    return removal_unknown("workspace_scratch_deadline");
                }
            }
            // Empty directories are left to ordinary Git removal; never recurse over
            // a scratch root that might also contain clean tracked files.
            let mutator = match timeout_at(deadline, git.prepare_remove(preparation)).await {
                Ok(Ok(command)) => command,
                Ok(Err(error)) if !deleted => return removal_preflight_error(&error),
                Ok(Err(_)) => return removal_unknown("workspace_remove_after_scratch"),
                Err(_) => return removal_unknown("workspace_remove_deadline"),
            };
            let gate = slot.gate.clone().lock_owned().await;
            if self.require_removing(preparation, command).await.is_err() {
                return removal_unknown("workspace_removal_marker_changed");
            }
            if Instant::now() >= deadline {
                return removal_unknown("workspace_remove_deadline");
            }
            let mut progress = self.inner.progress.subscribe();
            let observed_progress = *progress.borrow_and_update();
            let reservation = {
                let mut store = self.inner.store.lock().await;
                store.begin_workspace_git_effect(&command.id, preparation, now_ms())
            };
            match reservation {
                Err(error) => return removal_preflight_error(&error),
                Ok(WorkspaceGitReservation::Deferred) => {
                    drop(gate);
                    #[cfg(test)]
                    self.signal_deferred_git_effect();
                    wait_for_git_write_progress(&mut progress, observed_progress).await;
                    continue;
                }
                Ok(WorkspaceGitReservation::MayHaveBegun) => {
                    drop(gate);
                    if !workspace_git_effect_quiescent(slot) {
                        return removal_unknown("workspace_git_custody_unknown");
                    }
                    return match timeout_at(deadline, git.inspect_removal(preparation)).await {
                        Ok(removed @ WorkspaceRemovalOutcome::Removed { .. }) => removed,
                        _ => removal_unknown("workspace_removal_unconfirmed"),
                    };
                }
                Ok(WorkspaceGitReservation::Reserved) => {
                    let result = self
                        .run_owned(
                            slot,
                            mutator,
                            deadline,
                            gate,
                            WorkspaceProcessContext::new(
                                preparation,
                                &command.id,
                                "workspace_remove",
                                None,
                            ),
                        )
                        .await;
                    if !result.quiescent {
                        return removal_unknown(result.code);
                    }
                    // A nonzero Git exit never proves no destructive effect. Only exact
                    // absence and retained-branch readback can establish successful removal.
                    return match timeout_at(deadline, git.inspect_removal(preparation)).await {
                        Ok(removed @ WorkspaceRemovalOutcome::Removed { .. }) => removed,
                        _ => removal_unknown("workspace_removal_unconfirmed"),
                    };
                }
            }
        }
    }

    async fn wait_git_sources_closed(
        &self,
        preparation: &WorkspacePreparation,
        command: &CoreCommand,
        slot: &Arc<Slot>,
        deadline: Instant,
    ) -> Result<()> {
        loop {
            let open = timeout_at(deadline, async {
                let _gate = slot.gate.clone().lock_owned().await;
                self.require_removing(preparation, command).await?;
                let store = self.inner.store.lock().await;
                store.has_open_git_sources_within(&preparation.worktree_path)
            })
            .await
            .map_err(|_| problem("unknown", "Git source drain deadline elapsed"))??;
            if Instant::now() >= deadline {
                return Err(problem("unknown", "Git source drain deadline elapsed"));
            }
            if !open {
                return Ok(());
            }
            // Source closure follows the monitor's final flush and Git child
            // quiescence. No Store lock or cancellation/spawn gate is held while waiting.
            sleep_until((Instant::now() + Duration::from_millis(100)).min(deadline)).await;
        }
    }

    async fn require_removing(
        &self,
        expected: &WorkspacePreparation,
        command: &CoreCommand,
    ) -> Result<()> {
        let store = self.inner.store.lock().await;
        let current = store
            .get_workspace(&expected.agent)?
            .ok_or_else(|| problem("not_found", "workspace preparation disappeared"))?;
        require_same(expected, &current)?;
        let retained = store
            .get_command(&command.id)?
            .ok_or_else(|| problem("not_found", "removal command disappeared"))?;
        let owner = store
            .get_agent(&expected.agent)?
            .ok_or_else(|| problem("not_found", "workspace owner disappeared"))?;
        if current.phase != WorkspacePhase::Removing
            || cancelled(&current)
            || retained.verb != "worktree.remove"
            || retained.state != CommandState::Dispatched
            || retained.target.as_deref() != Some(expected.agent.as_str())
            || retained.generation != Some(owner.generation)
            || !owner.is_terminal()
            || current
                .outcome
                .as_ref()
                .and_then(|value| value.get("stage"))
                .and_then(Value::as_str)
                != Some("removal")
            || current
                .outcome
                .as_ref()
                .and_then(|value| value.get("command_id"))
                .and_then(Value::as_str)
                != Some(command.id.as_str())
        {
            return Err(problem(
                "unknown",
                "workspace removal marker or authority changed",
            ));
        }
        Ok(())
    }

    fn checked_scratch_file(
        &self,
        preparation: &WorkspacePreparation,
        scratch: &[String],
        candidate: &Path,
    ) -> Result<PathBuf> {
        let managed = self.inner.data_root.join("worktrees");
        let managed = canonical_root(
            managed
                .to_str()
                .ok_or_else(|| problem("invalid", "managed workspace root is not UTF-8"))?,
        )?;
        let workspace = canonical_root(&preparation.worktree_path)?;
        let source = canonical_root(&preparation.repo_path)?;
        if workspace == managed
            || !workspace.starts_with(&managed)
            || workspace == source
            || !candidate.is_absolute()
        {
            return Err(problem(
                "invalid",
                "scratch file is outside its managed worktree",
            ));
        }
        reject_path_components(candidate)?;
        reject_links(candidate)?;
        let metadata = fs::symlink_metadata(candidate)
            .map_err(|_| problem("unknown", "scratch file cannot be inspected"))?;
        if !metadata.file_type().is_file() {
            return Err(problem(
                "invalid",
                "scratch candidate is not a regular file",
            ));
        }
        let file = fs::canonicalize(candidate)
            .map_err(|_| problem("unknown", "scratch file cannot be resolved"))?;
        if !file.starts_with(&workspace) || file == workspace {
            return Err(problem("invalid", "scratch file escaped its worktree"));
        }
        for directory in scratch {
            let path = workspace.join(directory);
            reject_links(&path)?;
            let Some(text) = path.to_str() else { continue };
            if let Ok(root) = canonical_root(text)
                && root != workspace
                && root.starts_with(&workspace)
                && file != root
                && file.starts_with(&root)
            {
                return Ok(file);
            }
        }
        Err(problem(
            "invalid",
            "file is outside admitted scratch directories",
        ))
    }

    pub(crate) async fn recover(&self) -> Result<Vec<String>> {
        let mut forward = Vec::new();
        let preparations = self.preparations().await?;
        if preparations.is_empty() {
            return Ok(forward);
        }
        let git = WorkspaceGit::new()?;
        for preparation in preparations {
            let slot = self.slot(&preparation);
            let _operation = slot.operation.lock().await;
            if !workspace_git_effect_quiescent(&slot) {
                continue;
            }
            let stage = preparation
                .outcome
                .as_ref()
                .and_then(|value| value.get("stage"))
                .and_then(Value::as_str);
            if preparation.phase == WorkspacePhase::Unknown {
                match stage {
                    Some("creation") if !cancelled(&preparation) => {
                        let reuse = preparation
                            .outcome
                            .as_ref()
                            .and_then(|value| value.get("reuse"))
                            .and_then(Value::as_bool)
                            == Some(true);
                        let outcome = if reuse {
                            git.inspect_retained(&preparation).await
                        } else {
                            git.inspect(&preparation).await
                        };
                        if matches!(outcome, WorkspaceCreationOutcome::Verified { .. }) {
                            let _gate = slot.gate.clone().lock_owned().await;
                            let mut store = self.inner.store.lock().await;
                            let change = store.settle_workspace_creation(
                                &preparation.start_command_id,
                                preparation.target_generation,
                                safe_creation(outcome),
                                now_ms(),
                            )?;
                            self.publish(&change.events);
                        }
                    }
                    Some("removal") => {
                        let id = preparation
                            .outcome
                            .as_ref()
                            .and_then(|value| value.get("command_id"))
                            .and_then(Value::as_str)
                            .ok_or_else(|| {
                                problem("source_integrity", "unknown removal has no command id")
                            })?;
                        if let outcome @ WorkspaceRemovalOutcome::Removed { .. } =
                            git.inspect_removal(&preparation).await
                        {
                            let _gate = slot.gate.clone().lock_owned().await;
                            let mut store = self.inner.store.lock().await;
                            let change = store.settle_workspace_removal(
                                id,
                                preparation.target_generation,
                                outcome,
                                now_ms(),
                            )?;
                            self.publish(&change.events);
                        }
                    }
                    _ => {}
                }
            }
            let store = self.inner.store.lock().await;
            let current = store
                .get_workspace(&preparation.agent)?
                .ok_or_else(|| problem("not_found", "workspace disappeared during recovery"))?;
            let command = store
                .get_command(&current.start_command_id)?
                .ok_or_else(|| problem("source_integrity", "workspace start is missing"))?;
            let agent = store
                .get_agent(&current.agent)?
                .ok_or_else(|| problem("not_found", "workspace Agent disappeared"))?;
            let eligible = matches!(
                current.phase,
                WorkspacePhase::Ready | WorkspacePhase::SetupReady
            ) && !cancelled(&current)
                && matches!(command.state, CommandState::Accepted | CommandState::Queued)
                && agent.generation.checked_add(1) == Some(current.target_generation)
                && command.generation == Some(agent.generation);
            drop(store);
            if eligible
                && matches!(
                    git.inspect_retained(&current).await,
                    WorkspaceCreationOutcome::Verified { .. }
                )
            {
                forward.push(current.start_command_id);
            }
        }
        Ok(forward)
    }

    pub(crate) fn has_owned_processes(&self) -> bool {
        self.inner
            .slots
            .lock_recover()
            .values()
            .any(|slot| slot.active.lock_recover().is_some() || slot.operation.try_lock().is_err())
    }

    pub(crate) async fn stop_all(&self, force: bool) {
        if !force {
            // Drain rejects new starts in Router and waits for this manager's blockers.
            return;
        }
        let Ok(preparations) = self.preparations().await else {
            return;
        };
        for preparation in preparations {
            if preparation.phase == WorkspacePhase::Removing {
                let slot = self.slot(&preparation);
                let _gate = slot.gate.clone().lock_owned().await;
                let Some(id) = preparation
                    .outcome
                    .as_ref()
                    .and_then(|value| value.get("command_id"))
                    .and_then(Value::as_str)
                else {
                    continue;
                };
                let mut store = self.inner.store.lock().await;
                let Ok(change) = store.settle_workspace_removal(
                    id,
                    preparation.target_generation,
                    removal_unknown("workspace_shutdown"),
                    now_ms(),
                ) else {
                    continue;
                };
                self.publish(&change.events);
                drop(store);
                if let Some(active) = slot.active.lock_recover().as_ref() {
                    active.cancel.send_replace(true);
                }
                continue;
            }
            let id = if let Some(id) = stop_id(&preparation) {
                id.to_owned()
            } else {
                let mut store = self.inner.store.lock().await;
                let Ok(Some(start)) = store.get_command(&preparation.start_command_id) else {
                    continue;
                };
                if !matches!(
                    start.state,
                    CommandState::Accepted | CommandState::Queued | CommandState::Unknown
                ) {
                    continue;
                }
                let Ok(Some(agent)) = store.get_agent(&preparation.agent) else {
                    continue;
                };
                let id = Ulid::new().to_string();
                let request = SubmitCommand {
                    id: id.clone(),
                    target: Some(agent.id),
                    verb: "stop".to_owned(),
                    args: json!({"force": true}),
                    expected_generation: Some(agent.generation),
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                };
                let Ok(admission) = store.submit("owner", request, now_ms()) else {
                    continue;
                };
                self.publish(&admission.events);
                id
            };
            let _ = self.cancel(&id).await;
        }
        // Removal cancellation also waits for the owner, without replaying a signal.
        let slots: Vec<_> = self.inner.slots.lock_recover().values().cloned().collect();
        for slot in slots {
            let active = slot.active.lock_recover().clone();
            if let Some(mut active) = active {
                let deadline = Instant::now() + CANCEL_TIME;
                while active.done.borrow().is_none() {
                    if !matches!(
                        timeout_at(deadline, active.done.changed()).await,
                        Ok(Ok(()))
                    ) {
                        break;
                    }
                }
            }
        }
    }

    async fn preparations(&self) -> Result<Vec<WorkspacePreparation>> {
        let mut result = Vec::new();
        let mut after = None;
        loop {
            let page = self
                .inner
                .store
                .lock()
                .await
                .list_workspace_preparations(after.as_deref(), PAGE_SIZE)?;
            if page.is_empty() {
                return Ok(result);
            }
            after = page.last().map(|preparation| preparation.agent.clone());
            result.extend(page);
        }
    }

    async fn start(&self, id: &str) -> Result<Option<(Agent, WorkspacePreparation)>> {
        let store = self.inner.store.lock().await;
        let command = store
            .get_command(id)?
            .ok_or_else(|| problem("not_found", "workspace command was not found"))?;
        let target = command
            .target
            .as_deref()
            .ok_or_else(|| problem("invalid", "workspace command has no Agent"))?;
        let Some(preparation) = store.get_workspace(target)? else {
            return Ok(None);
        };
        let agent = store
            .get_agent(target)?
            .ok_or_else(|| problem("not_found", "workspace Agent was not found"))?;
        if preparation.start_command_id != id
            || !matches!(command.verb.as_str(), "launch" | "resume")
            || !matches!(
                command.state,
                CommandState::Accepted | CommandState::Queued | CommandState::Unknown
            )
            || agent.generation.checked_add(1) != Some(preparation.target_generation)
        {
            return Err(problem(
                "stale_generation",
                "workspace no longer precedes its exact start",
            ));
        }
        Ok(Some((agent, preparation)))
    }

    async fn require_current(
        &self,
        expected: &WorkspacePreparation,
        phase: WorkspacePhase,
    ) -> Result<()> {
        let store = self.inner.store.lock().await;
        let actual = store
            .get_workspace(&expected.agent)?
            .ok_or_else(|| problem("not_found", "workspace preparation disappeared"))?;
        require_same(expected, &actual)?;
        if actual.phase != phase || actual.setup_index != expected.setup_index || cancelled(&actual)
        {
            return Err(problem(
                "bad_state",
                "workspace marker changed or cancellation is pending",
            ));
        }
        Ok(())
    }

    fn slot(&self, preparation: &WorkspacePreparation) -> Arc<Slot> {
        // Cancellation reserves this same gate even before the first spawn slot exists.
        self.inner
            .slots
            .lock_recover()
            .entry(Key {
                agent: preparation.agent.clone(),
                target_generation: preparation.target_generation,
            })
            .or_default()
            .clone()
    }

    fn publish(&self, events: &[eliot_core::Event]) {
        self.inner.bus.publish(events);
        self.inner.progress.send_modify(|value| {
            *value = value.wrapping_add(1);
        });
    }

    fn check_nonsecret(&self, value: Value) -> Result<()> {
        let secrets: Vec<_> = self.inner.secrets.lock_recover().iter().cloned().collect();
        if SecretPolicy::new(secrets).contains_value(&value) {
            return Err(problem(
                "secret_in_payload",
                "workspace preparation contains a known credential",
            ));
        }
        Ok(())
    }

    pub(crate) async fn cancel(&self, stop_command_id: &str) -> Result<bool> {
        let preparation = {
            let store = self.inner.store.lock().await;
            let stop = store
                .get_command(stop_command_id)?
                .ok_or_else(|| problem("not_found", "stop command was not found"))?;
            if stop.verb != "stop" {
                return Err(problem(
                    "invalid",
                    "workspace cancellation requires a stop command",
                ));
            }
            let Some(agent) = stop.target.as_deref() else {
                return Ok(false);
            };
            let Some(preparation) = store.get_workspace(agent)? else {
                return Ok(false);
            };
            let start = store
                .get_command(&preparation.start_command_id)?
                .ok_or_else(|| problem("source_integrity", "workspace start is missing"))?;
            if !matches!(
                start.state,
                CommandState::Accepted | CommandState::Queued | CommandState::Unknown
            ) || matches!(
                preparation.phase,
                WorkspacePhase::Removing | WorkspacePhase::Removed
            ) {
                return Ok(false);
            }
            preparation
        };
        let slot = self.slot(&preparation);
        let gate = slot.gate.clone().lock_owned().await;
        let phase = {
            let mut store = self.inner.store.lock().await;
            let current = store
                .get_workspace(&preparation.agent)?
                .ok_or_else(|| problem("not_found", "workspace preparation disappeared"))?;
            require_same(&preparation, &current)?;
            if cancelled(&current) {
                if stop_id(&current) != Some(stop_command_id) {
                    return Err(problem(
                        "unknown",
                        "another workspace cancellation is unresolved",
                    ));
                }
            } else {
                let change = store.request_workspace_cancel(
                    stop_command_id,
                    &current.start_command_id,
                    current.target_generation,
                    now_ms(),
                )?;
                self.publish(&change.events);
            }
            current.phase
        };
        // The durable marker is committed before any cancellation signal is sent.
        let active = slot.active.lock_recover().clone();
        if let Some(active) = &active {
            active.cancel.send_replace(true);
        }
        drop(gate);
        let quiescent = match active {
            Some(mut active) => {
                let deadline = Instant::now() + CANCEL_TIME;
                loop {
                    if let Some(result) = *active.done.borrow() {
                        break result.quiescent;
                    }
                    if !matches!(
                        timeout_at(deadline, active.done.changed()).await,
                        Ok(Ok(()))
                    ) {
                        break false;
                    }
                }
            }
            None => match *slot.last.lock_recover() {
                Some(result) => result.quiescent,
                // After exclusive daemon acquisition, prior Windows kill-on-close
                // Jobs cannot survive. This proves physical custody only: the
                // cancellation below withdraws the start, never succeeds/replays setup.
                // A retained failed current run above cannot use this prior-custody rule.
                None => {
                    cfg!(windows)
                        || !matches!(
                            phase,
                            WorkspacePhase::Unknown | WorkspacePhase::SetupRunning
                        )
                }
            },
        };
        let _gate = slot.gate.clone().lock_owned().await;
        let mut store = self.inner.store.lock().await;
        let outcome = if quiescent {
            WorkspaceCancelOutcome::Stopped
        } else {
            WorkspaceCancelOutcome::Unknown {
                code: "workspace_custody_unknown".to_owned(),
                message: "workspace process quiescence is not confirmed".to_owned(),
            }
        };
        let change = store.settle_workspace_cancel(
            stop_command_id,
            &preparation.start_command_id,
            preparation.target_generation,
            outcome,
            now_ms(),
        )?;
        self.publish(&change.events);
        Ok(true)
    }

    // Caller holds the same per-target gate used by cancel, and has checked/committed
    // the exact Store marker. The gate is released only after handle registration.
    async fn run_owned(
        &self,
        slot: &Arc<Slot>,
        mut command: Command,
        deadline: Instant,
        gate: OwnedMutexGuard<()>,
        context: WorkspaceProcessContext,
    ) -> RunResult {
        // Commit observation ownership before spawning, without advancing the
        // Agent's native generation or granting any control authority.
        if self
            .inner
            .monitoring
            .prepare_workspace_process(&context)
            .await
            .is_err()
            || Instant::now() >= deadline
        {
            let result = RunResult {
                state: RunState::Interrupted,
                quiescent: true,
                code: "workspace_process_not_started",
            };
            *slot.last.lock_recover() = Some(result);
            drop(gate);
            return result;
        }
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = match OwnedChild::spawn(command) {
            Ok(child) => child,
            Err(_) => {
                let _ = self
                    .inner
                    .monitoring
                    .workspace_process_unavailable(&context)
                    .await;
                let result = RunResult {
                    state: RunState::Interrupted,
                    quiescent: false,
                    code: "workspace_spawn_unknown",
                };
                *slot.last.lock_recover() = Some(result);
                drop(gate);
                return result;
            }
        };
        // Capture exact birth/Job identity before wait() can reap the leader.
        // This observer never authorizes a signal, including on Unix.
        let observer = child.observer().ok();
        let stdout = child.take_stdout();
        let stderr = child.take_stderr();
        let (cancel, mut cancellation) = watch::channel(false);
        let (done, done_rx) = watch::channel(None);
        *slot.active.lock_recover() = Some(ActiveRun {
            cancel,
            done: done_rx,
        });
        drop(gate);
        let observation = match observer.as_ref() {
            Some(observer) => self
                .inner
                .monitoring
                .attach_workspace_process(context.clone(), observer.clone())
                .await
                .ok(),
            None => None,
        };
        if observation.is_none() {
            let _ = self
                .inner
                .monitoring
                .workspace_process_unavailable(&context)
                .await;
        }
        let result = if let (Some(stdout), Some(stderr)) = (stdout, stderr) {
            let capture = async {
                let (_, _, status) =
                    tokio::try_join!(drain_bounded(stdout), drain_bounded(stderr), child.wait(),)?;
                Ok::<_, Error>(status)
            };
            let captured = tokio::select! {
                biased;
                _ = cancellation.changed() => Err("workspace_cancelled"),
                _ = sleep_until(deadline) => Err("workspace_timeout"),
                result = capture => result.map_err(|_| "workspace_capture_unknown"),
            };
            match captured {
                Ok(status) => {
                    #[cfg(windows)]
                    let quiescent = matches!(child.wait_family_empty(CLEANUP_TIME).await, Ok(true));
                    #[cfg(unix)]
                    let quiescent =
                        wait_known_group_empty(observer.as_ref(), Instant::now() + CLEANUP_TIME)
                            .await;
                    #[cfg(not(any(windows, unix)))]
                    let quiescent = false;
                    #[cfg(windows)]
                    if !quiescent {
                        let _ = cleanup_owned(&mut child, observer.as_ref()).await;
                    }
                    match status.code() {
                        Some(code) if !cfg!(windows) || quiescent => RunResult {
                            state: RunState::Exited(code),
                            quiescent,
                            code: "workspace_process_exit",
                        },
                        _ => RunResult {
                            state: RunState::Interrupted,
                            quiescent,
                            code: "workspace_exit_unknown",
                        },
                    }
                }
                Err(code) => RunResult {
                    state: RunState::Interrupted,
                    quiescent: cleanup_owned(&mut child, observer.as_ref()).await,
                    code,
                },
            }
        } else {
            RunResult {
                state: RunState::Interrupted,
                quiescent: cleanup_owned(&mut child, observer.as_ref()).await,
                code: "workspace_pipes_unavailable",
            }
        };
        // Persist the terminal edge while the exact owned child/observer still
        // exists; the periodic sampler alone would miss short-lived setup.
        if let Some(observation) = observation {
            let _ = self
                .inner
                .monitoring
                .finish_workspace_process(&observation)
                .await;
        }
        let _gate = slot.gate.clone().lock_owned().await;
        *slot.last.lock_recover() = Some(result);
        *slot.active.lock_recover() = None;
        let _ = done.send_replace(Some(result));
        self.inner
            .progress
            .send_modify(|value| *value = value.wrapping_add(1));
        result
    }
}

async fn wait_for_git_write_progress(progress: &mut watch::Receiver<u64>, observed: u64) {
    if *progress.borrow_and_update() == observed {
        let _ = progress.changed().await;
    }
}

fn workspace_git_effect_quiescent(slot: &Slot) -> bool {
    if slot.active.lock_recover().is_some() {
        return false;
    }
    // `last` records the owned process-family result. After exclusive daemon
    // acquisition, Windows Job close proves a prior child family empty; Unix
    // cannot infer quiescence when no birth-checked result was retained.
    match *slot.last.lock_recover() {
        Some(result) => result.quiescent,
        None => cfg!(windows),
    }
}

async fn drain_bounded<R: AsyncRead + Unpin>(mut stream: R) -> Result<()> {
    let mut buffer = [0_u8; 4096];
    let mut total = 0_usize;
    loop {
        let count = stream
            .read(&mut buffer)
            .await
            .map_err(|_| problem("unknown", "workspace output read failed"))?;
        if count == 0 {
            return Ok(());
        }
        total = total.saturating_add(count);
        if total > OUTPUT_BYTES {
            return Err(problem(
                "unknown",
                "workspace output exceeds its capture bound",
            ));
        }
    }
}

async fn cleanup_owned(child: &mut OwnedChild, observer: Option<&CustodyObserver>) -> bool {
    let signalled = child.terminate().is_ok();
    let deadline = Instant::now() + CLEANUP_TIME;
    let reaped = matches!(timeout_at(deadline, child.wait()).await, Ok(Ok(_)));
    #[cfg(windows)]
    {
        let _ = observer;
        signalled && reaped && matches!(child.wait_family_empty(CLEANUP_TIME).await, Ok(true))
    }
    #[cfg(unix)]
    {
        let _ = signalled;
        reaped && wait_known_group_empty(observer, deadline).await
    }
    #[cfg(not(any(windows, unix)))]
    {
        let _ = (signalled, reaped, observer);
        false
    }
}

#[cfg(unix)]
async fn wait_known_group_empty(observer: Option<&CustodyObserver>, deadline: Instant) -> bool {
    let Some(observer) = observer else {
        return false;
    };
    loop {
        if Instant::now() >= deadline {
            return false;
        }
        match observer.known_process_group_empty() {
            Ok(true) => return true,
            Ok(false) => {}
            Err(_) => return false,
        }
        // Saved-PGID observation only. No new signal or PID-only adoption.
        sleep_until((Instant::now() + Duration::from_millis(50)).min(deadline)).await;
    }
}

fn require_same(expected: &WorkspacePreparation, actual: &WorkspacePreparation) -> Result<()> {
    if expected.agent != actual.agent
        || expected.target_generation != actual.target_generation
        || expected.start_command_id != actual.start_command_id
    {
        return Err(problem(
            "stale_generation",
            "workspace preparation identity changed",
        ));
    }
    Ok(())
}

fn cancelled(preparation: &WorkspacePreparation) -> bool {
    preparation
        .outcome
        .as_ref()
        .and_then(|value| value.get("cancel_requested"))
        .and_then(Value::as_bool)
        == Some(true)
}

fn stop_id(preparation: &WorkspacePreparation) -> Option<&str> {
    preparation
        .outcome
        .as_ref()
        .and_then(|value| value.get("stop_command_id"))
        .and_then(Value::as_str)
}

fn problem(code: &str, message: &str) -> Error {
    Error::new(code, message)
}

fn frozen_scratch(profile: &Value) -> Result<Vec<String>> {
    let directories = match profile.get("scratch") {
        None => return Ok(vec!["target/".to_owned(), "node_modules/".to_owned()]),
        Some(Value::Array(values)) if values.len() <= 64 => values,
        _ => return Err(problem("invalid", "frozen scratch directories are invalid")),
    };
    let mut scratch = Vec::with_capacity(directories.len());
    for directory in directories {
        let directory = directory
            .as_str()
            .ok_or_else(|| problem("invalid", "frozen scratch directory is not text"))?;
        let relative = directory.trim_end_matches('/');
        if relative.is_empty()
            || directory.len() > 4096
            || directory.contains(['\\', ':', '\0'])
            || relative.split('/').any(|part| {
                part.is_empty() || part == "." || part == ".." || part.eq_ignore_ascii_case(".git")
            })
        {
            return Err(problem(
                "invalid",
                "frozen scratch directory is not a safe relative path",
            ));
        }
        scratch.push(directory.to_owned());
    }
    Ok(scratch)
}

fn reject_path_components(path: &Path) -> Result<()> {
    for component in path.components() {
        match component {
            Component::ParentDir | Component::CurDir => {
                return Err(problem("invalid", "scratch path contains traversal"));
            }
            Component::Normal(part) => {
                let part = part
                    .to_str()
                    .ok_or_else(|| problem("invalid", "scratch path is not UTF-8"))?;
                if part.contains([':', '\0']) || part.eq_ignore_ascii_case(".git") {
                    return Err(problem(
                        "invalid",
                        "scratch path contains Git metadata or an alternate stream",
                    ));
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn removal_refused() -> WorkspaceRemovalOutcome {
    WorkspaceRemovalOutcome::Failed {
        code: "workspace_remove_refused".to_owned(),
        message: "worktree removal was refused before destructive effects".to_owned(),
    }
}

fn removal_preflight_error(error: &Error) -> WorkspaceRemovalOutcome {
    if matches!(
        error.code.as_str(),
        "dirty" | "not_allowed" | "invalid" | "bad_state"
    ) {
        removal_refused()
    } else {
        removal_unknown("workspace_scratch_preflight_unknown")
    }
}

fn creation_unknown(code: &str) -> WorkspaceCreationOutcome {
    WorkspaceCreationOutcome::Unknown {
        code: code.to_owned(),
        message: "workspace creation or child quiescence is unknown".to_owned(),
    }
}

fn removal_unknown(code: &str) -> WorkspaceRemovalOutcome {
    WorkspaceRemovalOutcome::Unknown {
        code: code.to_owned(),
        message: "workspace removal or child quiescence is unknown".to_owned(),
    }
}

fn safe_creation(outcome: WorkspaceCreationOutcome) -> WorkspaceCreationOutcome {
    match outcome {
        verified @ WorkspaceCreationOutcome::Verified { .. } => verified,
        WorkspaceCreationOutcome::Failed { .. } => WorkspaceCreationOutcome::Failed {
            code: "workspace_creation_failed".to_owned(),
            message: "workspace creation readback proved no created subject".to_owned(),
        },
        WorkspaceCreationOutcome::Unknown { .. } => creation_unknown("workspace_creation_unknown"),
    }
}

fn safe_removal(outcome: WorkspaceRemovalOutcome) -> WorkspaceRemovalOutcome {
    match outcome {
        removed @ WorkspaceRemovalOutcome::Removed { .. } => removed,
        WorkspaceRemovalOutcome::Failed { .. } => WorkspaceRemovalOutcome::Failed {
            code: "workspace_remove_refused".to_owned(),
            message: "workspace removal was refused before mutation spawn".to_owned(),
        },
        WorkspaceRemovalOutcome::Unknown { .. } => removal_unknown("workspace_removal_unknown"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command as GitCommand;

    struct TestRoot(PathBuf);

    impl TestRoot {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "eliot-workspace-git-fence-{}",
                Ulid::new().to_string().to_ascii_lowercase()
            ));
            fs::create_dir_all(&path).expect("create workspace Git fixture root");
            Self(path)
        }
    }

    impl Drop for TestRoot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn git(checkout: &Path, args: &[&str]) -> String {
        let output = GitCommand::new("git")
            .arg("-C")
            .arg(checkout)
            .args(args)
            .output()
            .expect("run local Git fixture command");
        assert!(
            output.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout)
            .expect("Git fixture output is UTF-8")
            .trim()
            .to_owned()
    }

    async fn admit_workspace(
        store: &StoreHandle,
        repo_path: &str,
        worktrees: &Path,
        label: &str,
    ) -> (String, String, String, String, WorkspacePreparation) {
        let id = Ulid::new().to_string();
        let suffix = Ulid::new().to_string().to_ascii_lowercase();
        let branch = format!("codex/{label}-{suffix}");
        let requested_worktree_path = worktrees
            .join(format!("{label}-{suffix}"))
            .to_string_lossy()
            .into_owned();
        let frozen = WorkspaceGit::new()
            .expect("resolve installed Git")
            .freeze(repo_path, &branch, Some("HEAD"), &requested_worktree_path)
            .await
            .expect("freeze canonical workspace identity");
        let input = WorkspacePreparationInput {
            project: frozen.repo_path.clone(),
            repo_path: frozen.repo_path.clone(),
            repo_common_dir: frozen.repo_common_dir.clone(),
            requested_branch: frozen.requested_branch.clone(),
            base_ref: Some("HEAD".to_owned()),
            base_oid: frozen.base_oid.clone(),
            worktree_path: frozen.worktree_path.clone(),
            frozen_setup: Vec::new(),
        };
        let request = SubmitCommand {
            id: id.clone(),
            target: None,
            verb: "launch".to_owned(),
            args: json!({
                "project": frozen.repo_path.clone(),
                "workspace": frozen.repo_path.clone(),
                "worktree": frozen.requested_branch.clone(),
                "base": "HEAD",
                "harness": "fake",
                "profile": "fake",
                "profile_snapshot": {},
            }),
            expected_generation: Some(0),
            expected_turn_id: None,
            request_id: None,
            probe: false,
        };
        let mut store = store.lock().await;
        let admission = store
            .submit_with_workspace("owner", request, input, now_ms())
            .expect("admit managed workspace launch");
        let agent = admission.command.target.expect("launch Agent target");
        let preparation = store
            .get_workspace(&agent)
            .expect("read prepared workspace")
            .expect("workspace row");
        (
            id,
            agent,
            frozen.worktree_path,
            frozen.requested_branch,
            preparation,
        )
    }

    #[tokio::test]
    async fn workspace_git_fence_defers_before_spawn_and_never_replays_may_have_begun() {
        let root = TestRoot::new();
        let checkout = root.0.join("checkout");
        fs::create_dir_all(&checkout).expect("create local Git checkout");
        git(&checkout, &["init", "--quiet", "--initial-branch=main"]);
        git(&checkout, &["config", "user.name", "Workspace Fence Test"]);
        git(
            &checkout,
            &["config", "user.email", "workspace-fence@example.invalid"],
        );
        git(
            &checkout,
            &["commit", "--quiet", "--allow-empty", "-m", "base"],
        );
        let checkout_input = checkout.to_string_lossy().into_owned();
        let (repo_path, _) = WorkspaceGit::new()
            .expect("resolve installed Git")
            .repository_identity(&checkout_input)
            .await
            .expect("read repository identity");
        let worktrees = root.0.join("worktrees");
        fs::create_dir_all(&worktrees).expect("create worktree parent");

        let store = Arc::new(AsyncMutex::new(
            eliot_store::Store::open(Path::new(":memory:")).expect("open test Store"),
        ));
        let bus = Bus::new(32);
        let secrets: SecretSet = Arc::new(Mutex::new(HashSet::new()));
        let (progress, _progress_rx) = watch::channel(0_u64);
        let monitoring = Monitoring::new(store.clone(), bus.clone(), secrets.clone());
        let manager = WorkspaceManager::new(
            root.0.join("data"),
            store.clone(),
            bus,
            secrets,
            progress,
            monitoring,
        );

        let (blocker_id, _, _, _, blocker) =
            admit_workspace(&store, &repo_path, &worktrees, "blocker").await;
        let (deferred_id, deferred_agent, deferred_path, deferred_branch, _) =
            admit_workspace(&store, &repo_path, &worktrees, "deferred").await;
        let blocker_reservation = store
            .lock()
            .await
            .begin_workspace_git_effect(&blocker_id, &blocker, now_ms())
            .expect("reserve first repository writer");
        assert_eq!(blocker_reservation, WorkspaceGitReservation::Reserved);

        let (deferred_tx, deferred_rx) = tokio::sync::oneshot::channel();
        *manager.inner.deferred_git_effect.lock_recover() = Some(deferred_tx);
        let worker_manager = manager.clone();
        let worker_id = deferred_id.clone();
        let mut worker = tokio::spawn(async move { worker_manager.prepare(&worker_id).await });
        let deferred_signal = tokio::time::timeout(Duration::from_secs(15), deferred_rx).await;
        let observed_deferred = matches!(deferred_signal, Ok(Ok(())));
        let deferred_was_pending = !worker.is_finished();
        let deferred_worktree_absent = !Path::new(&deferred_path).exists();
        let deferred_branch_absent =
            git(&checkout, &["branch", "--list", &deferred_branch]).is_empty();
        let deferred_marker_false = {
            let store = store.lock().await;
            let command = store
                .get_command(&deferred_id)
                .expect("read deferred launch")
                .expect("deferred command");
            let workspace = store
                .get_workspace(&deferred_agent)
                .expect("read deferred workspace")
                .expect("deferred workspace row");
            command.state == CommandState::Accepted
                && workspace
                    .outcome
                    .as_ref()
                    .and_then(|outcome| outcome["native_git_write_dispatched"].as_bool())
                    == Some(false)
        };

        let release = {
            let mut store = store.lock().await;
            store
                .settle_workspace_creation(
                    &blocker_id,
                    blocker.target_generation,
                    WorkspaceCreationOutcome::Failed {
                        code: "fixture_no_git_effect".to_owned(),
                        message: "fixture released its reservation before spawn".to_owned(),
                    },
                    now_ms(),
                )
                .expect("release reserved repository writer")
        };
        manager.publish(&release.events);
        let deferred_result = tokio::time::timeout(Duration::from_secs(60), &mut worker)
            .await
            .expect("deferred workspace resumes after repository release")
            .expect("workspace manager task");

        assert!(
            observed_deferred,
            "manager did not take the Deferred branch"
        );
        assert!(
            deferred_was_pending,
            "Deferred settled or returned to Router"
        );
        assert!(
            deferred_worktree_absent,
            "Deferred spawned a Git worktree mutation"
        );
        assert!(deferred_branch_absent, "Deferred created a local branch");
        assert!(
            deferred_marker_false,
            "Deferred marked a possible Git write"
        );
        deferred_result.expect("workspace create after fence release");
        assert!(Path::new(&deferred_path).exists());
        assert!(
            git(&checkout, &["branch", "--list", &deferred_branch]).ends_with(&deferred_branch),
            "the branch is created after the fence releases"
        );
        let ready = store
            .lock()
            .await
            .get_workspace(&deferred_agent)
            .expect("read created workspace")
            .expect("created workspace row");
        assert_eq!(ready.phase, WorkspacePhase::Ready);

        let (
            may_have_begun_id,
            may_have_begun_agent,
            may_have_begun_path,
            may_have_begun_branch,
            may_have_begun,
        ) = admit_workspace(&store, &repo_path, &worktrees, "may-have-begun").await;
        let reservation = store
            .lock()
            .await
            .begin_workspace_git_effect(&may_have_begun_id, &may_have_begun, now_ms())
            .expect("reserve workspace mutator before simulated restart");
        assert_eq!(reservation, WorkspaceGitReservation::Reserved);
        assert!(manager.prepare(&may_have_begun_id).await.is_err());
        assert!(
            !Path::new(&may_have_begun_path).exists(),
            "MayHaveBegun must not rerun git worktree add"
        );
        assert!(
            git(&checkout, &["branch", "--list", &may_have_begun_branch]).is_empty(),
            "MayHaveBegun must not create a local branch"
        );
        let uncertain = store
            .lock()
            .await
            .get_workspace(&may_have_begun_agent)
            .expect("read MayHaveBegun workspace")
            .expect("MayHaveBegun workspace row");
        assert_eq!(
            uncertain
                .outcome
                .as_ref()
                .and_then(|outcome| outcome["native_git_write_dispatched"].as_bool()),
            Some(true)
        );
        assert!(matches!(
            uncertain.phase,
            WorkspacePhase::Unknown | WorkspacePhase::Failed
        ));
    }
}
