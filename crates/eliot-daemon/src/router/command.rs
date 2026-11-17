//! Command execution: dispatch a Store-admitted command to native I/O and settle it.
//!
//! This module holds `process_command_inner` with its private preparation,
//! dispatch-wait, verb-family, and settlement helpers as plain `impl Router`
//! blocks. Shared admission, read and progress helpers stay in the parent
//! module; native I/O still runs outside the Store lock exactly as before.
use super::{
    MODE_FORCE, Router, RouterInner, RunWorkerHandle, context,
    validate_project_rule_launch_command, validate_project_rule_resume_command,
    validate_retained_resume_agent,
};
use crate::common::now_ms;
use eliot_core::{
    Agent, AgentState, Command, CommandState, ContextItem, Error, Event, FailureOrigin,
    FrozenScriptRun, InjectArgs, InjectPoint, Result, SecretPolicy,
    api::{FrozenGitFetch, ProjectRegistrationView},
    classify_dispatched_error,
};
use eliot_plugin::{MAX_EVENT_BYTES, sync::LockExt};
use futures_util::future::BoxFuture;
use serde_json::{Value, json};
use std::sync::{Arc, Weak, atomic::Ordering};
use tokio::sync::{broadcast, oneshot, watch};

pub(super) const CONTROLLED_PLUGIN_SOURCE: &str = "stream";

/// Bounded pre-dispatch re-reads of one admitted command: a transient Store
/// blip must not strand accepted work, but an unreadable command must never
/// dispatch.
const LOAD_COMMAND_READ_ATTEMPTS: usize = 3;

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
    #[serde(default)]
    reason: Option<String>,
}

pub(super) struct RunWorkerCompletion {
    pub(super) router: Weak<RouterInner>,
    pub(super) run_id: String,
    pub(super) completed: watch::Sender<bool>,
    pub(super) finished: bool,
}

impl RunWorkerCompletion {
    pub(super) fn finish(&mut self) {
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
pub(super) enum RunWorkerStart {
    Started,
    AlreadyActive,
    ForceRequested,
}

/// A command that passed every pre-dispatch gate with the inputs its
/// dispatch and execution need.
struct PreparedCommand {
    command: Command,
    git_fetch_registration: Option<ProjectRegistrationView>,
    launch_binding: Option<(Agent, eliot_plugin::protocol::ProfileStoreResult)>,
    resume_source_state: Option<Value>,
}

/// Launch/resume start inputs captured before dispatch: the native Store
/// binding for a Codex launch and the retained source state for a resume.
struct StartInputs {
    launch_binding: Option<(Agent, eliot_plugin::protocol::ProfileStoreResult)>,
    resume_source_state: Option<Value>,
}

impl Router {
    pub(super) fn process_command(
        self,
        id: String,
        linked_id: Option<String>,
    ) -> BoxFuture<'static, ()> {
        Box::pin(self.process_command_inner(id, linked_id))
    }

    pub(super) async fn process_command_inner(self, id: String, linked_id: Option<String>) {
        if self
            .inner
            .store
            .lock()
            .await
            .get_command(&id)
            .ok()
            .flatten()
            .is_some_and(|command| command.verb == "switch")
        {
            self.process_switch(&id).await;
            return;
        }
        let mut receiver = self.inner.bus.subscribe();
        let Some(prepared) = self
            .prepare_command_dispatch(&id, linked_id.is_some())
            .await
        else {
            return;
        };
        let Some((dispatched, dispatch_seq, context_items)) = self
            .dispatch_and_wait_for_effect(
                &id,
                &mut receiver,
                &prepared.command,
                &prepared.launch_binding,
            )
            .await
        else {
            return;
        };
        self.execute_dispatched_command(
            dispatched,
            prepared.git_fetch_registration,
            prepared.resume_source_state,
            linked_id,
            dispatch_seq,
            context_items,
        )
        .await;
    }

    /// Run every pre-dispatch gate in order: load, script gates, Git fetch
    /// registration, project authority, workspace preparation, then launch
    /// and resume start inputs. Each gate settles its own rejection; `None`
    /// means the caller must return without dispatching.
    async fn prepare_command_dispatch(
        &self,
        id: &str,
        has_linked: bool,
    ) -> Option<PreparedCommand> {
        let command = match self.load_command_for_dispatch(id).await {
            Ok(Some(command)) => command,
            Ok(None) => return None,
            Err(error) => {
                self.abandon_unreadable_command(id, &error).await;
                return None;
            }
        };
        if command.verb == "inject" {
            // Queued inject points are durable pending context: only a
            // concrete next_input send or authenticated safe-point callback
            // may claim them. An active_turn inject is a direct command: it
            // dispatches to the plugin's native turn/steer effect now.
            let point = serde_json::from_value::<InjectArgs>(command.args.clone())
                .map(|args| args.point)
                .ok();
            if point != Some(InjectPoint::ActiveTurn) {
                return None;
            }
        }
        if !self.check_script_dispatch_gate(id, &command).await {
            return None;
        }
        let git_fetch_registration = self.load_git_fetch_registration(id, &command).await.ok()?;
        if !self
            .check_project_dispatch(id, &command, &git_fetch_registration, has_linked)
            .await
        {
            return None;
        }
        if !self.prepare_start_workspace(id, &command).await {
            return None;
        }
        let inputs = self.load_dispatch_binding(id, &command).await?;
        Some(PreparedCommand {
            command,
            git_fetch_registration,
            launch_binding: inputs.launch_binding,
            resume_source_state: inputs.resume_source_state,
        })
    }

    /// Load the command to dispatch. A missing command ends silently because
    /// a retained/settled command cannot acquire new I/O; a Store read
    /// failure keeps its typed error for bounded recovery below. Nothing
    /// dispatches on this path, so no external effect is uncertain.
    async fn load_command_for_dispatch(&self, id: &str) -> Result<Option<Command>> {
        for _ in 1..LOAD_COMMAND_READ_ATTEMPTS {
            let store = self.inner.store.lock().await;
            if let Ok(command) = store.get_command(id) {
                return Ok(command);
            }
        }
        let store = self.inner.store.lock().await;
        store.get_command(id)
    }

    /// A Store read failure before any dispatch keeps its typed error and
    /// never dispatches: the command may still be accepted, so the worker
    /// records the in-memory unknown cause (no Store write is assumed) and
    /// signals progress. Recovery re-drives the still-accepted command once
    /// the Store is readable; nothing here claims an external effect.
    async fn abandon_unreadable_command(&self, id: &str, error: &Error) {
        let error = self.sanitized_failure(error, "router.dispatch.load_command");
        crate::logging::record(
            &SecretPolicy::new(self.daemon_secrets()),
            &error.code,
            &[("command", id)],
            Some(&error),
        );
        self.note_unknown_id(id, "store_command_read_unknown");
        self.signal_progress();
    }

    /// Reject `run` commands outside owner/project authority and re-verify
    /// the frozen owner Run before dispatch. Returns false after settling.
    async fn check_script_dispatch_gate(&self, id: &str, command: &Command) -> bool {
        if command.verb == "run"
            && command.principal != "owner"
            && !command.principal.starts_with("project:")
        {
            self.reject_undispatched_error(
                id,
                &Error::new(
                    "unsupported",
                    "run commands need owner or project authority",
                )
                .with_where("router.dispatch.script_gate")
                .with_origin(
                    FailureOrigin::Swarm,
                    "script dispatch gate: principal is neither owner nor project",
                ),
            )
            .await;
            return false;
        }
        if command.verb == "run" && command.principal == "owner" {
            let frozen: FrozenScriptRun = match serde_json::from_value(command.args.clone()) {
                Ok(frozen) => frozen,
                Err(_) => {
                    self.reject_undispatched_error(
                        id,
                        &Error::new("invalid", "run args are not a valid frozen script run")
                            .with_where("router.dispatch.script_gate")
                            .with_origin(
                                FailureOrigin::Swarm,
                                "script dispatch gate: frozen run args failed validation",
                            ),
                    )
                    .await;
                    return false;
                }
            };
            if let Err(error) = self.verify_global_run(&frozen).await {
                self.reject_undispatched_error(id, &error).await;
                return false;
            }
        }
        true
    }

    /// Validate the frozen `git.fetch` args and load its project
    /// registration. `Err` means the rejection was settled already.
    async fn load_git_fetch_registration(
        &self,
        id: &str,
        command: &Command,
    ) -> std::result::Result<Option<ProjectRegistrationView>, ()> {
        if command.verb != "git.fetch" {
            return Ok(None);
        }
        let frozen = match serde_json::from_value::<FrozenGitFetch>(command.args.clone()) {
            Ok(frozen) if frozen.validate().is_ok() => frozen,
            _ => {
                self.reject_undispatched_error(
                    id,
                    &Error::new("invalid", "git.fetch args are not a valid frozen fetch")
                        .with_where("router.dispatch.git_fetch_gate")
                        .with_origin(
                            FailureOrigin::Swarm,
                            "git.fetch admission gate: frozen fetch args failed validation",
                        ),
                )
                .await;
                return Err(());
            }
        };
        let project_principal = command.principal.strip_prefix("project:");
        if command.principal != "owner" && project_principal != Some(frozen.repository.as_str()) {
            self.reject_undispatched_error(
                id,
                &Error::new(
                    "not_allowed",
                    "git.fetch principal does not own the repository",
                )
                .with_where("router.dispatch.git_fetch_gate")
                .with_origin(
                    FailureOrigin::Swarm,
                    "git.fetch admission gate: principal mismatch",
                ),
            )
            .await;
            return Err(());
        }
        let registration = {
            let store = self.inner.store.lock().await;
            store.get_project_registration("owner", &frozen.repository)
        };
        match registration {
            Ok(Some(registration)) => Ok(Some(registration)),
            Ok(None) => {
                self.reject_undispatched_error(
                    id,
                    &Error::new(
                        "not_allowed",
                        "git.fetch project registration is unavailable",
                    )
                    .with_where("router.dispatch.git_fetch_gate")
                    .with_origin(
                        FailureOrigin::Swarm,
                        "git.fetch admission gate: project registration missing",
                    ),
                )
                .await;
                Err(())
            }
            Err(error) => {
                self.reject_undispatched_error(id, &error).await;
                Err(())
            }
        }
    }

    /// Enforce project-rule authority for a project-principal command: the
    /// registration must load and verify, then launch, resume, and targeted
    /// verbs each keep their own fences. Returns false after settling.
    async fn check_project_dispatch(
        &self,
        id: &str,
        command: &Command,
        git_fetch_registration: &Option<ProjectRegistrationView>,
        has_linked: bool,
    ) -> bool {
        let Some(project_id) = command.principal.strip_prefix("project:") else {
            return true;
        };
        let registration = if command.verb == "git.fetch" {
            match git_fetch_registration {
                Some(registration) => Ok(Some(registration.clone())),
                None => Err(Error::new(
                    "not_allowed",
                    "Git fetch registration is unavailable",
                )),
            }
        } else {
            let store = self.inner.store.lock().await;
            store.get_project_registration("owner", project_id)
        };
        let registration = match registration {
            Ok(Some(registration)) => registration,
            Ok(None) => {
                self.reject_undispatched_error(
                    id,
                    &Error::new("not_allowed", "project registration is unavailable")
                        .with_where("router.dispatch.project_gate")
                        .with_origin(
                            FailureOrigin::Swarm,
                            "project dispatch gate: registration missing",
                        ),
                )
                .await;
                return false;
            }
            Err(error) => {
                self.reject_undispatched_error(id, &error).await;
                return false;
            }
        };
        if let Err(error) =
            crate::automation::verify_project_registration(&self.inner.data_root, &registration)
                .await
        {
            self.reject_undispatched_error(id, &error).await;
            return false;
        }
        if command.verb == "launch" {
            if let Err(error) = validate_project_rule_launch_command(&registration, command) {
                self.reject_undispatched_error(id, &error).await;
                return false;
            }
        } else if command.verb == "resume" {
            if let Err(error) =
                validate_project_rule_resume_command(&registration, command, has_linked)
            {
                self.reject_undispatched_error(id, &error).await;
                return false;
            }
            if let Err(error) = self
                .verify_project_rule_resume_after_admission(&registration, command)
                .await
            {
                self.reject_undispatched_error(id, &error).await;
                return false;
            }
        } else if matches!(
            command.verb.as_str(),
            "stop" | "send" | "steer" | "interrupt" | "approve" | "deny" | "answer"
        ) {
            let (Some(target), Some(generation)) = (command.target.as_deref(), command.generation)
            else {
                self.reject_undispatched_error(
                    id,
                    &Error::new(
                        "invalid",
                        "targeted verbs need an exact target and generation",
                    )
                    .with_where("router.dispatch.project_gate")
                    .with_origin(
                        FailureOrigin::Swarm,
                        "project dispatch gate: command target binding missing",
                    ),
                )
                .await;
                return false;
            };
            if let Err(error) = self
                .verify_project_rule_target(&registration, target, generation)
                .await
            {
                self.reject_undispatched_error(id, &error).await;
                return false;
            }
        }
        true
    }

    /// Prepare the managed workspace for a start verb before dispatch.
    /// Returns false after settling a preparation failure.
    async fn prepare_start_workspace(&self, id: &str, command: &Command) -> bool {
        if matches!(command.verb.as_str(), "launch" | "resume")
            && let Err(error) = self.inner.workspaces.prepare(id).await
        {
            self.settle_workspace_prepare_error(id, &error).await;
            self.signal_progress();
            return false;
        }
        true
    }

    /// Capture launch and resume start inputs: the admitted launch Agent,
    /// the retained resume source state, route readiness, and the native
    /// Store binding for a Codex launch. `None` means settled already.
    async fn load_dispatch_binding(&self, id: &str, command: &Command) -> Option<StartInputs> {
        let launch_agent = if command.verb == "launch" {
            match self.accepted_launch_agent(id).await {
                Ok(Some(agent)) => Some(agent),
                Ok(None) => return None, // A retained/settled command cannot acquire new I/O.
                Err(error) => {
                    self.reject_undispatched_error(id, &error).await;
                    return None;
                }
            }
        } else {
            None
        };
        let (resume_profile, resume_source_state) = if command.verb == "resume" {
            match self.capture_resume_source_state(id).await {
                Ok((profile, source_state)) => (Some(profile), Some(source_state)),
                Err(error) => {
                    self.reject_undispatched_error(id, &error).await;
                    return None;
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
            self.reject_undispatched_error(id, &error).await;
            return None;
        }
        let launch_binding = if let Some(agent) = launch_agent {
            if agent.harness == "codex" {
                let workspace = match agent.workspace.as_deref() {
                    Some(workspace) => workspace,
                    None => {
                        self.reject_undispatched_error(
                            id,
                            &Error::new(
                                "invalid",
                                "this codex launch is missing its workspace binding",
                            )
                            .with_where("router.dispatch.profile_store")
                            .with_origin(
                                FailureOrigin::Swarm,
                                "launch dispatch gate: codex workspace binding missing",
                            ),
                        )
                        .await;
                        return None;
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
                        self.reject_undispatched_error(id, &error).await;
                        return None;
                    }
                };
                Some((agent, binding))
            } else {
                None // Fake retains its ordinary dispatch; no metadata filesystem I/O.
            }
        } else {
            None
        };
        Some(StartInputs {
            launch_binding,
            resume_source_state,
        })
    }

    /// Commit the dispatch through the one Store writer, waiting on the bus
    /// while the command is queued, and return only a Store-confirmed
    /// `Dispatched` command. Queued/held, terminal, unknown, and dispatch
    /// errors settle here; `None` means the caller must return.
    async fn dispatch_and_wait_for_effect(
        &self,
        id: &str,
        receiver: &mut broadcast::Receiver<Event>,
        command: &Command,
        launch_binding: &Option<(Agent, eliot_plugin::protocol::ProfileStoreResult)>,
    ) -> Option<(Command, i64, Vec<ContextItem>)> {
        let (dispatched, dispatch_seq, context_items) = loop {
            let change = {
                let mut store = self.inner.store.lock().await;
                let dispatched = match launch_binding {
                    Some((expected_agent, binding)) => store
                        .dispatch_with_native_store_binding(
                            id,
                            now_ms(),
                            expected_agent,
                            &binding.binding,
                            binding.startup.as_ref(),
                        )
                        .map(|change| (change.value, Vec::new(), change.events)),
                    None if command.verb == "run"
                        && self.inner.mode.load(Ordering::SeqCst) == MODE_FORCE =>
                    {
                        Err(Error::new(
                            "bad_state",
                            "force shutdown prevents script dispatch",
                        ))
                    }
                    None if command.verb == "git.fetch"
                        && self.inner.mode.load(Ordering::SeqCst) == MODE_FORCE =>
                    {
                        Err(Error::new(
                            "bad_state",
                            "force shutdown prevents Git fetch dispatch",
                        ))
                    }
                    None if matches!(
                        command.verb.as_str(),
                        "notify" | "emit" | "attention.emit"
                    ) && self.inner.mode.load(Ordering::SeqCst) == MODE_FORCE =>
                    {
                        Err(Error::new(
                            "bad_state",
                            "force shutdown prevents local event dispatch",
                        ))
                    }
                    None if matches!(command.verb.as_str(), "mail.send" | "var.set")
                        && self.inner.mode.load(Ordering::SeqCst) == MODE_FORCE =>
                    {
                        Err(Error::new(
                            "bad_state",
                            "force shutdown prevents coordination dispatch",
                        ))
                    }
                    None if command.verb == "send" => {
                        store.dispatch_with_context(id, now_ms()).map(|change| {
                            (
                                change.value.command,
                                change.value.context_items,
                                change.events,
                            )
                        })
                    }
                    None => store
                        .dispatch(id, now_ms())
                        .map(|change| (change.value, Vec::new(), change.events)),
                };
                let result = match dispatched {
                    Ok((command, context_items, events)) => {
                        self.publish_locked(&events);
                        Ok((command, context_items, events))
                    }
                    Err(error) => Err(error),
                };
                (result, store.is_write_faulted())
            };
            match change.0 {
                Ok((command, _, _)) if command.state == CommandState::Queued => {
                    let target = command.target.clone();
                    let generation = command.generation;
                    if !self
                        .wait_for_queue_release(id, receiver, target, generation)
                        .await
                    {
                        return None;
                    }
                }
                Ok((command, _, _)) if command.state.is_terminal() => {
                    // Store may durably cancel a terminal predecessor's
                    // successor at this dispatch gate; never pass it to a native
                    // plugin after that result.
                    self.signal_progress();
                    return None;
                }
                Ok((command, context_items, events)) => {
                    let seq = match capture_dispatch_seq(&command, &events) {
                        Some(seq) => seq,
                        None => {
                            self.signal_progress();
                            return None;
                        }
                    };
                    break (command, seq, context_items);
                }
                Err(error) => {
                    self.settle_dispatch_error(id, &error, change.1).await;
                    return None;
                }
            }
        };

        // Only a Store-confirmed dispatch may reach a plugin. Queued/held,
        // terminal, unknown, and any future non-dispatched state are not effects.
        if dispatched.state != CommandState::Dispatched {
            self.signal_progress();
            return None;
        }
        Some((dispatched, dispatch_seq, context_items))
    }

    /// Wait for one bus event that can release a queued dispatch: provider
    /// and command-settlement frames, workspace frames, or the target's
    /// turn/session/request frames. Returns false only when the bus closed,
    /// after settling that the dispatch cannot proceed.
    async fn wait_for_queue_release(
        &self,
        id: &str,
        receiver: &mut broadcast::Receiver<Event>,
        target: Option<String>,
        generation: Option<i64>,
    ) -> bool {
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
                                | "command.unknown"
                                | "command.resolved"
                        )
                        || event.kind.starts_with("workspace.")
                        || (event.agent == target
                            && event.generation == generation
                            && (event.kind.starts_with("turn.")
                                || event.kind.starts_with("session.")
                                || event.kind.starts_with("request."))) =>
                {
                    return true;
                }
                Ok(_) => continue,
                Err(broadcast::error::RecvError::Lagged(_)) => return true,
                Err(broadcast::error::RecvError::Closed) => {
                    self.reject_undispatched_error(
                        id,
                        &Error::new(
                            "event bus closed",
                            "the daemon event bus closed before dispatch completed",
                        )
                        .with_where("router.dispatch.event_bus")
                        .with_origin(
                            FailureOrigin::Swarm,
                            "dispatch wait: event bus closed while watching for dispatch",
                        ),
                    )
                    .await;
                    return false;
                }
            }
        }
    }

    /// Run native I/O for one Store-confirmed dispatch and settle it, keeping
    /// the order committed dispatch, native I/O, then settlement. `run` and
    /// `git.fetch` carry their own settlement; every other verb loads its
    /// Agent and runs one verb-family helper.
    async fn execute_dispatched_command(
        &self,
        dispatched: Command,
        git_fetch_registration: Option<ProjectRegistrationView>,
        resume_source_state: Option<Value>,
        linked_id: Option<String>,
        dispatch_seq: i64,
        context_items: Vec<ContextItem>,
    ) {
        let id = dispatched.id.clone();
        if dispatched.verb == "run" {
            self.execute_run_command(dispatched).await;
            return;
        }
        if dispatched.verb == "git.fetch" {
            self.execute_git_fetch_command(&id, dispatched, git_fetch_registration)
                .await;
            return;
        }
        if matches!(dispatched.verb.as_str(), "notify" | "emit") {
            self.execute_user_event_command(&id, dispatched).await;
            return;
        }
        if dispatched.verb == "attention.emit" {
            self.execute_attention_event_command(&id, dispatched).await;
            return;
        }
        if dispatched.verb == "mail.send" {
            self.execute_mail_send_command(&id, dispatched).await;
            return;
        }
        if dispatched.verb == "var.set" {
            self.execute_var_set_command(&id, dispatched).await;
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
        let verb = dispatched.verb.clone();
        let trailing_signal = match verb.as_str() {
            "attach" => {
                self.execute_attach_command(&id, &agent, dispatched).await;
                true
            }
            "worktree.remove" => {
                self.execute_worktree_remove_command(&id, dispatched).await;
                true
            }
            "launch" | "resume" => {
                self.execute_start_command(&id, agent, dispatched, resume_source_state, linked_id)
                    .await;
                true
            }
            "send" => {
                self.execute_send_command(
                    &id,
                    &agent,
                    dispatched,
                    Some(dispatch_seq),
                    context_items,
                )
                .await
            }
            "steer" | "interrupt" | "approve" | "deny" | "answer" | "inject" => {
                self.execute_control_command(&id, &agent, dispatched, Some(dispatch_seq))
                    .await
            }
            "stop" => self.execute_stop_command(&id, &agent, dispatched).await,
            "configure" => {
                self.execute_configure_command(&id, &agent, dispatched, Some(dispatch_seq))
                    .await;
                true
            }
            "detach" => {
                self.execute_detach_command(&id, &agent, dispatched).await;
                true
            }
            _ => {
                let _ = Error::new("unsupported", "command verb is not implemented");
                true
            }
        };
        if trailing_signal {
            self.signal_progress();
        }
    }

    /// Run a dispatched `run` command to worker completion, or settle the
    /// pre-start cancellation when Force was requested first.
    async fn execute_run_command(&self, dispatched: Command) {
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
    }

    /// Execute a dispatched `git.fetch` against its registration and settle
    /// the recorded outcome, cleaning up only after a durable match.
    async fn execute_git_fetch_command(
        &self,
        id: &str,
        dispatched: Command,
        registration: Option<ProjectRegistrationView>,
    ) {
        let Some(registration) = registration else {
            let error = self.sanitized_failure(
                &Error::new(
                    "not_allowed",
                    "git.fetch execution needs its project registration",
                )
                .with_where("router.dispatch.git_fetch_gate")
                .with_origin(
                    FailureOrigin::Swarm,
                    "git.fetch execution gate: project registration missing",
                ),
                "router.dispatch.git_fetch_gate",
            );
            self.settle(
                id,
                CommandState::Rejected,
                Some(self.command_error_outcome(&error, "code")),
            )
            .await;
            return;
        };
        let frozen = match serde_json::from_value::<FrozenGitFetch>(dispatched.args.clone()) {
            Ok(frozen) => frozen,
            Err(_) => {
                let error = self.sanitized_failure(
                    &Error::new("invalid", "git.fetch args are not a valid frozen fetch")
                        .with_where("router.dispatch.git_fetch_gate")
                        .with_origin(
                            FailureOrigin::Swarm,
                            "git.fetch execution gate: frozen fetch args failed validation",
                        ),
                    "router.dispatch.git_fetch_gate",
                );
                self.settle(
                    id,
                    CommandState::Rejected,
                    Some(self.command_error_outcome(&error, "code")),
                )
                .await;
                return;
            }
        };
        let mut execution = crate::git_actions::execute(
            &self.inner.data_root,
            id,
            &frozen,
            &registration,
            self.inner.run_cancellation.subscribe(),
        )
        .await;
        self.redact_native_value(&mut execution.outcome);
        let state = execution.state;
        let expected_outcome = execution.outcome.clone();
        let cleanup_view = execution.cleanup_view.take();
        let settled = self.settle(id, state, Some(execution.outcome)).await;
        let durable_match = settled.as_ref().is_some_and(|command| {
            command.id == id
                && command.verb == "git.fetch"
                && command.state == state
                && state != CommandState::Unknown
                && command.outcome.as_ref() == Some(&expected_outcome)
        });
        if durable_match && let Some(view) = cleanup_view {
            crate::git_actions::cleanup_after_settlement(&self.inner.data_root, &view);
        }
        self.signal_progress();
    }

    /// Execute a dispatched local `notify`/`emit`: commit its validated DTO
    /// as one persisted `x.user.*` event together with its Applied
    /// settlement in a single Store transaction, publishing the committed
    /// batch in order while still serialized with the writer. No external
    /// send.
    async fn execute_user_event_command(&self, id: &str, dispatched: Command) {
        let (kind, mut data) = match super::user_event_kind_data(&dispatched.verb, &dispatched.args)
        {
            Ok(kind_data) => kind_data,
            Err(error) => {
                let error = self.sanitized_failure(&error, "router.dispatch.user_event_gate");
                self.settle(
                    id,
                    CommandState::Rejected,
                    Some(self.command_error_outcome(&error, "code")),
                )
                .await;
                self.signal_progress();
                return;
            }
        };
        // The reserved attention topic is never bound through the public
        // `emit` verb: admission rejects it, and a retained pre-fix command
        // that still reaches execution is rejected here. Only the internal
        // `attention.emit` verb below may use the bound commit.
        if kind == super::ATTENTION_USER_EVENT_KIND {
            let error = self.sanitized_failure(
                &Error::new(
                    "invalid",
                    "the reserved attention topic is never bound through emit",
                )
                .with_where("router.dispatch.user_event_gate")
                .with_origin(
                    FailureOrigin::Swarm,
                    "user event gate: reserved attention topic through emit",
                ),
                "router.dispatch.user_event_gate",
            );
            self.settle(
                id,
                CommandState::Rejected,
                Some(self.command_error_outcome(&error, "code")),
            )
            .await;
            self.signal_progress();
            return;
        }
        self.redact_native_value(&mut data);
        let committed = {
            let mut store = self.inner.store.lock().await;
            let result = store.commit_user_event_applied(id, &kind, data, now_ms());
            if let Ok(change) = &result
                && !change.events.is_empty()
            {
                self.publish_locked(&change.events);
            }
            result
        };
        if let Err(error) = committed {
            self.settle(
                id,
                CommandState::Unknown,
                Some(json!({"code": error.code, "cause": "user_event_commit_failed"})),
            )
            .await;
        }
        self.signal_progress();
    }

    /// Execute a dispatched internal `attention.emit` command: commit its
    /// validated report as one bound `x.user.agent.attention` event with its
    /// Applied settlement in a single Store transaction. The Store admits the
    /// `attention.emit` verb only from the internal attention producer, and
    /// the bound commit additionally requires that verb, so a public emit
    /// can never acquire an authoritative subject binding.
    async fn execute_attention_event_command(&self, id: &str, dispatched: Command) {
        let (kind, mut data) = match super::user_event_kind_data(&dispatched.verb, &dispatched.args)
        {
            Ok(kind_data) => kind_data,
            Err(error) => {
                let error = self.sanitized_failure(&error, "router.dispatch.attention_gate");
                self.settle(
                    id,
                    CommandState::Rejected,
                    Some(self.command_error_outcome(&error, "code")),
                )
                .await;
                self.signal_progress();
                return;
            }
        };
        self.redact_native_value(&mut data);
        let committed = {
            let mut store = self.inner.store.lock().await;
            let result = store.commit_attention_event_applied(id, &kind, data, now_ms());
            if let Ok(change) = &result
                && !change.events.is_empty()
            {
                self.publish_locked(&change.events);
            }
            result
        };
        if let Err(error) = committed {
            self.settle(
                id,
                CommandState::Unknown,
                Some(json!({"code": error.code, "cause": "user_event_commit_failed"})),
            )
            .await;
        }
        self.signal_progress();
    }

    /// Execute a dispatched rule `mail.send`: commit the frozen queued-mail
    /// effect together with its Applied settlement in a single Store
    /// transaction, publishing the committed batch in order while still
    /// serialized with the writer, then fan out mailbox wakes only after
    /// commit through the existing coordination wake path.
    async fn execute_mail_send_command(&self, id: &str, dispatched: Command) {
        if let Err(error) = self.validate_coordination_command(&dispatched) {
            self.settle(
                id,
                CommandState::Rejected,
                Some(json!({"code": error.code})),
            )
            .await;
            self.signal_progress();
            return;
        }
        let limits = self.inner.mail.limits();
        let committed = {
            let mut store = self.inner.store.lock().await;
            let result = store.commit_mail_send_applied(id, limits, now_ms());
            if let Ok(change) = &result
                && !change.events.is_empty()
            {
                self.publish_locked(&change.events);
            }
            result
        };
        match committed {
            Ok(change) if !change.events.is_empty() => {
                self.wake_mail_recipients(&change.value).await;
            }
            Ok(_) => {}
            Err(error) => {
                self.settle(
                    id,
                    coordination_commit_error_state(&error.code),
                    Some(json!({"code": error.code, "cause": "mail_commit_failed"})),
                )
                .await;
            }
        }
        self.signal_progress();
    }

    /// Execute a dispatched rule `var.set`: commit the frozen versioned
    /// variable effect together with its Applied settlement in a single Store
    /// transaction, publishing the committed batch (`var.changed` first) in
    /// order while still serialized with the writer.
    async fn execute_var_set_command(&self, id: &str, dispatched: Command) {
        if let Err(error) = self.validate_coordination_command(&dispatched) {
            self.settle(
                id,
                CommandState::Rejected,
                Some(json!({"code": error.code})),
            )
            .await;
            self.signal_progress();
            return;
        }
        let committed = {
            let mut store = self.inner.store.lock().await;
            let result = store.commit_var_set_applied(id, now_ms());
            if let Ok(change) = &result
                && !change.events.is_empty()
            {
                self.publish_locked(&change.events);
            }
            result
        };
        if let Err(error) = committed {
            self.settle(
                id,
                coordination_commit_error_state(&error.code),
                Some(json!({"code": error.code, "cause": "var_commit_failed"})),
            )
            .await;
        }
        self.signal_progress();
    }

    fn validate_coordination_command(&self, dispatched: &Command) -> Result<()> {
        if dispatched.target.is_some() || dispatched.generation.is_some() {
            return Err(Error::new(
                "invalid",
                "coordination command must be targetless",
            ));
        }
        match dispatched.verb.as_str() {
            "mail.send" => serde_json::from_value::<eliot_core::api::MailDefinitionArgs>(
                dispatched.args.clone(),
            )
            .map_err(|_| Error::new("invalid", "mail.send definition is malformed"))?
            .validate(),
            "var.set" => serde_json::from_value::<eliot_core::api::VarDefinitionArgs>(
                dispatched.args.clone(),
            )
            .map_err(|_| Error::new("invalid", "var.set definition is malformed"))?
            .validate(),
            _ => Err(Error::new("invalid", "not a coordination command")),
        }
    }

    async fn wake_mail_recipients(&self, settled: &Command) {
        let recipients: Vec<String> = settled
            .outcome
            .as_ref()
            .and_then(|outcome| outcome.get("deliveries"))
            .and_then(|deliveries| deliveries.as_array())
            .map(|deliveries| {
                deliveries
                    .iter()
                    .filter_map(|delivery| {
                        delivery
                            .get("recipient")
                            .and_then(|recipient| recipient.as_str())
                            .map(str::to_owned)
                    })
                    .collect()
            })
            .unwrap_or_default();
        if recipients.is_empty() {
            if let Ok(definition) =
                serde_json::from_value::<eliot_core::api::MailDefinitionArgs>(settled.args.clone())
                && definition.validate().is_ok()
            {
                for recipient in &definition.to {
                    self.offer_notice(recipient, eliot_store::NoticePending::Mail)
                        .await;
                }
            }
            return;
        }
        for recipient in &recipients {
            self.offer_notice(recipient, eliot_store::NoticePending::Mail)
                .await;
        }
    }

    /// Attach an observer through the owned plugin and settle the validated
    /// snapshot.
    async fn execute_attach_command(&self, id: &str, agent: &Agent, dispatched: Command) {
        match self
            .inner
            .plugin_host
            .attach_observer(agent.clone(), dispatched.clone(), None)
            .await
        {
            Ok(mut value) => {
                self.redact_native_value(&mut value);
                let failed = value["state"] == "failed";
                let failed_closed = failed && value["reader_closed"].as_bool() == Some(true);
                let expected_state = if failed { "failed" } else { "attached" };
                match validate_observer_command_result(value, agent, &dispatched, expected_state) {
                    Ok(snapshot) => {
                        if failed {
                            let reason = snapshot["reason"]
                                .as_str()
                                .unwrap_or("observer_start_failed");
                            let _ = self
                                .settle(id, CommandState::Failed, Some(json!({"reason": reason})))
                                .await;
                            self.inner
                                .plugin_host
                                .observer_unavailable(agent, "observer_start_failed", true)
                                .await;
                        } else {
                            let _ = self.settle_observe_attach(id, snapshot.clone()).await;
                        }
                    }
                    Err(error) => {
                        self.settle_plugin_error(id, &error).await;
                        if failed_closed {
                            self.inner
                                .plugin_host
                                .observer_unavailable(agent, "observer_start_failed", true)
                                .await;
                        }
                    }
                }
            }
            Err(error) => {
                self.settle_plugin_error(id, &error).await;
            }
        }
    }

    /// Remove a managed worktree and settle a removal failure.
    async fn execute_worktree_remove_command(&self, id: &str, dispatched: Command) {
        match self.inner.workspaces.remove(&dispatched).await {
            Ok(_) => {}
            Err(error) => {
                self.settle_plugin_error(id, &error).await;
            }
        }
    }

    /// Start a launched or resumed Agent through the owned plugin, settle
    /// ready, and schedule the linked input if there is one. A plugin
    /// `"failed"` start state is an authoritative proven-no-effect verdict
    /// (the plugin returns it instead of an error only for checks before any
    /// native session effect), so the launch settles failed, never unknown.
    async fn execute_start_command(
        &self,
        id: &str,
        agent: Agent,
        dispatched: Command,
        resume_source_state: Option<Value>,
        linked_id: Option<String>,
    ) {
        if let Err(error) = self.prepare_agent_artifact_root(&agent).await {
            self.settle_plugin_error(id, &Error::new("native_not_dispatched", *error.message))
                .await;
            return;
        }
        let artifact_agent = agent.id.clone();
        let harness = agent.harness.clone();
        let start = self
            .inner
            .plugin_host
            .start(agent, dispatched.clone(), resume_source_state)
            .await;
        match start {
            Ok(mut value) => {
                if classify_start_result(&value) == CommandState::Failed {
                    self.redact_native_value(&mut value);
                    Self::ensure_outcome_failure_fields(
                        &mut value,
                        &format!("plugin.{harness}.command.start"),
                    );
                    let _ = self.settle(id, CommandState::Failed, Some(value)).await;
                } else {
                    let artifact_snapshot =
                        self.inner.store.lock().await.get_agent(&artifact_agent);
                    if let Ok(Some(agent)) = artifact_snapshot {
                        let _ = self.record_agent_artifacts(&agent).await;
                    }
                    let _ = self
                        .settle(id, CommandState::Applied, Some(json!({"state": "ready"})))
                        .await;
                    if let Some(linked_id) = linked_id {
                        self.schedule_command(linked_id, None);
                    }
                }
            }
            Err(error) => {
                self.settle_plugin_error(id, &error).await;
            }
        }
    }

    /// Send one turn through the owned plugin and settle its outcome,
    /// requiring a committed turn association for an applied send. Returns
    /// whether the caller must send the trailing progress signal.
    async fn execute_send_command(
        &self,
        id: &str,
        agent: &Agent,
        dispatched: Command,
        dispatch_seq: Option<i64>,
        context_items: Vec<ContextItem>,
    ) -> bool {
        let dispatched = match context::with_next_input(&dispatched, &context_items) {
            Ok(dispatched) => dispatched,
            Err(error) => {
                self.settle_plugin_error(id, &error).await;
                return false;
            }
        };
        let response = self
            .inner
            .plugin_host
            .command(agent, dispatched.clone(), dispatch_seq)
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
                    let _ = self.settle(id, state, Some(value.clone())).await;
                    return false;
                }
                if response_state != Some("applied") {
                    let error = Error::new(
                        "unknown",
                        "plugin send response omitted its command outcome",
                    );
                    self.settle_plugin_error(id, &error).await;
                    return false;
                }
                let expected_turn = value
                    .get("turn_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let current = {
                    let store = self.inner.store.lock().await;
                    store.get_command(id)
                };
                match current {
                    Ok(Some(command))
                        if command.state == CommandState::Applied
                            && command.turn_id.as_deref() == Some(expected_turn) => {}
                    Ok(Some(command)) if command.state == CommandState::Unknown => {}
                    Ok(Some(_)) => {
                        let error = Error::new(
                            "unknown",
                            "plugin returned without committed turn association",
                        );
                        self.settle_plugin_error(id, &error).await;
                    }
                    Ok(None) => {}
                    Err(_) => {}
                }
            }
            Err(error) => {
                self.settle_plugin_error(id, &error).await;
            }
        }
        true
    }

    /// Run one native control (steer, interrupt, or a request reply) through
    /// the owned plugin and settle its outcome. Returns whether the caller
    /// must send the trailing progress signal.
    async fn execute_control_command(
        &self,
        id: &str,
        agent: &Agent,
        dispatched: Command,
        dispatch_seq: Option<i64>,
    ) -> bool {
        match self
            .inner
            .plugin_host
            .command(agent, dispatched.clone(), dispatch_seq)
            .await
        {
            Ok(mut value) => {
                let state = match value.get("state").and_then(Value::as_str) {
                    Some("applied") => CommandState::Applied,
                    Some("rejected") => CommandState::Rejected,
                    Some("failed") => CommandState::Failed,
                    Some("unknown") => CommandState::Unknown,
                    _ => {
                        let error =
                            Error::new("unknown", "plugin command response omitted its outcome");
                        self.settle_plugin_error(id, &error).await;
                        return false;
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
                let _ = self.settle(id, state, Some(value.clone())).await;
            }
            Err(error) => {
                self.settle_plugin_error(id, &error).await;
            }
        }
        true
    }

    /// Apply a dispatched `configure`: store the model/effort overlay, then
    /// settle applied with the requested and effective values, the assigned
    /// revision and the application point. Codex and Muse stay Store-only
    /// here (no native I/O runs; the next send carries the effective
    /// values in its native turn request). OpenCode and Antigravity first
    /// switch the live native session through the owned plugin and prove
    /// the switch with the native echo or `GET` before the Router stores
    /// anything: the outcome carries the native `model_verified` (and
    /// Antigravity `effort_verified`) flags, set only from that proof, and
    /// an unverified or refused switch never stores an overlay. Settlement
    /// happens only after the Store commit, so an applied configure always
    /// has a retained row to read back.
    async fn execute_configure_command(
        &self,
        id: &str,
        agent: &Agent,
        dispatched: Command,
        dispatch_seq: Option<i64>,
    ) {
        if matches!(agent.harness.as_str(), "opencode" | "antigravity") {
            self.execute_native_configure_command(id, agent, dispatched, dispatch_seq)
                .await;
            return;
        }
        let outcome = {
            let mut store = self.inner.store.lock().await;
            store.apply_agent_config(id, now_ms())
        };
        match outcome {
            Ok(config) => {
                let outcome = json!({
                    "state": "applied",
                    "requested": {
                        "model": config.requested_model,
                        "effort": config.requested_effort,
                    },
                    "effective": {
                        "model": config.model,
                        "effort": config.effort,
                    },
                    "config_revision": config.revision,
                    "application": config.application,
                    "harness": agent.harness,
                });
                let _ = self.settle(id, CommandState::Applied, Some(outcome)).await;
            }
            Err(error) => {
                let error = self.sanitized_failure(
                    &error.with_where("store.dispatch.config_apply").with_origin(
                        FailureOrigin::Swarm,
                        "agent config Store gate refused the command",
                    ),
                    "store.dispatch.config_apply",
                );
                let _ = self
                    .settle(
                        id,
                        CommandState::Rejected,
                        Some(self.command_error_outcome(&error, "reason")),
                    )
                    .await;
            }
        }
    }

    /// Apply a dispatched `configure` on a sticky-session harness: switch
    /// the live native session through the owned plugin, prove the switch
    /// with the native echo or `GET`, then store the verified overlay and
    /// settle applied with the native verification flags. A refused switch
    /// settles the plugin's verdict and stores nothing; an ambiguous or
    /// unproven outcome settles unknown and stores nothing, so no
    /// unverified application is ever claimed or retained.
    async fn execute_native_configure_command(
        &self,
        id: &str,
        agent: &Agent,
        dispatched: Command,
        dispatch_seq: Option<i64>,
    ) {
        let response = self
            .inner
            .plugin_host
            .command(agent, dispatched, dispatch_seq)
            .await;
        let proven = match response {
            Ok(value) => match value.get("state").and_then(Value::as_str) {
                Some("applied") => Some(value),
                Some("rejected") | Some("failed") => {
                    let mut value = value;
                    self.redact_native_value(&mut value);
                    Self::ensure_outcome_failure_fields(
                        &mut value,
                        &format!("plugin.{}.command.configure", agent.harness),
                    );
                    let state = if value.get("state").and_then(Value::as_str) == Some("failed") {
                        CommandState::Failed
                    } else {
                        CommandState::Rejected
                    };
                    let _ = self.settle(id, state, Some(value)).await;
                    return;
                }
                Some("unknown") => {
                    let mut value = value;
                    if let Some(cause) = self
                        .inner
                        .plugin_host
                        .take_late_batch_cause(&agent.id, agent.generation)
                    {
                        value["cause"] = Value::String(cause);
                    }
                    self.redact_native_value(&mut value);
                    Self::ensure_outcome_failure_fields(
                        &mut value,
                        &format!("plugin.{}.command.configure", agent.harness),
                    );
                    let _ = self.settle(id, CommandState::Unknown, Some(value)).await;
                    return;
                }
                _ => None,
            },
            Err(error) => {
                self.settle_plugin_error(id, &error).await;
                return;
            }
        };
        let Some(proven) = proven else {
            self.settle_plugin_error(
                id,
                &Error::new(
                    "unknown",
                    "plugin configure response omitted its command outcome",
                )
                .with_where(format!("plugin.{}.command.configure", agent.harness))
                .with_origin(
                    FailureOrigin::Harness,
                    "plugin configure reply omitted its command outcome frame",
                ),
            )
            .await;
            return;
        };
        let native_model = proven
            .get("model")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        let native_effort = proven
            .get("effort")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        let model_verified = proven
            .get("model_verified")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let effort_verified = proven
            .get("effort_verified")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let outcome = {
            let mut store = self.inner.store.lock().await;
            store.apply_agent_config_native(id, now_ms(), native_model, native_effort)
        };
        match outcome {
            Ok(config) => {
                let mut outcome = json!({
                    "state": "applied",
                    "requested": {
                        "model": config.requested_model,
                        "effort": config.requested_effort,
                    },
                    "effective": {
                        "model": config.model,
                        "effort": config.effort,
                    },
                    "config_revision": config.revision,
                    "application": config.application,
                    "harness": agent.harness,
                    "model_verified": model_verified,
                });
                if agent.harness == "antigravity" {
                    outcome["effort_verified"] = json!(effort_verified);
                }
                let _ = self.settle(id, CommandState::Applied, Some(outcome)).await;
            }
            Err(error) => {
                let error = self.sanitized_failure(
                    &error.with_where("store.dispatch.config_apply").with_origin(
                        FailureOrigin::Swarm,
                        "agent config Store gate refused the command",
                    ),
                    "store.dispatch.config_apply",
                );
                let _ = self
                    .settle(
                        id,
                        CommandState::Rejected,
                        Some(self.command_error_outcome(&error, "reason")),
                    )
                    .await;
            }
        }
    }

    /// Stop an Agent: a latched preparation cancel settles through the
    /// Store, otherwise the owned plugin stops natively and the summary is
    /// settled. Returns whether the caller must send the trailing progress
    /// signal.
    async fn execute_stop_command(&self, id: &str, agent: &Agent, dispatched: Command) -> bool {
        // Store dispatch atomically latches a preparation cancel before
        // the manager signals its custody. Otherwise this is native stop.
        match self.inner.workspaces.cancel(id).await {
            Ok(true) => {
                self.signal_progress();
                return false;
            }
            Ok(false) => {}
            Err(error) => {
                self.settle_plugin_error(id, &error).await;
                self.signal_progress();
                return false;
            }
        }
        if let Some(summary) = self
            .terminal_stop_noop_summary(id, agent, &dispatched)
            .await
        {
            let _ = self.settle(id, CommandState::Applied, Some(summary)).await;
            // Assignment retirement runs inline in this stop worker, never in
            // a second executor. Best effort: refused items stay registered.
            self.retire_assignment_cleanup(&agent.id, agent.generation)
                .await;
            return true;
        }
        let force = dispatched
            .args
            .get("force")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        match self
            .inner
            .plugin_host
            .stop(agent, dispatched.clone(), force)
            .await
        {
            Ok((value, force_stop)) => {
                let mut summary = json!({
                    "state": "applied",
                    "custody": value.get("custody").cloned().unwrap_or(Value::Null),
                    "graceful": value.get("graceful").cloned().unwrap_or(Value::Bool(false)),
                });
                if let Some(diagnostics) = value.get("diagnostics") {
                    summary["diagnostics"] = diagnostics.clone();
                }
                self.redact_native_value(&mut summary);
                if self
                    .settle(id, CommandState::Applied, Some(summary))
                    .await
                    .is_some_and(|command| command.state == CommandState::Applied)
                {
                    // Assignment retirement runs inline in this stop worker,
                    // never in a second executor. Best effort: refused items
                    // stay registered.
                    self.retire_assignment_cleanup(&agent.id, agent.generation)
                        .await;
                    if let Some(guard) = force_stop {
                        guard.finish();
                    }
                }
            }
            Err(error) => {
                self.settle_plugin_error(id, &error).await;
            }
        }
        true
    }

    /// Applied no-op summary for an already-terminal generation with verified
    /// no owned runtime. Returns `None` when native stop must still run, so
    /// exact member/generation binding and honest Unknown handling are kept.
    async fn terminal_stop_noop_summary(
        &self,
        id: &str,
        agent: &Agent,
        dispatched: &Command,
    ) -> Option<Value> {
        if dispatched.verb != "stop"
            || dispatched.id != id
            || dispatched.target.as_deref() != Some(agent.id.as_str())
            || dispatched.generation != Some(agent.generation)
            || dispatched.state != CommandState::Dispatched
            || !agent.is_terminal()
            || agent.active_turn.is_some()
            || !agent.pending.is_empty()
            || agent.observe_only
        {
            return None;
        }
        if self
            .inner
            .plugin_host
            .controls_generation(&agent.id, agent.generation)
        {
            return None;
        }
        let store = self.inner.store.lock().await;
        let current = match store.get_agent(&agent.id) {
            Ok(Some(current)) => current,
            _ => return None,
        };
        if Some(current.generation) != dispatched.generation
            || current.generation != agent.generation
            || !current.is_terminal()
            || current.active_turn.is_some()
            || !current.pending.is_empty()
            || current.observe_only
        {
            return None;
        }
        match store.get_command(id) {
            Ok(Some(command))
                if command.verb == "stop"
                    && command.state == CommandState::Dispatched
                    && command.target.as_deref() == Some(agent.id.as_str())
                    && command.generation == Some(agent.generation) => {}
            _ => return None,
        }
        match store.has_owned_exit_and_reader_drain(&agent.id, agent.generation) {
            Ok(true) => {}
            _ => return None,
        }
        Some(json!({
            "state": "applied",
            "already_terminal": true,
        }))
    }

    /// Detach an observer through the owned plugin and settle the validated
    /// outcome.
    async fn execute_detach_command(&self, id: &str, agent: &Agent, dispatched: Command) {
        match self
            .inner
            .plugin_host
            .detach_observer(agent, dispatched.clone())
            .await
        {
            Ok(value) => {
                match validate_observer_command_result(value, agent, &dispatched, "detached") {
                    Ok(outcome) => {
                        let _ = self.settle_observe_detach(id, outcome.clone()).await;
                    }
                    Err(error) => {
                        self.settle_plugin_error(id, &error).await;
                    }
                }
            }
            Err(error) => {
                self.settle_plugin_error(id, &error).await;
            }
        }
    }
}

impl Router {
    pub(super) fn start_run_worker(&self, command: Command) -> RunWorkerStart {
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
}

impl Router {
    pub(super) async fn accepted_launch_agent(&self, command_id: &str) -> Result<Option<Agent>> {
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

    pub(super) async fn capture_resume_source_state(
        &self,
        command_id: &str,
    ) -> Result<(Value, Value)> {
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
}

impl Router {
    pub(super) async fn settle_dispatch_error(&self, id: &str, error: &Error, write_faulted: bool) {
        // This is only the local Store dispatch result, before any plugin I/O:
        // the dispatch transaction rolled back, so no generation, source, or
        // token changed and no plugin was invoked. A persistence fault is the
        // only uncertain case; every other dispatch refusal proves no native
        // effect and settles rejected instead of stranding the command.
        // A plugin error with the same code still follows post-I/O C1 handling.
        if write_faulted {
            self.note_unknown_if_uncertain(id, "store_dispatch_outcome_unknown")
                .await;
        } else {
            self.reject_undispatched_error(id, error).await;
        }
        self.signal_progress();
    }

    pub(super) fn daemon_secrets(&self) -> Vec<String> {
        self.inner
            .secrets
            .lock_recover()
            .iter()
            .cloned()
            .collect::<Vec<_>>()
    }

    /// Sanitize a failure for persistence: redact the message, origin
    /// evidence and stderr tails with the daemon secrets, normalize `where`,
    /// and fill the stable fallback location/origin when the creation site
    /// did not classify them. The fallback origin is `unknown` with a note
    /// naming the missing classification; it never invents a location.
    fn sanitized_failure(&self, error: &Error, default_where: &str) -> Error {
        let secrets = self.daemon_secrets();
        let mut error = error.clone().sanitized(&SecretPolicy::new(&secrets));
        if error.failure_where.is_none() {
            error = error.with_where(default_where);
        }
        if error.origin.is_none() {
            error = error.with_origin(
                FailureOrigin::Unknown,
                "no origin classified where this error was created",
            );
        }
        error
    }

    /// Fill missing failure-location keys on an outcome value without
    /// touching producer-supplied codes, messages, states or causes. Only the
    /// stable fallback is recorded; real classification belongs at the error
    /// creation site.
    fn ensure_outcome_failure_fields(value: &mut Value, default_where: &str) {
        let Some(object) = value.as_object_mut() else {
            return;
        };
        if object
            .get("where")
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
        {
            object.insert("where".to_owned(), json!(default_where));
        }
        if object
            .get("origin")
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
        {
            object.insert("origin".to_owned(), json!("unknown"));
        }
        if object
            .get("origin_evidence")
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
        {
            object.insert(
                "origin_evidence".to_owned(),
                json!("origin not classified by the producer of this outcome"),
            );
        }
    }

    /// Every rejection, failure and unknown outcome carries the code, the
    /// bounded redacted message, the stable `where` and the origin with its
    /// evidence. Optional stderr tails ride along only when the error
    /// already carries a sanitized tail; they never invalidate the primary
    /// outcome.
    fn command_error_outcome(&self, error: &Error, code_key: &str) -> Value {
        let mut outcome = json!({
            "message": error.message,
            "where": error
                .failure_where
                .clone()
                .unwrap_or_else(|| "router.dispatch.unknown".into()),
            "origin": error.origin_or_unknown().as_str(),
        });
        outcome[code_key] = json!(error.code);
        if let Some(evidence) = error.origin_evidence.as_deref() {
            outcome["origin_evidence"] = json!(evidence);
        }
        if let Some(exit_code) = error.exit_code.as_deref() {
            outcome["exit_code"] = json!(exit_code);
        }
        if let Some(ntstatus) = error.ntstatus.as_deref() {
            outcome["ntstatus"] = json!(ntstatus);
        }
        for diagnostic in error.diagnostics.iter() {
            crate::redaction::attach_stderr(
                &mut outcome,
                diagnostic.source,
                diagnostic.snapshot.clone(),
                &[],
            );
        }
        self.redact_native_value(&mut outcome);
        outcome
    }

    pub(super) async fn reject_undispatched_error(&self, id: &str, error: &Error) {
        // Pre-dispatch refusals prove no native I/O. The persisted outcome
        // keeps the full failure: code, bounded redacted message, stable
        // `where` and origin with evidence. Nothing here is a secret,
        // parameter, prompt or model output: the error was sanitized above.
        let error = self.sanitized_failure(error, "router.dispatch.unknown");
        let outcome = self.command_error_outcome(&error, "reason");
        crate::logging::record(
            &SecretPolicy::new(self.daemon_secrets()),
            &error.code,
            &[("command", id)],
            Some(&error),
        );
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
                        Ok(true) => store.reject_workspace_resume(id, &outcome, now_ms()),
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
        let error = self.sanitized_failure(error, "plugin.command.unknown");
        crate::logging::record(
            &SecretPolicy::new(self.daemon_secrets()),
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
        outcome["message"] = json!(error.message.clone());
        outcome["where"] = json!(
            error
                .failure_where
                .clone()
                .unwrap_or_else(|| "plugin.command.unknown".into())
        );
        outcome["origin"] = json!(error.origin_or_unknown().as_str());
        if let Some(evidence) = error.origin_evidence.as_deref() {
            outcome["origin_evidence"] = json!(evidence);
        }
        if let Some(exit_code) = error.exit_code.as_deref() {
            outcome["exit_code"] = json!(exit_code);
        }
        if let Some(ntstatus) = error.ntstatus.as_deref() {
            outcome["ntstatus"] = json!(ntstatus);
        }
        let secrets = self.daemon_secrets();
        for diagnostic in error.diagnostics.iter() {
            crate::redaction::attach_stderr(
                &mut outcome,
                diagnostic.source,
                diagnostic.snapshot.clone(),
                &secrets,
            );
        }
        Self::ensure_outcome_failure_fields(&mut outcome, "plugin.command.unknown");
        self.redact_native_value(&mut outcome);
        let _ = self.settle(id, state, Some(outcome)).await;
    }

    pub(super) async fn settle_workspace_prepare_error(&self, id: &str, error: &Error) {
        // WorkspaceManager persists its typed creation/setup outcomes. A Ready
        // retained Resume readback is the remaining pre-native Unknown; Store
        // rechecks that exact pending marker atomically before recording it.
        if error.code == "unknown" {
            let mut store = self.inner.store.lock().await;
            if let Ok(Some(command)) = store.get_command(id)
                && command.verb == "resume"
                && matches!(command.state, CommandState::Accepted | CommandState::Queued)
                && let Some(target_generation) = command
                    .generation
                    .and_then(|generation| generation.checked_add(1))
                && let Ok(change) = store.settle_workspace_creation(
                    id,
                    target_generation,
                    eliot_core::WorkspaceCreationOutcome::Unknown {
                        code: "workspace_identity_unknown".to_owned(),
                        message: "retained workspace identity could not be confirmed".to_owned(),
                        failure_where: Some("store.dispatch.workspace_gate".to_owned()),
                        origin: Some(eliot_core::FailureOrigin::Unknown),
                        origin_evidence: Some(
                            "retained identity readback neither proved nor disproved the workspace"
                                .to_owned(),
                        ),
                    },
                    now_ms(),
                )
            {
                self.publish_locked(&change.events);
            }
        }
        let error = self.sanitized_failure(error, "store.dispatch.workspace_gate");
        crate::logging::record(
            &SecretPolicy::new(self.daemon_secrets()),
            &error.code,
            &[("command", id)],
            Some(&error),
        );
        self.signal_progress();
    }
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
        || (expected_state == "failed" && result.reader_closed != Some(true))
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
    if expected_state == "failed"
        && !matches!(
            result.reason.as_deref(),
            Some("observer_preparation_failed" | "native_start_failed")
        )
    {
        return Err(Error::new(
            "source_integrity",
            "observer failure omitted its safe startup reason",
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

/// Settlement for an authoritative plugin `agent.start` result. A `"failed"`
/// state proves the plugin dispatched no native session effect, so the
/// launch settles failed, never unknown. Any other shape keeps the
/// historical ready settlement.
fn classify_start_result(value: &Value) -> CommandState {
    if value.get("state").and_then(Value::as_str) == Some("failed") {
        CommandState::Failed
    } else {
        CommandState::Applied
    }
}

#[cfg(test)]
mod start_result_tests {
    use super::classify_start_result;
    use eliot_core::CommandState;
    use serde_json::json;

    #[test]
    fn failed_state_settles_failed_and_everything_else_stays_ready() {
        assert_eq!(
            classify_start_result(&json!({"state": "failed", "reason": "x"})),
            CommandState::Failed
        );
        assert_eq!(
            classify_start_result(&json!({"state": "ready"})),
            CommandState::Applied
        );
        assert_eq!(classify_start_result(&json!({})), CommandState::Applied);
        assert_eq!(
            classify_start_result(&json!({"state": "unknown"})),
            CommandState::Applied
        );
    }
}

#[cfg(test)]
mod observer_attach_result_tests {
    use super::validate_observer_command_result;
    use eliot_core::{Agent, AgentOrigin, AgentState, Command, CommandState};
    use serde_json::json;

    fn binding() -> (Agent, Command) {
        let mut agent = Agent::new("agent-1", "project-1", "codex", "profile-1", 1);
        agent.generation = 2;
        agent.origin = AgentOrigin::External;
        agent.observe_only = true;
        agent.native_scope = Some("scope-1".to_owned());
        agent.native_ref = Some("thread-1".to_owned());
        agent.state = AgentState::Unknown;
        let command = Command {
            id: "018f0a00-0000-7000-8000-000000000001".to_owned(),
            principal: "owner".to_owned(),
            method: "command.submit".to_owned(),
            verb: "attach".to_owned(),
            target: Some(agent.id.clone()),
            selector: None,
            expected_generation: Some(2),
            generation: Some(2),
            args: json!({}),
            state: CommandState::Dispatched,
            turn_id: None,
            request_id: None,
            outcome: None,
            created_at: 1,
            updated_at: 1,
            probe: false,
            config_revision: 0,
            config: None,
        };
        (agent, command)
    }

    fn failed_result() -> serde_json::Value {
        json!({
            "agent_id":"agent-1", "generation":2,
            "command_id":"018f0a00-0000-7000-8000-000000000001",
            "native_scope":"scope-1", "native_ref":"thread-1",
            "state":"failed", "observation_state":"unknown",
            "reader_closed":true, "reason":"native_start_failed"
        })
    }

    #[test]
    fn failed_attach_requires_exact_subject_command_and_confirmed_reader_close() {
        let (agent, command) = binding();
        let value = validate_observer_command_result(failed_result(), &agent, &command, "failed")
            .expect("exact failed attach result is admitted");
        assert_eq!(value["state"], "failed");
        assert_eq!(value["reason"], "native_start_failed");
        assert!(value.get("reader_closed").is_none());

        let mut changed = failed_result();
        changed["command_id"] = json!("018f0a00-0000-7000-8000-000000000002");
        assert_eq!(
            validate_observer_command_result(changed, &agent, &command, "failed")
                .unwrap_err()
                .code,
            "source_integrity"
        );
        let mut changed = failed_result();
        changed["native_ref"] = json!("another-thread");
        assert_eq!(
            validate_observer_command_result(changed, &agent, &command, "failed")
                .unwrap_err()
                .code,
            "source_integrity"
        );
        let mut changed = failed_result();
        changed["reader_closed"] = json!(false);
        assert_eq!(
            validate_observer_command_result(changed, &agent, &command, "failed")
                .unwrap_err()
                .code,
            "source_integrity"
        );
        let mut changed = failed_result();
        changed["reason"] = json!("unknown");
        assert_eq!(
            validate_observer_command_result(changed, &agent, &command, "failed")
                .unwrap_err()
                .code,
            "source_integrity"
        );
        assert_eq!(
            validate_observer_command_result(
                json!({"state":"unknown"}),
                &agent,
                &command,
                "attached",
            )
            .unwrap_err()
            .code,
            "source_integrity",
            "uncertain attach results stay outside the definite Failed path"
        );
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

/// Capture the exact one positive matching `command.dispatched` event sequence
/// from the committed dispatch change events for this command. If missing,
/// non-positive, or ambiguous, fails closed by returning `None`.
fn capture_dispatch_seq(command: &Command, events: &[Event]) -> Option<i64> {
    let mut matched_seq: Option<i64> = None;
    for event in events {
        if event.seq > 0
            && event.kind == "command.dispatched"
            && event.agent == command.target
            && event.generation == command.generation
            && event.data.get("command_id").and_then(Value::as_str) == Some(&command.id)
            && event.data.get("verb").and_then(Value::as_str) == Some(&command.verb)
        {
            if matched_seq.is_some() {
                return None;
            }
            matched_seq = Some(event.seq);
        }
    }
    matched_seq
}

// These local Store helpers roll back their transaction on a validation or
// capacity/CAS refusal. Persistence errors remain uncertain; this classification
// is deliberately separate from errors returned by a native adapter after I/O.
fn coordination_commit_error_state(code: &str) -> CommandState {
    match code {
        "invalid" | "not_allowed" | "not_found" | "stale_generation" | "secret_in_payload"
        | "history_gap" | "version_conflict" | "mailbox_full" => CommandState::Rejected,
        _ => CommandState::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::{Command, capture_dispatch_seq, validate_retained_resume_agent};
    use crate::common::now_ms;
    use eliot_core::{Agent, AgentState, CommandState, Error, Event, FailureOrigin, SubmitCommand};
    use eliot_plugin::sync::LockExt;
    use serde_json::json;
    use ulid::Ulid;

    #[tokio::test]
    async fn busy_rejection_retains_redacted_bound_in_command_and_event() {
        let router = super::super::tests::router();
        let secret = "s".repeat(1100);
        router.inner.secrets.lock_recover().insert(secret.clone());
        let id = Ulid::new().to_string();
        {
            let mut store = router.inner.store.lock().await;
            let mut request = SubmitCommand::new(id.clone(), "launch");
            request.args = json!({"harness":"codex", "profile":"luna", "project":"k65-regression", "workspace":".",
                "profile_snapshot":{"harness":"codex"}});
            store.submit("owner", request, now_ms()).unwrap();
        }
        router
            .reject_undispatched_error(
                &id,
                &Error::new("busy", format!("metadata_readers limit=4 {secret}"))
                    .with_where("router.dispatch.discovery_readers")
                    .with_origin(
                        FailureOrigin::Swarm,
                        "discovery readers at their in-flight bound",
                    ),
            )
            .await;
        let store = router.inner.store.lock().await;
        let command = store.get_command(&id).unwrap().unwrap();
        assert_eq!(command.state, CommandState::Rejected);
        let outcome = command.outcome.unwrap();
        assert_eq!(outcome["reason"], "busy");
        let message = outcome["message"].as_str().unwrap();
        assert!(message.contains("metadata_readers limit=4"));
        assert!(!message.contains(&"s".repeat(100)));
        assert_eq!(outcome["where"], "router.dispatch.discovery_readers");
        assert_eq!(outcome["origin"], "swarm");
        assert_eq!(
            outcome["origin_evidence"],
            "discovery readers at their in-flight bound"
        );
        let rejected = store
            .read_events(0, 100, None)
            .unwrap()
            .into_iter()
            .find(|event| event.kind == "command.rejected")
            .unwrap();
        assert_eq!(rejected.data["outcome"], outcome);
        assert_eq!(command.generation, Some(0));
    }

    #[tokio::test]
    async fn dispatch_store_read_error_marks_unknown_instead_of_dropping() {
        use std::sync::Arc;
        // File-backed Store so a second connection can break command reads
        // the way a wedged SQLite file breaks production dispatch.
        let dir = std::env::temp_dir().join("eliot-k94-dispatch-read");
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).expect("dispatch read fixture dir");
        let db = dir.join("state.sqlite");
        let id = Ulid::new().to_string();
        let router = {
            let mut store = eliot_store::Store::open(&db).expect("open fixture store");
            let mut request = SubmitCommand::new(id.clone(), "launch");
            request.args = json!({"harness":"codex","profile":"luna","project":"k94-regression","workspace":".","profile_snapshot":{"harness":"codex"}});
            store
                .submit("owner", request, now_ms())
                .expect("admit launch");
            super::super::Router::new(
                dir.clone(),
                "owner-credential".to_owned(),
                Arc::new(tokio::sync::Mutex::new(store)),
                crate::bus::Bus::new(16),
            )
        };
        rusqlite::Connection::open(&db)
            .expect("reopen fixture db")
            .execute("DROP TABLE commands", ())
            .expect("break command reads");
        assert!(router.prepare_command_dispatch(&id, false).await.is_none());
        assert!(router.unknown_ids().contains(&id));
        assert_eq!(
            router.unknown_cause(&id).as_deref(),
            Some("store_command_read_unknown")
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn coordination_stale_cas_settles_rejected_without_a_second_write() {
        let router = super::super::tests::router();
        let now = crate::common::now_ms();
        let child_id = {
            let mut store = router.inner.store.lock().await;
            let definition = json!({"id": "cas-refusal", "enabled": true});
            let state = store
                .ensure_rule_state(
                    "owner",
                    &eliot_store::RuleDefinitionInput {
                        id: "cas-refusal".to_owned(),
                        canonical_definition: definition.to_string(),
                        definition_digest: eliot_core::canonical_json_sha256(&definition).unwrap(),
                        enabled: true,
                    },
                    now,
                )
                .unwrap();
            let seed = store
                .var_set(
                    "owner",
                    &eliot_core::api::VarSetRequest {
                        id: ulid::Ulid::new().to_string(),
                        scope: Some("global".to_owned()),
                        key: "result".to_owned(),
                        value: json!("original"),
                        expect_version: Some(0),
                        note: None,
                    },
                    now,
                )
                .unwrap();
            assert!(seed.events[0].cause.is_none(), "standalone event semantics");
            let mut request = eliot_core::SubmitCommand::new("", "var.set");
            request.args = json!({"scope": "global", "key": "result", "value": "replacement", "expect_version": 0});
            store
                .admit_rule_action("owner", &state, seed.events[0].seq, request, now)
                .unwrap()
                .child
                .unwrap()
                .command
                .id
        };
        router
            .clone()
            .process_command_inner(child_id.clone(), None)
            .await;
        let store = router.inner.store.lock().await;
        let command = store.get_command(&child_id).unwrap().unwrap();
        assert_eq!(command.state, CommandState::Rejected);
        assert_eq!(command.outcome.unwrap()["code"], "version_conflict");
        let value = store
            .var_get(
                "owner",
                &eliot_core::api::VarGetRequest {
                    scope: Some("global".to_owned()),
                    key: "result".to_owned(),
                },
            )
            .unwrap();
        assert_eq!(value.version, 1);
        assert_eq!(value.value, json!("original"));
    }

    #[test]
    fn coordination_commit_classification_keeps_persistence_unknown() {
        for code in [
            "mailbox_full",
            "version_conflict",
            "not_allowed",
            "history_gap",
        ] {
            assert_eq!(
                super::coordination_commit_error_state(code),
                CommandState::Rejected
            );
        }
        for code in ["internal", "unavailable", "source_integrity", "bad_state"] {
            assert_eq!(
                super::coordination_commit_error_state(code),
                CommandState::Unknown
            );
        }
    }

    #[tokio::test]
    async fn dispatch_fence_refusal_settles_rejected_instead_of_stranding() {
        use eliot_core::SubmitCommand;
        let router = super::super::tests::router();
        let now = crate::common::now_ms();
        let attach_id = ulid::Ulid::new().to_string();
        let (agent_id, dispatch_error) = {
            let mut store = router.inner.store.lock().await;
            let mut attach = SubmitCommand::new(attach_id.clone(), "attach");
            attach.args = json!({
                "mode": "observe", "profile": "codex-fixture", "harness": "codex",
                "native_scope": "observe-scope", "native_ref": "thread-1",
                "profile_snapshot": {"harness": "codex", "model": "fixture"},
            });
            let admitted = store.submit("owner", attach, now).unwrap();
            assert_eq!(admitted.command.state, CommandState::Accepted);
            let agent = admitted
                .command
                .target
                .clone()
                .expect("attach admits its agent");
            // Fence the attach dispatch with a still-pending detach: the Store
            // dispatch refuses `busy` after commit but before any native I/O.
            let mut detach = SubmitCommand::new(ulid::Ulid::new().to_string(), "detach");
            detach.target = Some(agent.clone());
            detach.args = json!({"mode": "observe"});
            let admitted_detach = store.submit("owner", detach, now).unwrap();
            assert_eq!(admitted_detach.command.state, CommandState::Accepted);
            let error = store.dispatch(&attach_id, now).unwrap_err();
            assert_eq!(error.code, "busy");
            (agent, error)
        };
        router
            .settle_dispatch_error(&attach_id, &dispatch_error, false)
            .await;
        let store = router.inner.store.lock().await;
        let settled = store.get_command(&attach_id).unwrap().unwrap();
        assert_eq!(settled.state, CommandState::Rejected);
        assert_eq!(
            settled.outcome,
            Some(
                json!({"reason": dispatch_error.code, "message": dispatch_error.message, "where": "router.dispatch.unknown", "origin": "unknown", "origin_evidence": "no origin classified where this error was created"})
            )
        );
        // No generation, source, or token changed: the fence fired before any
        // native effect, and the command no longer blocks drain or re-observe.
        let agent = store.get_agent(&agent_id).unwrap().unwrap();
        assert_eq!(agent.generation, 0);
        assert!(store.list_source_states(&agent_id, 0).unwrap().is_empty());
        assert!(
            !store
                .list_unsettled_commands(100)
                .unwrap()
                .iter()
                .any(|command| command.id == attach_id)
        );
    }

    fn resume_command(agent_id: &str) -> Command {
        Command {
            id: "resume-fixture".to_owned(),
            principal: "owner".to_owned(),
            method: "command.submit".to_owned(),
            verb: "resume".to_owned(),
            target: Some(agent_id.to_owned()),
            selector: None,
            expected_generation: Some(2),
            generation: Some(2),
            args: json!({}),
            state: CommandState::Accepted,
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

    fn retained_agent(harness: &str) -> Agent {
        let mut agent = Agent::new("a7", "descriptive-project", harness, "profile", 10);
        agent.generation = 2;
        agent.state = AgentState::Stopped;
        agent.native_scope = Some(format!("{harness}-scope-a7"));
        agent.native_ref = Some(format!("{harness}-subject-a7"));
        agent
    }

    #[test]
    fn resume_capture_admits_connected_harnesses() {
        let resume = resume_command("a7");
        for harness in ["muse", "claude", "opencode"] {
            let agent = retained_agent(harness);
            assert_eq!(
                validate_retained_resume_agent(&agent, &resume).unwrap(),
                2,
                "{harness} resume captures its retained generation"
            );
        }
        // A Codex binding on a non-Codex agent is still refused.
        let mut agent = retained_agent("opencode");
        agent.profile_snapshot = json!({"native_store_binding": {"home": "h"}});
        assert_eq!(
            validate_retained_resume_agent(&agent, &resume)
                .unwrap_err()
                .code,
            "source_integrity"
        );
        // Harnesses without a connected resume stay unsupported.
        let agent = retained_agent("acp");
        assert_eq!(
            validate_retained_resume_agent(&agent, &resume)
                .unwrap_err()
                .code,
            "unsupported"
        );
    }

    fn test_event(
        seq: i64,
        kind: &str,
        agent: Option<&str>,
        generation: Option<i64>,
        command_id: &str,
        verb: &str,
    ) -> Event {
        Event {
            seq,
            ts: 100,
            kind: kind.to_owned(),
            agent: agent.map(ToOwned::to_owned),
            subject: None,
            generation,
            source: "daemon".to_owned(),
            native_id: None,
            data: json!({"command_id": command_id, "verb": verb}),
            cause: None,
        }
    }

    fn test_command(
        id: &str,
        verb: &str,
        target: Option<&str>,
        generation: Option<i64>,
    ) -> Command {
        Command {
            id: id.to_owned(),
            principal: "owner".to_owned(),
            method: "command.submit".to_owned(),
            verb: verb.to_owned(),
            target: target.map(ToOwned::to_owned),
            selector: None,
            expected_generation: generation,
            generation,
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

    #[test]
    fn capture_dispatch_seq_selects_exact_committed_event() {
        let cmd = test_command("cmd-1", "send", Some("agent-1"), Some(1));
        let events = vec![
            test_event(
                10,
                "turn.started",
                Some("agent-1"),
                Some(1),
                "cmd-0",
                "send",
            ),
            test_event(
                11,
                "command.dispatched",
                Some("agent-2"),
                Some(1),
                "cmd-other",
                "send",
            ),
            test_event(
                12,
                "command.dispatched",
                Some("agent-1"),
                Some(2),
                "cmd-1",
                "send",
            ),
            test_event(
                13,
                "command.dispatched",
                Some("agent-1"),
                Some(1),
                "cmd-1",
                "stop",
            ),
            test_event(
                14,
                "command.dispatched",
                Some("agent-1"),
                Some(1),
                "cmd-different",
                "send",
            ),
            test_event(
                15,
                "command.dispatched",
                Some("agent-1"),
                Some(1),
                "cmd-1",
                "send",
            ),
            test_event(
                16,
                "turn.completed",
                Some("agent-1"),
                Some(1),
                "cmd-1",
                "send",
            ),
        ];
        assert_eq!(
            capture_dispatch_seq(&cmd, &events),
            Some(15),
            "exact matching event sequence is extracted from committed change events"
        );
    }

    #[test]
    fn capture_dispatch_seq_fails_closed_when_missing_or_ambiguous() {
        let cmd = test_command("cmd-1", "send", Some("agent-1"), Some(1));

        // Missing matching event -> None
        let empty_events = vec![];
        assert_eq!(capture_dispatch_seq(&cmd, &empty_events), None);

        let unrelated_events = vec![test_event(
            1,
            "command.dispatched",
            Some("agent-1"),
            Some(1),
            "cmd-2",
            "send",
        )];
        assert_eq!(capture_dispatch_seq(&cmd, &unrelated_events), None);

        // Non-positive sequence -> None
        let zero_seq = vec![test_event(
            0,
            "command.dispatched",
            Some("agent-1"),
            Some(1),
            "cmd-1",
            "send",
        )];
        assert_eq!(capture_dispatch_seq(&cmd, &zero_seq), None);

        let neg_seq = vec![test_event(
            -1,
            "command.dispatched",
            Some("agent-1"),
            Some(1),
            "cmd-1",
            "send",
        )];
        assert_eq!(capture_dispatch_seq(&cmd, &neg_seq), None);

        // Ambiguous matching events (> 1) -> None (fail closed)
        let ambiguous_events = vec![
            test_event(
                21,
                "command.dispatched",
                Some("agent-1"),
                Some(1),
                "cmd-1",
                "send",
            ),
            test_event(
                22,
                "command.dispatched",
                Some("agent-1"),
                Some(1),
                "cmd-1",
                "send",
            ),
        ];
        assert_eq!(
            capture_dispatch_seq(&cmd, &ambiguous_events),
            None,
            "ambiguous matching events fail closed without native IO"
        );
    }

    #[test]
    fn capture_dispatch_seq_retained_retry_never_allocates_fresh_seq() {
        let cmd = test_command("cmd-retry", "send", Some("agent-1"), Some(1));
        let original_event = test_event(
            55,
            "command.dispatched",
            Some("agent-1"),
            Some(1),
            "cmd-retry",
            "send",
        );
        let events = vec![original_event];

        // First dispatch / capture
        let seq1 = capture_dispatch_seq(&cmd, &events);
        assert_eq!(seq1, Some(55));

        // Retained retry evaluation against the committed events must retain exact seq, never a fresh seq
        let seq2 = capture_dispatch_seq(&cmd, &events);
        assert_eq!(seq2, seq1);
        assert_eq!(seq2, Some(55));
    }
}
