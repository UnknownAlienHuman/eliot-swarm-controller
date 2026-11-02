//! Command execution: dispatch a Store-admitted command to native I/O and settle it.
//!
//! This module holds `process_command_inner` with its private preparation and
//! settlement helpers as plain `impl Router` blocks. Shared admission, read and
//! progress helpers stay in the parent module; native I/O still runs outside
//! the Store lock exactly as before.
use super::{
    MODE_FORCE, Router, RouterInner, RunWorkerHandle, validate_project_rule_launch_command,
    validate_project_rule_resume_command, validate_retained_resume_agent,
};
use crate::common::now_ms;
use eliot_core::{
    Agent, AgentState, Command, CommandState, Error, FrozenScriptRun, Result, SecretPolicy,
    api::FrozenGitFetch, classify_dispatched_error,
};
use eliot_plugin::{MAX_EVENT_BYTES, sync::LockExt};
use futures_util::future::BoxFuture;
use serde_json::{Value, json};
use std::sync::{Arc, Weak, atomic::Ordering};
use tokio::sync::{broadcast, oneshot, watch};

pub(super) const CONTROLLED_PLUGIN_SOURCE: &str = "stream";

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

impl Router {
    pub(super) fn process_command(
        self,
        id: String,
        linked_id: Option<String>,
    ) -> BoxFuture<'static, ()> {
        Box::pin(self.process_command_inner(id, linked_id))
    }

    pub(super) async fn process_command_inner(self, id: String, linked_id: Option<String>) {
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
        let git_fetch_registration = if command.verb == "git.fetch" {
            let frozen = match serde_json::from_value::<FrozenGitFetch>(command.args.clone()) {
                Ok(frozen) if frozen.validate().is_ok() => frozen,
                _ => {
                    self.reject_undispatched(&id, "invalid").await;
                    return;
                }
            };
            let project_principal = command.principal.strip_prefix("project:");
            if command.principal != "owner" && project_principal != Some(frozen.repository.as_str())
            {
                self.reject_undispatched(&id, "not_allowed").await;
                return;
            }
            let registration = {
                let store = self.inner.store.lock().await;
                store.get_project_registration("owner", &frozen.repository)
            };
            match registration {
                Ok(Some(registration)) => Some(registration),
                Ok(None) => {
                    self.reject_undispatched(&id, "not_allowed").await;
                    return;
                }
                Err(error) => {
                    self.reject_undispatched(&id, &error.code).await;
                    return;
                }
            }
        } else {
            None
        };
        if let Some(project_id) = command.principal.strip_prefix("project:") {
            let registration = if command.verb == "git.fetch" {
                match &git_fetch_registration {
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
            self.settle_workspace_prepare_error(&id, &error).await;
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
                    None if command.verb == "git.fetch"
                        && self.inner.mode.load(Ordering::SeqCst) == MODE_FORCE =>
                    {
                        Err(Error::new(
                            "bad_state",
                            "force shutdown prevents Git fetch dispatch",
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
                                            | "command.unknown"
                                    )
                                    || event.kind.starts_with("workspace.")
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
                    self.settle_dispatch_error(&id, &error, change.1, launch_binding.is_some())
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
        if dispatched.verb == "git.fetch" {
            let Some(registration) = git_fetch_registration else {
                self.settle(
                    &id,
                    CommandState::Rejected,
                    Some(json!({"code":"not_allowed"})),
                )
                .await;
                return;
            };
            let frozen = match serde_json::from_value::<FrozenGitFetch>(dispatched.args.clone()) {
                Ok(frozen) => frozen,
                Err(_) => {
                    self.settle(&id, CommandState::Rejected, Some(json!({"code":"invalid"})))
                        .await;
                    return;
                }
            };
            let mut execution = crate::git_actions::execute(
                &self.inner.data_root,
                &id,
                &frozen,
                &registration,
                self.inner.run_cancellation.subscribe(),
            )
            .await;
            self.redact_native_value(&mut execution.outcome);
            let state = execution.state;
            let expected_outcome = execution.outcome.clone();
            let cleanup_view = execution.cleanup_view.take();
            let settled = self.settle(&id, state, Some(execution.outcome)).await;
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
    pub(super) async fn settle_dispatch_error(
        &self,
        id: &str,
        error: &Error,
        write_faulted: bool,
        native_binding: bool,
    ) {
        // This is only the local Store dispatch result, before any control RPC.
        // A plugin error with the same code still follows post-I/O C1 handling.
        if write_faulted {
            self.note_unknown_if_uncertain(id, "store_dispatch_outcome_unknown")
                .await;
        } else if matches!(
            error.code.as_str(),
            "bad_state" | "stale_generation" | "not_found" | "provider_held" | "not_allowed"
        ) || native_binding
            && matches!(
                error.code.as_str(),
                "source_integrity" | "frame_too_large" | "invalid"
            )
        {
            self.reject_undispatched(id, &error.code).await;
        }
        self.signal_progress();
    }

    pub(super) async fn reject_undispatched(&self, id: &str, reason: &str) {
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
                    },
                    now_ms(),
                )
            {
                self.publish_locked(&change.events);
            }
        }
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
