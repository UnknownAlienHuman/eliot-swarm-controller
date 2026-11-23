//! Command execution: dispatch a Store-admitted command to native I/O and settle it.
//!
//! This module holds `process_command_inner` with its private preparation,
//! dispatch-wait and settlement helpers as plain `impl Router`
//! blocks. Shared admission, read and progress helpers stay in the parent
//! module; per-verb executors live in `command_verbs`. Native I/O still runs
//! outside the Store lock exactly as before.
use super::{
    MODE_FORCE, Router, RouterInner, RunWorkerHandle, validate_project_rule_launch_command,
    validate_project_rule_resume_command, validate_retained_resume_agent,
};
use crate::common::now_ms;
use crate::router::plugin_host::PendingCommand;
use eliot_core::{
    Agent, AgentState, Command, CommandState, ContextItem, Error, Event, FailureOrigin,
    FrozenScriptRun, InjectPoint, Result, SecretPolicy, WorkspacePhase,
    api::{
        FrozenGitFetch, FrozenGitPublish, ProjectRegistrationView, configure_uses_native_session,
    },
    classify_dispatched_error,
};
use eliot_core::{is_coordination_verb, is_start_verb, is_user_event_verb};
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

struct DispatchedCommand {
    command: Command,
    plugin: Option<Result<(Agent, PendingCommand)>>,
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
        let prepare_started = std::time::Instant::now();
        let Some(prepared) = self
            .prepare_command_dispatch(&id, linked_id.is_some())
            .await
        else {
            return;
        };
        crate::logging::record_debug(
            &self.secret_policy_snapshot(),
            "command.prepared",
            &[("command_id", &id), ("verb", &prepared.command.verb)],
            &[(
                "elapsed_ms",
                prepare_started
                    .elapsed()
                    .as_millis()
                    .min(u128::from(u64::MAX)) as u64,
            )],
        );
        let Some(dispatched) = self
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
            Ok(Some(command))
                if matches!(command.state, CommandState::Accepted | CommandState::Queued) =>
            {
                command
            }
            Ok(_) => return None,
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
            let point = eliot_core::decode_retained_inject_args(&command.args)
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
        let assignment = self.inner.store.lock().await.check_assignment_dispatch(id);
        if let Err(error) = assignment {
            self.reject_undispatched_error(id, &error).await;
            return None;
        }
        if command.verb == "send" && !self.capture_send_owner_rules_delivery(id, &command).await {
            return None;
        }
        if !self.prepare_start_workspace(id, &command).await {
            return None;
        }
        let inputs = self.load_dispatch_binding(id, &command).await?;
        if let Err(error) = self.check_rule_worktree_guard(&command).await {
            self.reject_undispatched_error(id, &error).await;
            return None;
        }
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
        if command.verb == "git.integrate" {
            let frozen = match serde_json::from_value::<eliot_core::api::FrozenGitIntegrate>(
                command.args.clone(),
            ) {
                Ok(frozen) if frozen.validate().is_ok() => frozen,
                _ => {
                    self.reject_undispatched_error(
                        id,
                        &Error::new(
                            "invalid",
                            "git.integrate args are not a valid frozen intent",
                        )
                        .with_where("router.dispatch.git_integrate_gate"),
                    )
                    .await;
                    return Err(());
                }
            };
            let principal_matches = match (command.principal.as_str(), frozen.caller.as_ref()) {
                ("owner", None) => true,
                (principal, None)
                    if principal.strip_prefix("project:") == Some(frozen.repository.as_str()) =>
                {
                    true
                }
                (principal, Some(caller)) => {
                    principal == format!("agent:{}:{}", caller.agent, caller.generation)
                }
                _ => false,
            };
            if !principal_matches {
                self.reject_undispatched_error(
                    id,
                    &Error::new(
                        "not_allowed",
                        "git.integrate principal differs from frozen authority",
                    )
                    .with_where("router.dispatch.git_integrate_gate"),
                )
                .await;
                return Err(());
            }
            let registration = {
                let store = self.inner.store.lock().await;
                store.get_project_registration("owner", &frozen.repository)
            };
            let registration = match registration {
                Ok(Some(registration))
                    if registration.revision == frozen.registration_revision
                        && registration.id == frozen.repository
                        && registration.checkout == frozen.canonical_checkout
                        && registration.repo_common_dir == frozen.common_dir =>
                {
                    registration
                }
                Ok(_) => {
                    self.reject_undispatched_error(
                        id,
                        &Error::new("not_allowed", "git.integrate project registration is stale")
                            .with_where("router.dispatch.git_integrate_gate"),
                    )
                    .await;
                    return Err(());
                }
                Err(error) => {
                    self.reject_undispatched_error(id, &error).await;
                    return Err(());
                }
            };
            if let Some(caller) = &frozen.caller {
                let agent = {
                    let store = self.inner.store.lock().await;
                    store.get_agent(&caller.agent)
                };
                let agent = match agent {
                    Ok(Some(agent)) => agent,
                    Ok(None) => {
                        self.reject_undispatched_error(
                            id,
                            &Error::new("stale_generation", "git.integrate manager disappeared")
                                .with_where("router.dispatch.git_integrate_gate"),
                        )
                        .await;
                        return Err(());
                    }
                    Err(error) => {
                        self.reject_undispatched_error(id, &error).await;
                        return Err(());
                    }
                };
                if agent.generation != caller.generation
                    || agent.is_terminal()
                    || agent.observe_only
                    || agent.control != caller.control
                    || agent.project_scope() != caller.project_scope
                    || agent.workspace.as_deref() != Some(caller.workspace.as_str())
                    || !registration
                        .actions
                        .iter()
                        .any(|action| action == "git.integrate")
                    || frozen.fast_forward.as_ref().is_some_and(|target| {
                        agent.branch.as_deref() != Some(target.branch.as_str())
                    })
                {
                    self.reject_undispatched_error(
                        id,
                        &Error::new("not_allowed", "git.integrate manager authority changed")
                            .with_where("router.dispatch.git_integrate_gate"),
                    )
                    .await;
                    return Err(());
                }
                let identity = match crate::workspace_git::WorkspaceGit::new() {
                    Ok(git) => git.repository_identity(&caller.workspace).await,
                    Err(error) => Err(error),
                };
                let identity = match identity {
                    Ok(identity) => identity,
                    Err(error) => {
                        self.reject_undispatched_error(id, &error).await;
                        return Err(());
                    }
                };
                if identity.0 != caller.workspace || identity.1 != frozen.common_dir {
                    self.reject_undispatched_error(
                        id,
                        &Error::new(
                            "not_allowed",
                            "git.integrate manager workspace no longer matches its registered repository",
                        )
                        .with_where("router.dispatch.git_integrate_gate"),
                    )
                    .await;
                    return Err(());
                }
            }
            return Ok(Some(registration));
        }
        if command.verb == "git.publish" {
            let frozen = match serde_json::from_value::<FrozenGitPublish>(command.args.clone()) {
                Ok(frozen) if frozen.validate().is_ok() => frozen,
                _ => {
                    self.reject_undispatched_error(
                        id,
                        &Error::new("invalid", "git.publish args are not a valid frozen intent")
                            .with_where("router.dispatch.git_publish_gate"),
                    )
                    .await;
                    return Err(());
                }
            };
            if command.principal != format!("project:{}", frozen.repository) {
                self.reject_undispatched_error(
                    id,
                    &Error::new(
                        "not_allowed",
                        "git.publish principal does not own the repository",
                    )
                    .with_where("router.dispatch.git_publish_gate"),
                )
                .await;
                return Err(());
            }
            let registration = {
                let store = self.inner.store.lock().await;
                store.get_project_registration("owner", &frozen.repository)
            };
            return match registration {
                Ok(Some(registration))
                    if registration.revision == frozen.registration_revision
                        && registration.id == frozen.repository =>
                {
                    Ok(Some(registration))
                }
                Ok(_) => {
                    self.reject_undispatched_error(
                        id,
                        &Error::new(
                            "not_allowed",
                            "git.publish project registration is unavailable or stale",
                        )
                        .with_where("router.dispatch.git_publish_gate"),
                    )
                    .await;
                    Err(())
                }
                Err(error) => {
                    self.reject_undispatched_error(id, &error).await;
                    Err(())
                }
            };
        }
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
        let registration = if matches!(
            command.verb.as_str(),
            "git.fetch" | "git.publish" | "git.integrate"
        ) {
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
        if command.verb == "launch" && command.args.get("chat") == Some(&Value::Bool(true)) {
            let prepared = self.prepare_chat_directory(command).await;
            if let Err(error) = prepared {
                self.reject_undispatched_error(id, &error).await;
                return false;
            }
        }
        if is_start_verb(command.verb.as_str())
            && let Err(error) = self.inner.workspaces.prepare(id).await
        {
            self.settle_workspace_prepare_error(command, &error).await;
            return false;
        }
        if command.verb == "launch" && !self.capture_launch_instruction_delivery(id, command).await
        {
            return false;
        }
        if command.verb == "resume" && !self.capture_resume_owner_rules_delivery(id, command).await
        {
            return false;
        }
        if command.verb == "launch"
            && command
                .args
                .get(eliot_core::PROJECT_CHECK_GOAL_ARG)
                .is_some()
            && !self.capture_launch_goal_baseline(id, command).await
        {
            return false;
        }
        true
    }

    /// Inventory the actual execution checkout after workspace preparation,
    /// then persist its facts and optional primary source before start dispatch.
    async fn capture_launch_instruction_delivery(&self, id: &str, command: &Command) -> bool {
        let Some(agent_id) = command.target.as_deref() else {
            return false;
        };
        let initial = {
            let store = self.inner.store.lock().await;
            (|| {
                let agent = store
                    .get_agent(agent_id)?
                    .ok_or_else(|| Error::new("not_found", "launch Agent was not found"))?;
                let existing = store.get_agent_instruction_delivery(agent_id)?;
                let linked = store.get_linked_send(id)?;
                Ok::<_, Error>((agent, existing, linked))
            })()
        };
        let (agent, existing, linked) = match initial {
            Ok(values) => values,
            Err(error) => {
                self.abandon_unreadable_command(id, &error).await;
                return false;
            }
        };
        if agent.chat {
            return self.capture_launch_owner_rules_delivery(id, &agent).await;
        }
        if existing.is_some() {
            return self.capture_launch_owner_rules_delivery(id, &agent).await;
        }

        let workspace = agent.workspace.clone();
        let harness = agent.harness.clone();
        let profile = agent.profile_snapshot.clone();
        let capture = tokio::task::spawn_blocking(move || {
            super::instructions::capture(workspace.as_deref(), &harness, &profile)
        })
        .await;
        let mut capture = match capture {
            Ok(capture) => capture,
            Err(_) => {
                let mut unavailable =
                    super::instructions::capture(None, &agent.harness, &agent.profile_snapshot);
                unavailable.facts.source_workspace = agent.workspace.clone();
                unavailable
            }
        };

        super::instructions::reuse_explicit_delivery(
            &mut capture,
            &command.args["instruction_sources"],
        );
        let mut expected_text = None;
        let mut delivered_text = None;
        if capture.should_deliver_primary {
            match (agent.workspace.as_deref(), linked.as_ref()) {
                (Some(workspace), Some(linked)) => {
                    if let Some(text) = linked.args.get("text").and_then(Value::as_str) {
                        match self
                            .deliver_instruction_source(
                                std::path::Path::new(workspace),
                                text,
                                &capture.facts.primary_path,
                            )
                            .await
                        {
                            Ok((text_after, delivery)) => {
                                super::instructions::apply_delivery(&mut capture, delivery);
                                expected_text = Some(text.to_owned());
                                delivered_text = Some(text_after);
                            }
                            Err(_) => super::instructions::mark_unavailable(&mut capture),
                        }
                    } else {
                        super::instructions::mark_unavailable(&mut capture);
                    }
                }
                _ => super::instructions::mark_unavailable(&mut capture),
            }
        }

        let result = self
            .inner
            .store
            .lock()
            .await
            .record_launch_instruction_delivery(
                id,
                &capture.facts,
                expected_text.as_deref(),
                delivered_text.as_deref(),
                now_ms(),
            );
        match result {
            Ok(_) => self.capture_launch_owner_rules_delivery(id, &agent).await,
            Err(error) => {
                self.abandon_unreadable_command(id, &error).await;
                false
            }
        }
    }

    async fn capture_launch_owner_rules_delivery(&self, id: &str, agent: &Agent) -> bool {
        let existing = self
            .inner
            .store
            .lock()
            .await
            .get_agent_owner_rules_delivery(&agent.id);
        match existing {
            Ok(Some(view))
                if view.status != eliot_core::api::OwnerRulesDeliveryStatus::NotDelivered =>
            {
                return true;
            }
            Ok(Some(_)) => {
                let error = Error::new(
                    "owner_rules_unavailable",
                    "launch retained a refusal to deliver owner rules",
                );
                self.reject_undispatched_error(id, &error).await;
                return false;
            }
            Ok(None) => {}
            Err(error) => {
                self.abandon_unreadable_command(id, &error).await;
                return false;
            }
        }
        let view = match self.read_owner_rules_receipt(&agent.harness) {
            Ok(Some(view)) => view,
            Ok(None) => return true,
            Err(error) => {
                self.reject_undispatched_error(id, &error).await;
                return false;
            }
        };
        let result = {
            let mut store = self.inner.store.lock().await;
            (|| {
                if store.record_launch_owner_rules_delivery(id, &view)? {
                    return Ok(true);
                }
                if let Some(retained) = store.get_agent_owner_rules_delivery(&agent.id)? {
                    return Ok(
                        retained.status != eliot_core::api::OwnerRulesDeliveryStatus::NotDelivered
                    );
                }
                let mut refused = view;
                refused.status = eliot_core::api::OwnerRulesDeliveryStatus::NotDelivered;
                store.record_launch_owner_rules_delivery(id, &refused)?;
                Ok::<_, Error>(false)
            })()
        };
        match result {
            Ok(true) => true,
            Ok(false) => {
                let error = Error::new(
                    "owner_rules_unavailable",
                    "launch could not retain owner rules delivery; no native start was dispatched",
                );
                self.reject_undispatched_error(id, &error).await;
                false
            }
            Err(error) => {
                self.abandon_unreadable_command(id, &error).await;
                false
            }
        }
    }

    fn read_owner_rules_receipt(
        &self,
        harness: &str,
    ) -> Result<Option<eliot_core::api::OwnerRulesDeliveryView>> {
        use eliot_core::api::{
            OwnerRulesDeliveryMethod, OwnerRulesDeliveryStatus, OwnerRulesDeliveryView,
        };
        let Some((revision, text)) =
            crate::config::settings::owner_rules_snapshot(&self.inner.data_root)?
        else {
            return Ok(None);
        };
        crate::config::settings::validate_owner_rules_text(&text, &self.secret_policy_snapshot())?;
        let method = match harness {
            "codex" => OwnerRulesDeliveryMethod::CodexDeveloperInstructions,
            "claude" => OwnerRulesDeliveryMethod::ClaudeAppendSystemPromptFile,
            "opencode" => OwnerRulesDeliveryMethod::OpenCodeAgentsFile,
            _ => OwnerRulesDeliveryMethod::FirstInput,
        };
        Ok(Some(OwnerRulesDeliveryView {
            revision,
            digest: eliot_core::api::inference::sha256_hex(text.as_bytes()),
            method,
            status: OwnerRulesDeliveryStatus::Pending,
            delivered: false,
        }))
    }

    async fn capture_resume_owner_rules_delivery(&self, id: &str, command: &Command) -> bool {
        let result = async {
            let agent_id = command
                .target
                .as_deref()
                .ok_or_else(|| Error::new("source_integrity", "resume has no Agent target"))?;
            let agent = {
                let store = self.inner.store.lock().await;
                if store.get_agent_owner_rules_delivery(agent_id)?.is_some() {
                    return Ok(());
                }
                store
                    .get_agent(agent_id)?
                    .ok_or_else(|| Error::new("not_found", "resume Agent was not found"))?
            };
            if let Some(view) = self.read_owner_rules_receipt(&agent.harness)? {
                let mut store = self.inner.store.lock().await;
                if !store.record_resume_owner_rules_snapshot(agent_id, agent.generation, &view)?
                    && store.get_agent_owner_rules_delivery(agent_id)?.is_none()
                {
                    return Err(Error::new(
                        "owner_rules_unavailable",
                        "resume could not retain owner rules delivery",
                    ));
                }
            }
            Ok::<_, Error>(())
        }
        .await;
        if let Err(error) = result {
            self.reject_undispatched_error(id, &error).await;
            return false;
        }
        true
    }

    async fn capture_send_owner_rules_delivery(&self, id: &str, command: &Command) -> bool {
        use eliot_core::api::{OwnerRulesDeliveryMethod, OwnerRulesDeliveryStatus};
        let result = async {
            let agent_id = command
                .target
                .as_deref()
                .ok_or_else(|| Error::new("source_integrity", "send has no Agent target"))?;
            let offer = self
                .inner
                .store
                .lock()
                .await
                .get_agent_owner_rules_offer(agent_id)?;
            let Some((view, bound)) = offer else {
                return Ok(());
            };
            if matches!(
                view.status,
                OwnerRulesDeliveryStatus::Delivered | OwnerRulesDeliveryStatus::Unknown
            ) {
                return Ok(());
            }
            let input = command
                .args
                .get("text")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    Error::new("invalid", "send requires text for owner rules delivery")
                })?;
            let already_prepared =
                view.status == OwnerRulesDeliveryStatus::Offered && bound.as_deref() == Some(id);
            let delivered_text =
                if view.method == OwnerRulesDeliveryMethod::FirstInput && !already_prepared {
                    let text = crate::config::settings::owner_rules_for_delivery(
                        &self.inner.data_root,
                        &view.digest,
                        &self.secret_policy_snapshot(),
                    )?;
                    Some(super::instructions::append_owner_rules(
                        input,
                        &view.revision,
                        &view.digest,
                        &text,
                    )?)
                } else {
                    None
                };
            if self
                .inner
                .store
                .lock()
                .await
                .record_send_owner_rules_offer(
                    id,
                    &view.digest,
                    input,
                    delivered_text.as_deref(),
                    now_ms(),
                )?
            {
                return Ok(());
            }
            Err(Error::new(
                "owner_rules_unavailable",
                "send could not bind the retained owner rules to this input",
            ))
        }
        .await;
        if let Err(error) = result {
            self.reject_undispatched_error(id, &error).await;
            return false;
        }
        true
    }

    /// Record A3's starting point in the final workspace before native start.
    async fn capture_launch_goal_baseline(&self, id: &str, command: &Command) -> bool {
        let result = async {
            let agent = command.target.as_deref().ok_or_else(|| {
                Error::new("source_integrity", "scoped launch has no Agent target")
            })?;
            let (goal, workspace) = {
                let store = self.inner.store.lock().await;
                let Some(goal) = store.launch_goal_scope_target(agent, id)? else {
                    return Ok::<_, Error>(());
                };
                let workspace = store
                    .get_agent(agent)?
                    .and_then(|agent| agent.workspace)
                    .ok_or_else(|| {
                        Error::new("invalid", "a scoped launch Goal needs an Agent workspace")
                    })?;
                (goal, workspace)
            };
            let base = self
                .goal_scope_base_for_workspace(&workspace, &goal.definition)
                .await?;
            self.inner.store.lock().await.set_launch_goal_scope(
                id,
                agent,
                &workspace,
                goal.revision,
                base,
                now_ms(),
            )
        }
        .await;
        if let Err(error) = result {
            let write_faulted = self.inner.store.lock().await.is_write_faulted();
            if write_faulted {
                self.abandon_unreadable_command(id, &error).await;
            } else {
                self.reject_undispatched_error(id, &error).await;
            }
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
    ) -> Option<DispatchedCommand> {
        loop {
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
                    None if command.verb == "git.integrate"
                        && self.inner.mode.load(Ordering::SeqCst) == MODE_FORCE =>
                    {
                        Err(Error::new(
                            "bad_state",
                            "force shutdown prevents Git integration dispatch",
                        ))
                    }
                    None if command.verb == "git.publish"
                        && self.inner.mode.load(Ordering::SeqCst) == MODE_FORCE =>
                    {
                        Err(Error::new(
                            "bad_state",
                            "force shutdown prevents Git publication dispatch",
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
                    None if is_coordination_verb(command.verb.as_str())
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
                    Ok((command, context, events)) => {
                        // A retained Dispatched row carries no new commit event.
                        // Only the worker that made this transition may enqueue.
                        let fresh = command.state == CommandState::Dispatched
                            && capture_dispatch_seq(&command, &events).is_some();
                        self.publish_locked(&events);
                        self.note_command_rejection(&command, &events);
                        let plugin = if fresh {
                            self.enqueue_plugin_command(&store, &command, context)
                        } else {
                            None
                        };
                        Ok((DispatchedCommand { command, plugin }, fresh))
                    }
                    Err(error) => Err(error),
                };
                (result, store.is_write_faulted())
            };
            match change.0 {
                Ok((dispatched, _)) if dispatched.command.state == CommandState::Queued => {
                    let target = dispatched.command.target;
                    let generation = dispatched.command.generation;
                    if !self
                        .wait_for_queue_release(id, receiver, target, generation)
                        .await
                    {
                        return None;
                    }
                }
                Ok((dispatched, true)) => return Some(dispatched),
                Ok((_, false)) => {
                    self.signal_progress();
                    return None;
                }
                Err(error) => {
                    self.settle_dispatch_error(id, &error, change.1).await;
                    return None;
                }
            }
        }
    }

    fn enqueue_plugin_command(
        &self,
        store: &eliot_store::Store,
        command: &Command,
        context: Vec<ContextItem>,
    ) -> Option<Result<(Agent, PendingCommand)>> {
        if !matches!(
            command.verb.as_str(),
            "send"
                | "steer"
                | "interrupt"
                | "approve"
                | "deny"
                | "answer"
                | "inject"
                | "configure"
                | "compact"
        ) {
            return None;
        }
        let agent = match command
            .target
            .as_deref()
            .map(|target| store.get_agent(target))
        {
            Some(Ok(Some(agent))) => agent,
            _ => {
                return Some(Err(Error::new(
                    "native_not_dispatched",
                    "the command's Agent was unavailable for plugin handoff",
                )));
            }
        };
        if command.verb == "configure"
            && !configure_uses_native_session(&agent.harness, &command.args)
        {
            return None;
        }
        let mut command = command.clone();
        if command.verb == "inject" {
            let Some(text) = command.args.get("text").and_then(Value::as_str) else {
                return Some(Err(Error::new("invalid", "context delivery requires text")));
            };
            if text.len() > eliot_core::limits::MAX_TEXT_BYTES {
                return Some(Err(Error::new(
                    "frame_too_large",
                    "inject text exceeds 512 KiB",
                )));
            }
            let text = match store.render_injection(&command, text) {
                Ok(text) => text,
                Err(error) => return Some(Err(error)),
            };
            command.args["text"] = Value::String(text);
        }
        let pending = self
            .inner
            .plugin_host
            .enqueue_command(&agent, command, context);
        Some(pending.map(|pending| (agent, pending)))
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
    /// `git.fetch` and `git.publish` carry their own settlement; every other verb loads its
    /// Agent and runs one verb-family helper.
    async fn execute_dispatched_command(
        &self,
        dispatched: DispatchedCommand,
        git_fetch_registration: Option<ProjectRegistrationView>,
        resume_source_state: Option<Value>,
        linked_id: Option<String>,
    ) {
        let DispatchedCommand {
            command: dispatched,
            plugin,
        } = dispatched;
        let id = dispatched.id.clone();
        if dispatched.is_owner_interrupt()
            && let Some(plugin) = plugin
        {
            self.execute_owner_interrupt(&dispatched, plugin).await;
            self.signal_progress();
            return;
        }
        if let Some(plugin) = plugin {
            let (agent, pending) = match plugin {
                Ok(dispatched) => dispatched,
                Err(error) => {
                    self.settle_plugin_error(&id, &error).await;
                    return;
                }
            };
            let response = pending.wait().await;
            let trailing_signal = match dispatched.verb.as_str() {
                "send" => self.execute_send_command(&id, &agent, response).await,
                "configure" => {
                    self.execute_native_configure_command(&id, &agent, response)
                        .await;
                    true
                }
                "compact" => {
                    let response = response.and_then(|value| {
                        if value["state"] == "applied"
                            && (value["command_id"].as_str() != Some(id.as_str())
                                || value["compaction"]["completed"] != true
                                || value["compaction"]["receipt_id"]
                                    .as_str()
                                    .is_none_or(|receipt| receipt.trim().is_empty()))
                        {
                            return Err(Error::new(
                                "unknown",
                                "native compaction completion is unconfirmed",
                            ));
                        }
                        Ok(value)
                    });
                    self.execute_control_command(&id, &agent, response).await
                }
                _ => self.execute_control_command(&id, &agent, response).await,
            };
            if trailing_signal {
                self.signal_progress();
            }
            return;
        }
        if dispatched.verb == "run" {
            self.execute_run_command(dispatched).await;
            return;
        }
        if dispatched.verb == "git.integrate" {
            self.execute_git_integrate_command(&id, dispatched, git_fetch_registration)
                .await;
            return;
        }
        if dispatched.verb == "git.fetch" {
            self.execute_git_fetch_command(&id, dispatched, git_fetch_registration)
                .await;
            return;
        }
        if dispatched.verb == "git.publish" {
            self.execute_git_publish_command(&id, dispatched, git_fetch_registration)
                .await;
            return;
        }
        if is_user_event_verb(dispatched.verb.as_str()) {
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
            "stop" => self.execute_stop_command(&id, &agent, dispatched).await,
            "configure" => {
                self.execute_configure_command(&id, &agent).await;
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

    pub(super) async fn wait_run_worker(&self, run_id: &str) {
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

    pub(super) async fn settle_run_cancelled_before_start(&self, run_id: &str) {
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
    pub(super) fn sanitized_failure(&self, error: &Error, default_where: &str) -> Error {
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
    pub(super) fn ensure_outcome_failure_fields(value: &mut Value, default_where: &str) {
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
    pub(super) fn command_error_outcome(&self, error: &Error, code_key: &str) -> Value {
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
        let error = self.sanitized_failure(error, "router.dispatch.preflight");
        let outcome = self.command_error_outcome(&error, "reason");
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
        if let Ok(change) = &result
            && !change.events.is_empty()
        {
            self.log_command_error(id, Some(&change.value), &error);
        }
        self.finish_settlement(id, result, write_faulted, "store_settlement_failed", false)
            .await;
    }

    pub(super) async fn settle_observe_attach(&self, id: &str, snapshot: Value) -> Option<Command> {
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

    pub(super) async fn settle_observe_detach(&self, id: &str, outcome: Value) -> Option<Command> {
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

    pub(super) async fn settle_unspawned_start(&self, id: &str, error: &Error) {
        let error = self.sanitized_failure(error, "swarm.start.pre_spawn");
        let mut outcome = self.command_error_outcome(&error, "code");
        outcome["reason"] = json!(error.code);
        let (result, write_faulted) = {
            let mut store = self.inner.store.lock().await;
            let result = store.settle_unspawned_start(id, outcome, now_ms());
            if let Ok(change) = &result {
                self.publish_locked(&change.events);
            }
            (result, store.is_write_faulted())
        };
        if result.is_err() {
            // The Store could not validate this proof. The original start
            // failure no longer establishes that no external effect occurred.
            let error = if error.code == "native_not_dispatched" {
                error.with_code("unknown")
            } else {
                error
            };
            self.settle_plugin_error(id, &error).await;
            return;
        }
        self.finish_settlement(id, result, write_faulted, "store_settlement_failed", false)
            .await;
    }

    pub(super) async fn settle_plugin_error(&self, id: &str, error: &Error) {
        let current = {
            let store = self.inner.store.lock().await;
            store.get_command(id).ok().flatten()
        };
        let error = self.sanitized_failure(error, "plugin.command");
        self.log_command_error(id, current.as_ref(), &error);
        let (current_state, command_target, previous_outcome) = match current {
            Some(command) => (
                Some(command.state),
                command.target.zip(command.generation),
                command.outcome,
            ),
            None => (None, None, None),
        };
        let late_batch_cause = command_target.as_ref().and_then(|(agent, generation)| {
            self.inner
                .plugin_host
                .take_late_batch_cause(agent, *generation)
        });
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

    async fn settle_workspace_prepare_error(&self, start: &Command, error: &Error) {
        let id = start.id.as_str();
        let workspace = if let Some(target) = start.target.as_deref() {
            let store = self.inner.store.lock().await;
            store
                .get_workspace(target)
                .ok()
                .flatten()
                .filter(|workspace| {
                    workspace.start_command_id == id
                        && start
                            .generation
                            .and_then(|generation| generation.checked_add(1))
                            == Some(workspace.target_generation)
                })
        } else {
            None
        };
        let ready_resume = start.verb == "resume"
            && workspace
                .as_ref()
                .is_some_and(|workspace| workspace.phase == WorkspacePhase::Ready);
        // A raw error can be rejected regardless of code when the exact
        // creation marker rules out a write and cancellation has not latched.
        let before_git_write = workspace.as_ref().is_some_and(|workspace| {
            workspace.outcome.as_ref().is_some_and(|outcome| {
                outcome["stage"] == "creation"
                    && outcome["native_git_write_dispatched"] == false
                    && outcome["cancel_requested"] != true
                    && (workspace.phase == WorkspacePhase::Creating
                        || ready_resume
                            && outcome["reuse"] == true
                            && outcome["resume_previous"].is_object())
            })
        });
        if before_git_write
            || !ready_resume && matches!(error.code.as_str(), "stale_assignment" | "not_allowed")
        {
            self.reject_undispatched_error(id, error).await;
            self.signal_progress();
            return;
        }
        let error = self.sanitized_failure(error, "store.dispatch.workspace_gate");
        let command = {
            let mut store = self.inner.store.lock().await;
            let command = store.get_command(id).ok().flatten();
            // Older Ready rows can carry reuse evidence without the write
            // marker. Preserve the failure and fence that exact pending resume.
            if (ready_resume || error.code == "unknown")
                && let Some(command) = &command
                && is_start_verb(command.verb.as_str())
                && matches!(command.state, CommandState::Accepted | CommandState::Queued)
                && let Some(target_generation) = command
                    .generation
                    .and_then(|generation| generation.checked_add(1))
                && let Ok(change) = store.settle_workspace_creation(
                    id,
                    target_generation,
                    eliot_core::WorkspaceCreationOutcome::Unknown {
                        code: if ready_resume {
                            error.code.clone()
                        } else {
                            "workspace_identity_unknown".to_owned()
                        },
                        message: if ready_resume {
                            (*error.message).clone()
                        } else {
                            "retained workspace identity could not be confirmed".to_owned()
                        },
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
            command
        };
        self.log_command_error(id, command.as_ref(), &error);
        self.signal_progress();
    }

    fn log_command_error(&self, id: &str, command: Option<&Command>, error: &Error) {
        let mut ids = vec![("command", id)];
        if let Some(command) = command {
            ids.push(("principal", command.principal.as_str()));
            ids.push(("method", command.verb.as_str()));
            if let Some(agent) = command.target.as_deref() {
                ids.push(("agent", agent));
            }
        }
        crate::logging::record(
            &self.secret_policy_snapshot(),
            &error.code,
            &ids,
            Some(error),
        );
    }
}

pub(super) fn validate_observer_command_result(
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
pub(super) fn classify_start_result(value: &Value) -> CommandState {
    if value.get("state").and_then(Value::as_str) == Some("failed") {
        CommandState::Failed
    } else {
        CommandState::Applied
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
pub(super) fn coordination_commit_error_state(code: &str) -> CommandState {
    match code {
        "invalid" | "not_allowed" | "not_found" | "stale_generation" | "secret_in_payload"
        | "history_gap" | "version_conflict" | "mailbox_full" => CommandState::Rejected,
        _ => CommandState::Unknown,
    }
}
