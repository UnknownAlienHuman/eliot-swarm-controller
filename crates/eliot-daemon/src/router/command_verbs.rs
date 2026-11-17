//! Per-verb execution of Store-dispatched commands and their settlement.

use super::{
    Router,
    command::{
        RunWorkerStart, classify_start_result, coordination_commit_error_state,
        validate_observer_command_result,
    },
    context,
};
use crate::common::now_ms;
use eliot_core::{
    Agent, Command, CommandState, ContextItem, Error, FailureOrigin, Result,
    api::{FrozenGitFetch, ProjectRegistrationView},
};
use serde_json::{Value, json};

impl Router {
    /// Run a dispatched `run` command to worker completion, or settle the
    /// pre-start cancellation when Force was requested first.
    pub(super) async fn execute_run_command(&self, dispatched: Command) {
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
    pub(super) async fn execute_git_fetch_command(
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
    pub(super) async fn execute_user_event_command(&self, id: &str, dispatched: Command) {
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
    pub(super) async fn execute_attention_event_command(&self, id: &str, dispatched: Command) {
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
    pub(super) async fn execute_mail_send_command(&self, id: &str, dispatched: Command) {
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
    pub(super) async fn execute_var_set_command(&self, id: &str, dispatched: Command) {
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
    pub(super) async fn execute_attach_command(
        &self,
        id: &str,
        agent: &Agent,
        dispatched: Command,
    ) {
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
    pub(super) async fn execute_worktree_remove_command(&self, id: &str, dispatched: Command) {
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
    pub(super) async fn execute_start_command(
        &self,
        id: &str,
        agent: Agent,
        dispatched: Command,
        resume_source_state: Option<Value>,
        linked_id: Option<String>,
    ) {
        if let Err(error) = self.prepare_agent_artifact_root(&agent).await {
            let mut refused = Error::new("native_not_dispatched", *error.message);
            refused.failure_where = error.failure_where;
            refused.origin = error.origin;
            refused.origin_evidence = error.origin_evidence;
            let refused = super::cleanup::swarm_preparation_failure(
                refused,
                "router.dispatch.prepare_artifact_root",
                "Swarm's Agent artifact root preparation refused the start before dispatch",
            );
            self.settle_plugin_error(id, &refused).await;
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
    pub(super) async fn execute_send_command(
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
    pub(super) async fn execute_control_command(
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
    pub(super) async fn execute_configure_command(
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
    pub(super) async fn execute_stop_command(
        &self,
        id: &str,
        agent: &Agent,
        dispatched: Command,
    ) -> bool {
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
    pub(super) async fn execute_detach_command(
        &self,
        id: &str,
        agent: &Agent,
        dispatched: Command,
    ) {
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
