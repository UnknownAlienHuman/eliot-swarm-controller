//! Timer and event-rule RPC glue: catch-up, manual timer runs, and rule-action admission.
//!
//! `rule.catch_up` replays a retained parent before the current gates, then
//! prepares and admits one bounded catch-up over the rule history without
//! advancing the live cursor. `timer.run` replays an exact external id
//! first, then validates the manual firing against the current timer and
//! registration before admission. `admit_rule_action` is the shared
//! event-rule firing admission also used by the automation driver: retained
//! resume/fetch replays resolve first, then the prepared action is checked
//! against the current registration and admitted through the one Store
//! writer. Scheduling and progress signals stay exactly as before.
use super::{
    EVENT_SCAN_MAX, MODE_OPEN, PreparedSubmission, Principal, Router,
    contains_known_secret_including_keys, require_owner, rule_principal,
    validate_rule_launch_preparation, validate_rule_resume_preparation,
};
use crate::common::now_ms;
use eliot_core::{
    Command, Error, Event, FrozenScriptRun, Result, ScriptTimerInvocation,
    api::{
        FrozenGitFetch, ProjectRegistrationView, RuleCatchUpRequest, RuleStateView,
        TimerRunRequest, TimerStateView,
    },
    is_deterministic_pre_admission_rejection,
};
use eliot_store::{RuleCatchUpAction, RuleCatchUpAdmission, TimerSlotAdmission};
use serde_json::Value;
use std::sync::atomic::Ordering;

impl Router {
    pub(crate) async fn assignment_status(
        &self,
        key: &str,
        max_busy: u64,
    ) -> Result<eliot_store::AssignmentStatus> {
        self.inner
            .store
            .lock()
            .await
            .assignment_status(key, max_busy)
    }

    pub(crate) async fn is_assignment_event(&self, event: &Event, key: &str) -> Result<bool> {
        let (Some(agent), Some(generation)) = (event.agent.as_deref(), event.generation) else {
            return Ok(false);
        };
        self.inner
            .store
            .lock()
            .await
            .is_assignment_event(event.seq, key, agent, generation)
    }

    pub(super) async fn rule_catch_up(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        require_owner(principal)?;
        let request: RuleCatchUpRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
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

    pub(super) async fn timer_run(&self, params: &Value, principal: &Principal) -> Result<Value> {
        require_owner(principal)?;
        let request: TimerRunRequest = eliot_core::api::discovery::decode_params(params.clone())?;
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
            return self
                .timer_run_error_after_replay(
                    &rule_principal,
                    &request,
                    Error::new(
                        "secret_in_payload",
                        "timer.run request contains a known credential",
                    ),
                )
                .await;
        }

        if !self.rule_firings_open() {
            return self
                .timer_run_error_after_replay(
                    &rule_principal,
                    &request,
                    Error::new("unavailable", "automation is draining"),
                )
                .await;
        }
        let project_view = match request.project.as_deref() {
            Some(project) => {
                let registration = self
                    .inner
                    .store
                    .lock()
                    .await
                    .get_project_registration("owner", project)?;
                match registration {
                    Some(registration) => Some(registration),
                    None => {
                        return self
                            .timer_run_error_after_replay(
                                &rule_principal,
                                &request,
                                Error::new("not_allowed", "project registration is not active"),
                            )
                            .await;
                    }
                }
            }
            None => None,
        };
        let Some((expected_rule_revision, definition)) =
            self.rule_definition(&rule_principal, &request.rule)
        else {
            return self
                .timer_run_error_after_replay(
                    &rule_principal,
                    &request,
                    Error::new("not_found", "timer rule was not found"),
                )
                .await;
        };
        if definition.schedule().is_none() {
            return self
                .timer_run_error_after_replay(
                    &rule_principal,
                    &request,
                    Error::new("unsupported", "rule is not a timer"),
                )
                .await;
        }
        let timer = self
            .inner
            .store
            .lock()
            .await
            .get_timer_state(&rule_principal, &request.rule)?;
        let Some(timer) = timer else {
            return self
                .timer_run_error_after_replay(
                    &rule_principal,
                    &request,
                    Error::new("not_found", "timer was not found"),
                )
                .await;
        };
        if !timer.enabled {
            return self
                .timer_run_error_after_replay(
                    &rule_principal,
                    &request,
                    Error::new("bad_state", "timer is disabled or paused"),
                )
                .await;
        }
        if !self.rule_firings_open() {
            return self
                .timer_run_error_after_replay(
                    &rule_principal,
                    &request,
                    Error::new("unavailable", "automation is draining"),
                )
                .await;
        }

        let command = match definition.evaluate_timer() {
            Ok(command) => command,
            Err(error) => {
                return self
                    .timer_run_error_after_replay(&rule_principal, &request, error)
                    .await;
            }
        };
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
        } else if command.args.get("assignment").is_some() {
            if project_view.is_some() {
                Err(Error::new(
                    "not_allowed",
                    "assignments require an Owner timer",
                ))
            } else {
                match crate::automation::assignment::prepare(self, command).await {
                    Ok(command) => self.prepare_owner_submission(command).await,
                    Err(error) => Err(error),
                }
            }
        } else {
            match project_view.as_ref() {
                Some(project) => self.prepare_project_rule_submission(command, project).await,
                None => self.prepare_owner_submission(command).await,
            }
        };
        let prepared = match prepared_result {
            Ok(prepared) => prepared,
            Err(error) => {
                return self
                    .timer_run_error_after_replay(&rule_principal, &request, error)
                    .await;
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
            return self
                .timer_run_error_after_replay(
                    &rule_principal,
                    &request,
                    Error::new("unavailable", "automation is draining"),
                )
                .await;
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

    async fn timer_run_error_after_replay(
        &self,
        principal: &str,
        request: &TimerRunRequest,
        error: Error,
    ) -> Result<Value> {
        let Some(parent) = self.replay_timer_run(principal, request).await? else {
            return Err(error);
        };
        request.validate_response(&parent)?;
        serde_json::to_value(parent)
            .map_err(|_| Error::new("internal", "timer.run result could not be encoded"))
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
        validate_rule_resume_preparation(&prepared, project)?;
        let PreparedSubmission {
            request: action,
            workspace,
        } = prepared;
        if !matches!(
            action.verb.as_str(),
            "stop"
                | "send"
                | "launch"
                | "resume"
                | "run"
                | "git.fetch"
                | "notify"
                | "emit"
                | "mail.send"
                | "var.set"
        ) || workspace.is_some()
        {
            return Err(Error::new(
                "invalid",
                "timer firing must contain one prepared stop, send, launch, resume, Run, mail.send, var.set, or local event command",
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
        if action.verb == "git.fetch" {
            let frozen: FrozenGitFetch = serde_json::from_value(action.args.clone())
                .map_err(|_| Error::new("invalid", "manual timer Git fetch is not frozen"))?;
            frozen.validate()?;
            match project {
                Some(registration) if registration.id == frozen.repository => {}
                Some(_) => {
                    return Err(Error::new(
                        "not_allowed",
                        "manual project timer Git fetch differs from its saved registration",
                    ));
                }
                None if principal == "owner" => {}
                None => {
                    return Err(Error::new(
                        "not_allowed",
                        "global timer Git fetch requires the Owner principal",
                    ));
                }
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

    pub(crate) async fn admit_rule_action(
        &self,
        principal: &str,
        project: Option<&ProjectRegistrationView>,
        state: &RuleStateView,
        revision: i64,
        event_seq: i64,
        prepared: PreparedSubmission,
    ) -> Result<Option<eliot_store::RuleEventAdmission>> {
        if matches!(prepared.request.verb.as_str(), "resume" | "git.fetch")
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
            && request.verb != "git.fetch"
            && !matches!(
                request.verb.as_str(),
                "stop"
                    | "send"
                    | "steer"
                    | "interrupt"
                    | "approve"
                    | "deny"
                    | "answer"
                    | "notify"
                    | "emit"
                    | "mail.send"
                    | "var.set"
                    | "inject"
            )
        {
            return Err(Error::new(
                "invalid",
                "rule firing must contain a prepared stop, send, launch, resume, steer, interrupt, reply, Run, inject, mail.send, var.set, or local event action",
            ));
        }
        let admission = {
            let mut store = self.inner.store.lock().await;
            let retained_resume = if matches!(request.verb.as_str(), "resume" | "git.fetch") {
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
                    "launch" | "approve" | "deny" | "answer" | "git.fetch"
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
}
