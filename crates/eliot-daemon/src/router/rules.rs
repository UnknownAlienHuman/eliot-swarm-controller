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
    pub(super) async fn rule_catch_up(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
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

    pub(super) async fn timer_run(&self, params: &Value, principal: &Principal) -> Result<Value> {
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
            )
        {
            return Err(Error::new(
                "invalid",
                "rule firing must contain a prepared stop, send, launch, resume, steer, interrupt, reply, Run, mail.send, var.set, or local event action",
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

#[cfg(test)]
mod tests {
    use super::super::auth::authorize_command;
    use super::super::tests::{
        read_fixture_event, result_fixture, router, router_with_data_root, stopped_agent,
    };
    use super::super::{PreparedSubmission, Principal};
    use crate::common::now_ms;
    use eliot_core::{
        Command, CommandState, Error, SecretPolicy, SubmitCommand,
        api::{TimerRunRequest, TimerStateView},
    };
    use eliot_store::{ProjectRegistrationInput, TimerDefinitionInput};
    use serde_json::{Value, json};

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
                        selector: None,
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
            crate::test_support::install_live_owned_process_source(&mut store, &agent, 1, now_ms());
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
            crate::test_support::simulate_owned_process_exit_proof(&mut store, &agent, 1, now_ms());
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
                        selector: None,
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
                        git: None,
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
            git: None,
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
    async fn manual_timer_run_replays_parent_after_initial_miss_and_timer_disable() {
        const RULE_ID: &str = "manual-resume-replay";
        let router = router();
        let now = now_ms();
        let agent = {
            let mut store = router.inner.store.lock().await;
            stopped_agent(&mut store, "manual-timer-resume-replay")
        };
        let rule_value = json!({
            "id":RULE_ID,
            "enabled":true,
            "on":{"every":"1h"},
            "do":[{"action":"resume", "target":agent.clone(), "expected_generation":1,
                "text":"new timer input"}],
        });
        let rule_input = eliot_store::RuleDefinitionInput {
            id: RULE_ID.to_owned(),
            canonical_definition: serde_json::to_string(&rule_value).unwrap(),
            definition_digest: eliot_core::canonical_json_sha256(&rule_value).unwrap(),
            enabled: true,
        };
        let period_ms = 60 * 60 * 1_000;
        let timer_input = TimerDefinitionInput {
            id: RULE_ID.to_owned(),
            schedule_revision: 1,
            schedule: eliot_core::api::TimerSchedule::Interval {
                period_ms,
                anchor: Some(now),
            },
            enabled: true,
            initial_next_due_at: Some(now + period_ms),
        };
        let rule_revision = router
            .initialize_rules(
                "owner",
                std::slice::from_ref(&rule_input),
                std::slice::from_ref(&timer_input),
                now,
            )
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
            .view
            .revision;
        let timer_state = router
            .get_rule_timer("owner", RULE_ID)
            .await
            .unwrap()
            .unwrap();
        let request = TimerRunRequest::new(ulid::Ulid::new().to_string(), RULE_ID);
        assert!(
            router
                .replay_timer_run("owner", &request)
                .await
                .unwrap()
                .is_none()
        );
        let mut action = SubmitCommand::new("", "resume");
        action.target = Some(agent.clone());
        action.expected_generation = Some(1);
        action.args = json!({"text":"new timer input"});
        let parent = router
            .inner
            .store
            .lock()
            .await
            .admit_manual_timer_run("owner", &request, &timer_state, rule_revision, action, now)
            .unwrap()
            .parent
            .unwrap();
        let disabled_value = json!({
            "id":RULE_ID,
            "enabled":false,
            "on":{"every":"1h"},
            "do":[{"action":"resume", "target":agent, "expected_generation":1,
                "text":"new timer input"}],
        });
        let disabled_rule_input = eliot_store::RuleDefinitionInput {
            id: RULE_ID.to_owned(),
            canonical_definition: serde_json::to_string(&disabled_value).unwrap(),
            definition_digest: eliot_core::canonical_json_sha256(&disabled_value).unwrap(),
            enabled: false,
        };
        let disabled_timer_input = TimerDefinitionInput {
            id: RULE_ID.to_owned(),
            schedule_revision: 1,
            schedule: eliot_core::api::TimerSchedule::Interval {
                period_ms,
                anchor: Some(now),
            },
            enabled: false,
            initial_next_due_at: Some(now + period_ms),
        };
        router
            .initialize_rules(
                "owner",
                std::slice::from_ref(&disabled_rule_input),
                std::slice::from_ref(&disabled_timer_input),
                now + 1,
            )
            .await
            .unwrap();
        let disabled_timer = router
            .get_rule_timer("owner", RULE_ID)
            .await
            .unwrap()
            .unwrap();
        assert!(!disabled_timer.enabled);
        let high_water = router.inner.store.lock().await.high_water().unwrap();

        // The duplicate missed its first lookup; the winner committed a parent
        // before the current disabled-timer gate was rechecked.
        let replay = router
            .timer_run_error_after_replay(
                "owner",
                &request,
                Error::new("bad_state", "timer is disabled or paused"),
            )
            .await
            .unwrap();

        assert_eq!(replay, serde_json::to_value(parent).unwrap());
        let conflicting_request = TimerRunRequest::new(request.id.clone(), "different-rule");
        assert_eq!(
            router
                .timer_run_error_after_replay(
                    "owner",
                    &conflicting_request,
                    Error::new("bad_state", "timer is disabled or paused"),
                )
                .await
                .unwrap_err()
                .code,
            "id_conflict"
        );
        assert_eq!(
            router.inner.store.lock().await.high_water().unwrap(),
            high_water
        );
    }

    #[tokio::test]
    async fn manual_timer_resume_rechecks_current_project_registration() {
        const PROJECT_ID: &str = "timer-resume-authority";
        let router = router();
        let checkout = std::env::temp_dir()
            .canonicalize()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        let input = ProjectRegistrationInput {
            id: PROJECT_ID.to_owned(),
            checkout: checkout.clone(),
            repo_common_dir: checkout,
            git: None,
            profiles: vec!["fake".to_owned()],
            actions: vec!["resume".to_owned(), "send".to_owned()],
            interpreters: Default::default(),
            run_env: Vec::new(),
        };
        let registration = router
            .reconcile_project_registrations(std::slice::from_ref(&input))
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let principal = format!("project:{PROJECT_ID}");
        let request = TimerRunRequest::new(ulid::Ulid::new().to_string(), "project-timer")
            .in_project(PROJECT_ID);
        let timer_state = TimerStateView {
            id: "project-timer".to_owned(),
            project: Some(PROJECT_ID.to_owned()),
            schedule_revision: 1,
            schedule: eliot_core::api::TimerSchedule::Interval {
                period_ms: 60_000,
                anchor: None,
            },
            anchor: 0,
            last_key: None,
            next_due_at: Some(60_000),
            enabled: true,
        };
        let mut action = SubmitCommand::new("", "resume");
        action.target = Some("a1".to_owned());
        action.expected_generation = Some(1);
        action.args = json!({"text":"new timer input"});
        let prepared = PreparedSubmission {
            request: action,
            workspace: None,
        };

        let mut stale_registration = registration;
        stale_registration.revision += 1;
        let error = router
            .admit_manual_timer_run(
                &principal,
                Some(&stale_registration),
                &request,
                &timer_state,
                1,
                prepared,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, "not_allowed");
        assert_eq!(
            error.message,
            "project registration changed before timer admission"
        );
    }
}
