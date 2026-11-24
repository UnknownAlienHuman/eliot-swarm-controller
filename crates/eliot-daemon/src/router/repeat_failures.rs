use super::{Principal, Router, principal_name};
use eliot_core::{
    Command, CommandState, Error, Event, FrozenScriptContext, FrozenScriptRun, RemoteCallContext,
    Result,
    api::{GoalMode, GoalView, inference::sha256_hex},
};
use eliot_plugin::sync::LockExt;
use eliot_store::{RepeatAlertGoal, RepeatAlertUpdate};
use regex::Regex;
use serde::Serialize;
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    sync::LazyLock,
    time::{Duration, Instant},
};

const REPEAT_WINDOW_MS: u64 = 60_000;
const REPEAT_WINDOW: Duration = Duration::from_millis(REPEAT_WINDOW_MS);
const REPEAT_THRESHOLD: u32 = 3;
const STORM_WINDOW_MS: u64 = 10_000;
const STORM_THRESHOLD: u64 = 32;
const MAX_TRACKED_WINDOWS: usize = eliot_store::MAX_REPEAT_FAILURE_ALERTS;
const MAX_STORM_CONNECTION_IDS: usize = 64;

static VOLATILE_MESSAGE_VALUES: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"\b(?:[[:xdigit:]]{8}(?:-[[:xdigit:]]{4}){3}-[[:xdigit:]]{12}|[0-7][0-9A-HJKMNP-TV-Z]{25}|a[0-9]+|[0-9]+)\b",
    )
    .expect("valid pattern for UUIDs, ULIDs, Agent ids and numbers")
});

#[derive(Clone, PartialEq, Eq, Hash, Serialize)]
enum FailureOwner {
    Rule {
        principal: String,
        rule: String,
    },
    Goal {
        goal_id: String,
        agent: String,
        generation: i64,
        mode: GoalMode,
    },
}

#[derive(Clone, PartialEq, Eq, Hash, Serialize)]
struct FailureKey {
    owner: FailureOwner,
    failure_class: String,
    code: String,
    message: String,
    evidence: Option<String>,
}

#[derive(Clone, PartialEq, Eq, Hash, Serialize)]
struct StormKey {
    principal: String,
    method: &'static str,
    rpc_failure: Option<RpcFailure>,
}

#[derive(Clone, PartialEq, Eq, Hash, Serialize)]
struct RpcFailure {
    code: String,
    operation: String,
    field: Option<String>,
    message: String,
}

#[derive(Clone, PartialEq, Eq, Hash, Serialize)]
enum WindowKey {
    Failure(FailureKey),
    CommandRejection { agent: String, verb: String },
    Storm(StormKey),
}

struct CounterWindow {
    ends: Instant,
    count: u64,
    refused: u64,
    last_refusal_at: i64,
    storm: StormDiagnostics,
}

struct FailureObservation<'a> {
    class: &'a str,
    code: &'a str,
    message: &'a str,
    evidence: Option<&'a str>,
    goal: Option<RepeatAlertGoal>,
}

struct PendingAlert {
    first_observed_at: i64,
    last_observed_at: i64,
    goal: Option<RepeatAlertGoal>,
    message: Option<String>,
}

impl WindowKey {
    fn quiet_ms(&self) -> i64 {
        match self {
            // The total is reported at window closure. Keep it through the
            // following quiet window so even a burst at the start is visible.
            Self::Storm(_) => 2 * STORM_WINDOW_MS as i64,
            Self::Failure(_) | Self::CommandRejection { .. } => REPEAT_WINDOW_MS as i64,
        }
    }

    fn matches_goal(&self, view: &GoalView) -> bool {
        matches!(self, Self::Failure(FailureKey {
            owner: FailureOwner::Goal { agent, generation, mode, .. }, ..
        }) if agent == &view.agent && *generation == view.generation && *mode == view.mode)
    }
}

#[derive(Default)]
struct StormDiagnostics {
    connection_ids: HashSet<String>,
    connections_overflow: bool,
    source: Option<String>,
}

impl StormDiagnostics {
    fn observe_connection(&mut self, connection_id: &str) {
        if self.connections_overflow || self.connection_ids.contains(connection_id) {
            return;
        }
        if self.connection_ids.len() == MAX_STORM_CONNECTION_IDS {
            self.connections_overflow = true;
        } else {
            self.connection_ids.insert(connection_id.to_owned());
        }
    }

    fn observe_rule(&mut self, rule_id: &str) {
        if self.source.is_none() {
            self.source = Some(eliot_core::bound_chars(rule_id, 256));
        }
    }
}

#[derive(Default)]
pub(super) struct RepeatFailures {
    windows: HashMap<WindowKey, CounterWindow>,
    pending_alerts: HashMap<WindowKey, PendingAlert>,
    overflow_logged_at: Option<Instant>,
}

enum RepeatOutcome {
    Quiet,
    Threshold,
    Overflow,
}

impl RepeatFailures {
    fn queue_alert(
        &mut self,
        key: WindowKey,
        observed_at: i64,
        goal: Option<RepeatAlertGoal>,
        message: Option<String>,
    ) {
        if self.pending_alerts.len() >= MAX_TRACKED_WINDOWS
            && !self.pending_alerts.contains_key(&key)
        {
            return;
        }
        let quiet_ms = key.quiet_ms();
        let pending = self.pending_alerts.entry(key).or_insert(PendingAlert {
            first_observed_at: observed_at,
            last_observed_at: observed_at,
            goal,
            message: None,
        });
        if observed_at >= pending.last_observed_at.saturating_add(quiet_ms) {
            pending.first_observed_at = observed_at;
            pending.message = None;
        }
        pending.first_observed_at = pending.first_observed_at.min(observed_at);
        pending.last_observed_at = pending.last_observed_at.max(observed_at);
        if goal.is_some_and(|goal| pending.goal.is_none_or(|old| goal.revision > old.revision)) {
            pending.goal = goal;
        }
        if message.is_some() {
            pending.message = message;
        }
    }

    fn record(&mut self, key: WindowKey) -> RepeatOutcome {
        let now = Instant::now();
        match self
            .increment(key, now, 1, 0)
            .map(|(window, _)| window.count)
        {
            Some(count) if count == u64::from(REPEAT_THRESHOLD) => RepeatOutcome::Threshold,
            None if self
                .overflow_logged_at
                .is_none_or(|logged| now.duration_since(logged) >= REPEAT_WINDOW) =>
            {
                self.overflow_logged_at = Some(now);
                RepeatOutcome::Overflow
            }
            _ => RepeatOutcome::Quiet,
        }
    }

    fn increment(
        &mut self,
        key: WindowKey,
        now: Instant,
        count: u64,
        refused: u64,
    ) -> Option<(&mut CounterWindow, Option<(WindowKey, CounterWindow)>)> {
        if self.windows.len() >= MAX_TRACKED_WINDOWS && !self.windows.contains_key(&key) {
            // Storm windows must survive until their final counts are logged.
            self.windows
                .retain(|key, window| matches!(key, WindowKey::Storm(_)) || now < window.ends);
            if self.windows.len() >= MAX_TRACKED_WINDOWS {
                return None;
            }
        }
        let closed = if self
            .windows
            .get(&key)
            .is_some_and(|window| now >= window.ends)
        {
            self.windows.remove_entry(&key)
        } else {
            None
        };
        let duration = match key {
            WindowKey::Failure(_) | WindowKey::CommandRejection { .. } => REPEAT_WINDOW,
            WindowKey::Storm(_) => Duration::from_millis(STORM_WINDOW_MS),
        };
        let window = self.windows.entry(key).or_insert(CounterWindow {
            ends: now + duration,
            count: 0,
            refused: 0,
            last_refusal_at: 0,
            storm: StormDiagnostics::default(),
        });
        window.count = window.count.saturating_add(count);
        window.refused = window.refused.saturating_add(refused);
        Some((window, closed))
    }
}

fn normalized_message(message: &str) -> String {
    let message = message.split_whitespace().collect::<Vec<_>>().join(" ");
    VOLATILE_MESSAGE_VALUES
        .replace_all(&message, "<value>")
        .into_owned()
}

impl Router {
    /// Count currently live failure, command-rejection, and command-storm windows.
    pub(super) fn active_repeat_windows(&self) -> usize {
        let now = Instant::now();
        self.inner
            .repeat_failures
            .lock_recover()
            .windows
            .iter()
            .filter(|(key, window)| {
                now < window.ends && (!matches!(key, WindowKey::Storm(_)) || window.refused > 0)
            })
            .count()
    }

    /// Record a failed Goal check in the shared repeat counter. A matching
    /// failure is bound to this Goal, Agent generation, failure class, code,
    /// normalized diagnostic text and any exact check-output digest.
    pub(super) fn note_goal_failure(
        &self,
        principal: &str,
        goal: &GoalView,
        failure_class: &str,
        code: &str,
        message: &str,
        evidence: Option<&str>,
    ) {
        // A Goal's revision advances after each round; its definition is
        // stable across those rounds and separates replacement definitions.
        let Ok(definition) = serde_json::to_vec(&goal.definition) else {
            return;
        };
        let Ok(agent) = eliot_core::parse_agent_id(&goal.agent) else {
            return;
        };
        self.note_repeated_failure(
            FailureOwner::Goal {
                goal_id: sha256_hex(&definition),
                agent: goal.agent.clone(),
                generation: goal.generation,
                mode: goal.mode,
            },
            principal,
            FailureObservation {
                class: failure_class,
                code,
                message,
                evidence,
                goal: Some(RepeatAlertGoal {
                    agent,
                    generation: goal.generation,
                    mode: goal.mode,
                    revision: goal.revision,
                }),
            },
        );
    }

    pub(super) fn reset_goal_repeat_failures(&self, goal: &GoalView) {
        let mut counter = self.inner.repeat_failures.lock_recover();
        counter.windows.retain(|key, _| !key.matches_goal(goal));
        counter
            .pending_alerts
            .retain(|key, _| !key.matches_goal(goal));
    }

    pub(super) fn note_command_rejection(&self, command: &Command, events: &[Event]) {
        let Some(agent) = command.target.as_deref().filter(|_| {
            command.state == CommandState::Rejected
                && events.iter().any(|event| {
                    event.kind == "command.rejected"
                        && event.data["command_id"].as_str() == Some(command.id.as_str())
                })
        }) else {
            return;
        };
        let verb = command.verb.as_str();
        let key = WindowKey::CommandRejection {
            agent: agent.to_owned(),
            verb: verb.to_owned(),
        };
        let observed_at = super::now_ms();
        let reached_threshold = {
            let mut counter = self.inner.repeat_failures.lock_recover();
            let reached = counter
                .increment(key.clone(), Instant::now(), 1, 0)
                .is_some_and(|(window, _)| window.count == u64::from(REPEAT_THRESHOLD));
            counter.queue_alert(key, observed_at, None, reached.then(|| format!(
                "Agent {agent} had {REPEAT_THRESHOLD} rejected {verb} commands in one minute."
            )));
            reached
        };
        if reached_threshold {
            crate::logging::record(
                &self.secret_policy_snapshot(),
                "daemon.repeated_command_rejected",
                &[("agent", agent), ("command", verb)],
                None,
            );
        }
    }

    pub(super) fn note_rpc_failure(
        &self,
        principal: &str,
        method: &'static str,
        remote: Option<&RemoteCallContext>,
        error: &Error,
    ) {
        let policy = self.secret_policy_snapshot();
        let bounded =
            |value: &str, limit| eliot_core::bound_chars(&policy.redact_text(value), limit);
        let source = StormKey {
            principal: principal.to_owned(),
            method,
            rpc_failure: Some(RpcFailure {
                code: bounded(&error.code, 64),
                operation: bounded(error.failure_where.as_deref().unwrap_or(method), 128),
                field: error
                    .details
                    .as_ref()
                    .map(|details| bounded(&details.field, 128)),
                message: normalized_message(&bounded(&error.message, 512)),
            }),
        };
        self.note_command_activity(
            source,
            1,
            1,
            remote.map(|call| call.connection_id.as_str()),
            None,
        );
    }

    pub(super) fn note_command_submission(
        &self,
        principal: &Principal,
        remote: Option<&RemoteCallContext>,
        method: &'static str,
        result: &Result<Value>,
    ) {
        if method == "daemon.attention" {
            return;
        }
        let source = StormKey {
            principal: principal_name(principal),
            method,
            rpc_failure: None,
        };
        let refused = result
            .as_ref()
            .map_or(true, |reply| reply["state"] == "rejected");
        self.note_command_activity(
            source,
            1,
            u64::from(refused),
            remote.map(|call| call.connection_id.as_str()),
            None,
        );
    }

    pub(super) fn note_rule_command_admission(&self, admission: &eliot_store::Admission) {
        if !admission.is_new {
            return;
        }
        let source = StormKey {
            principal: admission.command.principal.clone(),
            method: "daemon.rule_action",
            rpc_failure: None,
        };
        let attempts = 1 + u64::from(admission.linked_command.is_some());
        let refused = u64::from(admission.command.state == CommandState::Rejected)
            + admission.linked_command.as_ref().map_or(0, |command| {
                u64::from(command.state == CommandState::Rejected)
            });
        let rule_id = admission
            .events
            .iter()
            .find_map(|event| event.cause.as_ref()?.rule.as_deref());
        self.note_command_activity(source, attempts, refused, None, rule_id);
    }

    fn note_command_activity(
        &self,
        source: StormKey,
        count: u64,
        refused: u64,
        connection_id: Option<&str>,
        rule_id: Option<&str>,
    ) {
        let closed = {
            let mut counter = self.inner.repeat_failures.lock_recover();
            let key = WindowKey::Storm(source);
            let Some((window, closed)) =
                counter.increment(key.clone(), Instant::now(), count, refused)
            else {
                return;
            };
            if let Some(connection_id) = connection_id {
                window.storm.observe_connection(connection_id);
            }
            if let Some(rule_id) = rule_id {
                window.storm.observe_rule(rule_id);
            }
            if refused > 0 {
                let observed_at = super::now_ms();
                window.last_refusal_at = observed_at;
                counter.queue_alert(key, observed_at, None, None);
            }
            closed
        };
        if let Some((WindowKey::Storm(source), window)) = closed {
            self.log_command_storm(source, window);
        }
    }

    pub(crate) async fn flush_repeat_failures(&self, shutdown: bool) {
        let closed: Vec<_> = {
            let mut counter = self.inner.repeat_failures.lock_recover();
            let now = Instant::now();
            counter
                .windows
                .extract_if(|_, window| shutdown || window.ends <= now)
                .filter_map(|(key, window)| match key {
                    WindowKey::Storm(source) if window.refused >= STORM_THRESHOLD => {
                        Some((source, window))
                    }
                    _ => None,
                })
                .collect()
        };
        for (source, window) in closed {
            self.log_command_storm(source, window);
        }
        let pending = std::mem::take(&mut self.inner.repeat_failures.lock_recover().pending_alerts);
        if pending.is_empty() {
            return;
        }
        let policy = self.secret_policy_snapshot();
        let updates: Vec<_> = pending
            .into_iter()
            .filter_map(|(key, pending)| {
                let encoded = serde_json::to_vec(&key).ok()?;
                Some(RepeatAlertUpdate {
                    cause: sha256_hex(&encoded),
                    observed_at: pending.first_observed_at,
                    expires_at: pending.last_observed_at.saturating_add(key.quiet_ms()),
                    goal: pending.goal,
                    message: pending
                        .message
                        .map(|text| eliot_core::bound_chars(&policy.redact_text(&text), 1024)),
                })
            })
            .collect();
        let mut store = self.inner.store.lock().await;
        match store.update_repeat_alerts(&updates, super::now_ms()) {
            Ok(events) => self.publish_locked(&events),
            Err(error) => crate::logging::record(
                &policy,
                "daemon.repeat_alert_commit_failed",
                &[],
                Some(&error),
            ),
        }
    }

    fn log_command_storm(&self, source: StormKey, window: CounterWindow) {
        if window.refused >= STORM_THRESHOLD {
            let mut ids = vec![
                ("principal", source.principal.as_str()),
                ("method", source.method),
            ];
            if let Some(rule_id) = window.storm.source.as_deref() {
                ids.push(("source", rule_id));
            }
            let error = source.rpc_failure.as_ref().map(|failure| {
                Error::new(&failure.code, &failure.message).with_where(&failure.operation)
            });
            crate::logging::record_command_storm(
                &self.secret_policy_snapshot(),
                &ids,
                window.count,
                window.refused,
                STORM_WINDOW_MS,
                crate::logging::CommandStormMetadata {
                    connection_count: window.storm.connection_ids.len() as u64
                        + u64::from(window.storm.connections_overflow),
                    connections_overflow: window.storm.connections_overflow,
                },
                error.as_ref(),
            );
            let message = match &error {
                Some(error) => format!(
                    "RPC refusal storm from {} on {}: {} failures in {} ms ({}). {}",
                    source.principal,
                    source.method,
                    window.refused,
                    STORM_WINDOW_MS,
                    error.code,
                    error.message
                ),
                None => format!(
                    "Command rejection storm from {} on {}: {} of {} submissions were refused in one {} ms window.",
                    source.principal, source.method, window.refused, window.count, STORM_WINDOW_MS
                ),
            };
            self.inner.repeat_failures.lock_recover().queue_alert(
                WindowKey::Storm(source),
                window.last_refusal_at,
                None,
                Some(message),
            );
        }
    }

    pub(super) fn note_rule_refusals(&self, principal: &str, events: &[Event]) {
        for event in events.iter().filter(|event| event.kind == "rule.rejected") {
            if let (Some(rule), Some(code), Some(message)) = (
                event.data["rule"].as_str(),
                event.data["code"].as_str(),
                event.data["message"].as_str(),
            ) {
                self.note_repeated_failure(
                    FailureOwner::Rule {
                        principal: principal.to_owned(),
                        rule: rule.to_owned(),
                    },
                    principal,
                    FailureObservation {
                        class: "rule",
                        code,
                        message,
                        evidence: None,
                        goal: None,
                    },
                );
            }
        }
    }

    pub(super) fn note_script_failure(&self, command: &Command) {
        if command.verb != "run" || command.state != CommandState::Failed {
            return;
        }
        let Some(outcome) = command.outcome.as_ref() else {
            return;
        };
        let Some(cause) = outcome["cause"].as_str() else {
            return;
        };
        let Ok(frozen) = serde_json::from_value::<FrozenScriptRun>(command.args.clone()) else {
            return;
        };
        let rule = match &frozen.context {
            FrozenScriptContext::Event { rule, .. } | FrozenScriptContext::Timer { rule, .. } => {
                rule
            }
        };
        let message = outcome["process"]["direct_exit"]["exit_code"]
            .as_i64()
            .map(|code| format!("Run action exited with code {code}"))
            .unwrap_or_else(|| "Run action failed".to_owned());
        self.note_repeated_failure(
            FailureOwner::Rule {
                principal: command.principal.clone(),
                rule: rule.id.clone(),
            },
            &command.principal,
            FailureObservation {
                class: "rule",
                code: cause,
                message: &message,
                evidence: None,
                goal: None,
            },
        );
    }

    fn note_repeated_failure(
        &self,
        owner: FailureOwner,
        principal: &str,
        failure: FailureObservation<'_>,
    ) {
        let policy = self.secret_policy_snapshot();
        let failure_class = eliot_core::bound_chars(&policy.redact_text(failure.class), 64);
        let code = eliot_core::bound_chars(&policy.redact_text(failure.code), 64);
        let message = eliot_core::bound_chars(&policy.redact_text(failure.message), 512);
        let key = WindowKey::Failure(FailureKey {
            owner: owner.clone(),
            failure_class: failure_class.clone(),
            code: code.clone(),
            message: normalized_message(&message),
            evidence: failure.evidence.map(str::to_owned),
        });
        let observed_at = super::now_ms();
        let outcome = {
            let mut counter = self.inner.repeat_failures.lock_recover();
            let outcome = counter.record(key.clone());
            counter.queue_alert(key.clone(), observed_at, failure.goal, None);
            outcome
        };
        match outcome {
            RepeatOutcome::Quiet => return,
            RepeatOutcome::Overflow => {
                let event = match owner {
                    FailureOwner::Rule { .. } => "daemon.rule_failure_counter_overflow",
                    FailureOwner::Goal { .. } => "daemon.goal_failure_counter_overflow",
                };
                crate::logging::record(&policy, event, &[], None);
                return;
            }
            RepeatOutcome::Threshold => {}
        }
        let count = REPEAT_THRESHOLD;
        let text = match owner {
            FailureOwner::Rule { principal, rule } => {
                crate::logging::record_repeated_rule_failure(
                    &policy, &principal, &rule, &code, &message, count,
                );
                format!(
                    "Rule {rule} ({principal}) failed {count} times in one minute ({code}). {message}"
                )
            }
            FailureOwner::Goal {
                goal_id,
                agent,
                generation,
                ..
            } => {
                crate::logging::record_repeated_goal_failure(
                    &policy,
                    crate::logging::RepeatedGoalFailure {
                        goal_id: &goal_id,
                        principal,
                        agent: &agent,
                        generation,
                        failure_class: &failure_class,
                        code: &code,
                        message: &message,
                    },
                    count,
                );
                format!(
                    "Goal for Agent {agent}#{generation} failed {count} times in one minute ({failure_class}: {code}). {message}"
                )
            }
        };
        self.inner.repeat_failures.lock_recover().queue_alert(
            key,
            observed_at,
            failure.goal,
            Some(text),
        );
    }
}
