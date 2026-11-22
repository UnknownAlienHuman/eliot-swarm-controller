use super::{Principal, Router, principal_name};
use eliot_core::{
    Command, CommandState, Error, Event, FrozenScriptContext, FrozenScriptRun, RemoteCallContext,
    Result,
};
use eliot_plugin::sync::LockExt;
use regex::Regex;
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    sync::LazyLock,
    time::{Duration, Instant},
};

const REPEAT_WINDOW: Duration = Duration::from_secs(60);
const REPEAT_THRESHOLD: u32 = 3;
const STORM_WINDOW_MS: u64 = 10_000;
const STORM_THRESHOLD: u64 = 32;
const MAX_TRACKED_WINDOWS: usize = 1024;
const MAX_STORM_CONNECTION_IDS: usize = 64;

static VOLATILE_MESSAGE_VALUES: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"\b(?:[[:xdigit:]]{8}(?:-[[:xdigit:]]{4}){3}-[[:xdigit:]]{12}|[0-7][0-9A-HJKMNP-TV-Z]{25}|a[0-9]+|[0-9]+)\b",
    )
    .expect("valid pattern for UUIDs, ULIDs, Agent ids and numbers")
});

#[derive(PartialEq, Eq, Hash)]
struct FailureKey {
    principal: String,
    object: String,
    code: String,
    message: String,
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct StormKey {
    principal: String,
    method: &'static str,
}

#[derive(PartialEq, Eq, Hash)]
enum WindowKey {
    Failure(FailureKey),
    Storm(StormKey),
}

struct CounterWindow {
    ends: Instant,
    count: u64,
    refused: u64,
    storm: StormDiagnostics,
}

#[derive(Default)]
struct StormDiagnostics {
    connection_ids: HashSet<String>,
    connection_count: u64,
    connections_overflow: bool,
    source: Option<String>,
}

impl StormDiagnostics {
    fn observe_connection(&mut self, connection_id: &str) {
        let connection_id = eliot_core::bound_chars(connection_id, 256);
        if self.connection_ids.contains(&connection_id) {
            return;
        }
        if self.connection_ids.len() == MAX_STORM_CONNECTION_IDS {
            self.connection_count = MAX_STORM_CONNECTION_IDS as u64 + 1;
            self.connections_overflow = true;
            return;
        }
        self.connection_ids.insert(connection_id);
        self.connection_count = self.connection_ids.len() as u64;
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
    overflow_logged_at: Option<Instant>,
}

enum RepeatOutcome {
    Quiet,
    Threshold,
    Overflow,
}

impl RepeatFailures {
    fn record(
        &mut self,
        principal: &str,
        object: &str,
        code: &str,
        message: &str,
    ) -> RepeatOutcome {
        let key = WindowKey::Failure(FailureKey {
            principal: principal.to_owned(),
            object: object.to_owned(),
            code: code.to_owned(),
            message: normalized_message(message),
        });
        let now = Instant::now();
        match self.increment(key, now, 1, 0) {
            Some((count, _)) if count == u64::from(REPEAT_THRESHOLD) => RepeatOutcome::Threshold,
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
    ) -> Option<(u64, Option<(WindowKey, CounterWindow)>)> {
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
            WindowKey::Failure(_) => REPEAT_WINDOW,
            WindowKey::Storm(_) => Duration::from_millis(STORM_WINDOW_MS),
        };
        let window = self.windows.entry(key).or_insert(CounterWindow {
            ends: now + duration,
            count: 0,
            refused: 0,
            storm: StormDiagnostics::default(),
        });
        window.count = window.count.saturating_add(count);
        window.refused = window.refused.saturating_add(refused);
        Some((window.count, closed))
    }
}

fn normalized_message(message: &str) -> String {
    let message = message.split_whitespace().collect::<Vec<_>>().join(" ");
    VOLATILE_MESSAGE_VALUES
        .replace_all(&message, "<value>")
        .into_owned()
}

impl Router {
    pub(super) fn note_command_submission(
        &self,
        principal: &Principal,
        remote: Option<&RemoteCallContext>,
        method: &'static str,
        result: &Result<Value>,
    ) {
        let source = StormKey {
            principal: principal_name(principal),
            method,
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
        let sample = {
            let mut counter = self.inner.repeat_failures.lock_recover();
            let key = WindowKey::Storm(source.clone());
            let sample = counter.increment(key.clone(), Instant::now(), count, refused);
            if sample.is_some()
                && let Some(window) = counter.windows.get_mut(&key)
            {
                if let Some(connection_id) = connection_id {
                    window.storm.observe_connection(connection_id);
                }
                if let Some(rule_id) = rule_id {
                    window.storm.observe_rule(rule_id);
                }
            }
            sample
        };
        if let Some((_, Some((WindowKey::Storm(source), window)))) = sample {
            self.log_command_storm(source, window);
        }
    }

    pub(crate) fn flush_command_storms(&self, shutdown: bool) {
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
            crate::logging::record_command_storm(
                &self.secret_policy_snapshot(),
                &ids,
                window.count,
                window.refused,
                STORM_WINDOW_MS,
                crate::logging::CommandStormMetadata {
                    connection_count: window.storm.connection_count,
                    connections_overflow: window.storm.connections_overflow,
                },
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
                self.note_repeated_failure(principal, rule, code, message);
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
        self.note_repeated_failure(&command.principal, &rule.id, cause, &message);
    }

    fn note_repeated_failure(&self, principal: &str, rule: &str, code: &str, message: &str) {
        let policy = self.secret_policy_snapshot();
        let message = eliot_core::bound_chars(&policy.redact_text(message), 512);
        let outcome = self
            .inner
            .repeat_failures
            .lock_recover()
            .record(principal, rule, code, &message);
        match outcome {
            RepeatOutcome::Quiet => return,
            RepeatOutcome::Overflow => {
                crate::logging::record(&policy, "daemon.rule_failure_counter_overflow", &[], None);
                return;
            }
            RepeatOutcome::Threshold => {}
        }
        let count = REPEAT_THRESHOLD;
        crate::logging::record_repeated_rule_failure(
            &policy, principal, rule, code, &message, count,
        );
        let router = self.clone();
        let text = format!(
            "Rule {rule} ({principal}) failed {count} times in one minute ({code}). {message}"
        );
        tokio::spawn(async move {
            if let Err(error) = router.report_daemon_attention(text).await {
                crate::logging::record(
                    &router.secret_policy_snapshot(),
                    "daemon.attention_delivery_failed",
                    &[],
                    Some(&Error::new(
                        error.code,
                        "rule failure notice could not be delivered",
                    )),
                );
            }
        });
    }
}
