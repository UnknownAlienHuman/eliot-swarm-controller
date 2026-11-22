use super::Router;
use eliot_core::{Command, CommandState, Error, Event, FrozenScriptContext, FrozenScriptRun};
use eliot_plugin::sync::LockExt;
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

const REPEAT_WINDOW: Duration = Duration::from_secs(60);
const REPEAT_THRESHOLD: u32 = 3;
const MAX_TRACKED_FAILURES: usize = 1024;

#[derive(PartialEq, Eq, Hash)]
struct FailureKey {
    principal: String,
    object: String,
    code: String,
    message: String,
}

#[derive(Default)]
pub(super) struct RepeatFailures {
    windows: HashMap<FailureKey, (Instant, u32)>,
}

impl RepeatFailures {
    fn record(&mut self, principal: &str, object: &str, code: &str, message: &str) -> Option<u32> {
        let now = Instant::now();
        let key = FailureKey {
            principal: principal.to_owned(),
            object: object.to_owned(),
            code: code.to_owned(),
            message: message.to_owned(),
        };
        if self.windows.len() >= MAX_TRACKED_FAILURES && !self.windows.contains_key(&key) {
            self.windows
                .retain(|_, (started, _)| now.duration_since(*started) < REPEAT_WINDOW);
            if self.windows.len() >= MAX_TRACKED_FAILURES {
                return None;
            }
        }
        let (started, count) = self.windows.entry(key).or_insert((now, 0));
        if now.duration_since(*started) >= REPEAT_WINDOW {
            *started = now;
            *count = 0;
        }
        *count = count.saturating_add(1);
        (*count == REPEAT_THRESHOLD).then_some(*count)
    }
}

impl Router {
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
        let count = self
            .inner
            .repeat_failures
            .lock_recover()
            .record(principal, rule, code, &message);
        let Some(count) = count else {
            return;
        };
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
