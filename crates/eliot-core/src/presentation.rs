//! Owner-facing words. Protocol codes remain unchanged for decisions/readback.

use crate::Error;

pub fn reason_words(code: &str) -> String {
    let words = match code {
        "created" | "starting" => "Starting",
        "idle" => "Ready for a message",
        "busy" => "Swarm is busy; try again shortly",
        "waiting" => "Waiting for your answer",
        "stopped" => "Stopped",
        "accepted" => "Request accepted",
        "queued" => "Waiting to run",
        "dispatched" => "Sent to the Agent",
        "bad_state" => "The Agent is not ready for this action",
        "stale_generation" => "The Agent started a new run; refresh and try again",
        "unsupported" => "This action is not supported",
        "unknown" => "The result could not be confirmed",
        "unavailable" => "This action is unavailable",
        "not_allowed" => "This action is not permitted here",
        "invalid" => "Check the supplied values",
        "not_found" => "The requested item no longer exists",
        "timeout" => "The request took too long",
        "internal" => "Swarm could not complete the request",
        "frame_too_large" => "The reply is too large to display",
        "history_gap" => "Older activity is no longer available",
        "fanout_limit" => "The manager's Agent limit would be exceeded",
        "id_conflict" => "This action's receipt belongs to a different request",
        "daemon_restart" => "Stopped by a Swarm restart",
        "no_progress" => "No progress for a while",
        "source_unhealthy" => "Lost contact with its harness",
        "children_unattended" => "Its subagents are unattended",
        "failure" | "failed" | "agent.failed" => "The Agent could not finish",
        "lost" | "agent.lost" => "Lost contact with the Agent",
        "interrupted" => "Interrupted",
        "refused" | "rejected" => "The request was declined",
        "cancelled" => "Cancelled",
        "crashed" => "The Agent software stopped unexpectedly",
        "success" | "completed" | "applied" => "Completed",
        _ => return code.replace(['_', '.'], " "),
    };
    words.to_owned()
}

pub fn origin_words(origin: &str) -> &'static str {
    match origin {
        "provider" => "Model service",
        "environment" => "This computer",
        "harness" | "harness_contract_mismatch" => "Agent software",
        "swarm" => "Swarm",
        "user" | "owner" => "You",
        _ => "Cause not determined",
    }
}

/// Shared words lead, followed by the daemon's specific explanation intact,
/// including colons or the text `origin:`.
pub fn reason_message(code: &str, message: &str) -> String {
    if message.trim().is_empty() {
        reason_words(code)
    } else {
        format!("{}: {message}", reason_words(code))
    }
}

impl Error {
    pub fn user_message(&self) -> String {
        reason_message(&self.code, &self.message)
    }
}

impl crate::api::DiagnoseStep {
    pub fn user_message(&self) -> String {
        if self.kind == "process" {
            let words = reason_words(self.code.as_deref().unwrap_or("unknown"));
            return match self
                .message
                .as_deref()
                .filter(|reason| !reason.trim().is_empty())
            {
                Some(reason) => format!("{words}: {}", reason_words(reason)),
                None => words,
            };
        }
        if self.kind == "agent" && self.message.is_none() {
            match self.code.as_deref() {
                Some("busy") => return "Working".to_owned(),
                Some("unknown") => return "State not confirmed".to_owned(),
                _ => {}
            }
        }
        reason_message(
            self.code.as_deref().unwrap_or("unknown"),
            self.message.as_deref().unwrap_or_default(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_words_preserve_the_daemons_specific_message() {
        for (code, message) in [
            ("invalid", "reparent would create a cycle"),
            ("not_allowed", "observe-only Agent cannot be reparented"),
            ("bad_state", "new parent is not a live manager"),
            ("unavailable", "artifact ledger is full"),
            ("failure", "origin: request: refused"),
        ] {
            let shown = reason_message(code, message);
            assert!(shown.starts_with(&reason_words(code)), "{shown}");
            assert!(shown.ends_with(message), "{shown}");
        }
        assert_eq!(reason_message("invalid", " \n"), reason_words("invalid"));
        assert!(!reason_message("unavailable", "artifact ledger is full").contains("temporarily"));
    }

    #[test]
    fn unmapped_codes_are_neutral_and_commands_show_their_progress() {
        assert_eq!(reason_words("native_exit"), "native exit");
        assert_eq!(reason_words("agent.custom_state"), "agent custom state");
        assert_eq!(reason_words("accepted"), "Request accepted");
        assert_eq!(reason_words("queued"), "Waiting to run");
        assert_eq!(reason_words("dispatched"), "Sent to the Agent");
    }

    #[test]
    fn process_steps_keep_the_terminal_state_and_explain_the_reason() {
        for (code, words) in [
            ("agent.lost", "Lost contact with the Agent"),
            ("agent.failed", "The Agent could not finish"),
        ] {
            let step: crate::api::DiagnoseStep = serde_json::from_value(serde_json::json!({
                "kind": "process", "code": code, "message": "daemon_restart"
            }))
            .unwrap();
            let shown = step.user_message();
            assert!(shown.starts_with(words), "{shown}");
            assert!(shown.contains("Stopped by a Swarm restart"), "{shown}");
        }
    }
}
