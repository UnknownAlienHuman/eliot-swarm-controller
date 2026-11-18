//! Owner-facing words. Protocol codes remain unchanged for decisions/readback.
//! Plain words only: no internal terms (generation, harness, observe-only),
//! and an unknown outcome reads "couldn't be confirmed", never success.

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
        "stale_generation" => "The task restarted since then; refresh and try again",
        "unsupported" => "This action is not supported",
        "unknown" => "The result couldn't be confirmed",
        "unavailable" => "This action is unavailable",
        "not_allowed" => "This action is not permitted here",
        "invalid" => "Check the supplied values",
        "profile_error" => "The settings are not valid",
        "auth_required" => "Sign-in required",
        "not_found" => "The requested item no longer exists",
        "timeout" => "The request took too long",
        "internal" => "Swarm could not complete the request",
        "frame_too_large" => "The reply is too large to display",
        "history_gap" => "Older activity is no longer available",
        "fanout_limit" => "The manager's Agent limit would be exceeded",
        "id_conflict" => "This request was already used for a different action",
        "daemon_restart" => "Interrupted by a Swarm restart",
        "no_progress" => "No progress for a while",
        "source_unhealthy" => "Lost contact with its agent app",
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

/// A count with its noun in the right number: "1 turn", "2 turns" (never
/// "turn(s)").
pub fn count_words(count: impl Into<u64>, one: &str, many: &str) -> String {
    let count = count.into();
    format!("{count} {}", if count == 1 { one } else { many })
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
                Some("unknown") => return "State couldn't be confirmed".to_owned(),
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
            assert!(shown.contains("Interrupted by a Swarm restart"), "{shown}");
        }
    }

    #[test]
    fn owner_words_carry_no_internal_terms_and_unknown_is_never_success() {
        for code in [
            "stale_generation",
            "source_unhealthy",
            "daemon_restart",
            "id_conflict",
            "unknown",
        ] {
            let words = reason_words(code).to_lowercase();
            for term in ["generation", "harness", "observe", "receipt", "daemon"] {
                assert!(!words.contains(term), "{code}: {words}");
            }
        }
        assert_eq!(reason_words("unknown"), "The result couldn't be confirmed");
        let step: crate::api::DiagnoseStep =
            serde_json::from_value(serde_json::json!({"kind": "agent", "code": "unknown"}))
                .unwrap();
        assert_eq!(step.user_message(), "State couldn't be confirmed");
        assert_eq!(count_words(1_u32, "turn", "turns"), "1 turn");
        assert_eq!(count_words(0_u32, "turn", "turns"), "0 turns");
        assert_eq!(count_words(12_u64, "item", "items"), "12 items");
    }
}
