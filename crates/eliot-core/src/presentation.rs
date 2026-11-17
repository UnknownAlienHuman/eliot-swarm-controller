//! Owner-facing words. Protocol codes remain unchanged for decisions/readback.

use crate::Error;

pub fn reason_words(code: &str) -> &'static str {
    match code {
        "created" | "starting" => "Starting",
        "idle" => "Ready for a message",
        "busy" => "Swarm is busy; try again shortly",
        "waiting" => "Waiting for your answer",
        "stopped" => "Stopped",
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
        "failure" | "failed" => "The Agent could not finish",
        "lost" => "Lost contact with the Agent",
        "interrupted" => "Interrupted",
        "refused" | "rejected" => "The request was declined",
        "cancelled" => "Cancelled",
        "crashed" => "The Agent software stopped unexpectedly",
        "success" | "completed" | "applied" => "Completed",
        _ => "Could not complete the request",
    }
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
        reason_words(code).to_owned()
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
            assert!(shown.starts_with(reason_words(code)), "{shown}");
            assert!(shown.ends_with(message), "{shown}");
        }
        assert_eq!(reason_message("invalid", " \n"), reason_words("invalid"));
        assert!(!reason_message("unavailable", "artifact ledger is full").contains("temporarily"));
    }
}
