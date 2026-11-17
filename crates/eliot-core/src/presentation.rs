//! Owner-facing words. Protocol codes remain unchanged for decisions/readback.

use crate::Error;

pub fn reason_words(code: &str) -> &'static str {
    match code {
        "bad_state" => "The Agent is not ready for this action",
        "stale_generation" => "The Agent started a new run; refresh and try again",
        "unsupported" => "This action is not supported",
        "unknown" => "The result could not be confirmed",
        "unavailable" => "Swarm is temporarily unavailable",
        "not_allowed" => "This action is not permitted here",
        "invalid" => "Check the supplied values",
        "not_found" => "The requested item no longer exists",
        "busy" => "Swarm is busy; try again shortly",
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

/// Internal admission/transport explanations use the shared words. Native
/// failure prose stays intact, including colons or the text `origin:`.
pub fn reason_message(code: &str, message: &str) -> String {
    if message.trim().is_empty()
        || matches!(
            code,
            "bad_state"
                | "stale_generation"
                | "unsupported"
                | "unknown"
                | "unavailable"
                | "not_allowed"
                | "invalid"
                | "not_found"
                | "busy"
                | "timeout"
                | "internal"
                | "frame_too_large"
                | "history_gap"
                | "fanout_limit"
                | "id_conflict"
                | "daemon_restart"
        )
    {
        reason_words(code).to_owned()
    } else {
        message.to_owned()
    }
}

impl Error {
    pub fn user_message(&self) -> String {
        reason_message(&self.code, &self.message)
    }
}
