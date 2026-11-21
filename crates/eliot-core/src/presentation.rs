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
        "capacity_unavailable" => "Launch capacity is full",
        "transfer_in_progress" => "A project transfer is in progress",
        "id_conflict" => "This request was already used for a different action",
        "daemon_restart" => "Interrupted by a Swarm restart",
        "root_exited" => "The agent app exited unexpectedly",
        "no_progress" => "No progress for a while",
        "no_code_progress" => "Working without writing code",
        "source_unhealthy" => "Lost contact with its agent app",
        "children_unattended" => "Its subagents are unattended",
        "failure" | "failed" | "agent.failed" => "The Agent could not finish",
        "lost" | "agent.lost" => "Lost contact with the Agent",
        "interrupted" => "Interrupted",
        "refused" | "rejected" => "The request was declined",
        "cancelled" => "Cancelled",
        "crashed" => "The Agent software stopped unexpectedly",
        "success" | "completed" | "applied" => "Completed",
        "native_not_dispatched" => "The agent app did not start",
        "native_spawn_refused" | "native_spawn" => "The agent app could not start",
        "native_model_unavailable" => "The model is unavailable",
        "native_rejected" => "The agent app declined the request",
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
    // The words already name the code: drop a repeated " (code)" from the
    // daemon's explanation so the owner reads each fact once.
    let message = message.replacen(&format!(" ({code})"), "", 1);
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
