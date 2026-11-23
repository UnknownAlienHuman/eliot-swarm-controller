//! Owner-facing words. Protocol codes remain unchanged for decisions/readback.
//! Plain words only: no internal terms (generation, harness, observe-only),
//! and an unknown outcome reads "couldn't be confirmed", never success.

use crate::Error;
use crate::context::{ReviewWorktreeState, TrustedReviewLabels};

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
        "routing_unavailable" => "No model can take this task right now",
        "provider_held" => "The provider is on hold",
        "workspace_unavailable" => "The project folder is unavailable",
        "rate_limited" => "Too many automatic starts in a short time",
        "transfer_in_progress" => "A project transfer is in progress",
        "id_conflict" => "This request was already used for a different action",
        "daemon_restart" => "Interrupted by a Swarm restart",
        "root_exited" => "The agent app exited unexpectedly",
        "stream_eof_without_session_terminal" => {
            "The agent app closed its event stream before reporting the result"
        }
        "plugin_startup_exit" => "The agent app exited during startup",
        "no_progress" => "No progress for a while",
        "no_code_progress" => "Working without writing code",
        "source_unhealthy" => "Lost contact with its agent app",
        "children_unattended" => "Its subagents are unattended",
        "failure" | "failed" | "agent.failed" => "The Agent could not finish",
        "lost" | "agent.lost" => "Lost contact with the Agent",
        "interrupted" => "Interrupted",
        "interrupted_by_owner" | "stopped_by_owner" => "Stopped by you",
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
        if self.kind == "worktree.remove"
            && let (Some(code), Some(message)) = (self.code.as_deref(), self.message.as_deref())
        {
            return format!("{code}: {message}");
        }
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
/// Quote one external text body with a delimiter longer than every backtick run.
/// Compose trusted labels and metadata outside the returned block.
pub fn fence_untrusted_text(text: &str) -> String {
    let fence = "`".repeat(fence_width(text));
    format!("{fence}text\n{text}\n{fence}")
}

/// Render an insertion using the verified submitting principal as its sender.
pub fn swarm_note(text: &str, principal: &str) -> String {
    swarm_note_with_labels(text, principal, None)
}

/// Render daemon-verified review labels before the single untrusted-body fence.
pub fn swarm_note_with_trusted_review_labels(
    text: &str,
    principal: &str,
    labels: &TrustedReviewLabels,
) -> String {
    swarm_note_with_labels(text, principal, Some(labels))
}

fn swarm_note_with_labels(
    text: &str,
    principal: &str,
    labels: Option<&TrustedReviewLabels>,
) -> String {
    let sender = match principal.split_once(':') {
        Some(("agent", identity)) => identity.split(':').next().unwrap_or("Swarm"),
        Some(("project", _)) => "project rule",
        _ if principal == "owner" => "owner",
        _ if principal == "swarm_rule" => "Swarm rule",
        _ => "Swarm",
    };
    let trusted_labels = labels.map_or_else(String::new, |labels| {
        let worktree_state = match labels.worktree_state {
            ReviewWorktreeState::CurrentAtDeliveryCheck => "still current at delivery check",
            ReviewWorktreeState::ChangedOrUnconfirmedAtDeliveryCheck => {
                "changed or could not be confirmed at delivery check"
            }
        };
        format!(
            "Worktree fingerprint: {:?}\nWorktree state: {worktree_state}\n",
            labels.worktree_fingerprint
        )
    });
    format!(
        "[Swarm note from {sender}] This adds to your task and does not replace it.\n{trusted_labels}{}",
        fence_untrusted_text(text)
    )
}

fn fence_width(text: &str) -> usize {
    let longest = text.split(|ch| ch != '`').map(str::len).max().unwrap_or(0);
    3.max(longest + 1)
}

pub(crate) fn fenced_untrusted_len(text: &str) -> usize {
    text.len() + 2 * fence_width(text) + "text\n\n".len()
}

/// Quote the longest UTF-8 prefix that leaves room for both delimiters.
/// Returns an empty block when there is no room for the minimum frame.
pub fn fence_untrusted_prefix(text: &str, budget: usize) -> (&str, String) {
    if fenced_untrusted_len("") > budget {
        return ("", String::new());
    }
    let excerpt = &text[..text.floor_char_boundary(budget)];
    if fenced_untrusted_len(excerpt) <= budget {
        return (excerpt, fence_untrusted_text(excerpt));
    }
    let mut low = 0;
    let mut high = excerpt.len();
    let mut best = "";
    while low < high {
        let middle = low + (high - low) / 2;
        let excerpt = &text[..text.floor_char_boundary(middle)];
        if fenced_untrusted_len(excerpt) <= budget {
            best = excerpt;
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    (best, fence_untrusted_text(best))
}
