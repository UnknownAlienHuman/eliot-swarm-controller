//! Pure command error classification and state checks. Store still proves exact
//! command/generation/turn bindings and authoritative outcomes before committing.

use crate::CommandState;

/// Validate an exact retained command identity, including bounded derived IDs.
/// External mutation IDs have a separate ULID contract; command references are
/// opaque and must not be trimmed, normalized, or parsed as mutation IDs.
pub fn validate_command_id(id: &str) -> crate::Result<()> {
    if id.trim().is_empty() || id.len() > crate::limits::MAX_COMMAND_ID_BYTES {
        return Err(crate::Error::new(
            "invalid",
            "command ID must be nonblank and at most 256 bytes",
        ));
    }
    Ok(())
}

/// Whether a code is a deterministic refusal before an external effect is
/// dispatched. After dispatch, callers must use `classify_dispatched_error`;
/// these codes alone do not prove that native I/O did not occur.
/// `version_conflict` is the Store's compare-and-swap guard: it returns
/// before any write, so a stale expectation is a confirmed pre-effect
/// refusal, not an uncertain outcome.
pub fn is_deterministic_pre_admission_rejection(code: &str) -> bool {
    matches!(
        code,
        "assignment_occupied"
            | "brief_settling"
            | "brief_requires_include"
            | "bad_state"
            | "capacity_unavailable"
            | "conflict"
            | "frame_too_large"
            | "invalid"
            | "not_allowed"
            | "not_found"
            | "provider_held"
            | "quota_reserve"
            | "quota_unknown"
            | "routing_unavailable"
            | "secret_in_payload"
            | "stale_generation"
            | "stale_assignment"
            | "transfer_in_progress"
            | "unsupported"
            | "version_conflict"
    )
}

/// Classify an error after command dispatch without inventing a native receipt.
/// Only `native_not_dispatched` explicitly proves that native I/O did not start.
/// Generic validation, framing and transport codes can also arrive after I/O.
/// Authoritative plugin result states and typed workspace failures are settled
/// separately; an error code alone never establishes `Failed`.
pub fn classify_dispatched_error(code: &str) -> CommandState {
    match code {
        "native_not_dispatched" => CommandState::Rejected,
        _ => CommandState::Unknown,
    }
}

/// Ordinary command edges, not permission to repeat external work. A same-state
/// acknowledgement still requires Store's payload/outcome idempotency checks;
/// leaving Unknown requires exact readback or another authoritative result.
pub fn command_transition_allowed(from: CommandState, to: CommandState) -> bool {
    use CommandState::{
        Accepted, Applied, Cancelled, Dispatched, Failed, Queued, Rejected, Unknown,
    };

    if from == to {
        return true;
    }
    match from {
        Accepted => matches!(to, Queued | Dispatched | Rejected | Cancelled),
        Queued => matches!(to, Dispatched | Rejected | Cancelled),
        Dispatched | Unknown => matches!(to, Applied | Rejected | Failed | Unknown),
        Applied | Rejected | Failed | Cancelled => false,
    }
}

/// Narrow Owner assertion for a failed handoff brief. Store proves the brief
/// identity and Owner principal; this never authorizes another native SEND.
pub fn handoff_brief_resolution_allowed(from: CommandState, to: CommandState) -> bool {
    command_transition_allowed(from, to)
        || (from == CommandState::Failed && to == CommandState::Applied)
}

/// Existing pre-native workspace exceptions. The caller must prove the exact
/// preparation marker, command verb and generation before using these edges.
/// Unknown -> Accepted additionally needs verified creation readback proving the
/// remaining steps/native dispatch unperformed; it never authorizes Git replay.
/// Unknown -> Cancelled requires confirmed preparation cancellation/cleanup.
pub fn workspace_preparation_transition_allowed(from: CommandState, to: CommandState) -> bool {
    use CommandState::{Accepted, Cancelled, Failed, Queued, Unknown};

    command_transition_allowed(from, to)
        || matches!(
            (from, to),
            (Accepted | Queued, Failed | Unknown) | (Unknown, Accepted | Cancelled)
        )
}

/// Verbs that start an Agent generation: first launch or resume.
pub fn is_start_verb(verb: &str) -> bool {
    matches!(verb, "launch" | "resume")
}

/// Verbs that attach or detach an observer of an exact native subject.
pub fn is_attachment_verb(verb: &str) -> bool {
    matches!(verb, "attach" | "detach")
}

/// Verbs that answer a pending Agent request.
pub fn is_request_reply_verb(verb: &str) -> bool {
    matches!(verb, "approve" | "deny" | "answer")
}

/// Targetless verbs that record a user-visible event.
pub fn is_user_event_verb(verb: &str) -> bool {
    matches!(verb, "notify" | "emit")
}

/// Targetless verbs that move coordination data between Agents.
pub fn is_coordination_verb(verb: &str) -> bool {
    matches!(verb, "mail.send" | "var.set")
}
