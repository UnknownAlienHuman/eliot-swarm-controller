//! Pure command error classification and state checks. Store still proves exact
//! command/generation/turn bindings and authoritative outcomes before committing.

use crate::CommandState;

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

#[cfg(test)]
mod tests {
    use super::{
        classify_dispatched_error, command_transition_allowed,
        workspace_preparation_transition_allowed,
    };
    use crate::CommandState::{
        Accepted, Applied, Cancelled, Dispatched, Failed, Queued, Rejected, Unknown,
    };

    #[test]
    fn dispatched_errors_require_explicit_no_io_proof() {
        assert_eq!(classify_dispatched_error("native_not_dispatched"), Rejected);
        for code in [
            "unknown",
            "unavailable",
            "internal",
            "invalid",
            "frame_too_large",
            "source_integrity",
            "profile_error",
            "unsupported",
            "not_allowed",
            "not_found",
            "bad_state",
            "stale_generation",
            "secret_in_payload",
            "busy",
            "native_rejected",
            "process_registration",
            "failed",
            "future_error_code",
            "",
        ] {
            assert_eq!(classify_dispatched_error(code), Unknown, "code={code}");
        }
    }

    #[test]
    fn ordinary_edges_keep_uncertain_work_out_of_dispatch() {
        for (from, to, allowed) in [
            (Accepted, Queued, true),
            (Accepted, Dispatched, true),
            (Accepted, Rejected, true),
            (Accepted, Cancelled, true),
            (Accepted, Applied, false),
            (Accepted, Failed, false),
            (Accepted, Unknown, false),
            (Queued, Dispatched, true),
            (Queued, Rejected, true),
            (Queued, Cancelled, true),
            (Queued, Accepted, false),
            (Queued, Applied, false),
            (Queued, Failed, false),
            (Queued, Unknown, false),
            (Dispatched, Applied, true),
            (Dispatched, Rejected, true),
            (Dispatched, Failed, true),
            (Dispatched, Unknown, true),
            (Dispatched, Cancelled, false),
            (Unknown, Applied, true),
            (Unknown, Rejected, true),
            (Unknown, Failed, true),
            (Unknown, Accepted, false),
            (Unknown, Queued, false),
            (Unknown, Dispatched, false),
            (Unknown, Cancelled, false),
        ] {
            assert_eq!(
                command_transition_allowed(from, to),
                allowed,
                "{from:?} -> {to:?}"
            );
        }
        for terminal in [Applied, Rejected, Failed, Cancelled] {
            for target in [
                Accepted, Queued, Dispatched, Applied, Rejected, Failed, Unknown, Cancelled,
            ] {
                assert_eq!(
                    command_transition_allowed(terminal, target),
                    terminal == target
                );
            }
        }
    }

    #[test]
    fn workspace_exceptions_do_not_widen_ordinary_settlement() {
        for (from, to) in [
            (Accepted, Failed),
            (Accepted, Unknown),
            (Queued, Failed),
            (Queued, Unknown),
            (Unknown, Accepted),
            (Unknown, Cancelled),
        ] {
            assert!(!command_transition_allowed(from, to));
            assert!(workspace_preparation_transition_allowed(from, to));
        }
        for (from, to) in [
            (Accepted, Applied),
            (Queued, Applied),
            (Unknown, Dispatched),
            (Unknown, Queued),
            (Dispatched, Accepted),
            (Failed, Accepted),
            (Applied, Failed),
            (Cancelled, Unknown),
        ] {
            assert!(!workspace_preparation_transition_allowed(from, to));
        }
    }
}
