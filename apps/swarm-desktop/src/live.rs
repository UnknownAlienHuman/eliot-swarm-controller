//! Live delivery for the selected conversation as pure logic.
//!
//! The cursor contract from the events API: capture a committed cursor
//! (`events.bounds.hi`) before the snapshot reads, then consume strictly
//! after it and continue from `next`, never from `high_water`. A wake call
//! (`events.subscribe`, limit 1, `wait_ms` set explicitly) returns on the
//! first matching event; a drain (`events.read`) then takes the rest of the
//! burst so one UI update carries it. `truncated` only means "more pages":
//! keep draining without inventing a gap. `lagged` means the live buffer
//! overflowed: mark stale and reread. Only `history_gap` proves pruned
//! history; it also forces a resnapshot with a coverage notice.
//!
//! Reactor 0.100.0 background tasks return one message each, so the loop is
//! a chain of one-shot tasks. Every result carries the selection epoch; a
//! result from an older epoch is dropped, never applied.

/// Long-poll wait. Explicit because the DTO default (0) would spin; the
/// protocol clamps at 30,000.
pub const WAKE_WAIT_MS: u64 = 25_000;
/// Burst coalescing before the drain read (display flush around 33–50 ms).
pub const COALESCE_MS: u64 = 40;
/// Drain page size.
pub const DRAIN_LIMIT: usize = 500;
/// Drain pages per wake before yielding back to the UI.
pub const DRAIN_PAGES: usize = 4;
const BACKOFF_BASE_MS: u64 = 500;
const BACKOFF_MAX_MS: u64 = 15_000;

/// What the UI shows about live delivery.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LiveStatus {
    /// No Agent selected.
    Idle,
    /// Snapshot reads in flight.
    Loading,
    /// Consuming after the committed cursor.
    Live,
    /// Delivery overflowed or history moved; a reread is in flight.
    Stale(String),
    /// Transport failure; retrying after a backoff.
    Reconnecting { attempt: u32, reason: String },
    /// Authentication or configuration failure; not retried automatically.
    Stopped(String),
}

impl LiveStatus {
    pub fn label(&self) -> String {
        match self {
            Self::Idle => "No conversation".to_owned(),
            Self::Loading => "Loading retained history…".to_owned(),
            Self::Live => "Live".to_owned(),
            Self::Stale(reason) => format!("Stale ({reason}); rereading"),
            Self::Reconnecting { attempt, reason } => {
                format!("Reconnecting (attempt {attempt}): {reason}")
            }
            Self::Stopped(reason) => format!("Live updates stopped: {reason}. Use Refresh."),
        }
    }
}

/// Next step after one delivery result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Next {
    /// Long-poll again from this cursor.
    Wake { after: i64 },
    /// More committed events are known: read again without waiting.
    Drain { after: i64 },
    /// Capture a new cursor and reload the reads.
    Resnapshot { reason: String },
    /// Wait, then resnapshot (transport failure).
    Backoff {
        delay_ms: u64,
        attempt: u32,
        reason: String,
    },
    /// Do not retry automatically.
    Stop { reason: String },
}

/// How a daemon error code affects live delivery.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorClass {
    /// The cursor fell below the retention floor.
    HistoryGap,
    /// Transport or transient daemon state: back off and resnapshot.
    Transient,
    /// Authentication, authorization or configuration: surface, do not loop.
    Fatal,
}

pub fn classify(code: &str) -> ErrorClass {
    match code {
        "history_gap" => ErrorClass::HistoryGap,
        "unavailable" | "unknown" | "busy" | "timeout" | "internal" | "frame_too_large" => {
            ErrorClass::Transient
        }
        _ => ErrorClass::Fatal,
    }
}

pub fn backoff_ms(attempt: u32) -> u64 {
    BACKOFF_BASE_MS
        .saturating_mul(1_u64 << attempt.min(16))
        .min(BACKOFF_MAX_MS)
}

/// Decide after a successful page read strictly after `after`; `next` is
/// the page's scan cursor. A truncated page that made no progress waits
/// instead of draining again, so a stuck cursor cannot spin.
pub fn after_page(after: i64, next: i64, truncated: bool, lagged: bool) -> Next {
    if lagged {
        Next::Resnapshot {
            reason: "live buffer overflowed".to_owned(),
        }
    } else if truncated && next > after {
        Next::Drain { after: next }
    } else {
        Next::Wake { after: next }
    }
}

/// Decide after a failed read; `attempt` counts consecutive failures.
pub fn after_error(code: &str, message: &str, attempt: u32) -> Next {
    match classify(code) {
        ErrorClass::HistoryGap => Next::Resnapshot {
            reason: "cursor fell below the retention floor".to_owned(),
        },
        ErrorClass::Transient => Next::Backoff {
            delay_ms: backoff_ms(attempt),
            attempt: attempt + 1,
            reason: format!("{code}: {message}"),
        },
        ErrorClass::Fatal => Next::Stop {
            reason: format!("{code}: {message}"),
        },
    }
}

/// Split `"code: message"` as produced by `eliot_core::Error`'s Display.
pub fn split_error(error: &str) -> (&str, &str) {
    match error.split_once(": ") {
        Some((code, message)) if !code.contains(' ') => (code, message),
        _ => ("unknown", error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn continue_from_next_and_drain_truncated_pages() {
        assert_eq!(after_page(30, 41, false, false), Next::Wake { after: 41 });
        assert_eq!(after_page(30, 41, true, false), Next::Drain { after: 41 });
        assert_eq!(after_page(41, 41, true, false), Next::Wake { after: 41 });
    }

    #[test]
    fn lag_marks_stale_and_rereads() {
        assert!(matches!(
            after_page(30, 41, true, true),
            Next::Resnapshot { .. }
        ));
    }

    #[test]
    fn history_gap_resnapshots_and_auth_stops() {
        assert!(matches!(
            after_error("history_gap", "x", 0),
            Next::Resnapshot { .. }
        ));
        assert!(matches!(
            after_error("not_allowed", "x", 0),
            Next::Stop { .. }
        ));
        assert!(matches!(after_error("invalid", "x", 3), Next::Stop { .. }));
    }

    #[test]
    fn transport_failures_back_off_with_a_cap() {
        let Next::Backoff {
            delay_ms, attempt, ..
        } = after_error("unavailable", "down", 0)
        else {
            panic!("backoff expected");
        };
        assert_eq!((delay_ms, attempt), (500, 1));
        assert_eq!(backoff_ms(10), BACKOFF_MAX_MS);
    }

    #[test]
    fn error_text_splits_into_code_and_message() {
        assert_eq!(
            split_error("unavailable: daemon is not running"),
            ("unavailable", "daemon is not running")
        );
        assert_eq!(
            split_error("plain words: here"),
            ("unknown", "plain words: here")
        );
    }
}
