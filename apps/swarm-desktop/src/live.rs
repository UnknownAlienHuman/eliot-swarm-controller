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
//!
//! Every retry and poll of the desktop paces itself through one [`Pace`]
//! (#826): its failure count grows on each failed attempt, resets on a
//! success, and the pause after a failure grows up to 15 s.

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
/// Failed reconnects in a row after which the live line offers Start Swarm
/// instead of only counting attempts.
pub const OFFER_START_AFTER: u32 = 3;
/// The desktop's code for a daemon reply that fails its own checks
/// (`validate_response`, or a shape the reply type does not decode): the
/// message names what did not match. Retrying gets the same reply.
pub const INVALID_REPLY: &str = "invalid_reply";

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

    /// The feed failed to reconnect often enough that the owner is offered
    /// Start Swarm (it still retries on its own meanwhile).
    pub fn offers_start(&self) -> bool {
        matches!(self, Self::Reconnecting { attempt, .. } if *attempt >= OFFER_START_AFTER)
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
    /// Wait (the feed's [`Pace`]), then resnapshot (transport failure).
    Backoff { reason: String },
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
        // The same reply comes back on every retry: show it, stop.
        INVALID_REPLY => ErrorClass::Fatal,
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

/// One retry or poll loop's consecutive failures: every loop of the desktop
/// (the conversation's reads and feed, the waiting-input check, the
/// Monitor, the sidebar's refresh, the daemon link's probes, the catalogue
/// watch and Swarm autonomy) waits through it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Pace {
    failures: u32,
}

impl Pace {
    /// Failed attempts in a row.
    pub fn failures(self) -> u32 {
        self.failures
    }

    pub fn reset(&mut self) {
        self.failures = 0;
    }

    /// One more failure: the pause before the next attempt, growing from
    /// 500 ms to 15 s and never shorter than the loop's own `period_ms`.
    pub fn fail(&mut self, period_ms: u64) -> u64 {
        let delay = backoff_ms(self.failures).max(period_ms);
        self.failures = self.failures.saturating_add(1);
        delay
    }

    /// The pause after an attempt that succeeded (`period_ms`, the count
    /// reset) or failed ([`Self::fail`]).
    pub fn next(&mut self, ok: bool, period_ms: u64) -> u64 {
        if ok {
            self.reset();
            period_ms
        } else {
            self.fail(period_ms)
        }
    }
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

/// Decide after a failed read.
pub fn after_error(error: &eliot_core::Error) -> Next {
    let code = error.code.as_str();
    let message = error.message.as_str();
    match classify(code) {
        ErrorClass::HistoryGap => Next::Resnapshot {
            reason: "cursor fell below the retention floor".to_owned(),
        },
        ErrorClass::Transient => Next::Backoff {
            reason: eliot_core::reason_message(code, message),
        },
        ErrorClass::Fatal => Next::Stop {
            reason: eliot_core::reason_message(code, message),
        },
    }
}
