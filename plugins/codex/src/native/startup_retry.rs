//! Narrow retry proof for Codex app-server initialization failures.

use eliot_core::{Error, StderrSnapshot};
use std::time::Duration;

pub(crate) const MAX_STARTUP_RETRIES: u8 = 1;

const SQLITE_INIT_SIGNATURE: &str = "failed to initialize sqlite state runtime";
const STATE_RUNTIME_SIGNATURE: &str = "failed to initialize state runtime";

#[derive(Clone, Copy)]
pub(crate) struct StartupFailureEvidence<'a> {
    pub(crate) initialize_request_failed: bool,
    pub(crate) initialize_response_received: bool,
    pub(crate) process_exited_unsuccessfully: bool,
    pub(crate) output_fully_drained: bool,
    pub(crate) thread_or_session_request_sent: bool,
    pub(crate) native_frame_seen: bool,
    pub(crate) stderr: &'a StderrSnapshot,
}

impl StartupFailureEvidence<'_> {
    pub(crate) fn retryable(self) -> bool {
        self.initialize_request_failed
            && !self.initialize_response_received
            && self.process_exited_unsuccessfully
            && self.output_fully_drained
            && !self.thread_or_session_request_sent
            && !self.native_frame_seen
            && state_runtime_init_signature(self.stderr)
    }

    pub(crate) fn proves_no_thread_or_session_effect(self, custody_confirmed: bool) -> bool {
        custody_confirmed
            && self.output_fully_drained
            && self.stderr.eof
            && !self.stderr.truncated
            && !self.stderr.read_error
            && !self.thread_or_session_request_sent
            && !self.native_frame_seen
    }
}

/// Advance the real startup loop's retry counter only for the one proven-safe
/// initialization failure. The retry count is the number already consumed.
pub(crate) fn next_retry_count(
    retries_used: u8,
    evidence: StartupFailureEvidence<'_>,
) -> Option<u8> {
    (retries_used < MAX_STARTUP_RETRIES && evidence.retryable()).then_some(retries_used + 1)
}

pub(crate) fn exhausted_start_failure(
    retries_used: u8,
    evidence: StartupFailureEvidence<'_>,
    custody_confirmed: bool,
) -> Option<Error> {
    (retries_used >= MAX_STARTUP_RETRIES
        && evidence.initialize_request_failed
        && !evidence.initialize_response_received
        && evidence.proves_no_thread_or_session_effect(custody_confirmed))
    .then(native_start_failed)
}

pub(crate) fn native_start_failed() -> Error {
    Error::new(
        "native_start_failed",
        "Codex native startup failed before any session or thread was created",
    )
}

pub(crate) fn randomized_retry_delay() -> Duration {
    // ULID's low byte is backed by its process RNG and gives enough spread for
    // a single retry without adding another dependency or retaining entropy.
    let random_byte = ulid::Ulid::new().to_bytes()[15];
    Duration::from_millis(25 + u64::from(random_byte) % 76)
}

fn state_runtime_init_signature(stderr: &StderrSnapshot) -> bool {
    if !stderr.eof || stderr.truncated || stderr.read_error {
        return false;
    }
    let Some(sqlite_at) = stderr.text.find(SQLITE_INIT_SIGNATURE) else {
        return false;
    };
    stderr.text[sqlite_at + SQLITE_INIT_SIGNATURE.len()..].contains(STATE_RUNTIME_SIGNATURE)
}
