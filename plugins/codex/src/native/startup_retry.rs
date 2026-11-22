//! Narrow retry proof for Codex app-server initialization failures.

use eliot_core::{Error, StderrSnapshot};

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

pub(crate) fn native_start_failed() -> Error {
    Error::new(
        "native_start_failed",
        "Codex native startup failed before any session or thread was created",
    )
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
