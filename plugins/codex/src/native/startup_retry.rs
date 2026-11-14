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

#[cfg(test)]
mod tests {
    use super::{
        MAX_STARTUP_RETRIES, StartupFailureEvidence, exhausted_start_failure, native_start_failed,
        next_retry_count, state_runtime_init_signature,
    };
    use eliot_core::StderrSnapshot;

    fn signature() -> StderrSnapshot {
        StderrSnapshot {
            text: "failed to initialize sqlite state runtime: storage error\nfailed to initialize state runtime".into(),
            truncated: false,
            eof: true,
            read_error: false,
        }
    }

    fn retryable_evidence(stderr: &StderrSnapshot) -> StartupFailureEvidence<'_> {
        StartupFailureEvidence {
            initialize_request_failed: true,
            initialize_response_received: false,
            process_exited_unsuccessfully: true,
            output_fully_drained: true,
            thread_or_session_request_sent: false,
            native_frame_seen: false,
            stderr,
        }
    }

    #[test]
    fn exact_complete_sqlite_state_runtime_signature_matches() {
        assert!(state_runtime_init_signature(&signature()));
        for stderr in [
            StderrSnapshot {
                text: "failed to initialize state runtime".into(),
                ..signature()
            },
            StderrSnapshot {
                truncated: true,
                ..signature()
            },
            StderrSnapshot {
                eof: false,
                ..signature()
            },
            StderrSnapshot {
                read_error: true,
                ..signature()
            },
        ] {
            assert!(!state_runtime_init_signature(&stderr));
        }
    }

    #[test]
    fn exactly_one_retry_is_spent_only_for_the_proven_initialize_exit() {
        let stderr = signature();
        let evidence = retryable_evidence(&stderr);
        assert_eq!(next_retry_count(0, evidence), Some(1));
        assert_eq!(next_retry_count(MAX_STARTUP_RETRIES, evidence), None);

        let bound = StartupFailureEvidence {
            thread_or_session_request_sent: true,
            ..evidence
        };
        assert_eq!(next_retry_count(0, bound), None);
    }

    #[test]
    fn retry_proof_rejects_non_init_or_unobserved_failures() {
        let stderr = signature();
        let different_stderr = StderrSnapshot {
            text: "different init error".into(),
            ..signature()
        };
        let evidence = retryable_evidence(&stderr);
        for rejected in [
            StartupFailureEvidence {
                initialize_request_failed: false,
                ..evidence
            },
            StartupFailureEvidence {
                initialize_response_received: true,
                ..evidence
            },
            StartupFailureEvidence {
                process_exited_unsuccessfully: false,
                ..evidence
            },
            StartupFailureEvidence {
                output_fully_drained: false,
                ..evidence
            },
            StartupFailureEvidence {
                native_frame_seen: true,
                ..evidence
            },
            StartupFailureEvidence {
                stderr: &different_stderr,
                ..evidence
            },
        ] {
            assert_eq!(next_retry_count(0, rejected), None);
        }
    }

    #[test]
    fn definite_start_failure_requires_confirmed_custody_and_no_effect_evidence() {
        let stderr = signature();
        let evidence = retryable_evidence(&stderr);
        assert!(evidence.proves_no_thread_or_session_effect(true));
        assert!(!evidence.proves_no_thread_or_session_effect(false));
        assert!(
            !StartupFailureEvidence {
                native_frame_seen: true,
                ..evidence
            }
            .proves_no_thread_or_session_effect(true)
        );
        assert_eq!(native_start_failed().code, "native_start_failed");
    }

    #[test]
    fn exhausted_retry_classifies_a_clean_second_initialize_failure_definitely() {
        let stderr = StderrSnapshot {
            text: "a different initialize failure".into(),
            truncated: false,
            eof: true,
            read_error: false,
        };
        let incomplete_stderr = StderrSnapshot {
            text: stderr.text.clone(),
            truncated: true,
            eof: stderr.eof,
            read_error: stderr.read_error,
        };
        let evidence = StartupFailureEvidence {
            initialize_request_failed: true,
            initialize_response_received: false,
            process_exited_unsuccessfully: false,
            output_fully_drained: true,
            thread_or_session_request_sent: false,
            native_frame_seen: false,
            stderr: &stderr,
        };
        assert!(exhausted_start_failure(0, evidence, true).is_none());
        assert_eq!(
            exhausted_start_failure(1, evidence, true)
                .expect("only the second clean initialize failure is definite")
                .code,
            "native_start_failed"
        );
        assert!(exhausted_start_failure(1, evidence, false).is_none());
        assert!(
            exhausted_start_failure(
                1,
                StartupFailureEvidence {
                    initialize_request_failed: false,
                    ..evidence
                },
                true,
            )
            .is_none()
        );
        assert!(
            exhausted_start_failure(
                1,
                StartupFailureEvidence {
                    initialize_response_received: true,
                    ..evidence
                },
                true,
            )
            .is_none()
        );
        assert!(
            exhausted_start_failure(
                1,
                StartupFailureEvidence {
                    stderr: &incomplete_stderr,
                    ..evidence
                },
                true,
            )
            .is_none()
        );
    }
}
