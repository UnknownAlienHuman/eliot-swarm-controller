//! Native diagnostics for Codex app-server initialization failures.

use eliot_core::Error;

const SQLITE_INIT_SIGNATURE: &str = "failed to initialize sqlite state runtime";
const STATE_RUNTIME_SIGNATURE: &str = "failed to initialize state runtime";

pub(crate) fn native_start_failed() -> Error {
    Error::new(
        "native_start_failed",
        "Codex native startup failed before any session or thread was created",
    )
}

pub(crate) fn state_runtime_init_signature(stderr: &str) -> bool {
    let Some(sqlite_at) = stderr.find(SQLITE_INIT_SIGNATURE) else {
        return false;
    };
    stderr[sqlite_at + SQLITE_INIT_SIGNATURE.len()..].contains(STATE_RUNTIME_SIGNATURE)
}
