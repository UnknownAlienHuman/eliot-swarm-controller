//! Store-independent ScriptRun planning and result projection.
//!
//! This crate deliberately has no database, host, or process-launch dependency.
//! The Store supplies a current authorized route and safe event projection,
//! persists the returned plan through its existing journal/Operation path, and
//! passes the resulting private process plan to the selected process worker.

mod canonical;
mod error;
pub mod event;
pub mod plan;
pub mod process;
pub mod protocol;
pub mod result;
pub mod schema;

pub use error::{Result, ScriptError};
pub use swarm_bus::{EventMetadata, EventSelector, EventStatus};

pub const MAX_INPUT_BYTES: usize = 256 * 1024;
pub const MAX_BUNDLE_FILE_BYTES: usize = 256 * 1024;
pub const MAX_BUNDLE_BYTES: usize = 512 * 1024;
pub const MAX_INVOCATION_BYTES: usize = MAX_INPUT_BYTES + 4096;
pub const MAX_RESULT_BYTES: usize = 256 * 1024;
pub const MAX_STDOUT_BYTES: usize = MAX_RESULT_BYTES;
pub const MAX_STDERR_BYTES: usize = 1024 * 1024;
pub const MAX_SCRIPT_DURATION_MS: u64 = 5 * 60 * 1000;
pub const MAX_ARGUMENTS: usize = 32;
pub const MAX_ARGUMENT_BYTES: usize = 4096;
pub const MAX_INHERITED_ENVIRONMENT: usize = 32;
pub const MAX_ENVIRONMENT_VALUE_BYTES: usize = 16 * 1024;
pub const MAX_ENVIRONMENT_BYTES: usize = 256 * 1024;
pub const MAX_CONTROLLER_EFFECTS: usize = 1;
pub const MAX_CONTROLLER_EFFECT_TEXT_BYTES: usize = 4096;

pub(crate) fn canonical_json(value: &serde_json::Value) -> Result<String> {
    canonical::json(value)
}

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    canonical::sha256_hex(bytes)
}
