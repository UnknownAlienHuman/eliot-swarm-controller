#![forbid(unsafe_code)]

pub mod capabilities;
mod events;
mod history;
mod host;
pub mod inference;
pub mod install;
mod instructions;
mod native;
mod owner_scan;
mod profile_config;
mod quota;

pub async fn run_claude() -> eliot_core::Result<()> {
    host::run().await
}

pub use profile_config::{CLAUDE_EFFORT_LEVELS, CLAUDE_EFFORTS, CLAUDE_PERMISSION_MODES};
