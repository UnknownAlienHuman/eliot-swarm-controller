#![deny(unsafe_code)]

pub(crate) mod automation;
mod bus;
pub mod cloud_mcp;
mod common;
mod config;
mod daemon;
mod fs_invalidation;
mod git_actions;
mod git_monitor;
pub(crate) mod logging;
mod maintenance;
mod monitor;
mod platform;
mod plugin_host;
mod proxy;
mod redaction;
mod restart_capacity;
mod router;
pub(crate) mod scripts;
mod setup;
#[cfg(test)]
pub(crate) mod test_support;
mod workspace_git;
mod workspaces;

pub use daemon::{
    MAX_RPC_FRAME_BYTES, client_call, client_call_typed, client_call_with_owner_credential, run,
};

/// Start this executable's background daemon without inheriting caller handles.
#[cfg(windows)]
pub fn start_background_daemon(data_dir: &std::path::Path) -> eliot_core::Result<()> {
    platform::windows::start_background_daemon(data_dir)
}
