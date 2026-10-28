#![deny(unsafe_code)]

mod bus;
mod config;
mod daemon;
mod fs_invalidation;
mod git_monitor;
mod monitor;
mod platform;
mod plugin_host;
mod redaction;
mod router;
mod setup;
mod workspace_git;
mod workspaces;

pub use daemon::{client_call, run};

/// Start this executable's background daemon without inheriting caller handles.
#[cfg(windows)]
pub fn start_background_daemon(data_dir: &std::path::Path) -> eliot_core::Result<()> {
    platform::windows::start_background_daemon(data_dir)
}
