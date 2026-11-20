#![deny(unsafe_code)]

pub(crate) mod adhoc;
pub(crate) mod automation;
pub mod bootstrap;
mod bus;
pub mod cloud_mcp;
mod common;
mod config;
pub mod crash;
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
mod skill;
pub mod startup;
mod workspace_git;
mod workspaces;

/// The closed Claude Code `--effort` vocabulary (ad-hoc launches and the
/// desktop picker; Claude Code has no live model catalogue).
pub use adhoc::CLAUDE_EFFORT_LEVELS;
pub use config::load_cloud_access_config;
pub use config::settings::stored_section as stored_settings_section;
pub use daemon::client_call_with_remote_context;
pub use daemon::{
    CallTimings, DaemonStartupFailure, MAX_RPC_FRAME_BYTES, client_call, client_call_measured,
    client_call_typed, client_call_with_agent_identity, client_call_with_owner_credential,
    daemon_connect_hint, daemon_failure_since, last_daemon_failure, last_daemon_phase,
    require_agent_identity, run, wait_terminal,
};
pub use platform::data_dir::{persist_selected_root, resolve_data_dir, resolve_selected_root};
pub use router::discovery::is_mutation_id_method;
pub use router::discovery::mutation_id_methods;

/// Start this executable's background daemon without inheriting caller handles.
#[cfg(windows)]
pub fn start_background_daemon(data_dir: &std::path::Path) -> eliot_core::Result<()> {
    platform::windows::start_background_daemon(data_dir)
}
