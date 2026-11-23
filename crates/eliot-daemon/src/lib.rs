#![deny(unsafe_code)]

mod acl_notice;
pub(crate) mod adhoc;
pub(crate) mod automation;
pub mod bootstrap;
mod build_identity;
mod bus;
pub mod cloud_mcp;
mod command_guard;
mod common;
mod config;
pub mod crash;
pub(crate) mod daemon;
mod fs_invalidation;
mod git_actions;
mod git_integration;
mod git_monitor;
mod harness_capabilities;
pub(crate) mod logging;
mod maintenance;
mod monitor;
pub mod operator;
mod platform;
mod proxy;
pub(crate) mod publication;
mod redaction;
mod restart_capacity;
mod router;
pub(crate) mod scripts;
mod setup;
mod skill;
mod stand_cleanup;
pub mod startup;
mod update_backup;
mod workspace_git;
mod workspaces;

/// The closed Claude Code `--effort` vocabulary (ad-hoc launches and the
/// desktop picker; Claude Code has no live model catalogue).
pub use adhoc::CLAUDE_EFFORT_LEVELS;
pub use config::desktop_window_in_bounds;
pub use config::load_cloud_access_config;
pub use config::recovery::{read_settings_for_repair, save_settings_for_repair};
pub use config::settings::stored_section as stored_settings_section;
pub use daemon::{
    CallTimings, DaemonStartupFailure, MAX_RPC_FRAME_BYTES, StoreCompactionOutcome,
    StoreCompactionSpaceEstimate, client_call, client_call_measured, client_call_typed,
    client_call_with_agent_identity, client_call_with_agent_mcp_tool,
    client_call_with_owner_credential, compact_store_offline, daemon_connect_hint,
    daemon_failure_since, last_daemon_failure, last_daemon_phase, require_agent_identity, run,
    wait_command, wait_terminal,
};
pub use daemon::{client_call_with_remote_context, client_call_with_remote_mcp_tool};
pub use platform::data_dir::{
    DataDir, ForeignDataDirAce, data_dir_acl_report, owner_stopped, persist_selected_root,
    resolve_data_dir, resolve_selected_root,
};
pub use router::discovery::is_mutation_id_method;
pub use router::discovery::mutation_id_methods;

/// Start this executable's background daemon without inheriting caller handles.
#[cfg(windows)]
pub fn start_background_daemon(data_dir: &std::path::Path) -> eliot_core::Result<()> {
    platform::windows::start_background_daemon(data_dir)
}
