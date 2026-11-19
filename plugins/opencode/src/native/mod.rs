pub(crate) mod auth;
pub(crate) mod client;
pub(crate) mod models;
pub(crate) mod requests;
pub(crate) mod server;
pub(crate) mod sse;
pub(crate) mod startup;
pub(crate) mod types;
pub(crate) mod usage;

pub(crate) use auth::{
    AuthSetup, AuthTransfer, IntegrationSelector, transfer_selected_integration,
};
pub(crate) use client::{HttpOutcome, OpenCodeHttp};
pub(crate) use server::{OwnedServer, SWARM_MCP_SERVER_NAME, ServerLaunch};

/// Use the same isolated config discovery for serve and both model readers.
pub(crate) fn apply_environment(
    command: &mut tokio::process::Command,
    environment: &[(std::ffi::OsString, std::ffi::OsString)],
    local_provider: bool,
) {
    if local_provider {
        const OVERRIDES: &[&str] = &[
            "OPENCODE_CONFIG",
            "OPENCODE_CONFIG_CONTENT",
            "OPENCODE_CONFIG_DIR",
            "OPENCODE_CONFIG_PROJECT_DISABLE",
        ];
        command
            .env_clear()
            .envs(environment.iter().filter_map(|(name, value)| {
                let is_override = OVERRIDES.iter().any(|key| name.eq_ignore_ascii_case(key));
                (!is_override).then_some((name, value))
            }));
        command.env("OPENCODE_CONFIG_PROJECT_DISABLE", "1");
    } else {
        command.envs(environment.iter().map(|(name, value)| (name, value)));
    }
}
