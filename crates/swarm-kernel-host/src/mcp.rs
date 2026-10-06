//! Compatibility facade: the independent `swarm-mcp` package owns MCP transport,
//! tools, catalog, profile filtering, and session behavior. The controller maps
//! its already validated config into that frontend; Store authorization remains
//! on the host for every forwarded method.

pub use swarm_mcp::ProfiledFacade;

use crate::{
    config::{Config, McpToolProfile},
    error::Result,
    model::Credential,
};
use serde_json::Value;

fn frontend_config(config: &Config) -> swarm_mcp::Config {
    swarm_mcp::Config {
        storage: swarm_mcp::config::Storage {
            data_dir: config.storage.data_dir.clone(),
        },
        ipc: config.ipc.clone(),
        mcp: config.mcp.clone(),
    }
}

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "Retained host-config adapter for the compatibility facade now implemented by swarm_mcp"
    )
)]
pub(crate) fn profiled_facade(
    config: &Config,
    credential: Credential,
    profile_name: Option<&str>,
) -> Result<ProfiledFacade> {
    swarm_mcp::profiled_facade(&frontend_config(config), credential, profile_name)
        .map_err(Into::into)
}

pub async fn run(config: Config, credential: Credential) -> Result<()> {
    run_profiled(config, credential, None).await
}

pub async fn run_profiled(
    config: Config,
    credential: Credential,
    profile_name: Option<&str>,
) -> Result<()> {
    swarm_mcp::run_profiled(frontend_config(&config), credential, profile_name)
        .await
        .map_err(Into::into)
}

pub fn application_method_read_only(method: &str) -> Option<bool> {
    swarm_mcp::application_method_read_only(method)
}

pub(crate) fn registered_application_methods() -> Vec<&'static str> {
    swarm_contracts::method_policy::METHOD_REGISTRY
        .iter()
        .filter_map(|entry| {
            (entry.mcp
                && entry.method != "swarm.tools.search"
                && swarm_mcp::application_method_read_only(entry.method).is_some())
            .then_some(entry.method)
        })
        .collect()
}

pub(crate) fn participant_core_tool_contracts() -> Result<Vec<Value>> {
    swarm_mcp::participant_core_tool_contracts().map_err(Into::into)
}

pub(crate) fn launch_profile_surface(
    profile: McpToolProfile,
    surface_name: &str,
    groups: &[String],
    manual_tools: &[String],
) -> Result<Value> {
    swarm_mcp::launch_profile_surface(profile, surface_name, groups, manual_tools)
        .map_err(Into::into)
}
