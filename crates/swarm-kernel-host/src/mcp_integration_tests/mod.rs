use std::{collections::BTreeMap, path::PathBuf};

use crate::config::McpToolProfile;
use swarm_mcp::config::{McpConfig, McpProfileConfig, Storage};

pub(super) fn public_facade(
    data_dir: PathBuf,
    credential: crate::model::Credential,
    ipc: swarm_mcp::config::Ipc,
    tool_profile: McpToolProfile,
) -> swarm_mcp::ProfiledFacade {
    let expected_client_id = credential.client_id.clone();
    public_facade_with_expected_client_id(
        data_dir,
        credential,
        ipc,
        tool_profile,
        expected_client_id,
    )
    .expect("the test frontend profile is valid")
}

pub(super) fn public_facade_with_expected_client_id(
    data_dir: PathBuf,
    credential: crate::model::Credential,
    ipc: swarm_mcp::config::Ipc,
    tool_profile: McpToolProfile,
    expected_client_id: String,
) -> std::result::Result<swarm_mcp::ProfiledFacade, String> {
    let profile_name = "mcp-integration";
    let frontend = swarm_mcp::Config {
        storage: Storage { data_dir },
        ipc,
        mcp: McpConfig {
            default_profile: profile_name.to_owned(),
            profiles: BTreeMap::from([(
                profile_name.to_owned(),
                McpProfileConfig {
                    tool_profile,
                    expected_client_id,
                    surface: None,
                    deferred_groups: Vec::new(),
                    manual_tools: Vec::new(),
                },
            )]),
        },
    };
    swarm_mcp::profiled_facade(&frontend, credential, Some(profile_name))
        .map_err(|error| error.code)
}

mod profiles;
mod subscriptions;
mod tasks;
