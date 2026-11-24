mod connection;
mod host;
#[cfg(windows)]
mod launcher_update;

pub(crate) use connection::{PluginClient, PluginConnection};
pub(crate) use host::{
    AgentCredentials, MetadataRoute, OWNER_INTERRUPT_TIMEOUT, PendingCommand, PluginHost,
    PluginHostOptions, SecretSet, StartError, UnknownCommands, build_process_environment,
    forget_revoked_agent_token, metadata_route, minimal_plugin_environment,
};
