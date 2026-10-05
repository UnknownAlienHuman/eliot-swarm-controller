//! The gateway's compatible projection of the existing controller TOML.
//!
//! `swarm-mcp` owns the existing storage/IPC/MCP profile slice. This module
//! reads only `[gateway]`, so the standalone binary does not depend on the
//! controller's full configuration type or validation graph.

use serde::Deserialize;
use std::path::{Path, PathBuf};
use swarm_client::IpcConfig;
use swarm_contracts::error::{Error, Result};

pub const MAX_GATEWAY_BODY_BYTES: usize = 1_048_576;

#[derive(Debug, Clone)]
pub struct Config {
    pub frontend: swarm_mcp::Config,
    pub gateway: GatewaySettings,
}

/// Existing `[gateway]` fields and defaults, kept compatible with
/// `eliot_swarm_controller::config::GatewayConfig`.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GatewaySettings {
    pub enabled: bool,
    pub bind: String,
    pub profile: String,
    pub credential_file: Option<PathBuf>,
    pub local_bearer_file: Option<PathBuf>,
    pub max_body_bytes: usize,
    pub request_timeout_seconds: u64,
}

impl Default for GatewaySettings {
    fn default() -> Self {
        Self {
            enabled: false,
            bind: "127.0.0.1:8787".into(),
            profile: "local-observer".into(),
            credential_file: None,
            local_bearer_file: None,
            max_body_bytes: MAX_GATEWAY_BODY_BYTES,
            request_timeout_seconds: 30,
        }
    }
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct ExistingConfigSlice {
    schema_version: Option<u32>,
    gateway: GatewaySettings,
}

impl Config {
    /// Load the gateway plus the shared MCP/IPC subset from the existing
    /// `config.toml` shape. Unrelated controller tables are ignored.
    pub fn load(path: Option<&Path>, data_override: Option<&Path>) -> Result<Self> {
        let frontend = swarm_mcp::Config::load(path, data_override)?;
        let mut gateway = if let Some(path) = path {
            let source = std::fs::read_to_string(path)?;
            let loaded: ExistingConfigSlice = toml::from_str(&source)
                .map_err(|error| Error::new("CONFIG_ERROR", error.to_string()))?;
            if loaded.schema_version.unwrap_or(1) != 1 {
                return Err(Error::new(
                    "CONFIG_ERROR",
                    "unsupported config schema version",
                ));
            }
            loaded.gateway
        } else {
            GatewaySettings::default()
        };

        let config_dir = std::env::current_dir()?.join(
            path.and_then(Path::parent)
                .unwrap_or_else(|| Path::new(".")),
        );
        gateway.resolve_paths(&config_dir);
        gateway.validate(&frontend)?;
        Ok(Self { frontend, gateway })
    }
}

impl GatewaySettings {
    fn resolve_paths(&mut self, config_dir: &Path) {
        for path in [&mut self.credential_file, &mut self.local_bearer_file]
            .into_iter()
            .flatten()
        {
            if path.is_relative() {
                *path = config_dir.join(&*path);
            }
        }
    }

    fn validate(&self, frontend: &swarm_mcp::Config) -> Result<()> {
        let ipc: &IpcConfig = &frontend.ipc;
        let bind = self
            .bind
            .parse::<std::net::SocketAddr>()
            .map_err(|_| Error::new("CONFIG_ERROR", "gateway.bind must be a socket address"))?;
        if !bind.ip().is_loopback() {
            return Err(Error::new(
                "CONFIG_ERROR",
                "gateway.bind must use a loopback address",
            ));
        }
        if self.enabled {
            if self.max_body_bytes < 1024
                || self.max_body_bytes > MAX_GATEWAY_BODY_BYTES
                || self.max_body_bytes > ipc.max_frame_bytes
                || self.request_timeout_seconds == 0
                || self.request_timeout_seconds > 300
            {
                return Err(Error::new(
                    "CONFIG_ERROR",
                    "gateway body limit must be 1 KiB through the IPC frame limit and request timeout must be 1 through 300 seconds",
                ));
            }
            let profile = frontend.mcp.profiles.get(&self.profile).ok_or_else(|| {
                Error::new(
                    "CONFIG_ERROR",
                    "enabled gateway.profile must name a configured MCP profile",
                )
            })?;
            if profile.tool_profile == swarm_mcp::config::McpToolProfile::Full {
                return Err(Error::new(
                    "CONFIG_ERROR",
                    "the gateway cannot use the full MCP profile",
                ));
            }
            let credential_file = self
                .credential_file
                .as_ref()
                .filter(|path| !path.as_os_str().is_empty());
            let bearer_file = self
                .local_bearer_file
                .as_ref()
                .filter(|path| !path.as_os_str().is_empty());
            if credential_file.is_none() || bearer_file.is_none() {
                return Err(Error::new(
                    "CONFIG_ERROR",
                    "enabled gateway requires credential_file and local_bearer_file",
                ));
            }
            if credential_file == bearer_file {
                return Err(Error::new(
                    "CONFIG_ERROR",
                    "gateway credential_file and local_bearer_file must be different files",
                ));
            }
        }
        Ok(())
    }
}
