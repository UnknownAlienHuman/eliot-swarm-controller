//! Exact local MCP/IPC configuration slice from the controller's existing
//! config.toml shape. Unrelated controller configuration keys are ignored;
//! the `mcp`, `ipc`, and `storage.data_dir` objects keep their existing names.

use serde::Deserialize;
use std::path::{Path, PathBuf};
use swarm_contracts::error::{Error, Result};

pub use swarm_client::IpcConfig as Ipc;
pub use swarm_contracts::mcp_frontend::{McpConfig, McpProfileConfig, McpToolProfile};

#[derive(Debug, Clone)]
pub struct Config {
    pub storage: Storage,
    pub ipc: Ipc,
    pub mcp: McpConfig,
}

#[derive(Debug, Clone)]
pub struct Storage {
    pub data_dir: PathBuf,
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct ExistingConfigSlice {
    schema_version: Option<u32>,
    storage: ExistingStorageSlice,
    ipc: Ipc,
    mcp: McpConfig,
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct ExistingStorageSlice {
    data_dir: Option<PathBuf>,
}

impl Config {
    pub fn load(path: Option<&Path>, data_override: Option<&Path>) -> Result<Self> {
        let mut loaded = if let Some(path) = path {
            let source = std::fs::read_to_string(path)?;
            let mut parsed: ExistingConfigSlice = toml::from_str(&source)
                .map_err(|error| Error::new("CONFIG_ERROR", error.to_string()))?;
            if parsed.schema_version.unwrap_or(1) != 1 {
                return Err(Error::new(
                    "CONFIG_ERROR",
                    "unsupported config schema version",
                ));
            }
            let mut data_dir = parsed
                .storage
                .data_dir
                .take()
                .unwrap_or_else(default_data_dir);
            if data_dir.is_relative() {
                data_dir = path.parent().unwrap_or(Path::new(".")).join(data_dir);
            }
            Self {
                storage: Storage { data_dir },
                ipc: parsed.ipc,
                mcp: parsed.mcp,
            }
        } else {
            Self {
                storage: Storage {
                    data_dir: default_data_dir(),
                },
                ipc: Ipc::default(),
                mcp: McpConfig::default(),
            }
        };

        if let Some(data_dir) = data_override {
            loaded.storage.data_dir = data_dir.to_path_buf();
        }
        if loaded.storage.data_dir.is_relative() {
            loaded.storage.data_dir = std::env::current_dir()?.join(&loaded.storage.data_dir);
        }
        loaded.validate()?;
        Ok(loaded)
    }

    pub fn validate(&self) -> Result<()> {
        if self.ipc.max_connections == 0
            || self.ipc.max_inflight_per_connection == 0
            || self.ipc.max_frame_bytes < 1024
            || self.ipc.write_timeout_seconds == 0
        {
            return Err(Error::new(
                "CONFIG_ERROR",
                "invalid IPC limits in frontend configuration",
            ));
        }
        self.mcp.validate()
    }
}

fn default_data_dir() -> PathBuf {
    if let Some(root) = std::env::var_os(if cfg!(windows) {
        "LOCALAPPDATA"
    } else {
        "XDG_STATE_HOME"
    }) {
        PathBuf::from(root).join("eliot-swarm-controller")
    } else if let Some(home) = std::env::var_os(if cfg!(windows) { "USERPROFILE" } else { "HOME" })
    {
        PathBuf::from(home).join(".local/state/eliot-swarm-controller")
    } else {
        PathBuf::from(".swarm-controller")
    }
}
