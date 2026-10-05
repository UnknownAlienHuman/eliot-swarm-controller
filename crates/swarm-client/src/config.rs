use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use swarm_contracts::error::{Error, Result};

/// Transport bounds shared by the host and local clients.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct IpcConfig {
    pub max_frame_bytes: usize,
    pub max_connections: usize,
    pub max_inflight_per_connection: usize,
    pub write_timeout_seconds: u64,
}

/// Versioned bootstrap connection data materialized by the trusted host for
/// an owned module launch. This contains transport location and bounds only;
/// binding credentials and owner identity are delivered through the protected
/// module-owner environment after process admission.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostConnectionConfig {
    pub schema_version: u32,
    pub host_data_dir: PathBuf,
    #[serde(default)]
    pub ipc: IpcConfig,
}

impl HostConnectionConfig {
    pub fn validate(&self) -> Result<()> {
        if self.schema_version != 1
            || !self.host_data_dir.is_absolute()
            || self.host_data_dir.to_str().is_none()
            || self.host_data_dir.as_os_str().is_empty()
        {
            return Err(Error::new(
                "MODULE_HOST_CONFIG_INVALID",
                "module host connection config requires schema 1 and an absolute Unicode IPC root",
            ));
        }
        Ok(())
    }
}

impl Default for IpcConfig {
    fn default() -> Self {
        Self {
            max_frame_bytes: 1_048_576,
            max_connections: 256,
            max_inflight_per_connection: 8,
            write_timeout_seconds: 15,
        }
    }
}
