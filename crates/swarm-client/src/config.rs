use serde::{Deserialize, Serialize};

/// Transport bounds shared by the host and local clients.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct IpcConfig {
    pub max_frame_bytes: usize,
    pub max_connections: usize,
    pub max_inflight_per_connection: usize,
    pub write_timeout_seconds: u64,
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
