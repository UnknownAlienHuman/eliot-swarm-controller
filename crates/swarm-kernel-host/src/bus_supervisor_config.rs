//! Optional local pin for the one managed-bus lifecycle coordinator.
//!
//! Enabling this actor creates no service process by itself. A process can be
//! started only for an explicitly managed, currently ready Store registration.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BusSupervisorConfig {
    pub enabled: bool,
    /// Absolute dispatcher path after config-relative path resolution.
    pub dispatcher_executable: Option<PathBuf>,
    /// Operator-pinned SHA-256 of the dispatcher executable bytes.
    pub dispatcher_sha256: Option<String>,
}

impl Default for BusSupervisorConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            dispatcher_executable: None,
            dispatcher_sha256: None,
        }
    }
}

impl BusSupervisorConfig {
    pub(crate) fn resolve_paths(&mut self, config_dir: &Path) {
        if let Some(path) = self.dispatcher_executable.as_mut()
            && path.is_relative()
        {
            *path = config_dir.join(&*path);
        }
    }
}
