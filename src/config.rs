use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub schema_version: u32,
    pub storage: Storage,
    pub ipc: Ipc,
    pub routes: Vec<Route>,
    pub checks: crate::checks::model::CheckConfig,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Storage {
    pub data_dir: PathBuf,
    pub queue_capacity: usize,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Ipc {
    pub max_frame_bytes: usize,
    pub max_connections: usize,
    pub max_inflight_per_connection: usize,
    pub write_timeout_seconds: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Route {
    pub alias: String,
    pub runtime: String,
    pub module_artifact_id: String,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub native_options: Value,
}
impl Default for Storage {
    fn default() -> Self {
        Self {
            data_dir: default_data_dir(),
            queue_capacity: 256,
        }
    }
}
impl Default for Ipc {
    fn default() -> Self {
        Self {
            max_frame_bytes: 1_048_576,
            max_connections: 256,
            max_inflight_per_connection: 8,
            write_timeout_seconds: 15,
        }
    }
}
impl Default for Config {
    fn default() -> Self {
        Self {
            schema_version: 1,
            storage: Storage::default(),
            ipc: Ipc::default(),
            routes: Vec::new(),
            checks: crate::checks::model::CheckConfig::default(),
        }
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
impl Config {
    pub fn load(path: Option<&Path>, data_override: Option<&Path>) -> Result<Self> {
        let mut cfg = if let Some(path) = path {
            let source = std::fs::read_to_string(path)?;
            let mut cfg: Self =
                toml::from_str(&source).map_err(|e| Error::new("CONFIG_ERROR", e.to_string()))?;
            if cfg.storage.data_dir.is_relative() {
                cfg.storage.data_dir = path
                    .parent()
                    .unwrap_or(Path::new("."))
                    .join(&cfg.storage.data_dir);
            }
            cfg
        } else {
            Self::default()
        };
        if let Some(dir) = data_override {
            cfg.storage.data_dir = dir.to_path_buf();
        }
        if cfg.storage.data_dir.is_relative() {
            cfg.storage.data_dir = std::env::current_dir()?.join(&cfg.storage.data_dir);
        }
        if cfg.schema_version != 1
            || cfg.storage.queue_capacity == 0
            || cfg.ipc.max_connections == 0
            || cfg.ipc.max_inflight_per_connection == 0
            || cfg.ipc.max_frame_bytes < 1024
            || cfg.ipc.write_timeout_seconds == 0
        {
            return Err(Error::new(
                "CONFIG_ERROR",
                "unsupported version or invalid IPC/queue capacity",
            ));
        }
        let mut aliases = std::collections::BTreeSet::new();
        for r in &cfg.routes {
            if r.alias.trim().is_empty()
                || r.runtime.trim().is_empty()
                || r.module_artifact_id.trim().is_empty()
                || !aliases.insert(&r.alias)
            {
                return Err(Error::new(
                    "CONFIG_ERROR",
                    "route aliases must be unique; runtime and artifact are required",
                ));
            }
        }
        let mut services = std::collections::BTreeMap::new();
        let mut records = std::collections::BTreeMap::new();
        for route in cfg
            .routes
            .iter()
            .filter(|r| r.enabled && r.runtime == crate::runtime::opencode_v2::RUNTIME)
        {
            if route.module_artifact_id != crate::runtime::opencode_v2::ARTIFACT_ID {
                return Err(Error::new(
                    "CONFIG_ERROR",
                    "unsupported builtin OpenCode module artifact",
                ));
            }
            let options = crate::runtime::opencode_v2::Options::parse(&route.native_options)?;
            if records
                .insert(options.connection_file.clone(), options.service_id.clone())
                .is_some_and(|old| old != options.service_id)
            {
                return Err(Error::new(
                    "CONFIG_ERROR",
                    "one connection record must have one service namespace",
                ));
            }
            let key = (
                options.connection_file.clone(),
                options.expected_version.clone(),
            );
            if services
                .insert(options.service_id, key.clone())
                .is_some_and(|old| old != key)
            {
                return Err(Error::new(
                    "CONFIG_ERROR",
                    "one OpenCode service ID must use one connection record and version",
                ));
            }
        }
        cfg.checks.validate()?;
        Ok(cfg)
    }
    pub fn route(&self, alias: &str) -> Result<Route> {
        let r = self
            .routes
            .iter()
            .find(|r| r.alias == alias)
            .ok_or_else(|| Error::new("UNKNOWN_ROUTE", alias))?;
        if !r.enabled {
            return Err(Error::new("ROUTE_DISABLED", alias));
        }
        Ok(r.clone())
    }
}
