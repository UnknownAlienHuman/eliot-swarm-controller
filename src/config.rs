use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

const MAX_GATEWAY_BODY_BYTES: usize = 1_048_576;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub schema_version: u32,
    pub storage: Storage,
    pub ipc: Ipc,
    pub routes: Vec<Route>,
    pub checks: crate::checks::model::CheckConfig,
    pub mcp: McpConfig,
    pub gateway: GatewayConfig,
    pub forge: crate::forge::ForgeConfig,
    pub workspace: crate::workspace::WorkspaceConfig,
    pub schedules: Vec<crate::scheduler::ScheduleConfig>,
}

/// Closed MCP method surfaces. A profile never changes the ELIOT role carried
/// by the selected credential; the application checks that role separately.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpToolProfile {
    Observer,
    Reviewer,
    Participant,
    AssignedReviewer,
    Manager,
    Gm,
    Full,
}

/// One local, named binding between an MCP tool profile and its ELIOT client.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpProfileConfig {
    pub tool_profile: McpToolProfile,
    pub expected_client_id: String,
    /// Named presentation surface; absent means the profile's small role core.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub surface: Option<String>,
    /// Authorized catalog groups selected for this session's initial view.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deferred_groups: Vec<String>,
    /// Exact ManualOnly methods selected for this session's initial view.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub manual_tools: Vec<String>,
}

/// MCP profile selection is local configuration; the selected name and
/// client binding are fixed when `swarm mcp` starts.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct McpConfig {
    pub default_profile: String,
    pub profiles: BTreeMap<String, McpProfileConfig>,
}

/// Optional loopback Streamable HTTP facade. Its ELIOT principal and MCP
/// profile are selected only by this local configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GatewayConfig {
    pub enabled: bool,
    pub bind: String,
    pub profile: String,
    pub credential_file: Option<PathBuf>,
    pub local_bearer_file: Option<PathBuf>,
    pub max_body_bytes: usize,
    pub request_timeout_seconds: u64,
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
            mcp: McpConfig::default(),
            gateway: GatewayConfig::default(),
            forge: crate::forge::ForgeConfig::default(),
            workspace: crate::workspace::WorkspaceConfig::default(),
            schedules: Vec::new(),
        }
    }
}
impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind: "127.0.0.1:8787".into(),
            profile: "local-observer".into(),
            credential_file: None,
            local_bearer_file: None,
            max_body_bytes: 1_048_576,
            request_timeout_seconds: 30,
        }
    }
}
impl Default for McpConfig {
    fn default() -> Self {
        Self {
            default_profile: "local-observer".into(),
            profiles: BTreeMap::from([
                (
                    "local-observer".into(),
                    McpProfileConfig {
                        tool_profile: McpToolProfile::Observer,
                        expected_client_id: "operator".into(),
                        surface: None,
                        deferred_groups: Vec::new(),
                        manual_tools: Vec::new(),
                    },
                ),
                (
                    "local-full".into(),
                    McpProfileConfig {
                        tool_profile: McpToolProfile::Full,
                        expected_client_id: "operator".into(),
                        surface: None,
                        deferred_groups: Vec::new(),
                        manual_tools: Vec::new(),
                    },
                ),
            ]),
        }
    }
}

impl McpConfig {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.profiles.is_empty() || !self.profiles.contains_key(&self.default_profile) {
            return Err(Error::new(
                "CONFIG_ERROR",
                "MCP default_profile must name a configured profile",
            ));
        }
        let mut client_ids = BTreeMap::new();
        for (name, profile) in &self.profiles {
            if name.is_empty()
                || name.starts_with('-')
                || name.ends_with('-')
                || !name
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
                || profile.expected_client_id.trim().is_empty()
            {
                return Err(Error::new(
                    "CONFIG_ERROR",
                    "MCP profile names must be lowercase identifiers and expected_client_id must be non-empty",
                ));
            }
            if profile.surface.as_deref().is_some_and(|surface| {
                surface.is_empty()
                    || !surface.bytes().all(|byte| {
                        byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-'
                    })
            }) || !unique_mcp_labels(&profile.deferred_groups, false)
                || !unique_mcp_labels(&profile.manual_tools, true)
            {
                return Err(Error::new(
                    "CONFIG_ERROR",
                    "MCP surface, deferred groups, and exact manual tools must be unique lowercase identifiers",
                ));
            }
            // Full is the explicit local compatibility surface and may share
            // its local operator identity with the default observer profile.
            // Restricted named principals must remain one-to-one so Dot and
            // Muse cannot silently select a credential bound to the other.
            if profile.tool_profile != McpToolProfile::Full
                && client_ids
                    .insert(profile.expected_client_id.as_str(), name.as_str())
                    .is_some()
            {
                return Err(Error::new(
                    "CONFIG_ERROR",
                    "restricted MCP profiles must use distinct expected_client_id values",
                ));
            }
        }
        Ok(())
    }

    pub fn selected_tool_profile(
        &self,
        selected_name: Option<&str>,
        client_id: &str,
    ) -> Result<McpToolProfile> {
        let name = selected_name.unwrap_or(&self.default_profile);
        let profile = self
            .profiles
            .get(name)
            .ok_or_else(|| Error::new("CONFIG_ERROR", format!("unknown MCP profile {name:?}")))?;
        if profile.expected_client_id != client_id {
            return Err(Error::new(
                "PROFILE_MISMATCH",
                "selected MCP profile is not bound to this ELIOT client",
            ));
        }
        Ok(profile.tool_profile)
    }
}

fn unique_mcp_labels(values: &[String], allow_method_separators: bool) -> bool {
    let mut seen = BTreeMap::new();
    values.iter().all(|value| {
        !value.is_empty()
            && value.bytes().all(|byte| {
                byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || byte == b'-'
                    || (allow_method_separators && matches!(byte, b'.' | b'_'))
            })
            && seen.insert(value.as_str(), ()).is_none()
    })
}

impl GatewayConfig {
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

    fn validate(&self, mcp: &McpConfig, ipc: &Ipc) -> Result<()> {
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
            let profile = mcp.profiles.get(&self.profile).ok_or_else(|| {
                Error::new(
                    "CONFIG_ERROR",
                    "enabled gateway.profile must name a configured MCP profile",
                )
            })?;
            if profile.tool_profile == McpToolProfile::Full {
                return Err(Error::new(
                    "CONFIG_ERROR",
                    "the gateway cannot use the full MCP profile",
                ));
            }
            let credential_file = self
                .credential_file
                .as_ref()
                .filter(|p| !p.as_os_str().is_empty());
            let bearer_file = self
                .local_bearer_file
                .as_ref()
                .filter(|p| !p.as_os_str().is_empty());
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
        cfg.mcp.validate()?;
        let config_dir =
            std::env::current_dir()?.join(path.and_then(Path::parent).unwrap_or(Path::new(".")));
        cfg.gateway.resolve_paths(&config_dir);
        cfg.gateway.validate(&cfg.mcp, &cfg.ipc)?;
        cfg.forge.resolve_paths(&config_dir)?;
        cfg.forge.validate()?;
        cfg.workspace.resolve_paths(&config_dir)?;
        cfg.workspace.validate(&cfg.forge)?;
        crate::scheduler::validate_schedules(&cfg.schedules)?;
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
            if r.enabled
                && r.runtime == crate::runtime::codex::RUNTIME
                && r.module_artifact_id != crate::runtime::codex::ARTIFACT_ID
            {
                return Err(Error::new(
                    "CONFIG_ERROR",
                    "Codex controller routes require the exact .2 artifact; the .1 observer is standalone",
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
