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
    /// Explicit operator-authorized OpenCode auth.json sources. References
    /// are usable by owned routes but this private source map is never emitted
    /// by Config serialization or copied into Store state.
    #[serde(default, skip_serializing)]
    pub opencode_provider_auth_sources: BTreeMap<String, OwnedProviderAuthSourceConfig>,
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
pub use swarm_client::IpcConfig as Ipc;
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
    /// Explicit fresh foreground service declaration. External HTTP options
    /// remain unchanged; this declaration grants no process-start authority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owned_service: Option<OwnedOpenCodeServiceConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnedOpenCodeServiceConfig {
    pub origin: String,
    pub service_id: String,
    pub model: crate::runtime::opencode_v2::ModelRef,
    /// Explicit choice; offline qualification does not enable a live catalog.
    pub model_catalog: String,
    /// Opaque reference into `Config::opencode_provider_auth_sources`.
    /// Absent means no provider credential is resolved or bootstrapped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_ref: Option<String>,
    pub bun_executable: PathBuf,
    pub bun_sha256: String,
    pub server_program: PathBuf,
    pub server_program_sha256: String,
    pub state_root: PathBuf,
    pub port: u16,
}

/// Host-only mapping from an opaque reference to one explicitly authorized
/// source file. The source path never leaves local Config resolution.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnedProviderAuthSourceConfig {
    pub provider_id: String,
    #[serde(skip_serializing)]
    pub auth_file: PathBuf,
}

impl std::fmt::Debug for OwnedProviderAuthSourceConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OwnedProviderAuthSourceConfig")
            .field("provider_id", &self.provider_id)
            .field("auth_file", &"<redacted>")
            .finish()
    }
}

impl Route {
    pub(crate) fn owned_opencode_service(
        &self,
    ) -> Result<Option<crate::runtime::opencode_v2::owned_service::OwnedServiceRoute>> {
        let Some(definition) = &self.owned_service else {
            return Ok(None);
        };
        if self.runtime != crate::runtime::opencode_v2::RUNTIME
            || self.module_artifact_id != crate::runtime::opencode_v2::ARTIFACT_ID
        {
            return Err(Error::new(
                "CONFIG_ERROR",
                "owned service requires an OpenCode V2 route",
            ));
        }
        crate::runtime::opencode_v2::owned_service::OwnedServiceRoute::from_config(definition)
            .map(Some)
    }
}

impl Config {
    /// Resolve an exact route reference to its locally configured auth source.
    /// This returns a host-only path for the credential reader; callers must
    /// never persist or serialize it.
    pub(crate) fn opencode_provider_auth_source(
        &self,
        credential_ref: &str,
        model: &crate::runtime::opencode_v2::ModelRef,
    ) -> Result<Option<PathBuf>> {
        validate_provider_credential_ref(credential_ref)?;
        if model.provider_id != "opencode-go" {
            return Err(Error::new(
                "CONFIG_ERROR",
                "owned provider credentials are restricted to opencode-go",
            ));
        }
        let source = self
            .opencode_provider_auth_sources
            .get(credential_ref)
            .ok_or_else(|| {
                Error::new(
                    "CONFIG_ERROR",
                    "owned provider credential reference is not configured",
                )
            })?;
        validate_provider_auth_source(credential_ref, source)?;
        Ok(Some(source.auth_file.clone()))
    }

    fn validate_opencode_provider_auth_sources(&self) -> Result<()> {
        for (credential_ref, source) in &self.opencode_provider_auth_sources {
            validate_provider_auth_source(credential_ref, source)?;
        }
        Ok(())
    }
}

fn validate_provider_auth_source(
    credential_ref: &str,
    source: &OwnedProviderAuthSourceConfig,
) -> Result<()> {
    validate_provider_credential_ref(credential_ref)?;
    if source.provider_id != "opencode-go"
        || !source.auth_file.is_absolute()
        || source.auth_file.file_name().and_then(|name| name.to_str()) != Some("auth.json")
        || source.auth_file.components().any(|component| {
            matches!(
                component,
                std::path::Component::CurDir | std::path::Component::ParentDir
            )
        })
    {
        return Err(Error::new(
            "CONFIG_ERROR",
            "owned provider auth source must be an absolute opencode-go auth.json path",
        ));
    }
    Ok(())
}

fn validate_provider_credential_ref(credential_ref: &str) -> Result<()> {
    if credential_ref.is_empty()
        || credential_ref.len() > 128
        || !credential_ref
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Err(Error::new(
            "CONFIG_ERROR",
            "owned provider credential reference is malformed",
        ));
    }
    Ok(())
}
impl Default for Storage {
    fn default() -> Self {
        Self {
            data_dir: default_data_dir(),
            queue_capacity: 256,
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
            opencode_provider_auth_sources: BTreeMap::new(),
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
        cfg.validate_opencode_provider_auth_sources()?;
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
            if let Some(owned) = &r.owned_service {
                r.owned_opencode_service()?;
                if let Some(credential_ref) = owned.credential_ref.as_deref() {
                    cfg.opencode_provider_auth_source(credential_ref, &owned.model)?;
                }
            }
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
            if let Some(owned) = route.owned_opencode_service()? {
                let key = (owned.state_root().to_path_buf(), owned.version().to_owned());
                if services
                    .insert(owned.service_id().to_owned(), key.clone())
                    .is_some_and(|old| old != key)
                {
                    return Err(Error::new(
                        "CONFIG_ERROR",
                        "owned service namespace must use one private state root",
                    ));
                }
                continue;
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
