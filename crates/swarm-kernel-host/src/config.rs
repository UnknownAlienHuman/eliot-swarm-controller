pub use crate::bus_supervisor_config::BusSupervisorConfig;
use crate::error::{Error, Result};
pub use crate::module_supervisor_config::{ModuleRouteConfigMapper, ModuleSupervisorConfig};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};
pub use swarm_contracts::mcp_frontend::{McpConfig, McpProfileConfig, McpToolProfile};

const MAX_GATEWAY_BODY_BYTES: usize = 1_048_576;
// Stable route ID; Store selection pins the concrete descriptor version for new bindings.
const CODEX_RUST_ARTIFACT_ID: &str = "codex-rust-controller.1";
pub(crate) const OPENCODE_RUST_ARTIFACT_ID: &str = "eliot-opencode-v2.rust-http.1";
const COMMAND_RUST_ARTIFACT_ID: &str = "eliot-command.rust-headless.1";
const COMMAND_ACP_ARTIFACT_ID: &str = "eliot-command.acp-rust.1";
const ANTIGRAVITY_RUST_ARTIFACT_ID: &str = "eliot-antigravity.rust-headless.1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OpenCodeRouteKind {
    Builtin,
    Standalone,
}

pub(crate) fn opencode_route_kind(runtime: &str, artifact_id: &str) -> Option<OpenCodeRouteKind> {
    if runtime == crate::runtime::opencode_v2::RUNTIME
        && (artifact_id == crate::runtime::opencode_v2::ARTIFACT_ID
            || artifact_id == crate::runtime::opencode_v2::TASK_PROMPT_ARTIFACT_ID)
    {
        Some(OpenCodeRouteKind::Builtin)
    } else if runtime == "module" && artifact_id == OPENCODE_RUST_ARTIFACT_ID {
        Some(OpenCodeRouteKind::Standalone)
    } else {
        None
    }
}

/// The current built-in OpenCode artifact uses the Store-owned TaskPrompt
/// contract. The retained `.1` decoder intentionally does not select it.
pub(crate) fn is_task_prompt_builtin_route(runtime: &str, artifact_id: &str) -> bool {
    runtime == crate::runtime::opencode_v2::RUNTIME
        && artifact_id == crate::runtime::opencode_v2::TASK_PROMPT_ARTIFACT_ID
}

/// The built-in OpenCode and Zed decoders retain their bounded selector-less
/// route contract. Every other runtime/artifact topology is module-backed and
/// requires an exact trusted descriptor for new work.
pub(crate) fn is_selectorless_builtin_route(runtime: &str, artifact_id: &str) -> bool {
    opencode_route_kind(runtime, artifact_id) == Some(OpenCodeRouteKind::Builtin)
        || (runtime == crate::runtime::zed::RUNTIME
            && (artifact_id == crate::runtime::zed::ARTIFACT_ID
                || artifact_id == crate::runtime::zed::LEGACY_ARTIFACT_ID))
}

/// Trusted local recorder settings. This controls optional diagnostic
/// metadata and explicitly selected redacted text; it never disables or
/// redirects Store/business receipts.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ObservabilityConfig {
    pub enabled: bool,
    /// Relative paths resolve against the controller config file. When absent,
    /// the host uses `<storage.data_dir>/diagnostics`.
    #[serde(default, skip_serializing)]
    pub directory: Option<PathBuf>,
    /// Optional pinned JSON settings for live severity/category filters and
    /// retention; read only after the lazy recorder starts.
    #[serde(default, skip_serializing)]
    pub live_config_file: Option<PathBuf>,
    pub queue_records: usize,
    pub queue_bytes: usize,
    pub max_record_bytes: usize,
    pub file_segment_bytes: u64,
    pub retention_bytes: u64,
    pub retention_days: u64,
}

/// Optional operator-pinned standalone ScriptRun process adapter. A selected
/// pin is copied into each new immutable Work receipt; old runs retain their
/// legacy backend discriminator through upgrade.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ScriptConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor: Option<swarm_script_worker::ExecutorPin>,
}

/// Explicit opt-in for the independent Store-backed scheduler process. The
/// built-in scheduler remains the compatibility and recovery path when this
/// is absent or disabled.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct AutomationSchedulerConfig {
    pub enabled: bool,
}

impl ScriptConfig {
    pub fn validate(&self) -> Result<()> {
        if let Some(executor) = &self.executor {
            swarm_script_worker::validate_executor_pin(executor)
                .map_err(|error| Error::new("CONFIG_ERROR", error.message))?;
        }
        Ok(())
    }
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            directory: None,
            live_config_file: None,
            queue_records: 256,
            queue_bytes: 8_388_608,
            max_record_bytes: 65_536,
            file_segment_bytes: 16_777_216,
            retention_bytes: 134_217_728,
            retention_days: 7,
        }
    }
}

impl ObservabilityConfig {
    pub fn recording_directory(&self, data_dir: &Path) -> PathBuf {
        self.directory
            .clone()
            .unwrap_or_else(|| data_dir.join("diagnostics"))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub schema_version: u32,
    pub storage: Storage,
    pub observability: ObservabilityConfig,
    pub ipc: Ipc,
    pub routes: Vec<Route>,
    /// Explicit operator-authorized OpenCode auth.json sources. References
    /// are usable by owned routes but this private source map is never emitted
    /// by Config serialization or copied into Store state.
    #[serde(default, skip_serializing)]
    pub opencode_provider_auth_sources: BTreeMap<String, OwnedProviderAuthSourceConfig>,
    #[serde(default)]
    pub scripts: ScriptConfig,
    pub checks: crate::checks::model::CheckConfig,
    pub mcp: McpConfig,
    pub gateway: GatewayConfig,
    pub forge: crate::forge::ForgeConfig,
    pub workspace: crate::workspace::WorkspaceConfig,
    pub schedules: Vec<crate::scheduler::ScheduleConfig>,
    /// Independent Rust worker stays dormant unless explicitly enabled and
    /// the Store reports a current scheduler due/future fact.
    #[serde(default)]
    pub automation_scheduler: AutomationSchedulerConfig,
    /// Optional module failures isolate the actor while the Store remains available.
    pub module_supervisor: ModuleSupervisorConfig,
    /// Optional managed bus lifecycle selected by trusted local configuration.
    pub bus_supervisor: BusSupervisorConfig,
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
    /// Native option key to receive the exact admitted workspace path. New
    /// standalone module artifacts declare this in their route contract.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_option: Option<String>,
    /// Explicit fresh foreground service declaration. External HTTP options
    /// remain unchanged; this declaration grants no process-start authority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owned_service: Option<OwnedOpenCodeServiceConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admission_policy: Option<RouteAdmissionPolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteAdmissionPolicy {
    pub max_concurrent_roots: u16,
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
    /// Preserve the workspace-field convention of routes that predate the
    /// descriptor-selected module contract. New selected descriptors supply
    /// their RFC 6901 native-options pointer through the module contract; this
    /// helper is only the bounded compatibility translation for an unselected
    /// legacy route.
    pub(crate) fn compatibility_workspace_option(&self) -> Option<&'static str> {
        match self.runtime.as_str() {
            crate::runtime::opencode_v2::RUNTIME => Some("directory"),
            "zed" => Some("workdir"),
            "codex" | "command" | "claude" | "antigravity" | "muse" => Some("workspaceRoot"),
            _ => None,
        }
    }

    fn validate_activation_contract(&self) -> Result<()> {
        if self
            .admission_policy
            .as_ref()
            .is_some_and(|policy| policy.max_concurrent_roots == 0)
        {
            return Err(Error::new(
                "CONFIG_ERROR",
                "route max_concurrent_roots must be positive",
            ));
        }
        if self.workspace_option.as_deref().is_some_and(|field| {
            let mut bytes = field.bytes();
            let first = bytes.next();
            field.len() > 128
                || !first.is_some_and(|byte| byte == b'_' || byte.is_ascii_alphabetic())
                || !bytes.all(|byte| byte == b'_' || byte.is_ascii_alphanumeric())
        }) {
            return Err(Error::new(
                "CONFIG_ERROR",
                "workspace_option must be a bounded native-options field name",
            ));
        }

        if self.runtime == crate::runtime::zed::RUNTIME
            && self.module_artifact_id != crate::runtime::zed::ARTIFACT_ID
            && self.module_artifact_id != crate::runtime::zed::LEGACY_ARTIFACT_ID
        {
            return Err(Error::new(
                "CONFIG_ERROR",
                "Zed routes require the current or explicitly retained legacy artifact",
            ));
        }

        let (runtime, workspace_field) = match self.module_artifact_id.as_str() {
            CODEX_RUST_ARTIFACT_ID => ("codex", "workspaceRoot"),
            OPENCODE_RUST_ARTIFACT_ID => ("module", "directory"),
            COMMAND_RUST_ARTIFACT_ID | COMMAND_ACP_ARTIFACT_ID => ("command", "workspaceRoot"),
            ANTIGRAVITY_RUST_ARTIFACT_ID => ("antigravity", "workspaceRoot"),
            // Generic module routes get their exact launch contract from the
            // selected trusted descriptor, not an artifact-ID allowlist here.
            _ if self.runtime == "module" => return Ok(()),
            _ if self.workspace_option.is_some() => {
                return Err(Error::new(
                    "CONFIG_ERROR",
                    "workspace_option is supported only by the exact standalone Rust adapter artifacts",
                ));
            }
            _ => return Ok(()),
        };
        if self.runtime != runtime || self.workspace_option.as_deref() != Some(workspace_field) {
            return Err(Error::new(
                "CONFIG_ERROR",
                "standalone adapter route must retain its exact runtime and workspace option",
            ));
        }

        match self.module_artifact_id.as_str() {
            CODEX_RUST_ARTIFACT_ID => validate_codex_rust_options(&self.native_options),
            OPENCODE_RUST_ARTIFACT_ID => {
                if self.runtime == "module" && self.owned_service.is_some() {
                    validate_owned_opencode_rust_options(&self.native_options)
                } else {
                    validate_opencode_rust_options(&self.native_options)
                }
            }
            COMMAND_RUST_ARTIFACT_ID | COMMAND_ACP_ARTIFACT_ID => {
                validate_command_rust_options(&self.native_options)
            }
            ANTIGRAVITY_RUST_ARTIFACT_ID => validate_antigravity_rust_options(&self.native_options),
            _ => Ok(()),
        }
    }

    pub(crate) fn owned_opencode_service(
        &self,
    ) -> Result<Option<crate::runtime::opencode_v2::owned_service::OwnedServiceRoute>> {
        let Some(definition) = &self.owned_service else {
            return Ok(None);
        };
        if opencode_route_kind(&self.runtime, &self.module_artifact_id).is_none() {
            return Err(Error::new(
                "CONFIG_ERROR",
                "owned service requires an OpenCode V2 route",
            ));
        }
        crate::runtime::opencode_v2::owned_service::OwnedServiceRoute::from_config(definition)
            .map(Some)
    }
}

fn validate_codex_rust_options(value: &Value) -> Result<()> {
    let options = exact_option_object(value, &["modelProvider", "model", "workspaceRoot"], &[])?;
    required_option_string(options, "modelProvider", 256)?;
    required_option_string(options, "model", 256)?;
    required_absolute_path(options, "workspaceRoot", 32 * 1024)?;
    Ok(())
}

fn validate_opencode_rust_options(value: &Value) -> Result<()> {
    let options = exact_option_object(
        value,
        &["service_id", "connection_file", "directory", "model"],
        &[],
    )?;
    let service_id = required_option_string(options, "service_id", 128)?;
    if !service_id
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(Error::new("CONFIG_ERROR", "OpenCode service_id is invalid"));
    }
    required_absolute_path(options, "connection_file", 4096)?;
    required_absolute_path(options, "directory", 4096)?;
    let model = options
        .get("model")
        .and_then(Value::as_object)
        .ok_or_else(|| Error::new("CONFIG_ERROR", "OpenCode model must be an object"))?;
    ensure_exact_keys(model, &["id", "providerID", "variant"], &[])?;
    required_option_string(model, "id", 256)?;
    required_option_string(model, "providerID", 256)?;
    required_option_string(model, "variant", 256)?;
    Ok(())
}

fn validate_owned_opencode_rust_options(value: &Value) -> Result<()> {
    let options = exact_option_object(value, &["directory"], &[])?;
    required_absolute_path(options, "directory", 4096)?;
    Ok(())
}

fn validate_command_rust_options(value: &Value) -> Result<()> {
    let options = exact_option_object(value, &["modelId", "workspaceRoot"], &[])?;
    required_option_string(options, "modelId", 256)?;
    required_absolute_path(options, "workspaceRoot", 32 * 1024)?;
    Ok(())
}

fn validate_antigravity_rust_options(value: &Value) -> Result<()> {
    let options = exact_option_object(
        value,
        &["modelId", "workspaceRoot"],
        &["reasoningEffort", "agent", "dangerouslySkipPermissions"],
    )?;
    required_option_string(options, "modelId", 256)?;
    required_absolute_path(options, "workspaceRoot", 32 * 1024)?;
    if let Some(value) = options.get("reasoningEffort") {
        let effort = option_string(value, "reasoningEffort", 32)?;
        if !matches!(effort, "low" | "medium" | "high") {
            return Err(Error::new(
                "CONFIG_ERROR",
                "Antigravity reasoningEffort must be low, medium, or high",
            ));
        }
    }
    if let Some(value) = options.get("agent") {
        option_string(value, "agent", 256)?;
    }
    if options
        .get("dangerouslySkipPermissions")
        .is_some_and(|value| !value.is_boolean())
    {
        return Err(Error::new(
            "CONFIG_ERROR",
            "Antigravity dangerouslySkipPermissions must be boolean",
        ));
    }
    Ok(())
}

fn exact_option_object<'a>(
    value: &'a Value,
    required: &[&str],
    optional: &[&str],
) -> Result<&'a serde_json::Map<String, Value>> {
    let options = value
        .as_object()
        .ok_or_else(|| Error::new("CONFIG_ERROR", "adapter native_options must be an object"))?;
    ensure_exact_keys(options, required, optional)?;
    Ok(options)
}

fn ensure_exact_keys(
    options: &serde_json::Map<String, Value>,
    required: &[&str],
    optional: &[&str],
) -> Result<()> {
    if required.iter().any(|key| !options.contains_key(*key))
        || options
            .keys()
            .any(|key| !required.contains(&key.as_str()) && !optional.contains(&key.as_str()))
    {
        return Err(Error::new(
            "CONFIG_ERROR",
            "adapter native_options omit required values or contain unsupported fields",
        ));
    }
    Ok(())
}

fn required_option_string<'a>(
    options: &'a serde_json::Map<String, Value>,
    key: &str,
    maximum_bytes: usize,
) -> Result<&'a str> {
    let value = options
        .get(key)
        .ok_or_else(|| Error::new("CONFIG_ERROR", "adapter native option is missing"))?;
    option_string(value, key, maximum_bytes)
}

fn option_string<'a>(value: &'a Value, key: &str, maximum_bytes: usize) -> Result<&'a str> {
    let value = value
        .as_str()
        .filter(|value| {
            !value.trim().is_empty()
                && value.len() <= maximum_bytes
                && !value.chars().any(char::is_control)
        })
        .ok_or_else(|| {
            Error::new(
                "CONFIG_ERROR",
                format!("adapter native option {key} is invalid"),
            )
        })?;
    Ok(value)
}

fn required_absolute_path(
    options: &serde_json::Map<String, Value>,
    key: &str,
    maximum_bytes: usize,
) -> Result<()> {
    let value = required_option_string(options, key, maximum_bytes)?;
    if !Path::new(value).is_absolute() {
        return Err(Error::new(
            "CONFIG_ERROR",
            format!("adapter native option {key} must be an absolute path"),
        ));
    }
    Ok(())
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
        validate_provider_id(&model.provider_id)?;
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
        if source.provider_id != model.provider_id {
            return Err(Error::new(
                "CONFIG_ERROR",
                "owned provider auth source does not match the admitted model provider",
            ));
        }
        Ok(Some(source.auth_file.clone()))
    }

    fn validate_opencode_provider_auth_sources(&self) -> Result<()> {
        for (credential_ref, source) in &self.opencode_provider_auth_sources {
            validate_provider_auth_source(credential_ref, source)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod owned_opencode_route_validation_tests {
    use super::{
        OPENCODE_RUST_ARTIFACT_ID, OpenCodeRouteKind, OwnedOpenCodeServiceConfig, Route,
        opencode_route_kind,
    };
    use serde_json::{Value, json};

    fn absolute_fixture_path(name: &str) -> String {
        std::env::temp_dir()
            .join(name)
            .to_string_lossy()
            .into_owned()
    }

    fn owned_service() -> OwnedOpenCodeServiceConfig {
        OwnedOpenCodeServiceConfig {
            origin: "fresh_owned_service".to_owned(),
            service_id: "native-prereq-opencode".to_owned(),
            model: crate::runtime::opencode_v2::ModelRef {
                id: "step-5-preview-free".to_owned(),
                provider_id: "opencode".to_owned(),
                variant: "high".to_owned(),
            },
            model_catalog: "refresh".to_owned(),
            credential_ref: None,
            bun_executable: std::env::temp_dir().join("fixture-bun.exe"),
            bun_sha256: "a".repeat(64),
            server_program: std::env::temp_dir().join("fixture-serve.mjs"),
            server_program_sha256: "b".repeat(64),
            state_root: std::env::temp_dir().join("fixture-owned-state"),
            port: 0,
        }
    }

    fn route(native_options: Value, owned_service: Option<OwnedOpenCodeServiceConfig>) -> Route {
        Route {
            alias: "native-prereq-opencode".to_owned(),
            runtime: "module".to_owned(),
            module_artifact_id: OPENCODE_RUST_ARTIFACT_ID.to_owned(),
            enabled: true,
            native_options,
            workspace_option: Some("directory".to_owned()),
            owned_service,
            admission_policy: None,
        }
    }

    #[test]
    fn opencode_route_kind_accepts_only_exact_runtime_artifact_pairs() {
        let cases = [
            (
                crate::runtime::opencode_v2::RUNTIME,
                crate::runtime::opencode_v2::ARTIFACT_ID,
                Some(OpenCodeRouteKind::Builtin),
            ),
            (
                crate::runtime::opencode_v2::RUNTIME,
                crate::runtime::opencode_v2::TASK_PROMPT_ARTIFACT_ID,
                Some(OpenCodeRouteKind::Builtin),
            ),
            (
                "module",
                OPENCODE_RUST_ARTIFACT_ID,
                Some(OpenCodeRouteKind::Standalone),
            ),
            (
                crate::runtime::opencode_v2::RUNTIME,
                OPENCODE_RUST_ARTIFACT_ID,
                None,
            ),
            ("module", crate::runtime::opencode_v2::ARTIFACT_ID, None),
            (
                "module",
                crate::runtime::opencode_v2::TASK_PROMPT_ARTIFACT_ID,
                None,
            ),
            ("module", "eliot-opencode-v2.rust-http.1.extra", None),
            ("MODULE", OPENCODE_RUST_ARTIFACT_ID, None),
            ("unknown", "unknown-artifact", None),
        ];

        for (runtime, artifact_id, expected) in cases {
            assert_eq!(
                opencode_route_kind(runtime, artifact_id),
                expected,
                "unexpected classification for runtime={runtime:?}, artifact={artifact_id:?}"
            );
        }
    }

    #[test]
    fn documented_fresh_owned_route_accepts_directory_only_and_keeps_its_pin() {
        let directory = absolute_fixture_path("owned-opencode-workspace");
        let route = route(json!({"directory": directory}), Some(owned_service()));

        assert!(route.validate_activation_contract().is_ok());
        assert_eq!(route.runtime, "module");
        assert_eq!(route.module_artifact_id, OPENCODE_RUST_ARTIFACT_ID);
        assert_eq!(route.workspace_option.as_deref(), Some("directory"));
        assert_eq!(
            route.native_options["directory"].as_str(),
            Some(directory.as_str())
        );
        let owner = route.owned_service.as_ref().expect("owned route fixture");
        assert_eq!(owner.service_id, "native-prereq-opencode");
        assert_eq!(owner.model.id, "step-5-preview-free");
        assert_eq!(owner.model.provider_id, "opencode");
        assert_eq!(owner.model.variant, "high");
    }

    #[test]
    fn owned_route_rejects_static_external_identity_fields() {
        for (key, value) in [
            ("service_id", json!("external-opencode")),
            (
                "connection_file",
                json!(absolute_fixture_path("external-connection.json")),
            ),
            ("expected_version", json!("2.0.7")),
            (
                "model",
                json!({"id": "step-5-preview-free", "providerID": "opencode", "variant": "high"}),
            ),
        ] {
            let mut options = json!({
                "directory": absolute_fixture_path("owned-opencode-workspace")
            });
            options
                .as_object_mut()
                .expect("object fixture")
                .insert(key.to_owned(), value);

            assert!(
                route(options, Some(owned_service()))
                    .validate_activation_contract()
                    .is_err(),
                "owned route accepted conflicting external field {key}"
            );
        }
    }

    #[test]
    fn external_route_still_requires_the_complete_static_connection_source() {
        let directory = absolute_fixture_path("external-opencode-workspace");
        assert!(
            route(json!({"directory": directory}), None)
                .validate_activation_contract()
                .is_err()
        );

        let external_options = json!({
            "service_id": "external-opencode",
            "connection_file": absolute_fixture_path("external-connection.json"),
            "directory": absolute_fixture_path("external-opencode-workspace"),
            "model": {
                "id": "step-5-preview-free",
                "providerID": "opencode",
                "variant": "high"
            }
        });
        assert!(
            route(external_options, None)
                .validate_activation_contract()
                .is_ok()
        );

        let versioned_options = json!({
            "service_id": "external-opencode",
            "connection_file": absolute_fixture_path("external-connection.json"),
            "expected_version": "2.0.7",
            "directory": absolute_fixture_path("external-opencode-workspace"),
            "model": {
                "id": "step-5-preview-free",
                "providerID": "opencode",
                "variant": "high"
            }
        });
        assert!(
            route(versioned_options, None)
                .validate_activation_contract()
                .is_err()
        );
    }
}

fn validate_provider_auth_source(
    credential_ref: &str,
    source: &OwnedProviderAuthSourceConfig,
) -> Result<()> {
    validate_provider_credential_ref(credential_ref)?;
    validate_provider_id(&source.provider_id)?;
    if !source.auth_file.is_absolute()
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
            "owned provider auth source must be an absolute selected-provider auth.json path",
        ));
    }
    Ok(())
}

fn validate_provider_id(provider_id: &str) -> Result<()> {
    if provider_id.is_empty()
        || provider_id.len() > 256
        || !provider_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(Error::new(
            "CONFIG_ERROR",
            "owned provider auth source provider ID is malformed",
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
            observability: ObservabilityConfig::default(),
            ipc: Ipc::default(),
            routes: Vec::new(),
            opencode_provider_auth_sources: BTreeMap::new(),
            scripts: ScriptConfig::default(),
            checks: crate::checks::model::CheckConfig::default(),
            mcp: McpConfig::default(),
            gateway: GatewayConfig::default(),
            forge: crate::forge::ForgeConfig::default(),
            workspace: crate::workspace::WorkspaceConfig::default(),
            schedules: Vec::new(),
            automation_scheduler: AutomationSchedulerConfig::default(),
            module_supervisor: ModuleSupervisorConfig::default(),
            bus_supervisor: BusSupervisorConfig::default(),
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
        if let Some(path) = cfg.observability.live_config_file.as_mut()
            && path.is_relative()
        {
            *path = config_dir.join(&*path);
        }
        if let Some(directory) = cfg.observability.directory.as_mut()
            && directory.is_relative()
        {
            *directory = config_dir.join(&*directory);
        }
        cfg.module_supervisor.resolve_paths(&config_dir);
        cfg.bus_supervisor.resolve_paths(&config_dir);
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
            r.validate_activation_contract()?;
            if r.enabled
                && r.runtime == crate::runtime::codex::RUNTIME
                && r.module_artifact_id != crate::runtime::codex::ARTIFACT_ID
                && r.module_artifact_id != CODEX_RUST_ARTIFACT_ID
            {
                return Err(Error::new(
                    "CONFIG_ERROR",
                    "Codex routes require the pinned bridge.3 controller or standalone Rust adapter artifact",
                ));
            }
        }
        let mut services = std::collections::BTreeMap::new();
        let mut records = std::collections::BTreeMap::new();
        for route in cfg.routes.iter().filter(|r| {
            r.enabled
                && (r.runtime == crate::runtime::opencode_v2::RUNTIME
                    || (r.runtime == "module" && r.owned_service.is_some()))
        }) {
            if opencode_route_kind(&route.runtime, &route.module_artifact_id).is_none() {
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
        cfg.scripts.validate()?;
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
