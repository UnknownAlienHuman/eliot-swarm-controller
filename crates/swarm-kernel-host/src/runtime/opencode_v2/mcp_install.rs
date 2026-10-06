//! Prepare and register one launcher-issued Participant MCP core in an
//! explicitly selected OpenCode 2.0.7 location. OpenCode exposes this as an
//! in-memory, location-scoped runtime override; it is not a config-file edit
//! and its public status API does not reveal the registered command or tools.

use super::{Options, Service};
use crate::{
    config::{Config, Ipc, McpConfig, McpToolProfile},
    error::{Error, Result},
    model,
    native_mcp::AssignmentContext,
    participant_credentials, platform,
};
use reqwest::Url;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    io::Read,
    path::{Path, PathBuf},
};

const PINNED_OPENCODE_VERSION: &str = "2.0.7";
const MAX_PROFILE_CONFIG_BYTES: u64 = 131_072;
const MAX_MCP_SERVERS: usize = 256;
const MAX_TEXT_BYTES: usize = 32_768;
const MAX_EXECUTABLE_BYTES: u64 = 536_870_912;

/// Effect-free admission object. The Store should durably retain `identity()`
/// before calling `register`; fields that contain private paths and the
/// command are intentionally private and are never serialized.
pub(crate) struct PreparedMcpInstall {
    assignment: AssignmentContext,
    service_id: String,
    expected_version: String,
    directory: PathBuf,
    directory_text: String,
    directory_sha256: String,
    server_name: String,
    command: Vec<String>,
    command_sha256: String,
    executable_sha256: String,
}

impl PreparedMcpInstall {
    /// Sanitized intent identity for the Store's durable admission record.
    /// It contains no token, opaque artifact ref, or private filesystem path.
    pub(crate) fn identity(&self) -> Value {
        json!({
            "schema_version":1,
            "kind":"opencode_v2_mcp_install_intent",
            "registration":"location_scoped_in_memory",
            "assignment":self.assignment.as_value(),
            "service_id":self.service_id,
            "expected_version":self.expected_version,
            "location_sha256":self.directory_sha256,
            "server_name":self.server_name,
            "command_sha256":self.command_sha256,
            "executable_sha256":self.executable_sha256,
            "current_executable_file_sha256":self.executable_sha256,
            "current_executable_identity":"file_hash_only",
            "native_tool_set":"unknown",
            "provider_request_context":"unknown",
            "model_consumption":"unknown",
            "dispatch_permitted":false
        })
    }

    /// Build the private command envelope consumed by the independently
    /// launched OpenCode adapter. The actual command and HTTP bodies remain
    /// inside this authenticated module handoff; the Store never exposes them
    /// through the public identity/readback projection.
    pub(crate) fn native_mcp_command(
        &self,
        action: &str,
        binding_id: &str,
        binding_generation: i64,
        service_pid: u32,
    ) -> Result<Value> {
        if !matches!(action, "install" | "observe") {
            return Err(Error::invalid("prepared MCP install only supports install/observe"));
        }
        let list_path = list_route(self)?;
        let install_path = install_route(self)?;
        let config = json!({
            "type":"local",
            "command":self.command.clone(),
            "cwd":self.directory_text,
            "disabled":false
        });
        let mut envelope = json!({
            "schema_version":1,
            "kind":"swarm.native_mcp_command",
            "action":action,
            "scope":{
                "binding_id":binding_id,
                "binding_generation":binding_generation,
                "native_scope_key":format!("opencode-v2:{}", self.service_id),
                "service_id":self.service_id,
                "expected_version":self.expected_version,
                "service_pid":service_pid,
                "directory":self.directory_text,
                "assignment":self.assignment.as_value(),
            },
            "prepared":{
                "server_name":self.server_name,
                "install_intent":self.identity(),
                "command_sha256":self.command_sha256,
                "location_sha256":self.directory_sha256,
                "executable_sha256":self.executable_sha256,
            },
            "request":if action == "install" {
                json!({"method":"PUT","path":install_path,"body":{"config":config}})
            } else {
                json!({"method":"GET","path":list_path})
            },
            "precondition":if action == "install" {
                json!({"method":"GET","path":list_path})
            } else {
                Value::Null
            },
            "readback":if action == "install" {
                json!({"method":"GET","path":list_path})
            } else {
                Value::Null
            },
        });
        if action == "observe" {
            let object = envelope.as_object_mut().ok_or_else(|| {
                Error::invalid("native MCP command envelope is not an object")
            })?;
            object.remove("precondition");
            object.remove("readback");
        }
        Ok(envelope)
    }
}

/// Runtime status read from the exact OpenCode location after registration.
/// This reports only that a uniquely named runtime entry is visible.
pub(crate) struct InstallReadback {
    intent: Value,
    process_id: u32,
    version: String,
    status: &'static str,
    error_present: bool,
    observed_at_ms: i64,
}

impl InstallReadback {
    pub(crate) fn as_value(&self) -> Value {
        json!({
            "schema_version":1,
            "kind":"opencode_v2_mcp_install_readback",
            "intent":self.intent,
            "runtime_entry_present":true,
            "runtime_status":self.status,
            "runtime_error_present":self.error_present,
            "runtime_config_readback":"not_exposed_by_pinned_api",
            "matches_prepared_command":"unknown",
            "service_process_id":self.process_id,
            "service_version":self.version,
            "observed_at_ms":self.observed_at_ms,
            "native_tool_set":"unknown",
            "provider_request_context":"unknown",
            "model_consumption":"unknown",
            "dispatch_permitted":false
        })
    }
}

/// Validate the current Store-selected assignment and resolve its exact
/// launcher-issued credential/profile pair, without making a runtime call.
/// The Store must first bind `assignment` to the exact service/location route.
pub(crate) fn prepare(
    config: &Config,
    options: &Options,
    assignment: AssignmentContext,
    credential_ref: &str,
    profile_config_ref: &str,
) -> Result<PreparedMcpInstall> {
    validate_options(options)?;

    let scope = assignment.as_value();
    if scope.get("mcp_profile").and_then(Value::as_str) != Some("participant") {
        return Err(Error::new(
            "FORBIDDEN",
            "native MCP installation requires the exact Participant assignment profile",
        ));
    }

    let artifacts =
        participant_credentials::resolve_refs(config, credential_ref, profile_config_ref)?;
    let credential_path = private_regular_path(artifacts.credential_path())?;
    let profile_path = private_regular_path(artifacts.profile_config_path())?;
    let credential = platform::load_credential(&credential_path)?;
    let participant_id = scope
        .get("participant_id")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            Error::new(
                "NATIVE_MCP_SCOPE_MISMATCH",
                "assignment lacks participant identity",
            )
        })?;
    if credential.client_id != participant_id {
        return Err(Error::new(
            "NATIVE_MCP_SCOPE_MISMATCH",
            "resolved credential does not belong to the exact Participant assignment",
        ));
    }

    let profile_bytes = read_private_profile(&profile_path)?;
    let profile_contents = std::str::from_utf8(&profile_bytes).map_err(|_| {
        Error::new(
            "PRIVATE_ARTIFACT_REFERENCE",
            "launcher-issued Participant profile is not valid UTF-8",
        )
    })?;
    let scoped_config: ScopedParticipantConfig =
        toml::from_str(profile_contents).map_err(|_| {
            Error::new(
                "PRIVATE_ARTIFACT_REFERENCE",
                "launcher-issued Participant profile has an invalid config schema",
            )
        })?;
    if scoped_config.schema_version != 1
        || scoped_config.storage.queue_capacity != config.storage.queue_capacity
        || !same_ipc(&scoped_config.ipc, &config.ipc)
    {
        return Err(Error::new(
            "PRIVATE_ARTIFACT_REFERENCE",
            "launcher-issued Participant config does not match the current controller IPC scope",
        ));
    }
    scoped_config.mcp.validate()?;
    if scoped_config.mcp.profiles.len() != 1 {
        return Err(Error::new(
            "PRIVATE_ARTIFACT_REFERENCE",
            "launcher-issued Participant config must contain exactly one MCP profile",
        ));
    }
    let profile = scoped_config
        .mcp
        .profiles
        .get(&scoped_config.mcp.default_profile)
        .ok_or_else(|| {
            Error::new(
                "PRIVATE_ARTIFACT_REFERENCE",
                "launcher-issued Participant config has no selected MCP profile",
            )
        })?;
    if profile.tool_profile != McpToolProfile::Participant
        || profile.expected_client_id != credential.client_id
    {
        return Err(Error::new(
            "FORBIDDEN",
            "resolved MCP profile is not the hard Participant profile bound to its credential",
        ));
    }
    let surface = profile.surface.as_deref().ok_or_else(|| {
        Error::new(
            "PRIVATE_ARTIFACT_REFERENCE",
            "launcher-issued Participant config must name its validated surface",
        )
    })?;
    crate::mcp::launch_profile_surface(
        profile.tool_profile,
        surface,
        &profile.deferred_groups,
        &profile.manual_tools,
    )?;

    let host_data_dir = canonical_directory(&config.storage.data_dir)?;
    let scoped_data_dir = canonical_directory(&scoped_config.storage.data_dir)?;
    if host_data_dir != scoped_data_dir {
        return Err(Error::new(
            "PRIVATE_ARTIFACT_REFERENCE",
            "launcher-issued Participant config targets a different controller data directory",
        ));
    }

    // The native MCP command is run by the independently installed sibling,
    // never by the kernel host. Keep this path explicit so a future kernel
    // relocation cannot silently turn the command into `<kernel> mcp`.
    let executable = sibling_mcp_executable()?;
    let executable_sha256 = hash_executable(&executable)?;
    let directory_text = options.directory.to_str().ok_or_else(|| {
        Error::new(
            "NATIVE_MCP_SCOPE_MISMATCH",
            "OpenCode location path is not valid Unicode",
        )
    })?;
    validate_text(directory_text, "OpenCode location", MAX_TEXT_BYTES)?;
    let credential_text = path_text(&credential_path, "credential path")?;
    let profile_arg = path_text(&profile_path, "profile path")?;
    let data_dir_arg = path_text(&scoped_data_dir, "controller data directory")?;
    let executable_text = path_text(&executable, "swarm-mcp executable")?;
    let profile_name = &scoped_config.mcp.default_profile;
    let command = vec![
        executable_text,
        "--config".to_owned(),
        profile_arg,
        "--data-dir".to_owned(),
        data_dir_arg,
        "--credential".to_owned(),
        credential_text,
        "--profile".to_owned(),
        profile_name.clone(),
    ];

    let location_sha256 = format!("sha256:{}", model::digest(directory_text.as_bytes()));
    let install_identity = json!({
        "version":1,
        "assignment":assignment.as_value(),
        "service_id":options.service_id,
        "directory":directory_text,
        "credential_ref":credential_ref,
        "profile_config_ref":profile_config_ref
    });
    let install_identity = model::canonical(&install_identity)?;
    let server_name = format!("eliot-swarm-{}", model::digest(install_identity.as_bytes()));
    let command_sha256 = format!(
        "sha256:{}",
        model::digest(
            model::canonical(&json!({
                "command":command.clone(),
                "cwd":directory_text,
                "disabled":false
            }))?
            .as_bytes()
        )
    );

    Ok(PreparedMcpInstall {
        assignment,
        service_id: options.service_id.clone(),
        expected_version: options.expected_version.clone(),
        directory: options.directory.clone(),
        directory_text: directory_text.to_owned(),
        directory_sha256: location_sha256,
        server_name,
        command,
        command_sha256,
        executable_sha256,
    })
}

/// Add one server at the exact OpenCode location. A pre-existing derived name
/// is a conflict: OpenCode 2.0.7 implements this route as add-or-replace and
/// offers no compare-and-create. The PUT is issued once; any uncertain result
/// must be recorded by Store and read back, never replayed.
pub(crate) async fn register(
    service: &Service,
    options: &Options,
    prepared: &PreparedMcpInstall,
) -> Result<InstallReadback> {
    validate_prepared_route(service, options, prepared)?;
    let existing = list_location(service, prepared).await?;
    if existing
        .iter()
        .any(|server| server.name == prepared.server_name)
    {
        return Err(Error::new(
            "NATIVE_MCP_INSTALL_CONFLICT",
            "derived runtime MCP name already exists; refusing to replace it",
        ));
    }

    // Verify the pinned process once more immediately before its effect.
    service.verify().await?;
    let path = install_route(prepared)?;
    let config = json!({
        "type":"local",
        "command":prepared.command.clone(),
        "cwd":prepared.directory_text,
        "disabled":false
    });
    service.put(&path, json!({"config":config})).await?;

    match observe(service, options, prepared).await {
        Ok(Some(readback)) => Ok(readback),
        Ok(None) => Err(unknown_after_put(
            "OpenCode accepted registration but exact-location readback did not find its name; do not replay",
        )),
        Err(_) => Err(unknown_after_put(
            "OpenCode accepted registration but exact-location readback failed; do not replay",
        )),
    }
}

/// Read only the uniquely named entry for a previously prepared intent. This
/// is the safe recovery path after an unknown PUT result; it never mutates.
pub(crate) async fn observe(
    service: &Service,
    options: &Options,
    prepared: &PreparedMcpInstall,
) -> Result<Option<InstallReadback>> {
    validate_prepared_route(service, options, prepared)?;
    let servers = list_location(service, prepared).await?;
    let mut matches = servers
        .into_iter()
        .filter(|server| server.name == prepared.server_name);
    let Some(server) = matches.next() else {
        return Ok(None);
    };
    if matches.next().is_some() {
        return Err(Error::new(
            "NATIVE_MCP_SCHEMA",
            "exact OpenCode location returned duplicate MCP names",
        ));
    }
    let (status, error_present) = server.status.readback();
    Ok(Some(InstallReadback {
        intent: prepared.identity(),
        process_id: service.pid,
        version: service.version.clone(),
        status,
        error_present,
        observed_at_ms: model::now_ms()?,
    }))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LocationEnvelope<T> {
    location: LocationRef,
    data: T,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LocationRef {
    directory: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ScopedParticipantConfig {
    schema_version: u32,
    storage: ScopedStorage,
    ipc: Ipc,
    mcp: McpConfig,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ScopedStorage {
    data_dir: PathBuf,
    queue_capacity: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeMcpServer {
    name: String,
    status: NativeMcpStatus,
    #[serde(rename = "integrationID", default)]
    _integration_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(tag = "status", deny_unknown_fields)]
enum NativeMcpStatus {
    #[serde(rename = "connected")]
    Connected,
    #[serde(rename = "pending")]
    Pending,
    #[serde(rename = "disabled")]
    Disabled,
    #[serde(rename = "failed")]
    Failed { error: String },
    #[serde(rename = "needs_auth")]
    NeedsAuth { error: String },
}

impl NativeMcpStatus {
    fn readback(self) -> (&'static str, bool) {
        match self {
            Self::Connected => ("connected", false),
            Self::Pending => ("pending", false),
            Self::Disabled => ("disabled", false),
            Self::Failed { error } => {
                let _ = error;
                ("failed", true)
            }
            Self::NeedsAuth { error } => {
                let _ = error;
                ("needs_auth", true)
            }
        }
    }
}

async fn list_location(
    service: &Service,
    prepared: &PreparedMcpInstall,
) -> Result<Vec<NativeMcpServer>> {
    service.verify().await?;
    let raw = service
        .get(
            "/api/mcp",
            &[("location[directory]", prepared.directory_text.clone())],
        )
        .await?;
    let response: LocationEnvelope<Vec<NativeMcpServer>> =
        serde_json::from_value(raw).map_err(|_| {
            Error::new(
                "NATIVE_MCP_SCHEMA",
                "OpenCode MCP readback does not match pinned 2.0.7 schema",
            )
        })?;
    if response.location.directory != prepared.directory_text
        || response.data.len() > MAX_MCP_SERVERS
    {
        return Err(Error::new(
            "NATIVE_MCP_SCOPE_MISMATCH",
            "OpenCode MCP readback is outside the requested location or response bound",
        ));
    }
    service.verify().await?;
    Ok(response.data)
}

fn validate_options(options: &Options) -> Result<()> {
    let service_id_valid = !options.service_id.is_empty()
        && options.service_id.len() <= 128
        && options
            .service_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte));
    if !service_id_valid
        || !options.directory.is_absolute()
        || options.directory.to_str().is_none()
        || options.expected_version != PINNED_OPENCODE_VERSION
    {
        return Err(Error::new(
            "NATIVE_MCP_SCOPE_MISMATCH",
            "OpenCode route must use an explicit absolute location and pinned 2.0.7 service",
        ));
    }
    Ok(())
}

fn validate_prepared_route(
    service: &Service,
    options: &Options,
    prepared: &PreparedMcpInstall,
) -> Result<()> {
    validate_options(options)?;
    if options.service_id != prepared.service_id
        || options.expected_version != prepared.expected_version
        || options.directory != prepared.directory
        || service.version != PINNED_OPENCODE_VERSION
        || service.version != options.expected_version
        || service.pid == 0
    {
        return Err(Error::new(
            "NATIVE_MCP_SCOPE_MISMATCH",
            "prepared install does not match the exact OpenCode service and location",
        ));
    }
    Ok(())
}

fn install_route(prepared: &PreparedMcpInstall) -> Result<String> {
    let mut url = Url::parse("http://127.0.0.1/")
        .map_err(|_| Error::new("NATIVE_ENDPOINT", "cannot construct local MCP route"))?;
    url.set_path(&format!("/api/experimental/mcp/{}", prepared.server_name));
    url.query_pairs_mut()
        .append_pair("location[directory]", &prepared.directory_text);
    let query = url.query().ok_or_else(|| {
        Error::new(
            "NATIVE_ENDPOINT",
            "cannot construct location-scoped MCP route",
        )
    })?;
    Ok(format!("{}?{}", url.path(), query))
}

fn list_route(prepared: &PreparedMcpInstall) -> Result<String> {
    let mut url = Url::parse("http://127.0.0.1/")
        .map_err(|_| Error::new("NATIVE_ENDPOINT", "cannot construct local MCP route"))?;
    url.set_path("/api/mcp");
    url.query_pairs_mut()
        .append_pair("location[directory]", &prepared.directory_text);
    let query = url.query().ok_or_else(|| {
        Error::new(
            "NATIVE_ENDPOINT",
            "cannot construct location-scoped MCP route",
        )
    })?;
    Ok(format!("{}?{}", url.path(), query))
}

fn unknown_after_put(message: &str) -> Error {
    Error::new("NATIVE_OUTCOME_UNKNOWN", message)
}

fn private_regular_path(path: &Path) -> Result<PathBuf> {
    let metadata = fs::symlink_metadata(path).map_err(|_| {
        Error::new(
            "PRIVATE_ARTIFACT_REFERENCE",
            "launcher-issued private artifact is unavailable",
        )
    })?;
    if !metadata.is_file() || is_link_or_reparse(&metadata) {
        return Err(Error::new(
            "PRIVATE_ARTIFACT_REFERENCE",
            "launcher-issued artifact must be a private regular file",
        ));
    }
    platform::private_permissions(path, false)?;
    let canonical = fs::canonicalize(path).map_err(|_| {
        Error::new(
            "PRIVATE_ARTIFACT_REFERENCE",
            "launcher-issued private artifact path cannot be resolved",
        )
    })?;
    let canonical_metadata = fs::symlink_metadata(&canonical).map_err(|_| {
        Error::new(
            "PRIVATE_ARTIFACT_REFERENCE",
            "launcher-issued private artifact path cannot be verified",
        )
    })?;
    if !canonical_metadata.is_file() || is_link_or_reparse(&canonical_metadata) {
        return Err(Error::new(
            "PRIVATE_ARTIFACT_REFERENCE",
            "launcher-issued artifact must resolve to a regular file",
        ));
    }
    Ok(canonical)
}

fn read_private_profile(path: &Path) -> Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path).map_err(|_| {
        Error::new(
            "PRIVATE_ARTIFACT_REFERENCE",
            "launcher-issued Participant profile is unavailable",
        )
    })?;
    if !metadata.is_file()
        || is_link_or_reparse(&metadata)
        || metadata.len() > MAX_PROFILE_CONFIG_BYTES
    {
        return Err(Error::new(
            "PRIVATE_ARTIFACT_REFERENCE",
            "launcher-issued Participant profile is not a bounded regular file",
        ));
    }
    let file = File::open(path).map_err(|_| {
        Error::new(
            "PRIVATE_ARTIFACT_REFERENCE",
            "launcher-issued Participant profile cannot be read",
        )
    })?;
    if !file
        .metadata()
        .is_ok_and(|opened| opened.is_file() && opened.len() <= MAX_PROFILE_CONFIG_BYTES)
    {
        return Err(Error::new(
            "PRIVATE_ARTIFACT_REFERENCE",
            "launcher-issued Participant profile changed while being opened",
        ));
    }
    let mut bytes = Vec::new();
    file.take(MAX_PROFILE_CONFIG_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| {
            Error::new(
                "PRIVATE_ARTIFACT_REFERENCE",
                "launcher-issued Participant profile read failed",
            )
        })?;
    if bytes.len() as u64 > MAX_PROFILE_CONFIG_BYTES {
        return Err(Error::new(
            "PRIVATE_ARTIFACT_REFERENCE",
            "launcher-issued Participant profile exceeds its size boundary",
        ));
    }
    Ok(bytes)
}

fn canonical_directory(path: &Path) -> Result<PathBuf> {
    fs::canonicalize(path).map_err(|_| {
        Error::new(
            "PRIVATE_ARTIFACT_REFERENCE",
            "Participant controller data directory cannot be resolved",
        )
    })
}

fn same_ipc(left: &Ipc, right: &Ipc) -> bool {
    left.max_frame_bytes == right.max_frame_bytes
        && left.max_connections == right.max_connections
        && left.max_inflight_per_connection == right.max_inflight_per_connection
        && left.write_timeout_seconds == right.write_timeout_seconds
}

fn sibling_mcp_executable() -> Result<PathBuf> {
    let current = std::env::current_exe().map_err(|_| {
        Error::new(
            "NATIVE_MCP_LAUNCHER",
            "current launcher executable cannot be identified",
        )
    })?;
    let mut path = fs::canonicalize(&current).map_err(|_| {
        Error::new(
            "NATIVE_MCP_LAUNCHER",
            "current launcher executable path cannot be resolved",
        )
    })?;
    path.set_file_name(if cfg!(windows) { "swarm-mcp.exe" } else { "swarm-mcp" });
    let metadata = fs::symlink_metadata(&path).map_err(|_| {
        Error::new(
            "NATIVE_MCP_LAUNCHER",
            "installed swarm-mcp sibling cannot be verified",
        )
    })?;
    if !metadata.is_file() || is_link_or_reparse(&metadata) {
        return Err(Error::new(
            "NATIVE_MCP_LAUNCHER",
            "installed swarm-mcp sibling must resolve to a regular file",
        ));
    }
    fs::canonicalize(&path).map_err(|_| {
        Error::new(
            "NATIVE_MCP_LAUNCHER",
            "installed swarm-mcp sibling path cannot be resolved",
        )
    })
}

fn hash_executable(path: &Path) -> Result<String> {
    let mut file = File::open(path).map_err(|_| {
        Error::new(
            "NATIVE_MCP_LAUNCHER",
            "current controller executable cannot be read for identity",
        )
    })?;
    let length = file
        .metadata()
        .map_err(|_| {
            Error::new(
                "NATIVE_MCP_LAUNCHER",
                "current controller executable metadata is unavailable",
            )
        })?
        .len();
    if length == 0 || length > MAX_EXECUTABLE_BYTES {
        return Err(Error::new(
            "NATIVE_MCP_LAUNCHER",
            "current controller executable is outside the supported size boundary",
        ));
    }
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    let mut total = 0_u64;
    loop {
        let read = file.read(&mut buffer).map_err(|_| {
            Error::new(
                "NATIVE_MCP_LAUNCHER",
                "current controller executable identity read failed",
            )
        })?;
        if read == 0 {
            break;
        }
        total = total.saturating_add(read as u64);
        if total > MAX_EXECUTABLE_BYTES {
            return Err(Error::new(
                "NATIVE_MCP_LAUNCHER",
                "current controller executable exceeds the supported size boundary",
            ));
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

fn path_text(path: &Path, field: &str) -> Result<String> {
    let text = path.to_str().ok_or_else(|| {
        Error::new(
            "NATIVE_MCP_LAUNCHER",
            format!("{field} is not valid Unicode"),
        )
    })?;
    validate_text(text, field, MAX_TEXT_BYTES)?;
    Ok(text.to_owned())
}

fn validate_text(value: &str, field: &str, max: usize) -> Result<()> {
    if value.is_empty() || value.len() > max || value.bytes().any(|byte| byte.is_ascii_control()) {
        return Err(Error::new(
            "NATIVE_MCP_SCOPE_MISMATCH",
            format!("{field} must be bounded, nonempty text without control bytes"),
        ));
    }
    Ok(())
}

fn is_link_or_reparse(metadata: &fs::Metadata) -> bool {
    if metadata.file_type().is_symlink() {
        return true;
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0400;
        if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return true;
        }
    }
    false
}
