//! Challenge-scoped native MCP evidence from the pinned OpenCode 2.0.7
//! in-process plugin/service boundary. This remains an observation only.

use super::{ModelRef, Options, Service, http::decode};
use crate::{
    error::{Error, Result},
    model,
    native_mcp::AssignmentContext,
};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

const PLUGIN_ID: &str = "eliot.native-mcp-proof.v1";
pub(super) const RPC_ID: &str = PLUGIN_ID;
const PINNED_VERSION: &str = "2.0.7";
const CHALLENGE_TTL_MS: i64 = 120_000;
const MAX_PLUGIN_SOURCE_BYTES: u64 = 512 * 1024;
const MAX_EVIDENCE_BYTES: usize = 512 * 1024;
const MAX_TOOLS: usize = 512;
const PLUGIN_ENTRY_BYTES: &[u8] = b"export { default } from './native-mcp-proof.mjs';\n";

#[derive(Debug, Clone)]
pub(crate) struct NativeMcpChallenge {
    assignment: AssignmentContext,
    challenge_id: String,
    nonce: String,
    issued_at_ms: i64,
    expires_at_ms: i64,
    service_id: String,
    service_pid: u32,
    service_version: String,
    directory: String,
    model: ModelRef,
    module_path: PathBuf,
    module_sha256: String,
}

/// Read-only preflight result for the one allowed native observer arm.
/// Fields are private and the type has no Serde input path, so callers cannot
/// substitute a different challenge, route, or expected plugin configuration
/// after the Store has durably reserved the effect.
pub(crate) struct PreparedNativeMcpArm {
    challenge: NativeMcpChallenge,
    expected_plugin_config: Value,
    request_path: String,
    request_body: Value,
}

/// Construct a new opaque challenge only from an AssignmentContext minted by
/// the authorized Store read path. The type deliberately has no Serde input.
pub(crate) fn new_challenge(
    assignment: AssignmentContext,
    service: &Service,
    options: &Options,
) -> Result<NativeMcpChallenge> {
    let (module_path, module_sha256) = module_source()?;
    let issued_at_ms = model::now_ms()?;
    let expires_at_ms = issued_at_ms
        .checked_add(CHALLENGE_TTL_MS)
        .ok_or_else(|| Error::new("CLOCK_ERROR", "native MCP challenge expiry overflow"))?;
    let challenge = NativeMcpChallenge {
        assignment,
        challenge_id: model::new_id(),
        nonce: model::new_id(),
        issued_at_ms,
        expires_at_ms,
        service_id: options.service_id.clone(),
        service_pid: service.pid,
        service_version: service.version.clone(),
        directory: options
            .directory
            .to_str()
            .ok_or_else(|| scope_error("native project directory is not valid Unicode"))?
            .to_owned(),
        model: options.model.clone(),
        module_path,
        module_sha256,
    };
    validate_challenge(&challenge, service, options)?;
    Ok(challenge)
}

/// Private metadata for the controller's existing Store record. It contains a
/// replay nonce but is not a request schema or a caller-grantable capability.
pub(crate) fn challenge_metadata(challenge: &NativeMcpChallenge) -> Value {
    json!({
        "schema": "opencode-v2-native-mcp-challenge-v1",
        "assignment": challenge.assignment.as_value(),
        "challenge_id": challenge.challenge_id,
        "nonce": challenge.nonce,
        "issued_at_ms": challenge.issued_at_ms,
        "expires_at_ms": challenge.expires_at_ms,
        "service_id": challenge.service_id,
        "service_pid": challenge.service_pid,
        "service_version": challenge.service_version,
        "directory": challenge.directory,
        "model": challenge.model,
        "module_path": challenge.module_path,
        "module_sha256": challenge.module_sha256,
    })
}

/// Exact local plugin config for the pinned service. The host owns where this
/// config is installed; this helper only projects the verified source identity.
pub(crate) fn plugin_config_value(options: &Options) -> Result<Value> {
    if options.expected_version != PINNED_VERSION {
        return Err(source_error_code(
            "NATIVE_MCP_PROOF_PLUGIN_CONFIG_MISMATCH",
            "native MCP observer requires OpenCode 2.0.7",
        ));
    }
    let (module_path, module_sha256) = module_source()?;
    let entry = plugin_entry_path(&module_path)?;
    let package_dir = entry.parent().ok_or_else(|| {
        source_error_code(
            "NATIVE_MCP_PROOF_PLUGIN_CONFIG_INVALID",
            "native MCP plugin directory is missing",
        )
    })?;
    let package = plugin_config_package_path(package_dir)?;
    Ok(json!({
        "package": package,
        "options": {
            "serviceId": options.service_id,
            "serviceVersion": PINNED_VERSION,
            "moduleSha256": module_sha256,
        },
    }))
}

/// Return the path spelling the pinned Bun resolver accepts at the OpenCode
/// config boundary. Source identity and filesystem checks continue to use the
/// canonical `Path`; only Windows verbatim namespace prefixes are projected.
pub(crate) fn plugin_config_package_path(path: &Path) -> Result<String> {
    let value = path.to_str().ok_or_else(|| {
        source_error_code(
            "NATIVE_MCP_PROOF_PLUGIN_CONFIG_INVALID",
            "native MCP observer path is not valid Unicode",
        )
    })?;
    plugin_config_package_path_text(value)
}

fn plugin_config_package_path_text(value: &str) -> Result<String> {
    const VERBATIM_PREFIX: &str = "\\\\?\\";
    const VERBATIM_UNC_PREFIX: &str = "\\\\?\\UNC\\";

    if value
        .get(..VERBATIM_UNC_PREFIX.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(VERBATIM_UNC_PREFIX))
    {
        let ordinary = &value[VERBATIM_UNC_PREFIX.len()..];
        let mut components = ordinary.split(['\\', '/']);
        if components
            .next()
            .is_none_or(|component| component.is_empty())
            || components
                .next()
                .is_none_or(|component| component.is_empty())
        {
            return Err(source_error_code(
                "NATIVE_MCP_PROOF_PLUGIN_CONFIG_INVALID",
                "native MCP observer UNC path is invalid",
            ));
        }
        return Ok(format!("\\\\{ordinary}"));
    }

    if let Some(ordinary) = value.strip_prefix(VERBATIM_PREFIX) {
        let bytes = ordinary.as_bytes();
        if bytes.len() >= 3
            && bytes[0].is_ascii_alphabetic()
            && bytes[1] == b':'
            && matches!(bytes[2], b'\\' | b'/')
        {
            return Ok(ordinary.to_owned());
        }
        return Err(source_error_code(
            "NATIVE_MCP_PROOF_PLUGIN_CONFIG_INVALID",
            "native MCP observer Windows path namespace is unsupported",
        ));
    }

    Ok(value.to_owned())
}

/// Recreate a challenge from exact private Store metadata after a lost HTTP
/// response. The caller must first authorize the retained AssignmentContext
/// and read these values from the controller's own persisted challenge record.
// Restoration keeps the exact retained scope, route, expiry window, and source proof inputs explicit.
#[allow(clippy::too_many_arguments)]
pub(crate) fn restore_challenge(
    assignment: AssignmentContext,
    service: &Service,
    options: &Options,
    challenge_id: String,
    nonce: String,
    issued_at_ms: i64,
    expires_at_ms: i64,
    module_sha256: String,
) -> Result<NativeMcpChallenge> {
    let (module_path, current_sha256) = module_source()?;
    if !module_hash_valid(&module_sha256) {
        return Err(source_error_code(
            "NATIVE_MCP_PROOF_MODULE_INTEGRITY",
            "native MCP observer challenge has an invalid source hash",
        ));
    }
    if module_sha256 != current_sha256 {
        return Err(rotated_module());
    }
    let directory = options
        .directory
        .to_str()
        .ok_or_else(|| scope_error("native project directory is not valid Unicode"))?
        .to_owned();
    let challenge = NativeMcpChallenge {
        assignment,
        challenge_id,
        nonce,
        issued_at_ms,
        expires_at_ms,
        service_id: options.service_id.clone(),
        service_pid: service.pid,
        service_version: service.version.clone(),
        directory,
        model: options.model.clone(),
        module_path,
        module_sha256,
    };
    validate_challenge(&challenge, service, options)?;
    Ok(challenge)
}

/// Restore only the controller's own persisted metadata after the caller has
/// authorized the same exact AssignmentContext from current Store state.
pub(crate) fn restore_challenge_metadata(
    assignment: AssignmentContext,
    service: &Service,
    options: &Options,
    metadata: Value,
) -> Result<NativeMcpChallenge> {
    let stored: StoredChallengeMetadata = serde_json::from_value(metadata)
        .map_err(|_| challenge_error("stored native MCP challenge metadata is invalid"))?;
    let (module_path, _) = module_source()?;
    let directory = options
        .directory
        .to_str()
        .ok_or_else(|| scope_error("native project directory is not valid Unicode"))?;
    let module_path_text = module_path.to_str().ok_or_else(|| {
        source_error_code(
            "NATIVE_MCP_PROOF_MODULE_INTEGRITY",
            "native MCP observer path is not valid Unicode",
        )
    })?;
    if stored.schema != "opencode-v2-native-mcp-challenge-v1"
        || model::canonical(&stored.assignment)? != model::canonical(&assignment.as_value())?
        || stored.service_id != options.service_id
        || stored.service_pid != service.pid
        || stored.service_version != service.version
        || stored.directory != directory
        || stored.model != options.model
    {
        return Err(challenge_error(
            "stored native MCP challenge belongs to another assignment or service",
        ));
    }
    if stored.module_path != module_path_text {
        return Err(rotated_module());
    }
    restore_challenge(
        assignment,
        service,
        options,
        stored.challenge_id,
        stored.nonce,
        stored.issued_at_ms,
        stored.expires_at_ms,
        stored.module_sha256,
    )
}

/// Verify the observer is already active in the exact location and prepare
/// the exact expected plugin configuration without arming it. This must run
/// before the Store records its write-ahead `outcome_unknown` state.
pub(crate) async fn preflight_arm(
    service: &Service,
    options: &Options,
    challenge: &NativeMcpChallenge,
) -> Result<PreparedNativeMcpArm> {
    validate_challenge(challenge, service, options)?;
    verify_plugin_source(service, options, challenge).await?;
    service.verify().await?;

    let expected_plugin_config = expected_plugin_config(options, challenge)?;
    let request_path = rpc_path("arm", &challenge.directory)?;
    let request_body = json!({
        "input": {
            "challenge_id": challenge.challenge_id,
            "nonce": challenge.nonce,
            "issued_at_ms": challenge.issued_at_ms,
            "expires_at_ms": challenge.expires_at_ms,
            "service_id": challenge.service_id,
            "service_pid": challenge.service_pid,
            "service_version": challenge.service_version,
            "module_sha256": challenge.module_sha256,
            "directory": challenge.directory,
            "session_id": challenge.assignment.native_session_id(),
            "model": challenge.model,
            "assignment": challenge.assignment.as_value(),
        }
    });
    Ok(PreparedNativeMcpArm {
        challenge: challenge.clone(),
        expected_plugin_config,
        request_path,
        request_body,
    })
}

/// Arm the exact observer which passed the read-only preflight. This sends
/// the single RPC POST; any transport or acknowledgement error is ambiguous
/// and must be followed by read-only observation rather than another arm.
pub(crate) async fn arm_prepared(
    service: &Service,
    options: &Options,
    prepared: PreparedNativeMcpArm,
) -> Result<()> {
    // Everything that can reject the exact challenge/config locally is done
    // by preflight before the Store's durable effect reservation. Once this
    // function is called, go straight to the single prepared POST; any
    // transport/acknowledgement failure is conservatively outcome-unknown.
    let PreparedNativeMcpArm {
        challenge,
        expected_plugin_config,
        request_path,
        request_body,
    } = prepared;
    let response = service.post(&request_path, request_body).await?;
    let envelope: RpcEnvelope<ArmAck> = decode(response)?;
    if !envelope.output.accepted
        || envelope.output.challenge_id != challenge.challenge_id
        || envelope.output.nonce != challenge.nonce
        || envelope.output.module_sha256 != challenge.module_sha256
        || envelope.output.service_id != challenge.service_id
        || envelope.output.service_pid != challenge.service_pid
        || envelope.output.service_version != challenge.service_version
        || expected_plugin_config["options"]["serviceId"].as_str()
            != Some(envelope.output.service_id.as_str())
        || expected_plugin_config["options"]["serviceVersion"].as_str()
            != Some(envelope.output.service_version.as_str())
        || expected_plugin_config["options"]["moduleSha256"].as_str()
            != Some(envelope.output.module_sha256.as_str())
    {
        return Err(challenge_error(
            "native observer did not acknowledge the exact challenge",
        ));
    }

    service.verify().await?;
    verify_plugin_source(service, options, &challenge).await?;
    Ok(())
}

fn expected_plugin_config(options: &Options, challenge: &NativeMcpChallenge) -> Result<Value> {
    let config = plugin_config_value(options)?;
    plugin_entry_path(&challenge.module_path)?;
    let package_dir = challenge.module_path.parent().ok_or_else(|| {
        source_error_code(
            "NATIVE_MCP_PROOF_PLUGIN_CONFIG_INVALID",
            "native MCP plugin directory is missing",
        )
    })?;
    let package_path = plugin_config_package_path(package_dir)?;
    if config["package"] != package_path {
        return Err(source_error_code(
            "NATIVE_MCP_PROOF_PLUGIN_CONFIG_MISMATCH",
            "native MCP observer package differs from its expected configuration",
        ));
    }
    if config["options"]["moduleSha256"] != challenge.module_sha256 {
        return Err(rotated_module());
    }
    if config["options"]["serviceId"] != challenge.service_id
        || config["options"]["serviceVersion"] != challenge.service_version
    {
        return Err(source_error_code(
            "NATIVE_MCP_PROOF_PLUGIN_CONFIG_MISMATCH",
            "expected native MCP observer configuration differs from its challenge",
        ));
    }
    Ok(config)
}

/// Read the plugin's retained native discovery, session hook and provider
/// request observations. The result remains non-dispatchable and never claims
/// that a model consumed the supplied schemas.
pub(crate) async fn read(
    service: &Service,
    options: &Options,
    challenge: &NativeMcpChallenge,
) -> Result<NativeMcpToolsReadback> {
    validate_challenge(challenge, service, options)?;
    verify_plugin_source(service, options, challenge).await?;
    service.verify().await?;

    let path = rpc_path("read", &challenge.directory)?;
    let response = service
        .post(
            &path,
            json!({
                "input": {
                    "challenge_id": challenge.challenge_id,
                    "nonce": challenge.nonce,
                }
            }),
        )
        .await?;
    let envelope: RpcEnvelope<PluginReadback> = decode(response)?;
    let raw = envelope.output;
    validate_readback(&raw, challenge, service, options)?;

    service.verify().await?;
    verify_plugin_source(service, options, challenge).await?;
    let (_, source_hash_after) = module_source()?;
    if source_hash_after != challenge.module_sha256 {
        return Err(rotated_module());
    }

    let value = project_readback(raw, challenge, service)?;
    Ok(NativeMcpToolsReadback {
        scope: challenge.assignment.clone(),
        value,
    })
}

#[derive(Debug, Clone)]
pub(crate) struct NativeMcpToolsReadback {
    scope: AssignmentContext,
    value: Value,
}

impl NativeMcpToolsReadback {
    pub(crate) fn scope(&self) -> &AssignmentContext {
        &self.scope
    }

    pub(crate) fn as_value(&self) -> Value {
        self.value.clone()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RpcEnvelope<T> {
    output: T,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredChallengeMetadata {
    schema: String,
    assignment: Value,
    challenge_id: String,
    nonce: String,
    issued_at_ms: i64,
    expires_at_ms: i64,
    service_id: String,
    service_pid: u32,
    service_version: String,
    directory: String,
    model: ModelRef,
    module_path: String,
    module_sha256: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ArmAck {
    accepted: bool,
    challenge_id: String,
    nonce: String,
    module_sha256: String,
    service_id: String,
    service_pid: u32,
    service_version: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LocationEnvelope<T> {
    location: PluginLocation,
    data: T,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PluginLocation {
    directory: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PluginReadback {
    plugin_id: String,
    module_path: String,
    module_sha256: String,
    service_id: String,
    service_pid: u32,
    service_version: String,
    directory: String,
    challenge_id: String,
    nonce: String,
    issued_at_ms: i64,
    expires_at_ms: i64,
    assignment: Value,
    expected_model: ModelRef,
    session_id: String,
    receipt_observed_at_ms: i64,
    observation_sequence: u64,
    native_discovered: NativeDiscovery,
    session_context: SessionContextEvidence,
    provider_request: ProviderRequestEvidence,
    model_consumed: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeDiscovery {
    status: String,
    observed_at_ms: i64,
    tools: Vec<NativeTool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeTool {
    server: String,
    name: String,
    description: String,
    input_schema: Value,
    codemode: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ContextTool {
    name: String,
    description: String,
    input_schema: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionContextEvidence {
    status: String,
    stage: Option<String>,
    observed_at_ms: Option<i64>,
    agent: Option<String>,
    tools: Vec<ContextTool>,
    reason_code: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderRequestEvidence {
    status: String,
    transport: Option<String>,
    stage: Option<String>,
    kind: Option<String>,
    observed_at_ms: Option<i64>,
    agent: Option<String>,
    model: Option<ModelRef>,
    tools: Vec<ContextTool>,
    reason_code: Option<String>,
}

fn validate_challenge(
    challenge: &NativeMcpChallenge,
    service: &Service,
    options: &Options,
) -> Result<()> {
    let now = model::now_ms()?;
    let challenge_span = challenge.expires_at_ms.checked_sub(challenge.issued_at_ms);
    if challenge.expires_at_ms <= now {
        return Err(expired_challenge());
    }
    if !valid_uuid_v4(&challenge.challenge_id)
        || !valid_uuid_v4(&challenge.nonce)
        || challenge.issued_at_ms <= 0
        || challenge.issued_at_ms > now
        || challenge.expires_at_ms <= challenge.issued_at_ms
        || challenge_span.is_none_or(|span| span > CHALLENGE_TTL_MS)
        || challenge.service_id != options.service_id
        || challenge.service_pid != service.pid
        || challenge.service_version != PINNED_VERSION
        || challenge.service_version != service.version
        || options.expected_version != PINNED_VERSION
        || challenge.directory != options.directory.to_str().unwrap_or_default()
        || challenge.model != options.model
        || !challenge.model.valid()
        || !module_hash_valid(&challenge.module_sha256)
        || service.pid == 0
    {
        return Err(scope_error(
            "native MCP challenge is expired or bound to another route",
        ));
    }
    let (module_path, module_hash) = module_source()?;
    if module_path != challenge.module_path || module_hash != challenge.module_sha256 {
        return Err(rotated_module());
    }
    Ok(())
}

fn valid_uuid_v4(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 36
        && [8, 13, 18, 23].iter().all(|index| bytes[*index] == b'-')
        && bytes.iter().enumerate().all(|(index, byte)| {
            [8, 13, 18, 23].contains(&index)
                || byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()
        })
        && bytes[14] == b'4'
        && matches!(bytes[19], b'8' | b'9' | b'a' | b'b')
}

pub(crate) fn module_source() -> Result<(PathBuf, String)> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("modules")
        .join("opencode")
        .join("native-mcp-proof.mjs");
    let metadata = fs::symlink_metadata(&path).map_err(|_| {
        source_error_code(
            "NATIVE_MCP_PROOF_MODULE_UNAVAILABLE",
            "native MCP observer module is unavailable",
        )
    })?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() > MAX_PLUGIN_SOURCE_BYTES
    {
        return Err(source_error_code(
            "NATIVE_MCP_PROOF_MODULE_INTEGRITY",
            "native MCP observer module must be a bounded regular file",
        ));
    }
    let canonical = path.canonicalize().map_err(|_| {
        source_error_code(
            "NATIVE_MCP_PROOF_MODULE_INTEGRITY",
            "native MCP observer module path is invalid",
        )
    })?;
    if canonical.to_str().is_none() {
        return Err(source_error_code(
            "NATIVE_MCP_PROOF_MODULE_INTEGRITY",
            "native MCP observer module path is not valid Unicode",
        ));
    }
    let bytes = fs::read(&canonical).map_err(|_| {
        source_error_code(
            "NATIVE_MCP_PROOF_MODULE_UNAVAILABLE",
            "native MCP observer module cannot be read",
        )
    })?;
    if bytes.is_empty() || bytes.len() as u64 > MAX_PLUGIN_SOURCE_BYTES {
        return Err(source_error_code(
            "NATIVE_MCP_PROOF_MODULE_INTEGRITY",
            "native MCP observer module exceeds the source boundary",
        ));
    }
    Ok((canonical, model::digest(&bytes)))
}

/// OpenCode 2.0.7 config resolves a directory's index/server entrypoint.
/// Accept only this exact transparent entry; the observer has its own hash.
pub(crate) fn plugin_entry_path(module_path: &Path) -> Result<PathBuf> {
    let directory = module_path.parent().ok_or_else(|| {
        source_error_code(
            "NATIVE_MCP_PROOF_ENTRY_INTEGRITY",
            "native MCP plugin directory is missing",
        )
    })?;
    let entry = directory.join("index.mjs");
    let metadata = fs::symlink_metadata(&entry).map_err(|_| {
        source_error_code(
            "NATIVE_MCP_PROOF_ENTRY_UNAVAILABLE",
            "native MCP plugin entry is unavailable",
        )
    })?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() != PLUGIN_ENTRY_BYTES.len() as u64
    {
        return Err(source_error_code(
            "NATIVE_MCP_PROOF_ENTRY_INTEGRITY",
            "native MCP plugin entry is not the bounded canonical wrapper",
        ));
    }
    let canonical = entry.canonicalize().map_err(|_| {
        source_error_code(
            "NATIVE_MCP_PROOF_ENTRY_INTEGRITY",
            "native MCP plugin entry path is invalid",
        )
    })?;
    let bytes = fs::read(&canonical).map_err(|_| {
        source_error_code(
            "NATIVE_MCP_PROOF_ENTRY_UNAVAILABLE",
            "native MCP plugin entry cannot be read",
        )
    })?;
    if canonical.parent() != Some(directory) || bytes != PLUGIN_ENTRY_BYTES {
        return Err(source_error_code(
            "NATIVE_MCP_PROOF_ENTRY_INTEGRITY",
            "native MCP plugin entry differs from the canonical wrapper",
        ));
    }
    Ok(canonical)
}

async fn verify_plugin_source(
    service: &Service,
    options: &Options,
    challenge: &NativeMcpChallenge,
) -> Result<()> {
    let (expected_path, expected_hash) = module_source()?;
    if expected_path != challenge.module_path || expected_hash != challenge.module_sha256 {
        return Err(rotated_module());
    }
    let expected_entry = plugin_entry_path(&expected_path)?;
    let directory = options
        .directory
        .to_str()
        .ok_or_else(|| scope_error("native project directory is not valid Unicode"))?;
    let response = service
        .get(
            "/api/plugin",
            &[("location[directory]", directory.to_owned())],
        )
        .await?;
    let inventory: LocationEnvelope<Vec<Value>> = decode(response)?;
    if inventory.location.directory != directory {
        return Err(source_error_code(
            "NATIVE_MCP_PROOF_INVENTORY_SCOPE",
            "native plugin inventory is outside the requested location",
        ));
    }
    if inventory.data.len() > 512 {
        return Err(source_error_code(
            "NATIVE_MCP_PROOF_INVENTORY_BOUND",
            "native plugin inventory exceeds its bound",
        ));
    }
    let mut matching = inventory
        .data
        .iter()
        .filter(|plugin| plugin.get("id").and_then(Value::as_str) == Some(PLUGIN_ID));
    let plugin = matching.next().ok_or_else(|| {
        source_error_code(
            "NATIVE_MCP_PROOF_PLUGIN_MISSING",
            "native MCP observer plugin is not loaded",
        )
    })?;
    // In 2.0.7, features.rpc describes a separate rpc file entrypoint.
    // This server plugin registers its RPC through the effect context;
    // arm/read acknowledgements below verify that concrete registration.
    if matching.next().is_some() {
        return Err(source_error_code(
            "NATIVE_MCP_PROOF_PLUGIN_NONUNIQUE",
            "native MCP observer plugin identity is not unique",
        ));
    }
    if plugin["state"]["status"] != "active" || plugin["features"]["server"] != true {
        return Err(source_error_code(
            "NATIVE_MCP_PROOF_PLUGIN_STATE_MISMATCH",
            "native MCP observer plugin is not active with the server feature",
        ));
    }
    if plugin["source"]["type"] != "local" {
        return Err(source_error_code(
            "NATIVE_MCP_PROOF_PLUGIN_SOURCE_MISMATCH",
            "native MCP observer plugin is not loaded from a local source",
        ));
    }
    let loaded_path = plugin["source"]["path"]
        .as_str()
        .filter(|path| !path.is_empty() && path.len() <= 4096)
        .ok_or_else(|| {
            source_error_code(
                "NATIVE_MCP_PROOF_PLUGIN_PATH_MISSING",
                "native MCP observer source path is missing",
            )
        })?;
    let loaded_path = Path::new(loaded_path).canonicalize().map_err(|_| {
        source_error_code(
            "NATIVE_MCP_PROOF_PLUGIN_PATH_INVALID",
            "loaded native MCP observer source path is invalid",
        )
    })?;
    if loaded_path != expected_entry || expected_hash != challenge.module_sha256 {
        return Err(source_error_code(
            "NATIVE_MCP_PROOF_PLUGIN_PATH_MISMATCH",
            "loaded native MCP observer does not match the host-owned module",
        ));
    }
    Ok(())
}

fn rpc_path(method: &str, directory: &str) -> Result<String> {
    if !matches!(method, "arm" | "read") {
        return Err(Error::invalid("invalid native MCP observer method"));
    }
    let mut url = Url::parse("http://127.0.0.1/")
        .map_err(|_| Error::new("NATIVE_ENDPOINT", "cannot construct native RPC route"))?;
    url.set_path(&format!("/api/rpc/{RPC_ID}/{method}"));
    url.query_pairs_mut()
        .append_pair("location[directory]", directory);
    let mut path = url.path().to_owned();
    if let Some(query) = url.query() {
        path.push('?');
        path.push_str(query);
    }
    Ok(path)
}

fn validate_readback(
    raw: &PluginReadback,
    challenge: &NativeMcpChallenge,
    service: &Service,
    options: &Options,
) -> Result<()> {
    let module_path = Path::new(&raw.module_path)
        .canonicalize()
        .map_err(|_| source_error("observer-reported module path is invalid"))?;
    let directory = options
        .directory
        .to_str()
        .ok_or_else(|| scope_error("native project directory is not valid Unicode"))?;
    if raw.plugin_id != PLUGIN_ID
        || module_path != challenge.module_path
        || raw.module_sha256 != challenge.module_sha256
        || raw.service_id != challenge.service_id
        || raw.service_pid != service.pid
        || raw.service_version != PINNED_VERSION
        || raw.directory != directory
        || raw.challenge_id != challenge.challenge_id
        || raw.nonce != challenge.nonce
        || raw.issued_at_ms != challenge.issued_at_ms
        || raw.expires_at_ms != challenge.expires_at_ms
        || raw.session_id != challenge.assignment.native_session_id()
        || raw.expected_model != challenge.model
        || raw.model_consumed != "unknown"
        || model::canonical(&raw.assignment)? != model::canonical(&challenge.assignment.as_value())?
        || raw.observation_sequence > 2
        || raw.receipt_observed_at_ms < challenge.issued_at_ms
        || raw.receipt_observed_at_ms > challenge.expires_at_ms
        || raw.receipt_observed_at_ms > model::now_ms()?
        || raw.native_discovered.status != "observed"
        || raw.native_discovered.observed_at_ms < challenge.issued_at_ms
        || raw.native_discovered.observed_at_ms > challenge.expires_at_ms
        || raw.native_discovered.observed_at_ms > raw.receipt_observed_at_ms
    {
        return Err(challenge_error(
            "native MCP observer readback does not match the exact challenge",
        ));
    }
    validate_native_tools(&raw.native_discovered.tools)?;
    validate_context_evidence(&raw.session_context, challenge)?;
    validate_provider_evidence(&raw.provider_request, challenge)?;
    let expected_sequence = (raw.session_context.status != "unknown") as u64
        + (raw.provider_request.status != "unknown") as u64;
    if raw.observation_sequence != expected_sequence
        || (raw.session_context.agent.is_some()
            && raw.provider_request.agent.is_some()
            && raw.session_context.agent != raw.provider_request.agent)
        || raw
            .session_context
            .observed_at_ms
            .is_some_and(|time| time > raw.receipt_observed_at_ms)
        || raw
            .provider_request
            .observed_at_ms
            .is_some_and(|time| time > raw.receipt_observed_at_ms)
    {
        return Err(challenge_error(
            "native MCP hook sequence or agent identity is inconsistent",
        ));
    }
    Ok(())
}

fn validate_native_tools(tools: &[NativeTool]) -> Result<()> {
    if tools.len() > MAX_TOOLS {
        return Err(schema_error("native MCP tool inventory exceeds its bound"));
    }
    let mut names = BTreeSet::new();
    for tool in tools {
        if !bounded_label(&tool.server, 256)
            || !bounded_label(&tool.name, 256)
            || tool.description.len() > 16 * 1024
            || !tool.input_schema.is_object()
            || !names.insert((tool.server.clone(), tool.name.clone()))
        {
            return Err(schema_error("native MCP tool inventory is invalid"));
        }
        validate_json_size(&tool.input_schema)?;
    }
    validate_serialized_size(tools)?;
    Ok(())
}

fn validate_context_tools(tools: &[ContextTool]) -> Result<()> {
    if tools.len() > MAX_TOOLS {
        return Err(schema_error("observed tool list exceeds its bound"));
    }
    let mut names = BTreeSet::new();
    for tool in tools {
        if !bounded_label(&tool.name, 256)
            || tool.description.len() > 16 * 1024
            || !tool.input_schema.is_object()
            || !names.insert(tool.name.clone())
        {
            return Err(schema_error("observed tool list is invalid"));
        }
        validate_json_size(&tool.input_schema)?;
    }
    validate_serialized_size(tools)?;
    Ok(())
}

fn validate_context_evidence(
    evidence: &SessionContextEvidence,
    challenge: &NativeMcpChallenge,
) -> Result<()> {
    match evidence.status.as_str() {
        "unknown"
            if evidence.stage.is_none()
                && evidence.observed_at_ms.is_none()
                && evidence.agent.is_none()
                && evidence.reason_code.is_none()
                && evidence.tools.is_empty() =>
        {
            Ok(())
        }
        "observed"
            if evidence.stage.as_deref() == Some("session_context_hook")
                && evidence.observed_at_ms.is_some_and(|time| {
                    time >= challenge.issued_at_ms && time <= challenge.expires_at_ms
                })
                && evidence
                    .agent
                    .as_deref()
                    .is_some_and(|agent| bounded_label(agent, 256))
                && evidence.reason_code.is_none() =>
        {
            validate_context_tools(&evidence.tools)
        }
        "unsupported"
            if evidence.stage.as_deref() == Some("session_context_hook")
                && evidence.observed_at_ms.is_some_and(|time| {
                    time >= challenge.issued_at_ms && time <= challenge.expires_at_ms
                })
                && evidence
                    .agent
                    .as_deref()
                    .is_some_and(|agent| bounded_label(agent, 256))
                && evidence.tools.is_empty()
                && evidence.reason_code.as_deref() == Some("context_schema_unrecognized") =>
        {
            Ok(())
        }
        _ => Err(schema_error(
            "session-context observer status is inconsistent",
        )),
    }
}

fn validate_provider_evidence(
    evidence: &ProviderRequestEvidence,
    challenge: &NativeMcpChallenge,
) -> Result<()> {
    match evidence.status.as_str() {
        "unknown"
            if evidence.transport.is_none()
                && evidence.stage.is_none()
                && evidence.kind.is_none()
                && evidence.observed_at_ms.is_none()
                && evidence.agent.is_none()
                && evidence.model.is_none()
                && evidence.reason_code.is_none()
                && evidence.tools.is_empty() =>
        {
            Ok(())
        }
        "observed" | "unsupported"
            if matches!(evidence.transport.as_deref(), Some("http" | "websocket"))
                && evidence.stage.as_deref() == Some("before_transport")
                && evidence.kind.as_deref() == Some("primary")
                && evidence.observed_at_ms.is_some_and(|time| {
                    time >= challenge.issued_at_ms && time <= challenge.expires_at_ms
                })
                && evidence
                    .agent
                    .as_deref()
                    .is_some_and(|agent| bounded_label(agent, 256))
                && evidence.model.as_ref() == Some(&challenge.model) =>
        {
            if evidence.status == "observed" && evidence.reason_code.is_none() {
                validate_context_tools(&evidence.tools)
            } else if evidence.status == "unsupported"
                && evidence.tools.is_empty()
                && evidence
                    .reason_code
                    .as_deref()
                    .is_some_and(valid_reason_code)
            {
                Ok(())
            } else {
                Err(schema_error(
                    "provider-request observer status is inconsistent",
                ))
            }
        }
        _ => Err(schema_error(
            "provider-request observer status is inconsistent",
        )),
    }
}

fn valid_reason_code(code: &str) -> bool {
    matches!(
        code,
        "request_not_object"
            | "tools_field_unrecognized"
            | "tool_count_limit"
            | "tool_schema_limit"
            | "tool_schema_unrecognized"
            | "request_body_limit"
            | "request_json_invalid"
            | "request_unreadable"
            | "frame_limit"
            | "frame_json_invalid"
    )
}

fn bounded_label(value: &str, limit: usize) -> bool {
    !value.is_empty() && value.len() <= limit && !value.bytes().any(|byte| byte.is_ascii_control())
}

fn validate_json_size(value: &Value) -> Result<()> {
    if !value.is_object() || model::canonical(value)?.len() > MAX_EVIDENCE_BYTES {
        return Err(schema_error("tool schema exceeds its evidence bound"));
    }
    Ok(())
}

fn validate_serialized_size<T: Serialize + ?Sized>(value: &T) -> Result<()> {
    let value = serde_json::to_value(value)
        .map_err(|_| schema_error("tool evidence cannot be represented as JSON"))?;
    if model::canonical(&value)?.len() > MAX_EVIDENCE_BYTES {
        return Err(schema_error("tool evidence exceeds its evidence bound"));
    }
    Ok(())
}

fn digest_value(value: &Value) -> Result<String> {
    Ok(format!(
        "sha256:{}",
        model::digest(model::canonical(value)?.as_bytes())
    ))
}

fn project_readback(
    raw: PluginReadback,
    challenge: &NativeMcpChallenge,
    service: &Service,
) -> Result<Value> {
    let native_tools = serde_json::to_value(&raw.native_discovered.tools)
        .map_err(|_| schema_error("native tool schemas cannot be represented as JSON"))?;
    let context_tools = serde_json::to_value(&raw.session_context.tools)
        .map_err(|_| schema_error("session tool schemas cannot be represented as JSON"))?;
    let provider_tools = serde_json::to_value(&raw.provider_request.tools)
        .map_err(|_| schema_error("provider tool schemas cannot be represented as JSON"))?;

    let mut value = json!({
        "contract": "opencode-v2-native-mcp-proof-v1",
        "dispatch_permitted": false,
        "model_consumed": "unknown",
        "assignment": challenge.assignment.as_value(),
        "challenge": {
            "id": challenge.challenge_id,
            "issued_at_ms": challenge.issued_at_ms,
            "expires_at_ms": challenge.expires_at_ms,
        },
        "service": {
            "id": challenge.service_id,
            "pid": service.pid,
            "version": service.version,
            "directory": challenge.directory,
        },
        "observer": {
            "plugin_id": PLUGIN_ID,
            "module_path": challenge.module_path,
            "module_sha256": format!("sha256:{}", challenge.module_sha256),
        },
        "session": {
            "id": challenge.assignment.native_session_id(),
            "model": challenge.model,
        },
        "observation_sequence": raw.observation_sequence,
        "native_discovered": {
            "status": raw.native_discovered.status,
            "observed_at_ms": raw.native_discovered.observed_at_ms,
            "tools": native_tools,
        },
        "session_context": {
            "status": raw.session_context.status,
            "stage": raw.session_context.stage,
            "observed_at_ms": raw.session_context.observed_at_ms,
            "agent": raw.session_context.agent,
            "tools": context_tools,
            "reason_code": raw.session_context.reason_code,
        },
        "provider_request": {
            "status": raw.provider_request.status,
            "transport": raw.provider_request.transport,
            "stage": raw.provider_request.stage,
            "kind": raw.provider_request.kind,
            "observed_at_ms": raw.provider_request.observed_at_ms,
            "agent": raw.provider_request.agent,
            "model": raw.provider_request.model,
            "tools": provider_tools,
            "reason_code": raw.provider_request.reason_code,
        },
        "receipt_observed_at_ms": raw.receipt_observed_at_ms,
    });

    value["native_discovered"]["digest"] = json!(digest_value(&native_tools)?);
    value["session_context"]["digest"] = if raw.session_context.status == "unknown" {
        Value::Null
    } else {
        json!(digest_value(&context_tools)?)
    };
    value["provider_request"]["digest"] = if raw.provider_request.status == "unknown" {
        Value::Null
    } else {
        json!(digest_value(&provider_tools)?)
    };
    let evidence_digest = digest_value(&value)?;
    value["evidence_digest"] = json!(evidence_digest);
    Ok(value)
}

fn module_hash_valid(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn scope_error(message: &str) -> Error {
    Error::new("NATIVE_MCP_PROOF_SCOPE", message)
}

fn source_error(message: &str) -> Error {
    Error::new("NATIVE_MCP_PROOF_SOURCE", message)
}

fn source_error_code(code: &'static str, message: &str) -> Error {
    Error::new(code, message)
}

fn challenge_error(message: &str) -> Error {
    Error::new("NATIVE_MCP_PROOF_CHALLENGE", message)
}

fn expired_challenge() -> Error {
    Error::new(
        "NATIVE_MCP_PROOF_CHALLENGE_EXPIRED",
        "native MCP challenge expired before it could be used",
    )
}

fn rotated_module() -> Error {
    Error::new(
        "NATIVE_MCP_PROOF_MODULE_ROTATED",
        "host-owned native MCP observer module changed after challenge preparation",
    )
}

fn schema_error(message: &str) -> Error {
    Error::new("NATIVE_MCP_PROOF_SCHEMA", message)
}

#[cfg(test)]
mod plugin_package_path_tests {
    use super::{
        module_source, plugin_config_package_path, plugin_config_package_path_text,
        plugin_config_value,
    };
    use crate::runtime::opencode_v2::{ModelRef, Options, mcp_plugin};
    use std::path::PathBuf;

    #[test]
    fn plugin_package_path_projects_verbatim_drive_and_unc_paths() {
        assert_eq!(
            plugin_config_package_path_text(r"\\?\C:\repo\modules\opencode").unwrap(),
            r"C:\repo\modules\opencode",
        );
        assert_eq!(
            plugin_config_package_path_text(r"\\?\UNC\server\share\repo\plugin").unwrap(),
            r"\\server\share\repo\plugin",
        );
        assert_eq!(
            plugin_config_package_path_text(r"\\?\unc\server\share\repo\plugin").unwrap(),
            r"\\server\share\repo\plugin",
        );
        assert_eq!(
            plugin_config_package_path_text(r"C:\repo\modules\opencode").unwrap(),
            r"C:\repo\modules\opencode",
        );
        assert_eq!(
            plugin_config_package_path_text("/repo/modules/opencode").unwrap(),
            "/repo/modules/opencode",
        );
        assert!(plugin_config_package_path_text(r"\\?\Volume{abc}\repo\plugin").is_err());
        assert!(plugin_config_package_path_text(r"\\?\UNC\server").is_err());
    }

    #[test]
    fn plugin_package_path_config_roundtrips_through_source_validation() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let options = Options {
            service_id: "plugin_path_fixture".to_owned(),
            connection_file: root.join("unused-connection.json"),
            expected_version: "2.0.7".to_owned(),
            directory: root.clone(),
            model: ModelRef {
                id: "fixture-model".to_owned(),
                provider_id: "fixture-provider".to_owned(),
                variant: "fixture-variant".to_owned(),
            },
        };

        let config = plugin_config_value(&options).unwrap();
        let (module_path, _) = module_source().unwrap();
        let package_dir = module_path.parent().unwrap();
        let expected_package = plugin_config_package_path(package_dir).unwrap();
        assert_eq!(config["package"].as_str(), Some(expected_package.as_str()));
        if cfg!(windows) {
            assert!(!expected_package.starts_with(r"\\?\"));
        }

        let serialized_config = serde_json::json!({
            "plugin": [[config["package"].clone(), config["options"].clone()]]
        });
        mcp_plugin::verify_plugin_config_value(&serialized_config, &options.service_id).unwrap();
        assert!(package_dir.is_absolute());
    }
}
