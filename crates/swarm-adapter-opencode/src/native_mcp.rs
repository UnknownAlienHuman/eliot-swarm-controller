//! Consumer for the schema-1 private effect carried by the
//! Store-admitted `swarm.native_mcp_command@2` DTO.
//!
//! This module owns only the OpenCode HTTP effects.  The Store constructs the
//! envelope after C8 reservation and remains the authority for assignment,
//! challenge, descriptor, and sanitized readback validation.  The envelope is
//! intentionally a Value at this boundary so the neutral schema stays in
//! `swarm-contracts`; this module never invents an alternate public DTO.

use crate::{config::NativeOptions, native::NativeClient};
use reqwest::Url;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::Digest;
use std::time::{SystemTime, UNIX_EPOCH};
use swarm_contracts::{
    error::{Error, Result},
    module_catalog::Sha256Digest,
    native_mcp::{
        NativeMcpAssignmentReadback, NativeMcpObservationKind, NativeMcpServerObservation,
        NativeMcpServerStatus, NativeMcpSessionObservation,
    },
    runtime::{EffectOutcome, RuntimeCommand},
};

pub const SCHEMA_ID: &str = "swarm.native_mcp_command";
pub const SCHEMA_VERSION: u64 = 1;
const MAX_ENVELOPE_BYTES: usize = 1_048_576;
const MAX_PATH_BYTES: usize = 16 * 1024;
const MAX_ID_BYTES: usize = 256;
const MAX_COMMAND_ARGS: usize = 64;
const MAX_ARG_BYTES: usize = 32 * 1024;
const MAX_SERVER_ENTRIES: usize = 256;
const PINNED_OPENCODE_VERSION: &str = "2.0.7";
const RPC_ID: &str = "eliot.native-mcp-proof.v1";

/// A native request failure carries the outcome classification that the
/// Store's existing RuntimeOutcome path records.  A rejected validation has
/// no native effect; a transport failure after PUT/POST is deliberately
/// unknown and is never replayed by this adapter.
pub struct EffectFailure {
    pub error: Error,
    pub outcome: EffectOutcome,
}

pub type EffectResult<T = Value> = std::result::Result<T, EffectFailure>;

struct RequestSpec {
    method: String,
    path: String,
    body: Option<Value>,
}

struct ParsedCommand {
    action: String,
    observation_kind: Option<NativeMcpObservationKind>,
    service_pid: u32,
    request: RequestSpec,
    precondition: Option<RequestSpec>,
    readback: Option<RequestSpec>,
    prepared: Value,
    challenge: Option<Value>,
    assignment_observation: Option<NativeMcpAssignmentObservationArtifact>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeMcpAssignmentObservationArtifact {
    schema_version: u16,
    kind: String,
    assignment: Value,
    assignment_sha256: Sha256Digest,
    native_session_id: String,
    location_sha256: Sha256Digest,
    service_id: String,
    service_pid: u32,
    service_version: String,
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
struct NativeMcpServerResponse {
    name: String,
    status: NativeMcpStatusResponse,
    #[serde(rename = "integrationID", default)]
    integration_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(tag = "status", deny_unknown_fields)]
enum NativeMcpStatusResponse {
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

/// Execute exactly one Store-admitted action.  Install performs one GET
/// precondition, one PUT, and one GET readback.  Observe is GET-only.  Arm and
/// read each perform exactly one POST.  The journal in `lib.rs` persists the
/// intent before this function is called and persists the outcome before it
/// acknowledges it to Store.
pub async fn execute(
    native: &NativeClient,
    command: &RuntimeCommand,
    options: &NativeOptions,
) -> EffectResult {
    let parsed = parse(command, options)?;
    native
        .verify_mcp_service()
        .await
        .map_err(|error| read_failure(error, false))?;
    if parsed.service_pid != native.process_id() {
        return Err(reject(
            "NATIVE_INSTANCE_CHANGED",
            "native MCP command targets another retained service process",
        ));
    }
    if parsed
        .assignment_observation
        .as_ref()
        .is_some_and(|observation| {
            observation.service_id != options.service_id
                || observation.service_pid != native.process_id()
                || observation.service_version != PINNED_OPENCODE_VERSION
                || observation.service_version != native.process_version()
        })
    {
        return Err(reject(
            "NATIVE_INSTANCE_CHANGED",
            "assigned-session observation targets another OpenCode service identity",
        ));
    }

    let result = match parsed.action.as_str() {
        "install" => execute_install(native, options, &parsed).await,
        "observe" => execute_observe(native, options, &parsed).await,
        "arm" => execute_arm(native, options, &parsed).await,
        "read" => execute_read(native, options, &parsed).await,
        _ => Err(reject(
            "NATIVE_MCP_COMMAND_ACTION",
            "native MCP command action is unsupported",
        )),
    }?;

    native.verify_mcp_service().await.map_err(|error| {
        read_failure(
            error,
            matches!(parsed.action.as_str(), "install" | "arm" | "read"),
        )
    })?;
    Ok(result)
}

async fn execute_install(
    native: &NativeClient,
    options: &NativeOptions,
    parsed: &ParsedCommand,
) -> EffectResult {
    let precondition = parsed.precondition.as_ref().ok_or_else(|| {
        reject(
            "NATIVE_MCP_COMMAND_INPUT",
            "install command has no exact precondition GET",
        )
    })?;
    let readback = parsed.readback.as_ref().ok_or_else(|| {
        reject(
            "NATIVE_MCP_COMMAND_INPUT",
            "install command has no exact readback GET",
        )
    })?;
    let precondition_value = native
        .mcp_get(&precondition.path)
        .await
        .map_err(|error| read_failure(error, false))?;
    ensure_install_absent(&precondition_value, parsed, options)?;

    let request = &parsed.request;
    let body = request.body.clone().ok_or_else(|| {
        reject(
            "NATIVE_MCP_COMMAND_INPUT",
            "install PUT has no prepared request body",
        )
    })?;
    let response = native
        .mcp_put(&request.path, body)
        .await
        .map_err(effect_failure)?;
    let observed = native
        .mcp_get(&readback.path)
        .await
        .map_err(|error| read_failure(error, true))?;
    let readback = project_install_readback(&observed, parsed, native, options, true)?;
    Ok(json!({
        "schema_version": SCHEMA_VERSION,
        "kind": "native_mcp_effect_receipt",
        "action": "install",
        "put_response_digest": digest_value(&response)
            .map_err(|error| read_failure(error, true))?,
        "readback": readback,
        "native_replay": false,
    }))
}

async fn execute_observe(
    native: &NativeClient,
    options: &NativeOptions,
    parsed: &ParsedCommand,
) -> EffectResult {
    match parsed.observation_kind {
        Some(NativeMcpObservationKind::InstalledServer) => {
            execute_installed_server_observe(native, options, parsed).await
        }
        Some(NativeMcpObservationKind::AssignedSession) => {
            execute_assigned_session_observe(native, options, parsed).await
        }
        None => Err(reject(
            "NATIVE_MCP_COMMAND_INPUT",
            "observe command has no admitted observation purpose",
        )),
    }
}

async fn execute_installed_server_observe(
    native: &NativeClient,
    options: &NativeOptions,
    parsed: &ParsedCommand,
) -> EffectResult {
    let response = native
        .mcp_get(&parsed.request.path)
        .await
        .map_err(|error| read_failure(error, false))?;
    let readback = project_install_readback(&response, parsed, native, options, false)?;
    Ok(json!({
        "schema_version": SCHEMA_VERSION,
        "kind": "native_mcp_effect_receipt",
        "action": "observe",
        "observation_kind": "installed_server",
        "readback": readback,
        "native_replay": false,
    }))
}

async fn execute_assigned_session_observe(
    native: &NativeClient,
    options: &NativeOptions,
    parsed: &ParsedCommand,
) -> EffectResult {
    let readback_request = parsed.readback.as_ref().ok_or_else(|| {
        reject(
            "NATIVE_MCP_COMMAND_INPUT",
            "assigned-session observe has no exact session GET",
        )
    })?;
    let mcp_response = native
        .mcp_get(&parsed.request.path)
        .await
        .map_err(|error| read_failure(error, false))?;
    // The first verification is in execute() before this MCP GET. This check
    // separates both reads so a replaced service cannot supply a mixed proof.
    native
        .verify_mcp_service()
        .await
        .map_err(|error| read_failure(error, false))?;
    let session_response = native
        .mcp_get(&readback_request.path)
        .await
        .map_err(|error| read_failure(error, false))?;
    // Verify after the session GET as well; execute() performs a final check
    // after the typed receipt has been projected.
    native
        .verify_mcp_service()
        .await
        .map_err(|error| read_failure(error, false))?;

    let readback =
        project_assignment_readback(&mcp_response, session_response, parsed, native, options)?;
    Ok(json!({
        "schema_version": SCHEMA_VERSION,
        "kind": "native_mcp_effect_receipt",
        "action": "observe",
        "observation_kind": "assigned_session",
        "readback": readback,
        "native_replay": false,
    }))
}

async fn execute_arm(
    native: &NativeClient,
    _options: &NativeOptions,
    parsed: &ParsedCommand,
) -> EffectResult {
    let body = parsed.request.body.clone().ok_or_else(|| {
        reject(
            "NATIVE_MCP_COMMAND_INPUT",
            "arm POST has no prepared challenge body",
        )
    })?;
    let response = native
        .mcp_post(&parsed.request.path, body)
        .await
        .map_err(effect_failure)?;
    let output = response.get("output").ok_or_else(|| {
        unknown_failure(
            "NATIVE_MCP_ACK_SCHEMA",
            "native observer arm response lacks output",
        )
    })?;
    validate_arm_ack(output, parsed.challenge.as_ref())?;
    Ok(json!({
        "schema_version": SCHEMA_VERSION,
        "kind": "native_mcp_effect_receipt",
        "action": "arm",
        "ack": {
            "accepted": true,
            "challenge_id": output["challenge_id"],
            "nonce": output["nonce"],
            "module_sha256": output["module_sha256"],
            "service_id": output["service_id"],
            "service_pid": output["service_pid"],
            "service_version": output["service_version"],
        },
        "response": response,
        "native_replay": false,
    }))
}

async fn execute_read(
    native: &NativeClient,
    _options: &NativeOptions,
    parsed: &ParsedCommand,
) -> EffectResult {
    let body = parsed.request.body.clone().ok_or_else(|| {
        reject(
            "NATIVE_MCP_COMMAND_INPUT",
            "read POST has no prepared challenge body",
        )
    })?;
    let response = native
        .mcp_post(&parsed.request.path, body)
        .await
        .map_err(effect_failure)?;
    bounded_json(&response)?;
    Ok(json!({
        "schema_version": SCHEMA_VERSION,
        "kind": "native_mcp_effect_receipt",
        "action": "read",
        "challenge": parsed.challenge.clone().unwrap_or(Value::Null),
        "response": response,
        "native_replay": false,
    }))
}

fn parse(command: &RuntimeCommand, options: &NativeOptions) -> EffectResult<ParsedCommand> {
    let input = command.input.as_object().ok_or_else(|| {
        reject(
            "NATIVE_MCP_COMMAND_INPUT",
            "native MCP command input must be an object",
        )
    })?;
    if input.get("schema_version") != Some(&Value::from(SCHEMA_VERSION))
        || input.get("kind").and_then(Value::as_str) != Some(SCHEMA_ID)
    {
        return Err(reject(
            "NATIVE_MCP_COMMAND_SCHEMA",
            "native MCP private effect schema is not swarm.native_mcp_command@1",
        ));
    }
    let action = input
        .get("action")
        .and_then(Value::as_str)
        .filter(|value| matches!(*value, "install" | "observe" | "arm" | "read"))
        .ok_or_else(|| {
            reject(
                "NATIVE_MCP_COMMAND_ACTION",
                "native MCP command action is missing or unsupported",
            )
        })?
        .to_owned();
    let observation_kind = if action == "observe" {
        Some(
            match input.get("observation_kind").and_then(Value::as_str) {
                Some("installed_server") => NativeMcpObservationKind::InstalledServer,
                Some("assigned_session") => NativeMcpObservationKind::AssignedSession,
                _ => {
                    return Err(reject(
                        "NATIVE_MCP_COMMAND_INPUT",
                        "observe command has no supported observation purpose",
                    ));
                }
            },
        )
    } else {
        if input.contains_key("observation_kind") {
            return Err(reject(
                "NATIVE_MCP_COMMAND_INPUT",
                "non-observe command cannot carry an observation purpose",
            ));
        }
        None
    };
    let scope = input.get("scope").cloned().ok_or_else(|| {
        reject(
            "NATIVE_MCP_COMMAND_SCOPE",
            "native MCP command has no exact binding/service scope",
        )
    })?;
    validate_scope(&scope, command, options)?;
    let prepared = input.get("prepared").cloned().ok_or_else(|| {
        reject(
            "NATIVE_MCP_COMMAND_INPUT",
            "native MCP command has no Store-prepared private request",
        )
    })?;
    if !prepared.is_object() {
        return Err(reject(
            "NATIVE_MCP_COMMAND_INPUT",
            "native MCP prepared input must be an object",
        ));
    }
    let request = parse_request(input.get("request"), options, &action)?;
    let precondition = input
        .get("precondition")
        .filter(|value| !value.is_null())
        .map(|value| parse_request(Some(value), options, "observe"))
        .transpose()?;
    let readback = input
        .get("readback")
        .filter(|value| !value.is_null())
        .map(|value| parse_request(Some(value), options, "observe"))
        .transpose()?;
    let challenge = input
        .get("challenge")
        .filter(|value| !value.is_null())
        .cloned();
    if matches!(action.as_str(), "arm" | "read") {
        let challenge = challenge.as_ref().ok_or_else(|| {
            reject(
                "NATIVE_MCP_COMMAND_SCOPE",
                "observer action has no retained challenge metadata",
            )
        })?;
        validate_prepared_challenge(&prepared, challenge)?;
        validate_challenge(challenge, &request)?;
    }
    if action == "install" {
        validate_install_request(&request, &prepared, options)?;
        if precondition.is_none() || readback.is_none() {
            return Err(reject(
                "NATIVE_MCP_COMMAND_INPUT",
                "install requires both precondition and readback GETs",
            ));
        }
    } else if action == "observe" {
        match observation_kind {
            Some(NativeMcpObservationKind::InstalledServer) => {
                validate_observe_request(&request, &prepared, options)?;
            }
            Some(NativeMcpObservationKind::AssignedSession) => {
                if input.contains_key("precondition") || input.contains_key("challenge") {
                    return Err(reject(
                        "NATIVE_MCP_COMMAND_INPUT",
                        "assigned-session observe cannot carry a precondition or challenge",
                    ));
                }
            }
            None => {
                return Err(reject(
                    "NATIVE_MCP_COMMAND_INPUT",
                    "observe command has no admitted observation purpose",
                ));
            }
        }
    } else {
        validate_rpc_request(&request, &action, &challenge, options)?;
    }
    let assignment_observation =
        if observation_kind == Some(NativeMcpObservationKind::AssignedSession) {
            Some(validate_assignment_observe_request(
                &request,
                precondition.as_ref(),
                readback.as_ref(),
                &prepared,
                challenge.as_ref(),
                &scope,
                options,
            )?)
        } else {
            None
        };
    let serialized = serde_json::to_vec(&command.input).map_err(|_| {
        reject(
            "NATIVE_MCP_COMMAND_INPUT",
            "native MCP command could not be bounded",
        )
    })?;
    if serialized.len() > MAX_ENVELOPE_BYTES {
        return Err(reject(
            "NATIVE_MCP_COMMAND_INPUT_LIMIT",
            "native MCP command exceeds its bounded handoff size",
        ));
    }
    Ok(ParsedCommand {
        action,
        observation_kind,
        service_pid: scope["service_pid"]
            .as_u64()
            .and_then(|pid| u32::try_from(pid).ok())
            .ok_or_else(|| reject("NATIVE_MCP_COMMAND_SCOPE", "native service PID is invalid"))?,
        request,
        precondition,
        readback,
        prepared,
        challenge,
        assignment_observation,
    })
}

fn validate_scope(
    scope: &Value,
    command: &RuntimeCommand,
    options: &NativeOptions,
) -> EffectResult<()> {
    let object = scope.as_object().ok_or_else(|| {
        reject(
            "NATIVE_MCP_COMMAND_SCOPE",
            "native MCP command scope must be an object",
        )
    })?;
    for key in [
        "binding_id",
        "binding_generation",
        "native_scope_key",
        "service_id",
        "service_pid",
        "directory",
        "assignment",
    ] {
        if !object.contains_key(key) {
            return Err(reject(
                "NATIVE_MCP_COMMAND_SCOPE",
                "native MCP command scope is incomplete",
            ));
        }
    }
    if scope["binding_id"] != command.binding_id
        || scope["binding_generation"] != command.generation
        || scope["native_scope_key"] != options.scope_key()
        || scope["service_id"] != options.service_id
        || scope["service_pid"].as_u64().is_none_or(|pid| pid == 0)
        || scope["directory"] != options.directory.to_str().unwrap_or_default()
    {
        return Err(reject(
            "NATIVE_MCP_SCOPE_MISMATCH",
            "native MCP command scope differs from the selected module route",
        ));
    }
    let assignment = scope["assignment"].as_object().ok_or_else(|| {
        reject(
            "NATIVE_MCP_COMMAND_SCOPE",
            "native MCP assignment scope must be an object",
        )
    })?;
    if assignment.get("binding_id") != Some(&Value::String(command.binding_id.clone()))
        || assignment.get("binding_generation") != Some(&Value::from(command.generation))
        || assignment
            .get("native_session_id")
            .and_then(Value::as_str)
            .is_none_or(|value| !value.starts_with("ses_") || value.len() > MAX_ID_BYTES)
    {
        return Err(reject(
            "NATIVE_MCP_SCOPE_MISMATCH",
            "native MCP assignment is outside the authenticated binding generation",
        ));
    }
    Ok(())
}

fn parse_request(
    value: Option<&Value>,
    options: &NativeOptions,
    action: &str,
) -> EffectResult<RequestSpec> {
    let value = value.ok_or_else(|| {
        reject(
            "NATIVE_MCP_COMMAND_INPUT",
            "native MCP action has no prepared HTTP request",
        )
    })?;
    let object = value.as_object().ok_or_else(|| {
        reject(
            "NATIVE_MCP_COMMAND_INPUT",
            "native MCP HTTP request must be an object",
        )
    })?;
    let method = object
        .get("method")
        .and_then(Value::as_str)
        .filter(|value| value.len() <= 8)
        .ok_or_else(|| {
            reject(
                "NATIVE_MCP_COMMAND_INPUT",
                "native MCP HTTP method is invalid",
            )
        })?
        .to_owned();
    let path = object
        .get("path")
        .and_then(Value::as_str)
        .filter(|value| {
            !value.is_empty() && value.len() <= MAX_PATH_BYTES && value.starts_with("/api/")
        })
        .ok_or_else(|| {
            reject(
                "NATIVE_MCP_COMMAND_INPUT",
                "native MCP HTTP path is invalid",
            )
        })?
        .to_owned();
    if path.contains("://") || path.bytes().any(|byte| byte.is_ascii_control()) {
        return Err(reject(
            "NATIVE_MCP_COMMAND_INPUT",
            "native MCP HTTP path contains an invalid origin or control byte",
        ));
    }
    validate_location_query(&path, options)?;
    if action == "install" && method != "PUT"
        || action == "observe" && method != "GET"
        || matches!(action, "arm" | "read") && method != "POST"
    {
        return Err(reject(
            "NATIVE_MCP_COMMAND_INPUT",
            "native MCP HTTP method does not match its action",
        ));
    }
    let body = match object.get("body") {
        None | Some(Value::Null) => None,
        Some(value) => {
            bounded_json(value)?;
            Some(value.clone())
        }
    };
    Ok(RequestSpec { method, path, body })
}

fn validate_location_query(path: &str, options: &NativeOptions) -> EffectResult<()> {
    let url = Url::parse(&format!("http://127.0.0.1{path}")).map_err(|_| {
        reject(
            "NATIVE_MCP_COMMAND_INPUT",
            "native MCP path is not a valid local URL path",
        )
    })?;
    let expected = options.directory.to_str().unwrap_or_default();
    let pairs = url.query_pairs().collect::<Vec<_>>();
    let location = pairs
        .iter()
        .filter(|(key, _)| key == "location[directory]")
        .map(|(_, value)| value.as_ref())
        .collect::<Vec<_>>();
    if pairs.len() != 1 || location.len() != 1 || location[0] != expected {
        return Err(reject(
            "NATIVE_MCP_SCOPE_MISMATCH",
            "native MCP request is not location scoped to the selected directory",
        ));
    }
    Ok(())
}

fn validate_install_request(
    request: &RequestSpec,
    prepared: &Value,
    options: &NativeOptions,
) -> EffectResult<()> {
    let server_name = prepared["server_name"].as_str().ok_or_else(|| {
        reject(
            "NATIVE_MCP_COMMAND_INPUT",
            "install command lacks its prepared server name",
        )
    })?;
    let expected_path = format!("/api/experimental/mcp/{server_name}");
    let parsed_path = Url::parse(&format!("http://127.0.0.1{}", request.path))
        .map_err(|_| reject("NATIVE_MCP_COMMAND_INPUT", "install path is invalid"))?;
    if parsed_path.path() != expected_path {
        return Err(reject(
            "NATIVE_MCP_COMMAND_INPUT",
            "install path is outside the pinned OpenCode MCP route",
        ));
    }
    let config = request
        .body
        .as_ref()
        .and_then(|body| body.get("config"))
        .ok_or_else(|| reject("NATIVE_MCP_COMMAND_INPUT", "install body lacks config"))?;
    if config["type"] != "local"
        || config["cwd"] != options.directory.to_str().unwrap_or_default()
        || config["disabled"] != false
    {
        return Err(reject(
            "NATIVE_MCP_SCOPE_MISMATCH",
            "install config differs from the admitted location",
        ));
    }
    let command = config["command"].as_array().ok_or_else(|| {
        reject(
            "NATIVE_MCP_COMMAND_INPUT",
            "install config lacks the actual prepared command",
        )
    })?;
    if command.is_empty() || command.len() > MAX_COMMAND_ARGS {
        return Err(reject(
            "NATIVE_MCP_COMMAND_INPUT",
            "prepared MCP command has an invalid argument count",
        ));
    }
    for argument in command {
        let text = argument.as_str().filter(|value| {
            !value.is_empty()
                && value.len() <= MAX_ARG_BYTES
                && !value.bytes().any(|byte| byte.is_ascii_control())
        });
        if text.is_none() {
            return Err(reject(
                "NATIVE_MCP_COMMAND_INPUT",
                "prepared MCP command contains an invalid argument",
            ));
        }
    }
    if prepared["install_intent"].as_object().is_none()
        || prepared["command_sha256"].as_str().is_none()
        || prepared["location_sha256"].as_str().is_none()
        || prepared["executable_sha256"].as_str().is_none()
    {
        return Err(reject(
            "NATIVE_MCP_COMMAND_INPUT",
            "install command lacks its retained prepared identity",
        ));
    }
    Ok(())
}

fn validate_observe_request(
    request: &RequestSpec,
    prepared: &Value,
    _options: &NativeOptions,
) -> EffectResult<()> {
    let parsed_path = Url::parse(&format!("http://127.0.0.1{}", request.path))
        .map_err(|_| reject("NATIVE_MCP_COMMAND_INPUT", "observe path is invalid"))?;
    if request.method != "GET"
        || request.path != request.path.trim()
        || parsed_path.path() != "/api/mcp"
        || prepared["server_name"].as_str().is_none()
        || prepared["install_intent"].as_object().is_none()
    {
        return Err(reject(
            "NATIVE_MCP_COMMAND_INPUT",
            "observe command is not the exact location-scoped MCP readback",
        ));
    }
    Ok(())
}

fn validate_assignment_observe_request(
    request: &RequestSpec,
    precondition: Option<&RequestSpec>,
    readback: Option<&RequestSpec>,
    prepared: &Value,
    challenge: Option<&Value>,
    scope: &Value,
    options: &NativeOptions,
) -> EffectResult<NativeMcpAssignmentObservationArtifact> {
    if precondition.is_some() || challenge.is_some() {
        return Err(reject(
            "NATIVE_MCP_COMMAND_INPUT",
            "assigned-session observe cannot carry a precondition or challenge",
        ));
    }
    let observation: NativeMcpAssignmentObservationArtifact =
        serde_json::from_value(prepared.clone()).map_err(|_| {
            reject(
                "NATIVE_MCP_COMMAND_INPUT",
                "assigned-session prepared artifact schema is invalid",
            )
        })?;
    let readback = readback.ok_or_else(|| {
        reject(
            "NATIVE_MCP_COMMAND_INPUT",
            "assigned-session observe has no exact session readback GET",
        )
    })?;
    let directory = options.directory.to_str().unwrap_or_default();
    let expected_session_path = format!("/api/session/{}", observation.native_session_id);
    let request_url = Url::parse(&format!("http://127.0.0.1{}", request.path))
        .map_err(|_| reject("NATIVE_MCP_COMMAND_INPUT", "observe path is invalid"))?;
    let readback_url = Url::parse(&format!("http://127.0.0.1{}", readback.path)).map_err(|_| {
        reject(
            "NATIVE_MCP_COMMAND_INPUT",
            "session readback path is invalid",
        )
    })?;
    let assignment = scope.get("assignment").ok_or_else(|| {
        reject(
            "NATIVE_MCP_COMMAND_SCOPE",
            "assigned-session observation has no assignment scope",
        )
    })?;
    let service_pid = scope["service_pid"].as_u64();
    let assignment_digest = digest_value_raw(assignment)?;
    let location_digest = format!("{:x}", sha2::Sha256::digest(directory.as_bytes()));

    if observation.schema_version != 1
        || observation.kind != "swarm.native_mcp_assignment_observation"
        || observation.assignment != *assignment
        || observation.assignment_sha256.as_str() != assignment_digest
        || observation.location_sha256.as_str() != location_digest
        || observation.service_id != options.service_id
        || scope.get("service_id").and_then(Value::as_str) != Some(observation.service_id.as_str())
        || service_pid != Some(u64::from(observation.service_pid))
        || observation.service_version != PINNED_OPENCODE_VERSION
        || scope.get("expected_version").and_then(Value::as_str)
            != Some(observation.service_version.as_str())
        || observation.native_session_id
            != assignment["native_session_id"].as_str().unwrap_or_default()
        || !valid_session_path_segment(&observation.native_session_id)
        || request.method != "GET"
        || request.body.is_some()
        || request.path != request.path.trim()
        || request_url.path() != "/api/mcp"
        || request_url.fragment().is_some()
        || readback.method != "GET"
        || readback.body.is_some()
        || readback.path != readback.path.trim()
        || readback_url.path() != expected_session_path
        || readback_url.fragment().is_some()
        || scope.get("directory").and_then(Value::as_str) != Some(directory)
    {
        return Err(reject(
            "NATIVE_MCP_SCOPE_MISMATCH",
            "assigned-session observation differs from its exact service, assignment, or routes",
        ));
    }
    Ok(observation)
}

fn valid_session_path_segment(value: &str) -> bool {
    value.starts_with("ses_")
        && value.len() <= MAX_ID_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._:-".contains(&byte))
}

fn digest_value_raw(value: &Value) -> EffectResult<String> {
    crate::native_mcp_intake::digest_json(value).map_err(|_| {
        reject(
            "NATIVE_MCP_COMMAND_INPUT",
            "native MCP identity could not be canonicalized",
        )
    })
}

fn validate_rpc_request(
    request: &RequestSpec,
    action: &str,
    challenge: &Option<Value>,
    _options: &NativeOptions,
) -> EffectResult<()> {
    let expected_suffix = format!("/api/rpc/{RPC_ID}/{action}");
    let parsed_path = Url::parse(&format!("http://127.0.0.1{}", request.path))
        .map_err(|_| reject("NATIVE_MCP_COMMAND_INPUT", "observer RPC path is invalid"))?;
    if request.method != "POST"
        || parsed_path.path() != expected_suffix
        || request
            .body
            .as_ref()
            .and_then(|body| body.get("input"))
            .and_then(Value::as_object)
            .is_none()
    {
        return Err(reject(
            "NATIVE_MCP_COMMAND_INPUT",
            "observer command is not the exact prepared RPC request",
        ));
    }
    let input = request
        .body
        .as_ref()
        .and_then(|body| body.get("input"))
        .unwrap();
    let challenge = challenge.as_ref().unwrap();
    for key in ["challenge_id", "nonce"] {
        if input.get(key) != challenge.get(key) {
            return Err(reject(
                "NATIVE_MCP_SCOPE_MISMATCH",
                "observer request challenge differs from retained challenge metadata",
            ));
        }
    }
    Ok(())
}

fn validate_challenge(challenge: &Value, request: &RequestSpec) -> EffectResult<()> {
    if challenge["challenge_id"].as_str().is_none()
        || challenge["nonce"].as_str().is_none()
        || challenge["assignment"].as_object().is_none()
        || challenge["service_id"].as_str().is_none()
        || challenge["service_version"].as_str().is_none()
        || challenge["directory"].as_str().is_none()
        || challenge["module_sha256"]
            .as_str()
            .is_none_or(|hash| hash.len() != 64)
        || request
            .body
            .as_ref()
            .and_then(|body| body.get("input"))
            .is_none()
    {
        return Err(reject(
            "NATIVE_MCP_COMMAND_SCOPE",
            "observer command challenge metadata is incomplete",
        ));
    }
    Ok(())
}

fn validate_prepared_challenge(prepared: &Value, challenge: &Value) -> EffectResult<()> {
    let retained = prepared["challenge"].as_object().ok_or_else(|| {
        reject(
            "NATIVE_MCP_COMMAND_INPUT",
            "observer command lacks its retained challenge projection",
        )
    })?;
    for key in [
        "challenge_id",
        "nonce",
        "issued_at_ms",
        "expires_at_ms",
        "service_id",
        "service_pid",
        "service_version",
        "directory",
        "module_sha256",
        "assignment",
    ] {
        if retained.get(key) != challenge.get(key) {
            return Err(reject(
                "NATIVE_MCP_SCOPE_MISMATCH",
                "observer request differs from its retained challenge",
            ));
        }
    }
    Ok(())
}

fn ensure_install_absent(
    value: &Value,
    parsed: &ParsedCommand,
    options: &NativeOptions,
) -> EffectResult<()> {
    if value["location"]["directory"] != options.directory.to_str().unwrap_or_default() {
        return Err(reject(
            "NATIVE_MCP_SCOPE_MISMATCH",
            "install precondition returned another OpenCode location",
        ));
    }
    let servers = value["data"].as_array().ok_or_else(|| {
        reject(
            "NATIVE_MCP_SCHEMA",
            "install precondition response lacks its bounded server list",
        )
    })?;
    if servers.len() > MAX_SERVER_ENTRIES {
        return Err(reject(
            "NATIVE_MCP_SCHEMA",
            "install precondition returned too many MCP servers",
        ));
    }
    let server_name = parsed.prepared["server_name"].as_str().unwrap_or_default();
    if servers.iter().any(|server| server["name"] == server_name) {
        return Err(reject(
            "NATIVE_MCP_INSTALL_CONFLICT",
            "derived runtime MCP name already exists; refusing to replace it",
        ));
    }
    Ok(())
}

fn project_install_readback(
    value: &Value,
    parsed: &ParsedCommand,
    native: &NativeClient,
    options: &NativeOptions,
    effect_already_attempted: bool,
) -> EffectResult<Value> {
    if value["location"]["directory"] != options.directory.to_str().unwrap_or_default() {
        return Err(reject(
            "NATIVE_MCP_SCOPE_MISMATCH",
            "OpenCode MCP readback returned another location",
        ));
    }
    let servers = value["data"].as_array().ok_or_else(|| {
        readback_failure(
            "NATIVE_MCP_SCHEMA",
            "OpenCode MCP readback lacks its bounded server list",
            effect_already_attempted,
        )
    })?;
    if servers.len() > MAX_SERVER_ENTRIES {
        return Err(readback_failure(
            "NATIVE_MCP_SCHEMA",
            "OpenCode MCP readback returned too many servers",
            effect_already_attempted,
        ));
    }
    let server_name = parsed.prepared["server_name"].as_str().unwrap_or_default();
    let mut matching = servers
        .iter()
        .filter(|server| server["name"] == server_name);
    let server = matching.next().ok_or_else(|| {
        readback_failure(
            "NATIVE_OUTCOME_UNKNOWN",
            "OpenCode accepted the install but exact name readback is absent",
            effect_already_attempted,
        )
    })?;
    if matching.next().is_some() {
        return Err(readback_failure(
            "NATIVE_MCP_SCHEMA",
            "OpenCode returned duplicate MCP server names",
            effect_already_attempted,
        ));
    }
    let status = server["status"]["status"]
        .as_str()
        .or_else(|| server["status"].as_str())
        .filter(|value| {
            matches!(
                *value,
                "connected" | "pending" | "disabled" | "failed" | "needs_auth"
            )
        })
        .ok_or_else(|| {
            readback_failure(
                "NATIVE_MCP_SCHEMA",
                "OpenCode MCP status is outside its pinned schema",
                effect_already_attempted,
            )
        })?;
    Ok(json!({
        "schema_version":1,
        "kind":"opencode_v2_mcp_install_readback",
        "intent":parsed.prepared["install_intent"],
        "runtime_entry_present":true,
        "runtime_status":status,
        "runtime_error_present":matches!(status,"failed" | "needs_auth") || !server["status"]["error"].is_null(),
        "runtime_config_readback":"not_exposed_by_pinned_api",
        "matches_prepared_command":"unknown",
        "service_process_id":native.process_id(),
        "service_version":native.process_version(),
        "observed_at_ms":now_ms(),
        "native_tool_set":"unknown",
        "provider_request_context":"unknown",
        "model_consumption":"unknown",
        "dispatch_permitted":false
    }))
}

fn project_assignment_readback(
    mcp_value: &Value,
    session_value: Value,
    parsed: &ParsedCommand,
    native: &NativeClient,
    options: &NativeOptions,
) -> EffectResult<NativeMcpAssignmentReadback> {
    let observation = parsed.assignment_observation.as_ref().ok_or_else(|| {
        reject(
            "NATIVE_MCP_COMMAND_INPUT",
            "assigned-session observation metadata is missing",
        )
    })?;
    let response: LocationEnvelope<Vec<NativeMcpServerResponse>> =
        serde_json::from_value(mcp_value.clone()).map_err(|_| {
            reject(
                "NATIVE_MCP_SCHEMA",
                "OpenCode MCP response does not match the pinned 2.0.7 contract",
            )
        })?;
    let directory = options.directory.to_str().unwrap_or_default();
    if response.location.directory != directory
        || response.data.len() > MAX_SERVER_ENTRIES
        || native.process_version() != PINNED_OPENCODE_VERSION
        || native.process_id() != observation.service_pid
    {
        return Err(reject(
            "NATIVE_MCP_SCOPE_MISMATCH",
            "OpenCode MCP readback is outside the admitted service or location",
        ));
    }

    // The pinned session GET has the same `data` envelope consumed by
    // NativeClient::session; its body is not a flat session object.
    let session_data = session_value
        .get("data")
        .filter(|value| value.is_object())
        .cloned()
        .ok_or_else(|| {
            reject(
                "NATIVE_MCP_SCHEMA",
                "OpenCode session readback lacks its data object",
            )
        })?;
    let native_session = session_observation(session_data)?;
    if native_session.id != observation.native_session_id {
        return Err(reject(
            "NATIVE_MCP_SCOPE_MISMATCH",
            "OpenCode returned another native session",
        ));
    }

    let mut mcp_servers = response
        .data
        .into_iter()
        .map(|server| {
            if server.name.is_empty()
                || server.name.len() > MAX_ID_BYTES
                || server.name.trim().is_empty()
                || server.name.chars().any(char::is_control)
                || server.integration_id.as_deref().is_some_and(|value| {
                    value.is_empty()
                        || value.trim().is_empty()
                        || value.len() > MAX_ID_BYTES
                        || value.chars().any(char::is_control)
                })
            {
                return Err(reject(
                    "NATIVE_MCP_SCHEMA",
                    "OpenCode MCP server identity is outside its bounded schema",
                ));
            }
            let (status, error_present) = match server.status {
                NativeMcpStatusResponse::Connected => (NativeMcpServerStatus::Connected, false),
                NativeMcpStatusResponse::Pending => (NativeMcpServerStatus::Pending, false),
                NativeMcpStatusResponse::Disabled => (NativeMcpServerStatus::Disabled, false),
                NativeMcpStatusResponse::Failed { error } => {
                    drop(error);
                    (NativeMcpServerStatus::Failed, true)
                }
                NativeMcpStatusResponse::NeedsAuth { error } => {
                    drop(error);
                    (NativeMcpServerStatus::NeedsAuth, true)
                }
            };
            let integration_id_sha256 = server
                .integration_id
                .map(|integration_id| {
                    Sha256Digest::new(format!(
                        "{:x}",
                        sha2::Sha256::digest(integration_id.as_bytes())
                    ))
                })
                .transpose()
                .map_err(|_| {
                    reject(
                        "NATIVE_MCP_SCHEMA",
                        "OpenCode MCP integration identity could not be sanitized",
                    )
                })?;
            Ok(NativeMcpServerObservation {
                name: server.name,
                status,
                integration_id_sha256,
                error_present,
            })
        })
        .collect::<EffectResult<Vec<_>>>()?;
    mcp_servers.sort_by(|left, right| left.name.cmp(&right.name));

    let readback = NativeMcpAssignmentReadback {
        schema_version: 1,
        kind: "native_mcp_assignment_readback".into(),
        assignment_sha256: observation.assignment_sha256.clone(),
        service_id: observation.service_id.clone(),
        service_pid: observation.service_pid,
        service_version: observation.service_version.clone(),
        location_sha256: observation.location_sha256.clone(),
        native_session,
        mcp_servers,
        observed_at_ms: now_ms(),
    };
    readback.validate().map_err(|_| {
        reject(
            "NATIVE_MCP_SCHEMA",
            "native MCP assignment readback is invalid",
        )
    })?;
    Ok(readback)
}

fn session_observation(value: Value) -> EffectResult<NativeMcpSessionObservation> {
    let id = required_session_text(&value, "id")?;
    let project_id = required_session_text(&value, "projectID")?;
    let parent_id = optional_session_text(&value, "parentID")?;
    let agent = optional_session_text(&value, "agent")?;
    let created_at_ms = value["time"]["created"].as_u64().ok_or_else(|| {
        reject(
            "NATIVE_MCP_SCHEMA",
            "OpenCode session creation time is invalid",
        )
    })?;
    let updated_at_ms = value["time"]["updated"].as_u64().ok_or_else(|| {
        reject(
            "NATIVE_MCP_SCHEMA",
            "OpenCode session update time is invalid",
        )
    })?;
    Ok(NativeMcpSessionObservation {
        id,
        project_id,
        parent_id,
        agent,
        created_at_ms,
        updated_at_ms,
    })
}

fn required_session_text(value: &Value, field: &str) -> EffectResult<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|text| {
            !text.trim().is_empty()
                && text.len() <= MAX_ID_BYTES
                && !text.chars().any(char::is_control)
        })
        .map(str::to_owned)
        .ok_or_else(|| reject("NATIVE_MCP_SCHEMA", "OpenCode session fact is invalid"))
}

fn optional_session_text(value: &Value, field: &str) -> EffectResult<Option<String>> {
    match value.get(field) {
        None => Ok(None),
        Some(value) => value
            .as_str()
            .filter(|text| {
                !text.trim().is_empty()
                    && text.len() <= MAX_ID_BYTES
                    && !text.chars().any(char::is_control)
            })
            .map(|text| Some(text.to_owned()))
            .ok_or_else(|| reject("NATIVE_MCP_SCHEMA", "OpenCode session fact is invalid")),
    }
}

fn validate_arm_ack(output: &Value, challenge: Option<&Value>) -> EffectResult<()> {
    let challenge =
        challenge.ok_or_else(|| reject("NATIVE_MCP_COMMAND_SCOPE", "arm challenge is missing"))?;
    if output["accepted"] != true
        || output["challenge_id"] != challenge["challenge_id"]
        || output["nonce"] != challenge["nonce"]
        || output["module_sha256"] != challenge["module_sha256"]
        || output["service_id"] != challenge["service_id"]
        || output["service_pid"] != challenge["service_pid"]
        || output["service_version"] != challenge["service_version"]
    {
        return Err(unknown_failure(
            "NATIVE_MCP_PROOF_CHALLENGE",
            "native observer arm acknowledgement differs from the retained challenge",
        ));
    }
    Ok(())
}

fn bounded_json(value: &Value) -> EffectResult<()> {
    let bytes = serde_json::to_vec(value)
        .map_err(|_| reject("NATIVE_MCP_COMMAND_INPUT", "native MCP JSON is invalid"))?;
    if bytes.len() > MAX_ENVELOPE_BYTES {
        return Err(reject(
            "NATIVE_MCP_COMMAND_INPUT_LIMIT",
            "native MCP request JSON exceeds its bounded handoff size",
        ));
    }
    Ok(())
}

fn digest_value(value: &Value) -> Result<String> {
    let bytes = serde_json::to_vec(value)?;
    Ok(format!("sha256:{:x}", sha2::Sha256::digest(bytes)))
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .unwrap_or(0)
}

fn reject(code: &'static str, message: &str) -> EffectFailure {
    EffectFailure {
        error: Error::new(code, message),
        outcome: EffectOutcome::Rejected,
    }
}

fn readback_failure(
    code: &'static str,
    message: &str,
    effect_already_attempted: bool,
) -> EffectFailure {
    if effect_already_attempted {
        unknown_failure(code, message)
    } else {
        reject(code, message)
    }
}

fn unknown_failure(code: &'static str, message: &str) -> EffectFailure {
    EffectFailure {
        error: Error::new(code, message),
        outcome: EffectOutcome::Unknown,
    }
}

fn effect_failure(error: Error) -> EffectFailure {
    let outcome = if error.code == "NATIVE_REJECTED" {
        EffectOutcome::Rejected
    } else {
        EffectOutcome::Unknown
    };
    EffectFailure { error, outcome }
}

fn read_failure(error: Error, effect_already_attempted: bool) -> EffectFailure {
    EffectFailure {
        outcome: if effect_already_attempted {
            EffectOutcome::Unknown
        } else {
            EffectOutcome::Rejected
        },
        error,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ModelRef;
    use std::{collections::BTreeMap, path::PathBuf, time::Duration};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    fn raw_sha256(bytes: &[u8]) -> String {
        format!("{:x}", sha2::Sha256::digest(bytes))
    }

    fn canonical(value: &Value) -> Value {
        match value {
            Value::Object(object) => {
                let sorted = object
                    .iter()
                    .map(|(key, value)| (key.clone(), canonical(value)))
                    .collect::<BTreeMap<_, _>>();
                Value::Object(sorted.into_iter().collect())
            }
            Value::Array(values) => Value::Array(values.iter().map(canonical).collect()),
            value => value.clone(),
        }
    }

    fn digest_json_for_test(value: &Value) -> String {
        raw_sha256(&serde_json::to_vec(&canonical(value)).unwrap())
    }

    fn test_options(
        directory: PathBuf,
        connection_file: PathBuf,
        service_id: &str,
    ) -> NativeOptions {
        NativeOptions {
            service_id: service_id.to_owned(),
            connection_file,
            directory,
            model: ModelRef {
                id: "model".into(),
                provider_id: "provider".into(),
                variant: "default".into(),
            },
        }
    }

    fn assigned_value(directory: &str, session_id: &str) -> Value {
        json!({
            "task_id":"task_1",
            "task_revision":1,
            "attempt_id":"attempt_1",
            "binding_id":"binding_1",
            "binding_generation":4,
            "native_session_id":session_id,
            "participant_id":"participant_1",
            "mcp_profile":"participant",
            "grant_revision":1,
            "participation_basis":"attempt_owner",
            "assignment_id":null,
            "review_assignment_id":null
        })
    }

    fn scoped_path(path: &str, directory: &str) -> String {
        let mut url = Url::parse("http://127.0.0.1/").unwrap();
        url.set_path(path);
        url.query_pairs_mut()
            .append_pair("location[directory]", directory);
        format!("{}?{}", url.path(), url.query().unwrap())
    }

    fn assigned_effect(options: &NativeOptions, service_pid: u32) -> Value {
        let directory = options.directory.to_str().unwrap();
        let session_id = "ses_123";
        let assignment = assigned_value(directory, session_id);
        let assignment_sha256 = digest_json_for_test(&assignment);
        let location_sha256 = raw_sha256(directory.as_bytes());
        json!({
            "schema_version":1,
            "kind":"swarm.native_mcp_command",
            "action":"observe",
            "observation_kind":"assigned_session",
            "scope":{
                "binding_id":"binding_1",
                "binding_generation":4,
                "native_scope_key":format!("opencode-v2:{}", options.service_id),
                "service_id":options.service_id,
                "service_pid":service_pid,
                "expected_version":PINNED_OPENCODE_VERSION,
                "directory":directory,
                "assignment":assignment,
            },
            "prepared":{
                "schema_version":1,
                "kind":"swarm.native_mcp_assignment_observation",
                "assignment":assignment,
                "assignment_sha256":assignment_sha256,
                "native_session_id":session_id,
                "location_sha256":location_sha256,
                "service_id":options.service_id,
                "service_pid":service_pid,
                "service_version":PINNED_OPENCODE_VERSION,
            },
            "request":{
                "method":"GET",
                "path":scoped_path("/api/mcp", directory),
            },
            "readback":{
                "method":"GET",
                "path":scoped_path("/api/session/ses_123", directory),
            },
        })
    }

    fn installed_server_effect(options: &NativeOptions, service_pid: u32) -> Value {
        let directory = options.directory.to_str().unwrap();
        let assignment = assigned_value(directory, "ses_123");
        json!({
            "schema_version":1,
            "kind":"swarm.native_mcp_command",
            "action":"observe",
            "observation_kind":"installed_server",
            "scope":{
                "binding_id":"binding_1",
                "binding_generation":4,
                "native_scope_key":format!("opencode-v2:{}", options.service_id),
                "service_id":options.service_id,
                "service_pid":service_pid,
                "expected_version":PINNED_OPENCODE_VERSION,
                "directory":directory,
                "assignment":assignment,
            },
            "prepared":{
                "server_name":"eliot_test",
                "install_intent":{"schema_version":1,"kind":"test"},
            },
            "request":{
                "method":"GET",
                "path":scoped_path("/api/mcp", directory),
            },
            "precondition":null,
            "readback":null,
        })
    }

    fn runtime_command(input: Value) -> RuntimeCommand {
        RuntimeCommand {
            operation_id: "operation_1".into(),
            method: "native.mcp.observe".into(),
            created_at_ms: 1,
            binding_id: "binding_1".into(),
            generation: 4,
            native_root_id: None,
            route: Value::Null,
            input,
            input_sha256: Some("a".repeat(64)),
            target_input_sha256: None,
        }
    }

    fn parse_effect(input: Value, options: &NativeOptions) -> EffectResult<ParsedCommand> {
        parse(&runtime_command(input), options)
    }

    #[test]
    fn assigned_session_observe_requires_the_exact_two_get_routes() {
        let directory = std::env::current_dir().unwrap();
        let options = test_options(
            directory,
            PathBuf::from("unused-connection.json"),
            "service",
        );
        let input = assigned_effect(&options, 42);
        let parsed = match parse_effect(input.clone(), &options) {
            Ok(parsed) => parsed,
            Err(error) => panic!("unexpected parse rejection: {}", error.error.code),
        };
        assert_eq!(
            parsed.observation_kind,
            Some(NativeMcpObservationKind::AssignedSession)
        );
        assert_eq!(
            parsed
                .readback
                .as_ref()
                .map(|request| request.method.as_str()),
            Some("GET")
        );
        assert_eq!(
            parsed
                .assignment_observation
                .as_ref()
                .map(|observation| observation.native_session_id.as_str()),
            Some("ses_123")
        );

        let mut body = input.clone();
        body["request"]["body"] = json!({"unexpected":true});
        assert!(parse_effect(body, &options).is_err());

        let mut wrong_session = input.clone();
        wrong_session["readback"]["path"] = json!(scoped_path(
            "/api/session/ses_other",
            options.directory.to_str().unwrap()
        ));
        assert!(parse_effect(wrong_session, &options).is_err());

        let mut wrong_location = input.clone();
        wrong_location["request"]["path"] = json!(scoped_path("/api/mcp", "C:/another/location"));
        assert!(parse_effect(wrong_location, &options).is_err());

        let mut with_precondition = input.clone();
        with_precondition["precondition"] = json!({
            "method":"GET",
            "path":scoped_path("/api/mcp", options.directory.to_str().unwrap()),
        });
        assert!(parse_effect(with_precondition, &options).is_err());

        let mut with_challenge = input.clone();
        with_challenge["challenge"] = json!({"challenge_id":"challenge_1"});
        assert!(parse_effect(with_challenge, &options).is_err());

        let mut private_schema_v2 = input;
        private_schema_v2["schema_version"] = json!(2);
        assert!(parse_effect(private_schema_v2, &options).is_err());
    }

    #[test]
    fn installed_server_observe_keeps_its_prepared_install_projection() {
        let options = test_options(
            std::env::current_dir().unwrap(),
            PathBuf::from("unused-connection.json"),
            "service",
        );
        let parsed = match parse_effect(installed_server_effect(&options, 42), &options) {
            Ok(parsed) => parsed,
            Err(error) => panic!("unexpected parse rejection: {}", error.error.code),
        };
        assert_eq!(
            parsed.observation_kind,
            Some(NativeMcpObservationKind::InstalledServer)
        );
        assert_eq!(parsed.prepared["server_name"], "eliot_test");
        assert!(parsed.readback.is_none());
    }

    struct TempConnectionFile(PathBuf);

    impl Drop for TempConnectionFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    async fn read_request(stream: &mut TcpStream) -> (String, String) {
        let mut request = Vec::new();
        let mut buffer = [0_u8; 4096];
        loop {
            let count = stream.read(&mut buffer).await.unwrap();
            assert!(count > 0 && request.len() + count <= 16 * 1024);
            request.extend_from_slice(&buffer[..count]);
            if request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                break;
            }
        }
        let header_end = request
            .windows(4)
            .position(|bytes| bytes == b"\r\n\r\n")
            .unwrap();
        assert_eq!(request.len(), header_end + 4, "GET unexpectedly had a body");
        let header = String::from_utf8(request[..header_end].to_vec()).unwrap();
        let mut request_line = header.lines().next().unwrap().split_whitespace();
        (
            request_line.next().unwrap().to_owned(),
            request_line.next().unwrap().to_owned(),
        )
    }

    async fn respond(stream: &mut TcpStream, value: Value) {
        let body = serde_json::to_vec(&value).unwrap();
        let headers = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        stream.write_all(headers.as_bytes()).await.unwrap();
        stream.write_all(&body).await.unwrap();
    }

    #[tokio::test]
    async fn assigned_session_observe_uses_two_scoped_gets_and_sanitizes_receipt() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/", listener.local_addr().unwrap());
        let directory = std::env::current_dir().unwrap();
        let directory_text = directory.to_str().unwrap().to_owned();
        let service_pid = std::process::id();
        let options = test_options(
            directory,
            std::env::temp_dir().join(format!("native-mcp-{}.json", uuid::Uuid::new_v4())),
            "service",
        );
        let connection_file = TempConnectionFile(options.connection_file.clone());
        tokio::fs::write(
            &connection_file.0,
            serde_json::to_vec(&json!({
                "schema_version":1,
                "endpoint":endpoint,
                "pid":service_pid,
                "username":"fixture",
                "password":"fixture-secret",
            }))
            .unwrap(),
        )
        .await
        .unwrap();

        let expected_mcp_path = scoped_path("/api/mcp", &directory_text);
        let expected_session_path = scoped_path("/api/session/ses_123", &directory_text);
        let server = tokio::spawn(async move {
            for index in 0..7 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let (method, path) = read_request(&mut stream).await;
                assert_eq!(method, "GET");
                let response = match index {
                    2 => {
                        assert_eq!(path, expected_mcp_path);
                        json!({
                            "location":{"directory":directory_text},
                            "data":[
                                {"name":"zeta","status":{"status":"connected"},"integrationID":"integration-zeta"},
                                {"name":"alpha","status":{"status":"failed","error":"private server error"},"integrationID":"integration-alpha"}
                            ]
                        })
                    }
                    4 => {
                        assert_eq!(path, expected_session_path);
                        json!({"data":{
                            "id":"ses_123",
                            "projectID":"native-project-without-prefix",
                            "parentID":"ses_parent",
                            "agent":"build",
                            "time":{"created":10,"updated":11},
                            "title":"private session payload"
                        }})
                    }
                    _ => {
                        assert_eq!(path, "/api/info");
                        json!({
                            "version":PINNED_OPENCODE_VERSION,
                            "pid":service_pid,
                            "urls":["http://127.0.0.1/"],
                            "paths":{"tmp":"fixture"}
                        })
                    }
                };
                respond(&mut stream, response).await;
            }
        });

        let (native, _) = NativeClient::connect(&options).await.unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            execute(
                &native,
                &runtime_command(assigned_effect(&options, service_pid)),
                &options,
            ),
        )
        .await
        .unwrap();
        let receipt = match result {
            Ok(receipt) => receipt,
            Err(error) => panic!("unexpected execution failure: {}", error.error.code),
        };
        assert_eq!(receipt["action"], "observe");
        assert_eq!(receipt["observation_kind"], "assigned_session");
        assert_eq!(receipt["native_replay"], false);
        assert_eq!(
            receipt["readback"]["kind"],
            "native_mcp_assignment_readback"
        );
        assert_eq!(receipt["readback"]["service_version"], "2.0.7");
        assert_eq!(receipt["readback"]["native_session"]["id"], "ses_123");
        assert_eq!(receipt["readback"]["mcp_servers"][0]["name"], "alpha");
        assert_eq!(receipt["readback"]["mcp_servers"][0]["status"], "failed");
        assert_eq!(receipt["readback"]["mcp_servers"][0]["error_present"], true);
        assert_eq!(
            receipt["readback"]["mcp_servers"][0]["integration_id_sha256"],
            raw_sha256(b"integration-alpha")
        );
        let serialized = serde_json::to_string(&receipt).unwrap();
        assert!(!serialized.contains("private server error"));
        assert!(!serialized.contains("integration-alpha"));
        assert!(!serialized.contains("private session payload"));
        tokio::time::timeout(Duration::from_secs(3), server)
            .await
            .unwrap()
            .unwrap();
    }
}
