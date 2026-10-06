//! Consumer for the Store-admitted `swarm.native_mcp_command@1` envelope.
//!
//! This module owns only the OpenCode HTTP effects.  The Store constructs the
//! envelope after C8 reservation and remains the authority for assignment,
//! challenge, descriptor, and sanitized readback validation.  The envelope is
//! intentionally a Value at this boundary so the neutral schema stays in
//! `swarm-contracts`; this module never invents an alternate public DTO.

use crate::{config::NativeOptions, native::NativeClient};
use reqwest::Url;
use serde_json::{Value, json};
use sha2::Digest;
use std::time::{SystemTime, UNIX_EPOCH};
use swarm_contracts::{
    error::{Error, Result},
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
    service_pid: u32,
    request: RequestSpec,
    precondition: Option<RequestSpec>,
    readback: Option<RequestSpec>,
    prepared: Value,
    challenge: Option<Value>,
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
    let response = native
        .mcp_get(&parsed.request.path)
        .await
        .map_err(|error| read_failure(error, false))?;
    let readback = project_install_readback(&response, parsed, native, options, false)?;
    Ok(json!({
        "schema_version": SCHEMA_VERSION,
        "kind": "native_mcp_effect_receipt",
        "action": "observe",
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
            "native MCP command schema is not swarm.native_mcp_command@1",
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
        .map(|value| parse_request(Some(value), options, "observe"))
        .transpose()?;
    let readback = input
        .get("readback")
        .map(|value| parse_request(Some(value), options, "observe"))
        .transpose()?;
    let challenge = input.get("challenge").cloned();
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
        validate_observe_request(&request, &prepared, options)?;
    } else {
        validate_rpc_request(&request, &action, &challenge, options)?;
    }
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
        service_pid: scope["service_pid"]
            .as_u64()
            .and_then(|pid| u32::try_from(pid).ok())
            .ok_or_else(|| reject("NATIVE_MCP_COMMAND_SCOPE", "native service PID is invalid"))?,
        request,
        precondition,
        readback,
        prepared,
        challenge,
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
        "expected_version",
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
        || scope["expected_version"] != options.expected_version
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
