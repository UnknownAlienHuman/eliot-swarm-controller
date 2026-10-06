//! Readback of the MCP facts exposed by the pinned OpenCode V2 HTTP API.
//!
//! OpenCode's public `mcp.list` route reports configured server connection
//! status. It does not expose the tool inventory sent to a model, so this
//! adapter deliberately returns an incomplete capability observation.

use super::{Options, Service};
use crate::{
    error::{Error, Result},
    model,
    native_mcp::{
        AssignmentContext, McpServerFact, McpServerStatus, NativeMcpReadback, NativeSessionFact,
        OpenCodeV2ReadbackInput,
    },
};
use serde::Deserialize;
use serde_json::Value;

const MAX_MCP_SERVERS: usize = 256;
const PINNED_OPENCODE_VERSION: &str = "2.0.7";

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
struct NativeMcpServer {
    name: String,
    status: NativeMcpStatus,
    #[serde(rename = "integrationID", default)]
    integration_id: Option<String>,
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

/// Read exact native session identity and the only public HTTP MCP inventory
/// available in OpenCode 2.0.7. The caller must pass the Store-authorized
/// assignment corresponding to these route options.
pub(crate) async fn observe(
    service: &Service,
    options: &Options,
    assignment: AssignmentContext,
) -> Result<NativeMcpReadback> {
    validate_route_pairing(service, options)?;
    let directory = options.directory.to_str().ok_or_else(|| {
        Error::new(
            "NATIVE_MCP_SCOPE_MISMATCH",
            "configured native directory is not valid Unicode",
        )
    })?;

    service.verify().await?;
    let raw = service
        .get("/api/mcp", &[("location[directory]", directory.to_owned())])
        .await?;
    let response: LocationEnvelope<Vec<NativeMcpServer>> =
        serde_json::from_value(raw).map_err(|_| {
            Error::new(
                "NATIVE_MCP_SCHEMA",
                "OpenCode MCP response does not match the pinned 2.0.7 contract",
            )
        })?;
    if response.location.directory != directory || response.data.len() > MAX_MCP_SERVERS {
        return Err(Error::new(
            "NATIVE_MCP_SCOPE_MISMATCH",
            "OpenCode MCP readback is outside the requested directory or response bound",
        ));
    }

    let session_value = service.session(assignment.native_session_id()).await?;
    let session = session_fact(session_value)?;
    // The service may have been restarted or replaced while the two GETs ran.
    service.verify().await?;

    let servers = response
        .data
        .into_iter()
        .map(|server| {
            let (status, error_present) = match server.status {
                NativeMcpStatus::Connected => (McpServerStatus::Connected, false),
                NativeMcpStatus::Pending => (McpServerStatus::Pending, false),
                NativeMcpStatus::Disabled => (McpServerStatus::Disabled, false),
                NativeMcpStatus::Failed { error } => {
                    drop(error);
                    (McpServerStatus::Failed, true)
                }
                NativeMcpStatus::NeedsAuth { error } => {
                    drop(error);
                    (McpServerStatus::NeedsAuth, true)
                }
            };
            McpServerFact::new(server.name, status, server.integration_id, error_present)
        })
        .collect::<Result<Vec<_>>>()?;

    NativeMcpReadback::from_opencode_v2(
        assignment,
        OpenCodeV2ReadbackInput {
            service_id: options.service_id.clone(),
            service_pid: service.pid,
            service_version: service.version.clone(),
            directory_sha256: format!("sha256:{}", model::digest(directory.as_bytes())),
            session,
            servers,
            observed_at_ms: model::now_ms()?,
        },
    )
}

fn validate_route_pairing(service: &Service, options: &Options) -> Result<()> {
    let service_id_valid = !options.service_id.is_empty()
        && options.service_id.len() <= 128
        && options
            .service_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte));
    if !service_id_valid
        || !options.directory.is_absolute()
        || options.expected_version != PINNED_OPENCODE_VERSION
        || options.expected_version != service.version
        || service.pid == 0
    {
        return Err(Error::new(
            "NATIVE_MCP_SCOPE_MISMATCH",
            "OpenCode service identity does not match the explicit route options",
        ));
    }
    Ok(())
}

fn session_fact(session: Value) -> Result<NativeSessionFact> {
    let session_id = required_string(&session, "id")?;
    let project_id = required_string(&session, "projectID")?;
    let parent_session_id = optional_string(&session, "parentID")?;
    let agent = optional_string(&session, "agent")?;
    let created_at_ms = session["time"]["created"].as_u64().ok_or_else(|| {
        Error::new(
            "NATIVE_MCP_SCHEMA",
            "OpenCode session creation time is invalid",
        )
    })?;
    let updated_at_ms = session["time"]["updated"].as_u64().ok_or_else(|| {
        Error::new(
            "NATIVE_MCP_SCHEMA",
            "OpenCode session update time is invalid",
        )
    })?;
    NativeSessionFact::new(
        session_id,
        project_id,
        parent_session_id,
        agent,
        created_at_ms,
        updated_at_ms,
    )
}

fn required_string(value: &Value, field: &str) -> Result<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| Error::new("NATIVE_MCP_SCHEMA", "OpenCode session fact is invalid"))
}

fn optional_string(value: &Value, field: &str) -> Result<Option<String>> {
    match value.get(field) {
        None => Ok(None),
        Some(value) => value
            .as_str()
            .map(|value| Some(value.to_owned()))
            .ok_or_else(|| Error::new("NATIVE_MCP_SCHEMA", "OpenCode session fact is invalid")),
    }
}
