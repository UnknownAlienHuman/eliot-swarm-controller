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
                "OpenCode MCP response does not match the supported typed contract",
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
    // An explicit pin must match the observed service. Without one, response
    // schemas and the live PID/version continuity checks define this readback.
    if !service_id_valid
        || !options.directory.is_absolute()
        || !Options::version_pin_valid(&service.version)
        || options
            .expected_version
            .as_deref()
            .is_some_and(|pin| pin != service.version)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::McpToolProfile, native_mcp::AssignmentSeed};
    use serde_json::json;

    fn assignment() -> AssignmentContext {
        AssignmentContext::new(AssignmentSeed {
            task_id: "fixture-task".into(),
            task_revision: 1,
            attempt_id: "fixture-attempt".into(),
            binding_id: "fixture-binding".into(),
            binding_generation: 1,
            native_session_id: "ses_fixture".into(),
            participant_id: "fixture-participant".into(),
            profile: McpToolProfile::Participant,
            grant_revision: 1,
            basis_kind: "attempt_owner".into(),
            assignment_id: None,
            review_assignment_id: None,
        })
        .unwrap()
    }

    #[tokio::test]
    async fn native_mcp_defaults_accept_typed_readback_from_observed_build() {
        use super::super::tests::{Fixture, Reply};
        let f = Fixture::new().await;
        let mut options = f.options.clone();
        options.expected_version = None;
        f.override_get(
            "/api/mcp",
            Reply::Json(
                200,
                json!({
                    "location":{"directory":options.directory}, "data":[]
                }),
            ),
        );
        f.world.lock().unwrap().sessions.insert(
            "ses_fixture".into(),
            json!({
                "id":"ses_fixture", "projectID":"prj_fixture",
                "time":{"created":1,"updated":2}
            }),
        );
        let service = Service::connect(&options).await.unwrap();
        let observed = observe(&service, &options, assignment()).await.unwrap();
        observed.verify_digest().unwrap();
        assert_eq!(
            observed.payload()["source"]["service_version"],
            "2.0.fixture"
        );
        assert_eq!(
            observed.payload()["source"]["api_contract"],
            "opencode-http-mcp-list-v1"
        );
        assert!(
            f.world
                .lock()
                .unwrap()
                .requests
                .iter()
                .all(|r| r.method == "GET")
        );

        // Omitting the release pin does not admit malformed native responses.
        f.override_get(
            "/api/mcp",
            Reply::Json(
                200,
                json!({
                    "location":{"directory":options.directory}, "data":[{"name":"bad"}]
                }),
            ),
        );
        assert_eq!(
            observe(&service, &options, assignment())
                .await
                .err()
                .unwrap()
                .code,
            "NATIVE_MCP_SCHEMA"
        );
    }

    #[tokio::test]
    async fn native_mcp_defaults_keep_explicit_pin_and_live_continuity_guards() {
        use super::super::tests::{Fixture, Reply};
        let f = Fixture::new().await;
        let mut options = f.options.clone();
        options.expected_version = None;
        let service = Service::connect(&options).await.unwrap();
        let mut pinned = options.clone();
        pinned.expected_version = Some("another-build".into());
        assert_eq!(
            observe(&service, &pinned, assignment())
                .await
                .err()
                .unwrap()
                .code,
            "NATIVE_MCP_SCOPE_MISMATCH"
        );
        f.override_get(
            "/api/info",
            Reply::Json(
                200,
                json!({
                    "version":"replacement-build", "pid":424242,
                    "urls":["http://127.0.0.1:1/"], "paths":{"tmp":"fixture"}
                }),
            ),
        );
        assert_eq!(
            observe(&service, &options, assignment())
                .await
                .err()
                .unwrap()
                .code,
            "NATIVE_INSTANCE_CHANGED"
        );
    }
}
