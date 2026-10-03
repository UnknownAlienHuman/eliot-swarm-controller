//! Store-side validation for host-owned OpenCode MCP readbacks.
//!
//! This path accepts only the non-deserializable adapter result type. No
//! participant request can provide server status, native session facts, or a
//! loaded-tool claim.

use crate::{
    config::McpToolProfile,
    error::{Error, Result},
    model::{self, Principal},
    native_mcp::{AssignmentContext, AssignmentSeed, NativeMcpReadback},
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde_json::{Value, json};

const OPENCODE_VERSION: &str = "2.0.7";
const MCP_API_CONTRACT: &str = "@opencode/protocol 2.0.7 mcp.list";
const SERVICE_IDENTITY_BASIS: &str = "explicit_route_id_and_verified_connection_pid_version";
type BindingRouteFacts = (String, String, Option<String>, Option<i64>, String);

/// Recheck the current assignment and exact configured binding before storing
/// a read-only native observation. The result always remains non-dispatchable:
/// OpenCode's public API does not prove the model's loaded tool set.
pub(crate) fn validate_and_record(
    tx: &Transaction<'_>,
    principal: &Principal,
    observation: &NativeMcpReadback,
    now: i64,
) -> Result<Value> {
    principal.require_participant()?;
    if now <= 0 || principal.client_id != observation.scope().participant_id() {
        return Err(Error::new(
            "NATIVE_MCP_SCOPE_MISMATCH",
            "native MCP observation is not for the authenticated participant",
        ));
    }
    observation.verify_digest()?;

    let current = current_assignment_context(tx, principal)?;
    if observation.scope() != &current {
        return Err(Error::new(
            "NATIVE_MCP_SCOPE_MISMATCH",
            "native MCP observation no longer matches the current participant assignment",
        ));
    }

    let payload = observation.payload();
    validate_readback_payload(tx, &current, &payload, now)?;

    let encoded = model::canonical(&payload)?;
    tx.execute(
        "INSERT INTO observations(source_stream_id,binding_id,binding_generation,kind,payload_json,recorded_at_ms) \
         VALUES('controller:native_mcp',?1,?2,'native.mcp.capability_readback',?3,?4)",
        params![current.binding_id(), current.binding_generation(), encoded, now],
    )?;
    let observation_id = tx.last_insert_rowid();
    Ok(json!({
        "observation_id":observation_id,
        "kind":"native.mcp.capability_readback",
        "assignment":current.as_value(),
        "native_readback":payload,
        "readiness":"incomplete",
        "dispatch_permitted":false,
        "completion_condition":"native_loaded_tool_set_and_model_context_receipt",
    }))
}

/// Resolve the host-only native assignment context from the live authenticated
/// Store scope. This is passed directly to the runtime adapter and never
/// serialized into a Participant request.
pub(crate) fn current_assignment_context(
    db: &Connection,
    principal: &Principal,
) -> Result<AssignmentContext> {
    principal.require_participant()?;
    let scope = super::coordination::current_scope(db, principal)?;
    let registration = super::meta(db, &format!("client:{}", principal.client_id))?
        .ok_or_else(|| Error::new("STALE_PARTICIPANT", "participant registration is missing"))?;
    assignment_context(db, &scope, &registration, &principal.client_id)
}

fn assignment_context(
    db: &Connection,
    scope: &Value,
    registration: &Value,
    participant_id: &str,
) -> Result<AssignmentContext> {
    if registration["role"] != "participant" || registration["disabled"] != false {
        return Err(Error::new(
            "STALE_PARTICIPANT",
            "native MCP readback requires an active participant registration",
        ));
    }
    let attempt = &scope["attempt"];
    let basis = &registration["participation_basis"];
    let basis_kind = basis["kind"]
        .as_str()
        .ok_or_else(|| Error::new("STALE_PARTICIPANT", "participant basis is missing"))?;
    let profile = match basis_kind {
        "attempt_owner" | "producer_ref" => McpToolProfile::Participant,
        "sponsored_reviewer" => McpToolProfile::AssignedReviewer,
        _ => {
            return Err(Error::new(
                "STALE_PARTICIPANT",
                "participant basis cannot select a native MCP profile",
            ));
        }
    };
    let native_session_id = match registration.get("native_session_id") {
        None | Some(Value::Null) if basis_kind == "attempt_owner" => {
            attempt_owner_root_session(db, scope, registration, participant_id)?
        }
        Some(Value::String(value)) if !value.is_empty() => value.clone(),
        _ => {
            return Err(Error::new(
                "NATIVE_MCP_SESSION_UNBOUND",
                "participant registration has no exact native session binding",
            ));
        }
    };
    AssignmentContext::new(AssignmentSeed {
        task_id: required_text(&scope["task"], "task_id")?.to_owned(),
        task_revision: positive_i64(&scope["task"], "revision")?,
        attempt_id: required_text(attempt, "attempt_id")?.to_owned(),
        binding_id: required_text(attempt, "binding_id")?.to_owned(),
        binding_generation: positive_i64(attempt, "binding_generation")?,
        native_session_id,
        participant_id: participant_id.to_owned(),
        profile,
        grant_revision: positive_i64(registration, "grant_revision")?,
        basis_kind: basis_kind.to_owned(),
        assignment_id: basis
            .get("assignment_id")
            .and_then(Value::as_str)
            .map(str::to_owned),
        review_assignment_id: basis
            .get("review_scope")
            .and_then(|value| value.get("review_assignment_id"))
            .and_then(Value::as_str)
            .map(str::to_owned),
    })
}

/// The initial attempt-owner registration can predate any producer record.
/// In that one case, derive the native session only from the exact ready
/// binding attached to the current Attempt and its active held launch lease.
/// No caller-provided session ID participates in this path.
fn attempt_owner_root_session(
    db: &Connection,
    scope: &Value,
    registration: &Value,
    participant_id: &str,
) -> Result<String> {
    let task = &scope["task"];
    let attempt = &scope["attempt"];
    let task_id = required_text(task, "task_id")?;
    let task_revision = positive_i64(task, "revision")?;
    let attempt_id = required_text(attempt, "attempt_id")?;
    let binding_id = required_text(attempt, "binding_id")?;
    let binding_generation = positive_i64(attempt, "binding_generation")?;
    let project_id = required_text(task, "project_id")?;
    let owner_id = required_text(attempt, "owner_id")?;
    let grant_revision = positive_i64(registration, "grant_revision")?;
    let registration_operation_id = required_text(registration, "created_operation_id")?;
    let expected_basis = json!({
        "kind":"attempt_owner",
        "assignment_id":Value::Null,
        "review_scope":Value::Null,
    });
    if registration["role"] != "participant"
        || registration["disabled"] != false
        || registration["task_id"].as_str() != Some(task_id)
        || registration["task_revision"] != task_revision
        || registration["attempt_id"].as_str() != Some(attempt_id)
        || registration["binding_id"].as_str() != Some(binding_id)
        || registration["binding_generation"] != binding_generation
        || registration["created_by"].as_str() != Some(owner_id)
        || registration["participation_basis"] != expected_basis
        || !registration["native_session_id"].is_null()
    {
        return Err(Error::new(
            "STALE_PARTICIPANT",
            "initial native MCP readback requires the current exact attempt-owner registration",
        ));
    }

    let binding: Option<BindingRouteFacts> = db
        .query_row(
            "SELECT state,module_artifact_id,native_root_id,released_at_ms,route_json \
             FROM bindings WHERE binding_id=?1 AND generation=?2",
            params![binding_id, binding_generation],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .optional()?;
    let Some((state, artifact_id, native_root_id, released_at_ms, route_json)) = binding else {
        return Err(Error::new(
            "NATIVE_MCP_BINDING_MISSING",
            "current attempt-owner binding is missing",
        ));
    };
    if state != "ready"
        || released_at_ms.is_some()
        || artifact_id != crate::runtime::opencode_v2::ARTIFACT_ID
    {
        return Err(Error::new(
            "NATIVE_MCP_BINDING_STALE",
            "initial attempt-owner readback requires the exact ready OpenCode binding",
        ));
    }
    let route: Value = serde_json::from_str(&route_json)?;
    if route["runtime"] != crate::runtime::opencode_v2::RUNTIME
        || route["module_artifact_id"] != crate::runtime::opencode_v2::ARTIFACT_ID
    {
        return Err(Error::new(
            "NATIVE_MCP_BINDING_STALE",
            "attempt-owner binding route is not the selected OpenCode V2 adapter",
        ));
    }
    let native_root_id = native_root_id
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            Error::new(
                "NATIVE_MCP_SESSION_UNBOUND",
                "ready OpenCode binding has no exact native root session",
            )
        })?;
    crate::runtime::opencode_v2::valid_id(&native_root_id, "ses")?;

    let (held_launch_count, effective_request): (i64, Option<String>) = db.query_row(
        "SELECT count(*),min(o.effective_request_json) FROM workspace_leases AS l \
         JOIN workspace_registrations AS r \
           ON r.registration_id=l.registration_id \
          AND r.generation=l.registration_generation \
         JOIN operations AS o ON o.operation_id=l.operation_id \
         WHERE l.task_id=?1 AND l.task_revision=?2 AND l.attempt_id=?3 \
           AND l.project_id=?4 AND l.owner_client_id=?5 AND l.state='held' \
           AND r.state='active' AND r.project_id=l.project_id \
           AND o.method='swarm.launch' AND o.state='queued' \
           AND o.task_id=?1 AND o.attempt_id=?3 AND o.binding_id=?6 AND o.binding_generation=?7 \
           AND json_extract(o.effective_request_json,'$.launch_manifest.state') \
               IN ('awaiting_capability','awaiting_native_mcp') \
           AND json_extract(o.effective_request_json,'$.launch_manifest.task.task_id')=?1 \
           AND json_extract(o.effective_request_json,'$.launch_manifest.task.observed_revision')=?2 \
           AND json_extract(o.effective_request_json,'$.launch_manifest.task.attempt_id')=?3 \
           AND json_extract(o.effective_request_json,'$.launch_manifest.binding.binding_id')=?6 \
           AND json_extract(o.effective_request_json,'$.launch_manifest.binding.generation')=?7 \
           AND json_extract(o.effective_request_json,'$.launch_manifest.participant.client_id')=?8 \
           AND json_extract(o.effective_request_json,'$.launch_manifest.mcp.participant_client_id')=?8 \
           AND json_extract(o.effective_request_json,'$.launch_manifest.participant.grant_revision')=?9 \
           AND json_extract(o.effective_request_json,'$.launch_manifest.participant.registration_operation_id')=?10 \
           AND json_extract(o.effective_request_json,'$.launch_manifest.participant.credential_ref') \
               = json_extract(o.effective_request_json,'$.launch_manifest.mcp.credential_ref') \
           AND json_extract(o.effective_request_json,'$.launch_manifest.participant.profile_config_ref') \
               = json_extract(o.effective_request_json,'$.launch_manifest.mcp.profile_config_ref') \
           AND json_extract(o.effective_request_json,'$.launch_manifest.request.mcp_profile') \
               = json_extract(o.effective_request_json,'$.launch_manifest.mcp.profile_name') \
           AND json_extract(o.effective_request_json,'$.launch_manifest.request.mcp_surface') \
               = json_extract(o.effective_request_json,'$.launch_manifest.mcp.surface') \
           AND json_extract(o.effective_request_json,'$.launch_manifest.mcp.hard_profile')='participant' \
           AND json_extract(o.effective_request_json,'$.launch_manifest.mcp.status')='validated_against_static_catalog' \
           AND json_extract(o.effective_request_json,'$.launch_manifest.mcp.identity.status')='registered_enabled_participant' \
           AND json_extract(o.effective_request_json,'$.launch_manifest.workspace.lease.state')='held' \
           AND json_extract(o.effective_request_json,'$.launch_manifest.workspace.lease.lease_id')=l.lease_id \
           AND json_extract(o.effective_request_json,'$.launch_manifest.workspace.lease.registration_id')=l.registration_id \
           AND json_extract(o.effective_request_json,'$.launch_manifest.workspace.lease.registration_generation')=l.registration_generation \
           AND json_extract(o.effective_request_json,'$.launch_manifest.workspace.lease.project_id')=l.project_id \
           AND json_extract(o.effective_request_json,'$.launch_manifest.workspace.lease.task_id')=l.task_id \
           AND json_extract(o.effective_request_json,'$.launch_manifest.workspace.lease.task_revision')=l.task_revision \
           AND json_extract(o.effective_request_json,'$.launch_manifest.workspace.lease.operation_id')=l.operation_id \
           AND json_extract(o.effective_request_json,'$.launch_manifest.workspace.lease.plan_digest')=l.plan_digest \
           AND json_extract(o.effective_request_json,'$.launch_manifest.workspace.lease.owner_client_id')=l.owner_client_id \
           AND json_extract(o.effective_request_json,'$.launch_manifest.workspace.lease.attempt_id')=l.attempt_id \
           AND json_extract(o.effective_request_json,'$.launch_manifest.workspace.lease.generation')=l.generation \
           AND json_extract(o.effective_request_json,'$.launch_manifest.workspace.lease.binding_digest')=l.binding_digest",
        params![
            task_id,
            task_revision,
            attempt_id,
            project_id,
            owner_id,
            binding_id,
            binding_generation,
            participant_id,
            grant_revision,
            registration_operation_id,
        ],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    if held_launch_count != 1 {
        return Err(Error::new(
            "NATIVE_MCP_WORKSPACE_LEASE_UNAVAILABLE",
            "initial attempt-owner readback requires one exact issued launch and active held lease",
        ));
    }

    let effective_request = effective_request.ok_or_else(|| {
        Error::new(
            "NATIVE_MCP_WORKSPACE_LEASE_UNAVAILABLE",
            "initial attempt-owner launch manifest is unavailable",
        )
    })?;
    let effective: Value = serde_json::from_str(&effective_request)?;
    let manifest = &effective["launch_manifest"];
    let participant = &manifest["participant"];
    let mcp = &manifest["mcp"];
    let credential_ref = participant["credential_ref"].as_str();
    let profile_config_ref = participant["profile_config_ref"].as_str();
    if !matches!(
        manifest["state"].as_str(),
        Some("awaiting_capability" | "awaiting_native_mcp")
    ) || participant["client_id"].as_str() != Some(participant_id)
        || mcp["participant_client_id"].as_str() != Some(participant_id)
        || participant["grant_revision"] != grant_revision
        || participant["registration_operation_id"].as_str() != Some(registration_operation_id)
        || participant["role"] != "participant"
        || participant["task_id"].as_str() != Some(task_id)
        || participant["task_revision"] != task_revision
        || participant["attempt_id"].as_str() != Some(attempt_id)
        || participant["binding_id"].as_str() != Some(binding_id)
        || participant["binding_generation"] != binding_generation
        || participant["participation_basis"] != registration["participation_basis"]
        || !participant["native_session_id"].is_null()
        || manifest["task"]["project_id"].as_str() != Some(project_id)
        || manifest["binding"]["native_root_id"].as_str() != Some(native_root_id.as_str())
        || manifest["workspace"]["lease"]["state"] != "held"
        || manifest["workspace"]["lease"]["project_id"].as_str() != Some(project_id)
        || manifest["workspace"]["lease"]["task_id"].as_str() != Some(task_id)
        || manifest["workspace"]["lease"]["task_revision"] != task_revision
        || manifest["workspace"]["lease"]["owner_client_id"].as_str() != Some(owner_id)
        || manifest["workspace"]["lease"]["attempt_id"].as_str() != Some(attempt_id)
        || manifest["workspace"]["lease"]["operation_id"]
            .as_str()
            .is_none()
        || credential_ref.is_none_or(|value| !valid_opaque_ref(value, "participant-credential"))
        || profile_config_ref.is_none_or(|value| !valid_opaque_ref(value, "participant-mcp-config"))
        || mcp["credential_ref"].as_str() != credential_ref
        || mcp["profile_config_ref"].as_str() != profile_config_ref
        || mcp["hard_profile"] != "participant"
        || mcp["profile_name"] != manifest["request"]["mcp_profile"]
        || mcp["surface"] != manifest["request"]["mcp_surface"]
        || mcp["status"] != "validated_against_static_catalog"
        || mcp["identity"]["status"] != "registered_enabled_participant"
        || mcp["capability_state"] != "unknown"
        || manifest["runtime"]["dispatch_permitted"] != false
        || manifest["progress"]["task_dispatch"] != "not_started"
    {
        return Err(Error::new(
            "STALE_PARTICIPANT",
            "initial native MCP readback launch profile or retained Participant grant changed",
        ));
    }
    Ok(native_root_id)
}

fn valid_opaque_ref(value: &str, prefix: &str) -> bool {
    let Some((actual_prefix, id)) = value.split_once(':') else {
        return false;
    };
    actual_prefix == prefix
        && uuid::Uuid::parse_str(id).is_ok_and(|parsed| parsed.to_string() == id)
}

fn validate_readback_payload(
    tx: &Transaction<'_>,
    assignment: &AssignmentContext,
    payload: &Value,
    now: i64,
) -> Result<()> {
    let binding: Option<(String, String, String, Option<i64>)> = tx
        .query_row(
            "SELECT state,module_artifact_id,route_json,released_at_ms FROM bindings \
             WHERE binding_id=?1 AND generation=?2",
            params![assignment.binding_id(), assignment.binding_generation()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let Some((state, module_artifact_id, route_json, released_at_ms)) = binding else {
        return Err(Error::new(
            "NATIVE_MCP_BINDING_MISSING",
            "current participant binding no longer exists",
        ));
    };
    if released_at_ms.is_some()
        || !matches!(state.as_str(), "ready" | "reconciling")
        || module_artifact_id != crate::runtime::opencode_v2::ARTIFACT_ID
    {
        return Err(Error::new(
            "NATIVE_MCP_BINDING_STALE",
            "native MCP readback requires a live OpenCode V2 binding",
        ));
    }
    let route: Value = serde_json::from_str(&route_json)?;
    let options = &route["native_options"];
    let directory = options["directory"].as_str().ok_or_else(|| {
        Error::new(
            "NATIVE_MCP_ROUTE_MISMATCH",
            "OpenCode binding has no explicit native directory",
        )
    })?;
    let expected_service_id = options["service_id"].as_str().ok_or_else(|| {
        Error::new(
            "NATIVE_MCP_ROUTE_MISMATCH",
            "OpenCode binding has no explicit native service ID",
        )
    })?;
    let expected_version = options["expected_version"].as_str().ok_or_else(|| {
        Error::new(
            "NATIVE_MCP_ROUTE_MISMATCH",
            "OpenCode binding has no explicit native version",
        )
    })?;
    let source = &payload["source"];
    let directory_digest = format!("sha256:{}", model::digest(directory.as_bytes()));
    if route["runtime"] != crate::runtime::opencode_v2::RUNTIME
        || route["module_artifact_id"] != crate::runtime::opencode_v2::ARTIFACT_ID
        || expected_version != OPENCODE_VERSION
        || source["runtime"] != crate::runtime::opencode_v2::RUNTIME
        || source["api_contract"] != MCP_API_CONTRACT
        || source["api_method"] != "GET /api/mcp"
        || source["api_scope"] != "configured_server_connection_status_only"
        || source["service_id"] != expected_service_id
        || source["service_identity_basis"] != SERVICE_IDENTITY_BASIS
        || source["service_version"] != expected_version
        || source["service_pid"].as_u64().is_none_or(|pid| pid == 0)
        || source["directory_sha256"] != directory_digest
        || payload["assignment"] != assignment.as_value()
        || payload["native_session"]["id"] != assignment.native_session_id()
        || payload["loaded_tool_set"]["status"] != "unknown"
        || !payload["loaded_tool_set"]["items"].is_null()
        || !payload["loaded_tool_set"]["digest"].is_null()
        || payload["model_context_loaded"] != "unknown"
        || payload["binding_session_mapping"]["status"] != "unknown"
        || payload["readiness"] != "incomplete"
        || payload["dispatch_permitted"] != false
        || payload["observed_at_ms"]
            .as_i64()
            .is_none_or(|observed_at| observed_at <= 0 || observed_at > now)
    {
        return Err(Error::new(
            "NATIVE_MCP_ROUTE_MISMATCH",
            "native MCP readback does not match the current route or required unknown-state boundary",
        ));
    }
    if payload["mcp_servers"]
        .as_array()
        .is_none_or(|servers| servers.len() > 256)
    {
        return Err(Error::new(
            "NATIVE_MCP_SCHEMA",
            "native MCP server status readback exceeds its schema bound",
        ));
    }
    Ok(())
}

fn required_text<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            Error::new(
                "STALE_PARTICIPANT",
                "current participant scope is incomplete",
            )
        })
}

fn positive_i64(value: &Value, field: &str) -> Result<i64> {
    value
        .get(field)
        .and_then(Value::as_i64)
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            Error::new(
                "STALE_PARTICIPANT",
                "current participant scope is incomplete",
            )
        })
}
