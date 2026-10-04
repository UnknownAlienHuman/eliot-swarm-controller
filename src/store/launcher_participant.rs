//! Store-side validation for launch-scoped Participant credential issuance.
//!
//! Host code creates the private credential/profile files and calls the
//! ordinary Participant registration operation. This module only prepares
//! the exact request and commits opaque references after rechecking Store
//! authority; it never reads or writes credential files.

use super::launcher::LaunchActor;
use crate::{
    config::{Config, McpToolProfile},
    error::{Error, Result},
    launcher::LaunchRequest,
    model,
    participant_credentials::{InboundPolicy, IssueRequest, IssuedParticipant, ParticipationBasis},
    workspace::LeaseAuthorityRef,
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde_json::{Value, json};

const AWAITING_ISSUANCE: &str = "awaiting_participant_credential";
const AWAITING_NATIVE_MCP: &str = "awaiting_native_mcp";

struct LaunchIssuanceScope {
    manifest: Value,
    operation: Value,
    task_id: String,
    task_revision: i64,
    attempt_id: String,
    binding_id: String,
    binding_generation: i64,
    mcp_profile: String,
    mcp_surface: String,
    native_session_id: Option<String>,
}

fn retained_manifest(db: &Connection, operation_id: &str) -> Result<Value> {
    let raw: String = db.query_row(
        "SELECT effective_request_json FROM operations WHERE operation_id=?1 AND method='swarm.launch'",
        [operation_id],
        |row| row.get(0),
    )?;
    let effective: Value = serde_json::from_str(&raw)?;
    effective
        .get("launch_manifest")
        .cloned()
        .ok_or_else(|| Error::new("INVALID_LAUNCH_MANIFEST", "launch manifest is missing"))
}

fn actor_caller_id(actor: &LaunchActor) -> &str {
    actor.technical_requester_id()
}

fn actor_owner_id(actor: &LaunchActor) -> &str {
    actor.effective_manager_id()
}

fn require_same_live_actor(db: &Connection, actor: &LaunchActor, operation_id: &str) -> Result<()> {
    let current = super::launcher::launch_actor(db, operation_id)?;
    if !current.same_authority_identity(actor) {
        return Err(Error::new(
            "FORBIDDEN",
            "launch issuance actor differs from the currently authorized launch actor",
        ));
    }
    Ok(())
}

fn validated_scope(
    db: &Connection,
    actor: &LaunchActor,
    operation_id: &str,
    config: &Config,
    accepted_phases: &[&str],
) -> Result<LaunchIssuanceScope> {
    require_same_live_actor(db, actor, operation_id)?;
    let operation = super::operations::get_operation(db, operation_id)?;
    if operation["method"] != "swarm.launch"
        || operation["caller_id"].as_str() != Some(actor_caller_id(actor))
    {
        return Err(Error::new(
            "FORBIDDEN",
            "participant issuance requires the actor-owned launch Operation",
        ));
    }
    if operation["state"] == "outcome_unknown" {
        return Err(Error::new(
            "LAUNCH_ISSUANCE_UNKNOWN",
            "launch has an unresolved external effect and cannot issue a participant",
        ));
    }
    if operation["state"] != "queued" {
        return Err(Error::new(
            "STALE_LAUNCH",
            "participant issuance requires a queued launch Operation",
        ));
    }

    let manifest = retained_manifest(db, operation_id)?;
    let phase = model::text(&manifest, "state")?;
    if !accepted_phases.contains(&phase) {
        return Err(Error::new(
            "STALE_LAUNCH",
            "launch is not in a participant issuance phase",
        ));
    }
    let original_raw: String = db.query_row(
        "SELECT original_request_json FROM operations WHERE operation_id=?1 AND method='swarm.launch'",
        [operation_id],
        |row| row.get(0),
    )?;
    let launch_request = LaunchRequest::parse(&serde_json::from_str::<Value>(&original_raw)?)?;
    if manifest["plan_digest"] != launch_request.plan_digest
        || manifest["client_request_id"] != launch_request.client_request_id
        || model::canonical(&manifest["request"])?
            != model::canonical(&launch_request.preview_params())?
    {
        return Err(Error::new(
            "STALE_LAUNCH",
            "retained launch request and manifest identities do not match",
        ));
    }

    let task_id = model::text(&manifest["task"], "task_id")?.to_owned();
    let task_revision = model::positive(&manifest["task"], "observed_revision")?;
    let attempt_id = model::text(&manifest["task"], "attempt_id")?.to_owned();
    if operation["task_id"].as_str() != Some(task_id.as_str())
        || operation["attempt_id"].as_str() != Some(attempt_id.as_str())
    {
        return Err(Error::new(
            "STALE_LAUNCH",
            "launch Operation is not linked to the retained Task and Attempt",
        ));
    }

    let task = super::tasks::get_task(db, &task_id)?;
    let attempt = super::tasks::get_attempt(db, &attempt_id)?;
    if task["state"] != "open"
        || task["revision"] != task_revision
        || task["current_attempt_id"].as_str() != Some(attempt_id.as_str())
        || attempt["task_id"].as_str() != Some(task_id.as_str())
        || attempt["task_revision"] != task_revision
        || attempt["released_at_ms"].is_number()
        || attempt["state"] != "reserved"
        || !attempt["start_operation_id"].is_null()
    {
        return Err(Error::new(
            "STALE_LAUNCH",
            "participant issuance requires the exact current unstarted Attempt",
        ));
    }
    let binding_id = model::text(&manifest["binding"], "binding_id")?.to_owned();
    let binding_generation = model::positive(&manifest["binding"], "generation")?;
    if manifest["binding"]["state"] != "ready"
        || operation["binding_id"].as_str() != Some(binding_id.as_str())
        || operation["binding_generation"] != binding_generation
        || attempt["binding_id"].as_str() != Some(binding_id.as_str())
        || attempt["binding_generation"] != binding_generation
    {
        return Err(Error::new(
            "STALE_LAUNCH",
            "launch Operation and Attempt do not retain the exact binding generation",
        ));
    }
    let binding = super::operations::get_binding(db, &binding_id, binding_generation)?;
    if binding["state"] != "ready" || binding["released_at_ms"].is_number() {
        return Err(Error::new(
            "BINDING_NOT_READY",
            "participant issuance requires the exact ready, unreleased binding",
        ));
    }
    actor.require_bound_launch_attempt(
        db,
        operation_id,
        &task_id,
        task_revision,
        &attempt_id,
        &binding_id,
        binding_generation,
    )?;
    let open_operation_id = model::text(&manifest["binding"], "operation_id")?;
    let open_operation = super::operations::get_operation(db, open_operation_id)?;
    if open_operation["method"] != "agent.open"
        || open_operation["state"] != "settled"
        || open_operation["caller_id"].as_str() != Some(actor_caller_id(actor))
        || open_operation["prerequisite_operation_id"].as_str() != Some(operation_id)
        || open_operation["task_id"].as_str() != Some(task_id.as_str())
        || open_operation["attempt_id"].as_str() != Some(attempt_id.as_str())
        || open_operation["binding_id"].as_str() != Some(binding_id.as_str())
        || open_operation["binding_generation"] != binding_generation
    {
        return Err(Error::new(
            "STALE_LAUNCH",
            "ready binding does not come from the exact retained launch open Operation",
        ));
    }

    let lease: LeaseAuthorityRef =
        serde_json::from_value(manifest["workspace"]["lease_authority"].clone()).map_err(|_| {
            Error::new(
                "INVALID_LAUNCH_MANIFEST",
                "workspace authority reference is invalid",
            )
        })?;
    let lease_view = super::workspace::get_lease_view(db, &lease)?;
    if lease.state != "held"
        || lease.operation_id != operation_id
        || lease.task_id != task_id
        || lease.task_revision != task_revision
        || lease.attempt_id.as_deref() != Some(attempt_id.as_str())
        || Some(lease.owner_client_id.as_str()) != attempt["owner_id"].as_str()
        || lease.owner_client_id != actor_owner_id(actor)
        || attempt["owner_id"].as_str() != Some(actor_owner_id(actor))
        || lease.project_id != task["project_id"]
        || lease.plan_digest != manifest["plan_digest"]
        || lease_view["state"] != "held"
        || lease_view["binding_digest"] != lease.binding_digest
        || lease_view["registration_id"] != lease.registration_id
        || lease_view["registration_generation"] != lease.registration_generation
        || lease_view["project_id"] != lease.project_id
        || lease_view["operation_id"] != operation_id
        || lease_view["task_id"] != task_id
        || lease_view["task_revision"] != task_revision
        || lease_view["attempt_id"] != attempt_id
        || lease_view["owner_client_id"] != lease.owner_client_id
        || lease_view["plan_digest"] != lease.plan_digest
        || manifest["workspace"]["manifest_digest"].as_str().is_none()
    {
        return Err(Error::new(
            "WORKSPACE_LEASE_STALE",
            "participant issuance does not match the exact held Task workspace lease",
        ));
    }

    let request = &manifest["request"];
    let mcp_profile = model::text(request, "mcp_profile")?.to_owned();
    let mcp_surface = model::text(request, "mcp_surface")?.to_owned();
    config.mcp.validate()?;
    let profile = config
        .mcp
        .profiles
        .get(&mcp_profile)
        .ok_or_else(|| Error::new("CONFIG_ERROR", "launch MCP profile is not configured"))?;
    if profile.tool_profile != McpToolProfile::Participant
        || profile
            .surface
            .as_deref()
            .is_some_and(|surface| surface != mcp_surface)
    {
        return Err(Error::new(
            "FORBIDDEN",
            "launch issuance requires the exact configured Participant profile and surface",
        ));
    }
    crate::mcp::launch_profile_surface(
        profile.tool_profile,
        &mcp_surface,
        &profile.deferred_groups,
        &profile.manual_tools,
    )?;

    let native_session_id = exact_native_session_id(&attempt, &binding);
    Ok(LaunchIssuanceScope {
        manifest,
        operation,
        task_id,
        task_revision,
        attempt_id,
        binding_id,
        binding_generation,
        mcp_profile,
        mcp_surface,
        native_session_id,
    })
}

/// The binding root is only a candidate session identity. It is retained on
/// the Participant registration only when the same live Attempt also carries
/// producer evidence for that exact session.
fn exact_native_session_id(attempt: &Value, binding: &Value) -> Option<String> {
    let root = binding["native_root_id"]
        .as_str()
        .filter(|value| !value.is_empty())?;
    let producers = attempt["producers"].as_array()?;
    producers
        .iter()
        .any(|producer| {
            producer["native_session_id"].as_str() == Some(root)
                && !matches!(
                    producer["disposition"].as_str(),
                    Some("completed" | "failed" | "cancelled")
                )
        })
        .then(|| root.to_owned())
}

fn issuance_request_id(operation_id: &str) -> Result<String> {
    let request_id = format!("launch:{operation_id}:participant");
    if request_id.len() > 128 {
        return Err(Error::invalid(
            "launch identity is too long for participant issuance",
        ));
    }
    Ok(request_id)
}

fn build_issue_request(scope: &LaunchIssuanceScope, operation_id: &str) -> Result<IssueRequest> {
    Ok(IssueRequest {
        launch_operation_id: operation_id.to_owned(),
        client_request_id: issuance_request_id(operation_id)?,
        task_id: scope.task_id.clone(),
        task_revision: scope.task_revision,
        attempt_id: scope.attempt_id.clone(),
        binding_id: scope.binding_id.clone(),
        binding_generation: scope.binding_generation,
        participation_basis: ParticipationBasis::AttemptOwner,
        mcp_profile: scope.mcp_profile.clone(),
        mcp_surface: scope.mcp_surface.clone(),
        display_alias: None,
        inbound_policy: Some(InboundPolicy::PullOnly),
        native_session_id: scope.native_session_id.clone(),
    })
}

fn expected_participant_client_id(actor: &LaunchActor, request: &IssueRequest) -> Result<String> {
    crate::participant_credentials::client_id_for_launch(actor, request)
}

/// Recheck launch authority inside the same Store transaction that will write
/// the ordinary Participant registration. The caller must invoke this before
/// applying `coordination.participant.register`; host filesystem preparation
/// and an earlier read-only validation do not substitute for this check.
pub(super) fn validate_launch_registration(
    tx: &Transaction<'_>,
    actor: &LaunchActor,
    launch_operation_id: &str,
    config: &Config,
    registration_params: &Value,
) -> Result<()> {
    let scope = validated_scope(tx, actor, launch_operation_id, config, &[AWAITING_ISSUANCE])?;
    let request = build_issue_request(&scope, launch_operation_id)?;
    let expected_client_id = expected_participant_client_id(actor, &request)?;
    model::fields(
        registration_params,
        &[
            "client_request_id",
            "client_id",
            "token_hash",
            "task_id",
            "task_revision",
            "attempt_id",
            "participation_basis",
            "binding_id",
            "binding_generation",
            "native_session_id",
            "display_alias",
            "inbound_policy",
        ],
    )?;
    let token_hash = model::text(registration_params, "token_hash")?;
    let expected_basis = json!({
        "kind":"attempt_owner",
        "assignment_id":Value::Null,
        "review_scope":Value::Null,
    });
    let expected_session = json!(request.native_session_id);
    if model::text(registration_params, "client_request_id")? != request.client_request_id
        || model::text(registration_params, "client_id")? != expected_client_id
        || token_hash.len() != 64
        || !token_hash
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || model::text(registration_params, "task_id")? != request.task_id
        || model::positive(registration_params, "task_revision")? != request.task_revision
        || model::text(registration_params, "attempt_id")? != request.attempt_id
        || model::canonical(&registration_params["participation_basis"])?
            != model::canonical(&expected_basis)?
        || model::text(registration_params, "binding_id")? != request.binding_id
        || model::positive(registration_params, "binding_generation")? != request.binding_generation
        || registration_params.get("native_session_id") != Some(&expected_session)
        || model::text(registration_params, "display_alias")? != expected_client_id
        || model::text(registration_params, "inbound_policy")? != "pull_only"
    {
        return Err(Error::new(
            "STALE_LAUNCH",
            "Participant registration does not match the exact current launch issuance plan",
        ));
    }
    Ok(())
}

/// Build an idempotent issuer request from one live, exact launch scope.
/// This is a pure Store read; credential creation and Store registration are
/// performed later by the host issuer using this stable request ID.
pub(super) fn prepare_launch_issuance(
    db: &Connection,
    actor: &LaunchActor,
    operation_id: &str,
    config: &Config,
) -> Result<IssueRequest> {
    let scope = validated_scope(db, actor, operation_id, config, &[AWAITING_ISSUANCE])?;
    let request = build_issue_request(&scope, operation_id)?;
    let prior_registration: Option<(String, String, String)> = db
        .query_row(
            "SELECT method,state,operation_id FROM operations WHERE caller_id=?1 AND client_request_id=?2",
            params![actor_caller_id(actor), request.client_request_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    if let Some((method, state, _)) = prior_registration {
        if method != "coordination.participant.register" {
            return Err(Error::new(
                "REQUEST_ID_CONFLICT",
                "participant issuance request ID is already bound to another method",
            ));
        }
        if state == "outcome_unknown" {
            return Err(Error::new(
                "LAUNCH_ISSUANCE_UNKNOWN",
                "participant registration has an unresolved effect and will not be replayed",
            ));
        }
        if matches!(state.as_str(), "rejected" | "cancelled") {
            return Err(Error::new(
                "LAUNCH_ISSUANCE_REJECTED",
                "participant registration already has a terminal failure and will not be replayed",
            ));
        }
    }
    Ok(request)
}

fn validate_opaque_ref(value: &str, prefix: &str) -> Result<()> {
    let (actual_prefix, id) = value
        .split_once(':')
        .ok_or_else(|| Error::invalid("participant artifact reference is malformed"))?;
    let parsed = uuid::Uuid::parse_str(id)
        .map_err(|_| Error::invalid("participant artifact reference is malformed"))?;
    if actual_prefix != prefix || parsed.to_string() != id {
        return Err(Error::invalid(
            "participant artifact reference is malformed",
        ));
    }
    Ok(())
}

fn check_registered_participant(
    tx: &Transaction<'_>,
    actor: &LaunchActor,
    scope: &LaunchIssuanceScope,
    operation_id: &str,
    request: &IssueRequest,
    issued: &IssuedParticipant,
) -> Result<(String, Value, String)> {
    validate_opaque_ref(&issued.credential_ref, "participant-credential")?;
    validate_opaque_ref(&issued.profile_config_ref, "participant-mcp-config")?;
    let client_id = model::text(&issued.registration, "client_id")?.to_owned();
    let registration_operation_id = model::text(&issued.registration, "operation_id")?.to_owned();
    let expected_client_id = expected_participant_client_id(actor, request)?;
    let client_suffix = client_id.strip_prefix("participant-").unwrap_or_default();
    if issued.registration["role"] != "participant"
        || client_id != expected_client_id
        || client_suffix.len() != 48
        || !client_suffix
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || issued.registration["task_id"].as_str() != Some(scope.task_id.as_str())
        || issued.registration["task_revision"] != scope.task_revision
        || issued.registration["attempt_id"].as_str() != Some(scope.attempt_id.as_str())
        || issued.registration["participation_basis"]["kind"] != "attempt_owner"
        || issued.registration["usable"] != true
        || registration_operation_id == operation_id
    {
        return Err(Error::new(
            "STALE_PARTICIPANT",
            "issuer receipt does not match the exact launch assignment",
        ));
    }
    let registration = super::meta(tx, &format!("client:{client_id}"))?
        .ok_or_else(|| Error::new("STALE_PARTICIPANT", "issued Participant is not registered"))?;
    let attempt = super::tasks::get_attempt(tx, &scope.attempt_id)?;
    let expected_basis = json!({
        "kind":"attempt_owner",
        "assignment_id":Value::Null,
        "review_scope":Value::Null,
    });
    if registration["role"] != "participant"
        || registration["disabled"] != false
        || registration["task_id"].as_str() != Some(scope.task_id.as_str())
        || registration["task_revision"] != scope.task_revision
        || registration["attempt_id"].as_str() != Some(scope.attempt_id.as_str())
        || registration["participation_basis"] != expected_basis
        || registration["binding_id"].as_str() != Some(scope.binding_id.as_str())
        || registration["binding_generation"] != scope.binding_generation
        || registration["created_by"].as_str() != attempt["owner_id"].as_str()
        || registration["created_by"].as_str() != Some(actor_owner_id(actor))
        || registration["created_operation_id"].as_str() != Some(registration_operation_id.as_str())
        || registration["grant_revision"] != issued.registration["grant_revision"]
        || issued.registration["grant_revision"] != 1
    {
        return Err(Error::new(
            "STALE_PARTICIPANT",
            "stored Participant registration no longer matches the launch authority",
        ));
    }
    if registration["native_session_id"].as_str() != request.native_session_id.as_deref()
        || registration["native_session_id"].as_str() != scope.native_session_id.as_deref()
    {
        return Err(Error::new(
            "STALE_PARTICIPANT",
            "Participant session is not proven by the exact binding and Attempt producer",
        ));
    }

    let stored_operation: Option<(String, String, String, String, String, Option<String>)> = tx
        .query_row(
            "SELECT method,caller_id,client_request_id,state,original_request_json,result_json \
             FROM operations WHERE operation_id=?1",
            [&registration_operation_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .optional()?;
    let Some((method, caller_id, request_id, state, original_raw, result_raw)) = stored_operation
    else {
        return Err(Error::new(
            "STALE_PARTICIPANT",
            "participant registration Operation is missing",
        ));
    };
    let original: Value = serde_json::from_str(&original_raw)?;
    model::fields(
        &original,
        &[
            "client_request_id",
            "client_id",
            "token_hash",
            "task_id",
            "task_revision",
            "attempt_id",
            "participation_basis",
            "binding_id",
            "binding_generation",
            "native_session_id",
            "display_alias",
            "inbound_policy",
            "review_profile",
        ],
    )?;
    let expected_alias = request.display_alias.as_deref().unwrap_or(&client_id);
    let expected_policy = request.inbound_policy.unwrap_or(InboundPolicy::PullOnly);
    let expected_policy = match expected_policy {
        InboundPolicy::PullOnly => "pull_only",
    };
    if method != "coordination.participant.register"
        || caller_id != actor_caller_id(actor)
        || request_id != request.client_request_id
        || state == "outcome_unknown"
        || original["client_id"] != client_id
        || original["client_request_id"] != request.client_request_id
        || original["task_id"] != scope.task_id
        || original["task_revision"] != scope.task_revision
        || original["attempt_id"] != scope.attempt_id
        || original["binding_id"] != scope.binding_id
        || original["binding_generation"] != scope.binding_generation
        || original["participation_basis"] != expected_basis
        || original["native_session_id"].as_str() != request.native_session_id.as_deref()
        || original["display_alias"] != expected_alias
        || original["inbound_policy"] != expected_policy
        || original["review_profile"].is_string()
        || original["token_hash"].as_str().is_none_or(|hash| {
            hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
    {
        return Err(Error::new(
            "STALE_PARTICIPANT",
            "participant registration Operation does not match the exact issuer request",
        ));
    }
    if state != "settled" {
        return Err(Error::new(
            "LAUNCH_ISSUANCE_PENDING",
            "participant registration Operation has not settled",
        ));
    }
    let stored_result: Value = serde_json::from_str(
        &result_raw.ok_or_else(|| Error::new("STALE_PARTICIPANT", "issuer receipt is missing"))?,
    )?;
    if model::canonical(&stored_result)? != model::canonical(&issued.registration)? {
        return Err(Error::new(
            "STALE_PARTICIPANT",
            "issuer receipt differs from the retained Store registration result",
        ));
    }
    Ok((client_id, registration, registration_operation_id))
}

struct LaunchIssuanceRetention<'a> {
    operation_id: &'a str,
    manifest: Value,
    result: &'a Value,
    task_id: &'a str,
    attempt_id: &'a str,
    binding_id: &'a str,
    binding_generation: i64,
    now: i64,
}

fn retain_launch_issuance(
    tx: &Transaction<'_>,
    retention: LaunchIssuanceRetention<'_>,
) -> Result<()> {
    let LaunchIssuanceRetention {
        operation_id,
        mut manifest,
        result,
        task_id,
        attempt_id,
        binding_id,
        binding_generation,
        now,
    } = retention;
    let raw: String = tx.query_row(
        "SELECT effective_request_json FROM operations WHERE operation_id=?1 AND method='swarm.launch'",
        [operation_id],
        |row| row.get(0),
    )?;
    let mut effective: Value = serde_json::from_str(&raw)?;
    manifest["state"] = json!(AWAITING_NATIVE_MCP);
    effective["launch_manifest"] = manifest;
    effective["receipt"] = json!({"ok":true,"value":result});
    let changed = tx.execute(
        "UPDATE operations SET task_id=?2,attempt_id=?3,binding_id=?4,binding_generation=?5,\
         state='queued',result_json=?6,effective_request_json=?7,settled_at_ms=NULL,updated_at_ms=?8\
         WHERE operation_id=?1 AND method='swarm.launch' AND state='queued'\
           AND task_id=?2 AND attempt_id=?3 AND binding_id=?4 AND binding_generation=?5\
           AND json_extract(effective_request_json,'$.launch_manifest.state')='awaiting_participant_credential'",
        params![
            operation_id,
            task_id,
            attempt_id,
            binding_id,
            binding_generation,
            model::canonical(result)?,
            model::canonical(&effective)?,
            now,
        ],
    )?;
    if changed != 1 {
        return Err(Error::conflict(
            "launch changed before Participant issuance receipt was retained",
        ));
    }
    let digest = model::digest(model::canonical(result)?.as_bytes());
    tx.execute(
        "INSERT OR IGNORE INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms)\
         VALUES('controller',?1,?2,'swarm.launch.participant_issued',?3,?4)",
        params![
            format!("launch-participant:{operation_id}:{digest}"),
            operation_id,
            model::canonical(result)?,
            now,
        ],
    )?;
    Ok(())
}

/// Commit only the opaque issuer references and exact redacted Store
/// registration after rechecking the current Manager, Task, Attempt, binding,
/// and held workspace lease in this transaction.
pub(super) fn commit_launch_issuance(
    tx: &Transaction<'_>,
    actor: &LaunchActor,
    operation_id: &str,
    config: &Config,
    issued: &IssuedParticipant,
    now: i64,
) -> Result<Value> {
    let scope = validated_scope(
        tx,
        actor,
        operation_id,
        config,
        &[AWAITING_ISSUANCE, AWAITING_NATIVE_MCP],
    )?;
    let request = build_issue_request(&scope, operation_id)?;
    let (client_id, registration, registration_operation_id) =
        check_registered_participant(tx, actor, &scope, operation_id, &request, issued)?;
    if scope.manifest["state"] == AWAITING_NATIVE_MCP {
        if scope.manifest["participant"]["client_id"] == client_id
            && scope.manifest["participant"]["credential_ref"] == issued.credential_ref
            && scope.manifest["participant"]["profile_config_ref"] == issued.profile_config_ref
        {
            let result = scope.operation["result"].clone();
            if result["launch_state"] != AWAITING_NATIVE_MCP
                || result["state"] != "queued"
                || result["participant"]["client_id"] != client_id
                || result["participant"]["credential_ref"] != issued.credential_ref
                || result["participant"]["profile_config_ref"] != issued.profile_config_ref
                || result["dispatch_permitted"] != false
            {
                return Err(Error::new(
                    "INVALID_LAUNCH_MANIFEST",
                    "retained launch receipt differs from committed Participant issuance",
                ));
            }
            return Ok(result);
        }
        return Err(Error::new(
            "REQUEST_ID_CONFLICT",
            "launch already committed a different Participant issuance",
        ));
    }
    if scope.manifest["state"] != AWAITING_ISSUANCE {
        return Err(Error::new(
            "STALE_LAUNCH",
            "launch is no longer awaiting Participant credential issuance",
        ));
    }
    let participant = json!({
        "client_id":client_id,
        "role":"participant",
        "task_id":scope.task_id,
        "task_revision":scope.task_revision,
        "attempt_id":scope.attempt_id,
        "binding_id":scope.binding_id,
        "binding_generation":scope.binding_generation,
        "grant_revision":registration["grant_revision"],
        "participation_basis":registration["participation_basis"],
        "native_session_id":registration["native_session_id"],
        "registration_operation_id":registration_operation_id,
        "credential_ref":issued.credential_ref,
        "profile_config_ref":issued.profile_config_ref,
        "capability_state":"unknown",
    });
    let result = json!({
        "operation_id":operation_id,
        "launch_state":AWAITING_NATIVE_MCP,
        "state":"queued",
        "plan_digest":scope.manifest["plan_digest"],
        "workspace_manifest_digest":scope.manifest["workspace"]["manifest_digest"],
        "task_id":scope.task_id,
        "task_revision":scope.task_revision,
        "attempt_id":scope.attempt_id,
        "binding":{
            "binding_id":scope.binding_id,
            "generation":scope.binding_generation,
            "state":"ready",
        },
        "participant":participant,
        "dispatch_permitted":false,
        "task_dispatch":"not_started",
        "native_mcp_capability_state":"unknown",
        "gaps":[
            "native_mcp_harness_readback_not_observed",
            "task_dispatch_is_held_until_native_mcp_capability_is_verified",
        ],
    });
    let mut manifest = scope.manifest;
    manifest["participant"] = participant;
    manifest["progress"]["participant_credential"] = json!("registered_and_refs_retained");
    manifest["progress"]["capability_readback"] = json!("not_observed");
    manifest["progress"]["task_dispatch"] = json!("not_started");
    manifest["runtime"]["state"] = json!(AWAITING_NATIVE_MCP);
    manifest["runtime"]["dispatch_permitted"] = json!(false);
    // The preview used this identity as a template because the concrete
    // Participant did not exist yet. Promote the retained summary only after
    // the exact Store registration has been validated above.
    manifest["mcp"]["identity"]["status"] = json!("registered_enabled_participant");
    manifest["mcp"]["capability_state"] = json!("unknown");
    manifest["mcp"]["participant_client_id"] = json!(client_id);
    manifest["mcp"]["credential_ref"] = json!(issued.credential_ref);
    manifest["mcp"]["profile_config_ref"] = json!(issued.profile_config_ref);
    retain_launch_issuance(
        tx,
        LaunchIssuanceRetention {
            operation_id,
            manifest,
            result: &result,
            task_id: &scope.task_id,
            attempt_id: &scope.attempt_id,
            binding_id: &scope.binding_id,
            binding_generation: scope.binding_generation,
            now,
        },
    )?;
    Ok(result)
}
