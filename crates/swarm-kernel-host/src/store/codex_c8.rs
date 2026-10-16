//! Store-owned Codex C8 observation, readback validation, and dispatch gate.
//!
//! Each native observation is an immutable Operation queued after the exact
//! launch open. Unknown outcomes are terminal for that Operation and are never
//! replayed; an observed proof is rechecked against current Store assignment,
//! lease, binding, and authorization facts before dispatch.
use super::{
    Store,
    launcher_dispatch::{self, LaunchParent},
    launcher_mcp_tools, launcher_participant, meta, operations, tasks, workspace,
};
use crate::{
    config::Config,
    error::{Error, Result},
    model::{self, Principal, Role},
    workspace::LeaseAuthorityRef,
};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde_json::{Value, json};
use swarm_contracts::{
    module_catalog::Sha256Digest,
    module_contract::{
        CODEX_RUST_CONTROLLER_ARTIFACT_ID, CODEX_RUST_CONTROLLER_CURRENT_DESCRIPTOR_VERSION,
        codex_native_mcp_command_schema,
    },
    native_mcp::{
        CODEX_NATIVE_MCP_COMMAND_SCHEMA_ID, CODEX_NATIVE_MCP_COMMAND_SCHEMA_VERSION,
        CodexNativeMcpCommand, CodexNativeMcpReadback, codex_native_mcp_server_name,
        codex_participant_toolset,
    },
};

const INTERNAL_NATIVE_MCP_CALLER: &str = "swarm.internal.c8.native_mcp";
const MAX_FUTURE_SKEW_MS: i64 = 30_000;
const MARKER_FIELD: &str = "codex_native_mcp_readback";
const MAX_COMMAND_BYTES: usize = 32 * 1024;

#[derive(Debug)]
struct OperationRow {
    method: String,
    state: String,
    caller_id: String,
    task_id: Option<String>,
    attempt_id: Option<String>,
    binding_id: Option<String>,
    binding_generation: Option<i64>,
    original: Value,
    effective: Value,
    result: Option<Value>,
}

pub(super) async fn reconcile_once(store: &Store) -> Result<Option<Value>> {
    let now = model::now_ms()?;
    let config = store.config.clone();
    let progress = store
        .run(move |db| {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let Some(launch_id) = next_candidate(&tx)? else {
                tx.commit()?;
                return Ok(None);
            };
            let parent = launcher_dispatch::load_parent(&tx, &launch_id)?;
            let marker = parent.manifest.get(MARKER_FIELD).cloned();
            let (value, changed) = match marker {
                None => {
                    let child_id = queue_observation(&tx, &config, &parent, now)?;
                    (step_value(&launch_id, &child_id, "reading"), true)
                }
                Some(marker) if marker["state"] == "reading" => {
                    let child_id = marker["module_operation_id"]
                        .as_str()
                        .filter(|value| !value.is_empty())
                        .ok_or_else(invalid_marker)?
                        .to_owned();
                    match read_operation(&tx, &child_id)? {
                        None => {
                            let next = terminal_marker(
                                marker,
                                "invalid",
                                "CODEX_C8_CHILD_OPERATION_MISSING",
                            );
                            persist_marker(&tx, &parent, next, now)?;
                            (step_value(&launch_id, &child_id, "invalid"), true)
                        }
                        Some(child)
                            if matches!(
                                child.state.as_str(),
                                "queued" | "sending" | "native_accepted"
                            ) =>
                        {
                            (step_value(&launch_id, &child_id, "reading"), false)
                        }
                        Some(child) => {
                            let next = consume_operation(
                                &tx, &config, &parent, marker, &child_id, child, now,
                            )?;
                            let state = next["state"].as_str().unwrap_or("invalid").to_owned();
                            persist_marker(&tx, &parent, next, now)?;
                            (step_value(&launch_id, &child_id, &state), true)
                        }
                    }
                }
                Some(_) => {
                    tx.commit()?;
                    return Ok(None);
                }
            };
            tx.commit()?;
            Ok(Some((value, changed)))
        })
        .await?;
    let Some((value, changed)) = progress else {
        return Ok(None);
    };
    if changed {
        store
            .changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
    }
    Ok(Some(value))
}

fn step_value(launch_id: &str, child_id: &str, state: &str) -> Value {
    json!({
        "operation_id":launch_id,
        "module_operation_id":child_id,
        "state":state,
        "dispatch_permitted":false,
        "native_replay":false,
        "model_consumed":"unknown",
    })
}

fn next_candidate(db: &Connection) -> Result<Option<String>> {
    db.query_row(
        "SELECT launch.operation_id FROM operations AS launch \
         WHERE launch.method='swarm.launch' AND launch.state='queued' \
           AND CASE WHEN json_valid(launch.effective_request_json) THEN \
              json_extract(launch.effective_request_json,'$.launch_manifest.state') IN ('awaiting_native_mcp','awaiting_capability') \
             AND json_extract(launch.effective_request_json,'$.launch_manifest.runtime.dispatch_permitted')=0 \
             AND json_extract(launch.effective_request_json,'$.launch_manifest.progress.task_dispatch')='not_started' \
             AND json_extract(launch.effective_request_json,'$.launch_manifest.runtime.route.runtime')='codex' \
             AND json_extract(launch.effective_request_json,'$.launch_manifest.runtime.route.module_artifact_id')=?1 \
              AND json_extract(launch.effective_request_json,'$.launch_manifest.binding.state')='ready' \
              AND json_type(launch.effective_request_json,'$.launch_manifest.binding.native_root_id')='text' \
              AND json_type(launch.effective_request_json,'$.launch_manifest.binding.native_scope_key')='text' \
              AND EXISTS(SELECT 1 FROM bindings AS selected_binding \
                         WHERE selected_binding.binding_id=json_extract(launch.effective_request_json,'$.launch_manifest.binding.binding_id') \
                           AND selected_binding.generation=json_extract(launch.effective_request_json,'$.launch_manifest.binding.generation') \
                            AND json_extract(selected_binding.state_json,'$.module_contract_selector.artifact.version')=?2) \
              AND EXISTS(SELECT 1 FROM operations AS native_open \
                         WHERE native_open.operation_id=json_extract(launch.effective_request_json,'$.launch_manifest.binding.operation_id') \
                           AND native_open.method='agent.open' AND native_open.state='settled' \
                           AND json_extract(native_open.result_json,'$.outcome')='applied') \
             AND ( \
               json_type(launch.effective_request_json,'$.launch_manifest.codex_native_mcp_readback') IS NULL \
               OR (json_extract(launch.effective_request_json,'$.launch_manifest.codex_native_mcp_readback.state')='reading' \
                   AND (COALESCE(json_type(launch.effective_request_json,'$.launch_manifest.codex_native_mcp_readback.module_operation_id'),'')!='text' \
                     OR EXISTS(SELECT 1 FROM operations AS child \
                               WHERE child.operation_id=json_extract(launch.effective_request_json,'$.launch_manifest.codex_native_mcp_readback.module_operation_id') \
                                 AND child.state IN ('settled','rejected','outcome_unknown','cancelled')))) \
             ) \
           ELSE 0 END \
         ORDER BY launch.created_at_ms,launch.operation_id LIMIT 1",
         params![
              CODEX_RUST_CONTROLLER_ARTIFACT_ID,
              CODEX_RUST_CONTROLLER_CURRENT_DESCRIPTOR_VERSION,
         ],
        |row| row.get(0),
    ).optional().map_err(Into::into)
}

fn queue_observation(
    tx: &Transaction<'_>,
    config: &Config,
    parent: &LaunchParent,
    now: i64,
) -> Result<String> {
    let operation_id = model::new_id();
    launcher_participant::require_launch_activation_facts(
        tx,
        &parent.operation_id,
        &parent.manifest,
        model::positive(&parent.manifest["task"], "observed_revision")?,
        &tasks::get_attempt(tx, model::text(&parent.manifest["task"], "attempt_id")?)?,
    )?;
    let request = current_command(tx, config, parent, &operation_id, None)?;
    launcher_mcp_tools::require_internal_native_mcp_client(tx)?;

    let original = serde_json::to_value(&request)?;
    let original_bytes = model::canonical(&original)?;
    if original_bytes.len() > MAX_COMMAND_BYTES {
        return Err(Error::new(
            "NATIVE_MCP_COMMAND_LIMIT",
            "Codex C8 command exceeds its bounded Operation input",
        ));
    }
    let input_sha256 = model::digest(original_bytes.as_bytes());
    let mut enriched = request.clone();
    enriched.input_sha256 = Some(
        Sha256Digest::new(input_sha256.clone())
            .map_err(|_| Error::invalid("Codex C8 Operation digest is invalid"))?,
    );
    enriched
        .validate()
        .map_err(|_| Error::invalid("Codex C8 command identity is invalid"))?;
    let effect = enriched.effect_identity();
    let effective = json!({"native_mcp":{
        "schema_version":2,
        "child_operation_id":operation_id,
        "parent_launch_operation_id":parent.operation_id,
        "launch_identity_digest":launch_identity_digest(parent)?,
        "phase":"observe_assignment",
        "method":"native.mcp.observe",
        "task_id":request.task_id,
        "task_revision":request.task_revision,
        "attempt_id":request.attempt_id,
        "binding_id":request.binding_id,
        "binding_generation":request.binding_generation,
        "assignment_digest":request.assignment_sha256,
        "input_sha256":input_sha256,
        "native_session_id":request.native_root_id,
        "command_sha256":model::digest(model::canonical(&effect)?.as_bytes()),
        "effect":effect,
    }});
    let request_id = format!("codex-native-mcp:{operation_id}");
    tx.execute(
        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,binding_id,binding_generation,state,native_refs_json,due_at_ms,created_at_ms,updated_at_ms) \
         VALUES(?1,?2,?3,'native.mcp.observe',?4,?5,?6,?7,?8,?9,'queued','{}',?10,?10,?10)",
        params![
            operation_id, INTERNAL_NATIVE_MCP_CALLER, request_id,
            original_bytes, model::canonical(&effective)?,
            request.task_id, request.attempt_id, request.binding_id,
            request.binding_generation, now,
        ],
    )?;
    let marker = json!({
        "schema_version":1,
        "kind":"codex_native_mcp_readback",
        "state":"reading",
        "module_operation_id":operation_id,
        "assignment_sha256":request.assignment_sha256,
        "started_at_ms":now,
        "dispatch_permitted":false,
        "native_replay":false,
        "model_consumed":"unknown",
    });
    persist_marker(tx, parent, marker, now)?;
    Ok(operation_id)
}
fn terminal_marker(mut marker: Value, state: &str, code: &str) -> Value {
    marker["state"] = json!(state);
    marker["diagnostic_code"] = json!(code);
    marker["dispatch_permitted"] = json!(false);
    marker["native_replay"] = json!(false);
    marker["model_consumed"] = json!("unknown");
    marker
}

fn consume_operation(
    tx: &Transaction<'_>,
    config: &Config,
    parent: &LaunchParent,
    mut marker: Value,
    child_id: &str,
    child: OperationRow,
    now: i64,
) -> Result<Value> {
    if child.state == "outcome_unknown"
        || child
            .result
            .as_ref()
            .is_some_and(|result| result["outcome"] == "unknown")
    {
        return Ok(terminal_marker(
            marker,
            "outcome_unknown",
            "CODEX_NATIVE_MCP_OUTCOME_UNKNOWN_NO_REPLAY",
        ));
    }
    if child.method != "native.mcp.observe"
        || child.caller_id != INTERNAL_NATIVE_MCP_CALLER
        || child.task_id.as_deref() != parent.task_id.as_deref()
        || child.attempt_id.as_deref() != parent.attempt_id.as_deref()
        || child.binding_id.as_deref() != parent.binding_id.as_deref()
        || child.binding_generation != parent.binding_generation
    {
        return Ok(terminal_marker(
            marker,
            "invalid",
            "CODEX_C8_CHILD_SCOPE_MISMATCH",
        ));
    }
    if child.state != "settled"
        || child
            .result
            .as_ref()
            .is_none_or(|result| result["outcome"] != "applied")
    {
        let code = child
            .result
            .as_ref()
            .and_then(|result| result["details"]["diagnostic_code"].as_str())
            .filter(|value| !value.is_empty() && value.len() <= 128)
            .unwrap_or("CODEX_NATIVE_MCP_READBACK_NOT_APPLIED");
        return Ok(terminal_marker(marker, "unavailable", code));
    }

    let expected = current_command(tx, config, parent, child_id, None)?;
    let stored: CodexNativeMcpCommand =
        serde_json::from_value(child.original.clone()).map_err(|_| {
            Error::new(
                "CODEX_C8_COMMAND_INVALID",
                "retained Codex C8 command is invalid",
            )
        })?;
    if stored != expected
        || stored.input_sha256.is_some()
        || marker["assignment_sha256"] != stored.assignment_sha256.as_str()
    {
        return Ok(terminal_marker(marker, "invalid", "CODEX_C8_COMMAND_STALE"));
    }
    let original_bytes = model::canonical(&child.original)?;
    let input_sha256 = model::digest(original_bytes.as_bytes());
    let effect = stored.effect_identity();
    let native_effect = &child.effective["native_mcp"];
    if native_effect["schema_version"] != 2
        || native_effect["child_operation_id"] != child_id
        || native_effect["parent_launch_operation_id"] != parent.operation_id
        || native_effect["launch_identity_digest"] != launch_identity_digest(parent)?
        || native_effect["task_id"] != stored.task_id
        || native_effect["task_revision"] != stored.task_revision
        || native_effect["attempt_id"] != stored.attempt_id
        || native_effect["binding_id"] != stored.binding_id
        || native_effect["binding_generation"] != stored.binding_generation
        || native_effect["native_session_id"] != stored.native_root_id
        || native_effect["method"] != "native.mcp.observe"
        || native_effect["phase"] != "observe_assignment"
        || native_effect["input_sha256"] != input_sha256
        || native_effect["assignment_digest"] != stored.assignment_sha256.as_str()
        || native_effect["effect"] != effect
        || native_effect["command_sha256"] != model::digest(model::canonical(&effect)?.as_bytes())
    {
        return Ok(terminal_marker(
            marker,
            "invalid",
            "CODEX_C8_EFFECT_IDENTITY_MISMATCH",
        ));
    }
    let result = child.result.as_ref().ok_or_else(|| {
        Error::new(
            "CODEX_C8_RESULT_MISSING",
            "applied Codex C8 result is missing",
        )
    })?;
    let details = &result["details"];
    if details["native_mcp_action"] != "native.mcp.observe"
        || details["native_replay"] != false
        || details["dispatch_permitted"] != false
        || details["model_consumed"] != "unknown"
    {
        return Ok(terminal_marker(
            marker,
            "invalid",
            "CODEX_C8_RESULT_ENVELOPE_INVALID",
        ));
    }
    if !module_receipt_is_exact(details, &stored, child_id, &input_sha256) {
        return Ok(terminal_marker(
            marker,
            "invalid",
            "CODEX_C8_MODULE_RECEIPT_MISMATCH",
        ));
    }
    let readback: CodexNativeMcpReadback =
        match serde_json::from_value(details["codex_native_mcp_readback"].clone()) {
            Ok(value) => value,
            Err(_) => {
                return Ok(terminal_marker(
                    marker,
                    "invalid",
                    "CODEX_C8_READBACK_SCHEMA_INVALID",
                ));
            }
        };
    if !readback_matches_command(&readback, &stored) {
        return Ok(terminal_marker(
            marker,
            "invalid",
            "CODEX_C8_READBACK_SCOPE_MISMATCH",
        ));
    }
    marker["state"] = json!("observed");
    marker["readback"] = serde_json::to_value(readback)?;
    marker["observed_at_ms"] = marker["readback"]["observed_at_ms"].clone();
    marker["diagnostic_code"] = Value::Null;
    marker["dispatch_permitted"] = json!(false);
    marker["native_replay"] = json!(false);
    marker["model_consumed"] = json!("unknown");
    marker["completed_at_ms"] = json!(now);
    Ok(marker)
}

fn module_receipt_is_exact(
    details: &Value,
    command: &CodexNativeMcpCommand,
    operation_id: &str,
    input_sha256: &str,
) -> bool {
    let receipt = &details["module_receipt"];
    receipt["module_id"] == "codex"
        && receipt["protocol"]["major"] == 1
        && receipt["protocol"]["minor"] == 0
        && receipt["operation_id"] == operation_id
        && receipt["input_sha256"] == input_sha256
        && receipt["binding_id"] == command.binding_id
        && receipt["binding_generation"] == command.binding_generation
        && receipt["artifact"]["artifact_id"] == CODEX_RUST_CONTROLLER_ARTIFACT_ID
        && receipt["artifact"]["version"] == CODEX_RUST_CONTROLLER_CURRENT_DESCRIPTOR_VERSION
}

fn readback_matches_command(
    readback: &CodexNativeMcpReadback,
    command: &CodexNativeMcpCommand,
) -> bool {
    let expected_tool_count = codex_participant_toolset()
        .ok()
        .map(|(tools, _)| tools.len());
    readback.validate().is_ok()
        && expected_tool_count == Some(readback.tool_count as usize)
        && readback.operation_id == command.operation_id
        && readback.launch_operation_id == command.launch_operation_id
        && readback.assignment_sha256 == command.assignment_sha256
        && readback.binding_id == command.binding_id
        && readback.binding_generation == command.binding_generation
        && readback.open_operation_id == command.open_operation_id
        && readback.native_root_id == command.native_root_id
        && readback.native_scope_key == command.native_scope_key
        && readback.adapter_boot_id == command.expected_adapter_boot_id
        && readback.participant_id == command.participant_id
        && readback.task_id == command.task_id
        && readback.task_revision == command.task_revision
        && readback.attempt_id == command.attempt_id
        && readback.grant_revision == command.grant_revision
        && readback.participant_native_session_id == command.participant_native_session_id
        && readback.surface == command.surface
        && readback.server_id_sha256.as_str() == model::digest(command.server_name.as_bytes())
        && readback.authorization_revision == command.expected_authorization_revision
        && readback.toolset_sha256 == command.expected_toolset_sha256
}

fn persist_marker(
    tx: &Transaction<'_>,
    parent: &LaunchParent,
    marker: Value,
    now: i64,
) -> Result<()> {
    let mut effective = parent.effective.clone();
    effective["launch_manifest"][MARKER_FIELD] = marker;
    let changed = tx.execute(
        "UPDATE operations SET effective_request_json=?2,updated_at_ms=?3 \
         WHERE operation_id=?1 AND method='swarm.launch' AND state='queued' AND effective_request_json=?4",
        params![
            parent.operation_id,
            model::canonical(&effective)?,
            now,
            parent.effective_request_json,
        ],
    )?;
    if changed != 1 {
        return Err(Error::conflict(
            "Codex C8 launch changed before its readback marker was retained",
        ));
    }
    Ok(())
}

fn read_operation(db: &Connection, operation_id: &str) -> Result<Option<OperationRow>> {
    type RawOperationRow = (
        String,
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<i64>,
        String,
        String,
        Option<String>,
    );
    let raw: Option<RawOperationRow> = db.query_row(
        "SELECT method,state,caller_id,task_id,attempt_id,binding_id,binding_generation,original_request_json,effective_request_json,result_json \
         FROM operations WHERE operation_id=?1",
        [operation_id],
        |row| Ok((
            row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?,
            row.get(5)?, row.get(6)?, row.get(7)?, row.get(8)?, row.get(9)?,
        )),
    ).optional()?;
    let Some((
        method,
        state,
        caller_id,
        task_id,
        attempt_id,
        binding_id,
        binding_generation,
        original,
        effective,
        result,
    )) = raw
    else {
        return Ok(None);
    };
    Ok(Some(OperationRow {
        method,
        state,
        caller_id,
        task_id,
        attempt_id,
        binding_id,
        binding_generation,
        original: serde_json::from_str(&original)?,
        effective: serde_json::from_str(&effective)?,
        result: result.map(|raw| serde_json::from_str(&raw)).transpose()?,
    }))
}
fn current_command(
    db: &Connection,
    config: &Config,
    parent: &LaunchParent,
    operation_id: &str,
    dispatch_operation_id: Option<&str>,
) -> Result<CodexNativeMcpCommand> {
    let task_id = parent
        .task_id
        .as_deref()
        .filter(|value| !value.is_empty())
        .ok_or_else(stale_scope)?;
    let attempt_id = parent
        .attempt_id
        .as_deref()
        .filter(|value| !value.is_empty())
        .ok_or_else(stale_scope)?;
    let binding_id = parent
        .binding_id
        .as_deref()
        .filter(|value| !value.is_empty())
        .ok_or_else(stale_scope)?;
    let generation = parent
        .binding_generation
        .filter(|value| *value > 0)
        .ok_or_else(stale_scope)?;
    let attempt = tasks::get_attempt(db, attempt_id)?;
    let task = tasks::get_task(db, task_id)?;
    let binding = operations::get_binding(db, binding_id, generation)?;
    launcher_dispatch::validate_parent_tuple(
        parent,
        &attempt,
        &task,
        &binding,
        dispatch_operation_id,
    )?;
    require_current_lease(db, parent, &attempt)?;

    let manifest = &parent.manifest;
    let participant_id = model::text(&manifest["participant"], "client_id")?;
    let credential_ref = model::text(&manifest["participant"], "credential_ref")?;
    let profile_config_ref = model::text(&manifest["participant"], "profile_config_ref")?;
    let grant_revision = model::positive(&manifest["participant"], "grant_revision")?;
    let profile_name = model::text(&manifest["mcp"], "profile_name")?;
    let surface = model::text(&manifest["mcp"], "surface")?;
    let registration = meta(db, &format!("client:{participant_id}"))?.ok_or_else(stale_scope)?;
    let expected_basis = json!({
        "kind":"attempt_owner",
        "assignment_id":Value::Null,
        "review_scope":Value::Null,
    });
    if manifest["participant"]["role"] != "participant"
        || manifest["participant"]["task_id"] != task_id
        || manifest["participant"]["task_revision"] != task["revision"]
        || manifest["participant"]["attempt_id"] != attempt_id
        || manifest["participant"]["binding_id"] != binding_id
        || manifest["participant"]["binding_generation"] != generation
        || manifest["participant"]["grant_revision"] != grant_revision
        || manifest["participant"]["participation_basis"] != expected_basis
        || manifest["participant"]["credential_ref"] != manifest["mcp"]["credential_ref"]
        || manifest["participant"]["profile_config_ref"] != manifest["mcp"]["profile_config_ref"]
        || manifest["mcp"]["participant_client_id"] != participant_id
        || manifest["mcp"]["hard_profile"] != "participant"
        || manifest["mcp"]["capability_state"] != "unknown"
        || registration["role"] != "participant"
        || registration["disabled"] != false
        || registration["task_id"] != task_id
        || registration["task_revision"] != task["revision"]
        || registration["attempt_id"] != attempt_id
        || registration["binding_id"] != binding_id
        || registration["binding_generation"] != generation
        || registration["grant_revision"] != grant_revision
        || registration["participation_basis"] != expected_basis
        || registration["created_operation_id"]
            != manifest["participant"]["registration_operation_id"]
        || registration["native_session_id"] != manifest["participant"]["native_session_id"]
        || manifest["mcp"]["credential_ref"] != credential_ref
        || manifest["mcp"]["profile_config_ref"] != profile_config_ref
        || manifest["mcp"]["profile_name"] != profile_name
        || manifest["mcp"]["surface"] != surface
    {
        return Err(stale_scope());
    }

    let route_alias = model::text(&manifest["runtime"]["route"], "alias")?;
    let route = config.route(route_alias)?;
    if route.runtime != "codex"
        || route.module_artifact_id != CODEX_RUST_CONTROLLER_ARTIFACT_ID
        || binding["route"]["runtime"] != "codex"
        || binding["module_artifact_id"] != CODEX_RUST_CONTROLLER_ARTIFACT_ID
        || binding["route"]["module_artifact_id"] != CODEX_RUST_CONTROLLER_ARTIFACT_ID
        || binding["state"] != "ready"
        || !binding["released_at_ms"].is_null()
    {
        return Err(stale_scope());
    }
    require_codex_descriptor(db, &binding)?;

    let native_root_id = model::text(&binding, "native_root_id")?;
    let native_scope_key = model::text(&binding, "native_scope_key")?;
    let expected_adapter_boot_id = model::text(&binding["observation"], "bridge_boot_id")?;
    let open_operation_id = model::text(&manifest["binding"], "operation_id")?;
    let open = operations::get_operation(db, open_operation_id)?;
    if open["method"] != "agent.open"
        || open["state"] != "settled"
        || open["result"]["outcome"] != "applied"
        || open["prerequisite_operation_id"] != parent.operation_id
        || open["task_id"] != task_id
        || open["attempt_id"] != attempt_id
        || open["binding_id"] != binding_id
        || open["binding_generation"] != generation
        || open["result"]["native_root_id"] != native_root_id
        || open["result"]["native_scope_key"] != native_scope_key
        || manifest["binding"]["native_root_id"] != native_root_id
    {
        return Err(stale_scope());
    }

    let (_tools, expected_toolset_sha256) = codex_participant_toolset().map_err(|_| {
        Error::new(
            "NATIVE_MCP_CAPABILITY_UNAVAILABLE",
            "Participant tool catalog is invalid",
        )
    })?;
    let authorization = super::mcp_authorization(
        db,
        &Principal {
            link_id: "internal-codex-c8-scope".to_owned(),
            client_id: participant_id.to_owned(),
            role: Role::Participant,
        },
        &json!({"task_id":task_id}),
    )?;
    let expected_authorization_revision =
        Sha256Digest::new(model::text(&authorization, "authorization_revision")?.to_owned())
            .map_err(|_| {
                Error::new(
                    "NATIVE_MCP_CAPABILITY_UNAVAILABLE",
                    "Participant authorization revision is invalid",
                )
            })?;
    let server_name = codex_native_mcp_server_name(&parent.operation_id).map_err(|_| {
        Error::new(
            "NATIVE_MCP_CAPABILITY_UNAVAILABLE",
            "Codex MCP server identity is invalid",
        )
    })?;

    let mut command = CodexNativeMcpCommand {
        schema_version: 3,
        codex_schema_version: 3,
        kind: "codex_native_mcp_command".to_owned(),
        operation_id: operation_id.to_owned(),
        launch_operation_id: parent.operation_id.clone(),
        phase: "observe".to_owned(),
        observation_kind: "assigned_session".to_owned(),
        input_sha256: None,
        assignment_sha256: Sha256Digest::new("0".repeat(64))
            .map_err(|_| Error::invalid("Codex C8 assignment digest is invalid"))?,
        binding_id: binding_id.to_owned(),
        binding_generation: generation,
        open_operation_id: open_operation_id.to_owned(),
        native_root_id: native_root_id.to_owned(),
        native_scope_key: native_scope_key.to_owned(),
        expected_adapter_boot_id: expected_adapter_boot_id.to_owned(),
        task_id: task_id.to_owned(),
        task_revision: model::positive(&manifest["task"], "observed_revision")?,
        attempt_id: attempt_id.to_owned(),
        participant_id: participant_id.to_owned(),
        grant_revision,
        participant_native_session_id: registration["native_session_id"]
            .as_str()
            .map(str::to_owned),
        surface: surface.to_owned(),
        server_name,
        expected_authorization_revision,
        expected_toolset_sha256,
    };
    command.assignment_sha256 = command
        .computed_assignment_sha256()
        .map_err(|_| Error::invalid("Codex C8 assignment digest is invalid"))?;
    command
        .validate()
        .map_err(|_| Error::invalid("Codex C8 command identity is invalid"))?;
    Ok(command)
}

pub(super) fn require_codex_descriptor(db: &Connection, binding: &Value) -> Result<()> {
    let identity = super::module_handshake::retained_contract_identity(
        db,
        CODEX_RUST_CONTROLLER_ARTIFACT_ID,
        binding["observation"].get("module_contract_selector"),
    )?
    .ok_or_else(stale_scope)?;
    let has_capability = identity
        .capabilities
        .iter()
        .any(|value| value.as_str() == "native.mcp.observe");
    let has_dispatch_capability = identity
        .capabilities
        .iter()
        .any(|value| value.as_str() == "task.dispatch");
    let has_codex_schema = identity
        .command_schemas
        .contains(&codex_native_mcp_command_schema());
    let has_launch_profile_schema = identity
        .command_schemas
        .contains(&swarm_contracts::module_contract::codex_native_mcp_launch_profile_schema());
    if identity.module_id.as_str() != "codex"
        || identity.artifact.artifact_id.as_str() != CODEX_RUST_CONTROLLER_ARTIFACT_ID
        || identity.artifact.version.as_str() != CODEX_RUST_CONTROLLER_CURRENT_DESCRIPTOR_VERSION
        || !has_capability
        || !has_dispatch_capability
        || !has_codex_schema
        || !has_launch_profile_schema
        || identity.command_schemas.iter().any(|schema| {
            schema.schema_id == CODEX_NATIVE_MCP_COMMAND_SCHEMA_ID
                && schema.version != CODEX_NATIVE_MCP_COMMAND_SCHEMA_VERSION
        })
    {
        return Err(Error::new(
            "NATIVE_MCP_CAPABILITY_UNAVAILABLE",
            "retained Codex descriptor does not select the C8 command contract",
        ));
    }
    Ok(())
}

fn require_current_lease(db: &Connection, parent: &LaunchParent, attempt: &Value) -> Result<()> {
    let manifest = &parent.manifest;
    let authority: LeaseAuthorityRef =
        serde_json::from_value(manifest["workspace"]["lease_authority"].clone())
            .map_err(|_| stale_scope())?;
    if authority.operation_id != parent.operation_id
        || authority.task_id != model::text(&manifest["task"], "task_id")?
        || authority.task_revision != model::positive(&manifest["task"], "observed_revision")?
        || authority.attempt_id.as_deref() != manifest["task"]["attempt_id"].as_str()
        || authority.state != "held"
        || authority.plan_digest.as_str() != manifest["plan_digest"].as_str().unwrap_or_default()
    {
        return Err(stale_scope());
    }
    let lease = workspace::get_lease_view(db, &authority).map_err(|_| stale_scope())?;
    if lease["state"] != "held"
        || lease["operation_id"] != parent.operation_id
        || lease["task_id"] != manifest["task"]["task_id"]
        || lease["attempt_id"] != manifest["task"]["attempt_id"]
        || lease["owner_client_id"] != attempt["owner_id"]
        || lease["plan_digest"] != manifest["plan_digest"]
    {
        return Err(stale_scope());
    }
    Ok(())
}
fn launch_identity_digest(parent: &LaunchParent) -> Result<String> {
    Ok(model::digest(
        model::canonical(&json!({
            "launch_operation_id":parent.operation_id,
            "plan_digest":parent.manifest["plan_digest"],
            "task":parent.manifest["task"],
            "binding":parent.manifest["binding"],
            "participant":parent.manifest["participant"],
            "mcp":{
                "profile_name":parent.manifest["mcp"]["profile_name"],
                "surface":parent.manifest["mcp"]["surface"],
                "credential_ref":parent.manifest["mcp"]["credential_ref"],
                "profile_config_ref":parent.manifest["mcp"]["profile_config_ref"],
            },
        }))?
        .as_bytes(),
    ))
}

fn invalid_marker() -> Error {
    Error::new(
        "CODEX_C8_MARKER_INVALID",
        "Codex C8 readback marker is malformed",
    )
}

fn stale_scope() -> Error {
    Error::new(
        "STALE_LAUNCH_DISPATCH_SCOPE",
        "Codex C8 proof no longer matches the current launch assignment",
    )
}

pub(super) fn require_current_dispatch_capability(
    db: &Connection,
    config: &Config,
    launch_operation_id: &str,
    dispatch_operation_id: Option<&str>,
) -> Result<Value> {
    let parent = launcher_dispatch::load_parent(db, launch_operation_id)?;
    let marker = &parent.manifest[MARKER_FIELD];
    if marker["schema_version"] != 1
        || marker["kind"] != "codex_native_mcp_readback"
        || marker["state"] != "observed"
    {
        return Err(Error::new(
            "NATIVE_MCP_CAPABILITY_UNAVAILABLE",
            "Codex C8 per-thread Participant MCP proof is not current",
        ));
    }
    let readback: CodexNativeMcpReadback = serde_json::from_value(marker["readback"].clone())
        .map_err(|_| {
            Error::new(
                "NATIVE_MCP_READBACK_INVALID",
                "Codex C8 readback is invalid",
            )
        })?;
    readback.validate().map_err(|_| {
        Error::new(
            "NATIVE_MCP_READBACK_INVALID",
            "Codex C8 readback is invalid",
        )
    })?;
    let now = model::now_ms()?;
    if readback.observed_at_ms > now.saturating_add(MAX_FUTURE_SKEW_MS) {
        return Err(Error::new(
            "NATIVE_MCP_READBACK_INVALID",
            "Codex C8 readback timestamp is in the future",
        ));
    }
    let child_id = marker["module_operation_id"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(invalid_marker)?;
    let expected = current_command(db, config, &parent, child_id, dispatch_operation_id)?;
    let child = read_operation(db, child_id)?.ok_or_else(|| {
        Error::new(
            "NATIVE_MCP_READBACK_INVALID",
            "Codex C8 Operation is missing",
        )
    })?;
    let stored: CodexNativeMcpCommand = serde_json::from_value(child.original.clone())
        .map_err(|_| Error::new("NATIVE_MCP_READBACK_INVALID", "Codex C8 command is invalid"))?;
    if stored != expected
        || stored.input_sha256.is_some()
        || !readback_matches_command(&readback, &stored)
    {
        return Err(Error::new(
            "NATIVE_MCP_READBACK_INVALID",
            "Codex C8 readback does not match its current Store assignment",
        ));
    }
    let result = child
        .result
        .as_ref()
        .ok_or_else(|| Error::new("NATIVE_MCP_READBACK_INVALID", "Codex C8 result is missing"))?;
    if child.method != "native.mcp.observe"
        || child.state != "settled"
        || result["outcome"] != "applied"
        || child.caller_id != INTERNAL_NATIVE_MCP_CALLER
        || child.task_id.as_deref() != Some(stored.task_id.as_str())
        || child.attempt_id.as_deref() != Some(stored.attempt_id.as_str())
        || child.binding_id.as_deref() != Some(stored.binding_id.as_str())
        || child.binding_generation != Some(stored.binding_generation)
        || result["details"]["native_mcp_action"] != "native.mcp.observe"
        || result["details"]["native_replay"] != false
        || result["details"]["dispatch_permitted"] != false
        || result["details"]["model_consumed"] != "unknown"
        || result["details"]["codex_native_mcp_readback"] != marker["readback"]
    {
        return Err(Error::new(
            "NATIVE_MCP_READBACK_INVALID",
            "Codex C8 native Operation is not a settled non-replayed observation",
        ));
    }
    let original_bytes = model::canonical(&child.original)?;
    let input_sha256 = model::digest(original_bytes.as_bytes());
    if !module_receipt_is_exact(&result["details"], &stored, child_id, &input_sha256) {
        return Err(Error::new(
            "NATIVE_MCP_READBACK_INVALID",
            "Codex C8 module receipt is not exact",
        ));
    }
    let effect = stored.effect_identity();
    let native_effect = &child.effective["native_mcp"];
    if native_effect["schema_version"] != 2
        || native_effect["child_operation_id"] != child_id
        || native_effect["parent_launch_operation_id"] != parent.operation_id
        || native_effect["launch_identity_digest"] != launch_identity_digest(&parent)?
        || native_effect["task_id"] != stored.task_id
        || native_effect["task_revision"] != stored.task_revision
        || native_effect["attempt_id"] != stored.attempt_id
        || native_effect["binding_id"] != stored.binding_id
        || native_effect["binding_generation"] != stored.binding_generation
        || native_effect["native_session_id"] != stored.native_root_id
        || native_effect["method"] != "native.mcp.observe"
        || native_effect["phase"] != "observe_assignment"
        || native_effect["input_sha256"] != input_sha256
        || native_effect["assignment_digest"] != stored.assignment_sha256.as_str()
        || native_effect["effect"] != effect
        || native_effect["command_sha256"] != model::digest(model::canonical(&effect)?.as_bytes())
    {
        return Err(Error::new(
            "NATIVE_MCP_READBACK_INVALID",
            "Codex C8 private effect identity is not exact",
        ));
    }
    serde_json::to_value(readback).map_err(Into::into)
}
