use super::{launcher_dispatch, meta, prerequisites, tasks};
use crate::{
    automation::authorization::{on_behalf_visible_to, operation_link},
    config::Config,
    error::{Error, Result},
    model::{self, Principal, Role},
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde_json::{Value, json};

const MAX_OWNED_SERVICE_START_FAILURE_BYTES: usize = 1024;
const MAX_OWNED_SERVICE_DISPATCH_FAILURE_BYTES: usize = 1024;
const MAX_WORKSPACE_LAUNCH_FAILURE_BYTES: usize = 1024;
const MAX_MANAGER_ACTION_REQUIRED_ITEMS: i64 = 32;
const MAX_PUBLIC_NATIVE_FAILURES: usize = 64;
const START_FAILURE_V1_KEYS: [&str; 5] = [
    "schema_version",
    "status",
    "stage",
    "error_code",
    "native_effect",
];
const START_FAILURE_V2_KEYS: [&str; 7] = [
    "schema_version",
    "status",
    "stage",
    "error_code",
    "native_effect",
    "request_phase",
    "http_status",
];
const DISPATCH_FAILURE_KEYS: [&str; 6] = [
    "schema_version",
    "status",
    "stage",
    "error_code",
    "native_effect",
    "retry_authorized",
];

pub(super) fn get_operation(db: &Connection, id: &str) -> Result<Value> {
    let raw:Option<String>=db.query_row("SELECT json_object('operation_id',operation_id,'caller_id',caller_id,'method',method,'state',state,'task_id',task_id,'attempt_id',attempt_id,'binding_id',binding_id,'binding_generation',binding_generation,'prerequisite_operation_id',prerequisite_operation_id,'operation_contract',json_extract(effective_request_json,'$.operation_contract'),'native_refs',json(native_refs_json),'result',json(result_json),'created_at_ms',created_at_ms,'updated_at_ms',updated_at_ms) FROM operations WHERE operation_id=?1",[id],|r|r.get(0)).optional()?;
    Ok(serde_json::from_str(&raw.ok_or_else(|| {
        Error::new("NOT_FOUND", format!("Operation {id}"))
    })?)?)
}

/// Current-manager readback adds bounded startup, bridge-recovery,
/// native-MCP and workspace-launch diagnostics. Other Operation readers keep
/// the existing projection and visibility boundary.
pub(super) fn get_operation_for_current_manager(
    db: &Connection,
    p: &Principal,
    id: &str,
) -> Result<Value> {
    let mut operation = get_operation(db, id)?;
    let current_manager = matches!(p.role, Role::Manager | Role::Operator)
        && super::gm::require_authority(db, p).is_ok()
        && super::operation_visible_to(db, p, id)?;
    if current_manager && let Some(action) = owned_service_start_action_for_operation(db, id)? {
        operation["manager_action_required"] = action;
    }
    if current_manager && let Some(action) = owned_service_dispatch_action_for_operation(db, id)? {
        operation["runtime_dispatch_action_required"] = action;
    }
    if current_manager && let Some(action) = module_bridge_recovery_action_for_operation(db, id)? {
        operation["module_recovery_action_required"] = action;
    }
    if current_manager && let Some(readback) = native_mcp_readback_for_operation(db, id)? {
        operation["native_mcp_readback"] = readback;
    }
    if current_manager
        && operation["method"] == "swarm.launch"
        && let Some(readback) = super::launcher_mcp_tools::diagnostic_for_operation(db, id)?
    {
        operation["native_mcp_tools_readback"] = readback;
    }
    if current_manager
        && operation["method"] == "swarm.launch"
        && let Some(readback) = workspace_launch_failure_readback_for_operation(db, id, &operation)?
    {
        operation["workspace_failure_readback"] = readback;
    }
    if current_manager && let Some(issuance) = participant_issuance_failure_for_operation(db, id)? {
        operation["participant_issuance"] = issuance;
    }
    Ok(operation)
}

/// Project only closed failure facts for an unknown workspace effect. The
/// existing operation visibility/current-Manager gate runs before this helper.
fn workspace_launch_failure_readback_for_operation(
    db: &Connection,
    operation_id: &str,
    operation: &Value,
) -> Result<Option<Value>> {
    if operation["state"] != "outcome_unknown"
        || operation["result"]["failure"]["code"] != "workspace_effect_unknown"
    {
        return Ok(None);
    }
    let Some(retained) = meta(db, &format!("launcher:failure:{operation_id}"))? else {
        return Ok(None);
    };
    if serde_json::to_vec(&retained)?.len() > MAX_WORKSPACE_LAUNCH_FAILURE_BYTES {
        return Ok(Some(workspace_launch_failure_diagnostic_corrupt()));
    }

    let latest_code = retained["code"].as_str();
    let latest_classification = retained["classification"].as_str();
    let latest_observed_at_ms = retained["observed_at_ms"].as_i64();
    let first = if retained.get("first_failure").is_some() {
        &retained["first_failure"]
    } else {
        &retained
    };
    let first_code = first["code"].as_str();
    let first_classification = first["classification"].as_str();
    let first_observed_at_ms = first["observed_at_ms"].as_i64();
    let safe_classification = |value: &str| {
        matches!(
            value,
            "workspace_effect_unknown"
                | "binding_effect_unknown"
                | "workspace_stale_before_effect"
                | "workspace_admission_rejected"
        )
    };
    let valid = latest_code.is_some_and(safe_start_failure_error_code)
        && latest_classification.is_some_and(safe_classification)
        && latest_observed_at_ms.is_some_and(|value| value >= 0)
        && first_code.is_some_and(safe_start_failure_error_code)
        && first_classification.is_some_and(safe_classification)
        && first_observed_at_ms
            .is_some_and(|value| value >= 0 && Some(value) <= latest_observed_at_ms)
        && latest_classification == Some("workspace_effect_unknown");
    if !valid {
        return Ok(Some(workspace_launch_failure_diagnostic_corrupt()));
    }

    Ok(Some(json!({
        "schema_version":1,
        "status":"readback_required",
        "first_retained_failure":{
            "code":first_code,
            "classification":first_classification,
            "observed_at_ms":first_observed_at_ms,
        },
        "latest_failure":{
            "code":latest_code,
            "classification":latest_classification,
            "observed_at_ms":latest_observed_at_ms,
        },
    })))
}

fn workspace_launch_failure_diagnostic_corrupt() -> Value {
    json!({
        "schema_version":1,
        "status":"unknown",
        "code":"LAUNCH_FAILURE_DIAGNOSTIC_CORRUPT",
    })
}

/// A verified module-owner departure can leave the original Operation unknown
/// after a new bridge boot. Preserve that Operation and offer its exact
/// adapter-supported readback only to the current Manager or local Operator.
fn module_bridge_recovery_action_for_operation(
    db: &Connection,
    operation_id: &str,
) -> Result<Option<Value>> {
    let operation = get_operation(db, operation_id)?;
    if operation["state"] != "outcome_unknown" {
        return Ok(None);
    }
    let Some(binding_id) = public_token(&operation["binding_id"]) else {
        return Ok(None);
    };
    let Some(generation) = operation["binding_generation"]
        .as_i64()
        .filter(|generation| *generation > 0)
    else {
        return Ok(None);
    };
    let Some(target_method) = operation["method"].as_str() else {
        return Ok(None);
    };
    let binding = get_binding(db, binding_id, generation)?;
    if binding["observation"]["recovery_required"] != true {
        return Ok(None);
    }
    let Some((runtime, artifact_id, readback_boundary)) =
        exact_module_recovery_contract(&binding["route"], target_method)
    else {
        return Ok(None);
    };
    let operation_id = public_token(&operation["operation_id"]);
    let Some(operation_id) = operation_id else {
        return Ok(Some(module_recovery_identity_gap(
            "BRIDGE_RECOVERY_OPERATION_ID_INVALID",
            None,
            binding_id,
            generation,
        )));
    };
    if binding["module_artifact_id"] != binding["route"]["module_artifact_id"] {
        return Ok(Some(module_recovery_conflict(
            "BRIDGE_RECOVERY_ARTIFACT_MISMATCH",
            operation_id,
            binding_id,
            generation,
        )));
    }
    if !binding["released_at_ms"].is_null() {
        return Ok(Some(module_recovery_conflict(
            "BRIDGE_RECOVERY_BINDING_RELEASED",
            operation_id,
            binding_id,
            generation,
        )));
    }
    if binding["state"] != "reconciling" {
        return Ok(Some(module_recovery_conflict(
            "BRIDGE_RECOVERY_BINDING_STATE_MISMATCH",
            operation_id,
            binding_id,
            generation,
        )));
    }

    let previous_boot_id = public_token(&binding["observation"]["previous_bridge_boot_id"]);
    let current_boot_id = public_token(&binding["observation"]["bridge_boot_id"]);
    let (Some(previous_boot_id), Some(current_boot_id)) = (previous_boot_id, current_boot_id)
    else {
        return Ok(Some(module_recovery_identity_gap(
            "BRIDGE_RECOVERY_BOOT_ID_INVALID",
            Some(operation_id),
            binding_id,
            generation,
        )));
    };
    if previous_boot_id == current_boot_id {
        return Ok(Some(module_recovery_identity_gap(
            "BRIDGE_RECOVERY_BOOT_TRANSITION_INVALID",
            Some(operation_id),
            binding_id,
            generation,
        )));
    }

    Ok(Some(json!({
        "schema_version":1,
        "state":"readback_required",
        "operation_id":operation_id,
        "operation_method":target_method,
        "operation_state":"outcome_unknown",
        "binding_id":binding_id,
        "binding_generation":generation,
        "runtime":runtime,
        "module_artifact_id":artifact_id,
        "verified_transition":{
            "previous_owner_departed":true,
            "from_bridge_boot_id":previous_boot_id,
            "to_bridge_boot_id":current_boot_id,
        },
        "cause":"unknown",
        "native_effect":"unknown",
        "retry_authorized":false,
        "next_step":"Read back this exact Operation before deciding whether any new input is appropriate.",
        "readback":{
            "method":"agent.reconcile",
            "supported_on_exact_route":true,
            "binding_id":binding_id,
            "generation":generation,
            "operation_id":operation_id,
            "request_template":{
                "binding_id":binding_id,
                "generation":generation,
                "operation_id":operation_id,
            },
            "fresh_client_request_id_required":true,
            "native_replay":false,
            "unresolved_outcome":"leave_the_original_operation_outcome_unknown_if_readback_cannot_resolve_it",
            "adapter_boundary":readback_boundary,
        },
    })))
}

/// Only advertise a reconciliation target implemented by this exact route.
/// Command is narrower than the persistent native-session adapters.
pub(super) fn exact_module_recovery_contract<'a>(
    route: &'a Value,
    target_method: &str,
) -> Option<(&'static str, &'a str, &'static str)> {
    if crate::runtime::batch::is_command_route(route)
        && crate::runtime::batch::supports(route, "agent.reconcile")
        && matches!(target_method, "agent.open" | "task.dispatch")
    {
        return Some((
            crate::runtime::batch::COMMAND_RUNTIME,
            route["module_artifact_id"].as_str()?,
            "saved command run artifacts; missing or inconsistent evidence remains unknown",
        ));
    }
    if crate::runtime::codex::is_controller_route(route)
        && matches!(target_method, "agent.open" | "task.dispatch" | "agent.send")
    {
        return Some((
            crate::runtime::codex::RUNTIME,
            crate::runtime::codex::ARTIFACT_ID,
            "saved controller checkpoint and exact native-history readback; gaps remain unknown",
        ));
    }
    if crate::runtime::warm_stream::is_route(route)
        && matches!(target_method, "agent.open" | "task.dispatch" | "agent.send")
    {
        return Some((
            crate::runtime::warm_stream::RUNTIME,
            crate::runtime::warm_stream::ARTIFACT_ID,
            "current bridge operation journal; a missing prior-boot entry remains unknown",
        ));
    }
    None
}

fn module_recovery_conflict(
    code: &str,
    operation_id: &str,
    binding_id: &str,
    generation: i64,
) -> Value {
    json!({
        "schema_version":1,
        "state":"blocked",
        "code":code,
        "operation_id":operation_id,
        "operation_state":"outcome_unknown",
        "binding_id":binding_id,
        "binding_generation":generation,
        "native_effect":"unknown",
        "retry_authorized":false,
        "next_step":"Resolve the retained binding conflict; do not resend the original input.",
    })
}

fn module_recovery_identity_gap(
    code: &str,
    operation_id: Option<&str>,
    binding_id: &str,
    generation: i64,
) -> Value {
    json!({
        "schema_version":1,
        "state":"blocked",
        "code":code,
        "operation_id":operation_id,
        "binding_id":binding_id,
        "binding_generation":generation,
        "operation_state":"outcome_unknown",
        "verified_transition":false,
        "cause":"unknown",
        "native_effect":"unknown",
        "retry_authorized":false,
        "next_step":"Inspect the original Operation and retained binding state; do not resend the input.",
    })
}

fn participant_issuance_failure_for_operation(db: &Connection, id: &str) -> Result<Option<Value>> {
    let retained: Option<(Option<String>, String)> = db
        .query_row(
            "SELECT json_type(effective_request_json,'$.launch_manifest.participant_issuance_latest_failure'),
                    json_quote(json_extract(effective_request_json,'$.launch_manifest.participant_issuance_latest_failure'))
             FROM operations WHERE operation_id=?1 AND method='swarm.launch'",
            [id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((Some(kind), raw)) = retained else {
        return Ok(None);
    };
    let failure = if kind == "object" && raw.len() <= 1024 {
        serde_json::from_str::<Value>(&raw).ok()
    } else {
        None
    };
    let valid = failure.as_ref().is_some_and(|failure| {
        failure["schema_version"].as_u64() == Some(1)
            && failure["code"]
                .as_str()
                .is_some_and(safe_start_failure_error_code)
            && failure["stage"].as_str().is_some_and(|stage| {
                matches!(
                    stage,
                    "participant_issuance_prepare"
                        | "participant_credential_issue"
                        | "participant_issuance_commit"
                )
            })
            && failure["recorded_at_ms"]
                .as_i64()
                .is_some_and(|time| time >= 0)
            && failure["category"].as_str().is_some_and(|category| {
                matches!(
                    category,
                    "scoped_artifact_unavailable"
                        | "participant_registration_rejected"
                        | "participant_issuance_incomplete"
                )
            })
    });
    let latest_failure = match failure {
        Some(failure) if valid => json!({
            "schema_version":1,
            "code":failure["code"],
            "stage":failure["stage"],
            "recorded_at_ms":failure["recorded_at_ms"],
            "category":failure["category"],
        }),
        _ => json!({"schema_version":1,"code":"PARTICIPANT_ISSUANCE_DIAGNOSTIC_CORRUPT"}),
    };
    Ok(Some(
        json!({"schema_version":1,"latest_failure":latest_failure}),
    ))
}

fn native_mcp_readback_for_operation(db: &Connection, id: &str) -> Result<Option<Value>> {
    let raw: Option<String> = db
        .query_row(
            "SELECT json_object(
                'marker',json_extract(effective_request_json,'$.launch_manifest.native_mcp_readback'),
                'has_marker',json_type(effective_request_json,'$.launch_manifest.native_mcp_readback') IS NOT NULL,
                'latest_failure',json_extract(effective_request_json,'$.launch_manifest.native_mcp_latest_failure'),
                'has_failure',json_type(effective_request_json,'$.launch_manifest.native_mcp_latest_failure') IS NOT NULL
             ) FROM operations WHERE operation_id=?1 AND method='swarm.launch'",
            [id],
            |row| row.get(0),
        )
        .optional()?;
    let Some(raw) = raw else { return Ok(None) };
    // A damaged optional diagnostic must not hide the retained Operation.
    if raw.len() > 4096 {
        return Ok(Some(native_mcp_diagnostic_corrupt()));
    }
    let retained: Value = match serde_json::from_str(&raw) {
        Ok(value) => value,
        _ => return Ok(Some(native_mcp_diagnostic_corrupt())),
    };
    let has_marker = retained["has_marker"].as_i64() == Some(1);
    let has_failure = retained["has_failure"].as_i64() == Some(1);
    if !has_marker && !has_failure {
        return Ok(None);
    }
    let mut public = json!({"schema_version":1});
    if has_marker {
        let marker = &retained["marker"];
        match marker["state"].as_str() {
            Some(state @ ("reading" | "retry_wait" | "observed_partial")) => {
                public["state"] = json!(state);
            }
            _ => public = native_mcp_diagnostic_corrupt(),
        }
        for key in ["attempts", "next_retry_at_ms"] {
            if let Some(value) = marker[key].as_i64().filter(|value| *value >= 0) {
                public[key] = json!(value);
            }
        }
        if let Some(category) = marker["last_failure_category"]
            .as_str()
            .filter(|category| safe_native_mcp_failure_category(category))
        {
            public["last_failure_category"] = json!(category);
        }
    }
    if has_failure {
        let failure = &retained["latest_failure"];
        let valid = failure["schema_version"].as_u64() == Some(1)
            && failure["code"]
                .as_str()
                .is_some_and(safe_start_failure_error_code)
            && failure["stage"].as_str().is_some_and(|stage| {
                matches!(
                    stage,
                    "launch_snapshot_validate" | "native_capability_readback"
                )
            })
            && failure["recorded_at_ms"]
                .as_i64()
                .is_some_and(|time| time >= 0)
            && failure["category"]
                .as_str()
                .is_some_and(safe_native_mcp_failure_category);
        public["latest_failure"] = if valid {
            json!({
                "schema_version":1,
                "code":failure["code"],
                "stage":failure["stage"],
                "recorded_at_ms":failure["recorded_at_ms"],
                "category":failure["category"],
            })
        } else {
            json!({"schema_version":1,"code":"NATIVE_MCP_DIAGNOSTIC_CORRUPT"})
        };
    }
    Ok(Some(public))
}

fn safe_native_mcp_failure_category(category: &str) -> bool {
    matches!(
        category,
        "native_service_unavailable"
            | "scoped_artifact_or_credential_unavailable"
            | "assignment_scope_unavailable"
            | "native_readback_incomplete"
    )
}

fn native_mcp_diagnostic_corrupt() -> Value {
    json!({"schema_version":1,"state":"unknown","code":"NATIVE_MCP_DIAGNOSTIC_CORRUPT"})
}

/// Retain only the safe code and closed stage for a failure selecting an
/// already-observed owned service's exact queued `agent.open`. The INSERT is
/// one statement so the binding, start proof, launch manifest and Operation
/// tuple are checked atomically with the durable observation.
pub(super) fn record_owned_open_dispatch_failure(
    db: &Connection,
    binding_id: &str,
    generation: i64,
    stage: &str,
    status: &str,
    error_code: &str,
    expected_open_state: &str,
) -> Result<bool> {
    let valid_case = matches!(
        (stage, status, expected_open_state),
        ("runtime_command_select", "selection_error", "queued")
            | (
                "opening_actor_validate",
                "rejected_before_dispatch",
                "rejected"
            )
    );
    if !valid_case || !safe_start_failure_error_code(error_code) {
        return Ok(false);
    }
    let payload = model::canonical(&json!({
        "schema_version":1,
        "status":status,
        "stage":stage,
        "error_code":error_code,
        "native_effect":"not_dispatched",
        "retry_authorized":false,
    }))?;
    if payload.len() > MAX_OWNED_SERVICE_DISPATCH_FAILURE_BYTES {
        return Ok(false);
    }
    let now = model::now_ms()?;
    let inserted = db.execute(
        "INSERT OR IGNORE INTO observations(
             source_stream_id,source_event_key,binding_id,binding_generation,
             operation_id,kind,payload_json,recorded_at_ms
         )
         SELECT 'controller:owned-service',
                'owned-service-dispatch-failure:' || start.launch_operation_id,
                start.binding_id,start.binding_generation,start.launch_operation_id,
                'owned_service.dispatch_failure',?4,?5
         FROM owned_service_starts AS start
         JOIN operations AS launch ON launch.operation_id=start.launch_operation_id
         JOIN operations AS opened ON opened.operation_id=start.open_operation_id
         JOIN bindings AS binding
           ON binding.binding_id=start.binding_id
          AND binding.generation=start.binding_generation
         WHERE start.binding_id=?1 AND start.binding_generation=?2
           AND start.state IN ('service_observed','service_departed')
           AND start.process_id IS NOT NULL
           AND start.process_birth_token IS NOT NULL
           AND length(start.process_birth_token)>0
           AND start.executable_sha256 IS NOT NULL
           AND length(start.executable_sha256)=64
           AND start.proof_json<>'{}' AND length(start.proof_json)<=8192
           AND launch.method='swarm.launch'
           AND opened.method='agent.open'
           AND opened.state=?3
           AND opened.due_at_ms<=?5
           AND opened.sent_at_ms IS NULL
           AND opened.binding_id=start.binding_id
           AND opened.binding_generation=start.binding_generation
           AND opened.prerequisite_operation_id=launch.operation_id
           AND opened.caller_id=launch.caller_id
           AND opened.task_id=start.task_id AND launch.task_id=start.task_id
           AND opened.attempt_id=start.attempt_id AND launch.attempt_id=start.attempt_id
           AND json_extract(launch.effective_request_json,'$.launch_manifest.binding.operation_id')=opened.operation_id
           AND json_extract(launch.effective_request_json,'$.launch_manifest.binding.binding_id')=start.binding_id
           AND json_extract(launch.effective_request_json,'$.launch_manifest.binding.generation')=start.binding_generation
           AND json_extract(launch.effective_request_json,'$.launch_manifest.task.task_id')=start.task_id
           AND json_extract(launch.effective_request_json,'$.launch_manifest.task.attempt_id')=start.attempt_id
           AND binding.released_at_ms IS NULL
           AND binding.state='opening'
           AND binding.native_root_id IS NULL
           AND binding.native_scope_key IS NULL
           AND json_extract(binding.route_json,'$.runtime')='opencode_v2'
           AND json_type(binding.route_json,'$.owned_service')='object'
           AND json_extract((SELECT value_json FROM meta WHERE key='execution_mode'),'$.new_work')='enabled'
           AND (?3<>'rejected' OR json_extract(opened.result_json,'$.code')=?6)
           AND (?3<>'queued' OR (
               COALESCE(json_extract(binding.state_json,'$.recovery_required'),0)=0
               AND (SELECT count(*) FROM operations AS candidate
                    WHERE candidate.binding_id=start.binding_id
                      AND candidate.binding_generation=start.binding_generation
                      AND candidate.state='queued' AND candidate.due_at_ms<=?5
                      AND (COALESCE(json_extract(binding.state_json,'$.recovery_required'),0)=0
                           OR candidate.method IN ('agent.recover','agent.reconcile'))
                      AND candidate.method IN ('agent.open','task.dispatch','agent.send','agent.reply','agent.configure','agent.goal','agent.background','agent.refresh','agent.reconcile','agent.result','agent.recover')
                      AND (candidate.method IN ('agent.reply','agent.background','agent.refresh','agent.reconcile','agent.result','agent.recover')
                           OR (candidate.method='agent.send' AND json_extract(candidate.original_request_json,'$.delivery')='steer')
                           OR (candidate.method='agent.goal' AND json_extract(candidate.original_request_json,'$.action') IN ('pause','clear'))
                           OR NOT EXISTS (
                               SELECT 1 FROM operations AS pending
                               WHERE pending.binding_id=candidate.binding_id
                                 AND pending.binding_generation=candidate.binding_generation
                                 AND pending.state IN ('sending','native_accepted','outcome_unknown')
                                 AND pending.method IN ('agent.open','task.dispatch','agent.send','agent.configure','agent.goal','agent.recover')
                           )))=1
           ))",
        params![binding_id, generation, expected_open_state, payload, now, error_code],
    )?;
    Ok(inserted == 1)
}

/// A startup diagnostic or interrupted unknown reservation remains an unknown
/// native effect until exact readback. Keep the projection bound to this
/// launch/open pair and never return raw observation payloads or process errors.
fn owned_service_start_action_for_operation(
    db: &Connection,
    operation_id: &str,
) -> Result<Option<Value>> {
    match owned_service_start_action_for_operation_inner(db, operation_id) {
        Err(error)
            if matches!(
                error.code.as_str(),
                "OWNED_SERVICE_LINK_CORRUPT" | "OWNED_SERVICE_START_DIAGNOSTIC_CORRUPT"
            ) =>
        {
            Ok(Some(owned_service_start_diagnostic_gap(
                operation_id,
                &error.code,
            )))
        }
        result => result,
    }
}

#[derive(Clone, Copy)]
struct InterruptedStartCut {
    host_epoch: i64,
    observed_at_ms: i64,
    current_started_at_ms: i64,
}

impl InterruptedStartCut {
    fn includes(self, start_updated_at_ms: i64) -> bool {
        start_updated_at_ms >= 0
            && start_updated_at_ms < self.observed_at_ms
            && start_updated_at_ms < self.current_started_at_ms
    }
}

/// Return only a validated restart cut. The lifecycle receipt identifies the
/// interrupted host, but owned-service rows do not store their host epoch, so
/// this proves only that the exact reservation was unresolved before the
/// interruption cut; it does not attribute the original start to that epoch.
fn interrupted_start_cut(db: &Connection) -> Result<Option<InterruptedStartCut>> {
    let lifecycle = super::host_lifecycle::status(db)?;
    let current = &lifecycle["current"];
    let failure = &lifecycle["latest_failure"];
    if current["state"] != "running"
        || failure["schema_version"] != 1
        || failure["error_code"] != "HOST_INTERRUPTED"
        || failure["manager_action_required"] != true
        || failure["retry_authorized"] != false
    {
        return Ok(None);
    }

    let Some(host_epoch) = failure["host_epoch"].as_i64().filter(|epoch| *epoch > 0) else {
        return Ok(None);
    };
    let Some(observed_at_ms) = failure["observed_at_ms"].as_i64().filter(|time| *time > 0) else {
        return Ok(None);
    };
    let Some(current_host_epoch) = current["host_epoch"].as_i64().filter(|epoch| *epoch > 0) else {
        return Ok(None);
    };
    let Some(current_started_at_ms) = current["started_at_ms"].as_i64().filter(|time| *time > 0)
    else {
        return Ok(None);
    };
    if current_host_epoch <= host_epoch || current_started_at_ms < observed_at_ms {
        return Ok(None);
    }

    Ok(Some(InterruptedStartCut {
        host_epoch,
        observed_at_ms,
        current_started_at_ms,
    }))
}

fn interrupted_owned_service_start_action(
    launch_operation_id: &str,
    open_operation_id: &str,
    binding_id: &str,
    binding_generation: i64,
    task_id: &str,
    attempt_id: &str,
    cut: InterruptedStartCut,
) -> Value {
    json!({
        "status":"required",
        "kind":"owned_service_start_readback_required",
        "schema_version":1,
        "manager_actionable":true,
        "launch_operation_id":launch_operation_id,
        "open_operation_id":open_operation_id,
        "binding_id":binding_id,
        "binding_generation":binding_generation,
        "task_id":task_id,
        "attempt_id":attempt_id,
        "start_state":"outcome_unknown",
        "native_effect":"unknown",
        "host_interruption":{
            "source":"host_lifecycle.latest_failure",
            "host_epoch":cut.host_epoch,
            "observed_at_ms":cut.observed_at_ms,
        },
        "retry_authorized":false,
        "next_readback":{
            "method":"operation.get",
            "params":{"operation_id":launch_operation_id},
        },
        "actions":[
            "Read the exact linked launch and owned-service state before any retry.",
            "The reservation was still unresolved at a host interruption; do not infer whether the helper started.",
        ],
    })
}

fn owned_service_start_action_for_operation_inner(
    db: &Connection,
    operation_id: &str,
) -> Result<Option<Value>> {
    let mut statement = db.prepare(
        "SELECT launch_operation_id,open_operation_id,binding_id,binding_generation,
                task_id,attempt_id,state,process_id,process_birth_token,executable_sha256,proof_json,
                updated_at_ms
         FROM owned_service_starts
         WHERE (launch_operation_id=?1 OR open_operation_id=?1)
         ORDER BY launch_operation_id LIMIT 2",
    )?;
    let rows = statement
        .query_map([operation_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, Option<i64>>(7)?,
                row.get::<_, Option<String>>(8)?,
                row.get::<_, Option<String>>(9)?,
                row.get::<_, String>(10)?,
                row.get::<_, i64>(11)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if rows.len() > 1 {
        return Err(owned_service_link_corrupt());
    }
    let Some((
        launch_operation_id,
        open_operation_id,
        binding_id,
        binding_generation,
        task_id,
        attempt_id,
        state,
        process_id,
        process_birth_token,
        executable_sha256,
        proof_json,
        start_updated_at_ms,
    )) = rows.into_iter().next()
    else {
        return Ok(None);
    };
    if state != "outcome_unknown" {
        return Ok(None);
    }
    if process_id.is_some()
        || process_birth_token.is_some()
        || executable_sha256.is_some()
        || proof_json != "{}"
        || binding_generation <= 0
        || start_updated_at_ms < 0
        || [
            launch_operation_id.as_str(),
            open_operation_id.as_str(),
            binding_id.as_str(),
            task_id.as_str(),
            attempt_id.as_str(),
        ]
        .iter()
        .any(|value| !safe_store_identifier(value))
    {
        return Err(owned_service_start_diagnostic_corrupt());
    }
    let operation_link_valid: bool = db.query_row(
        "SELECT EXISTS(
             SELECT 1
             FROM operations AS launch
             JOIN operations AS opened ON opened.operation_id=?2
             WHERE launch.operation_id=?1
               AND launch.method='swarm.launch'
               AND launch.state IN ('queued','outcome_unknown')
               AND opened.method='agent.open'
               AND opened.state IN ('queued','outcome_unknown')
               AND launch.caller_id=opened.caller_id
               AND launch.task_id=?3
               AND launch.attempt_id=?4
               AND opened.prerequisite_operation_id=launch.operation_id
               AND opened.task_id=?3
               AND opened.attempt_id=?4
               AND opened.binding_id=?5
               AND opened.binding_generation=?6
               AND json_extract(launch.effective_request_json,'$.launch_manifest.binding.operation_id')=opened.operation_id
               AND json_extract(launch.effective_request_json,'$.launch_manifest.binding.binding_id')=?5
               AND json_extract(launch.effective_request_json,'$.launch_manifest.binding.generation')=?6
         )",
        params![
            launch_operation_id,
            open_operation_id,
            task_id,
            attempt_id,
            binding_id,
            binding_generation
        ],
        |row| row.get(0),
    )?;
    if !operation_link_valid {
        return Err(owned_service_link_corrupt());
    }

    let event_key = format!("owned-service-start-failure:{launch_operation_id}");
    let mut statement = db.prepare(
        "SELECT observation_id,source_stream_id,source_event_key,operation_id,binding_id,
                binding_generation,kind,substr(CAST(payload_json AS BLOB),1,?1)
         FROM observations
         WHERE source_event_key=?2
            OR (operation_id=?3 AND kind='owned_service.start_failure')
         ORDER BY observation_id LIMIT 2",
    )?;
    let rows = statement
        .query_map(
            params![
                (MAX_OWNED_SERVICE_START_FAILURE_BYTES + 1) as i64,
                event_key,
                launch_operation_id
            ],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, Vec<u8>>(7)?,
                ))
            },
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if rows.len() > 1 {
        return Err(owned_service_start_diagnostic_corrupt());
    }
    let Some((
        observation_id,
        source_stream_id,
        source_event_key,
        diagnostic_operation_id,
        diagnostic_binding_id,
        diagnostic_binding_generation,
        kind,
        payload_bytes,
    )) = rows.into_iter().next()
    else {
        let Some(cut) = interrupted_start_cut(db)? else {
            return Ok(None);
        };
        if !cut.includes(start_updated_at_ms) {
            return Ok(None);
        }
        return Ok(Some(interrupted_owned_service_start_action(
            &launch_operation_id,
            &open_operation_id,
            &binding_id,
            binding_generation,
            &task_id,
            &attempt_id,
            cut,
        )));
    };
    if observation_id <= 0
        || source_stream_id != "controller:owned-service"
        || source_event_key != event_key
        || diagnostic_operation_id != launch_operation_id
        || diagnostic_binding_id != binding_id
        || diagnostic_binding_generation != binding_generation
        || kind != "owned_service.start_failure"
        || payload_bytes.len() > MAX_OWNED_SERVICE_START_FAILURE_BYTES
    {
        return Err(owned_service_start_diagnostic_corrupt());
    }
    let payload_text = std::str::from_utf8(&payload_bytes)
        .map_err(|_| owned_service_start_diagnostic_corrupt())?;
    let payload: Value =
        serde_json::from_str(payload_text).map_err(|_| owned_service_start_diagnostic_corrupt())?;
    let diagnostic = validate_owned_service_start_failure(&payload)?;

    Ok(Some(json!({
        "status":"required",
        "kind":"owned_service_start_failure",
        "manager_actionable":true,
        "launch_operation_id":launch_operation_id,
        "open_operation_id":open_operation_id,
        "binding_id":binding_id,
        "binding_generation":binding_generation,
        "task_id":task_id,
        "attempt_id":attempt_id,
        "schema_version":diagnostic["schema_version"],
        "failure_status":diagnostic["status"],
        "stage":diagnostic["stage"],
        "error_code":diagnostic["error_code"],
        "native_effect":"unknown",
        "request_phase":diagnostic["request_phase"],
        "http_status":diagnostic["http_status"],
        "source_observation":{
            "observation_id":observation_id,
            "source_stream_id":"controller:owned-service",
            "kind":"owned_service.start_failure",
        },
        "retry_authorized":false,
        "next_readback":{
            "method":"operation.get",
            "params":{"operation_id":launch_operation_id},
        },
        "actions":[
            "Read the exact linked launch and owned-service state before any retry.",
            "Keep the native effect unknown until process or no-effect readback resolves it.",
        ],
    })))
}

fn validate_owned_service_start_failure(payload: &Value) -> Result<Value> {
    let object = payload
        .as_object()
        .ok_or_else(owned_service_start_diagnostic_corrupt)?;
    let schema_version = payload["schema_version"]
        .as_i64()
        .ok_or_else(owned_service_start_diagnostic_corrupt)?;
    let expected_keys: &[&str] = match schema_version {
        1 => &START_FAILURE_V1_KEYS,
        2 => &START_FAILURE_V2_KEYS,
        _ => return Err(owned_service_start_diagnostic_corrupt()),
    };
    if object.len() != expected_keys.len()
        || expected_keys.iter().any(|key| !object.contains_key(*key))
        || payload["status"] != "startup_failed_unknown"
        || payload["native_effect"] != "unknown"
        || !safe_start_failure_stage(payload["stage"].as_str().unwrap_or_default())
        || !safe_start_failure_error_code(payload["error_code"].as_str().unwrap_or_default())
    {
        return Err(owned_service_start_diagnostic_corrupt());
    }
    let (request_phase, http_status) = if schema_version == 2 {
        let request_phase = if payload["request_phase"].is_null() {
            None
        } else {
            Some(
                payload["request_phase"]
                    .as_str()
                    .filter(|phase| *phase == "provider_key_post")
                    .ok_or_else(owned_service_start_diagnostic_corrupt)?,
            )
        };
        let http_status = if payload["http_status"].is_null() {
            None
        } else {
            Some(
                payload["http_status"]
                    .as_i64()
                    .filter(|status| safe_start_failure_http_status(*status))
                    .ok_or_else(owned_service_start_diagnostic_corrupt)?,
            )
        };
        let provider_post_failure =
            payload["stage"] == "bootstrap" && payload["error_code"] == "NATIVE_REJECTED";
        if (request_phase.is_none() && http_status.is_some())
            || (provider_post_failure && request_phase != Some("provider_key_post"))
            || (!provider_post_failure && request_phase.is_some())
        {
            return Err(owned_service_start_diagnostic_corrupt());
        }
        (request_phase, http_status)
    } else {
        (None, None)
    };
    Ok(json!({
        "schema_version":schema_version,
        "status":"startup_failed_unknown",
        "stage":payload["stage"],
        "error_code":payload["error_code"],
        "request_phase":request_phase,
        "http_status":http_status,
    }))
}

fn safe_store_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

fn safe_start_failure_stage(value: &str) -> bool {
    matches!(
        value,
        "permit_validation"
            | "pre_spawn"
            | "helper_spawn"
            | "helper_input"
            | "ready_receipt"
            | "from_route"
            | "connect_owned"
            | "route_verify"
            | "provider_scope"
            | "bootstrap"
            | "provider_proof"
    )
}

fn safe_start_failure_error_code(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.as_bytes().first().is_some_and(u8::is_ascii_uppercase)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}

fn safe_start_failure_http_status(value: i64) -> bool {
    matches!(value, 400 | 401 | 403 | 404 | 405 | 409 | 413 | 422)
}

fn owned_service_start_diagnostic_corrupt() -> Error {
    Error::new(
        "OWNED_SERVICE_START_DIAGNOSTIC_CORRUPT",
        "owned service startup failure diagnostic is invalid",
    )
}

fn owned_service_start_diagnostic_gap(operation_id: &str, error_code: &str) -> Value {
    json!({
        "status":"readback_required",
        "kind":"owned_service_start_diagnostic_gap",
        "manager_actionable":true,
        "source_operation_id":if safe_store_identifier(operation_id) {
            Value::String(operation_id.to_owned())
        } else {
            Value::Null
        },
        "error_code":error_code,
        "native_effect":"unknown",
        "retry_authorized":false,
        "next_readback":if safe_store_identifier(operation_id) {
            json!({"method":"operation.get","params":{"operation_id":operation_id}})
        } else {
            Value::Null
        },
        "actions":[
            "Have the local Operator inspect the exact owned-service record before any retry.",
            "Keep the native effect unknown until process or no-effect readback resolves it.",
        ],
    })
}

/// Current-manager readback for an exact owned-service open that failed before
/// `agent.open` crossed the durable dispatch boundary. The original Operation
/// receipt is never rewritten by this diagnostic.
fn owned_service_dispatch_action_for_operation(
    db: &Connection,
    operation_id: &str,
) -> Result<Option<Value>> {
    match owned_service_dispatch_action_for_operation_inner(db, operation_id) {
        Err(error)
            if matches!(
                error.code.as_str(),
                "OWNED_SERVICE_LINK_CORRUPT" | "OWNED_SERVICE_DISPATCH_DIAGNOSTIC_CORRUPT"
            ) =>
        {
            Ok(Some(owned_service_dispatch_diagnostic_gap(
                operation_id,
                &error.code,
            )))
        }
        result => result,
    }
}

fn owned_service_dispatch_action_for_operation_inner(
    db: &Connection,
    operation_id: &str,
) -> Result<Option<Value>> {
    let mut statement = db.prepare(
        "SELECT launch_operation_id,open_operation_id,binding_id,binding_generation,
                task_id,attempt_id,state,process_id,process_birth_token,executable_sha256,
                length(proof_json)
         FROM owned_service_starts
         WHERE (launch_operation_id=?1 OR open_operation_id=?1)
           AND EXISTS(
               SELECT 1 FROM observations AS diagnostic
               WHERE diagnostic.source_event_key='owned-service-dispatch-failure:' || launch_operation_id
                  OR (diagnostic.operation_id=launch_operation_id
                      AND diagnostic.kind='owned_service.dispatch_failure')
           )
         ORDER BY launch_operation_id LIMIT 2",
    )?;
    let rows = statement
        .query_map([operation_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, Option<i64>>(7)?,
                row.get::<_, Option<String>>(8)?,
                row.get::<_, Option<String>>(9)?,
                row.get::<_, i64>(10)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if rows.len() > 1 {
        return Err(owned_service_link_corrupt());
    }
    let Some((
        launch_operation_id,
        open_operation_id,
        binding_id,
        binding_generation,
        task_id,
        attempt_id,
        start_state,
        process_id,
        process_birth_token,
        executable_sha256,
        proof_bytes,
    )) = rows.into_iter().next()
    else {
        return Ok(None);
    };
    if !matches!(
        start_state.as_str(),
        "service_observed" | "service_departed"
    ) || process_id.is_none_or(|pid| pid <= 0)
        || process_birth_token
            .as_deref()
            .is_none_or(|token| token.is_empty() || token.len() > 256)
        || executable_sha256
            .as_deref()
            .is_none_or(|digest| !safe_sha256(digest))
        || !(3..=8192).contains(&proof_bytes)
        || binding_generation <= 0
        || [
            launch_operation_id.as_str(),
            open_operation_id.as_str(),
            binding_id.as_str(),
            task_id.as_str(),
            attempt_id.as_str(),
        ]
        .iter()
        .any(|value| !safe_store_identifier(value))
    {
        return Err(owned_service_dispatch_diagnostic_corrupt());
    }

    let link_valid: bool = db.query_row(
        "SELECT EXISTS(
             SELECT 1
             FROM operations AS launch
             JOIN operations AS opened ON opened.operation_id=?2
             JOIN bindings AS binding
               ON binding.binding_id=?5 AND binding.generation=?6
             WHERE launch.operation_id=?1
               AND launch.method='swarm.launch'
               AND opened.method='agent.open'
               AND opened.state IN ('queued','rejected','sending','native_accepted','outcome_unknown','settled','cancelled')
               AND launch.task_id=?3 AND launch.attempt_id=?4
               AND opened.task_id=?3 AND opened.attempt_id=?4
               AND opened.caller_id=launch.caller_id
               AND opened.binding_id=?5 AND opened.binding_generation=?6
               AND opened.prerequisite_operation_id=launch.operation_id
               AND json_extract(launch.effective_request_json,'$.launch_manifest.binding.operation_id')=opened.operation_id
               AND json_extract(launch.effective_request_json,'$.launch_manifest.binding.binding_id')=?5
               AND json_extract(launch.effective_request_json,'$.launch_manifest.binding.generation')=?6
               AND json_extract(launch.effective_request_json,'$.launch_manifest.task.task_id')=?3
               AND json_extract(launch.effective_request_json,'$.launch_manifest.task.attempt_id')=?4
         )",
        params![
            launch_operation_id,
            open_operation_id,
            task_id,
            attempt_id,
            binding_id,
            binding_generation
        ],
        |row| row.get(0),
    )?;
    if !link_valid {
        return Err(owned_service_link_corrupt());
    }

    let event_key = format!("owned-service-dispatch-failure:{launch_operation_id}");
    let mut statement = db.prepare(
        "SELECT observation_id,source_stream_id,source_event_key,operation_id,binding_id,
                binding_generation,kind,
                substr(CAST(payload_json AS BLOB),1,?1)
         FROM observations
         WHERE source_event_key=?2
            OR (operation_id=?3 AND kind='owned_service.dispatch_failure')
         ORDER BY observation_id LIMIT 2",
    )?;
    let rows = statement
        .query_map(
            params![
                (MAX_OWNED_SERVICE_DISPATCH_FAILURE_BYTES + 1) as i64,
                event_key,
                launch_operation_id
            ],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, Vec<u8>>(7)?,
                ))
            },
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if rows.len() > 1 {
        return Err(owned_service_dispatch_diagnostic_corrupt());
    }
    let Some((
        observation_id,
        source_stream_id,
        source_event_key,
        diagnostic_operation_id,
        diagnostic_binding_id,
        diagnostic_binding_generation,
        kind,
        payload_bytes,
    )) = rows.into_iter().next()
    else {
        return Ok(None);
    };
    if observation_id <= 0
        || source_stream_id != "controller:owned-service"
        || source_event_key != event_key
        || diagnostic_operation_id != launch_operation_id
        || diagnostic_binding_id != binding_id
        || diagnostic_binding_generation != binding_generation
        || kind != "owned_service.dispatch_failure"
        || payload_bytes.len() > MAX_OWNED_SERVICE_DISPATCH_FAILURE_BYTES
    {
        return Err(owned_service_dispatch_diagnostic_corrupt());
    }
    let payload_text = std::str::from_utf8(&payload_bytes)
        .map_err(|_| owned_service_dispatch_diagnostic_corrupt())?;
    let payload: Value = serde_json::from_str(payload_text)
        .map_err(|_| owned_service_dispatch_diagnostic_corrupt())?;
    let diagnostic = validate_owned_service_dispatch_failure(&payload)?;

    let (current_state, current_error_code, sent_at_ms): (String, Option<String>, Option<i64>) = db
        .query_row(
            "SELECT state,json_extract(result_json,'$.code'),sent_at_ms
         FROM operations WHERE operation_id=?1 AND method='agent.open'",
            [&open_operation_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
    if matches!(current_state.as_str(), "settled" | "cancelled") {
        return Ok(None);
    }
    let rejected_before_dispatch = current_state == "rejected"
        && diagnostic["status"] == "rejected_before_dispatch"
        && current_error_code.as_deref() == diagnostic["error_code"].as_str();
    let native_effect =
        if sent_at_ms.is_none() && (current_state == "queued" || rejected_before_dispatch) {
            "not_dispatched"
        } else if matches!(
            current_state.as_str(),
            "queued" | "rejected" | "sending" | "native_accepted" | "outcome_unknown"
        ) {
            "unknown"
        } else {
            return Err(owned_service_link_corrupt());
        };

    Ok(Some(json!({
        "status":"required",
        "kind":"owned_service_dispatch_failure",
        "manager_actionable":true,
        "launch_operation_id":launch_operation_id,
        "open_operation_id":open_operation_id,
        "binding_id":binding_id,
        "binding_generation":binding_generation,
        "task_id":task_id,
        "attempt_id":attempt_id,
        "failure_status":diagnostic["status"],
        "stage":diagnostic["stage"],
        "error_code":diagnostic["error_code"],
        "dispatch_state":current_state,
        "native_effect":native_effect,
        "retry_authorized":false,
        "source_observation":{
            "observation_id":observation_id,
            "source_stream_id":"controller:owned-service",
            "kind":"owned_service.dispatch_failure",
        },
        "next_readback":{
            "method":"operation.get",
            "params":{"operation_id":launch_operation_id},
        },
        "actions":[
            "Read the exact linked launch, open Operation, and binding state before any retry.",
            "Do not retry while dispatch state is sending or its native effect remains unknown.",
        ],
    })))
}

fn validate_owned_service_dispatch_failure(payload: &Value) -> Result<Value> {
    let object = payload
        .as_object()
        .ok_or_else(owned_service_dispatch_diagnostic_corrupt)?;
    if object.len() != DISPATCH_FAILURE_KEYS.len()
        || DISPATCH_FAILURE_KEYS
            .iter()
            .any(|key| !object.contains_key(*key))
        || payload["schema_version"].as_i64() != Some(1)
        || payload["native_effect"] != "not_dispatched"
        || payload["retry_authorized"] != false
    {
        return Err(owned_service_dispatch_diagnostic_corrupt());
    }
    let status = payload["status"].as_str().unwrap_or_default();
    let stage = payload["stage"].as_str().unwrap_or_default();
    let valid_pair = matches!(
        (status, stage),
        ("selection_error", "runtime_command_select")
            | ("rejected_before_dispatch", "opening_actor_validate")
    );
    let error_code = payload["error_code"].as_str().unwrap_or_default();
    if !valid_pair || !safe_start_failure_error_code(error_code) {
        return Err(owned_service_dispatch_diagnostic_corrupt());
    }
    Ok(json!({
        "status":status,
        "stage":stage,
        "error_code":error_code,
    }))
}

fn safe_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn owned_service_dispatch_diagnostic_corrupt() -> Error {
    Error::new(
        "OWNED_SERVICE_DISPATCH_DIAGNOSTIC_CORRUPT",
        "owned service dispatch diagnostic is invalid",
    )
}

fn owned_service_dispatch_diagnostic_gap(operation_id: &str, error_code: &str) -> Value {
    json!({
        "status":"readback_required",
        "kind":"owned_service_dispatch_diagnostic_gap",
        "manager_actionable":true,
        "source_operation_id":if safe_store_identifier(operation_id) {
            Value::String(operation_id.to_owned())
        } else {
            Value::Null
        },
        "error_code":error_code,
        "native_effect":"unknown",
        "retry_authorized":false,
        "next_readback":if safe_store_identifier(operation_id) {
            json!({"method":"operation.get","params":{"operation_id":operation_id}})
        } else {
            Value::Null
        },
        "actions":[
            "Have the local Operator inspect the exact owned-service record before any retry.",
            "Keep the native effect unknown until exact Operation and process readback resolves it.",
        ],
    })
}

/// Bounded current-manager attention feed for linked owned-service dispatch
/// failures. It remains available after the parent launch settles or the
/// observed helper process departs, while a queued/rejected/unknown open still
/// needs readback.
pub(super) fn owned_service_dispatch_failure_actions(db: &Connection) -> Result<Value> {
    let total_items: i64 = db.query_row(
        "SELECT count(*)
         FROM owned_service_starts AS start
         JOIN operations AS opened ON opened.operation_id=start.open_operation_id
         WHERE opened.state IN ('queued','rejected','sending','native_accepted','outcome_unknown')
           AND EXISTS(
             SELECT 1 FROM observations AS diagnostic
             WHERE diagnostic.source_event_key='owned-service-dispatch-failure:' || start.launch_operation_id
                OR (diagnostic.operation_id=start.launch_operation_id
                    AND diagnostic.kind='owned_service.dispatch_failure')
           )",
        [],
        |row| row.get(0),
    )?;
    let launch_ids = {
        let mut statement = db.prepare(
            "SELECT start.launch_operation_id
             FROM owned_service_starts AS start
             JOIN operations AS opened ON opened.operation_id=start.open_operation_id
             WHERE opened.state IN ('queued','rejected','sending','native_accepted','outcome_unknown')
               AND EXISTS(
                 SELECT 1 FROM observations AS diagnostic
                 WHERE diagnostic.source_event_key='owned-service-dispatch-failure:' || start.launch_operation_id
                    OR (diagnostic.operation_id=start.launch_operation_id
                        AND diagnostic.kind='owned_service.dispatch_failure')
               )
             ORDER BY start.updated_at_ms DESC,start.launch_operation_id
             LIMIT ?1",
        )?;
        let rows = statement.query_map([MAX_MANAGER_ACTION_REQUIRED_ITEMS], |row| {
            row.get::<_, String>(0)
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
    };
    let mut items = Vec::with_capacity(launch_ids.len());
    for launch_operation_id in launch_ids {
        if let Some(item) = owned_service_dispatch_action_for_operation(db, &launch_operation_id)? {
            items.push(item);
        }
    }
    let returned_items = items.len();
    Ok(json!({
        "status":if total_items == 0 {"clear"} else {"required"},
        "items":items,
        "total_items":total_items,
        "returned_items":returned_items,
        "has_more":total_items > MAX_MANAGER_ACTION_REQUIRED_ITEMS,
        "limit":MAX_MANAGER_ACTION_REQUIRED_ITEMS,
        "coverage":if total_items > MAX_MANAGER_ACTION_REQUIRED_ITEMS {
            "latest_unresolved_owned_service_dispatch_failures_bounded"
        } else {
            "all_unresolved_owned_service_dispatch_failures"
        },
    }))
}
/// Bounded current-attention projection for the exact current GM. The stored
/// Operation result remains unchanged; this links unresolved startup readback
/// to its retained diagnostic or validated host-interruption cut.
pub(super) fn owned_service_start_failure_actions(db: &Connection) -> Result<Value> {
    let interruption_cut = interrupted_start_cut(db)?;
    let interruption_at_ms = interruption_cut.map(|cut| cut.observed_at_ms);
    let current_started_at_ms = interruption_cut.map(|cut| cut.current_started_at_ms);
    let total_items: i64 = db.query_row(
        "SELECT count(*)
         FROM owned_service_starts AS start
         WHERE start.state='outcome_unknown'
           AND (
             EXISTS(
               SELECT 1 FROM observations AS diagnostic
               WHERE diagnostic.source_event_key='owned-service-start-failure:' || start.launch_operation_id
                  OR (diagnostic.operation_id=start.launch_operation_id
                      AND diagnostic.kind='owned_service.start_failure')
             )
             OR (?1 IS NOT NULL AND ?2 IS NOT NULL
                 AND start.updated_at_ms<?1 AND start.updated_at_ms<?2
                 AND NOT EXISTS(
                   SELECT 1 FROM observations AS diagnostic
                   WHERE diagnostic.source_event_key='owned-service-start-failure:' || start.launch_operation_id
                      OR (diagnostic.operation_id=start.launch_operation_id
                          AND diagnostic.kind='owned_service.start_failure')
                 ))
           )",
        params![interruption_at_ms, current_started_at_ms],
        |row| row.get(0),
    )?;
    let launch_ids = {
        let mut statement = db.prepare(
            "SELECT start.launch_operation_id
             FROM owned_service_starts AS start
             WHERE start.state='outcome_unknown'
               AND (
                 EXISTS(
                   SELECT 1 FROM observations AS diagnostic
                   WHERE diagnostic.source_event_key='owned-service-start-failure:' || start.launch_operation_id
                      OR (diagnostic.operation_id=start.launch_operation_id
                          AND diagnostic.kind='owned_service.start_failure')
                 )
                 OR (?1 IS NOT NULL AND ?2 IS NOT NULL
                     AND start.updated_at_ms<?1 AND start.updated_at_ms<?2
                     AND NOT EXISTS(
                       SELECT 1 FROM observations AS diagnostic
                       WHERE diagnostic.source_event_key='owned-service-start-failure:' || start.launch_operation_id
                          OR (diagnostic.operation_id=start.launch_operation_id
                              AND diagnostic.kind='owned_service.start_failure')
                     ))
               )
             ORDER BY start.updated_at_ms DESC,start.launch_operation_id
             LIMIT ?3",
        )?;
        let rows = statement.query_map(
            params![
                interruption_at_ms,
                current_started_at_ms,
                MAX_MANAGER_ACTION_REQUIRED_ITEMS
            ],
            |row| row.get::<_, String>(0),
        )?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
    };
    let mut items = Vec::with_capacity(launch_ids.len());
    for launch_operation_id in launch_ids {
        if let Some(item) = owned_service_start_action_for_operation(db, &launch_operation_id)? {
            items.push(item);
        }
    }
    let returned_items = items.len();
    Ok(json!({
        "status":if total_items == 0 {"clear"} else {"required"},
        "items":items,
        "total_items":total_items,
        "returned_items":returned_items,
        "has_more":total_items > MAX_MANAGER_ACTION_REQUIRED_ITEMS,
        "limit":MAX_MANAGER_ACTION_REQUIRED_ITEMS,
        "coverage":if total_items > MAX_MANAGER_ACTION_REQUIRED_ITEMS {
            "latest_unresolved_startup_failures_bounded"
        } else {
            "all_unresolved_startup_failures"
        },
    }))
}

pub(super) fn get_binding(db: &Connection, id: &str, generation: i64) -> Result<Value> {
    let raw:Option<String>=db.query_row("SELECT json_object('binding_id',binding_id,'generation',generation,'lane_id',lane_id,'module_instance_id',module_instance_id,'module_artifact_id',module_artifact_id,'state',state,'native_scope_key',native_scope_key,'native_root_id',native_root_id,'released_at_ms',released_at_ms,'route',json(route_json),'observation',json(state_json)) FROM bindings WHERE binding_id=?1 AND generation=?2",params![id,generation],|r|r.get(0)).optional()?;
    Ok(serde_json::from_str(&raw.ok_or_else(|| {
        Error::new("NOT_FOUND", format!("Binding {id}/{generation}"))
    })?)?)
}

fn public_text(value: &Value) -> Option<&str> {
    value.as_str().filter(|text| {
        !text.is_empty()
            && text.len() <= 256
            && !text
                .bytes()
                .any(|byte| byte.is_ascii_control() || matches!(byte, b'/' | b'\\'))
    })
}

fn public_token(value: &Value) -> Option<&str> {
    value.as_str().filter(|text| {
        !text.is_empty()
            && text.len() <= 256
            && text
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    })
}

fn project_tokens(source: &Value, keys: &[&str], target: &mut serde_json::Map<String, Value>) {
    for key in keys {
        if let Some(value) = source.get(*key).and_then(public_token) {
            target.insert((*key).to_owned(), Value::String(value.to_owned()));
        }
    }
}

fn project_texts(source: &Value, keys: &[&str], target: &mut serde_json::Map<String, Value>) {
    for key in keys {
        if let Some(value) = source.get(*key).and_then(public_text) {
            target.insert((*key).to_owned(), Value::String(value.to_owned()));
        }
    }
}

fn project_bools(source: &Value, keys: &[&str], target: &mut serde_json::Map<String, Value>) {
    for key in keys {
        if let Some(value) = source.get(*key).and_then(Value::as_bool) {
            target.insert((*key).to_owned(), Value::Bool(value));
        }
    }
}

fn project_numbers(source: &Value, keys: &[&str], target: &mut serde_json::Map<String, Value>) {
    for key in keys {
        if let Some(value) = source.get(*key).and_then(Value::as_i64).filter(|n| *n >= 0) {
            target.insert((*key).to_owned(), json!(value));
        }
    }
}

fn public_model(value: &Value) -> Option<Value> {
    if let Some(text) = public_text(value) {
        return Some(Value::String(text.to_owned()));
    }
    let source = value.as_object()?;
    let mut model = serde_json::Map::new();
    for key in ["id", "providerID", "provider_id", "variant", "modelID"] {
        if let Some(value) = source.get(key).and_then(public_text) {
            model.insert(key.to_owned(), Value::String(value.to_owned()));
        }
    }
    (!model.is_empty()).then_some(Value::Object(model))
}

fn public_receipt(value: &Value) -> Value {
    let mut receipt = serde_json::Map::new();
    project_tokens(
        value,
        &[
            "code",
            "completion_condition",
            "diagnostic_code",
            "durable_origin",
            "evidence",
            "native_session_state",
            "outcome",
            "state",
            "status",
        ],
        &mut receipt,
    );
    project_tokens(
        value,
        &[
            "binding_id",
            "bridge_boot_id",
            "native_frame_session_id",
            "native_input_id",
            "native_root_id",
            "native_session_id",
            "operation_id",
            "parentSessionId",
            "sessionId",
            "session_id",
            "system_init_session_id",
            "user_message_uuid",
        ],
        &mut receipt,
    );
    project_bools(
        value,
        &["execution_complete", "initial_task_dispatch"],
        &mut receipt,
    );
    if let Some(model) = value.get("model").and_then(public_model) {
        receipt.insert("model".to_owned(), model);
    }
    Value::Object(receipt)
}

fn public_native_session(value: &Value) -> Value {
    let mut session = serde_json::Map::new();
    project_tokens(
        value,
        &["native_session_outcome", "parentSessionId", "sessionId"],
        &mut session,
    );
    project_texts(value, &["agent"], &mut session);
    project_bools(value, &["agent_observed", "observed_now"], &mut session);
    if let Some(model) = value.get("model").and_then(public_model) {
        session.insert("model".to_owned(), model);
    }
    if let Some(time) = value.get("time") {
        let mut public_time = serde_json::Map::new();
        project_numbers(time, &["created", "updated"], &mut public_time);
        if !public_time.is_empty() {
            session.insert("time".to_owned(), Value::Object(public_time));
        }
    }
    Value::Object(session)
}

fn public_family_coverage(value: &Value) -> Value {
    let mut coverage = serde_json::Map::new();
    for key in [
        "members_total",
        "members_observed_now",
        "members_active_verified",
        "members_with_execution_evidence",
        "members_with_terminal_evidence",
    ] {
        if let Some(count) = value
            .get(key)
            .and_then(Value::as_u64)
            .filter(|count| *count <= 256)
        {
            coverage.insert(key.to_owned(), json!(count));
        }
    }
    project_bools(value, &["root_active_verified"], &mut coverage);
    Value::Object(coverage)
}

fn public_native_observation(value: &Value) -> Value {
    let mut native = serde_json::Map::new();
    project_tokens(
        value,
        &[
            "code",
            "completion_condition",
            "execution",
            "family_completeness",
            "native_root_id",
            "native_session_state",
            "state",
            "status",
        ],
        &mut native,
    );
    project_bools(value, &["enumeration_complete"], &mut native);
    project_numbers(
        value,
        &["active_drain_count", "observed_at_ms", "gaps"],
        &mut native,
    );
    if let Some(failures) = value.get("failures") {
        let projected = match failures.as_array() {
            Some(failures) => {
                native.insert("failure_count".to_owned(), json!(failures.len()));
                native.insert(
                    "failures_truncated".to_owned(),
                    json!(failures.len() > MAX_PUBLIC_NATIVE_FAILURES),
                );
                failures
                    .iter()
                    .take(MAX_PUBLIC_NATIVE_FAILURES)
                    .map(public_native_failure)
                    .collect::<Vec<_>>()
            }
            None => vec![json!({"code":"NATIVE_SNAPSHOT_DIAGNOSTIC_CORRUPT"})],
        };
        native.insert("failures".to_owned(), json!(projected));
    }
    if let Some(session) = value.get("session") {
        let session = public_native_session(session);
        if session.as_object().is_some_and(|value| !value.is_empty()) {
            native.insert("session".to_owned(), session);
        }
    }
    if let Some(coverage) = value.get("family_coverage") {
        let coverage = public_family_coverage(coverage);
        if coverage.as_object().is_some_and(|value| !value.is_empty()) {
            native.insert("family_coverage".to_owned(), coverage);
        }
    }
    Value::Object(native)
}

fn public_native_failure(value: &Value) -> Value {
    let Some(code) = value["code"]
        .as_str()
        .filter(|code| safe_start_failure_error_code(code))
    else {
        return json!({"code":"NATIVE_SNAPSHOT_DIAGNOSTIC_CORRUPT"});
    };
    let mut failure = serde_json::Map::new();
    failure.insert("code".to_owned(), json!(code));
    if let Some(source) = value["source"].as_str().filter(|source| {
        matches!(
            *source,
            "active_drains"
                | "instruction_entries"
                | "session_agent"
                | "session_model"
                | "session_goal"
                | "form"
                | "permission"
                | "child_execution_log"
                | "family_enumeration"
                | "snapshot"
        )
    }) {
        failure.insert("source".to_owned(), json!(source));
    }
    if let Some(session_id) = value["session_id"]
        .as_str()
        .filter(|id| crate::runtime::opencode_v2::valid_id(id, "ses").is_ok())
    {
        failure.insert("session_id".to_owned(), json!(session_id));
    }
    Value::Object(failure)
}

fn public_observation(value: &Value) -> Value {
    let mut observation = serde_json::Map::new();
    project_tokens(
        value,
        &[
            "bridge_boot_id",
            "code",
            "completion_condition",
            "connection",
            "execution",
            "family_completeness",
            "native_session_state",
            "state",
            "status",
            "waiting_for",
        ],
        &mut observation,
    );
    project_tokens(
        value,
        &[
            "binding_id",
            "native_input_id",
            "native_root_id",
            "operation_id",
        ],
        &mut observation,
    );
    project_bools(
        value,
        &[
            "enumeration_complete",
            "execution_complete",
            "recovery_required",
        ],
        &mut observation,
    );
    project_numbers(
        value,
        &[
            "active_drain_count",
            "connected_at_ms",
            "native_observation_id",
            "native_sequence",
            "observed_at_ms",
        ],
        &mut observation,
    );
    if let Some(model) = value.get("model").and_then(public_model) {
        observation.insert("model".to_owned(), model);
    }
    if let Some(failure) = value.get("latest_native_failure").filter(|v| !v.is_null()) {
        let projected = match (
            failure["code"].as_str(),
            failure["recorded_at_ms"]
                .as_i64()
                .filter(|value| *value > 0),
        ) {
            (Some(code), Some(recorded_at_ms)) if safe_start_failure_error_code(code) => {
                json!({"code":code,"recorded_at_ms":recorded_at_ms})
            }
            _ => json!({"code":"NATIVE_FAILURE_DIAGNOSTIC_CORRUPT"}),
        };
        observation.insert("latest_native_failure".to_owned(), projected);
    }
    for key in ["opening_evidence", "first_dispatch_adoption"] {
        if let Some(details) = value.get(key) {
            observation.insert(key.to_owned(), public_receipt(details));
        }
    }
    if let Some(native) = value.get("native") {
        let native = public_native_observation(native);
        if native.as_object().is_some_and(|value| !value.is_empty()) {
            observation.insert("native".to_owned(), native);
        }
    }
    Value::Object(observation)
}

/// Public binding projection for agent.state/list. The persisted route is also
/// consumed by adapters and internal dispatch, so `get_binding` remains the
/// private complete projection. Agent-facing readers retain route identity and
/// safe observation facts while omitting adapter options, launch paths, raw
/// native receipts, managed-owner data, and private configuration snapshots.
pub(super) fn get_binding_public(db: &Connection, id: &str, generation: i64) -> Result<Value> {
    let mut binding = get_binding(db, id, generation)?;
    // Persisted module identities are private inputs to the opening guard.
    if let Some(object) = binding.as_object_mut() {
        object.remove("module_instance_id");
        object.remove("module_artifact_id");
    }
    binding["native_scope_key"] = Value::Null;
    if public_token(&binding["native_root_id"]).is_none() {
        binding["native_root_id"] = Value::Null;
    }
    if let Some(route) = binding.get_mut("route").and_then(Value::as_object_mut) {
        route.remove("native_options");
    }
    binding["observation"] = public_observation(&binding["observation"]);
    Ok(binding)
}

pub(super) fn open(
    tx: &Transaction<'_>,
    p: &Principal,
    v: &Value,
    config: &Config,
    id: &str,
    now: i64,
) -> Result<Value> {
    p.require_operator()?;
    reserve_open(tx, v, config, id, now)
}

/// The admitted launcher may be a direct authenticated Manager or a verified
/// WorkDispatch actor. Both routes use the same held-lease and reserved-open
/// checks; only their typed identity/Task authorization differs.
#[allow(clippy::too_many_arguments)]
pub(super) fn open_for_launch_for_actor(
    tx: &Transaction<'_>,
    actor: &super::launcher::LaunchActor,
    v: &Value,
    config: &Config,
    id: &str,
    now: i64,
    lease_id: &str,
    lease_generation: i64,
) -> Result<Value> {
    let (caller_id, effective_manager_id) = match actor {
        super::launcher::LaunchActor::Direct(p) => {
            p.require_writer()?;
            if p.role == Role::Operator {
                super::require_local_operator(tx, &p.client_id)?;
            }
            (p.client_id.as_str(), p.client_id.as_str())
        }
        super::launcher::LaunchActor::OnBehalf(context) => (
            context.technical_requester_id(),
            context.effective_manager_id(),
        ),
    };
    type LaunchLeaseRow = (String, String, i64, String, Option<String>, String, String);
    let row: Option<LaunchLeaseRow> = tx.query_row(
        "SELECT l.operation_id,l.task_id,l.task_revision,l.owner_client_id,l.attempt_id,l.workspace_path,parent.original_request_json \
         FROM workspace_leases l JOIN workspace_registrations r ON r.registration_id=l.registration_id \
         JOIN operations parent ON parent.operation_id=l.operation_id \
         WHERE l.lease_id=?1 AND l.generation=?2 AND l.state='held' \
           AND r.state='active' AND r.generation=l.registration_generation \
           AND l.baseline_commit<>'' AND l.binding_digest<>'' \
           AND parent.method='swarm.launch' AND parent.caller_id=?3 \
           AND parent.state NOT IN ('rejected','cancelled','outcome_unknown')",
        params![lease_id,lease_generation,caller_id],
        |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?,row.get(6)?)),
    ).optional()?;
    let (parent_operation_id, task_id, revision, owner, attempt_id, workspace_path, parent_request) =
        row.ok_or_else(|| {
            Error::new(
                "WORKSPACE_LEASE_REQUIRED",
                "launch requires its own held current workspace lease",
            )
        })?;
    if owner != effective_manager_id {
        return Err(Error::new(
            "FORBIDDEN",
            "held workspace lease belongs to a different effective manager",
        ));
    }
    let attempt_id = attempt_id.ok_or_else(|| {
        Error::new(
            "WORKSPACE_LEASE_REQUIRED",
            "lease must be pinned to an Attempt before opening",
        )
    })?;
    match actor {
        super::launcher::LaunchActor::Direct(p) => {
            p.owns(&owner)?;
            actor.require_action_object(
                tx,
                "swarm.launch",
                &task_id,
                revision,
                Some(&attempt_id),
            )?;
        }
        super::launcher::LaunchActor::OnBehalf(context) => {
            if context.subject().attempt_id().is_some() {
                actor.require_action_object(
                    tx,
                    "swarm.launch",
                    &task_id,
                    revision,
                    Some(&attempt_id),
                )?;
            } else {
                actor.require_claimed_launch_attempt(
                    tx,
                    &parent_operation_id,
                    &task_id,
                    revision,
                    &attempt_id,
                )?;
            }
        }
    }
    let task = tasks::get_task(tx, &task_id)?;
    let attempt = tasks::get_attempt(tx, &attempt_id)?;
    if task["state"] != "open"
        || task["revision"] != revision
        || task["current_attempt_id"] != attempt_id
        || attempt["task_id"] != task_id
        || attempt["task_revision"] != revision
        || attempt["owner_id"] != owner
        || !attempt["released_at_ms"].is_null()
        || !attempt["binding_id"].is_null()
    {
        return Err(Error::new(
            "STALE_LAUNCH",
            "launch lease no longer matches an unbound current Attempt",
        ));
    }
    let child: Option<String> = tx
        .query_row(
            "SELECT caller_id FROM operations WHERE operation_id=?1 AND method='agent.open'",
            [id],
            |row| row.get(0),
        )
        .optional()?;
    if child.as_deref() != Some(caller_id) {
        return Err(Error::new(
            "FORBIDDEN",
            "launch opening must retain its real manager Operation",
        ));
    }
    model::fields(v, &["client_request_id", "lane_id", "route"])?;
    let parent_request: Value = serde_json::from_str(&parent_request)?;
    if parent_request["task_id"] != task_id
        || parent_request["expected_task_revision"] != revision
        || v["route"] != parent_request["route"]
    {
        return Err(Error::new(
            "STALE_LAUNCH",
            "binding lease or route differs from the admitted launch",
        ));
    }
    let mut route = config.route(model::text(v, "route")?)?.clone();
    let workspace_field = match route.runtime.as_str() {
        crate::runtime::opencode_v2::RUNTIME => "directory",
        "zed" => "workdir",
        "codex" | "command" | "claude" | "antigravity" | "muse" => "workspaceRoot",
        _ => {
            return Err(Error::new(
                "CAPABILITY_GAP",
                "runtime has no registered workspace-bound launch contract",
            ));
        }
    };
    let options = route.native_options.as_object_mut().ok_or_else(|| {
        Error::new(
            "CONFIG_ERROR",
            "launch route requires explicit native options",
        )
    })?;
    options.insert(workspace_field.into(), json!(workspace_path));
    reserve_open_route(tx, model::text(v, "lane_id")?, &route, id, now)
}

fn reserve_open(
    tx: &Transaction<'_>,
    v: &Value,
    config: &Config,
    id: &str,
    now: i64,
) -> Result<Value> {
    model::fields(v, &["client_request_id", "lane_id", "route"])?;
    let lane = model::text(v, "lane_id")?;
    let route = config.route(model::text(v, "route")?)?;
    reserve_open_route(tx, lane, &route, id, now)
}

fn reserve_open_route(
    tx: &Transaction<'_>,
    lane: &str,
    route: &crate::config::Route,
    id: &str,
    now: i64,
) -> Result<Value> {
    if meta(tx, "execution_mode")?.unwrap_or(Value::Null)["new_work"] != "enabled" {
        return Err(Error::new("ADMISSION_DISABLED", "new work is disabled"));
    }
    let existing: Option<String> = tx
        .query_row(
            "SELECT binding_id FROM bindings WHERE lane_id=?1 AND released_at_ms IS NULL",
            [lane],
            |r| r.get(0),
        )
        .optional()?;
    if let Some(existing) = existing {
        return Err(Error::new("LANE_ALREADY_OWNED", existing));
    }
    let binding = model::new_id();
    let instance = model::new_id();
    tx.execute("INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,module_artifact_id,state,route_json,state_json,created_at_ms) VALUES(?1,1,?2,?3,?4,'opening',?5,?6,?7)",params![binding,lane,instance,route.module_artifact_id,model::canonical(&json!(route))?,model::canonical(&json!({"execution":"not_observed","waiting_for":"runtime_adapter","family_completeness":"unknown"}))?,now])?;
    tx.execute("UPDATE operations SET binding_id=?2,binding_generation=1,effective_request_json=?3 WHERE operation_id=?1",params![id,binding,model::canonical(&json!({"route":route,"module_instance_id":instance}))?])?;
    Ok(
        json!({"operation_id":id,"binding_id":binding,"generation":1,"state":"queued","native_admission":"not_observed","waiting_for":"runtime_adapter"}),
    )
}
pub(super) fn dispatch(
    tx: &Transaction<'_>,
    p: &Principal,
    v: &Value,
    id: &str,
    now: i64,
    config: &Config,
) -> Result<(Value, bool)> {
    model::fields(
        v,
        &[
            "client_request_id",
            "attempt_id",
            "text",
            "prerequisite_operation_id",
            "launch_operation_id",
        ],
    )?;
    let attempt = model::text(v, "attempt_id")?;
    let body = model::text(v, "text")?;
    let a = tasks::get_attempt(tx, attempt)?;
    super::gm::require_attempt_control(tx, p, &a)?;
    if a["start_owner"] != "controller" {
        return Err(Error::new(
            "START_OWNED_BY_MANAGER",
            "native-manager claim never receives a second controller start",
        ));
    }
    if let Some(start) = a["start_operation_id"].as_str() {
        let (prior_method, prior_raw): (String, String) = tx.query_row(
            "SELECT method,original_request_json FROM operations WHERE operation_id=?1",
            [start],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        if prior_method != "task.dispatch" {
            return Err(Error::new(
                "ATTEMPT_START_CORRUPT",
                "Attempt start pointer is not a task.dispatch Operation",
            ));
        }
        launcher_dispatch::validate_coalesced_dispatch(tx, start, v, &a)?;
        let prior: Value = serde_json::from_str(&prior_raw)?;
        if prior["text"] != body
            || prior.get("prerequisite_operation_id") != v.get("prerequisite_operation_id")
            || prior.get("launch_operation_id") != v.get("launch_operation_id")
        {
            return Err(Error::conflict(
                "initial delivery already exists with different input or setup prerequisite; use correction, not dispatch",
            ));
        }
        return Ok((
            json!({"operation_id":start,"attempt_id":attempt,"coalesced":true}),
            false,
        ));
    }
    if a["state"] != "reserved" || !a["released_at_ms"].is_null() {
        return Err(Error::conflict(
            "initial dispatch requires an unreleased reserved Attempt",
        ));
    }
    let task = tasks::get_task(tx, model::text(&a, "task_id")?)?;
    if task["revision"] != a["task_revision"] || task["state"] != "open" {
        return Err(Error::new("STALE_REVISION", "Task changed before dispatch"));
    }
    if meta(tx, "execution_mode")?.unwrap_or(Value::Null)["new_work"] != "enabled" {
        return Err(Error::new("ADMISSION_DISABLED", "new work is disabled"));
    }
    let binding = a["binding_id"]
        .as_str()
        .ok_or_else(|| Error::new("BINDING_NOT_READY", "Attempt has no ready native binding"))?;
    let generation = model::positive(&a, "binding_generation")?;
    let b = get_binding(tx, binding, generation)?;
    if b["state"] != "ready" {
        return Err(Error::new(
            "BINDING_NOT_READY",
            "native binding has not been observed ready",
        ));
    }
    let prerequisite = prerequisites::validate_request(tx, &b, v, id)?;
    let launch_dispatch = launcher_dispatch::prepare_admission(tx, config, p, v, &a, &task, &b)?;
    let prerequisite_id = prerequisite.operation_id().map(str::to_owned);
    let prerequisite_contract_revision = prerequisite.contract_revision().map(str::to_owned);
    let mut effective = json!({
        "route":b["route"],
        "input":body,
        "task_snapshot":a["task_snapshot"]
    });
    if b["route"]["runtime"] == crate::runtime::opencode_v2::RUNTIME {
        effective["operation_contract"] = json!({
            "effect_scope":"native_session",
            "order_scope":{"binding_id":binding,"generation":generation},
            "completion_condition":"native_input_admitted",
            "replay_policy":"readback_only_no_mutation_replay",
            "fallback_used":false,
            "contract_revision":"opencode-input-v1"
        });
    }
    if let Some(admission) = &launch_dispatch {
        effective["launch_dispatch_packet"] = admission.packet.clone();
        effective["operation_contract"]["launch_dispatch"] = json!({
            "contract_revision":"launch-dispatch-v1",
            "launch_operation_id":admission.launch_operation_id,
            "packet_digest":admission.packet_digest,
            "completion_condition":"native_input_admitted",
            "replay_policy":"same_parent_and_packet_only_no_mutation_replay",
        });
    }
    if let Some(prerequisite_id) = &prerequisite_id {
        effective["prerequisite"] = json!({
            "operation_id":prerequisite_id,
            "required_completion_condition":"native_configuration_applied",
            "required_contract_revision":prerequisite_contract_revision
        });
    }
    let operation_changed = tx.execute(
        "UPDATE operations SET task_id=?2,attempt_id=?3,binding_id=?4,binding_generation=?5,prerequisite_operation_id=?6,effective_request_json=?7 \
         WHERE operation_id=?1 AND method='task.dispatch' AND state='queued'",
        params![
            id,
            a["task_id"].as_str(),
            attempt,
            binding,
            generation,
            prerequisite_id,
            model::canonical(&effective)?
        ],
    )?;
    if operation_changed != 1 {
        return Err(Error::conflict(
            "dispatch Operation changed before admission completed",
        ));
    }
    if let Some(admission) = &launch_dispatch {
        launcher_dispatch::retain_admission(tx, id, &a, admission, now)?;
    }
    let attempt_changed = tx.execute(
        "UPDATE attempts SET start_operation_id=?2,updated_at_ms=?3 \
         WHERE attempt_id=?1 AND start_operation_id IS NULL AND state='reserved' \
           AND released_at_ms IS NULL AND owner_id=?4 AND task_id=?5 \
           AND task_revision=?6 AND binding_id=?7 AND binding_generation=?8",
        params![
            attempt,
            id,
            now,
            a["owner_id"].as_str(),
            a["task_id"].as_str(),
            a["task_revision"].as_i64(),
            binding,
            generation,
        ],
    )?;
    if attempt_changed != 1 {
        return Err(Error::conflict(
            "Attempt changed before its initial dispatch was admitted",
        ));
    }
    let launch_receipt = launch_dispatch.as_ref().map(|admission| {
        json!({
            "launch_operation_id":admission.launch_operation_id,
            "packet_digest":admission.packet_digest,
        })
    });
    Ok((
        json!({"operation_id":id,"attempt_id":attempt,"state":"queued","admission":"durable_local","native_admission":"not_observed","prerequisite_operation_id":prerequisite_id,"prerequisite_state":prerequisite.receipt_state(),"launch_dispatch":launch_receipt}),
        true,
    ))
}
#[derive(Debug)]
struct OwnedServiceStartLink {
    launch_operation_id: String,
    open_operation_id: String,
    binding_id: String,
    binding_generation: i64,
    task_id: String,
    task_revision: i64,
    attempt_id: String,
    technical_requester_id: String,
    effective_manager_id: String,
    intent_nonce: String,
    route_digest: String,
    intent_digest: String,
    state: String,
    process_id: Option<i64>,
    process_birth_token: Option<String>,
    executable_sha256: Option<String>,
    proof_json: String,
}

const MAX_OWNED_SERVICE_PROOF_BYTES: usize = 8 * 1024;

type OwnedBindingRow = (String, Option<i64>, Option<String>, Option<String>);

type OwnedServiceOperationRow = (
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i64>,
    Option<String>,
);

fn owned_service_start_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<OwnedServiceStartLink> {
    Ok(OwnedServiceStartLink {
        launch_operation_id: row.get(0)?,
        open_operation_id: row.get(1)?,
        binding_id: row.get(2)?,
        binding_generation: row.get(3)?,
        task_id: row.get(4)?,
        task_revision: row.get(5)?,
        attempt_id: row.get(6)?,
        technical_requester_id: row.get(7)?,
        effective_manager_id: row.get(8)?,
        intent_nonce: row.get(9)?,
        route_digest: row.get(10)?,
        intent_digest: row.get(11)?,
        state: row.get(12)?,
        process_id: row.get(13)?,
        process_birth_token: row.get(14)?,
        executable_sha256: row.get(15)?,
        proof_json: row.get(16)?,
    })
}

fn owned_service_start_for_operation(
    db: &Connection,
    operation_id: &str,
) -> Result<Option<OwnedServiceStartLink>> {
    let mut statement = db.prepare(
        "SELECT launch_operation_id,open_operation_id,binding_id,binding_generation,
                task_id,task_revision,attempt_id,technical_requester_id,
                effective_manager_id,intent_nonce,route_digest,intent_digest,state,
                process_id,process_birth_token,executable_sha256,proof_json
         FROM owned_service_starts
         WHERE launch_operation_id=?1 OR open_operation_id=?1 LIMIT 2",
    )?;
    let rows = statement.query_map([operation_id], owned_service_start_from_row)?;
    let mut rows = rows.collect::<rusqlite::Result<Vec<_>>>()?;
    if rows.len() > 1 {
        return Err(owned_service_link_corrupt());
    }
    Ok(rows.pop())
}

fn owned_service_starts_for_attempt(
    db: &Connection,
    attempt_id: &str,
) -> Result<Vec<OwnedServiceStartLink>> {
    let mut statement = db.prepare(
        "SELECT launch_operation_id,open_operation_id,binding_id,binding_generation,
                task_id,task_revision,attempt_id,technical_requester_id,
                effective_manager_id,intent_nonce,route_digest,intent_digest,state,
                process_id,process_birth_token,executable_sha256,proof_json
         FROM owned_service_starts WHERE attempt_id=?1 ORDER BY launch_operation_id",
    )?;
    let rows = statement.query_map([attempt_id], owned_service_start_from_row)?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}

fn fail_reserved_owned_service_start(
    tx: &Transaction<'_>,
    link: &OwnedServiceStartLink,
    now: i64,
) -> Result<()> {
    match link.state.as_str() {
        "reserved" => {
            // The service-start coordinator must CAS this same row to
            // outcome_unknown before it can mint the one-shot process permit.
            // Winning this transaction therefore makes any prepared permit stale.
            let binding: Option<OwnedBindingRow> = tx
                .query_row(
                    "SELECT state,released_at_ms,native_root_id,native_scope_key
                     FROM bindings WHERE binding_id=?1 AND generation=?2",
                    params![link.binding_id, link.binding_generation],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .optional()?;
            if !binding.is_some_and(|(state, released, native_root, native_scope)| {
                state == "opening"
                    && released.is_none()
                    && native_root.is_none()
                    && native_scope.is_none()
            }) {
                return Err(Error::new(
                    "OWNED_SERVICE_ACTIVE",
                    "reserved service start no longer proves an unreleased pre-native binding",
                ));
            }
            if link.proof_json != "{}"
                || link.process_id.is_some()
                || link.process_birth_token.is_some()
                || link.executable_sha256.is_some()
                || !valid_sha256_digest(&link.route_digest)
                || !valid_sha256_digest(&link.intent_digest)
                || link.intent_nonce.is_empty()
            {
                return Err(owned_service_link_corrupt());
            }
            let cancellation_proof = model::canonical(&json!({
                "schema_version":1,
                "status":"failed_no_effect",
                "safe_code":"OWNED_SERVICE_CANCELLED_BEFORE_START",
                "owner_nonce":link.intent_nonce,
                "route_digest":link.route_digest,
                "intent_digest":link.intent_digest,
                "process_spawn_attempted":false,
            }))?;
            if cancellation_proof.len() > MAX_OWNED_SERVICE_PROOF_BYTES {
                return Err(owned_service_link_corrupt());
            }
            let changed = tx.execute(
                "UPDATE owned_service_starts
                 SET state='failed_no_effect',proof_json=?1,updated_at_ms=?2
                 WHERE launch_operation_id=?3 AND open_operation_id=?4
                   AND binding_id=?5 AND binding_generation=?6 AND task_id=?7
                   AND task_revision=?8 AND attempt_id=?9
                   AND technical_requester_id=?10 AND effective_manager_id=?11
                   AND intent_nonce=?12 AND route_digest=?13 AND intent_digest=?14
                   AND state='reserved' AND process_id IS NULL
                   AND process_birth_token IS NULL AND executable_sha256 IS NULL
                   AND proof_json='{}'",
                params![
                    cancellation_proof,
                    now,
                    link.launch_operation_id,
                    link.open_operation_id,
                    link.binding_id,
                    link.binding_generation,
                    link.task_id,
                    link.task_revision,
                    link.attempt_id,
                    link.technical_requester_id,
                    link.effective_manager_id,
                    link.intent_nonce,
                    link.route_digest,
                    link.intent_digest,
                ],
            )?;
            if changed != 1 {
                return Err(Error::conflict(
                    "owned service start changed before its no-effect cancellation fence",
                ));
            }
            Ok(())
        }
        "failed_no_effect" => validate_failed_no_effect_proof(link),
        "service_departed" => Ok(()),
        "outcome_unknown" => Err(Error::new(
            "OUTCOME_UNKNOWN",
            "owned service start may have crossed its process boundary; reconcile it before cancellation or release",
        )),
        "service_observed" => Err(Error::new(
            "OWNED_SERVICE_ACTIVE",
            "owned service process is retained; observe its departure before cancellation or release",
        )),
        _ => Err(owned_service_link_corrupt()),
    }
}

fn validate_failed_no_effect_proof(link: &OwnedServiceStartLink) -> Result<()> {
    if link.process_id.is_some()
        || link.process_birth_token.is_some()
        || link.executable_sha256.is_some()
        || link.proof_json.len() > MAX_OWNED_SERVICE_PROOF_BYTES
        || !valid_sha256_digest(&link.route_digest)
        || !valid_sha256_digest(&link.intent_digest)
        || link.intent_nonce.is_empty()
    {
        return Err(owned_service_link_corrupt());
    }
    let proof: Value =
        serde_json::from_str(&link.proof_json).map_err(|_| owned_service_link_corrupt())?;
    let canonical = model::canonical(&proof).map_err(|_| owned_service_link_corrupt())?;
    let Some(object) = proof.as_object() else {
        return Err(owned_service_link_corrupt());
    };
    let shared_valid = proof["schema_version"] == 1
        && proof["status"] == "failed_no_effect"
        && proof["owner_nonce"] == link.intent_nonce
        && proof["route_digest"] == link.route_digest
        && proof["intent_digest"] == link.intent_digest
        && proof["process_spawn_attempted"] == false;
    let valid = match proof["safe_code"].as_str() {
        Some("OWNED_SERVICE_CANCELLED_BEFORE_START") => {
            object.len() == 7
                && object.contains_key("schema_version")
                && object.contains_key("status")
                && object.contains_key("safe_code")
                && object.contains_key("owner_nonce")
                && object.contains_key("route_digest")
                && object.contains_key("intent_digest")
                && object.contains_key("process_spawn_attempted")
                && shared_valid
        }
        Some("OWNED_SERVICE_PRE_SPAWN_NO_EFFECT") => {
            object.len() == 9
                && object.contains_key("schema_version")
                && object.contains_key("status")
                && object.contains_key("safe_code")
                && object.contains_key("owner_nonce")
                && object.contains_key("route_digest")
                && object.contains_key("intent_digest")
                && object.contains_key("config_digest")
                && object.contains_key("process_spawn_attempted")
                && object.contains_key("scope_revalidation")
                && shared_valid
                && proof["config_digest"]
                    .as_str()
                    .is_some_and(valid_sha256_digest)
                && matches!(
                    proof["scope_revalidation"].as_str(),
                    Some("current" | "stale_or_unavailable")
                )
        }
        _ => false,
    };
    if canonical != link.proof_json || !valid {
        return Err(owned_service_link_corrupt());
    }
    Ok(())
}

fn valid_sha256_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn owned_service_link_corrupt() -> Error {
    Error::new(
        "OWNED_SERVICE_LINK_CORRUPT",
        "owned service start linkage is invalid",
    )
}

fn cancel_owned_service_linked_operation(
    tx: &Transaction<'_>,
    link: &OwnedServiceStartLink,
    operation_id: &str,
    method: &str,
    released_by_operation_id: &str,
    now: i64,
) -> Result<()> {
    let row: Option<OwnedServiceOperationRow> = tx
        .query_row(
            "SELECT caller_id,method,state,task_id,attempt_id,binding_id,
                    binding_generation,prerequisite_operation_id
             FROM operations WHERE operation_id=?1",
            [operation_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                ))
            },
        )
        .optional()?;
    let Some((caller, actual_method, state, task, attempt, binding, generation, prerequisite)) =
        row
    else {
        return Err(owned_service_link_corrupt());
    };
    let expected_prerequisite = if method == "agent.open" {
        Some(link.launch_operation_id.as_str())
    } else {
        None
    };
    if caller != link.technical_requester_id
        || actual_method != method
        || task.as_deref() != Some(link.task_id.as_str())
        || attempt.as_deref() != Some(link.attempt_id.as_str())
        || binding.as_deref() != Some(link.binding_id.as_str())
        || generation != Some(link.binding_generation)
        || prerequisite.as_deref() != expected_prerequisite
    {
        return Err(owned_service_link_corrupt());
    }
    match state.as_str() {
        "queued" => {
            let cancellation = json!({
                "reason":"attempt released before owned service completion",
                "cancelled_by":released_by_operation_id
            });
            let changed = tx.execute(
                "UPDATE operations SET state='cancelled',result_json=?2,settled_at_ms=?3,
                 updated_at_ms=?3 WHERE operation_id=?1 AND method=?4 AND state='queued'",
                params![operation_id, model::canonical(&cancellation)?, now, method,],
            )?;
            if changed != 1 {
                return Err(Error::conflict(
                    "owned service launch Operation changed before Attempt release",
                ));
            }
            super::capacity::sync_operation(tx, operation_id, now)?;
            Ok(())
        }
        "settled" | "rejected" | "cancelled" => Ok(()),
        "sending" | "native_accepted" | "outcome_unknown" => Err(Error::new(
            "OUTCOME_UNKNOWN",
            "linked launch Operation is already sent or unresolved; resolve it before Attempt release",
        )),
        _ => Err(owned_service_link_corrupt()),
    }?;
    if method == "agent.open" {
        tx.execute(
            "UPDATE bindings SET state='closed',released_at_ms=?3
             WHERE binding_id=?1 AND generation=?2 AND state='opening'
               AND released_at_ms IS NULL AND native_root_id IS NULL
               AND native_scope_key IS NULL",
            params![link.binding_id, link.binding_generation, now],
        )?;
    }
    Ok(())
}

pub(super) fn prepare_owned_service_attempt_release(
    tx: &Transaction<'_>,
    attempt: &Value,
    released_by_operation_id: &str,
    now: i64,
) -> Result<()> {
    let attempt_id = model::text(attempt, "attempt_id")?;
    let task_id = model::text(attempt, "task_id")?;
    let task_revision = model::positive(attempt, "task_revision")?;
    let owner_id = model::text(attempt, "owner_id")?;
    let binding_id = attempt["binding_id"].as_str();
    let binding_generation = attempt["binding_generation"].as_i64();
    for link in owned_service_starts_for_attempt(tx, attempt_id)? {
        if link.attempt_id != attempt_id
            || link.task_id != task_id
            || link.task_revision != task_revision
            || link.effective_manager_id != owner_id
            || binding_id != Some(link.binding_id.as_str())
            || binding_generation != Some(link.binding_generation)
        {
            return Err(owned_service_link_corrupt());
        }
        fail_reserved_owned_service_start(tx, &link, now)?;
        cancel_owned_service_linked_operation(
            tx,
            &link,
            &link.launch_operation_id,
            "swarm.launch",
            released_by_operation_id,
            now,
        )?;
        cancel_owned_service_linked_operation(
            tx,
            &link,
            &link.open_operation_id,
            "agent.open",
            released_by_operation_id,
            now,
        )?;
    }
    Ok(())
}

pub(super) fn cancel(
    tx: &Transaction<'_>,
    p: &Principal,
    v: &Value,
    id: &str,
    now: i64,
) -> Result<Value> {
    model::fields(v, &["client_request_id", "operation_id", "reason"])?;
    let target = model::text(v, "operation_id")?;
    let reason = model::text(v, "reason")?;
    let o = get_operation(tx, target)?;
    let linked = operation_link(tx, target)?;
    let manager_owns_link = matches!(o["method"].as_str(), Some("review.assign" | "swarm.launch"))
        && on_behalf_visible_to(tx, p, target)?
        && (o["method"] != "review.assign"
            || linked.as_ref().is_some_and(|link| link.belongs_to(p)));
    if !manager_owns_link {
        if p.role == Role::Manager && o["state"] == "queued" {
            if o["caller_id"] == p.client_id {
                p.owns(model::text(&o, "caller_id")?)?;
            } else if o["method"] == "github.pull_request.update_description" {
                // Cancelling an unsent effect is recovery of retained work,
                // not control of the Task's current execution Attempt.
                super::gm::require_authority(tx, p)?;
                let task_id = model::text(&o, "task_id")?;
                let attempt = tasks::get_attempt(tx, model::text(&o, "attempt_id")?)?;
                let task = tasks::get_task(tx, task_id)?;
                if attempt["task_id"] != task_id
                    || !super::operation_visible_to(tx, p, target)?
                    || !crate::automation::authorization::current_manager_has_task_scope(
                        tx,
                        p,
                        task_id,
                        model::text(&task, "project_id")?,
                    )?
                {
                    return Err(Error::new(
                        "FORBIDDEN",
                        "queued PR cancellation requires current GM visibility and exact retained Task/Attempt scope",
                    ));
                }
            } else if let Some(attempt_id) = o["attempt_id"].as_str()
                && manager_attempt_continuation_method(model::text(&o, "method")?)
            {
                let attempt = tasks::get_attempt(tx, attempt_id)?;
                if o["task_id"] != attempt["task_id"] {
                    return Err(Error::new(
                        "FORBIDDEN",
                        "queued Operation Task differs from its Attempt",
                    ));
                }
                super::gm::require_attempt_control(tx, p, &attempt)?;
            } else {
                p.owns(model::text(&o, "caller_id")?)?;
            }
        } else {
            p.owns(model::text(&o, "caller_id")?)?;
        }
    }
    let stale_publication = o["method"] == "forge.publish_ref"
        && o["state"] == "settled"
        && o["result"]["outcome"] == "stale_gm_epoch";
    if o["method"] == "forge.publish_ref" && p.role != Role::Operator {
        let admitted_epoch: Option<i64> = tx.query_row(
            "SELECT json_extract(effective_request_json,'$.publication_intent.admitted_gm_epoch') FROM operations WHERE operation_id=?1",
            [target],
            |row| row.get(0),
        )?;
        let current_epoch = match super::gm::record(tx)? {
            None => 0,
            Some(gm) => model::positive(&gm, "epoch")?,
        };
        if stale_publication || admitted_epoch != Some(current_epoch) {
            return Err(Error::new(
                "FORBIDDEN",
                "only the local operator may cancel a publication from a stale GM epoch",
            ));
        }
    }
    if o["method"] == "check.run" {
        return Err(Error::new(
            "CHECK_CANCEL_METHOD",
            "use check.cancel with the CheckRun ID",
        ));
    }
    if o["state"] != "queued" && !stale_publication {
        return Err(Error::new(
            "NOT_QUEUED",
            "already-sent operations require native cancellation/reconciliation, not local deletion",
        ));
    }
    if o["method"] == "github.source.poll" && o["state"] == "queued" {
        return Err(Error::new(
            "GITHUB_POLL_CANCELLATION_UNSUPPORTED",
            "a queued GitHub poll retains its source-read lease; resume the exact poll request or inspect github.source.get",
        ));
    }
    if let Some(service_start) = owned_service_start_for_operation(tx, target)? {
        let expected_method = if target == service_start.launch_operation_id {
            "swarm.launch"
        } else if target == service_start.open_operation_id {
            "agent.open"
        } else {
            return Err(owned_service_link_corrupt());
        };
        if o["method"].as_str() != Some(expected_method)
            || o["caller_id"].as_str() != Some(service_start.technical_requester_id.as_str())
            || o["task_id"].as_str() != Some(service_start.task_id.as_str())
            || o["attempt_id"].as_str() != Some(service_start.attempt_id.as_str())
            || o["binding_id"].as_str() != Some(service_start.binding_id.as_str())
            || o["binding_generation"].as_i64() != Some(service_start.binding_generation)
            || (expected_method == "swarm.launch" && !o["prerequisite_operation_id"].is_null())
            || (expected_method == "agent.open"
                && o["prerequisite_operation_id"].as_str()
                    != Some(service_start.launch_operation_id.as_str()))
        {
            return Err(owned_service_link_corrupt());
        }
        fail_reserved_owned_service_start(tx, &service_start, now)?;
    }
    let cancellation = if stale_publication {
        json!({"reason":reason,"cancelled_by":id,"previous_result":o["result"]})
    } else {
        json!({"reason":reason,"cancelled_by":id})
    };
    let count = if stale_publication {
        tx.execute(
            "UPDATE operations SET state='cancelled',settled_at_ms=?2,updated_at_ms=?2,result_json=?3 WHERE operation_id=?1 AND state='settled' AND method='forge.publish_ref' AND json_extract(result_json,'$.outcome')='stale_gm_epoch'",
            params![target, now, model::canonical(&cancellation)?],
        )?
    } else {
        tx.execute(
            "UPDATE operations SET state='cancelled',settled_at_ms=?2,updated_at_ms=?2,result_json=?3 WHERE operation_id=?1 AND state='queued'",
            params![target, now, model::canonical(&cancellation)?],
        )?
    };
    if count != 1 {
        return Err(Error::conflict("operation changed before cancellation"));
    }
    super::capacity::sync_operation(tx, target, now)?;
    if let Some(attempt_id) = o["attempt_id"].as_str() {
        super::capacity::sync_attempt(tx, attempt_id, now)?;
    }
    if o["method"] == "agent.open" {
        tx.execute("UPDATE bindings SET state='closed',released_at_ms=?3 WHERE binding_id=?1 AND generation=?2 AND state='opening' AND native_root_id IS NULL AND native_scope_key IS NULL",params![o["binding_id"].as_str(),o["binding_generation"].as_i64(),now])?;
    }
    Ok(json!({"operation_id":id,"cancelled_operation_id":target,"native_cancel_sent":false}))
}

fn manager_attempt_continuation_method(method: &str) -> bool {
    matches!(
        method,
        "task.dispatch" | "task.submit" | "task.request_changes" | "review.assign" | "swarm.launch"
    ) || (method.starts_with("agent.") && method != "agent.recover")
}
