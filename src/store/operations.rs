use super::{launcher_dispatch, meta, prerequisites, tasks};
use crate::{
    automation::authorization::{on_behalf_visible_to, operation_link},
    config::Config,
    error::{Error, Result},
    model::{self, Principal, Role},
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde_json::{Value, json};

pub(super) fn get_operation(db: &Connection, id: &str) -> Result<Value> {
    let raw:Option<String>=db.query_row("SELECT json_object('operation_id',operation_id,'caller_id',caller_id,'method',method,'state',state,'task_id',task_id,'attempt_id',attempt_id,'binding_id',binding_id,'binding_generation',binding_generation,'prerequisite_operation_id',prerequisite_operation_id,'operation_contract',json_extract(effective_request_json,'$.operation_contract'),'native_refs',json(native_refs_json),'result',json(result_json),'created_at_ms',created_at_ms,'updated_at_ms',updated_at_ms) FROM operations WHERE operation_id=?1",[id],|r|r.get(0)).optional()?;
    Ok(serde_json::from_str(&raw.ok_or_else(|| {
        Error::new("NOT_FOUND", format!("Operation {id}"))
    })?)?)
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
        &["active_drain_count", "observed_at_ms"],
        &mut native,
    );
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
    p.owns(model::text(&a, "owner_id")?)?;
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
        p.owns(model::text(&o, "caller_id")?)?;
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
