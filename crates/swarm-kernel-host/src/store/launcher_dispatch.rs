//! Store-owned admission and pre-effect validation for launch-linked initial
//! Task dispatch. The packet is immutable evidence, not a claim that the model
//! consumed the observed tools.

use super::{meta, set_meta, tasks};
use crate::{
    config::Config,
    error::{Error, Result},
    model::{self, Principal, Role},
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde_json::{Value, json};
use std::collections::BTreeSet;

const LINK_KEY_PREFIX: &str = "launcher:task_dispatch:v1:";
const LINK_KIND: &str = "launcher_task_dispatch";
const PACKET_REVISION: &str = "launch-dispatch-v1";

fn launch_manifest_route_alias(manifest: &Value) -> Result<&str> {
    manifest
        .get("runtime")
        .and_then(|runtime| runtime.get("route"))
        .and_then(|route| route.get("alias"))
        .and_then(Value::as_str)
        .filter(|alias| !alias.trim().is_empty())
        .ok_or_else(|| {
            Error::new(
                "LAUNCH_MANIFEST_CORRUPT",
                "launch runtime route alias is missing",
            )
        })
}

#[derive(Debug, Clone)]
pub(super) struct LaunchDispatchAdmission {
    pub(super) launch_operation_id: String,
    pub(super) packet: Value,
    pub(super) packet_digest: String,
}

struct LaunchParent {
    operation_id: String,
    state: String,
    task_id: Option<String>,
    attempt_id: Option<String>,
    binding_id: Option<String>,
    binding_generation: Option<i64>,
    effective_request_json: String,
    effective: Value,
    manifest: Value,
}

/// Resolve launch ownership and capture the C8 proof before a new
/// `Attempt.start_operation_id` is written. `None` preserves the legacy
/// direct-dispatch contract only when no retained launch owns this Attempt.
pub(super) fn prepare_admission(
    tx: &Transaction<'_>,
    config: &Config,
    caller: &Principal,
    request: &Value,
    attempt: &Value,
    task: &Value,
    binding: &Value,
) -> Result<Option<LaunchDispatchAdmission>> {
    let task_id = model::text(attempt, "task_id")?;
    let attempt_id = model::text(attempt, "attempt_id")?;
    let requested_parent = request
        .get("launch_operation_id")
        .map(|_| model::text(request, "launch_operation_id").map(str::to_owned))
        .transpose()?;
    let parent = retained_parent_for_attempt(tx, task_id, attempt_id)?;
    let Some(parent) = parent else {
        if requested_parent.is_some() {
            return Err(Error::new(
                "LAUNCH_PARENT_MISMATCH",
                "launch_operation_id does not own this Attempt",
            ));
        }
        return Ok(None);
    };
    if requested_parent.as_deref() != Some(parent.operation_id.as_str()) {
        return Err(Error::new(
            "LAUNCH_PARENT_REQUIRED",
            "launch-owned Attempt requires its exact launch_operation_id",
        ));
    }
    validate_parent_tuple(&parent, attempt, task, binding, None)?;

    let actor = super::launcher::dispatch_launch_actor(tx, &parent.operation_id, None)?;
    validate_dispatch_caller(tx, &actor, caller, attempt)?;
    let proof = super::launcher_mcp_tools::require_current_connection(
        tx,
        config,
        &parent.operation_id,
        None,
    )?;
    let packet = build_packet(&parent, attempt, binding, &proof)?;
    let packet_digest = digest_value(&packet)?;
    Ok(Some(LaunchDispatchAdmission {
        launch_operation_id: parent.operation_id,
        packet,
        packet_digest,
    }))
}

/// Atomically retain the packet, private ancestry link, Attempt start pointer,
/// and parent progress after the Operation row has received its effective
/// request. All writes remain in the caller's immediate Store transaction.
pub(super) fn retain_admission(
    tx: &Transaction<'_>,
    operation_id: &str,
    attempt: &Value,
    admission: &LaunchDispatchAdmission,
    now: i64,
) -> Result<()> {
    type DispatchAdmissionRow = (
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<i64>,
        String,
        String,
    );
    let (method, state, task_id, attempt_id, binding_id, generation, original_raw, effective_raw): DispatchAdmissionRow = tx.query_row(
        "SELECT method,state,task_id,attempt_id,binding_id,binding_generation,original_request_json,effective_request_json FROM operations WHERE operation_id=?1",
        [operation_id],
        |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?,row.get(6)?,row.get(7)?)),
    )?;
    if method != "task.dispatch"
        || state != "queued"
        || task_id.as_deref() != Some(model::text(attempt, "task_id")?)
        || attempt_id.as_deref() != Some(model::text(attempt, "attempt_id")?)
        || binding_id.as_deref() != Some(model::text(attempt, "binding_id")?)
        || generation != Some(model::positive(attempt, "binding_generation")?)
    {
        return Err(Error::conflict(
            "dispatch Operation changed before launch evidence was retained",
        ));
    }
    let original: Value = serde_json::from_str(&original_raw)?;
    if original["launch_operation_id"] != admission.launch_operation_id {
        return Err(Error::new(
            "LAUNCH_PARENT_MISMATCH",
            "dispatch request no longer names the exact launch parent",
        ));
    }
    let effective: Value = serde_json::from_str(&effective_raw)?;
    if effective.get("launch_dispatch_packet") != Some(&admission.packet) {
        return Err(Error::new(
            "LAUNCH_DISPATCH_PACKET_CORRUPT",
            "dispatch packet differs from its prepared immutable value",
        ));
    }
    let link = link_value(
        &admission.launch_operation_id,
        operation_id,
        model::text(attempt, "task_id")?,
        model::positive(attempt, "task_revision")?,
        model::text(attempt, "attempt_id")?,
        model::text(attempt, "binding_id")?,
        model::positive(attempt, "binding_generation")?,
        model::text(&admission.packet["capability"], "identity_digest")?,
        model::text(&admission.packet, "plan_digest")?,
        &admission.packet_digest,
        now,
    );
    let key = link_key(operation_id);
    if meta(tx, &key)?.is_some() {
        return Err(Error::new(
            "LAUNCH_DISPATCH_LINK_CORRUPT",
            "dispatch Operation already has a launch link",
        ));
    }
    set_meta(tx, &key, &link)?;

    let parent = load_parent(tx, &admission.launch_operation_id)?;
    validate_parent_tuple(
        &parent,
        attempt,
        &tasks::get_task(tx, model::text(attempt, "task_id")?)?,
        &super::operations::get_binding(
            tx,
            model::text(attempt, "binding_id")?,
            model::positive(attempt, "binding_generation")?,
        )?,
        None,
    )?;
    if parent.manifest["progress"]["task_dispatch"] != "not_started" {
        return Err(Error::conflict(
            "launch parent already has a dispatch admission",
        ));
    }
    let mut manifest = parent.manifest.clone();
    manifest["progress"]["task_dispatch"] = json!("queued");
    manifest["progress"]["task_dispatch_operation_id"] = json!(operation_id);
    manifest["progress"]["task_dispatch_packet_digest"] = json!(admission.packet_digest);
    let mut parent_effective = parent.effective.clone();
    parent_effective["launch_manifest"] = manifest;
    let mut parent_result: Option<Value> = tx
        .query_row(
            "SELECT result_json FROM operations WHERE operation_id=?1",
            [&admission.launch_operation_id],
            |row| row.get::<_, Option<String>>(0),
        )?
        .map(|raw| serde_json::from_str(&raw))
        .transpose()?;
    if let Some(result) = parent_result.as_mut() {
        if !result.is_object() {
            return Err(Error::new(
                "LAUNCH_MANIFEST_CORRUPT",
                "launch receipt is not an object",
            ));
        }
        result["task_dispatch"] = json!("queued");
        result["task_dispatch_operation_id"] = json!(operation_id);
        result["task_dispatch_packet_digest"] = json!(admission.packet_digest);
        parent_effective["receipt"] = json!({"ok":true,"value":result});
    }
    let parent_changed = tx.execute(
        "UPDATE operations SET effective_request_json=?2,result_json=?3,updated_at_ms=?4 \
         WHERE operation_id=?1 AND method='swarm.launch' AND state='queued' AND effective_request_json=?5",
        params![
            admission.launch_operation_id,
            model::canonical(&parent_effective)?,
            parent_result.as_ref().map(model::canonical).transpose()?,
            now,
            parent.effective_request_json,
        ],
    )?;
    if parent_changed != 1 {
        return Err(Error::conflict(
            "launch parent changed before dispatch linkage was retained",
        ));
    }
    Ok(())
}

/// Validate the retained ancestry and exact current C8 proof immediately
/// before a queued launch-owned dispatch becomes `sending`. Returns the
/// trusted packet for prompt rendering, or None for legacy unlinked dispatch.
pub(super) fn validate_before_effect(
    tx: &Transaction<'_>,
    config: Option<&Config>,
    operation_id: &str,
    input: &Value,
    binding: &Value,
) -> Result<Option<Value>> {
    if input.get("launch_dispatch_packet").is_some() {
        return Err(Error::new(
            "LAUNCH_DISPATCH_PACKET_CORRUPT",
            "client request cannot supply a launch dispatch packet",
        ));
    }
    let attempt_id = model::text(input, "attempt_id")?;
    let attempt = tasks::get_attempt(tx, attempt_id)?;
    let task = tasks::get_task(tx, model::text(&attempt, "task_id")?)?;
    let parent = retained_parent_for_attempt(tx, model::text(&attempt, "task_id")?, attempt_id)?;
    let effective: Option<String> = tx
        .query_row(
            "SELECT effective_request_json FROM operations WHERE operation_id=?1 AND method='task.dispatch' AND state='queued'",
            [operation_id],
            |row| row.get(0),
        )
        .optional()?;
    let effective: Value = serde_json::from_str(
        effective
            .as_deref()
            .ok_or_else(|| Error::conflict("queued dispatch Operation changed before effect"))?,
    )?;
    let packet = effective.get("launch_dispatch_packet").cloned();
    let requested_parent = input
        .get("launch_operation_id")
        .map(|_| model::text(input, "launch_operation_id").map(str::to_owned))
        .transpose()?;

    let Some(parent) = parent else {
        if requested_parent.is_some()
            || packet.is_some()
            || meta(tx, &link_key(operation_id))?.is_some()
        {
            return Err(Error::new(
                "LAUNCH_PARENT_MISMATCH",
                "dispatch retains launch ancestry that does not own this Attempt",
            ));
        }
        return Ok(None);
    };
    if requested_parent.as_deref() != Some(parent.operation_id.as_str()) {
        return Err(Error::new(
            "LAUNCH_PARENT_REQUIRED",
            "launch-owned dispatch no longer names its exact parent",
        ));
    }
    let packet = packet.ok_or_else(|| {
        Error::new(
            "LAUNCH_DISPATCH_PACKET_MISSING",
            "launch-owned dispatch has no retained immutable packet",
        )
    })?;
    let config = config.ok_or_else(|| {
        Error::new(
            "LAUNCH_DISPATCH_CONFIG_REQUIRED",
            "launch-linked dispatch requires current controller configuration",
        )
    })?;
    validate_parent_tuple(&parent, &attempt, &task, binding, Some(operation_id))?;
    validate_retained_link(tx, operation_id, &parent, &packet, &attempt)?;
    let _actor =
        super::launcher::dispatch_launch_actor(tx, &parent.operation_id, Some(operation_id))?;
    let proof = super::launcher_mcp_tools::require_current_connection(
        tx,
        config,
        &parent.operation_id,
        Some(operation_id),
    )?;
    let current = build_packet(&parent, &attempt, binding, &proof)?;
    if model::canonical(&packet)? != model::canonical(&current)? {
        return Err(Error::new(
            "STALE_LAUNCH_DISPATCH_PACKET",
            "launch plan, Task assignment, or C8 capability changed before native input",
        ));
    }
    Ok(Some(packet))
}

/// Validate an already-started Attempt coalescing request without reopening
/// its C8 gate or creating another packet/effect.
pub(super) fn validate_coalesced_dispatch(
    db: &Connection,
    operation_id: &str,
    request: &Value,
    attempt: &Value,
) -> Result<()> {
    let task_id = model::text(attempt, "task_id")?;
    let attempt_id = model::text(attempt, "attempt_id")?;
    let parent = retained_parent_for_attempt(db, task_id, attempt_id)?;
    let requested_parent = request
        .get("launch_operation_id")
        .map(|_| model::text(request, "launch_operation_id").map(str::to_owned))
        .transpose()?;
    let (original_raw, effective_raw): (String, String) = db.query_row(
        "SELECT original_request_json,effective_request_json FROM operations WHERE operation_id=?1 AND method='task.dispatch'",
        [operation_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let original: Value = serde_json::from_str(&original_raw)?;
    let effective: Value = serde_json::from_str(&effective_raw)?;
    match parent {
        Some(parent) => {
            if requested_parent.as_deref() != Some(parent.operation_id.as_str())
                || original["launch_operation_id"] != parent.operation_id
            {
                return Err(Error::new(
                    "LAUNCH_PARENT_REQUIRED",
                    "coalesced dispatch must retain the exact launch parent",
                ));
            }
            let packet = effective.get("launch_dispatch_packet").ok_or_else(|| {
                Error::new(
                    "LAUNCH_DISPATCH_PACKET_MISSING",
                    "started launch-owned dispatch has no immutable packet",
                )
            })?;
            if packet["launch_operation_id"] != parent.operation_id
                || effective["operation_contract"]["launch_dispatch"]["packet_digest"]
                    != digest_value(packet)?
                || effective["operation_contract"]["launch_dispatch"]["launch_operation_id"]
                    != parent.operation_id
            {
                return Err(Error::new(
                    "LAUNCH_DISPATCH_PACKET_CORRUPT",
                    "coalesced dispatch does not match its retained launch packet",
                ));
            }
            validate_retained_link(db, operation_id, &parent, packet, attempt)?;
        }
        None => {
            if requested_parent.is_some()
                || original.get("launch_operation_id").is_some()
                || effective.get("launch_dispatch_packet").is_some()
                || meta(db, &link_key(operation_id))?.is_some()
            {
                return Err(Error::new(
                    "LAUNCH_PARENT_MISMATCH",
                    "coalesced legacy dispatch contains launch-only evidence",
                ));
            }
        }
    }
    Ok(())
}

fn retained_parent_for_attempt(
    db: &Connection,
    task_id: &str,
    attempt_id: &str,
) -> Result<Option<LaunchParent>> {
    let mut statement = db.prepare(
        "SELECT operation_id,state,task_id,attempt_id,binding_id,binding_generation,effective_request_json \
         FROM operations WHERE method='swarm.launch' AND (attempt_id=?1 \
           OR json_extract(effective_request_json,'$.launch_manifest.task.attempt_id')=?1) \
         ORDER BY created_at_ms,operation_id",
    )?;
    let rows = statement.query_map([attempt_id], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, Option<String>>(4)?,
            row.get::<_, Option<i64>>(5)?,
            row.get::<_, String>(6)?,
        ))
    })?;
    let mut matches = Vec::new();
    for row in rows {
        let (id, state, row_task, row_attempt, binding, generation, effective_raw) = row?;
        let effective: Value = serde_json::from_str(&effective_raw)?;
        let manifest = effective
            .get("launch_manifest")
            .cloned()
            .ok_or_else(|| Error::new("LAUNCH_MANIFEST_CORRUPT", "launch manifest is missing"))?;
        let manifest_attempt = manifest["task"]["attempt_id"].as_str();
        let row_attempt_matches = row_attempt.as_deref() == Some(attempt_id);
        let manifest_attempt_matches = manifest_attempt == Some(attempt_id);
        if !row_attempt_matches && !manifest_attempt_matches {
            continue;
        }
        if row_task.as_deref() != Some(task_id)
            || manifest["task"]["task_id"] != task_id
            || !row_attempt_matches
            || !manifest_attempt_matches
        {
            return Err(Error::new(
                "LAUNCH_ANCESTRY_CORRUPT",
                "launch Operation and retained manifest disagree about the Attempt",
            ));
        }
        matches.push(LaunchParent {
            operation_id: id,
            state,
            task_id: row_task,
            attempt_id: row_attempt,
            binding_id: binding,
            binding_generation: generation,
            effective_request_json: effective_raw,
            effective,
            manifest,
        });
    }
    if matches.len() > 1 {
        return Err(Error::new(
            "LAUNCH_ANCESTRY_AMBIGUOUS",
            "multiple retained launch Operations name this Attempt",
        ));
    }
    Ok(matches.pop())
}

fn load_parent(db: &Connection, operation_id: &str) -> Result<LaunchParent> {
    let (state, task_id, attempt_id, binding_id, binding_generation, raw): (
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<i64>,
        String,
    ) = db.query_row(
        "SELECT state,task_id,attempt_id,binding_id,binding_generation,effective_request_json \
         FROM operations WHERE operation_id=?1 AND method='swarm.launch'",
        [operation_id],
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
    )?;
    let effective: Value = serde_json::from_str(&raw)?;
    let manifest = effective
        .get("launch_manifest")
        .cloned()
        .ok_or_else(|| Error::new("LAUNCH_MANIFEST_CORRUPT", "launch manifest is missing"))?;
    Ok(LaunchParent {
        operation_id: operation_id.to_owned(),
        state,
        task_id,
        attempt_id,
        binding_id,
        binding_generation,
        effective_request_json: raw,
        effective,
        manifest,
    })
}

fn validate_parent_tuple(
    parent: &LaunchParent,
    attempt: &Value,
    task: &Value,
    binding: &Value,
    dispatch_operation_id: Option<&str>,
) -> Result<()> {
    let task_id = model::text(attempt, "task_id")?;
    let attempt_id = model::text(attempt, "attempt_id")?;
    let binding_id = model::text(attempt, "binding_id")?;
    let generation = model::positive(attempt, "binding_generation")?;
    let task_revision = model::positive(attempt, "task_revision")?;
    let progress = &parent.manifest["progress"];
    let route_alias = launch_manifest_route_alias(&parent.manifest)?;
    let stage_ok = match dispatch_operation_id {
        None => {
            progress["task_dispatch"] == "not_started"
                && progress
                    .get("task_dispatch_operation_id")
                    .is_none_or(Value::is_null)
                && progress
                    .get("task_dispatch_packet_digest")
                    .is_none_or(Value::is_null)
                && attempt["start_operation_id"].is_null()
        }
        Some(operation_id) => {
            progress["task_dispatch"] == "queued"
                && progress["task_dispatch_operation_id"] == operation_id
                && progress["task_dispatch_packet_digest"]
                    .as_str()
                    .is_some_and(valid_digest)
                && attempt["start_operation_id"] == operation_id
        }
    };
    if parent.state != "queued"
        || parent.task_id.as_deref() != Some(task_id)
        || parent.attempt_id.as_deref() != Some(attempt_id)
        || parent.binding_id.as_deref() != Some(binding_id)
        || parent.binding_generation != Some(generation)
        || parent.manifest["state"] != "awaiting_native_mcp"
        || parent.manifest["runtime"]["dispatch_permitted"] != false
        || parent.manifest["task"]["task_id"] != task_id
        || parent.manifest["task"]["observed_revision"] != task_revision
        || parent.manifest["task"]["attempt_id"] != attempt_id
        || parent.manifest["binding"]["binding_id"] != binding_id
        || parent.manifest["binding"]["generation"] != generation
        || !stage_ok
        || task["state"] != "open"
        || task["revision"] != task_revision
        || task["current_attempt_id"] != attempt_id
        || attempt["state"] != "reserved"
        || attempt["start_owner"] != "controller"
        || !attempt["released_at_ms"].is_null()
        || binding["state"] != "ready"
        || !binding["released_at_ms"].is_null()
        || binding["route"]["alias"].as_str() != Some(route_alias)
    {
        return Err(Error::new(
            "STALE_LAUNCH_DISPATCH_SCOPE",
            "launch parent, Task, Attempt, or ready binding is no longer exact",
        ));
    }
    Ok(())
}

fn build_packet(
    parent: &LaunchParent,
    attempt: &Value,
    binding: &Value,
    proof: &Value,
) -> Result<Value> {
    let capability_identity_digest = validate_capability_proof(parent, attempt, binding, proof)?;
    let route_alias = launch_manifest_route_alias(&parent.manifest)?;
    let contracts = crate::mcp::participant_core_tool_contracts()?;
    let server_name = model::text(&proof["install"], "server_name")?;
    let tools = proof["native_discovered"]["tools"]
        .as_array()
        .ok_or_else(|| stale_capability("native tool inventory is missing"))?;
    let mut required_names = Vec::with_capacity(contracts.len());
    let mut seen_methods = BTreeSet::new();
    let mut seen_required_names = BTreeSet::new();
    let mut seen_tools = BTreeSet::new();
    for tool in tools {
        model::fields(tool, &["server", "name", "input_schema"])?;
        let observed_server = model::text(tool, "server")?;
        let observed_name = model::text(tool, "name")?;
        if observed_server != server_name
            || !name_is_bounded(observed_name)
            || !tool["input_schema"].is_object()
            || !seen_tools.insert(observed_name.to_owned())
        {
            return Err(stale_capability(
                "native inventory contains a duplicate or unscoped tool",
            ));
        }
    }
    for contract in contracts {
        model::fields(&contract, &["method", "name", "input_schema"])?;
        let method = model::text(&contract, "method")?;
        let name = model::text(&contract, "name")?;
        let schema = contract
            .get("input_schema")
            .filter(|value| value.is_object())
            .ok_or_else(|| stale_capability("canonical Participant schema is invalid"))?;
        if method.is_empty()
            || !name_is_bounded(name)
            || method.replace('.', "_") != name
            || !seen_methods.insert(method.to_owned())
            || !seen_required_names.insert(name.to_owned())
        {
            return Err(stale_capability(
                "canonical Participant tool contract is invalid",
            ));
        }
        let mut found = tools.iter().filter(|tool| {
            tool["server"].as_str() == Some(server_name) && tool["name"].as_str() == Some(name)
        });
        let observed = found
            .next()
            .ok_or_else(|| stale_capability("required Participant tool is not observed"))?;
        if found.next().is_some()
            || model::canonical(&observed["input_schema"])? != model::canonical(schema)?
        {
            return Err(stale_capability(
                "observed Participant tool schema differs from the canonical contract",
            ));
        }
        required_names.push(Value::String(name.to_owned()));
    }
    if required_names.is_empty() {
        return Err(stale_capability("Participant core contract is empty"));
    }
    let model = &proof["model"];
    Ok(json!({
        "schema_version":1,
        "launch_operation_id":parent.operation_id,
        "plan_digest":parent.manifest["plan_digest"],
        "task":{
            "task_id":model::text(&parent.manifest["task"],"task_id")?,
            "revision":model::positive(&parent.manifest["task"],"observed_revision")?,
            "attempt_id":model::text(&parent.manifest["task"],"attempt_id")?,
            "snapshot_digest":digest_value(&attempt["task_snapshot"])?,
        },
        "selection":{
            "route":route_alias,
            "provider":model::text(model,"provider_id")?,
            "model":model::text(model,"id")?,
            "variant":model::text(model,"variant")?,
        },
        "purpose":parent.manifest["runtime"]["purpose"],
        "capability":{
            "identity_digest":capability_identity_digest,
            "evidence_digest":proof["capability"]["evidence_digest"],
            "native_discovered_digest":proof["capability"]["native_discovered_digest"],
            "session_context_digest":proof["capability"]["session_context_digest"],
            "provider_request_digest":proof["capability"]["provider_request_digest"],
            "session_context_state":proof["session_context"]["status"],
            "provider_request_state":proof["provider_request"]["status"],
            "service_id":proof["capability"]["service_id"],
            "service_version":proof["capability"]["service_version"],
            "plugin_id":proof["capability"]["plugin_id"],
            "module_sha256":proof["capability"]["module_sha256"],
            "required_core_schemas":required_names,
        },
    }))
}

fn validate_session_context_proof(evidence: &Value) -> Result<()> {
    let status = model::text(evidence, "status")?;
    let tools = evidence["tools"]
        .as_array()
        .ok_or_else(|| stale_capability("session-context tool projection is missing"))?;
    match status {
        "unknown"
            if evidence["stage"].is_null()
                && evidence["observed_at_ms"].is_null()
                && tools.is_empty() =>
        {
            Ok(())
        }
        "unsupported"
            if evidence["stage"] == "session_context_hook"
                && evidence["observed_at_ms"]
                    .as_i64()
                    .is_some_and(|time| time > 0)
                && tools.is_empty() =>
        {
            Ok(())
        }
        "observed"
            if evidence["stage"] == "session_context_hook"
                && evidence["observed_at_ms"]
                    .as_i64()
                    .is_some_and(|time| time > 0) =>
        {
            validate_projected_hook_tools(tools)
        }
        _ => Err(stale_capability(
            "session-context observation status is invalid",
        )),
    }
}

fn validate_provider_request_proof(evidence: &Value) -> Result<()> {
    let status = model::text(evidence, "status")?;
    let tools = evidence["tools"]
        .as_array()
        .ok_or_else(|| stale_capability("provider-request tool projection is missing"))?;
    match status {
        "unknown"
            if evidence["transport"].is_null()
                && evidence["stage"].is_null()
                && evidence["observed_at_ms"].is_null()
                && tools.is_empty() =>
        {
            Ok(())
        }
        "unsupported" | "observed"
            if matches!(evidence["transport"].as_str(), Some("http" | "websocket"))
                && evidence["stage"] == "before_transport"
                && evidence["observed_at_ms"]
                    .as_i64()
                    .is_some_and(|time| time > 0) =>
        {
            if status == "unsupported" {
                if tools.is_empty() {
                    Ok(())
                } else {
                    Err(stale_capability(
                        "unsupported provider-request evidence contains tools",
                    ))
                }
            } else {
                validate_projected_hook_tools(tools)
            }
        }
        _ => Err(stale_capability(
            "provider-request observation status is invalid",
        )),
    }
}

fn validate_projected_hook_tools(tools: &[Value]) -> Result<()> {
    if tools.len() > 512 {
        return Err(stale_capability(
            "observed hook tool list exceeds its bound",
        ));
    }
    let mut names = BTreeSet::new();
    for tool in tools {
        model::fields(tool, &["name", "input_schema"])?;
        let name = model::text(tool, "name")?;
        if !name_is_bounded(name)
            || !names.insert(name)
            || !tool["input_schema"].is_object()
            || model::canonical(&tool["input_schema"])?.len() > 512 * 1024
        {
            return Err(stale_capability("observed hook tool entry is invalid"));
        }
    }
    Ok(())
}

fn validate_capability_proof(
    parent: &LaunchParent,
    attempt: &Value,
    binding: &Value,
    proof: &Value,
) -> Result<String> {
    model::fields(
        proof,
        &[
            "schema_version",
            "kind",
            "launch_operation_id",
            "dispatch_operation_id",
            "launch_identity_digest",
            "assignment_digest",
            "evidence_digest",
            "native_discovered_digest",
            "assignment",
            "service",
            "model",
            "install",
            "capability",
            "native_discovered",
            "session_context",
            "session_context_digest",
            "provider_request",
            "provider_request_digest",
            "dispatch_permitted",
            "model_consumed",
            "provider_auth",
        ],
    )?;
    model::fields(
        &proof["assignment"],
        &[
            "task_id",
            "task_revision",
            "attempt_id",
            "binding_id",
            "binding_generation",
            "participant_id",
            "grant_revision",
            "native_session_id",
        ],
    )?;
    model::fields(
        &proof["service"],
        &["id", "pid", "version", "process_identity_digest"],
    )?;
    model::fields(&proof["model"], &["id", "provider_id", "variant"])?;
    model::fields(
        &proof["install"],
        &[
            "server_name",
            "command_sha256",
            "location_sha256",
            "state",
            "runtime_config_readback",
            "matches_prepared_command",
        ],
    )?;
    model::fields(
        &proof["capability"],
        &[
            "identity_digest",
            "evidence_digest",
            "native_discovered_digest",
            "session_context_digest",
            "provider_request_digest",
            "service_id",
            "service_version",
            "plugin_id",
            "module_sha256",
        ],
    )?;
    model::fields(
        &proof["native_discovered"],
        &["status", "observed_at_ms", "tools"],
    )?;
    model::fields(
        &proof["session_context"],
        &["status", "stage", "observed_at_ms", "tools"],
    )?;
    model::fields(
        &proof["provider_request"],
        &["status", "transport", "stage", "observed_at_ms", "tools"],
    )?;

    let expected_assignment = json!({
        "task_id":attempt["task_id"],
        "task_revision":attempt["task_revision"],
        "attempt_id":attempt["attempt_id"],
        "binding_id":attempt["binding_id"],
        "binding_generation":attempt["binding_generation"],
        "native_session_id":parent.manifest["binding"]["native_root_id"],
        "participant_id":parent.manifest["participant"]["client_id"],
        "mcp_profile":"participant",
        "grant_revision":parent.manifest["participant"]["grant_revision"],
        "participation_basis":"attempt_owner",
        "assignment_id":Value::Null,
        "review_assignment_id":Value::Null,
    });
    let expected_assignment_projection = json!({
        "task_id":expected_assignment["task_id"],
        "task_revision":expected_assignment["task_revision"],
        "attempt_id":expected_assignment["attempt_id"],
        "binding_id":expected_assignment["binding_id"],
        "binding_generation":expected_assignment["binding_generation"],
        "participant_id":expected_assignment["participant_id"],
        "grant_revision":expected_assignment["grant_revision"],
        "native_session_id":expected_assignment["native_session_id"],
    });
    let route_alias = launch_manifest_route_alias(&parent.manifest)?;

    if proof["schema_version"] != 1
        || proof["kind"] != "launcher_native_mcp_dispatch_capability"
        || proof["launch_operation_id"] != parent.operation_id
        || proof["launch_identity_digest"].as_str().is_none()
        || proof["assignment_digest"].as_str().is_none()
        || proof["evidence_digest"].as_str().is_none()
        || proof["native_discovered_digest"].as_str().is_none()
        || proof["session_context_digest"].as_str().is_none()
        || proof["provider_request_digest"].as_str().is_none()
        || proof["dispatch_permitted"] != false
        || proof["model_consumed"] != "unknown"
        || proof["native_discovered"]["status"] != "observed"
        || proof["dispatch_operation_id"]
            != parent
                .manifest
                .get("progress")
                .and_then(|progress| progress.get("task_dispatch_operation_id"))
                .filter(|_| parent.manifest["progress"]["task_dispatch"] == "queued")
                .cloned()
                .unwrap_or(Value::Null)
        || proof["assignment"]["task_id"] != attempt["task_id"]
        || proof["assignment"]["task_revision"] != attempt["task_revision"]
        || proof["assignment"]["attempt_id"] != attempt["attempt_id"]
        || proof["assignment"]["binding_id"] != attempt["binding_id"]
        || proof["assignment"]["binding_generation"] != attempt["binding_generation"]
        || proof["assignment"]["participant_id"] != expected_assignment["participant_id"]
        || proof["assignment"]["grant_revision"] != expected_assignment["grant_revision"]
        || proof["assignment"]["native_session_id"] != expected_assignment["native_session_id"]
        || digest_value(&expected_assignment)? != proof["assignment_digest"]
        || model::canonical(&expected_assignment_projection)?
            != model::canonical(&proof["assignment"])?
        || proof["service"]["id"] != proof["capability"]["service_id"]
        || proof["service"]["version"] != proof["capability"]["service_version"]
        || proof["launch_identity_digest"] != proof["capability"]["identity_digest"]
        || proof["evidence_digest"] != proof["capability"]["evidence_digest"]
        || proof["native_discovered_digest"] != proof["capability"]["native_discovered_digest"]
        || proof["install"]["state"] != "registered"
        || proof["install"]["runtime_config_readback"] != "not_exposed_by_pinned_api"
        || proof["install"]["matches_prepared_command"] != "unknown"
        || proof["install"]["server_name"]
            .as_str()
            .is_none_or(|value| !name_is_bounded(value))
        || proof["native_discovered"]["tools"].as_array().is_none()
        || binding["route"]["alias"].as_str() != Some(route_alias)
        || parent.manifest["participant"]["role"] != "participant"
        || parent.manifest["participant"]["participation_basis"]["kind"] != "attempt_owner"
        || parent.manifest["participant"]["client_id"]
            .as_str()
            .is_none_or(str::is_empty)
        || parent.manifest["participant"]["grant_revision"]
            .as_i64()
            .is_none_or(|v| v <= 0)
        || parent.manifest["binding"]["native_root_id"]
            .as_str()
            .is_none_or(str::is_empty)
    {
        return Err(stale_capability(
            "native MCP proof is not current for this launch assignment",
        ));
    }
    let capability = &proof["capability"];
    for field in [
        "identity_digest",
        "evidence_digest",
        "native_discovered_digest",
        "service_id",
        "service_version",
        "plugin_id",
        "module_sha256",
    ] {
        model::text(capability, field)
            .map_err(|_| stale_capability("C8 capability field is missing"))?;
    }
    for field in [
        "identity_digest",
        "evidence_digest",
        "native_discovered_digest",
        "session_context_digest",
        "provider_request_digest",
        "module_sha256",
    ] {
        if !valid_digest(capability[field].as_str().unwrap_or_default()) {
            return Err(stale_capability("C8 capability digest is invalid"));
        }
    }
    for field in [
        "launch_identity_digest",
        "assignment_digest",
        "evidence_digest",
        "native_discovered_digest",
        "session_context_digest",
        "provider_request_digest",
        "service.process_identity_digest",
        "install.command_sha256",
        "install.location_sha256",
    ] {
        let digest = match field.split_once('.') {
            Some((object, member)) => proof[object][member].as_str().unwrap_or_default(),
            None => proof[field].as_str().unwrap_or_default(),
        };
        if !valid_digest(digest) {
            return Err(stale_capability("C8 readback digest is invalid"));
        }
    }
    for field in [
        "evidence_digest",
        "native_discovered_digest",
        "session_context_digest",
        "provider_request_digest",
    ] {
        if proof[field] != capability[field] {
            return Err(stale_capability("C8 capability digests disagree"));
        }
    }
    if digest_value(&proof["session_context"])? != proof["session_context_digest"]
        || digest_value(&proof["provider_request"])? != proof["provider_request_digest"]
    {
        return Err(stale_capability(
            "C8 hook readback digest does not match its projected evidence",
        ));
    }
    validate_session_context_proof(&proof["session_context"])?;
    validate_provider_request_proof(&proof["provider_request"])?;
    for field in [
        "identity_digest",
        "evidence_digest",
        "native_discovered_digest",
        "module_sha256",
    ] {
        if !valid_digest(capability[field].as_str().unwrap_or_default()) {
            return Err(stale_capability("C8 capability digest is invalid"));
        }
    }
    model::text(&proof["model"], "id").map_err(|_| stale_capability("C8 model is missing"))?;
    let model_provider = model::text(&proof["model"], "provider_id")
        .map_err(|_| stale_capability("C8 provider is missing"))?;
    model::text(&proof["model"], "variant")
        .map_err(|_| stale_capability("C8 variant is missing"))?;
    if proof["service"]["pid"].as_u64().is_none_or(|pid| pid == 0)
        || proof["native_discovered"]["observed_at_ms"]
            .as_i64()
            .is_none_or(|time| time <= 0)
        || proof["capability"]["plugin_id"] != "eliot.native-mcp-proof.v1"
        || !valid_digest(parent.manifest["plan_digest"].as_str().unwrap_or_default())
    {
        return Err(stale_capability(
            "C8 service, plan, or observation identity is invalid",
        ));
    }

    let owned = binding["route"]["owned_service"]
        .as_object()
        .ok_or_else(|| stale_capability("launch binding has no explicit owned-service route"))?;
    let provider_auth = match owned.get("credential_ref").and_then(Value::as_str) {
        Some(credential_ref) if !credential_ref.is_empty() => {
            let auth = proof
                .get("provider_auth")
                .ok_or_else(|| stale_capability("owned provider credential proof is missing"))?;
            model::fields(auth, &["status", "proof_digest"])?;
            if auth["status"] != "stored_unverified"
                || !valid_digest(auth["proof_digest"].as_str().unwrap_or_default())
                || model_provider != "opencode-go"
            {
                return Err(stale_capability(
                    "owned provider credential proof is not the exact retained unverified scope",
                ));
            }
            Some(auth["proof_digest"].as_str().unwrap_or_default())
        }
        Some(_) => return Err(stale_capability("owned credential reference is malformed")),
        None => {
            if proof.get("provider_auth").is_some() {
                return Err(stale_capability(
                    "provider credential proof is present for an uncredentialed route",
                ));
            }
            None
        }
    };
    let model_ref = &owned["model"];
    if !model_ref.is_object()
        || model_ref["providerID"] != proof["model"]["provider_id"]
        || model_ref["id"] != proof["model"]["id"]
        || model_ref["variant"] != proof["model"]["variant"]
        || owned["service_id"] != proof["service"]["id"]
        || proof["service"]["version"] != "2.0.7"
    {
        return Err(stale_capability(
            "native provider/model differs from the explicit owned route",
        ));
    }
    let c8_identity = model::text(capability, "identity_digest")?;
    if let Some(provider_auth_digest) = provider_auth {
        // Keep provider-auth evidence private while binding it into the
        // existing prompt identity digest; the packet has no credential field.
        return digest_value(&json!({
            "c8_identity_digest":c8_identity,
            "provider_auth_proof_digest":provider_auth_digest,
        }));
    }
    Ok(c8_identity.to_owned())
}

fn validate_dispatch_caller(
    db: &Connection,
    actor: &super::launcher::LaunchActor,
    caller: &Principal,
    attempt: &Value,
) -> Result<()> {
    let owner_id = model::text(attempt, "owner_id")?;
    match caller.role {
        // Operators retain the same direct Store scope they had before this
        // launch-specific gate, but a WorkDispatch child must be submitted by
        // its effective manager so the retained parent/child authority agrees.
        Role::Operator if actor.work_dispatch_context().is_some() => {
            return Err(Error::new(
                "FORBIDDEN",
                "WorkDispatch launch dispatch requires its effective Manager",
            ));
        }
        Role::Operator => {}
        Role::Manager => {
            // A live current GM may continue this exact prepared Attempt, but
            // the immutable launch actor and Attempt owner stay unchanged.
            super::gm::require_attempt_control(db, caller, attempt)?;
            if actor.work_dispatch_context().is_some() && actor.effective_manager_id() != owner_id {
                return Err(Error::new(
                    "FORBIDDEN",
                    "WorkDispatch effective manager differs from the Attempt owner",
                ));
            }
            if actor.role() == Role::Manager
                && actor.work_dispatch_context().is_none()
                && actor.effective_manager_id() != owner_id
            {
                return Err(Error::new(
                    "FORBIDDEN",
                    "retained Manager launch actor differs from the Attempt owner",
                ));
            }
        }
        _ => {
            return Err(Error::new(
                "FORBIDDEN",
                "launch-linked dispatch requires the current Task owner or Operator",
            ));
        }
    }
    Ok(())
}

fn validate_retained_link(
    db: &Connection,
    operation_id: &str,
    parent: &LaunchParent,
    packet: &Value,
    attempt: &Value,
) -> Result<()> {
    type DispatchLinkRow = (
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<i64>,
        Option<String>,
        String,
        String,
    );
    let operation: Option<DispatchLinkRow> = db
        .query_row(
            "SELECT method,state,task_id,attempt_id,binding_id,binding_generation,prerequisite_operation_id, \
                    original_request_json,effective_request_json \
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
                    row.get(8)?,
                ))
            },
        )
        .optional()?;
    let Some((
        method,
        state,
        task_id,
        attempt_id,
        binding_id,
        binding_generation,
        _prerequisite_operation_id,
        original_raw,
        effective_raw,
    )) = operation
    else {
        return Err(Error::new(
            "LAUNCH_DISPATCH_LINK_MISSING",
            "launch dispatch Operation is missing",
        ));
    };
    let original: Value = serde_json::from_str(&original_raw)?;
    let effective: Value = serde_json::from_str(&effective_raw)?;
    let dispatch_contract = &effective["operation_contract"]["launch_dispatch"];
    model::fields(
        dispatch_contract,
        &[
            "contract_revision",
            "launch_operation_id",
            "packet_digest",
            "completion_condition",
            "replay_policy",
        ],
    )?;
    if method != "task.dispatch"
        || !matches!(
            state.as_str(),
            "queued" | "sending" | "native_accepted" | "outcome_unknown" | "settled" | "rejected"
        )
        || original["launch_operation_id"] != parent.operation_id
        || original["attempt_id"] != attempt["attempt_id"]
        || task_id.as_deref() != attempt["task_id"].as_str()
        || attempt_id.as_deref() != attempt["attempt_id"].as_str()
        || binding_id.as_deref() != attempt["binding_id"].as_str()
        || binding_generation != attempt["binding_generation"].as_i64()
        || model::canonical(&effective["launch_dispatch_packet"])? != model::canonical(packet)?
        || dispatch_contract["contract_revision"] != PACKET_REVISION
        || dispatch_contract["launch_operation_id"] != parent.operation_id
        || dispatch_contract["packet_digest"] != digest_value(packet)?
        || dispatch_contract["completion_condition"] != "native_input_admitted"
        || dispatch_contract["replay_policy"] != "same_parent_and_packet_only_no_mutation_replay"
    {
        return Err(Error::new(
            "LAUNCH_DISPATCH_LINK_CORRUPT",
            "dispatch Operation request and immutable launch packet disagree",
        ));
    }
    let link = meta(db, &link_key(operation_id))?.ok_or_else(|| {
        Error::new(
            "LAUNCH_DISPATCH_LINK_MISSING",
            "launch dispatch ancestry is missing",
        )
    })?;
    model::fields(
        &link,
        &[
            "schema_version",
            "kind",
            "launch_operation_id",
            "dispatch_operation_id",
            "plan_digest",
            "task_id",
            "task_revision",
            "attempt_id",
            "binding_id",
            "binding_generation",
            "packet_digest",
            "capability_identity_digest",
            "created_at_ms",
        ],
    )?;
    let packet_digest = digest_value(packet)?;
    if link["schema_version"] != 1
        || link["kind"] != LINK_KIND
        || link["launch_operation_id"] != parent.operation_id
        || link["dispatch_operation_id"] != operation_id
        || link["plan_digest"] != packet["plan_digest"]
        || link["task_id"] != attempt["task_id"]
        || link["task_revision"] != attempt["task_revision"]
        || link["attempt_id"] != attempt["attempt_id"]
        || link["binding_id"] != attempt["binding_id"]
        || link["binding_generation"] != attempt["binding_generation"]
        || link["packet_digest"] != packet_digest
        || link["capability_identity_digest"] != packet["capability"]["identity_digest"]
        || link["created_at_ms"].as_i64().is_none_or(|time| time <= 0)
        || packet["launch_operation_id"] != parent.operation_id
        || parent.manifest["progress"]["task_dispatch"] != "queued"
        || parent.manifest["progress"]["task_dispatch_operation_id"] != operation_id
        || parent.manifest["progress"]["task_dispatch_packet_digest"] != packet_digest
    {
        return Err(Error::new(
            "LAUNCH_DISPATCH_LINK_CORRUPT",
            "launch dispatch packet, parent progress, and private link disagree",
        ));
    }
    Ok(())
}

/// Recover the immutable launch parent of a retained dispatch without
/// consulting the current Attempt or the parent's mutable progress/state.
/// Event-source provenance is historical: a completed launch keeps its
/// Manager attribution even after the effect-time dispatch validator would
/// correctly reject another mutation.
pub(super) fn historical_parent_for_operation(
    db: &Connection,
    operation_id: &str,
) -> Result<Option<String>> {
    type DispatchRow = (
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<i64>,
        String,
        String,
    );
    let operation: Option<DispatchRow> = db
        .query_row(
            "SELECT method,caller_id,task_id,attempt_id,binding_id,binding_generation,\
                    original_request_json,effective_request_json \
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
    let Some((
        method,
        caller_id,
        task_id,
        attempt_id,
        binding_id,
        binding_generation,
        original_raw,
        effective_raw,
    )) = operation
    else {
        return Err(Error::new(
            "LAUNCH_DISPATCH_LINK_MISSING",
            "launch dispatch Operation is missing",
        ));
    };
    if method != "task.dispatch" {
        return Ok(None);
    }

    let original: Value = serde_json::from_str(&original_raw)?;
    let effective: Value = serde_json::from_str(&effective_raw)?;
    let original_parent = match original.get("launch_operation_id") {
        None | Some(Value::Null) => None,
        Some(Value::String(value)) => Some(value.as_str()),
        Some(_) => {
            return Err(Error::new(
                "LAUNCH_DISPATCH_LINK_CORRUPT",
                "dispatch launch parent identity is malformed",
            ));
        }
    };
    let packet = effective.get("launch_dispatch_packet");
    match (original_parent, packet) {
        (None, None) => return Ok(None),
        (Some(_), Some(_)) => {}
        _ => {
            return Err(Error::new(
                "LAUNCH_DISPATCH_LINK_CORRUPT",
                "dispatch request and retained launch packet disagree about ancestry",
            ));
        }
    }
    let parent_operation_id = original_parent
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            Error::new(
                "LAUNCH_DISPATCH_LINK_CORRUPT",
                "dispatch launch parent identity is empty",
            )
        })?;
    let Some(packet) = packet else {
        return Err(Error::new(
            "LAUNCH_DISPATCH_LINK_CORRUPT",
            "dispatch launch packet is missing",
        ));
    };
    model::fields(
        packet,
        &[
            "schema_version",
            "launch_operation_id",
            "plan_digest",
            "task",
            "selection",
            "purpose",
            "capability",
        ],
    )?;
    model::fields(
        &packet["task"],
        &["task_id", "revision", "attempt_id", "snapshot_digest"],
    )?;
    let task_id = task_id
        .as_deref()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            Error::new(
                "LAUNCH_DISPATCH_LINK_CORRUPT",
                "dispatch Task identity is missing",
            )
        })?;
    let attempt_id = attempt_id
        .as_deref()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            Error::new(
                "LAUNCH_DISPATCH_LINK_CORRUPT",
                "dispatch Attempt identity is missing",
            )
        })?;
    let binding_id = binding_id
        .as_deref()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            Error::new(
                "LAUNCH_DISPATCH_LINK_CORRUPT",
                "dispatch binding identity is missing",
            )
        })?;
    let binding_generation = binding_generation
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            Error::new(
                "LAUNCH_DISPATCH_LINK_CORRUPT",
                "dispatch binding generation is missing",
            )
        })?;
    let task_revision = packet["task"]["revision"]
        .as_i64()
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            Error::new(
                "LAUNCH_DISPATCH_LINK_CORRUPT",
                "dispatch packet Task revision is missing",
            )
        })?;
    let packet_digest = digest_value(packet)?;
    model::fields(
        &effective["operation_contract"]["launch_dispatch"],
        &[
            "contract_revision",
            "launch_operation_id",
            "packet_digest",
            "completion_condition",
            "replay_policy",
        ],
    )?;
    if original["attempt_id"] != attempt_id
        || packet["schema_version"] != 1
        || packet["launch_operation_id"] != parent_operation_id
        || packet["task"]["task_id"] != task_id
        || packet["task"]["attempt_id"] != attempt_id
        || packet["plan_digest"]
            .as_str()
            .is_none_or(|value| !valid_digest(value))
        || packet["task"]["snapshot_digest"]
            .as_str()
            .is_none_or(|value| !valid_digest(value))
        || effective["operation_contract"]["launch_dispatch"]["contract_revision"]
            != PACKET_REVISION
        || effective["operation_contract"]["launch_dispatch"]["launch_operation_id"]
            != parent_operation_id
        || effective["operation_contract"]["launch_dispatch"]["packet_digest"] != packet_digest
        || effective["operation_contract"]["launch_dispatch"]["completion_condition"]
            != "native_input_admitted"
        || effective["operation_contract"]["launch_dispatch"]["replay_policy"]
            != "same_parent_and_packet_only_no_mutation_replay"
    {
        return Err(Error::new(
            "LAUNCH_DISPATCH_LINK_CORRUPT",
            "dispatch Operation and retained launch packet disagree",
        ));
    }

    let link = meta(db, &link_key(operation_id))?.ok_or_else(|| {
        Error::new(
            "LAUNCH_DISPATCH_LINK_MISSING",
            "launch dispatch ancestry is missing",
        )
    })?;
    model::fields(
        &link,
        &[
            "schema_version",
            "kind",
            "launch_operation_id",
            "dispatch_operation_id",
            "plan_digest",
            "task_id",
            "task_revision",
            "attempt_id",
            "binding_id",
            "binding_generation",
            "packet_digest",
            "capability_identity_digest",
            "created_at_ms",
        ],
    )?;
    if link["schema_version"] != 1
        || link["kind"] != LINK_KIND
        || link["launch_operation_id"] != parent_operation_id
        || link["dispatch_operation_id"] != operation_id
        || link["plan_digest"] != packet["plan_digest"]
        || link["task_id"] != task_id
        || link["task_revision"] != task_revision
        || link["attempt_id"] != attempt_id
        || link["binding_id"] != binding_id
        || link["binding_generation"] != binding_generation
        || link["packet_digest"] != packet_digest
        || link["capability_identity_digest"] != packet["capability"]["identity_digest"]
        || link["created_at_ms"].as_i64().is_none_or(|time| time <= 0)
    {
        return Err(Error::new(
            "LAUNCH_DISPATCH_LINK_CORRUPT",
            "dispatch packet and immutable ancestry link disagree",
        ));
    }

    type ParentRow = (
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<i64>,
        String,
    );
    let parent: Option<ParentRow> = db
        .query_row(
            "SELECT caller_id,method,task_id,attempt_id,binding_id,binding_generation,effective_request_json \
             FROM operations WHERE operation_id=?1",
            [parent_operation_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .optional()?;
    let Some((
        parent_caller_id,
        parent_method,
        parent_task_id,
        parent_attempt_id,
        parent_binding_id,
        parent_generation,
        parent_effective_raw,
    )) = parent
    else {
        return Err(Error::new(
            "LAUNCH_DISPATCH_LINK_CORRUPT",
            "dispatch launch parent is missing",
        ));
    };
    let parent_effective: Value = serde_json::from_str(&parent_effective_raw)?;
    let manifest = &parent_effective["launch_manifest"];
    if parent_method != "swarm.launch"
        || parent_caller_id != caller_id
        || parent_task_id.as_deref() != Some(task_id)
        || parent_attempt_id.as_deref() != Some(attempt_id)
        || parent_binding_id.as_deref() != Some(binding_id)
        || parent_generation != Some(binding_generation)
        || manifest["plan_digest"] != packet["plan_digest"]
        || manifest["task"]["task_id"] != task_id
        || manifest["task"]["observed_revision"] != task_revision
        || manifest["task"]["attempt_id"] != attempt_id
        || manifest["binding"]["binding_id"] != binding_id
        || manifest["binding"]["generation"] != binding_generation
        || manifest["binding"]["operation_id"]
            .as_str()
            .is_none_or(str::is_empty)
    {
        return Err(Error::new(
            "LAUNCH_DISPATCH_LINK_CORRUPT",
            "launch parent manifest and retained dispatch packet disagree",
        ));
    }
    Ok(Some(parent_operation_id.to_owned()))
}

#[allow(clippy::too_many_arguments)] // Canonical immutable dispatch-link tuple.
fn link_value(
    parent_id: &str,
    dispatch_id: &str,
    task_id: &str,
    task_revision: i64,
    attempt_id: &str,
    binding_id: &str,
    binding_generation: i64,
    capability_identity_digest: &str,
    plan_digest: &str,
    packet_digest: &str,
    now: i64,
) -> Value {
    json!({
        "schema_version":1,
        "kind":LINK_KIND,
        "launch_operation_id":parent_id,
        "dispatch_operation_id":dispatch_id,
        "plan_digest":plan_digest,
        "task_id":task_id,
        "task_revision":task_revision,
        "attempt_id":attempt_id,
        "binding_id":binding_id,
        "binding_generation":binding_generation,
        "packet_digest":packet_digest,
        "capability_identity_digest":capability_identity_digest,
        "created_at_ms":now,
    })
}

fn link_key(operation_id: &str) -> String {
    format!(
        "{LINK_KEY_PREFIX}{}",
        model::digest(operation_id.as_bytes())
    )
}

fn digest_value(value: &Value) -> Result<String> {
    Ok(format!(
        "sha256:{}",
        model::digest(model::canonical(value)?.as_bytes())
    ))
}

fn valid_digest(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|hex| {
        hex.len() == 64
            && hex
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

fn name_is_bounded(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_-.:".contains(&byte))
}

fn stale_capability(message: &str) -> Error {
    Error::new("NATIVE_MCP_CAPABILITY_UNAVAILABLE", message)
}
