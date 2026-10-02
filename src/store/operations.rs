use super::{meta, prerequisites, tasks};
use crate::{
    config::Config,
    error::{Error, Result},
    model::{self, Principal},
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
    let raw:Option<String>=db.query_row("SELECT json_object('binding_id',binding_id,'generation',generation,'lane_id',lane_id,'state',state,'native_scope_key',native_scope_key,'native_root_id',native_root_id,'released_at_ms',released_at_ms,'route',json(route_json),'observation',json(state_json)) FROM bindings WHERE binding_id=?1 AND generation=?2",params![id,generation],|r|r.get(0)).optional()?;
    Ok(serde_json::from_str(&raw.ok_or_else(|| {
        Error::new("NOT_FOUND", format!("Binding {id}/{generation}"))
    })?)?)
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
    model::fields(v, &["client_request_id", "lane_id", "route"])?;
    let lane = model::text(v, "lane_id")?;
    let route = config.route(model::text(v, "route")?)?;
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
) -> Result<(Value, bool)> {
    model::fields(
        v,
        &[
            "client_request_id",
            "attempt_id",
            "text",
            "prerequisite_operation_id",
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
        let prior: String = tx.query_row(
            "SELECT original_request_json FROM operations WHERE operation_id=?1",
            [start],
            |r| r.get(0),
        )?;
        let prior: Value = serde_json::from_str(&prior)?;
        if prior["text"] != body
            || prior.get("prerequisite_operation_id") != v.get("prerequisite_operation_id")
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
    if let Some(prerequisite_id) = &prerequisite_id {
        effective["prerequisite"] = json!({
            "operation_id":prerequisite_id,
            "required_completion_condition":"native_configuration_applied",
            "required_contract_revision":prerequisite_contract_revision
        });
    }
    tx.execute("UPDATE operations SET task_id=?2,attempt_id=?3,binding_id=?4,binding_generation=?5,prerequisite_operation_id=?6,effective_request_json=?7 WHERE operation_id=?1",params![id,a["task_id"].as_str(),attempt,binding,generation,prerequisite_id,model::canonical(&effective)?])?;
    tx.execute("UPDATE attempts SET start_operation_id=?2,updated_at_ms=?3 WHERE attempt_id=?1 AND start_operation_id IS NULL",params![attempt,id,now])?;
    Ok((
        json!({"operation_id":id,"attempt_id":attempt,"state":"queued","admission":"durable_local","native_admission":"not_observed","prerequisite_operation_id":prerequisite_id,"prerequisite_state":prerequisite.receipt_state()}),
        true,
    ))
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
    p.owns(model::text(&o, "caller_id")?)?;
    if o["method"] == "check.run" {
        return Err(Error::new(
            "CHECK_CANCEL_METHOD",
            "use check.cancel with the CheckRun ID",
        ));
    }
    if o["state"] != "queued" {
        return Err(Error::new(
            "NOT_QUEUED",
            "already-sent operations require native cancellation/reconciliation, not local deletion",
        ));
    }
    let count=tx.execute("UPDATE operations SET state='cancelled',settled_at_ms=?2,updated_at_ms=?2,result_json=?3 WHERE operation_id=?1 AND state='queued'",params![target,now,model::canonical(&json!({"reason":reason,"cancelled_by":id}))?])?;
    if count != 1 {
        return Err(Error::conflict("operation changed before cancellation"));
    }
    if o["method"] == "agent.open" {
        tx.execute("UPDATE bindings SET state='closed',released_at_ms=?3 WHERE binding_id=?1 AND generation=?2 AND state='opening' AND native_root_id IS NULL",params![o["binding_id"].as_str(),o["binding_generation"].as_i64(),now])?;
    }
    Ok(json!({"operation_id":id,"cancelled_operation_id":target,"native_cancel_sent":false}))
}
