//! Module admission and facts, scoped by a credential to one reserved native root.
//! Network I/O is never performed inside these transactions.
use super::{meta, operations, tasks};
use crate::{
    error::{Error, Result},
    model::{self, Principal, Role},
    runtime::{EffectOutcome, RuntimeCommand, RuntimeOutcome},
};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde_json::{Value, json};

fn scope(db: &Connection, p: &Principal, check_link: bool) -> Result<(String, i64, Value)> {
    if p.role != Role::Module {
        return Err(Error::new("FORBIDDEN", "module credential required"));
    }
    let c = meta(db, &format!("client:{}", p.client_id))?
        .ok_or_else(|| Error::new("UNAUTHORIZED", "module is not registered"))?;
    if c["disabled"] == true || c["role"] != "module" {
        return Err(Error::new("UNAUTHORIZED", "module is disabled"));
    }
    let id = model::text(&c, "binding_id")?.to_string();
    let generation = model::positive(&c, "binding_generation")?;
    let b = operations::get_binding(db, &id, generation)?;
    if !b["released_at_ms"].is_null() {
        return Err(Error::new("BINDING_CLOSED", "module binding is released"));
    }
    if check_link && b["observation"]["module_link_id"] != p.link_id {
        return Err(Error::new(
            "STALE_LINK",
            "reconnect and reconcile this module before new commands",
        ));
    }
    Ok((id, generation, b))
}

pub(super) fn register(db: &Connection, v: &Value, client_id: &str) -> Result<Value> {
    let id = model::text(v, "binding_id")?;
    let generation = model::positive(v, "binding_generation")?;
    let b = operations::get_binding(db, id, generation)?;
    if !b["released_at_ms"].is_null() {
        return Err(Error::new(
            "BINDING_CLOSED",
            "cannot register a released binding",
        ));
    }
    if b["observation"].get("module_client_id").is_some() {
        return Err(Error::conflict("binding already has a module credential"));
    }
    db.execute("UPDATE bindings SET state_json=json_set(state_json,'$.module_client_id',?3) WHERE binding_id=?1 AND generation=?2", params![id,generation,client_id])?;
    Ok(json!({"binding_id":id,"binding_generation":generation}))
}

pub(super) fn hello(db: &mut Connection, p: &Principal, v: &Value) -> Result<Value> {
    model::fields(
        v,
        &[
            "boot_id",
            "module_artifact_id",
            "native_root_id",
            "native_scope_key",
            "native_ready",
        ],
    )?;
    let boot = model::text(v, "boot_id")?;
    let artifact = model::text(v, "module_artifact_id")?;
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let (id, generation, b) = scope(&tx, p, false)?;
    if b["route"]["module_artifact_id"] != artifact {
        return Err(Error::new(
            "ARTIFACT_MISMATCH",
            "module artifact differs from the reserved route",
        ));
    }
    let old_boot = b["observation"]["bridge_boot_id"].as_str();
    if old_boot.is_some_and(|old| old != boot) {
        let possible:i64=tx.query_row("SELECT count(*) FROM operations WHERE binding_id=?1 AND binding_generation=?2 AND state IN ('sending','native_accepted','outcome_unknown','settled') AND method IN ('agent.open','task.dispatch','agent.send','agent.reply')",params![id,generation],|r|r.get(0))?;
        if possible > 0 || !b["native_root_id"].is_null() {
            return Err(Error::new(
                "RECOVERY_REQUIRED",
                "previous module may own native work; a new bridge must not spawn a second executor",
            ));
        }
    }
    if !b["native_root_id"].is_null()
        && (v.get("native_root_id") != b.get("native_root_id")
            || v.get("native_scope_key") != b.get("native_scope_key"))
    {
        return Err(Error::new(
            "NATIVE_IDENTITY_MISMATCH",
            "reconnect must identify the previously owned native session",
        ));
    }
    tx.execute("UPDATE bindings SET state_json=json_set(state_json,'$.bridge_boot_id',?3,'$.module_link_id',?4,'$.connection','connected','$.connected_at_ms',?5),state=CASE WHEN native_root_id IS NOT NULL AND state='reconciling' AND ?6 THEN 'ready' ELSE state END WHERE binding_id=?1 AND generation=?2", params![id,generation,boot,p.link_id,model::now_ms()?,v["native_ready"]==true])?;
    let result = json!({"binding_id":id,"generation":generation,"route":b["route"],"host_epoch":meta(&tx,"host_epoch")?,"native_root_id":b["native_root_id"]});
    tx.commit()?;
    Ok(result)
}

pub(super) fn next(db: &mut Connection, p: &Principal) -> Result<Value> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let (id, generation, b) = scope(&tx, p, true)?;
    if b["state"] != "opening" && b["state"] != "ready" {
        return Ok(json!({"command":null}));
    }
    let row:Option<(String,String,String,i64)>=tx.query_row(
        "SELECT operation_id,method,original_request_json,created_at_ms FROM operations AS candidate WHERE binding_id=?1 AND binding_generation=?2 AND state='queued' AND due_at_ms<=?3 AND method IN ('agent.open','task.dispatch','agent.send','agent.reply') AND (method='agent.reply' OR NOT EXISTS (SELECT 1 FROM operations AS pending WHERE pending.binding_id=?1 AND pending.binding_generation=?2 AND pending.state IN ('sending','native_accepted','outcome_unknown') AND pending.method IN ('agent.open','task.dispatch','agent.send'))) ORDER BY CASE WHEN method='agent.reply' THEN 0 ELSE 1 END,due_at_ms,operation_id LIMIT 1",
        params![id,generation,model::now_ms()?],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
    let Some((op, method, raw, created)) = row else {
        return Ok(json!({"command":null}));
    };
    let mut input: Value = serde_json::from_str(&raw)?;
    let guard = (|| -> Result<()> {
        let o = operations::get_operation(&tx, &op)?;
        let caller = meta(&tx, &format!("client:{}", model::text(&o, "caller_id")?))?
            .ok_or_else(|| Error::new("UNAUTHORIZED", "original caller no longer registered"))?;
        if caller["disabled"] == true {
            return Err(Error::new("UNAUTHORIZED", "original caller disabled"));
        }
        let starts_work = method == "agent.open"
            || method == "task.dispatch"
            || (method == "agent.send" && input["delivery"] == "next_turn");
        if starts_work
            && meta(&tx, "execution_mode")?.unwrap_or(Value::Null)["new_work"] != "enabled"
        {
            return Err(Error::new(
                "ADMISSION_DISABLED",
                "new work disabled before dispatch",
            ));
        }
        if method == "agent.open" {
            if b["state"] != "opening" || !b["native_root_id"].is_null() {
                return Err(Error::conflict("root already opened or changed"));
            }
        } else if b["state"] != "ready" {
            return Err(Error::new(
                "BINDING_NOT_READY",
                "binding not ready before dispatch",
            ));
        }
        if method != "agent.open" && caller["role"] != "operator" {
            let owns:bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM attempts WHERE owner_id=?1 AND binding_id=?2 AND binding_generation=?3 AND released_at_ms IS NULL)",params![o["caller_id"].as_str(),id,generation],|r|r.get(0))?;
            if caller["role"] != "manager" || !owns {
                return Err(Error::new(
                    "FORBIDDEN",
                    "original caller no longer has an assignment here",
                ));
            }
        }
        if method == "task.dispatch" {
            let a = tasks::get_attempt(&tx, model::text(&input, "attempt_id")?)?;
            let t = tasks::get_task(&tx, model::text(&a, "task_id")?)?;
            if !a["released_at_ms"].is_null()
                || a["state"] != "reserved"
                || a["start_owner"] != "controller"
                || a["start_operation_id"] != op
                || a["binding_id"] != id
                || a["binding_generation"] != generation
                || t["state"] != "open"
                || t["revision"] != a["task_revision"]
            {
                return Err(Error::new(
                    "STALE_ASSIGNMENT",
                    "task changed before native admission",
                ));
            }
            if caller["role"] != "operator"
                && (caller["role"] != "manager" || o["caller_id"] != a["owner_id"])
            {
                return Err(Error::new(
                    "FORBIDDEN",
                    "original caller no longer owns this task",
                ));
            }
        }
        Ok(())
    })();
    if let Err(e) = guard {
        let now = model::now_ms()?;
        tx.execute("UPDATE operations SET state='rejected',result_json=?2,settled_at_ms=?3,updated_at_ms=?3 WHERE operation_id=?1 AND state='queued'",params![op,model::canonical(&json!(e))?,now])?;
        tx.commit()?;
        return Ok(json!({"command":null,"rejected_operation_id":op,"error":e}));
    }
    let now = model::now_ms()?;
    let won=tx.execute("UPDATE operations SET state='sending',sent_at_ms=?2,updated_at_ms=?2 WHERE operation_id=?1 AND state='queued'",params![op,now])?;
    if won != 1 {
        return Err(Error::conflict("dispatch already admitted"));
    }
    if method == "task.dispatch" {
        let a = tasks::get_attempt(&tx, model::text(&input, "attempt_id")?)?;
        input["task_snapshot"] = a["task_snapshot"].clone();
    }
    let command = RuntimeCommand {
        operation_id: op,
        method,
        created_at_ms: created,
        binding_id: id,
        generation,
        native_root_id: b["native_root_id"].as_str().map(str::to_owned),
        route: b["route"].clone(),
        input,
    };
    tx.commit()?; // Never return a command while SQLite can still roll back admission.
    Ok(json!({"command":command}))
}

pub(super) fn outcome(db: &mut Connection, p: &Principal, v: &Value) -> Result<Value> {
    let r: RuntimeOutcome = serde_json::from_value(v.clone())?;
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let (id, generation, b) = scope(&tx, p, true)?;
    let o = operations::get_operation(&tx, &r.operation_id)?;
    if o["binding_id"] != id || o["binding_generation"] != generation {
        return Err(Error::new(
            "FORBIDDEN",
            "operation belongs to another binding",
        ));
    }
    let encoded = model::canonical(&json!(r))?;
    let key = format!("outcome:{}", r.operation_id);
    let previous:Option<String>=tx.query_row("SELECT payload_json FROM observations WHERE source_stream_id=?1 AND source_event_key=?2",params![format!("module:{}",p.client_id),key],|x|x.get(0)).optional()?;
    if let Some(previous) = previous {
        if previous != encoded {
            return Err(Error::conflict(
                "outcome already recorded with different evidence",
            ));
        }
        return Ok(json!({"recorded":true,"replayed":true}));
    }
    if !matches!(
        o["state"].as_str(),
        Some("sending" | "native_accepted" | "outcome_unknown")
    ) {
        return Err(Error::conflict(
            "operation was not admitted for native execution",
        ));
    }
    let now = model::now_ms()?;
    let state = match r.outcome {
        EffectOutcome::Applied => "settled",
        EffectOutcome::Rejected => "rejected",
        EffectOutcome::Unknown => "outcome_unknown",
    };
    if matches!(r.outcome, EffectOutcome::Applied) {
        if o["method"] == "agent.open" {
            let native = r
                .native_root_id
                .as_deref()
                .filter(|x| !x.is_empty())
                .ok_or_else(|| Error::invalid("open result requires native_root_id"))?;
            let namespace = r
                .native_scope_key
                .as_deref()
                .filter(|x| !x.is_empty())
                .ok_or_else(|| Error::invalid("open result requires native_scope_key"))?;
            if !b["native_root_id"].is_null()
                && (b["native_root_id"] != native || b["native_scope_key"] != namespace)
            {
                return Err(Error::conflict("native identity changed"));
            }
            tx.execute("UPDATE bindings SET native_root_id=?3,native_scope_key=?4,state='ready',state_json=json_set(state_json,'$.waiting_for',NULL,'$.opening_evidence',json(?5)) WHERE binding_id=?1 AND generation=?2",params![id,generation,native,namespace,model::canonical(&r.details)?])?;
        } else if o["method"] == "task.dispatch" {
            let turn = r
                .turn_id
                .as_deref()
                .filter(|x| !x.is_empty())
                .ok_or_else(|| Error::invalid("dispatch admission needs the native turn ID"))?;
            let attempt = model::text(&o, "attempt_id")?;
            let producer = json!({"assignment_id":r.operation_id,"native_session_id":b["native_root_id"],"native_run_id":turn,"disposition":"admitted"});
            tx.execute("UPDATE attempts SET state='running',producers_json=json_insert(producers_json,'$[#]',json(?2)),updated_at_ms=?3 WHERE attempt_id=?1 AND released_at_ms IS NULL",params![attempt,model::canonical(&producer)?,now])?;
        }
    } else if o["method"] == "agent.open" {
        // A failure may follow spawn: preserve ownership and its known native identity.
        tx.execute("UPDATE bindings SET state='reconciling',native_root_id=COALESCE(?3,native_root_id),native_scope_key=COALESCE(?4,native_scope_key),state_json=json_set(state_json,'$.opening_evidence',json(?5)) WHERE binding_id=?1 AND generation=?2",params![id,generation,r.native_root_id,r.native_scope_key,model::canonical(&r.details)?])?;
    }
    tx.execute("UPDATE operations SET state=?2,result_json=?3,native_refs_json=?4,settled_at_ms=?5,updated_at_ms=?6 WHERE operation_id=?1",params![r.operation_id,state,encoded,model::canonical(&json!({"session_id":r.native_root_id,"turn_id":r.turn_id}))?,if state=="outcome_unknown"{None}else{Some(now)},now])?;
    tx.execute("INSERT INTO observations(source_stream_id,source_event_key,binding_id,binding_generation,operation_id,kind,payload_json,recorded_at_ms) VALUES(?1,?2,?3,?4,?5,'runtime.outcome',?6,?7)",params![format!("module:{}",p.client_id),key,id,generation,r.operation_id,encoded,now])?;
    tx.commit()?;
    Ok(json!({"recorded":true}))
}

pub(super) fn observe(db: &mut Connection, p: &Principal, v: &Value) -> Result<Value> {
    model::fields(v, &["event_id", "state"])?;
    let event = model::text(v, "event_id")?;
    if !v["state"].is_object() {
        return Err(Error::invalid(
            "state must be a compact native observation object",
        ));
    }
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let (id, generation, _) = scope(&tx, p, true)?;
    let now = model::now_ms()?;
    let inserted=tx.execute("INSERT INTO observations(source_stream_id,source_event_key,binding_id,binding_generation,kind,payload_json,recorded_at_ms) VALUES(?1,?2,?3,?4,'runtime.state',?5,?6) ON CONFLICT DO NOTHING",params![format!("module:{}",p.client_id),event,id,generation,model::canonical(&v["state"])?,now])?;
    if inserted == 1 {
        tx.execute("UPDATE bindings SET state_json=json_set(state_json,'$.native',json(?3),'$.observed_at_ms',?4) WHERE binding_id=?1 AND generation=?2",params![id,generation,model::canonical(&v["state"])?,now])?;
    }
    if inserted == 1
        && let Some(turns) = v["state"]["turns"].as_array()
    {
        let mut stmt = tx.prepare("SELECT attempt_id, producers_json FROM attempts WHERE binding_id=?1 AND binding_generation=?2 AND released_at_ms IS NULL")?;
        let rows = stmt
            .query_map(params![id, generation], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(stmt);
        for (attempt, raw) in rows {
            let mut producers: Vec<Value> = serde_json::from_str(&raw)?;
            for producer in &mut producers {
                for turn in turns {
                    if producer["native_session_id"] == turn["sessionId"]
                        && producer["native_run_id"] == turn["turnId"]
                        && matches!(
                            turn["terminal"].as_str(),
                            Some("completed" | "failed" | "cancelled")
                        )
                    {
                        producer["disposition"] = turn["terminal"].clone();
                    }
                }
            }
            tx.execute(
                "UPDATE attempts SET producers_json=?2 WHERE attempt_id=?1",
                params![attempt, model::canonical(&json!(producers))?],
            )?;
        }
    }
    tx.commit()?;
    Ok(json!({"recorded":true,"replayed":inserted==0}))
}

pub(super) fn disconnected(db: &Connection, p: &Principal) -> Result<()> {
    if p.role != Role::Module {
        return Ok(());
    }
    let Some(c) = meta(db, &format!("client:{}", p.client_id))? else {
        return Ok(());
    };
    db.execute("UPDATE bindings SET state=CASE WHEN state='ready' THEN 'reconciling' ELSE state END,state_json=json_set(state_json,'$.connection','disconnected') WHERE binding_id=?1 AND generation=?2 AND json_extract(state_json,'$.module_link_id')=?3 AND released_at_ms IS NULL",params![c["binding_id"].as_str(),c["binding_generation"].as_i64(),p.link_id])?;
    Ok(())
}

pub(super) fn user_command(
    tx: &Connection,
    p: &Principal,
    method: &str,
    v: &Value,
    op: &str,
) -> Result<Value> {
    let allowed = if method == "agent.send" {
        vec![
            "client_request_id",
            "binding_id",
            "generation",
            "text",
            "delivery",
            "expected_turn_id",
        ]
    } else {
        vec!["client_request_id", "binding_id", "generation", "reply"]
    };
    model::fields(v, &allowed)?;
    let id = model::text(v, "binding_id")?;
    let generation = model::positive(v, "generation")?;
    let b = operations::get_binding(tx, id, generation)?;
    if b["state"] != "ready" {
        return Err(Error::new(
            "BINDING_NOT_READY",
            "native session is not ready",
        ));
    }
    if p.role != Role::Operator {
        let owns:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM attempts WHERE owner_id=?1 AND binding_id=?2 AND binding_generation=?3 AND released_at_ms IS NULL)",params![p.client_id,id,generation],|r|r.get(0))?;
        if !owns {
            return Err(Error::new("FORBIDDEN", "no assignment on this binding"));
        }
    }
    if method == "agent.send" {
        model::text(v, "text")?;
        match model::text(v, "delivery")? {
            "next_turn" => {}
            "steer" => {
                model::text(v, "expected_turn_id")?;
            }
            _ => return Err(Error::invalid("delivery must be next_turn or steer")),
        }
    } else if !v["reply"].is_object() {
        return Err(Error::invalid("reply object required"));
    }
    tx.execute("UPDATE operations SET binding_id=?2,binding_generation=?3,effective_request_json=?4 WHERE operation_id=?1",params![op,id,generation,model::canonical(&json!({"route":b["route"],"native_root_id":b["native_root_id"]}))?])?;
    Ok(json!({"operation_id":op,"state":"queued","native_admission":"not_observed"}))
}
