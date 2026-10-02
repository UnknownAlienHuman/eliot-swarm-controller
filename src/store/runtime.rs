//! Module admission and facts, scoped by a credential to one reserved native root.
//! Network I/O is never performed inside these transactions.
use super::{meta, operations, prerequisites, producers, tasks};
use crate::{
    error::{Error, Result},
    model::{self, Principal, Role},
    runtime::{EffectOutcome, RuntimeCommand, RuntimeOutcome},
};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde_json::{Value, json};

pub(super) fn scope(
    db: &Connection,
    p: &Principal,
    check_link: bool,
) -> Result<(String, i64, Value)> {
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
    if b["observation"]["module_client_id"] != p.client_id {
        return Err(Error::new(
            "MODULE_OWNER_MISMATCH",
            "credential does not own this module binding",
        ));
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

pub(super) fn hello_plan(db: &Connection, p: &Principal, v: &Value) -> Result<Value> {
    let (_, _, b) = scope(db, p, false)?;
    let changed = b["observation"]["bridge_boot_id"]
        .as_str()
        .is_some_and(|old| Some(old) != v["boot_id"].as_str());
    Ok(json!({"old_boot":b["observation"]["bridge_boot_id"],
        "owner":b["observation"]["managed_owner"],"changed":changed}))
}

pub(super) fn hello(
    db: &mut Connection,
    p: &Principal,
    v: &Value,
    verified: &Value,
) -> Result<Value> {
    model::fields(
        v,
        &[
            "boot_id",
            "module_artifact_id",
            "native_root_id",
            "native_scope_key",
            "native_ready",
            "managed_owner",
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
    if verified["old_boot"] != b["observation"]["bridge_boot_id"]
        || verified["owner"] != b["observation"]["managed_owner"]
    {
        return Err(Error::new(
            "STALE_RECOVERY",
            "module owner changed during OS inspection",
        ));
    }
    if old_boot == Some(boot) && v.get("managed_owner") != b["observation"].get("managed_owner") {
        return Err(Error::new(
            "OWNER_MISMATCH",
            "a live bridge cannot replace its process owner",
        ));
    }
    let recovered = old_boot.is_some_and(|old| old != boot) && verified["departed"] == true;
    let mut needs_recovery = false;
    if old_boot.is_some_and(|old| old != boot) {
        let possible:i64=tx.query_row("SELECT count(*) FROM operations WHERE binding_id=?1 AND binding_generation=?2 AND state IN ('sending','native_accepted','outcome_unknown','settled') AND method IN ('agent.open','task.dispatch','agent.send','agent.reply','agent.configure','agent.goal')",params![id,generation],|r|r.get(0))?;
        needs_recovery = possible > 0 || !b["native_root_id"].is_null();
        if needs_recovery && !recovered {
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
    if recovered && v.get("managed_owner").is_none() {
        return Err(Error::new(
            "MANAGED_OWNER_REQUIRED",
            "recovery requires the non-killing module launcher",
        ));
    }
    if recovered && needs_recovery {
        tx.execute("UPDATE bindings SET state='reconciling',state_json=json_set(state_json,'$.recovery_required',json('true'),'$.previous_bridge_boot_id',?3) WHERE binding_id=?1 AND generation=?2", params![id,generation,old_boot])?;
        tx.execute("UPDATE operations SET state='outcome_unknown',updated_at_ms=?3 WHERE binding_id=?1 AND binding_generation=?2 AND state IN ('sending','native_accepted')",params![id,generation,model::now_ms()?])?;
    }
    if let Some(owner) = v.get("managed_owner") {
        tx.execute("UPDATE bindings SET state_json=json_set(state_json,'$.managed_owner',json(?3)) WHERE binding_id=?1 AND generation=?2",params![id,generation,model::canonical(owner)?])?;
    }
    tx.execute("UPDATE bindings SET state_json=json_set(state_json,'$.bridge_boot_id',?3,'$.module_link_id',?4,'$.connection','connected','$.connected_at_ms',?5),state=CASE WHEN native_root_id IS NOT NULL AND state='reconciling' AND ?6 AND COALESCE(json_extract(state_json,'$.recovery_required'),0)=0 THEN 'ready' ELSE state END WHERE binding_id=?1 AND generation=?2", params![id,generation,boot,p.link_id,model::now_ms()?,v["native_ready"]==true])?;
    let result = json!({"binding_id":id,"generation":generation,"route":b["route"],"host_epoch":meta(&tx,"host_epoch")?,"native_root_id":b["native_root_id"],"native_scope_key":b["native_scope_key"],"recovery_required":(recovered && needs_recovery) || b["observation"]["recovery_required"]==true});
    tx.commit()?;
    Ok(result)
}

pub(super) fn next(db: &mut Connection, p: &Principal) -> Result<Value> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let (id, generation, b) = scope(&tx, p, true)?;
    if !matches!(
        b["state"].as_str(),
        Some("opening" | "ready" | "reconciling")
    ) {
        return Ok(json!({"command":null}));
    }
    // Readback, replies and continuation-stop controls stay available while an
    // ordinary mutation awaits application. They do not spawn another executor.
    let (op, method, raw, created) = {
        let row:Option<(String,String,String,i64)>=tx.query_row(
            "SELECT operation_id,method,original_request_json,created_at_ms FROM operations AS candidate
             WHERE binding_id=?1 AND binding_generation=?2 AND state='queued' AND due_at_ms<=?3
               AND (COALESCE(json_extract(?4,'$.recovery_required'),0)=0 OR method IN ('agent.recover','agent.reconcile'))
               AND method IN ('agent.open','task.dispatch','agent.send','agent.reply','agent.configure','agent.goal','agent.refresh','agent.reconcile','agent.result','agent.recover')
               AND (method IN ('agent.reply','agent.refresh','agent.reconcile','agent.result','agent.recover')
                 OR (method='agent.send' AND json_extract(original_request_json,'$.delivery')='steer')
                 OR (method='agent.goal' AND json_extract(original_request_json,'$.action') IN ('pause','clear'))
                 OR NOT EXISTS (SELECT 1 FROM operations AS pending
                   WHERE pending.binding_id=?1 AND pending.binding_generation=?2
                     AND pending.state IN ('sending','native_accepted','outcome_unknown')
                     AND pending.method IN ('agent.open','task.dispatch','agent.send','agent.configure','agent.goal','agent.recover')))
             ORDER BY CASE WHEN method='agent.reply' THEN 0
                           WHEN method='agent.send' AND json_extract(original_request_json,'$.delivery')='steer' THEN 1
                           WHEN method='agent.goal' AND json_extract(original_request_json,'$.action') IN ('pause','clear') THEN 1
                           WHEN method IN ('agent.refresh','agent.reconcile','agent.result','agent.recover') THEN 2 ELSE 3 END, due_at_ms, rowid LIMIT 1",
            params![id,generation,model::now_ms()?,model::canonical(&b["observation"])?],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
        let Some((op, method, raw, created)) = row else {
            return Ok(json!({"command":null}));
        };
        match prerequisites::for_operation(&tx, &b, &op)? {
            prerequisites::Gate::None | prerequisites::Gate::Ready { .. } => {
                (op, method, raw, created)
            }
            prerequisites::Gate::Pending {
                operation_id,
                blocking_operation_id,
                ..
            } => {
                return Ok(json!({
                    "command":null,
                    "waiting_operation_id":op,
                    "waiting_for_prerequisite_operation_id":operation_id,
                    "blocking_configuration_operation_id":blocking_operation_id
                }));
            }
            prerequisites::Gate::Failed(error) => {
                let now = model::now_ms()?;
                let result = json!({
                    "error":error,
                    "reason":"prerequisite_unsatisfied"
                });
                let changed = tx.execute(
                    "UPDATE operations SET state='rejected',result_json=?2,settled_at_ms=?3,updated_at_ms=?3 WHERE operation_id=?1 AND state='queued'",
                    params![op, model::canonical(&result)?, now],
                )?;
                if changed != 1 {
                    return Err(Error::conflict(
                        "prerequisite-dependent operation changed before rejection",
                    ));
                }
                tx.commit()?;
                return Ok(json!({
                    "command":null,
                    "rejected_operation_id":op,
                    "error":result["error"]
                }));
            }
        }
    };
    let mut input: Value = serde_json::from_str(&raw)?;
    let guard = (|| -> Result<()> {
        let o = operations::get_operation(&tx, &op)?;
        let caller = meta(&tx, &format!("client:{}", model::text(&o, "caller_id")?))?
            .ok_or_else(|| Error::new("UNAUTHORIZED", "original caller no longer registered"))?;
        if caller["disabled"] == true {
            return Err(Error::new("UNAUTHORIZED", "original caller disabled"));
        }
        let reconcile_starts_work = if method == "agent.reconcile"
            && b["route"]["runtime"] != crate::runtime::opencode_v2::RUNTIME
        {
            let target = operations::get_operation(&tx, model::text(&input, "operation_id")?)?;
            matches!(
                target["method"].as_str(),
                Some("task.dispatch" | "agent.send" | "agent.goal" | "agent.recover")
            )
        } else {
            false
        };
        let starts_work = reconcile_starts_work
            || method == "agent.open"
            || method == "agent.recover"
            || method == "task.dispatch"
            || (method == "agent.send" && input["delivery"] == "next_turn")
            || (method == "agent.goal"
                && matches!(input["action"].as_str(), Some("set" | "edit" | "resume")));
        if starts_work
            && meta(&tx, "execution_mode")?.unwrap_or(Value::Null)["new_work"] != "enabled"
        {
            return Err(Error::new(
                "ADMISSION_DISABLED",
                "new work disabled before dispatch",
            ));
        }
        if method == "agent.recover"
            && (caller["role"] != "operator"
                || input["expected_boot_id"] != b["observation"]["bridge_boot_id"]
                || b["observation"]["recovery_required"] != true)
        {
            return Err(Error::new(
                "STALE_RECOVERY",
                "recovery must target this unresolved bridge boot",
            ));
        }
        if method == "agent.open" {
            if b["state"] != "opening" || !b["native_root_id"].is_null() {
                return Err(Error::conflict("root already opened or changed"));
            }
        } else if b["state"] != "ready" && !is_recovery_control(&method, &input, &b) {
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
    if o["method"] != "agent.open"
        && (r
            .native_root_id
            .as_ref()
            .is_some_and(|n| b["native_root_id"] != *n)
            || r.native_scope_key
                .as_ref()
                .is_some_and(|n| b["native_scope_key"] != *n))
    {
        return Err(Error::new(
            "NATIVE_IDENTITY_MISMATCH",
            "outcome names another native root",
        ));
    }
    let encoded = model::canonical(&json!(r))?;
    let stream = format!("module:{}", p.client_id);
    let previous:bool=tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM observations WHERE source_stream_id=?1 AND operation_id=?2 AND kind='runtime.outcome' AND payload_json=?3)",
        params![stream,r.operation_id,encoded],|x|x.get(0))?;
    if previous {
        return Ok(json!({"recorded":true,"replayed":true,"state":o["state"]}));
    }
    // A saved unknown is not immutable failure: a later native fact may resolve
    // it. Conversely, late admission/unknown cannot roll a terminal result back.
    let terminal = matches!(o["state"].as_str(), Some("settled" | "rejected"));
    let superseded = (terminal
        && matches!(r.outcome, EffectOutcome::Accepted | EffectOutcome::Unknown))
        || (o["state"] == "native_accepted" && matches!(r.outcome, EffectOutcome::Unknown));
    if !superseded
        && !matches!(
            o["state"].as_str(),
            Some("sending" | "native_accepted" | "outcome_unknown")
        )
    {
        return Err(Error::conflict(
            "cannot replace a known native result or execute an unadmitted operation",
        ));
    }
    let now = model::now_ms()?;
    let result_value = serde_json::to_value(&r)?;
    let applied_configuration = if o["method"] == "agent.configure"
        && b["route"]["runtime"] == crate::runtime::opencode_v2::RUNTIME
        && matches!(r.outcome, EffectOutcome::Applied)
    {
        Some(prerequisites::applied_configuration(
            &tx,
            &b,
            &r.operation_id,
            &result_value,
            now,
        )?)
    } else {
        None
    };
    let key = format!(
        "outcome:{}:{}",
        r.operation_id,
        model::digest(encoded.as_bytes())
    );
    if superseded {
        tx.execute("INSERT INTO observations(source_stream_id,source_event_key,binding_id,binding_generation,operation_id,kind,payload_json,recorded_at_ms) VALUES(?1,?2,?3,?4,?5,'runtime.outcome',?6,?7)",params![stream,key,id,generation,r.operation_id,encoded,now])?;
        tx.commit()?;
        return Ok(json!({"recorded":true,"superseded":true,"state":o["state"]}));
    }
    let state = match r.outcome {
        EffectOutcome::Accepted => "native_accepted",
        EffectOutcome::Applied => "settled",
        EffectOutcome::Rejected => "rejected",
        EffectOutcome::Unknown => "outcome_unknown",
    };
    if o["method"] == "agent.result" && matches!(r.outcome, EffectOutcome::Applied) {
        return Err(Error::invalid(
            "result pages require module.result and durable artifact publication",
        ));
    }
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
            tx.execute("UPDATE bindings SET native_root_id=?3,native_scope_key=?4,state=CASE WHEN json_extract(state_json,'$.recovery_required')=1 THEN 'reconciling' ELSE 'ready' END,state_json=json_set(state_json,'$.waiting_for',NULL,'$.opening_evidence',json(?5)) WHERE binding_id=?1 AND generation=?2",params![id,generation,native,namespace,model::canonical(&r.details)?])?;
        } else if o["method"] == "task.dispatch" {
            let attempt = model::text(&o, "attempt_id")?;
            let mut producer = json!({"assignment_id":r.operation_id,"native_session_id":b["native_root_id"],"disposition":"admitted"});
            match (r.turn_id.as_deref(), r.native_input_id.as_deref()) {
                (Some(turn), None) if !turn.is_empty() => producer["native_run_id"] = json!(turn),
                (None, Some(input))
                    if !input.is_empty()
                        && r.details["completion_condition"] == "native_input_admitted" =>
                {
                    // A durable inbox ID is not a turn ID. Keep this producer
                    // unresolved until actual execution/disposition is evidenced.
                    producer["native_input_id"] = json!(input);
                    producer["admission_kind"] = json!("native_inbox");
                }
                _ => {
                    return Err(Error::invalid(
                        "dispatch admission needs an exact native turn or explicit inbox receipt",
                    ));
                }
            }
            // A short turn can finish before its admission response arrives.
            // Reuse already-recorded exact-run evidence instead of waiting for
            // another notification which may never be emitted.
            producers::apply_evidence(
                &mut producer,
                &b["observation"]["native"],
                b["observation"]["native_observation_id"].as_i64(),
            );
            tx.execute("UPDATE attempts SET state=CASE WHEN state='reserved' THEN 'running' ELSE state END,producers_json=json_insert(producers_json,'$[#]',json(?2)),updated_at_ms=?3 WHERE attempt_id=?1 AND released_at_ms IS NULL",params![attempt,model::canonical(&producer)?,now])?;
        }
    } else if o["method"] == "agent.open" && !matches!(r.outcome, EffectOutcome::Accepted) {
        // A failure may follow spawn: preserve ownership and its known native identity.
        tx.execute("UPDATE bindings SET state='reconciling',native_root_id=COALESCE(?3,native_root_id),native_scope_key=COALESCE(?4,native_scope_key),state_json=json_set(state_json,'$.opening_evidence',json(?5)) WHERE binding_id=?1 AND generation=?2",params![id,generation,r.native_root_id,r.native_scope_key,model::canonical(&r.details)?])?;
    }
    if let Some(configuration) = applied_configuration {
        match configuration {
            prerequisites::EffectiveConfiguration::InstructionEntries(settings) => {
                tx.execute(
                    "UPDATE bindings SET state_json=json_set(state_json,'$.effective_settings',json(?3)) WHERE binding_id=?1 AND generation=?2",
                    params![id, generation, model::canonical(&settings)?],
                )?;
            }
            prerequisites::EffectiveConfiguration::SessionAgent(agent) => {
                tx.execute(
                    "UPDATE bindings SET state_json=json_set(state_json,'$.effective_agent',json(?3)) WHERE binding_id=?1 AND generation=?2",
                    params![id, generation, model::canonical(&agent)?],
                )?;
            }
        }
    }
    if o["method"] == "agent.recover" && matches!(r.outcome, EffectOutcome::Applied) {
        if r.native_root_id.as_deref() != b["native_root_id"].as_str()
            || r.native_scope_key.as_deref() != b["native_scope_key"].as_str()
            || r.details["completion_condition"] != "native_session_resumed"
        {
            return Err(Error::invalid(
                "recovery must confirm the exact retained native session",
            ));
        }
        if r.details["resume_boot_id"] == b["observation"]["bridge_boot_id"] {
            tx.execute("UPDATE bindings SET state='ready',state_json=json_set(state_json,'$.recovery_required',json('false'),'$.last_recovery',json(?3)) WHERE binding_id=?1 AND generation=?2",params![id,generation,encoded])?;
        }
    }
    tx.execute("UPDATE operations SET state=?2,result_json=?3,native_refs_json=?4,settled_at_ms=?5,updated_at_ms=?6 WHERE operation_id=?1",params![r.operation_id,state,encoded,model::canonical(&json!({"session_id":r.native_root_id,"turn_id":r.turn_id,"input_id":r.native_input_id}))?,if matches!(state,"outcome_unknown"|"native_accepted"){None}else{Some(now)},now])?;
    tx.execute("INSERT INTO observations(source_stream_id,source_event_key,binding_id,binding_generation,operation_id,kind,payload_json,recorded_at_ms) VALUES(?1,?2,?3,?4,?5,'runtime.outcome',?6,?7)",params![format!("module:{}",p.client_id),key,id,generation,r.operation_id,encoded,now])?;
    tx.commit()?;
    Ok(json!({"recorded":true}))
}

pub(super) fn observe(db: &mut Connection, p: &Principal, v: &Value) -> Result<Value> {
    model::fields(v, &["event_id", "sequence", "state"])?;
    let event = model::text(v, "event_id")?;
    let sequence = v
        .get("sequence")
        .map(|n| {
            n.as_i64()
                .filter(|n| *n >= 0)
                .ok_or_else(|| Error::invalid("sequence must be a nonnegative integer"))
        })
        .transpose()?;
    if !v["state"].is_object() {
        return Err(Error::invalid(
            "state must be a compact native observation object",
        ));
    }
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let (id, generation, b) = scope(&tx, p, true)?;
    if v["state"]["boot_id"] != b["observation"]["bridge_boot_id"] {
        return Err(Error::new(
            "STALE_BOOT",
            "observation is not from the active module boot",
        ));
    }
    let now = model::now_ms()?;
    let encoded = model::canonical(&v["state"])?;
    let previous:Option<String> = tx.query_row("SELECT payload_json FROM observations WHERE source_stream_id=?1 AND source_event_key=?2",params![format!("module:{}",p.client_id),event],|r|r.get(0)).optional()?;
    if let Some(previous) = previous {
        if previous != encoded {
            return Err(Error::conflict("observation ID reused for different state"));
        }
        return Ok(json!({"recorded":true,"replayed":true}));
    }
    let prior = &b["observation"]["native"];
    let last_sequence = b["observation"]["native_sequence"].as_i64();
    // Existing live modules may omit the additive sequence field. They retain
    // their legacy partial observation path, never overwrite an ordered stream,
    // and do not acquire the newer monotonic-projection guarantee.
    let current = prior["boot_id"] != v["state"]["boot_id"]
        || match (sequence, last_sequence) {
            (Some(n), Some(last)) => n > last,
            (None, Some(_)) => false,
            (_, None) => true,
        };
    tx.execute("INSERT INTO observations(source_stream_id,source_event_key,binding_id,binding_generation,kind,payload_json,recorded_at_ms) VALUES(?1,?2,?3,?4,'runtime.state',?5,?6)",params![format!("module:{}",p.client_id),event,id,generation,encoded,now])?;
    let observation_id = tx.last_insert_rowid();
    if !current {
        tx.commit()?;
        return Ok(json!({"recorded":true,"stale":true}));
    }
    let observed_configurations = if b["route"]["runtime"] == crate::runtime::opencode_v2::RUNTIME {
        prerequisites::observed_configurations(&b, &v["state"], observation_id, now)?
    } else {
        Vec::new()
    };
    tx.execute("UPDATE bindings SET state_json=json_set(state_json,'$.native',json(?3),'$.observed_at_ms',?4,'$.native_sequence',?5,'$.native_observation_id',?6) WHERE binding_id=?1 AND generation=?2",params![id,generation,encoded,now,sequence,observation_id])?;
    for configuration in observed_configurations {
        match configuration {
            prerequisites::EffectiveConfiguration::InstructionEntries(settings) => {
                tx.execute(
                    "UPDATE bindings SET state_json=json_set(state_json,'$.effective_settings',json(?3)) WHERE binding_id=?1 AND generation=?2",
                    params![id, generation, model::canonical(&settings)?],
                )?;
            }
            prerequisites::EffectiveConfiguration::SessionAgent(agent) => {
                tx.execute(
                    "UPDATE bindings SET state_json=json_set(state_json,'$.effective_agent',json(?3)) WHERE binding_id=?1 AND generation=?2",
                    params![id, generation, model::canonical(&agent)?],
                )?;
            }
        }
    }
    if v["state"]["turns"].is_array() || v["state"]["observed_children"].is_array() {
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
                producers::apply_evidence(producer, &v["state"], Some(observation_id));
            }
            tx.execute(
                "UPDATE attempts SET producers_json=?2 WHERE attempt_id=?1",
                params![attempt, model::canonical(&json!(producers))?],
            )?;
        }
    }
    tx.commit()?;
    Ok(json!({"recorded":true,"stale":false,"observation_id":observation_id}))
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

fn is_recovery_control(method: &str, input: &Value, binding: &Value) -> bool {
    binding["state"] == "reconciling"
        && binding["native_root_id"].is_string()
        && (matches!(
            method,
            "agent.refresh" | "agent.reply" | "agent.reconcile" | "agent.result" | "agent.recover"
        ) || (method == "agent.goal"
            && matches!(input["action"].as_str(), Some("pause" | "clear"))))
}

pub(super) fn user_command(
    tx: &Connection,
    p: &Principal,
    method: &str,
    v: &Value,
    op: &str,
) -> Result<Value> {
    // Envelope shape is validated before persistence by model::validate_mutation.
    let id = model::text(v, "binding_id")?;
    let generation = model::positive(v, "generation")?;
    let b = operations::get_binding(tx, id, generation)?;
    if b["state"] != "ready" && !is_recovery_control(method, v, &b) {
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
    if method == "agent.recover" {
        p.require_operator()?;
        model::text(v, "reason")?;
        if v["expected_boot_id"] != b["observation"]["bridge_boot_id"]
            || b["observation"]["recovery_required"] != true
        {
            return Err(Error::new(
                "STALE_RECOVERY",
                "use the current boot of a recovery-required binding",
            ));
        }
    }
    if method == "agent.result" && v["selector"].as_object().is_none_or(|o| o.is_empty()) {
        return Err(Error::invalid("result selector object required"));
    }
    if method == "agent.send" {
        model::text(v, "text")?;
        match model::text(v, "delivery")? {
            "next_turn" => {}
            "steer" => {
                model::text(v, "expected_turn_id")?;
                if v.get("prerequisite_operation_id").is_some() {
                    return Err(Error::invalid("steer does not accept a setup prerequisite"));
                }
            }
            _ => return Err(Error::invalid("delivery must be next_turn or steer")),
        }
    } else if method == "agent.reply" && !v["reply"].is_object() {
        return Err(Error::invalid("reply object required"));
    } else if method == "agent.configure" {
        if v["settings"].as_object().is_none_or(|o| o.is_empty()) {
            return Err(Error::invalid("nonempty adapter settings object required"));
        }
        if b["route"]["runtime"] == crate::runtime::opencode_v2::RUNTIME {
            crate::runtime::opencode_v2::configuration_expectation(&v["settings"])?;
        }
    } else if method == "agent.goal" {
        match model::text(v, "action")? {
            "set" | "edit" => {
                model::text(v, "objective")?;
            }
            "pause" | "resume" | "clear" => {
                if v.get("objective").is_some() {
                    return Err(Error::invalid("objective is only valid for set/edit"));
                }
            }
            _ => {
                return Err(Error::invalid(
                    "goal action must be set/edit/pause/resume/clear",
                ));
            }
        }
    }
    if method == "agent.reconcile" {
        let target = operations::get_operation(tx, model::text(v, "operation_id")?)?;
        if target["binding_id"] != id
            || target["binding_generation"] != generation
            || !matches!(
                target["state"].as_str(),
                Some("sending" | "native_accepted" | "outcome_unknown")
            )
        {
            return Err(Error::invalid(
                "reconcile requires an unresolved operation on this exact binding",
            ));
        }
    }
    let prerequisite = prerequisites::validate_request(tx, &b, v, op)?;
    let prerequisite_id = prerequisite.operation_id().map(str::to_owned);
    let prerequisite_contract_revision = prerequisite.contract_revision().map(str::to_owned);
    if method == "agent.goal" && matches!(v["action"].as_str(), Some("pause" | "clear")) {
        // A pause received before dispatch also cancels old local goal starts.
        // Otherwise priority scheduling could execute pause first and revive the
        // goal later from its stale queued set/resume. Already-sent effects stay.
        let now = model::now_ms()?;
        tx.execute("UPDATE operations SET state='cancelled',result_json=?4,settled_at_ms=?3,updated_at_ms=?3 WHERE binding_id=?1 AND binding_generation=?2 AND state='queued' AND method='agent.goal' AND json_extract(original_request_json,'$.action') IN ('set','edit','resume')",
            params![id,generation,now,model::canonical(&json!({"reason":"superseded_by_goal_stop","stop_operation_id":op}))?])?;
    }
    let mut effective = json!({"route":b["route"],"native_root_id":b["native_root_id"]});
    if method == "agent.configure" && b["route"]["runtime"] == crate::runtime::opencode_v2::RUNTIME
    {
        effective["operation_contract"] =
            crate::runtime::opencode_v2::configuration_contract(&v["settings"], id, generation)?;
    } else if method == "agent.send"
        && v["delivery"] == "next_turn"
        && b["route"]["runtime"] == crate::runtime::opencode_v2::RUNTIME
    {
        effective["operation_contract"] = json!({
            "effect_scope":"native_session",
            "order_scope":{"binding_id":id,"generation":generation},
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
    tx.execute("UPDATE operations SET binding_id=?2,binding_generation=?3,prerequisite_operation_id=?4,effective_request_json=?5 WHERE operation_id=?1",params![op,id,generation,prerequisite_id,model::canonical(&effective)?])?;
    Ok(
        json!({"operation_id":op,"state":"queued","native_admission":"not_observed","prerequisite_operation_id":prerequisite_id,"prerequisite_state":prerequisite.receipt_state()}),
    )
}
