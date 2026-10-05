//! Module admission and facts, scoped by a credential to one reserved native root.
//! Network I/O is never performed inside these transactions.
use super::{Store, meta, operations, prerequisites, producers, set_meta, tasks};
use crate::{
    artifacts::ArtifactRecord,
    error::{Error, Result},
    model::{self, Principal, Role},
    runtime::{EffectOutcome, RuntimeCommand, RuntimeOutcome, batch, zed},
};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{sync::watch, task::JoinHandle};

fn batch_bindings(db: &Connection) -> Result<Vec<Value>> {
    let mut stmt = db.prepare(
        "SELECT binding_id,generation FROM bindings WHERE released_at_ms IS NULL AND route_json IS NOT NULL ORDER BY created_at_ms,binding_id",
    )?;
    let pairs = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut bindings = Vec::new();
    for (id, generation) in pairs {
        let binding = operations::get_binding(db, &id, generation)?;
        if binding["route"]["runtime"] == zed::RUNTIME
            && binding["route"]["module_artifact_id"] == zed::ARTIFACT_ID
        {
            bindings.push(binding);
        }
    }
    Ok(bindings)
}

fn attach_batch(db: &mut Connection, binding: &Value, boot: &str) -> Result<Principal> {
    let id = model::text(binding, "binding_id")?;
    let generation = model::positive(binding, "generation")?;
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let current = operations::get_binding(&tx, id, generation)?;
    if !current["released_at_ms"].is_null()
        || current["route"]["runtime"] != zed::RUNTIME
        || current["route"]["module_artifact_id"] != zed::ARTIFACT_ID
    {
        return Err(Error::new("BINDING_CLOSED", "built-in Zed binding changed"));
    }
    let client = format!("builtin:zed:{id}:{generation}");
    if let Some(existing) = meta(&tx, &format!("client:{client}"))? {
        if existing["builtin_runtime"] != zed::RUNTIME
            || existing["disabled"] == true
            || existing["binding_id"] != id
            || existing["binding_generation"] != generation
            || current["observation"]["module_client_id"] != client
        {
            return Err(Error::new(
                "MODULE_OWNER_MISMATCH",
                "refusing to replace another Zed module owner",
            ));
        }
    } else {
        if current["observation"].get("module_client_id").is_some() {
            return Err(Error::new(
                "MODULE_OWNER_MISMATCH",
                "binding already has another module credential",
            ));
        }
        register(
            &tx,
            &json!({"binding_id":id,"binding_generation":generation}),
            &client,
        )?;
        set_meta(
            &tx,
            &format!("client:{client}"),
            &json!({"role":"module","disabled":false,"builtin_runtime":zed::RUNTIME,"binding_id":id,"binding_generation":generation}),
        )?;
    }
    let principal = Principal {
        client_id: client,
        link_id: model::new_id(),
        role: Role::Module,
    };
    let now = model::now_ms()?;
    tx.execute(
        "UPDATE operations SET state='outcome_unknown',updated_at_ms=?3 WHERE binding_id=?1 AND binding_generation=?2 AND state IN ('sending','native_accepted') AND method IN ('agent.open','task.dispatch','agent.refresh','agent.reconcile','agent.result')",
        params![id, generation, now],
    )?;
    super::capacity::sync_binding(&tx, id, generation, now)?;
    tx.execute(
        "UPDATE bindings SET state=CASE WHEN COALESCE(json_extract(state_json,'$.recovery_required'),0)=1 OR EXISTS(SELECT 1 FROM operations WHERE binding_id=?1 AND binding_generation=?2 AND state IN ('sending','native_accepted','outcome_unknown') AND method IN ('agent.open','task.dispatch')) THEN 'reconciling' WHEN state='reconciling' THEN 'ready' ELSE state END,state_json=json_set(state_json,'$.bridge_boot_id',?3,'$.module_link_id',?4,'$.connection','connected','$.native_owner','controller_local_executor') WHERE binding_id=?1 AND generation=?2",
        params![id, generation, boot, principal.link_id],
    )?;
    tx.commit()?;
    Ok(principal)
}

fn batch_original(
    db: &Connection,
    principal: &Principal,
    operation_id: &str,
) -> Result<RuntimeCommand> {
    let (binding_id, generation, binding) = scope(db, principal, true)?;
    let operation = operations::get_operation(db, operation_id)?;
    if operation["binding_id"] != binding_id || operation["binding_generation"] != generation {
        return Err(Error::new(
            "FORBIDDEN",
            "batch operation belongs to another binding",
        ));
    }
    let raw: String = db.query_row(
        "SELECT original_request_json FROM operations WHERE operation_id=?1",
        [operation_id],
        |row| row.get(0),
    )?;
    let mut input: Value = serde_json::from_str(&raw)?;
    let input_sha256 = model::digest(model::canonical(&input)?.as_bytes());
    let method = model::text(&operation, "method")?.to_owned();
    if method == "task.dispatch" {
        let attempt = tasks::get_attempt(db, model::text(&input, "attempt_id")?)?;
        input["task_snapshot"] = attempt["task_snapshot"].clone();
    }
    Ok(RuntimeCommand {
        operation_id: operation_id.to_owned(),
        method,
        created_at_ms: model::positive(&operation, "created_at_ms")?,
        binding_id,
        generation,
        native_root_id: None,
        route: binding["route"].clone(),
        input,
        input_sha256: Some(input_sha256),
        target_input_sha256: None,
    })
}

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
    let module_contract_negotiation = super::module_handshake::negotiate_hello(
        db,
        model::text(&b, "module_artifact_id")?,
        b["observation"].get("module_contract_selector"),
        v.get("module_contract"),
    )?;
    let changed = b["observation"]["bridge_boot_id"]
        .as_str()
        .is_some_and(|old| Some(old) != v["boot_id"].as_str());
    Ok(json!({"old_boot":b["observation"]["bridge_boot_id"],
        "owner":b["observation"]["managed_owner"],"changed":changed,
        "module_contract_negotiation":module_contract_negotiation}))
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
            "module_contract",
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
    let module_contract_negotiation = super::module_handshake::negotiate_hello(
        &tx,
        model::text(&b, "module_artifact_id")?,
        b["observation"].get("module_contract_selector"),
        v.get("module_contract"),
    )?;
    if verified.get("module_contract_negotiation") != Some(&module_contract_negotiation) {
        return Err(Error::new(
            "STALE_MODULE_CONTRACT",
            "trusted module descriptor changed during hello preflight",
        ));
    }
    let sessionless_batch = crate::runtime::batch::is_sessionless_route(&b["route"]);
    if sessionless_batch
        && (v
            .get("native_root_id")
            .is_some_and(|value| !value.is_null())
            || v.get("native_scope_key")
                .is_some_and(|value| !value.is_null()))
    {
        return Err(Error::new(
            "NATIVE_IDENTITY_MISMATCH",
            "sessionless batch bindings cannot claim a native root or scope",
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
        let possible: i64 = if sessionless_batch {
            tx.query_row("SELECT count(*) FROM operations WHERE binding_id=?1 AND binding_generation=?2 AND state IN ('sending','native_accepted','outcome_unknown') AND method IN ('agent.open','task.dispatch','agent.refresh','agent.reconcile')",params![id,generation],|r|r.get(0))?
        } else {
            tx.query_row("SELECT count(*) FROM operations WHERE binding_id=?1 AND binding_generation=?2 AND state IN ('sending','native_accepted','outcome_unknown','settled') AND method IN ('agent.open','task.dispatch','agent.send','agent.reply','agent.configure','agent.goal')",params![id,generation],|r|r.get(0))?
        };
        needs_recovery = possible > 0 || (!sessionless_batch && !b["native_root_id"].is_null());
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
    if recovered && v.get("managed_owner").is_none() && !sessionless_batch {
        return Err(Error::new(
            "MANAGED_OWNER_REQUIRED",
            "recovery requires the non-killing module launcher",
        ));
    }
    if recovered && needs_recovery {
        tx.execute("UPDATE bindings SET state='reconciling',state_json=json_set(state_json,'$.recovery_required',json('true'),'$.previous_bridge_boot_id',?3) WHERE binding_id=?1 AND generation=?2", params![id,generation,old_boot])?;
        tx.execute("UPDATE operations SET state='outcome_unknown',updated_at_ms=?3 WHERE binding_id=?1 AND binding_generation=?2 AND state IN ('sending','native_accepted')",params![id,generation,model::now_ms()?])?;
        // The unknown outcomes free no capacity: the ledger marks the
        // entries unknown and keeps their phases (R23).
        super::capacity::sync_binding(&tx, &id, generation, model::now_ms()?)?;
    }
    if let Some(owner) = v.get("managed_owner") {
        tx.execute("UPDATE bindings SET state_json=json_set(state_json,'$.managed_owner',json(?3)) WHERE binding_id=?1 AND generation=?2",params![id,generation,model::canonical(owner)?])?;
    }
    let prepared_rootless_resume = if old_boot == Some(boot)
        && crate::runtime::prepared::is_prepared_claude_route(&b["route"])
        && b["native_root_id"].is_null()
        && b["native_scope_key"].is_null()
        && v["native_ready"] == true
        && b["observation"]["recovery_required"] != true
        && b["observation"]["opening_evidence"]["completion_condition"]
            == "native_executor_prepared"
        && b["observation"]["opening_evidence"]["native_session_state"] == "prepared"
        && b["observation"]["opening_evidence"]["bridge_boot_id"] == boot
    {
        !tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM operations WHERE binding_id=?1 AND binding_generation=?2 AND state IN ('sending','native_accepted','outcome_unknown') AND method IN ('agent.open','task.dispatch'))",
            params![id, generation],
            |row| row.get::<_, bool>(0),
        )?
    } else {
        false
    };
    tx.execute("UPDATE bindings SET state_json=json_set(state_json,'$.bridge_boot_id',?3,'$.module_link_id',?4,'$.connection','connected','$.connected_at_ms',?5),state=CASE WHEN state='reconciling' AND ?6 AND COALESCE(json_extract(state_json,'$.recovery_required'),0)=0 AND (native_root_id IS NOT NULL OR ?7 OR ?8) THEN 'ready' ELSE state END WHERE binding_id=?1 AND generation=?2", params![id,generation,boot,p.link_id,model::now_ms()?,v["native_ready"]==true,sessionless_batch,prepared_rootless_resume])?;
    let result = json!({"binding_id":id,"generation":generation,"route":b["route"],"host_epoch":meta(&tx,"host_epoch")?,"native_root_id":b["native_root_id"],"native_scope_key":b["native_scope_key"],"recovery_required":(recovered && needs_recovery) || b["observation"]["recovery_required"]==true,"module_contract_negotiation":module_contract_negotiation});
    tx.commit()?;
    Ok(result)
}

pub(super) fn next(db: &mut Connection, p: &Principal) -> Result<Value> {
    next_internal(db, p, None)
}

pub(super) fn next_with_config(
    db: &mut Connection,
    p: &Principal,
    config: &crate::config::Config,
) -> Result<Value> {
    next_internal(db, p, Some(config))
}

fn next_internal(
    db: &mut Connection,
    p: &Principal,
    config: Option<&crate::config::Config>,
) -> Result<Value> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let (id, generation, b) = scope(&tx, p, true)?;
    if crate::runtime::batch::is_legacy_command_route(&b["route"]) {
        // Artifact .2 is retained for historical operation reads only. Never
        // hand queued work to an old bridge after the .3 receipt contract ships.
        return Ok(json!({"command":null,"reason":"command_artifact_retired"}));
    }
    if !matches!(
        b["state"].as_str(),
        Some("opening" | "ready" | "reconciling")
    ) {
        return Ok(json!({"command":null}));
    }
    // Readback, replies and continuation-stop controls stay available while an
    // ordinary mutation awaits application. They do not spawn another executor.
    let (op, method, raw, created, opening_actor) = {
        let row:Option<(String,String,String,i64)>=tx.query_row(
            "SELECT operation_id,method,original_request_json,created_at_ms FROM operations AS candidate
             WHERE binding_id=?1 AND binding_generation=?2 AND state='queued' AND due_at_ms<=?3
               AND (COALESCE(json_extract(?4,'$.recovery_required'),0)=0 OR method IN ('agent.recover','agent.reconcile'))
               AND method IN ('agent.open','task.dispatch','agent.send','agent.reply','agent.configure','agent.goal','agent.background','agent.refresh','agent.reconcile','agent.result','agent.recover')
               AND (method IN ('agent.reply','agent.background','agent.refresh','agent.reconcile','agent.result','agent.recover')
                 OR (method='agent.send' AND json_extract(original_request_json,'$.delivery')='steer')
                 OR (method='agent.goal' AND json_extract(original_request_json,'$.action') IN ('pause','clear'))
                 OR NOT EXISTS (SELECT 1 FROM operations AS pending
                   WHERE pending.binding_id=?1 AND pending.binding_generation=?2
                     AND pending.state IN ('sending','native_accepted','outcome_unknown')
                     AND pending.method IN ('agent.open','task.dispatch','agent.send','agent.configure','agent.goal','agent.recover')))
             ORDER BY CASE WHEN method IN ('agent.reply','agent.background') THEN 0
                           WHEN method='agent.send' AND json_extract(original_request_json,'$.delivery')='steer' THEN 1
                           WHEN method='agent.goal' AND json_extract(original_request_json,'$.action') IN ('pause','clear') THEN 1
                           WHEN method IN ('agent.refresh','agent.reconcile','agent.result','agent.recover') THEN 2 ELSE 3 END, due_at_ms, rowid LIMIT 1",
            params![id,generation,model::now_ms()?,model::canonical(&b["observation"])?],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
        let Some((op, method, raw, created)) = row else {
            return Ok(json!({"command":null}));
        };
        let opening_actor = if method == "agent.open" {
            match super::launcher::opening_actor_for_open(&tx, &op, &id, generation) {
                Ok(actor) => actor,
                Err(error)
                    if matches!(
                        error.code.as_str(),
                        "STORE_ERROR" | "STORE_CLOSED" | "IO_ERROR" | "CLOCK_ERROR"
                    ) =>
                {
                    return Err(error);
                }
                Err(error) => {
                    // This validation precedes the ordinary dispatch guard.
                    // Retain a deterministic rejection instead of rolling back
                    // to queued work that the executor silently retries forever.
                    let now = model::now_ms()?;
                    let safe_error = Error::new(
                        error.code,
                        "opening launch validation failed before dispatch",
                    );
                    let changed = tx.execute(
                        "UPDATE operations SET state='rejected',result_json=?2,settled_at_ms=?3,updated_at_ms=?3 WHERE operation_id=?1 AND state='queued'",
                        params![op, model::canonical(&json!(safe_error))?, now],
                    )?;
                    if changed != 1 {
                        return Err(Error::conflict(
                            "opening operation changed before validation rejection",
                        ));
                    }
                    operations::record_owned_open_dispatch_failure(
                        &tx,
                        &id,
                        generation,
                        "opening_actor_validate",
                        "rejected_before_dispatch",
                        &safe_error.code,
                        "rejected",
                    )?;
                    tx.commit()?;
                    return Ok(json!({
                        "command":null,
                        "rejected_operation_id":op,
                        "error":safe_error
                    }));
                }
            }
        } else {
            None
        };
        let prerequisite = if opening_actor.is_some() {
            prerequisites::Gate::None
        } else {
            prerequisites::for_operation(&tx, &b, &op)?
        };
        match prerequisite {
            prerequisites::Gate::None | prerequisites::Gate::Ready { .. } => {
                (op, method, raw, created, opening_actor)
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
    let input_sha256 = model::digest(model::canonical(&input)?.as_bytes());
    let mut trusted_launch_dispatch_packet: Option<Value> = None;
    let guard = (|| -> Result<()> {
        let o = operations::get_operation(&tx, &op)?;
        let repair_context = if method == "agent.send"
            && o["caller_id"] == crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID
        {
            let context = super::automation_repair::context_for_delivery_operation(&tx, &op)?;
            context.require_current_for_effect(&tx, &op)?;
            Some(context)
        } else {
            None
        };
        let caller = if opening_actor.is_some() || repair_context.is_some() {
            // The exact opening guard above validated the retained actor in
            // this transaction. A technical requester is not a registered
            // client or a Principal; no synthetic profile is created here.
            Value::Null
        } else {
            let caller = meta(&tx, &format!("client:{}", model::text(&o, "caller_id")?))?
                .ok_or_else(|| {
                    Error::new("UNAUTHORIZED", "original caller no longer registered")
                })?;
            if caller["disabled"] == true {
                return Err(Error::new("UNAUTHORIZED", "original caller disabled"));
            }
            // Existing queued work must not retain an old remote Operator's
            // privilege after the local bootstrap identity has been anchored.
            if caller["role"] == "operator" {
                super::require_local_operator(&tx, model::text(&o, "caller_id")?)?;
            }
            caller
        };
        let reconcile_starts_work = if method == "agent.reconcile"
            && b["observation"].get("module_contract_selector").is_some()
        {
            let target = operations::get_operation(&tx, model::text(&input, "operation_id")?)?;
            if target["binding_id"] != id
                || target["binding_generation"] != generation
                || !matches!(
                    target["state"].as_str(),
                    Some("sending" | "native_accepted" | "outcome_unknown")
                )
                || operations::registered_module_recovery_contract(
                    &tx,
                    &b,
                    model::text(&target, "method")?,
                )?
                .is_none()
            {
                return Err(Error::new(
                    "MODULE_RECONCILIATION_INVALID",
                    "strict module reconciliation requires an eligible unresolved target on this binding generation",
                ));
            }
            // The strict v1 adapter contract is readback-only. Store admission
            // of this reconciliation never authorizes another native effect.
            false
        } else if method == "agent.reconcile"
            && !crate::runtime::batch::is_sessionless_route(&b["route"])
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
            if b["route"]
                .get("owned_service")
                .is_some_and(|v| !v.is_null())
            {
                let config = config.ok_or_else(|| {
                    Error::new(
                        "OWNED_SERVICE_SCOPE",
                        "owned open requires the current controller configuration",
                    )
                })?;
                super::launcher_owned_service::validate_owned_open_dispatch(
                    &tx, config, &id, generation, &op,
                )?;
            }
        } else {
            let rootless_open_reconcile =
                allows_rootless_open_reconcile(&tx, &method, &input, &b, &id, generation)?;
            if b["state"] != "ready"
                && !is_recovery_control(&method, &input, &b, rootless_open_reconcile)
            {
                return Err(Error::new(
                    "BINDING_NOT_READY",
                    "binding not ready before dispatch",
                ));
            }
        }
        if method != "agent.open" && repair_context.is_none() && caller["role"] != "operator" {
            let caller_id = model::text(&o, "caller_id")?;
            let owns: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM attempts WHERE owner_id=?1 AND binding_id=?2 AND binding_generation=?3 AND released_at_ms IS NULL)",
                params![caller_id, id, generation],
                |r| r.get(0),
            )?;
            if !owns {
                if caller["role"] != "manager" {
                    return Err(Error::new(
                        "FORBIDDEN",
                        "original caller no longer has an assignment here",
                    ));
                }
                let attempt_id = if method == "task.dispatch" {
                    model::text(&input, "attempt_id")?
                } else {
                    model::text(&o, "attempt_id")?
                };
                let attempt = tasks::get_attempt(&tx, attempt_id)?;
                if attempt["binding_id"] != id || attempt["binding_generation"] != generation {
                    return Err(Error::new(
                        "FORBIDDEN",
                        "queued Operation Attempt belongs to another binding generation",
                    ));
                }
                let operation_principal = operation_caller_principal(&tx, &o)?;
                super::gm::require_attempt_control(&tx, &operation_principal, &attempt)?;
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
            if caller["role"] != "operator" {
                let operation_principal = operation_caller_principal(&tx, &o)?;
                super::gm::require_attempt_control(&tx, &operation_principal, &a)?;
            }
            trusted_launch_dispatch_packet =
                super::launcher_dispatch::validate_before_effect(&tx, config, &op, &input, &b)?;
        }
        Ok(())
    })();
    if let Err(e) = guard {
        let now = model::now_ms()?;
        tx.execute("UPDATE operations SET state='rejected',result_json=?2,settled_at_ms=?3,updated_at_ms=?3 WHERE operation_id=?1 AND state='queued'",params![op,model::canonical(&json!(e))?,now])?;
        tx.commit()?;
        return Ok(json!({"command":null,"rejected_operation_id":op,"error":e}));
    }
    if let Some(packet) = trusted_launch_dispatch_packet {
        input["launch_dispatch_packet"] = packet;
    }
    let mut command_route = b["route"].clone();
    if command_route
        .get("owned_service")
        .is_some_and(|v| !v.is_null())
    {
        let config = config.ok_or_else(|| {
            Error::new(
                "OWNED_SERVICE_SCOPE",
                "owned command requires the current controller configuration",
            )
        })?;
        let options = super::launcher_owned_service::effective_options_for_binding(
            &tx, config, &id, generation,
        )?;
        command_route["native_options"] = json!(options);
    }
    let now = model::now_ms()?;
    let won=tx.execute("UPDATE operations SET state='sending',sent_at_ms=?2,updated_at_ms=?2 WHERE operation_id=?1 AND state='queued'",params![op,now])?;
    if won != 1 {
        return Err(Error::conflict("dispatch already admitted"));
    }
    if method == "task.dispatch" {
        let a = tasks::get_attempt(&tx, model::text(&input, "attempt_id")?)?;
        input["task_snapshot"] = a["task_snapshot"].clone();
        if crate::runtime::codex::is_controller_route(&b["route"])
            || crate::runtime::prepared::is_prepared_claude_route(&b["route"])
            || crate::runtime::batch::is_command_route(&b["route"])
        {
            input["task_snapshot_canonical"] = json!(model::canonical(&a["task_snapshot"])?);
        }
        if crate::runtime::batch::is_command_route(&b["route"]) {
            let instruction = crate::runtime::batch::instruction(&input)?;
            input["command_core_binding"] =
                crate::runtime::batch::command_receipt_facts(&op, &instruction).as_json();
        }
    }
    if method == "agent.reconcile" && crate::runtime::batch::is_command_route(&b["route"]) {
        let target_id = model::text(&input, "operation_id")?.to_owned();
        let target = operations::get_operation(&tx, &target_id)?;
        if target["binding_id"] != id || target["binding_generation"] != generation {
            return Err(Error::new(
                "FORBIDDEN",
                "reconcile target belongs to another binding generation",
            ));
        }
        if !matches!(
            target["method"].as_str(),
            Some("agent.open" | "task.dispatch")
        ) {
            return Err(Error::invalid(
                "Command reconciliation target must be agent.open or task.dispatch",
            ));
        }
        if target["method"] == "task.dispatch" {
            let raw: String = tx.query_row(
                "SELECT original_request_json FROM operations WHERE operation_id=?1",
                [&target_id],
                |row| row.get(0),
            )?;
            let target_request: Value = serde_json::from_str(&raw)?;
            let target_fields = command_target_receipt_fields(
                &tx,
                &target_id,
                &target,
                &target_request,
                &b["route"],
                &id,
                generation,
            )?;
            input["target_command_method"] = target_fields["target_command_method"].clone();
            // These reserved inputs are recomputed from the original Operation
            // and immutable Attempt. A module never derives a target receipt
            // identity from potentially corrupt saved files.
            input["target_command_requested_model"] =
                target_fields["target_command_requested_model"].clone();
            input["target_command_core_binding"] =
                target_fields["target_command_core_binding"].clone();
        } else {
            input["target_command_method"] = target["method"].clone();
        }
    }
    let target_input_sha256 = if method == "agent.reconcile" {
        let target_id = model::text(&input, "operation_id")?;
        let raw: String = tx.query_row(
            "SELECT original_request_json FROM operations WHERE operation_id=?1 AND binding_id=?2 AND binding_generation=?3",
            params![target_id, id, generation],
            |row| row.get(0),
        ).optional()?.ok_or_else(|| Error::new(
            "FORBIDDEN",
            "reconcile target is not an operation on this binding generation",
        ))?;
        let target_request: Value = serde_json::from_str(&raw)?;
        Some(model::digest(model::canonical(&target_request)?.as_bytes()))
    } else {
        None
    };
    let command = RuntimeCommand {
        operation_id: op,
        method,
        created_at_ms: created,
        binding_id: id,
        generation,
        native_root_id: b["native_root_id"].as_str().map(str::to_owned),
        route: command_route,
        input,
        input_sha256: Some(input_sha256),
        target_input_sha256,
    };
    tx.commit()?; // Never return a command while SQLite can still roll back admission.
    Ok(json!({"command":command}))
}

fn batch_output_artifacts(
    db: &Connection,
    operation_id: &str,
    binding_id: &str,
    generation: i64,
    native_output: &str,
) -> Result<Vec<ArtifactRecord>> {
    if !crate::runtime::batch::BATCH_OUTPUTS.contains(&native_output) {
        return Err(Error::invalid("native output is not allowlisted"));
    }
    let mut stmt = db.prepare(
        "SELECT artifact_id,relative_path,byte_length,content_digest,metadata_json FROM artifacts WHERE kind='native_result_page' AND json_extract(metadata_json,'$.operation_id')=?1 AND json_extract(metadata_json,'$.binding_id')=?2 AND json_extract(metadata_json,'$.binding_generation')=?3 AND json_extract(metadata_json,'$.native_output')=?4 ORDER BY json_extract(metadata_json,'$.page'),artifact_id",
    )?;
    let rows = stmt
        .query_map(
            params![operation_id, binding_id, generation, native_output],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                ))
            },
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut records = Vec::with_capacity(rows.len());
    for (artifact_id, relative_path, byte_length, content_digest, raw_metadata) in rows {
        records.push(ArtifactRecord {
            kind: "native_result_page".into(),
            artifact_id,
            relative_path,
            byte_length: u64::try_from(byte_length)
                .map_err(|_| Error::new("ARTIFACT_DAMAGED", "negative artifact length"))?,
            content_digest,
            metadata: serde_json::from_str(&raw_metadata)?,
        });
    }
    if !records.is_empty() {
        let count = records[0].metadata["pages"].as_u64().unwrap_or(0) as usize;
        if count != records.len()
            || records.iter().enumerate().any(|(index, record)| {
                record.metadata["page"].as_u64() != Some(index as u64)
                    || record.metadata["pages"].as_u64() != Some(count as u64)
            })
        {
            return Err(Error::new(
                "BATCH_ARTIFACT_DAMAGED",
                "retained batch output page manifest is incomplete or inconsistent",
            ));
        }
    }
    Ok(records)
}

fn selected_batch_output(
    records: &[ArtifactRecord],
    offset: u64,
    length: u64,
) -> (Vec<Value>, Vec<Value>, u64, bool) {
    let total = records.iter().map(|record| record.byte_length).sum::<u64>();
    if offset > total {
        return (Vec::new(), Vec::new(), total, false);
    }
    let end = offset.saturating_add(length).min(total);
    let mut cursor = 0u64;
    let mut refs = Vec::new();
    let mut pages = Vec::new();
    for record in records {
        let page_start = cursor;
        let page_end = cursor.saturating_add(record.byte_length);
        cursor = page_end;
        let start = offset.max(page_start);
        let selected_end = end.min(page_end);
        if selected_end > start || (total == 0 && records.len() == 1 && offset == 0) {
            refs.push(json!(record.artifact_id));
            pages.push(json!({
                "artifact_id": record.artifact_id,
                "source_offset_bytes": start.saturating_sub(page_start),
                "byte_length": selected_end.saturating_sub(start)
            }));
        }
    }
    if offset == total
        && total > 0
        && let Some(last) = records.last()
        && !refs.contains(&json!(last.artifact_id))
    {
        refs.push(json!(last.artifact_id));
        pages.push(json!({
            "artifact_id": last.artifact_id,
            "source_offset_bytes": last.byte_length,
            "byte_length": 0
        }));
    }
    (refs, pages, total, true)
}

pub(super) fn outcome(db: &mut Connection, p: &Principal, v: &Value) -> Result<Value> {
    outcome_with_artifacts(db, p, v, &[])
}

fn expected_command_dispatch_identity(
    db: &Connection,
    operation_id: &str,
    operation: &Value,
    request: &Value,
    route: &Value,
) -> Result<(Value, String, crate::runtime::batch::CommandReceiptFacts)> {
    let attempt_id = model::text(request, "attempt_id")?;
    if operation["attempt_id"] != attempt_id {
        return Err(Error::new(
            "NATIVE_IDENTITY_MISMATCH",
            "Command receipt does not match the dispatch Operation attempt",
        ));
    }
    let attempt = tasks::get_attempt(db, attempt_id)?;
    let mut frozen_input = request.clone();
    frozen_input["task_snapshot"] = attempt["task_snapshot"].clone();
    let instruction = crate::runtime::batch::instruction(&frozen_input)?;
    let expected = crate::runtime::batch::command_receipt_facts(operation_id, &instruction);
    let requested_model = model::text(&route["native_options"], "modelId")?.to_owned();
    Ok((attempt, requested_model, expected))
}

fn command_target_receipt_fields(
    db: &Connection,
    operation_id: &str,
    operation: &Value,
    request: &Value,
    route: &Value,
    binding_id: &str,
    generation: i64,
) -> Result<Value> {
    if operation["method"] != "task.dispatch"
        || operation["binding_id"] != binding_id
        || operation["binding_generation"] != generation
    {
        return Err(Error::new(
            "FORBIDDEN",
            "reconcile target belongs to another binding generation or method",
        ));
    }
    let (attempt, requested_model, facts) =
        expected_command_dispatch_identity(db, operation_id, operation, request, route)?;
    if attempt["binding_id"] != binding_id || attempt["binding_generation"] != generation {
        return Err(Error::new(
            "FORBIDDEN",
            "reconcile target Attempt belongs to another binding generation",
        ));
    }
    Ok(json!({
        "target_command_method":"task.dispatch",
        "target_command_requested_model":requested_model,
        "target_command_core_binding":facts.as_json()
    }))
}

fn validate_command_dispatch_receipt(
    db: &Connection,
    operation: &Value,
    route: &Value,
    receipt: &RuntimeOutcome,
) -> Result<()> {
    let raw: String = db.query_row(
        "SELECT original_request_json FROM operations WHERE operation_id=?1",
        [&receipt.operation_id],
        |row| row.get(0),
    )?;
    let request: Value = serde_json::from_str(&raw)?;
    let (_attempt, expected_model, expected) =
        expected_command_dispatch_identity(db, &receipt.operation_id, operation, &request, route)?;
    if receipt.details["batch_run_id"] != expected.batch_run_id
        || receipt.details["prompt_sha256"] != expected.prompt_sha256
        || receipt.details["prompt_bytes"].as_u64() != u64::try_from(expected.prompt_bytes).ok()
        || receipt.details["requested_model"].as_str() != Some(expected_model.as_str())
    {
        return Err(Error::new(
            "NATIVE_IDENTITY_MISMATCH",
            "Command receipt differs from the core-frozen Operation and prompt identity",
        ));
    }
    Ok(())
}

/// Validate a typed module receipt against the exact immutable operation and
/// the descriptor selected when its binding was admitted. Call this for each
/// RuntimeOutcome independently, including a target outcome returned while an
/// `agent.reconcile` operation is being settled.
pub(super) fn validate_module_receipt_for_operation(
    db: &Connection,
    binding_id: &str,
    binding_generation: i64,
    binding: &Value,
    outcome: &RuntimeOutcome,
) -> Result<swarm_contracts::runtime::ModuleReceiptIdentity> {
    let receipt_value = outcome
        .details
        .get("module_receipt")
        .ok_or_else(|| Error::new("MODULE_RECEIPT_INVALID", "module receipt is missing"))?;
    let receipt: swarm_contracts::runtime::ModuleReceiptIdentity =
        serde_json::from_value(receipt_value.clone()).map_err(|_| {
            Error::new(
                "MODULE_RECEIPT_INVALID",
                "module receipt has an invalid shape",
            )
        })?;
    receipt.validate().map_err(|_| {
        Error::new(
            "MODULE_RECEIPT_INVALID",
            "module receipt identity is invalid",
        )
    })?;

    if receipt.binding_id != binding_id
        || receipt.binding_generation != binding_generation
        || receipt.operation_id != outcome.operation_id
    {
        return Err(Error::new(
            "MODULE_RECEIPT_INVALID",
            "module receipt names another binding or Operation",
        ));
    }

    let artifact_id = model::text(binding, "module_artifact_id")?;
    let selector = binding["observation"]
        .get("module_contract_selector")
        .ok_or_else(|| {
            Error::new(
                "MODULE_RECEIPT_INVALID",
                "typed module receipt has no retained descriptor selector",
            )
        })?;
    let retained =
        super::module_handshake::retained_contract_identity(db, artifact_id, Some(selector))?
            .ok_or_else(|| {
                Error::new(
                    "MODULE_RECEIPT_INVALID",
                    "binding has no retained trusted module descriptor",
                )
            })?;
    if receipt.module_id != retained.module_id
        || receipt.artifact != retained.artifact
        || receipt.protocol != retained.protocol
    {
        return Err(Error::new(
            "MODULE_RECEIPT_INVALID",
            "module receipt identity differs from the binding's retained descriptor",
        ));
    }

    let original_request_json: String = db.query_row(
        "SELECT original_request_json FROM operations WHERE operation_id=?1 AND binding_id=?2 AND binding_generation=?3",
        params![outcome.operation_id.as_str(), binding_id, binding_generation],
        |row| row.get(0),
    )?;
    let original_request: Value = serde_json::from_str(&original_request_json).map_err(|_| {
        Error::new(
            "MODULE_RECEIPT_INVALID",
            "stored original Operation request is malformed",
        )
    })?;
    let expected_input_sha256 = model::digest(model::canonical(&original_request)?.as_bytes());
    if receipt.input_sha256 != expected_input_sha256 {
        return Err(Error::new(
            "MODULE_RECEIPT_INVALID",
            "module receipt request digest differs from the stored original request",
        ));
    }
    Ok(receipt)
}

fn exact_reconcile_target_id(
    db: &Connection,
    operation_id: &str,
    binding_id: &str,
    binding_generation: i64,
) -> Result<String> {
    let raw: String = db.query_row(
        "SELECT original_request_json FROM operations WHERE operation_id=?1 AND binding_id=?2 AND binding_generation=?3",
        params![operation_id, binding_id, binding_generation],
        |row| row.get(0),
    )?;
    let request: Value = serde_json::from_str(&raw).map_err(|_| {
        Error::new(
            "MODULE_RECONCILIATION_INVALID",
            "stored reconciliation request is malformed",
        )
    })?;
    Ok(model::text(&request, "operation_id")?.to_owned())
}

/// Bind strict-module reconciliation receipts to the exact admitted request.
/// A target outcome carries `details.reconcile_operation_id`; the reconcile
/// outcome carries `details.target_operation_id`. Ordinary durable outbox
/// outcomes have no reconciliation link and retain the normal reporting path.
fn validate_registered_module_recovery_link(
    db: &Connection,
    binding_id: &str,
    binding_generation: i64,
    binding: &Value,
    operation: &Value,
    outcome: &RuntimeOutcome,
) -> Result<()> {
    if binding["observation"]
        .get("module_contract_selector")
        .is_none()
    {
        return Ok(());
    }

    if model::text(operation, "method")? == "agent.reconcile" {
        let target_id = exact_reconcile_target_id(
            db,
            model::text(operation, "operation_id")?,
            binding_id,
            binding_generation,
        )?;
        if outcome.details["target_operation_id"].as_str() != Some(target_id.as_str()) {
            return Err(Error::new(
                "MODULE_RECONCILIATION_INVALID",
                "reconcile outcome does not name its exact requested Operation",
            ));
        }
        let target = operations::get_operation(db, &target_id)?;
        if target["binding_id"] != binding_id
            || target["binding_generation"] != binding_generation
            || !matches!(
                target["state"].as_str(),
                Some("sending" | "native_accepted" | "outcome_unknown" | "settled" | "rejected")
            )
            || operations::registered_module_recovery_contract(
                db,
                binding,
                model::text(&target, "method")?,
            )?
            .is_none()
        {
            return Err(Error::new(
                "MODULE_RECONCILIATION_INVALID",
                "reconcile target is not an eligible Operation on this exact binding generation",
            ));
        }
        if outcome.details["resolved"] == true
            && !matches!(target["state"].as_str(), Some("settled" | "rejected"))
        {
            return Err(Error::new(
                "MODULE_RECONCILIATION_INVALID",
                "reconcile cannot report resolution before the target Operation is terminal",
            ));
        }
        if outcome.details["native_replay"] == true {
            return Err(Error::new(
                "MODULE_RECONCILIATION_INVALID",
                "strict module reconciliation must remain readback-only",
            ));
        }
    }

    if let Some(link) = outcome.details.get("reconcile_operation_id") {
        let reconcile_id = link
            .as_str()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                Error::new(
                    "MODULE_RECONCILIATION_INVALID",
                    "target outcome reconciliation link is malformed",
                )
            })?;
        if model::text(operation, "method")? == "agent.reconcile" {
            return Err(Error::new(
                "MODULE_RECONCILIATION_INVALID",
                "reconcile summary cannot also claim to be its target outcome",
            ));
        }
        let reconcile = operations::get_operation(db, reconcile_id)?;
        if reconcile["binding_id"] != binding_id
            || reconcile["binding_generation"] != binding_generation
            || reconcile["method"] != "agent.reconcile"
            || !matches!(
                reconcile["state"].as_str(),
                Some("sending" | "native_accepted" | "outcome_unknown")
            )
            || exact_reconcile_target_id(db, reconcile_id, binding_id, binding_generation)?
                != outcome.operation_id
            || operations::registered_module_recovery_contract(
                db,
                binding,
                model::text(operation, "method")?,
            )?
            .is_none()
        {
            return Err(Error::new(
                "MODULE_RECONCILIATION_INVALID",
                "target outcome is not linked to an admitted reconcile for this exact Operation",
            ));
        }
        if !matches!(
            operation["state"].as_str(),
            Some("sending" | "native_accepted" | "outcome_unknown")
        ) {
            return Err(Error::new(
                "MODULE_RECONCILIATION_INVALID",
                "reconcile-linked target outcome cannot replace a terminal Operation",
            ));
        }
        if outcome.details["native_replay"] == true {
            return Err(Error::new(
                "MODULE_RECONCILIATION_INVALID",
                "strict module target reconciliation must remain readback-only",
            ));
        }
    }
    Ok(())
}
pub(super) fn outcome_with_artifacts(
    db: &mut Connection,
    p: &Principal,
    v: &Value,
    artifacts: &[ArtifactRecord],
) -> Result<Value> {
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
    // Versioned bindings require a typed receipt for every outcome. Legacy
    // unversioned bindings retain their existing validators and wire contract.
    if b["observation"].get("module_contract_selector").is_some() {
        validate_module_receipt_for_operation(&tx, &id, generation, &b, &r)?;
    }
    let sessionless_batch = crate::runtime::batch::is_sessionless_route(&b["route"]);
    if b["route"]["runtime"] == crate::runtime::codex::RUNTIME
        && !crate::runtime::codex::is_controller_route(&b["route"])
        && matches!(r.outcome, EffectOutcome::Applied | EffectOutcome::Accepted)
        && matches!(
            o["method"].as_str(),
            Some("agent.open" | "task.dispatch" | "agent.send")
        )
    {
        return Err(Error::new(
            "UNSUPPORTED_RUNTIME",
            "the standalone Codex observer cannot establish controller input admission",
        ));
    }
    if sessionless_batch {
        crate::runtime::batch::validate_outcome(&b["route"], model::text(&o, "method")?, &r)?;
        if crate::runtime::batch::is_command_route(&b["route"]) && o["method"] == "task.dispatch" {
            validate_command_dispatch_receipt(&tx, &o, &b["route"], &r)?;
        }
        if o["method"] == "agent.reconcile" {
            let raw: String = tx.query_row(
                "SELECT original_request_json FROM operations WHERE operation_id=?1",
                [&r.operation_id],
                |row| row.get(0),
            )?;
            let request: Value = serde_json::from_str(&raw)?;
            let target_id = model::text(&request, "operation_id")?;
            if r.details["target_operation_id"] != target_id {
                return Err(Error::new(
                    "NATIVE_IDENTITY_MISMATCH",
                    "batch reconciliation must name its exact requested Operation",
                ));
            }
            let target = operations::get_operation(&tx, target_id)?;
            if target["binding_id"] != id
                || target["binding_generation"] != generation
                || !matches!(
                    target["method"].as_str(),
                    Some("agent.open" | "task.dispatch")
                )
            {
                return Err(Error::new(
                    "FORBIDDEN",
                    "batch reconciliation target is not an exact Operation on this binding",
                ));
            }
            if matches!(r.outcome, EffectOutcome::Applied)
                && r.details["resolved"] == true
                && matches!(
                    target["state"].as_str(),
                    Some("sending" | "native_accepted" | "outcome_unknown")
                )
            {
                return Err(Error::invalid(
                    "batch reconciliation cannot report resolved before target evidence is recorded",
                ));
            }
        }
        if o["method"] == "agent.result" {
            let raw: String = tx.query_row(
                "SELECT original_request_json FROM operations WHERE operation_id=?1",
                [&r.operation_id],
                |row| row.get(0),
            )?;
            let request: Value = serde_json::from_str(&raw)?;
            let selector = &request["selector"];
            let operation_id = model::text(selector, "operation_id")?.to_owned();
            let expected = batch_output_artifacts(
                &tx,
                &operation_id,
                &id,
                generation,
                model::text(selector, "native_output")?,
            )?;
            let offset = request["offset_bytes"].as_u64().unwrap_or(0);
            let length = request["length_bytes"]
                .as_u64()
                .unwrap_or(crate::artifacts::MAX_PAGE_BYTES as u64);
            let (expected_refs, expected_pages, total, range_valid) =
                selected_batch_output(&expected, offset, length);
            let expected_completion = if expected.is_empty() {
                "batch_output_missing"
            } else if !range_valid {
                "batch_output_range_invalid"
            } else {
                "batch_output_artifacts_selected"
            };
            let unavailable = r.details["completion_condition"] == "batch_output_unavailable"
                && matches!(r.outcome, EffectOutcome::Rejected)
                && r.details["artifact_refs"] == json!([])
                && r.details["artifact_pages"] == json!([]);
            if r.details["dispatch_operation_id"] != selector["operation_id"]
                || r.details["native_output"] != selector["native_output"]
                || (!unavailable && r.details["artifact_refs"] != json!(expected_refs))
                || (!unavailable && r.details["artifact_pages"] != json!(expected_pages))
                || r.details["total_bytes"] != total
                || (!unavailable && r.details["completion_condition"] != expected_completion)
                || (!unavailable
                    && (expected.is_empty() || !range_valid)
                    && !matches!(r.outcome, EffectOutcome::Rejected))
                || (!unavailable
                    && !expected.is_empty()
                    && range_valid
                    && !matches!(r.outcome, EffectOutcome::Applied))
            {
                return Err(Error::new(
                    "NATIVE_IDENTITY_MISMATCH",
                    "batch result must select the exact dispatch Operation and output requested",
                ));
            }
        }
    }
    let prepared_first_dispatch = crate::runtime::prepared::is_prepared_claude_route(&b["route"])
        && o["method"] == "task.dispatch"
        && b["native_root_id"].is_null()
        && b["native_scope_key"].is_null()
        && (r.native_root_id.is_some() || r.native_scope_key.is_some());
    if prepared_first_dispatch {
        crate::runtime::prepared::adopt_first_input_identity(&tx, &b, &r)?;
    } else if o["method"] != "agent.open"
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
    if crate::runtime::codex::is_controller_route(&b["route"])
        && o["method"] == "agent.send"
        && matches!(r.outcome, EffectOutcome::Applied)
    {
        let raw: String = tx.query_row(
            "SELECT original_request_json FROM operations WHERE operation_id=?1",
            [&r.operation_id],
            |row| row.get(0),
        )?;
        let request: Value = serde_json::from_str(&raw)?;
        let expected_turn = if request["delivery"] == "steer" {
            Some(model::text(&request, "expected_turn_id")?)
        } else {
            None
        };
        crate::runtime::codex::validate_input_receipt(
            &b,
            &r,
            model::text(&request, "text")?,
            expected_turn,
        )?;
    }
    if crate::runtime::prepared::is_prepared_claude_route(&b["route"])
        && o["method"] == "agent.send"
        && matches!(r.outcome, EffectOutcome::Applied)
    {
        let raw: String = tx.query_row(
            "SELECT original_request_json FROM operations WHERE operation_id=?1",
            [&r.operation_id],
            |row| row.get(0),
        )?;
        let request: Value = serde_json::from_str(&raw)?;
        if request["delivery"] != "next_turn" {
            return Err(Error::new(
                "NATIVE_IDENTITY_MISMATCH",
                "prepared Claude only verifies next-turn send receipts",
            ));
        }
        crate::runtime::prepared::validate_send_receipt(&b, &r, model::text(&request, "text")?)?;
    }
    let encoded = model::canonical(&json!(r))?;
    let stream = format!("module:{}", p.client_id);
    let previous:bool=tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM observations WHERE source_stream_id=?1 AND operation_id=?2 AND kind='runtime.outcome' AND payload_json=?3)",
        params![stream,r.operation_id,encoded],|x|x.get(0))?;
    if previous {
        return Ok(json!({"recorded":true,"replayed":true,"state":o["state"]}));
    }
    validate_registered_module_recovery_link(&tx, &id, generation, &b, &o, &r)?;

    if crate::runtime::batch::is_legacy_command_route(&b["route"]) {
        return Err(Error::new(
            "ARTIFACT_RETIRED",
            "Command artifact .2 receipts are historical and cannot settle or mutate current Operations",
        ));
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
        && crate::runtime::prerequisites::validator_for(
            b["route"]["runtime"].as_str().unwrap_or_default(),
        )
        .is_some()
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
    if sessionless_batch && o["method"] == "task.dispatch" {
        if !matches!(r.outcome, EffectOutcome::Applied | EffectOutcome::Rejected)
            && !artifacts.is_empty()
        {
            return Err(Error::invalid(
                "unresolved batch outcomes cannot publish selectable artifacts",
            ));
        }
        for artifact in artifacts {
            if artifact.kind != "native_result_page"
                || artifact.metadata["operation_id"] != r.operation_id
                || artifact.metadata["binding_id"] != id
                || artifact.metadata["binding_generation"] != generation
                || !crate::runtime::batch::BATCH_OUTPUTS
                    .contains(&artifact.metadata["native_output"].as_str().unwrap_or(""))
            {
                return Err(Error::new(
                    "BATCH_ARTIFACT_MISMATCH",
                    "artifact is not an allowlisted page from this batch dispatch",
                ));
            }
            let existing: Option<(String, String, i64, String, String)> = tx
                .query_row(
                    "SELECT kind,relative_path,byte_length,content_digest,metadata_json FROM artifacts WHERE artifact_id=?1",
                    [&artifact.artifact_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
                )
                .optional()?;
            if let Some((kind, path, length, digest, metadata)) = existing {
                if kind != artifact.kind
                    || path != artifact.relative_path
                    || length != i64::try_from(artifact.byte_length).unwrap_or(-1)
                    || digest != artifact.content_digest
                    || serde_json::from_str::<Value>(&metadata)? != artifact.metadata
                {
                    return Err(Error::conflict(
                        "batch artifact identity already names different retained content",
                    ));
                }
            } else {
                tx.execute(
                    "INSERT INTO artifacts(artifact_id,relative_path,kind,byte_length,content_digest,created_at_ms,metadata_json) VALUES(?1,?2,'native_result_page',?3,?4,?5,?6)",
                    params![artifact.artifact_id, artifact.relative_path, i64::try_from(artifact.byte_length).map_err(|_| Error::invalid("batch artifact length is out of range"))?, artifact.content_digest, now, model::canonical(&artifact.metadata)?],
                )?;
            }
        }
    }
    let state = match r.outcome {
        EffectOutcome::Accepted => "native_accepted",
        EffectOutcome::Applied => "settled",
        EffectOutcome::Rejected => "rejected",
        EffectOutcome::Unknown => "outcome_unknown",
    };
    // Warm CLI results have no native turn or inbox identity. Validate their
    // explicit local receipt against the current, committed native observation.
    let warm_producer = if crate::runtime::warm_stream::is_route(&b["route"])
        && matches!(o["method"].as_str(), Some("task.dispatch" | "agent.send"))
    {
        let selected = b["observation"]["native_observation_id"].as_i64();
        let observation: Option<String> = selected
            .map(|selected| {
                tx.query_row(
                    "SELECT payload_json FROM observations WHERE observation_id=?1 AND binding_id=?2 AND binding_generation=?3 AND kind='runtime.state'",
                    params![selected, id, generation],
                    |row| row.get(0),
                ).optional()
            })
            .transpose()?
            .flatten();
        let observation = observation
            .map(|raw| serde_json::from_str::<Value>(&raw))
            .transpose()?
            .unwrap_or(Value::Null);
        if observation != b["observation"]["native"] {
            return Err(Error::new(
                "NATIVE_IDENTITY_MISMATCH",
                "warm-stream receipt requires the exact materialized observation",
            ));
        }
        crate::runtime::warm_stream::validate_outcome(
            crate::runtime::warm_stream::OutcomeContext {
                binding: &b,
                operation: &o,
                outcome: &r,
                observation_id: selected,
                observation: &observation,
            },
        )?
    } else {
        None
    };
    if o["method"] == "agent.result"
        && !sessionless_batch
        && matches!(r.outcome, EffectOutcome::Applied)
    {
        return Err(Error::invalid(
            "result pages require module.result and durable artifact publication",
        ));
    }
    if matches!(r.outcome, EffectOutcome::Applied) {
        if o["method"] == "agent.open" {
            if crate::runtime::prepared::is_prepared_claude_route(&b["route"]) {
                crate::runtime::prepared::validate_prepared_open(&b, &r)?;
                tx.execute("UPDATE bindings SET state=CASE WHEN json_extract(state_json,'$.recovery_required')=1 OR EXISTS(SELECT 1 FROM operations WHERE binding_id=?1 AND binding_generation=?2 AND operation_id<>?3 AND state IN ('sending','native_accepted','outcome_unknown') AND method IN ('agent.open','task.dispatch')) THEN 'reconciling' ELSE 'ready' END,state_json=json_set(state_json,'$.waiting_for',NULL,'$.opening_evidence',json(?4)) WHERE binding_id=?1 AND generation=?2",params![id,generation,r.operation_id,model::canonical(&r.details)?])?;
            } else if sessionless_batch {
                tx.execute("UPDATE bindings SET state=CASE WHEN json_extract(state_json,'$.recovery_required')=1 OR EXISTS(SELECT 1 FROM operations WHERE binding_id=?1 AND binding_generation=?2 AND operation_id<>?3 AND state IN ('sending','native_accepted','outcome_unknown') AND method IN ('agent.open','task.dispatch')) THEN 'reconciling' ELSE 'ready' END,state_json=json_set(state_json,'$.waiting_for',NULL,'$.opening_evidence',json(?4)) WHERE binding_id=?1 AND generation=?2",params![id,generation,r.operation_id,model::canonical(&r.details)?])?;
            } else {
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
            }
        } else if o["method"] == "task.dispatch" {
            if warm_producer.is_some() {
                // Its terminal producer is recorded below for both successful
                // and rejected native terminal outcomes.
            } else if sessionless_batch {
                producers::record_batch(&tx, &o, &r, now)?;
            } else {
                let attempt = model::text(&o, "attempt_id")?;
                let mut producer = json!({"assignment_id":r.operation_id,"native_session_id":b["native_root_id"],"disposition":"admitted"});
                match (r.turn_id.as_deref(), r.native_input_id.as_deref()) {
                    _ if crate::runtime::codex::is_controller_route(&b["route"])
                        || crate::runtime::prepared::is_prepared_claude_route(&b["route"]) =>
                    {
                        let a = tasks::get_attempt(&tx, attempt)?;
                        let raw: String = tx.query_row(
                            "SELECT original_request_json FROM operations WHERE operation_id=?1",
                            [&r.operation_id],
                            |row| row.get(0),
                        )?;
                        let request: Value = serde_json::from_str(&raw)?;
                        producer =
                            if crate::runtime::prepared::is_prepared_claude_route(&b["route"]) {
                                crate::runtime::prepared::dispatch_producer(
                                    &b,
                                    &r,
                                    &a["task_snapshot"],
                                    model::text(&request, "text")?,
                                )?
                            } else {
                                crate::runtime::codex::dispatch_producer(
                                    &b,
                                    &r,
                                    &a["task_snapshot"],
                                    model::text(&request, "text")?,
                                )?
                            };
                    }
                    (Some(turn), None) if !turn.is_empty() => {
                        producer["native_run_id"] = json!(turn)
                    }
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
        }
    } else if sessionless_batch
        && o["method"] == "task.dispatch"
        && matches!(r.outcome, EffectOutcome::Rejected)
    {
        producers::record_batch(&tx, &o, &r, now)?;
    } else if sessionless_batch && o["method"] == "agent.open" {
        tx.execute(
            "UPDATE bindings SET state=CASE WHEN ?3='rejected' AND NOT EXISTS(SELECT 1 FROM operations WHERE binding_id=?1 AND binding_generation=?2 AND operation_id<>?4 AND state IN ('sending','native_accepted','outcome_unknown') AND method IN ('agent.open','task.dispatch')) THEN 'opening' ELSE 'reconciling' END,state_json=json_set(state_json,'$.opening_evidence',json(?5)) WHERE binding_id=?1 AND generation=?2",
            params![id, generation, state, r.operation_id, model::canonical(&r.details)?],
        )?;
    } else if o["method"] == "agent.open" && !matches!(r.outcome, EffectOutcome::Accepted) {
        // A failure may follow spawn: preserve ownership and its known native identity.
        let reported_identity = match (r.native_root_id.as_deref(), r.native_scope_key.as_deref()) {
            (Some(root), Some(scope))
                if !root.is_empty()
                    && !scope.is_empty()
                    && (b["native_root_id"].is_null()
                        || (b["native_root_id"] == root && b["native_scope_key"] == scope)) =>
            {
                (Some(root), Some(scope))
            }
            _ => (None, None),
        };
        tx.execute("UPDATE bindings SET state='reconciling',native_root_id=COALESCE(?3,native_root_id),native_scope_key=COALESCE(?4,native_scope_key),state_json=json_set(state_json,'$.opening_evidence',json(?5)) WHERE binding_id=?1 AND generation=?2",params![id,generation,reported_identity.0,reported_identity.1,model::canonical(&r.details)?])?;
    }
    if let Some(producer) = warm_producer {
        let attempt = model::text(&o, "attempt_id")?;
        tx.execute(
            "UPDATE attempts SET state=CASE WHEN state='reserved' THEN 'running' ELSE state END,producers_json=json_insert(producers_json,'$[#]',json(?2)),updated_at_ms=?3 WHERE attempt_id=?1 AND released_at_ms IS NULL",
            params![attempt, model::canonical(&producer)?, now],
        )?;
    }
    if let Some(configuration) = applied_configuration {
        tx.execute(
            &format!(
                "UPDATE bindings SET state_json=json_set(state_json,'{}',json(?3)) WHERE binding_id=?1 AND generation=?2",
                prerequisites::slot_path(configuration.record.slot)
            ),
            params![id, generation, model::canonical(&configuration.record.value)?],
        )?;
        if let Some(snapshot) = configuration.setup_snapshot {
            let mut snapshots = b["observation"]["setup_snapshots"].clone();
            if !snapshots.is_object() {
                snapshots = json!({});
            }
            snapshots[&r.operation_id] = snapshot.to_json();
            tx.execute(
                "UPDATE bindings SET state_json=json_set(state_json,'$.setup_snapshots',json(?3)) WHERE binding_id=?1 AND generation=?2",
                params![id, generation, model::canonical(&snapshots)?],
            )?;
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
    let native_refs = if sessionless_batch {
        json!({"session_id":null,"turn_id":null,"input_id":null,
            "execution_shape":crate::runtime::batch::EXECUTION_SHAPE,
            "dispatch_operation_id":if o["method"]=="task.dispatch" {json!(r.operation_id)} else {Value::Null},
            "batch_run_id":r.details.get("batch_run_id").cloned().unwrap_or(Value::Null),
            "control_record_ref":r.details.get("control_record_ref").cloned().unwrap_or(Value::Null),
            "per_run_native_session_id":r.details.get("native_session_id").cloned().unwrap_or(Value::Null)})
    } else {
        json!({"session_id":r.native_root_id,"turn_id":r.turn_id,"input_id":r.native_input_id,
            "local_execution_ref":r.details.get("local_execution_ref").cloned().unwrap_or(Value::Null)})
    };
    tx.execute("UPDATE operations SET state=?2,result_json=?3,native_refs_json=?4,settled_at_ms=?5,updated_at_ms=?6 WHERE operation_id=?1",params![r.operation_id,state,encoded,model::canonical(&native_refs)?,if matches!(state,"outcome_unknown"|"native_accepted"){None}else{Some(now)},now])?;
    if sessionless_batch
        && matches!(
            o["method"].as_str(),
            Some("task.dispatch" | "agent.reconcile" | "agent.refresh")
        )
    {
        tx.execute(
            "UPDATE bindings SET state=CASE WHEN COALESCE(json_extract(state_json,'$.recovery_required'),0)=0 AND json_extract(state_json,'$.connection')='connected' AND NOT EXISTS(SELECT 1 FROM operations WHERE binding_id=?1 AND binding_generation=?2 AND state IN ('sending','native_accepted','outcome_unknown') AND method IN ('agent.open','task.dispatch')) THEN 'ready' ELSE 'reconciling' END WHERE binding_id=?1 AND generation=?2 AND state IN ('ready','reconciling')",
            params![id, generation],
        )?;
    }
    tx.execute("INSERT INTO observations(source_stream_id,source_event_key,binding_id,binding_generation,operation_id,kind,payload_json,recorded_at_ms) VALUES(?1,?2,?3,?4,?5,'runtime.outcome',?6,?7)",params![format!("module:{}",p.client_id),key,id,generation,r.operation_id,encoded,now])?;
    if matches!(r.outcome, EffectOutcome::Applied | EffectOutcome::Rejected) {
        let (status, phase) = match r.outcome {
            EffectOutcome::Applied => ("applied", "native_outcome_terminal"),
            EffectOutcome::Rejected => ("rejected", "native_outcome_terminal"),
            EffectOutcome::Accepted | EffectOutcome::Unknown => unreachable!(),
        };
        let occurrence_id = format!("operation:{}:{}", r.operation_id, phase);
        super::insert_safe_system_event(
            &tx,
            "controller:runtime",
            &format!("terminal:{}", r.operation_id),
            Some(&r.operation_id),
            "native.operation.completed",
            phase,
            status,
            Some(&occurrence_id),
            None,
            None,
            now,
        )?;
    }
    super::capacity::note_outcome(&tx, &o, &r, now)?;
    super::capacity::sync_operation(&tx, &r.operation_id, now)?;
    if let Some(attempt_id) = o["attempt_id"].as_str() {
        super::capacity::sync_attempt(&tx, attempt_id, now)?;
    }
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
    let observed_configurations =
        prerequisites::observed_configurations(&b, &v["state"], observation_id, now)?;
    tx.execute("UPDATE bindings SET state_json=json_set(state_json,'$.native',json(?3),'$.observed_at_ms',?4,'$.native_sequence',?5,'$.native_observation_id',?6) WHERE binding_id=?1 AND generation=?2",params![id,generation,encoded,now,sequence,observation_id])?;
    for configuration in observed_configurations {
        tx.execute(
            &format!(
                "UPDATE bindings SET state_json=json_set(state_json,'{}',json(?3)) WHERE binding_id=?1 AND generation=?2",
                prerequisites::slot_path(configuration.slot)
            ),
            params![id, generation, model::canonical(&configuration.value)?],
        )?;
    }
    if v["state"]["turns"].is_array()
        || v["state"]["observed_children"].is_array()
        || v["state"]["input_executions"].is_array()
    {
        let mut stmt = tx.prepare("SELECT attempt_id, producers_json FROM attempts WHERE binding_id=?1 AND binding_generation=?2 AND released_at_ms IS NULL")?;
        let rows = stmt
            .query_map(params![id, generation], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(stmt);
        for (attempt, raw) in &rows {
            let mut producers: Vec<Value> = serde_json::from_str(raw)?;
            for producer in &mut producers {
                producers::apply_evidence(producer, &v["state"], Some(observation_id));
            }
            tx.execute(
                "UPDATE attempts SET producers_json=?2 WHERE attempt_id=?1",
                params![attempt, model::canonical(&json!(producers))?],
            )?;
        }
        for (attempt, _) in &rows {
            super::capacity::sync_attempt(&tx, attempt, now)?;
        }
    }
    tx.commit()?;
    Ok(json!({"recorded":true,"stale":false,"observation_id":observation_id}))
}

pub(super) fn disconnected(db: &mut Connection, p: &Principal) -> Result<bool> {
    if p.role != Role::Module {
        return Ok(false);
    }
    let Some(c) = meta(db, &format!("client:{}", p.client_id))? else {
        return Ok(false);
    };
    let (Some(binding_id), Some(generation)) = (
        c["binding_id"].as_str(),
        c["binding_generation"].as_i64().filter(|value| *value > 0),
    ) else {
        return Ok(false);
    };
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let binding = operations::get_binding(&tx, binding_id, generation)?;
    if !binding["released_at_ms"].is_null() || binding["observation"]["module_link_id"] != p.link_id
    {
        return Ok(false);
    }
    let now = model::now_ms()?;
    // The closed module link cannot deliver another outcome. Preserve a
    // conservative unknown Operation state; migration 010 records the bounded
    // operation.outcome_unknown fact in this same transaction.
    let unknown = tx.execute(
        "UPDATE operations SET state='outcome_unknown',updated_at_ms=?3 \
         WHERE binding_id=?1 AND binding_generation=?2 \
           AND state IN ('sending','native_accepted')",
        params![binding_id, generation, now],
    )?;
    if unknown > 0 {
        super::capacity::sync_binding(&tx, binding_id, generation, now)?;
    }
    let binding_changed = tx.execute(
        "UPDATE bindings SET \
             state=CASE WHEN state IN ('ready','opening') THEN 'reconciling' ELSE state END, \
             state_json=json_set(state_json,'$.connection','disconnected') \
         WHERE binding_id=?1 AND generation=?2 \
           AND json_extract(state_json,'$.module_link_id')=?3 AND released_at_ms IS NULL",
        params![binding_id, generation, p.link_id],
    )?;
    tx.commit()?;
    Ok(unknown > 0 || binding_changed > 0)
}

fn ensure_batch_root(data_dir: &Path) -> Result<PathBuf> {
    let data_root = std::fs::canonicalize(data_dir)?;
    let root = data_dir.join("batch-runs");
    match std::fs::symlink_metadata(&root) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            return Err(Error::new(
                "BATCH_PATH",
                "batch evidence root must be a regular directory",
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir(&root)?;
        }
        Err(error) => return Err(error.into()),
    }
    let canonical = std::fs::canonicalize(&root)?;
    if canonical.parent() != Some(data_root.as_path()) {
        return Err(Error::new(
            "BATCH_PATH",
            "batch evidence root escaped the selected state directory",
        ));
    }
    crate::platform::private_permissions(&canonical, true)?;
    Ok(canonical)
}

fn batch_pending_ids(db: &Connection, p: &Principal) -> Result<Vec<String>> {
    let (id, generation, _) = scope(db, p, true)?;
    let mut stmt = db.prepare(
        "SELECT operation_id FROM operations WHERE binding_id=?1 AND binding_generation=?2 AND state IN ('sending','native_accepted','outcome_unknown') AND method IN ('agent.open','task.dispatch','agent.refresh','agent.reconcile','agent.result') ORDER BY created_at_ms,operation_id",
    )?;
    Ok(stmt
        .query_map(params![id, generation], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?)
}

fn zed_open_outcome(command: &RuntimeCommand, described: Result<Value>) -> RuntimeOutcome {
    let (outcome, details) = match described {
        Ok(description) => (
            EffectOutcome::Applied,
            json!({
                "execution_shape":batch::EXECUTION_SHAPE,
                "completion_condition":"executor_preflight_completed",
                "native_session_state":"not_started",
                "runtime":zed::RUNTIME,
                "module_artifact_id":zed::ARTIFACT_ID,
                "contract_revision":zed::CONTRACT_REVISION,
                "scope":description["scope"],
                "requested_model":description["model"],
                "effective_model":Value::Null,
                "effective_model_status":"unknown",
                "installed_runtime_verified":false,
                "capabilities":description["capabilities"],
                "environment_key_presence":description["env_keys_present"]
            }),
        ),
        Err(error) => (
            EffectOutcome::Rejected,
            json!({
                "execution_shape":batch::EXECUTION_SHAPE,
                "completion_condition":"executor_preflight_rejected",
                "native_session_state":"not_started",
                "diagnostic_code":error.code,
                "requested_model":command.route["native_options"]["model"],
                "effective_model":Value::Null,
                "effective_model_status":"unknown"
            }),
        ),
    };
    RuntimeOutcome {
        operation_id: command.operation_id.clone(),
        outcome,
        native_scope_key: None,
        native_root_id: None,
        turn_id: None,
        native_input_id: None,
        details,
    }
}

fn batch_diagnostic(command: &RuntimeCommand, code: &str, started: bool) -> RuntimeOutcome {
    let mut details = json!({
        "execution_shape":batch::EXECUTION_SHAPE,
        "batch_run_id":zed::run_id(&command.operation_id),
            "artifact_refs":[],
            "output_artifact_refs":{},
        "control_record_ref":format!("zed-batch:{}", zed::run_id(&command.operation_id)),
        "requested_model":command.route["native_options"]["model"],
        "effective_model":Value::Null,
        "effective_model_status":"unknown",
        "exit_code":Value::Null,
        "signal":Value::Null,
        "anomalies":[code]
    });
    let outcome = if started {
        details["anomalies"] = json!([code, "terminal_evidence_not_recorded"]);
        EffectOutcome::Unknown
    } else {
        details["completion_condition"] = json!("executor_launch_rejected");
        details["native_session_state"] = json!("not_started");
        details["diagnostic_code"] = json!(code);
        EffectOutcome::Rejected
    };
    RuntimeOutcome {
        operation_id: command.operation_id.clone(),
        outcome,
        native_scope_key: None,
        native_root_id: None,
        turn_id: None,
        native_input_id: None,
        details,
    }
}

fn zed_batch_outcome(
    command: &RuntimeCommand,
    options: &zed::Options,
    batch_outcome: &zed::BatchOutcome,
    intent: &zed::BatchIntent,
) -> RuntimeOutcome {
    let native_status = batch_outcome
        .native_result
        .as_ref()
        .and_then(|result| result["status"].as_str());
    let effective_model = batch_outcome
        .native_result
        .as_ref()
        .and_then(|result| result.get("model"))
        .cloned()
        .unwrap_or(Value::Null);
    let effective_model_status = if effective_model.is_string() {
        "observed"
    } else {
        "unknown"
    };
    let outcome = match (
        native_status,
        batch_outcome.exit_code,
        batch_outcome.host_terminated,
    ) {
        (Some("completed"), Some(0), false) => EffectOutcome::Applied,
        (Some("error" | "timeout" | "interrupted"), Some(1..=3), false) => EffectOutcome::Rejected,
        _ => EffectOutcome::Unknown,
    };
    let result_subtype = match native_status {
        Some("completed") => Some("success"),
        Some("error") => Some("error"),
        Some("timeout") => Some("timeout"),
        Some("interrupted") => Some("interrupted"),
        _ => None,
    };
    let mut details = json!({
        "execution_shape":batch::EXECUTION_SHAPE,
        "batch_run_id":intent.run_id,
        "control_record_ref":format!("zed-batch:{}", intent.run_id),
        "requested_model":options.model,
        "effective_model":effective_model,
        "effective_model_status":effective_model_status,
        "native_session_id":Value::Null,
        "exit_code":batch_outcome.exit_code,
        "signal":batch_outcome.signal,
        "host_terminated":batch_outcome.host_terminated,
        "prompt_sha256":intent.prompt_sha256,
        "prompt_bytes":intent.prompt_bytes,
        "task_snapshot_sha256":intent.task_snapshot_sha256,
        "result_subtype":result_subtype,
        "native_result":batch_outcome.native_result,
        "native_result_sha256":batch_outcome.native_result_sha256,
        "artifact_refs":batch_outcome.artifacts.iter().map(|record| record.artifact_id.clone()).collect::<Vec<_>>(),
        "output_artifact_refs":batch_outcome.artifacts.iter().fold(serde_json::Map::new(), |mut outputs, record| {
            let name = record.metadata["native_output"].as_str().unwrap_or_default().to_owned();
            let entry = outputs.entry(name).or_insert_with(|| json!([]));
            if let Some(items) = entry.as_array_mut() { items.push(json!(record.artifact_id)); }
            outputs
        }),
        "anomalies":if batch_outcome.host_terminated {json!(["host_deadline_terminated"])} else if result_subtype.is_none() {json!(["native_result_unavailable"])} else {json!([])}
    });
    if batch_outcome.native_result.is_some() {
        details["completion_condition"] = json!("native_result_observed");
    }
    RuntimeOutcome {
        operation_id: command.operation_id.clone(),
        outcome,
        native_scope_key: None,
        native_root_id: None,
        turn_id: None,
        native_input_id: None,
        details,
    }
}

fn batch_snapshot_outcome(
    db: &Connection,
    principal: &Principal,
    command: &RuntimeCommand,
) -> Result<RuntimeOutcome> {
    let (binding_id, generation, binding) = scope(db, principal, true)?;
    let mut stmt = db.prepare(
        "SELECT operation_id,method,state,updated_at_ms FROM operations WHERE binding_id=?1 AND binding_generation=?2 ORDER BY created_at_ms DESC,operation_id DESC LIMIT 32",
    )?;
    let operations = stmt
        .query_map(params![binding_id, generation], |row| {
            Ok(json!({
                "operation_id":row.get::<_,String>(0)?,
                "method":row.get::<_,String>(1)?,
                "state":row.get::<_,String>(2)?,
                "updated_at_ms":row.get::<_,i64>(3)?
            }))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(RuntimeOutcome {
        operation_id: command.operation_id.clone(),
        outcome: EffectOutcome::Applied,
        native_scope_key: None,
        native_root_id: None,
        turn_id: None,
        native_input_id: None,
        details: json!({
            "execution_shape":batch::EXECUTION_SHAPE,
            "completion_condition":"batch_snapshot_readback",
            "binding_id":binding_id,
            "binding_generation":generation,
            "binding_state":binding["state"],
            "connection":binding["observation"]["connection"],
            "native_session_state":"not_started",
            "operations":operations,
            "family_complete":false
        }),
    })
}

fn batch_result_readback(
    db: &Connection,
    principal: &Principal,
    command: &RuntimeCommand,
) -> Result<(RuntimeOutcome, Vec<ArtifactRecord>)> {
    let (binding_id, generation, _) = scope(db, principal, true)?;
    let selector = &command.input["selector"];
    let dispatch_id = model::text(selector, "operation_id")?;
    let output = model::text(selector, "native_output")?;
    let target = operations::get_operation(db, dispatch_id)?;
    if target["method"] != "task.dispatch"
        || target["binding_id"] != binding_id
        || target["binding_generation"] != generation
        || !matches!(target["state"].as_str(), Some("settled" | "rejected"))
    {
        return Err(Error::new(
            "BATCH_OUTPUT_UNAVAILABLE",
            "batch output requires a terminal dispatch on this exact binding",
        ));
    }
    let records = batch_output_artifacts(db, dispatch_id, &binding_id, generation, output)?;
    let offset = command.input["offset_bytes"].as_u64().unwrap_or(0);
    let length = command.input["length_bytes"]
        .as_u64()
        .unwrap_or(crate::artifacts::MAX_PAGE_BYTES as u64);
    let (artifact_refs, artifact_pages, total_bytes, range_valid) =
        selected_batch_output(&records, offset, length);
    let condition = if records.is_empty() {
        "batch_output_missing"
    } else if !range_valid {
        "batch_output_range_invalid"
    } else {
        "batch_output_artifacts_selected"
    };
    let outcome = if records.is_empty() || !range_valid {
        EffectOutcome::Rejected
    } else {
        EffectOutcome::Applied
    };
    Ok((
        RuntimeOutcome {
            operation_id: command.operation_id.clone(),
            outcome,
            native_scope_key: None,
            native_root_id: None,
            turn_id: None,
            native_input_id: None,
            details: json!({
                "execution_shape":batch::EXECUTION_SHAPE,
                "completion_condition":condition,
                "dispatch_operation_id":dispatch_id,
                "native_output":output,
                "artifact_refs":artifact_refs,
                "artifact_pages":artifact_pages,
                "total_bytes":total_bytes,
                "offset_bytes":offset,
                "requested_length_bytes":length,
                "eof":offset.saturating_add(length)>=total_bytes
            }),
        },
        records,
    ))
}

impl Store {
    pub async fn supervise_zed(self, mut stopping: watch::Receiver<bool>) {
        let mut workers: BTreeMap<(String, i64), JoinHandle<()>> = BTreeMap::new();
        let mut changed = self.changed.subscribe();
        while !*stopping.borrow() {
            let finished = workers
                .iter()
                .filter(|(_, task)| task.is_finished())
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>();
            for key in finished {
                if let Some(task) = workers.remove(&key) {
                    let _ = task.await;
                }
            }
            match self.run(|db| batch_bindings(db)).await {
                Ok(bindings) => {
                    for binding in bindings {
                        let key = (
                            binding["binding_id"]
                                .as_str()
                                .unwrap_or_default()
                                .to_owned(),
                            binding["generation"].as_i64().unwrap_or_default(),
                        );
                        if let std::collections::btree_map::Entry::Vacant(entry) =
                            workers.entry(key.clone())
                        {
                            let store = self.clone();
                            let stop = stopping.clone();
                            entry.insert(tokio::spawn(async move {
                                store.drive_zed_binding(&key.0, key.1, stop).await;
                            }));
                        }
                    }
                }
                Err(error) => eprintln!("Zed supervisor: {}", error.code),
            }
            tokio::select! {
                _ = stopping.changed() => {},
                _ = changed.changed() => {},
                _ = tokio::time::sleep(Duration::from_secs(2)) => {},
            }
        }
        for task in workers.into_values() {
            let _ = task.await;
        }
    }

    async fn record_zed_outcome(
        &self,
        principal: &Principal,
        outcome: RuntimeOutcome,
        artifacts: Vec<ArtifactRecord>,
    ) -> Result<()> {
        let principal = principal.clone();
        let value = json!(outcome);
        self.run(move |db| outcome_with_artifacts(db, &principal, &value, &artifacts))
            .await?;
        self.changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
        Ok(())
    }

    async fn read_zed_receipt(
        &self,
        command: &RuntimeCommand,
        output_root: &Path,
    ) -> Result<Option<zed::BatchReceipt>> {
        let command = command.clone();
        let output_root = output_root.to_path_buf();
        let instruction = batch::instruction(&command.input)?;
        self.file_io(move |files| {
            let receipt = zed::read_receipt(
                &output_root,
                zed::BatchReadContext {
                    operation_id: &command.operation_id,
                    binding_id: &command.binding_id,
                    generation: command.generation,
                    route: &command.route,
                    instruction: &instruction,
                    task_snapshot: &command.input["task_snapshot"],
                },
                &files,
            )?;
            Ok(receipt)
        })
        .await
    }

    async fn record_zed_receipt(
        &self,
        principal: &Principal,
        receipt: zed::BatchReceipt,
    ) -> Result<bool> {
        let has_terminal = matches!(
            receipt.outcome.outcome,
            EffectOutcome::Applied | EffectOutcome::Rejected
        );
        let artifacts = if has_terminal {
            receipt.artifacts
        } else {
            Vec::new()
        };
        let outcome = receipt.outcome;
        self.record_zed_outcome(principal, outcome, artifacts)
            .await?;
        Ok(has_terminal)
    }

    async fn batch_original(
        &self,
        principal: &Principal,
        operation_id: &str,
    ) -> Result<RuntimeCommand> {
        let principal = principal.clone();
        let operation_id = operation_id.to_owned();
        self.run(move |db| batch_original(db, &principal, &operation_id))
            .await
    }

    async fn reconcile_zed_target(
        &self,
        principal: &Principal,
        target_id: &str,
        output_root: &Path,
    ) -> Result<bool> {
        let command = self.batch_original(principal, target_id).await?;
        let state = {
            let p = principal.clone();
            let target_id = target_id.to_owned();
            self.run(move |db| {
                let (binding_id, generation, _) = scope(db, &p, true)?;
                let target = operations::get_operation(db, &target_id)?;
                if target["binding_id"] != binding_id
                    || target["binding_generation"] != generation
                    || !matches!(
                        target["method"].as_str(),
                        Some("agent.open" | "task.dispatch")
                    )
                {
                    return Err(Error::new("FORBIDDEN", "reconcile target changed binding"));
                }
                Ok(target["state"].as_str().unwrap_or_default().to_owned())
            })
            .await?
        };
        if matches!(state.as_str(), "settled" | "rejected") {
            return Ok(true);
        }
        match command.method.as_str() {
            "agent.open" => {
                let outcome = match zed::Options::parse(&command.route["native_options"]) {
                    Ok(options) => zed_open_outcome(&command, zed::describe(&options)),
                    Err(error) => zed_open_outcome(
                        &command,
                        Err(Error::new(
                            error.code,
                            "configured executor preflight failed",
                        )),
                    ),
                };
                let terminal = matches!(
                    outcome.outcome,
                    EffectOutcome::Applied | EffectOutcome::Rejected
                );
                self.record_zed_outcome(principal, outcome, Vec::new())
                    .await?;
                Ok(terminal)
            }
            "task.dispatch" => match self.read_zed_receipt(&command, output_root).await {
                Ok(Some(receipt)) => self.record_zed_receipt(principal, receipt).await,
                Ok(None) | Err(_) => Ok(false),
            },
            _ => Ok(false),
        }
    }

    async fn read_zed_result(&self, principal: &Principal, command: &RuntimeCommand) -> Result<()> {
        let p = principal.clone();
        let c = command.clone();
        let (outcome, records) = self
            .run(move |db| batch_result_readback(db, &p, &c))
            .await?;
        let verify = records.clone();
        let verified = self
            .file_io(move |files| {
                for record in &verify {
                    files.verify(record)?;
                }
                Ok(())
            })
            .await;
        if let Err(error) = verified {
            let selector = &command.input["selector"];
            let rejected = RuntimeOutcome {
                operation_id: command.operation_id.clone(),
                outcome: EffectOutcome::Rejected,
                native_scope_key: None,
                native_root_id: None,
                turn_id: None,
                native_input_id: None,
                details: json!({
                    "execution_shape":batch::EXECUTION_SHAPE,
                    "completion_condition":"batch_output_unavailable",
                    "dispatch_operation_id":selector["operation_id"],
                    "native_output":selector["native_output"],
                    "artifact_refs":[],
                    "artifact_pages":[],
                    "total_bytes":records.iter().map(|record| record.byte_length).sum::<u64>(),
                    "diagnostic_code":error.code
                }),
            };
            return self
                .record_zed_outcome(principal, rejected, Vec::new())
                .await;
        }
        self.record_zed_outcome(principal, outcome, Vec::new())
            .await
    }

    async fn process_zed_command(
        &self,
        principal: &Principal,
        command: RuntimeCommand,
        output_root: &Path,
    ) -> Result<()> {
        match command.method.as_str() {
            "agent.open" => {
                let outcome = match zed::Options::parse(&command.route["native_options"]) {
                    Ok(options) => zed_open_outcome(&command, zed::describe(&options)),
                    Err(error) => zed_open_outcome(
                        &command,
                        Err(Error::new(
                            error.code,
                            "configured executor preflight failed",
                        )),
                    ),
                };
                self.record_zed_outcome(principal, outcome, Vec::new())
                    .await
            }
            "task.dispatch" => {
                let instruction = match batch::instruction(&command.input) {
                    Ok(instruction) => instruction,
                    Err(error) => {
                        let rejected = batch_diagnostic(&command, &error.code, false);
                        return self
                            .record_zed_outcome(principal, rejected, Vec::new())
                            .await;
                    }
                };
                let options = match zed::Options::parse(&command.route["native_options"]) {
                    Ok(options) => options,
                    Err(error) => {
                        let rejected = batch_diagnostic(&command, &error.code, false);
                        return self
                            .record_zed_outcome(principal, rejected, Vec::new())
                            .await;
                    }
                };
                let intent = match zed::make_intent(&command, &instruction) {
                    Ok(intent) => intent,
                    Err(error) => {
                        let rejected = batch_diagnostic(&command, &error.code, false);
                        return self
                            .record_zed_outcome(principal, rejected, Vec::new())
                            .await;
                    }
                };
                let run_dir = zed::run_directory(output_root, &command.operation_id);
                if run_dir.exists() {
                    match self.read_zed_receipt(&command, output_root).await {
                        Ok(Some(receipt)) => {
                            self.record_zed_receipt(principal, receipt).await?;
                        }
                        Ok(None) => {
                            let unknown = batch_diagnostic(
                                &command,
                                "prior_run_marker_without_terminal",
                                true,
                            );
                            self.record_zed_outcome(principal, unknown, Vec::new())
                                .await?;
                        }
                        Err(error) => {
                            let unknown = batch_diagnostic(&command, &error.code, true);
                            self.record_zed_outcome(principal, unknown, Vec::new())
                                .await?;
                        }
                    }
                    return Ok(());
                }
                let options_for_run = options.clone();
                let command_for_run = command.clone();
                let instruction_for_run = instruction.clone();
                let root_for_run = output_root.to_path_buf();
                let files = self.artifacts.clone();
                let run = tokio::task::spawn_blocking(move || {
                    zed::run_batch_command(
                        &options_for_run,
                        &command_for_run,
                        &instruction_for_run,
                        &root_for_run,
                        &files,
                    )
                })
                .await
                .map_err(|error| Error::new("BATCH_WORKER", error.to_string()))?;
                match run {
                    Ok((batch_outcome, saved_intent)) => {
                        let outcome =
                            zed_batch_outcome(&command, &options, &batch_outcome, &saved_intent);
                        let receipt_outcome = serde_json::from_value::<RuntimeOutcome>(
                            serde_json::to_value(&outcome)?,
                        )?;
                        let receipt = zed::BatchReceipt {
                            version: 1,
                            intent: saved_intent,
                            outcome,
                            artifacts: batch_outcome.artifacts.clone(),
                        };
                        let root = output_root.to_path_buf();
                        let route = command.route.clone();
                        let persisted = self
                            .file_io(move |files| {
                                zed::persist_receipt(&root, &files, &route, &receipt)
                            })
                            .await;
                        if let Err(error) = persisted {
                            let unknown = batch_diagnostic(&command, &error.code, true);
                            self.record_zed_outcome(principal, unknown, Vec::new())
                                .await?;
                            return Ok(());
                        }
                        let records = if matches!(
                            receipt_outcome.outcome,
                            EffectOutcome::Applied | EffectOutcome::Rejected
                        ) {
                            batch_outcome.artifacts
                        } else {
                            Vec::new()
                        };
                        self.record_zed_outcome(principal, receipt_outcome, records)
                            .await
                    }
                    Err(error) => {
                        let started = run_dir.exists();
                        let unknown_or_rejected = batch_diagnostic(&command, &error.code, started);
                        if started {
                            let intent_path = run_dir.join("intent.json");
                            if std::fs::symlink_metadata(&intent_path).is_ok() {
                                let copy = serde_json::from_value::<RuntimeOutcome>(
                                    serde_json::to_value(&unknown_or_rejected)?,
                                )?;
                                let receipt = zed::BatchReceipt {
                                    version: 1,
                                    intent,
                                    outcome: copy,
                                    artifacts: Vec::new(),
                                };
                                let root = output_root.to_path_buf();
                                let route = command.route.clone();
                                let _ = self
                                    .file_io(move |files| {
                                        zed::persist_receipt(&root, &files, &route, &receipt)
                                    })
                                    .await;
                            }
                        }
                        self.record_zed_outcome(principal, unknown_or_rejected, Vec::new())
                            .await
                    }
                }
            }
            "agent.refresh" => {
                let p = principal.clone();
                let c = command.clone();
                let outcome = self
                    .run(move |db| batch_snapshot_outcome(db, &p, &c))
                    .await?;
                self.record_zed_outcome(principal, outcome, Vec::new())
                    .await
            }
            "agent.result" => self.read_zed_result(principal, &command).await,
            "agent.reconcile" => {
                let target_id = model::text(&command.input, "operation_id")?.to_owned();
                let resolved = self
                    .reconcile_zed_target(principal, &target_id, output_root)
                    .await?;
                let outcome = RuntimeOutcome {
                    operation_id: command.operation_id.clone(),
                    outcome: EffectOutcome::Applied,
                    native_scope_key: None,
                    native_root_id: None,
                    turn_id: None,
                    native_input_id: None,
                    details: json!({
                        "execution_shape":batch::EXECUTION_SHAPE,
                        "completion_condition":"batch_readback_recorded",
                        "target_operation_id":target_id,
                        "resolved":resolved,
                        "native_replay":false
                    }),
                };
                self.record_zed_outcome(principal, outcome, Vec::new())
                    .await
            }
            _ => {
                let rejected = RuntimeOutcome {
                    operation_id: command.operation_id.clone(),
                    outcome: EffectOutcome::Rejected,
                    native_scope_key: None,
                    native_root_id: None,
                    turn_id: None,
                    native_input_id: None,
                    details: json!({"execution_shape":batch::EXECUTION_SHAPE,"diagnostic_code":"unsupported_runtime_operation"}),
                };
                self.record_zed_outcome(principal, rejected, Vec::new())
                    .await
            }
        }
    }

    async fn recover_zed_pending(&self, principal: &Principal, output_root: &Path) {
        let p = principal.clone();
        let ids = match self.run(move |db| batch_pending_ids(db, &p)).await {
            Ok(ids) => ids,
            Err(error) => {
                eprintln!("Zed pending readback: {}", error.code);
                return;
            }
        };
        for operation_id in ids {
            let command = match self.batch_original(principal, &operation_id).await {
                Ok(command) => command,
                Err(_) => continue,
            };
            match command.method.as_str() {
                "agent.open" => {
                    let outcome = match zed::Options::parse(&command.route["native_options"]) {
                        Ok(options) => zed_open_outcome(&command, zed::describe(&options)),
                        Err(error) => zed_open_outcome(
                            &command,
                            Err(Error::new(
                                error.code,
                                "configured executor preflight failed",
                            )),
                        ),
                    };
                    let _ = self
                        .record_zed_outcome(principal, outcome, Vec::new())
                        .await;
                }
                "task.dispatch" => {
                    if let Ok(Some(receipt)) = self.read_zed_receipt(&command, output_root).await {
                        let _ = self.record_zed_receipt(principal, receipt).await;
                    }
                }
                "agent.refresh" => {
                    let p = principal.clone();
                    let c = command.clone();
                    if let Ok(outcome) =
                        self.run(move |db| batch_snapshot_outcome(db, &p, &c)).await
                    {
                        let _ = self
                            .record_zed_outcome(principal, outcome, Vec::new())
                            .await;
                    }
                }
                "agent.result" => {
                    let _ = self.read_zed_result(principal, &command).await;
                }
                "agent.reconcile" => {
                    if let Ok(target_id) = model::text(&command.input, "operation_id") {
                        let resolved = self
                            .reconcile_zed_target(principal, target_id, output_root)
                            .await
                            .unwrap_or(false);
                        let outcome = RuntimeOutcome {
                            operation_id: command.operation_id.clone(),
                            outcome: EffectOutcome::Applied,
                            native_scope_key: None,
                            native_root_id: None,
                            turn_id: None,
                            native_input_id: None,
                            details: json!({
                                "execution_shape":batch::EXECUTION_SHAPE,
                                "completion_condition":"batch_readback_recorded",
                                "target_operation_id":target_id,
                                "resolved":resolved,
                                "native_replay":false
                            }),
                        };
                        let _ = self
                            .record_zed_outcome(principal, outcome, Vec::new())
                            .await;
                    }
                }
                _ => {}
            }
        }
    }

    async fn drive_zed_binding(
        self,
        binding_id: &str,
        generation: i64,
        mut stopping: watch::Receiver<bool>,
    ) {
        let output_root = match ensure_batch_root(&self.data_dir) {
            Ok(root) => root,
            Err(error) => {
                eprintln!("Zed evidence root: {}", error.code);
                return;
            }
        };
        let binding = {
            let id = binding_id.to_owned();
            match self
                .run(move |db| operations::get_binding(db, &id, generation))
                .await
            {
                Ok(binding) => binding,
                Err(_) => return,
            }
        };
        let boot = model::new_id();
        let principal = {
            let boot = boot.clone();
            match self.run(move |db| attach_batch(db, &binding, &boot)).await {
                Ok(principal) => principal,
                Err(error) => {
                    eprintln!("Zed attachment: {}", error.code);
                    return;
                }
            }
        };
        let mut changed = self.changed.subscribe();
        while !*stopping.borrow() {
            let id = binding_id.to_owned();
            let active = self
                .run(move |db| operations::get_binding(db, &id, generation))
                .await
                .is_ok_and(|binding| binding["released_at_ms"].is_null());
            if !active {
                break;
            }
            self.recover_zed_pending(&principal, &output_root).await;
            if *stopping.borrow() {
                break;
            }
            let p = principal.clone();
            let command = self
                .run(move |db| next(db, &p))
                .await
                .ok()
                .and_then(|value| {
                    serde_json::from_value::<RuntimeCommand>(value["command"].clone()).ok()
                });
            if let Some(command) = command {
                if let Err(error) = self
                    .process_zed_command(&principal, command, &output_root)
                    .await
                {
                    eprintln!("Zed operation receipt: {}", error.code);
                }
                continue;
            }
            tokio::select! {
                _ = stopping.changed() => {},
                _ = changed.changed() => {},
                _ = tokio::time::sleep(Duration::from_secs(2)) => {},
            }
        }
    }
}

fn allows_rootless_open_reconcile(
    db: &Connection,
    method: &str,
    input: &Value,
    binding: &Value,
    binding_id: &str,
    generation: i64,
) -> Result<bool> {
    if method != "agent.reconcile"
        || binding["state"] != "reconciling"
        || binding["native_root_id"].is_string()
    {
        return Ok(false);
    }
    let target_id = model::text(input, "operation_id")?;
    let target = operations::get_operation(db, target_id)?;
    Ok(target["method"] == "agent.open"
        && target["binding_id"] == binding_id
        && target["binding_generation"] == generation
        && matches!(
            target["state"].as_str(),
            Some("sending" | "native_accepted" | "outcome_unknown")
        )
        && operations::module_recovery_contract_for_binding(db, binding, "agent.open")?.is_some())
}

fn is_recovery_control(
    method: &str,
    input: &Value,
    binding: &Value,
    rootless_open_reconcile: bool,
) -> bool {
    if binding["state"] != "reconciling" {
        return false;
    }
    if crate::runtime::batch::is_sessionless_route(&binding["route"]) {
        return matches!(method, "agent.refresh" | "agent.reconcile")
            || (binding["route"]["runtime"] == crate::runtime::zed::RUNTIME
                && method == "agent.result");
    }
    rootless_open_reconcile
        || (binding["native_root_id"].is_string()
            && (matches!(
                method,
                "agent.refresh"
                    | "agent.reply"
                    | "agent.background"
                    | "agent.reconcile"
                    | "agent.result"
                    | "agent.recover"
            ) || (method == "agent.goal"
                && matches!(input["action"].as_str(), Some("pause" | "clear")))))
}

pub(super) fn user_command(
    tx: &Connection,
    p: &Principal,
    method: &str,
    v: &Value,
    op: &str,
    config: &crate::config::Config,
) -> Result<Value> {
    user_command_with_actor(tx, UserCommandActor::Direct(p), method, v, op, config)
}

enum UserCommandActor<'a> {
    Direct(&'a Principal),
    Repair(&'a crate::automation::repair::RepairDispatchContext),
    GoalProgression(&'a crate::store::automation_goal_progression::GoalProgressionAdmission),
}

impl UserCommandActor<'_> {
    fn effective_client_id(&self) -> &str {
        match self {
            Self::Direct(principal) => &principal.client_id,
            Self::Repair(context) => context.effective_manager_id(),
            Self::GoalProgression(context) => context.effective_manager_id(),
        }
    }

    fn is_operator(&self) -> bool {
        matches!(self, Self::Direct(principal) if principal.role == Role::Operator)
    }

    fn require_operator(&self) -> Result<()> {
        match self {
            Self::Direct(principal) => principal.require_operator(),
            Self::Repair(_) => Err(Error::new(
                "FORBIDDEN",
                "repair authority cannot recover a binding",
            )),
            Self::GoalProgression(_) => Err(Error::new(
                "FORBIDDEN",
                "Goal progression authority cannot recover a binding",
            )),
        }
    }
}

fn operation_caller_principal(db: &Connection, operation: &Value) -> Result<Principal> {
    let client_id = model::text(operation, "caller_id")?.to_owned();
    let registration = meta(db, &format!("client:{client_id}"))?
        .ok_or_else(|| Error::new("UNAUTHORIZED", "original caller no longer registered"))?;
    let role: Role = serde_json::from_value(registration["role"].clone())?;
    super::current_principal(
        db,
        Principal {
            client_id,
            link_id: String::new(),
            role,
        },
    )
}

fn current_attempts_for_binding(
    db: &Connection,
    binding_id: &str,
    generation: i64,
) -> Result<Vec<Value>> {
    let attempt_ids = {
        let mut statement = db.prepare(
            "SELECT a.attempt_id FROM attempts AS a JOIN tasks AS t ON t.task_id=a.task_id \
             WHERE a.binding_id=?1 AND a.binding_generation=?2 AND a.released_at_ms IS NULL \
               AND a.task_revision=t.revision \
             ORDER BY a.created_at_ms,a.attempt_id LIMIT 2",
        )?;
        let rows = statement.query_map(params![binding_id, generation], |row| row.get(0))?;
        rows.collect::<rusqlite::Result<Vec<String>>>()?
    };
    attempt_ids
        .iter()
        .map(|attempt_id| tasks::get_attempt(db, attempt_id))
        .collect()
}

/// Derive the sole repair request from Store-validated immutable feedback.
/// A technical requester receives no general Manager or Operator authority.
pub(super) fn user_command_for_repair(
    tx: &Connection,
    context: &crate::automation::repair::RepairDispatchContext,
    operation_id: &str,
    config: &crate::config::Config,
) -> Result<Value> {
    context.require_current_for_admission(tx)?;
    if meta(tx, "execution_mode")?.unwrap_or(Value::Null)["new_work"] != "enabled" {
        return Err(Error::new(
            "ADMISSION_DISABLED",
            "new work is disabled before correction admission",
        ));
    }
    let request = context.delivery_request()?.value();
    user_command_with_actor(
        tx,
        UserCommandActor::Repair(context),
        "agent.send",
        &request,
        operation_id,
        config,
    )
}

/// Admit the exact manager-owned Goal continuation through the ordinary
/// runtime command path. The Store-derived context grants only one input for
/// the selected terminal EventRef; it is not a general Manager principal.
pub(super) fn user_command_for_goal_progression(
    tx: &Connection,
    context: &crate::store::automation_goal_progression::GoalProgressionAdmission,
    operation_id: &str,
    config: &crate::config::Config,
    now_ms: i64,
) -> Result<Value> {
    context.require_current_for_operation(tx, operation_id, now_ms)?;
    if meta(tx, "execution_mode")?.unwrap_or(Value::Null)["new_work"] != "enabled" {
        return Err(Error::new(
            "ADMISSION_DISABLED",
            "new work is disabled before Goal continuation admission",
        ));
    }
    user_command_with_actor(
        tx,
        UserCommandActor::GoalProgression(context),
        "agent.goal",
        context.request(),
        operation_id,
        config,
    )
}

fn user_command_with_actor(
    tx: &Connection,
    actor: UserCommandActor<'_>,
    method: &str,
    v: &Value,
    op: &str,
    config: &crate::config::Config,
) -> Result<Value> {
    // Envelope shape is validated before persistence by model::validate_mutation.
    let id = model::text(v, "binding_id")?;
    let generation = model::positive(v, "generation")?;
    let b = operations::get_binding(tx, id, generation)?;
    if crate::runtime::batch::is_sessionless_route(&b["route"]) {
        crate::runtime::batch::validate_command(&b["route"], method, v)?;
    }
    let rootless_open_reconcile =
        allows_rootless_open_reconcile(tx, method, v, &b, id, generation)?;
    if b["state"] != "ready" && !is_recovery_control(method, v, &b, rootless_open_reconcile) {
        return Err(Error::new(
            "BINDING_NOT_READY",
            "native session is not ready",
        ));
    }
    let mut manager_attempt = None;
    if !actor.is_operator() {
        let owns: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM attempts WHERE owner_id=?1 AND binding_id=?2 AND binding_generation=?3 AND released_at_ms IS NULL)",
            params![actor.effective_client_id(), id, generation],
            |row| row.get(0),
        )?;
        match &actor {
            UserCommandActor::Repair(context) => {
                if method != "agent.send" || v["delivery"] != "next_turn" {
                    return Err(Error::new(
                        "FORBIDDEN",
                        "repair authority is limited to its exact next-turn delivery",
                    ));
                }
                context.require_current_for_admission(tx)?;
                let expected_request = context.delivery_request()?.value();
                if id != context.binding_id()
                    || generation != context.binding_generation()
                    || model::canonical(v)? != model::canonical(&expected_request)?
                {
                    return Err(Error::new(
                        "FORBIDDEN",
                        "repair delivery differs from its exact retained request or binding",
                    ));
                }

                // The delivery request intentionally carries no caller-chosen
                // Attempt ID. Resolve the one retained by the sealed repair
                // context and independently confirm it is still the Task's
                // current correction Attempt on this exact binding.
                let identity = context.identity();
                let task = tasks::get_task(tx, &identity.task_id)?;
                let attempt = tasks::get_attempt(tx, &identity.attempt_id)?;
                if task["state"] != "open"
                    || task["revision"] != identity.task_revision
                    || task["current_attempt_id"] != identity.attempt_id
                    || attempt["task_id"] != identity.task_id
                    || attempt["task_revision"] != identity.task_revision
                    || attempt["attempt_id"] != identity.attempt_id
                    || attempt["state"] != "needs_correction"
                    || !attempt["released_at_ms"].is_null()
                    || attempt["binding_id"] != id
                    || attempt["binding_generation"] != generation
                {
                    return Err(Error::new(
                        "STALE_REPAIR_SUBJECT",
                        "the exact repair Attempt is no longer current on this binding",
                    ));
                }
            }
            UserCommandActor::GoalProgression(context) => {
                if method != "agent.goal"
                    || v["action"] != "continue"
                    || id != context.binding_id()
                    || generation != context.binding_generation()
                    || model::canonical(v)? != model::canonical(context.request())?
                {
                    return Err(Error::new(
                        "FORBIDDEN",
                        "Goal progression is limited to its exact admitted continuation request and binding",
                    ));
                }
                let task = tasks::get_task(tx, context.task_id())?;
                let attempt = tasks::get_attempt(tx, context.attempt_id())?;
                if task["state"] != "open"
                    || task["revision"] != context.task_revision()
                    || task["current_attempt_id"] != context.attempt_id()
                    || attempt["task_id"] != context.task_id()
                    || attempt["task_revision"] != context.task_revision()
                    || attempt["attempt_id"] != context.attempt_id()
                    || !attempt["released_at_ms"].is_null()
                    || attempt["binding_id"] != id
                    || attempt["binding_generation"] != generation
                {
                    return Err(Error::new(
                        "AUTOMATION_ACTION_CHANGED",
                        "exact Goal Task Attempt changed before native command admission",
                    ));
                }
                manager_attempt = Some(attempt);
            }
            UserCommandActor::Direct(principal) if principal.role == Role::Manager => {
                let principal = super::current_principal(tx, (*principal).clone())?;
                let requested_attempt = v
                    .get("attempt_id")
                    .filter(|value| !value.is_null())
                    .map(|_| model::text(v, "attempt_id").map(str::to_owned))
                    .transpose()?;
                let attempts = current_attempts_for_binding(tx, id, generation)?;
                let selected = if let Some(requested) = requested_attempt {
                    let attempt = tasks::get_attempt(tx, &requested)?;
                    if attempt["binding_id"] != id
                        || attempt["binding_generation"] != generation
                        || !attempt["released_at_ms"].is_null()
                    {
                        return Err(Error::new(
                            "FORBIDDEN",
                            "requested Attempt is outside this binding generation",
                        ));
                    }
                    Some(attempt)
                } else if attempts.len() == 1 {
                    attempts.into_iter().next()
                } else {
                    None
                };
                if let Some(attempt) = selected {
                    if principal.owns(model::text(&attempt, "owner_id")?).is_err() {
                        super::gm::require_attempt_control(tx, &principal, &attempt)?;
                    }
                    manager_attempt = Some(attempt);
                } else if !owns {
                    return Err(Error::new(
                        "FORBIDDEN",
                        "Manager has no unique current Attempt on this binding",
                    ));
                }
            }
            _ if !owns => {
                return Err(Error::new("FORBIDDEN", "no assignment on this binding"));
            }
            _ => {}
        }
    }
    if method == "agent.recover" {
        actor.require_operator()?;
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
    if method == "agent.result" && crate::runtime::batch::is_sessionless_route(&b["route"]) {
        let target = operations::get_operation(tx, model::text(&v["selector"], "operation_id")?)?;
        if target["method"] != "task.dispatch"
            || target["binding_id"] != id
            || target["binding_generation"] != generation
            || !matches!(target["state"].as_str(), Some("settled" | "rejected"))
        {
            return Err(Error::new(
                "BATCH_OUTPUT_UNAVAILABLE",
                "batch output requires a terminal dispatch on this exact binding",
            ));
        }
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
                if v.get("expected_revision").is_some() {
                    return Err(Error::invalid(
                        "expected_revision is only valid for continue",
                    ));
                }
            }
            "continue" => {
                model::text(v, "objective")?;
                if v.get("expected_revision").and_then(Value::as_u64).is_none() {
                    return Err(Error::invalid(
                        "continue requires a nonnegative expected_revision",
                    ));
                }
            }
            "pause" | "resume" | "clear" => {
                if v.get("objective").is_some() || v.get("expected_revision").is_some() {
                    return Err(Error::invalid(
                        "objective and expected_revision are only valid for set/edit/continue",
                    ));
                }
            }
            _ => {
                return Err(Error::invalid(
                    "goal action must be set/edit/pause/resume/clear/continue",
                ));
            }
        }
    } else if method == "agent.background" && v.get("session_id").is_some() {
        // Optional addressed target: an owned family member session. The
        // adapter re-proves ownership against the native parent chain
        // before any native call; without it the binding root is targeted.
        model::text(v, "session_id")?;
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
        if b["observation"].get("module_contract_selector").is_some()
            && operations::registered_module_recovery_contract(
                tx,
                &b,
                model::text(&target, "method")?,
            )?
            .is_none()
        {
            return Err(Error::invalid(
                "selected module descriptor does not support readback for this exact operation kind",
            ));
        }
        if crate::runtime::batch::is_sessionless_route(&b["route"])
            && !matches!(
                target["method"].as_str(),
                Some("agent.open" | "task.dispatch")
            )
        {
            return Err(Error::invalid(
                "sessionless batch reconciliation targets only its exact preflight or dispatch Operation",
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
        let native_options = if b["route"]
            .get("owned_service")
            .is_some_and(|v| !v.is_null())
        {
            json!(
                super::launcher_owned_service::effective_options_for_binding(
                    tx, config, id, generation
                )?
            )
        } else {
            b["route"]["native_options"].clone()
        };
        effective["operation_contract"] = crate::runtime::opencode_v2::configuration_contract(
            &v["settings"],
            id,
            generation,
            &native_options,
        )?;
    } else if method == "agent.goal"
        && b["route"]["runtime"] == crate::runtime::opencode_v2::RUNTIME
    {
        // OpenCode has no native goal API: the goal is a controller record in
        // the native instruction-entry backend, never `native_goal_admitted`.
        effective["operation_contract"] = json!({
            "effect_scope":"native_session",
            "order_scope":{"binding_id":id,"generation":generation},
            "completion_condition":if v["action"] == "continue" { "native_input_admitted" } else { "native_goal_recorded" },
            "application_boundary":if v["action"] == "continue" { "exact_goal_revision+prompt_admission" } else { "next_step_boundary+prompt_admission" },
            "replay_policy":"readback_only_no_mutation_replay",
            "fallback_used":false,
            "continuation_owner":"controller_record",
            "native_goal_api":false,
            "contract_revision":crate::runtime::opencode_v2::GOAL_CONTRACT_REVISION
        });
    } else if method == "agent.background"
        && b["route"]["runtime"] == crate::runtime::opencode_v2::RUNTIME
    {
        // Native `session.background`: the boundary is the native POST plus
        // the durable synthetic notice it admits; a lost response is
        // reconciled by notice readback only, never by replaying the POST.
        effective["operation_contract"] = json!({
            "effect_scope":"native_session",
            "order_scope":{"binding_id":id,"generation":generation},
            "completion_condition":"native_foreground_tools_backgrounded",
            "application_boundary":"native_background_boundary+notice_readback",
            "replay_policy":"readback_only_no_mutation_replay",
            "fallback_used":false,
            "contract_revision":crate::runtime::opencode_v2::BACKGROUND_CONTRACT_REVISION
        });
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
    if let UserCommandActor::Repair(context) = &actor {
        effective["automation_on_behalf"] = context.linkage_value();
        tx.execute(
            "UPDATE operations SET task_id=?2,attempt_id=?3 WHERE operation_id=?1",
            params![
                op,
                context.identity().task_id,
                context.identity().attempt_id
            ],
        )?;
    }
    if let UserCommandActor::GoalProgression(context) = &actor {
        effective["automation_on_behalf"] = context.linkage().clone();
    }
    if let Some(attempt) = manager_attempt {
        tx.execute(
            "UPDATE operations SET task_id=?2,attempt_id=?3 WHERE operation_id=?1",
            params![
                op,
                attempt["task_id"].as_str(),
                attempt["attempt_id"].as_str()
            ],
        )?;
    }
    tx.execute("UPDATE operations SET binding_id=?2,binding_generation=?3,prerequisite_operation_id=?4,effective_request_json=?5 WHERE operation_id=?1",params![op,id,generation,prerequisite_id,model::canonical(&effective)?])?;
    Ok(
        json!({"operation_id":op,"state":"queued","native_admission":"not_observed","prerequisite_operation_id":prerequisite_id,"prerequisite_state":prerequisite.receipt_state()}),
    )
}

#[cfg(test)]
mod command_receipt_binding_tests {
    use super::*;
    use crate::runtime::EffectOutcome;

    const OPERATION_ID: &str = "command-op-🐇-frozen";

    fn fixture() -> (Connection, Value, Value, RuntimeOutcome) {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(
            "CREATE TABLE operations(operation_id TEXT PRIMARY KEY, original_request_json TEXT NOT NULL);
             CREATE TABLE attempts(
               attempt_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, task_revision INTEGER NOT NULL,
               task_snapshot_json TEXT NOT NULL, owner_id TEXT NOT NULL, start_owner TEXT NOT NULL,
               start_operation_id TEXT, binding_id TEXT, binding_generation INTEGER, state TEXT NOT NULL,
               released_at_ms INTEGER, producers_json TEXT NOT NULL DEFAULT '[]',
               submission_ref TEXT, candidate_ref TEXT
             );",
        )
        .unwrap();
        let snapshot = json!({
            "objective":"Inspect the snowman 🐇",
            "labels":["alpha","β"],
            "nested":{"z":1,"a":"last"}
        });
        let request = json!({
            "attempt_id":"attempt-command",
            "text":"Use exact frozen words: café 🐇"
        });
        db.execute(
            "INSERT INTO operations(operation_id,original_request_json) VALUES(?1,?2)",
            params![OPERATION_ID, model::canonical(&request).unwrap()],
        )
        .unwrap();
        db.execute(
            "INSERT INTO attempts(attempt_id,task_id,task_revision,task_snapshot_json,owner_id,start_owner,binding_id,binding_generation,state) VALUES(?1,'task-command',1,?2,'operator','controller','binding-command',4,'reserved')",
            params!["attempt-command", model::canonical(&snapshot).unwrap()],
        )
        .unwrap();
        let route = json!({
            "runtime":crate::runtime::batch::COMMAND_RUNTIME,
            "module_artifact_id":crate::runtime::batch::COMMAND_ARTIFACT_ID,
            "native_options":{"modelId":"stealth/space-bunny-alpha"}
        });
        let mut frozen_input = request.clone();
        frozen_input["task_snapshot"] = snapshot;
        let instruction = crate::runtime::batch::instruction(&frozen_input).unwrap();
        let facts = crate::runtime::batch::command_receipt_facts(OPERATION_ID, &instruction);
        let operation = json!({
            "method":"task.dispatch",
            "attempt_id":"attempt-command",
            "binding_id":"binding-command",
            "binding_generation":4
        });
        let receipt = RuntimeOutcome {
            operation_id: OPERATION_ID.to_owned(),
            outcome: EffectOutcome::Unknown,
            native_root_id: None,
            native_scope_key: None,
            turn_id: None,
            native_input_id: None,
            details: json!({
                "batch_run_id":facts.batch_run_id,
                "prompt_sha256":facts.prompt_sha256,
                "prompt_bytes":facts.prompt_bytes,
                "requested_model":"stealth/space-bunny-alpha"
            }),
        };
        (db, operation, route, receipt)
    }

    #[test]
    fn command_receipt_is_bound_to_operation_unicode_text_and_frozen_attempt_snapshot() {
        let (db, operation, route, receipt) = fixture();
        validate_command_dispatch_receipt(&db, &operation, &route, &receipt).unwrap();

        for (field, value) in [
            ("batch_run_id", json!("command-batch:forged")),
            ("prompt_sha256", json!(model::digest(b"altered prompt"))),
            (
                "prompt_bytes",
                json!(receipt.details["prompt_bytes"].as_u64().unwrap() - 1),
            ),
        ] {
            let mut forged: RuntimeOutcome =
                serde_json::from_value(serde_json::to_value(&receipt).unwrap()).unwrap();
            forged.details[field] = value;
            let error =
                validate_command_dispatch_receipt(&db, &operation, &route, &forged).unwrap_err();
            assert_eq!(error.code, "NATIVE_IDENTITY_MISMATCH", "field {field}");
        }

        db.execute(
            "UPDATE operations SET original_request_json=?2 WHERE operation_id=?1",
            params![
                OPERATION_ID,
                model::canonical(&json!({
                    "attempt_id":"attempt-command",
                    "text":"changed words 🐇"
                }))
                .unwrap()
            ],
        )
        .unwrap();
        assert_eq!(
            validate_command_dispatch_receipt(&db, &operation, &route, &receipt)
                .unwrap_err()
                .code,
            "NATIVE_IDENTITY_MISMATCH"
        );
        db.execute(
            "UPDATE operations SET original_request_json=?2 WHERE operation_id=?1",
            params![
                OPERATION_ID,
                model::canonical(&json!({
                    "attempt_id":"attempt-command",
                    "text":"Use exact frozen words: café 🐇"
                }))
                .unwrap()
            ],
        )
        .unwrap();
        db.execute(
            "UPDATE attempts SET task_snapshot_json=?1 WHERE attempt_id='attempt-command'",
            [model::canonical(&json!({"objective":"altered snapshot 🐇"})).unwrap()],
        )
        .unwrap();
        assert_eq!(
            validate_command_dispatch_receipt(&db, &operation, &route, &receipt)
                .unwrap_err()
                .code,
            "NATIVE_IDENTITY_MISMATCH"
        );
    }

    #[test]
    fn command_reconcile_target_envelope_uses_original_request_attempt_and_exact_binding() {
        let (db, operation, route, receipt) = fixture();
        let raw: String = db
            .query_row(
                "SELECT original_request_json FROM operations WHERE operation_id=?1",
                [OPERATION_ID],
                |row| row.get(0),
            )
            .unwrap();
        let request: Value = serde_json::from_str(&raw).unwrap();
        let fields = command_target_receipt_fields(
            &db,
            OPERATION_ID,
            &operation,
            &request,
            &route,
            "binding-command",
            4,
        )
        .unwrap();
        assert_eq!(fields["target_command_method"], "task.dispatch");
        assert_eq!(
            fields["target_command_requested_model"],
            receipt.details["requested_model"]
        );
        assert_eq!(
            fields["target_command_core_binding"]["batch_run_id"],
            receipt.details["batch_run_id"]
        );
        assert_eq!(
            fields["target_command_core_binding"]["prompt_sha256"],
            receipt.details["prompt_sha256"]
        );
        assert_eq!(
            fields["target_command_core_binding"]["prompt_bytes"],
            receipt.details["prompt_bytes"]
        );

        let error = command_target_receipt_fields(
            &db,
            OPERATION_ID,
            &operation,
            &request,
            &route,
            "another-binding",
            4,
        )
        .unwrap_err();
        assert_eq!(error.code, "FORBIDDEN");
        let error = command_target_receipt_fields(
            &db,
            OPERATION_ID,
            &operation,
            &request,
            &route,
            "binding-command",
            5,
        )
        .unwrap_err();
        assert_eq!(error.code, "FORBIDDEN");
    }
}
