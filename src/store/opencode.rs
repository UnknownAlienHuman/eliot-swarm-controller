//! External HTTP bindings attach to an explicit service; the separate owned
//! route keeps its helper lifecycle and exact process proof binding-scoped.
mod execution_reads;
mod result_reads;
pub(crate) use super::launcher_owned_service::owned_service_for_binding;
use super::{Store, meta, operations, runtime, set_meta, tasks};
use crate::{
    error::{Error, Result},
    model::{self, Principal, Role},
    runtime::{
        EffectOutcome, RuntimeCommand, RuntimeOutcome,
        opencode_v2::{self as oc, Options, Service},
    },
};
use rusqlite::{Connection, TransactionBehavior, params};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant},
};
use tokio::{sync::watch, task::JoinHandle};

fn bindings(db: &Connection, service: Option<&str>) -> Result<Vec<Value>> {
    let mut stmt=db.prepare("SELECT binding_id,generation FROM bindings WHERE released_at_ms IS NULL AND json_extract(route_json,'$.runtime')=?1 AND module_artifact_id=?2 ORDER BY created_at_ms,binding_id")?;
    let ids = stmt
        .query_map(params![oc::RUNTIME, oc::ARTIFACT_ID], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut result = Vec::new();
    for (id, generation) in ids {
        let b = operations::get_binding(db, &id, generation)?;
        if !b["route"]["owned_service"].is_object()
            && service.is_none_or(|s| b["route"]["native_options"]["service_id"] == s)
        {
            result.push(b);
        }
    }
    Ok(result)
}

fn owned_bindings(db: &Connection) -> Result<Vec<Value>> {
    let mut stmt = db.prepare(
        "SELECT binding_id,generation FROM bindings WHERE released_at_ms IS NULL AND json_extract(route_json,'$.runtime')=?1 AND module_artifact_id=?2 AND json_type(route_json,'$.owned_service')='object' ORDER BY created_at_ms,binding_id,generation",
    )?;
    let ids = stmt
        .query_map(params![oc::RUNTIME, oc::ARTIFACT_ID], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(stmt);
    ids.into_iter()
        .map(|(id, generation)| operations::get_binding(db, &id, generation))
        .collect()
}

fn active_owned_binding(db: &Connection, id: &str, generation: i64) -> Result<Option<Value>> {
    let active: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM bindings WHERE binding_id=?1 AND generation=?2
          AND released_at_ms IS NULL AND json_extract(route_json,'$.runtime')=?3
          AND module_artifact_id=?4 AND json_type(route_json,'$.owned_service')='object')",
        params![id, generation, oc::RUNTIME, oc::ARTIFACT_ID],
        |row| row.get(0),
    )?;
    if !active {
        return Ok(None);
    }
    operations::get_binding(db, id, generation).map(Some)
}
/// Child sessions with a bound producer that no terminal evidence has
/// discharged. The snapshot reader tracks exactly these (plus active and
/// already-recorded open periods); unbound children are never log-read.
fn bound_child_sessions(db: &Connection, p: &Principal) -> Result<BTreeSet<String>> {
    let (id, generation, binding) = runtime::scope(db, p, true)?;
    let root = binding["native_root_id"].as_str().unwrap_or_default();
    let mut stmt = db.prepare(
        "SELECT producers_json FROM attempts WHERE binding_id=?1 AND binding_generation=?2 AND released_at_ms IS NULL",
    )?;
    let rows = stmt
        .query_map(params![id, generation], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(stmt);
    let mut out = BTreeSet::new();
    for raw in rows {
        let producers: Vec<Value> = serde_json::from_str(&raw)?;
        for producer in producers {
            if let Some(session) = producer["native_session_id"]
                .as_str()
                .filter(|s| !s.is_empty() && *s != root)
                && !matches!(
                    producer["disposition"].as_str(),
                    Some("completed" | "failed" | "cancelled")
                )
            {
                out.insert(session.to_owned());
            }
        }
    }
    Ok(out)
}
fn attach(
    db: &mut Connection,
    binding: &Value,
    boot: &str,
    native_owner: &str,
) -> Result<Principal> {
    let id = model::text(binding, "binding_id")?;
    let generation = model::positive(binding, "generation")?;
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let b = operations::get_binding(&tx, id, generation)?;
    if !b["released_at_ms"].is_null()
        || b["route"]["runtime"] != oc::RUNTIME
        || b["route"]["module_artifact_id"] != oc::ARTIFACT_ID
    {
        return Err(Error::new(
            "BINDING_CLOSED",
            "builtin module binding changed",
        ));
    }
    let client = format!("builtin:opencode:{id}:{generation}");
    if let Some(existing) = meta(&tx, &format!("client:{client}"))? {
        if existing["builtin_runtime"] != oc::RUNTIME
            || existing["disabled"] == true
            || existing["binding_id"] != id
            || existing["binding_generation"] != generation
            || b["observation"]["module_client_id"] != client
        {
            return Err(Error::new(
                "MODULE_OWNER_MISMATCH",
                "refusing to replace another module owner",
            ));
        }
    } else {
        runtime::register(
            &tx,
            &json!({"binding_id":id,"binding_generation":generation}),
            &client,
        )?;
        // No token hash: there is no credential that can authenticate this client
        // through IPC. Only this Store-owned worker constructs its Principal.
        set_meta(
            &tx,
            &format!("client:{client}"),
            &json!({"role":"module","disabled":false,"builtin_runtime":oc::RUNTIME,"binding_id":id,"binding_generation":generation}),
        )?;
    }
    let p = Principal {
        client_id: client,
        link_id: model::new_id(),
        role: Role::Module,
    };
    tx.execute("UPDATE operations SET state='outcome_unknown',updated_at_ms=?3 WHERE binding_id=?1 AND binding_generation=?2 AND state IN ('sending','native_accepted')",params![id,generation,model::now_ms()?])?;
    super::capacity::sync_binding(&tx, id, generation, model::now_ms()?)?;
    tx.execute("UPDATE bindings SET state=CASE WHEN state='ready' THEN 'reconciling' ELSE state END,state_json=json_set(state_json,'$.bridge_boot_id',?3,'$.module_link_id',?4,'$.connection','connecting','$.native_owner',?5) WHERE binding_id=?1 AND generation=?2",params![id,generation,boot,p.link_id,native_owner])?;
    tx.commit()?;
    Ok(p)
}

fn write_connection_state(
    db: &mut Connection,
    principal: &Principal,
    connected: bool,
    detail: &Value,
    failure_code: Option<&str>,
) -> Result<()> {
    let (id, generation, _) = runtime::scope(db, principal, true)?;
    let connection = if connected {
        "connected"
    } else {
        "native_unavailable"
    };
    let native_transport_error = model::canonical(detail)?;
    if let Some(code) = failure_code.and_then(safe_runtime_error_code) {
        let latest_native_failure = model::canonical(&json!({
            "code":code,
            "recorded_at_ms":model::now_ms()?
        }))?;
        db.execute("UPDATE bindings SET state=CASE WHEN NOT ?3 AND state='ready' THEN 'reconciling' ELSE state END,state_json=json_set(state_json,'$.connection',?4,'$.native_transport_error',json(?5),'$.latest_native_failure',json(?6)) WHERE binding_id=?1 AND generation=?2",
            params![id,generation,connected,connection,native_transport_error,latest_native_failure])?;
    } else {
        db.execute("UPDATE bindings SET state=CASE WHEN NOT ?3 AND state='ready' THEN 'reconciling' ELSE state END,state_json=json_set(state_json,'$.connection',?4,'$.native_transport_error',json(?5)) WHERE binding_id=?1 AND generation=?2",
            params![id,generation,connected,connection,native_transport_error])?;
    }
    Ok(())
}
fn original(db: &Connection, p: &Principal, id: &str) -> Result<RuntimeCommand> {
    let (binding, generation, b) = runtime::scope(db, p, true)?;
    let o = operations::get_operation(db, id)?;
    if o["binding_id"] != binding || o["binding_generation"] != generation {
        return Err(Error::new(
            "FORBIDDEN",
            "operation belongs to another native binding",
        ));
    }
    let raw: String = db.query_row(
        "SELECT original_request_json FROM operations WHERE operation_id=?1",
        [id],
        |r| r.get(0),
    )?;
    let mut input: Value = serde_json::from_str(&raw)?;
    let method = model::text(&o, "method")?.to_owned();
    if method == "task.dispatch" {
        let a = tasks::get_attempt(db, model::text(&input, "attempt_id")?)?;
        input["task_snapshot"] = a["task_snapshot"].clone();
    }
    Ok(RuntimeCommand {
        operation_id: id.into(),
        method,
        created_at_ms: model::positive(&o, "created_at_ms")?,
        binding_id: binding,
        generation,
        native_root_id: b["native_root_id"].as_str().map(str::to_owned),
        route: b["route"].clone(),
        input,
    })
}

fn original_with_config(
    db: &Connection,
    p: &Principal,
    id: &str,
    config: &crate::config::Config,
) -> Result<RuntimeCommand> {
    let mut command = original(db, p, id)?;
    if command.route["owned_service"].is_object() {
        let options = super::launcher_owned_service::effective_options_for_binding(
            db,
            config,
            &command.binding_id,
            command.generation,
        )?
        .ok_or_else(|| {
            Error::new(
                "NATIVE_OWNED_SERVICE_UNKNOWN",
                "owned service has no exact retained runtime options",
            )
        })?;
        command.route["native_options"] = serde_json::to_value(options)?;
    }
    Ok(command)
}
impl Store {
    async fn close_owned_opencode_service(
        &self,
        handle: crate::runtime::opencode_v2::owned_service::OwnedServiceHandle,
    ) {
        match handle.close_gracefully().await {
            Ok(_departure) => {
                if let Err(error) = self.reconcile_owned_opencode_departures_once().await {
                    eprintln!("OpenCode owned-service departure: {}", error.code);
                }
            }
            Err(error) if error.code == "OWNED_SERVICE_OWNER_UNAVAILABLE" => {}
            Err(error) => eprintln!("OpenCode owned-service owner: {}", error.code),
        }
    }

    pub async fn supervise_opencode(self, mut stopping: watch::Receiver<bool>) -> Result<()> {
        let (worker_stop, worker_stopping) = watch::channel(false);
        let mut workers: BTreeMap<String, JoinHandle<()>> = BTreeMap::new();
        let mut owned_workers: BTreeMap<(String, i64), JoinHandle<()>> = BTreeMap::new();
        let mut changed = self.changed.subscribe();
        let result = async {
            while !*stopping.borrow() {
                let finished = workers
                    .iter()
                    .filter(|(_, task)| task.is_finished())
                    .map(|(key, _)| key.clone())
                    .collect::<Vec<_>>();
                for key in finished {
                    if let Some(task) = workers.remove(&key) {
                        let result = task.await;
                        if !*stopping.borrow() {
                            self.recover_finished_shared_worker(&key, result).await?;
                        }
                    }
                }
                let finished_owned = owned_workers
                    .iter()
                    .filter(|(_, task)| task.is_finished())
                    .map(|(key, _)| key.clone())
                    .collect::<Vec<_>>();
                for key in finished_owned {
                    if let Some(task) = owned_workers.remove(&key) {
                        let result = task.await;
                        if !*stopping.borrow() {
                            self.recover_finished_owned_worker(&key.0, key.1, result)
                                .await?;
                        }
                    }
                }
                let active_bindings = self.run(|db| bindings(db, None)).await?;
                for b in active_bindings {
                    let Ok(options) = Options::parse(&b["route"]["native_options"]) else {
                        continue;
                    };
                    if let std::collections::btree_map::Entry::Vacant(entry) =
                        workers.entry(options.service_id.clone())
                    {
                        let store = self.clone();
                        let stop = worker_stopping.clone();
                        entry.insert(tokio::spawn(async move {
                            store.drive_opencode(&options.service_id, stop).await;
                        }));
                    }
                }
                let active_owned_bindings = self.run(|db| owned_bindings(db)).await?;
                for binding in active_owned_bindings {
                    let key = (
                        binding["binding_id"]
                            .as_str()
                            .unwrap_or_default()
                            .to_owned(),
                        binding["generation"].as_i64().unwrap_or(0),
                    );
                    if let std::collections::btree_map::Entry::Vacant(entry) =
                        owned_workers.entry(key.clone())
                    {
                        let store = self.clone();
                        let stop = worker_stopping.clone();
                        entry.insert(tokio::spawn(async move {
                            store.drive_owned_opencode(&key.0, key.1, stop).await;
                        }));
                    }
                }
                tokio::select! {
                    _ = stopping.changed() => {},
                    _ = changed.changed() => {},
                    _ = tokio::time::sleep(Duration::from_secs(2)) => {},
                }
            }
            Ok(())
        }
        .await;
        finish_opencode_supervisor(worker_stop, workers, owned_workers, result).await
    }

    async fn recover_finished_shared_worker(
        &self,
        service_id: &str,
        result: std::result::Result<(), tokio::task::JoinError>,
    ) -> Result<()> {
        let code = worker_failure_code(&result);
        let service_id = service_id.to_owned();
        let boot = model::new_id();
        let detail = oc::diagnostic(&Error::new(
            code,
            "builtin OpenCode worker terminated unexpectedly",
        ));
        let recovered = self
            .run(move |db| {
                // Re-resolve the exact live service scope in the same Store turn
                // as reattachment and uncertainty recording. A release between
                // worker exit and this point is an expected no-op.
                let active = bindings(db, Some(&service_id))?;
                let mut recovered = false;
                for binding in active {
                    let principal = attach(db, &binding, &boot, "external_shared_service")?;
                    write_connection_state(db, &principal, false, &detail, Some(code))?;
                    recovered = true;
                }
                Ok(recovered)
            })
            .await?;
        if recovered {
            self.changed
                .send_modify(|revision| *revision = revision.wrapping_add(1));
        }
        Ok(())
    }

    async fn recover_finished_owned_worker(
        &self,
        binding_id: &str,
        generation: i64,
        result: std::result::Result<(), tokio::task::JoinError>,
    ) -> Result<()> {
        let code = worker_failure_code(&result);
        let binding_id = binding_id.to_owned();
        let boot = model::new_id();
        let detail = oc::diagnostic(&Error::new(
            code,
            "builtin OpenCode owned worker terminated unexpectedly",
        ));
        let recovered = self
            .run(move |db| {
                // The binding ID and generation are both part of this worker's
                // retained identity; a replacement generation is never adopted.
                let Some(binding) = active_owned_binding(db, &binding_id, generation)? else {
                    return Ok(false);
                };
                let principal = attach(db, &binding, &boot, "owned_fresh_service")?;
                write_connection_state(db, &principal, false, &detail, Some(code))?;
                Ok(true)
            })
            .await?;
        if recovered {
            self.changed
                .send_modify(|revision| *revision = revision.wrapping_add(1));
        }
        Ok(())
    }
    async fn record_oc_outcome(&self, p: &Principal, outcome: RuntimeOutcome) -> Result<()> {
        let p = p.clone();
        let value = json!(outcome);
        self.run(move |db| runtime::outcome(db, &p, &value)).await?;
        self.changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
        Ok(())
    }

    async fn oc_reconcile_outcome(
        &self,
        p: &Principal,
        service: &Service,
        binding_options: &Options,
        command: &RuntimeCommand,
    ) -> RuntimeOutcome {
        let target_id = command.input["operation_id"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        let config = self.config.clone();
        let principal = p.clone();
        let target = self
            .run(move |db| original_with_config(db, &principal, &target_id, &config))
            .await;
        let resolved = match target {
            Ok(target) if target.method == "agent.result" => {
                self.oc_result_outcome(p, service, binding_options, &target)
                    .await
            }
            Ok(target) => {
                let result = service.reconcile(&target, binding_options).await;
                let resolved = matches!(result.outcome, EffectOutcome::Applied);
                self.record_oc_outcome(p, result).await.is_ok() && resolved
            }
            Err(error) => {
                return RuntimeOutcome {
                    operation_id: command.operation_id.clone(),
                    outcome: EffectOutcome::Unknown,
                    native_root_id: command.native_root_id.clone(),
                    native_scope_key: Some(binding_options.scope()),
                    turn_id: None,
                    native_input_id: None,
                    details: json!({
                        "stage":"reconcile_target_load",
                        "code":safe_runtime_error_code(&error.code)
                            .unwrap_or("NATIVE_RECONCILE_TARGET_LOAD_FAILED"),
                        "replayed_native_input":false
                    }),
                };
            }
        };
        RuntimeOutcome {
            operation_id: command.operation_id.clone(),
            outcome: EffectOutcome::Applied,
            native_root_id: command.native_root_id.clone(),
            native_scope_key: Some(binding_options.scope()),
            turn_id: None,
            native_input_id: None,
            details: json!({
                "completion_condition":"readback_attempted",
                "resolved":resolved,
                "replayed_native_input":false
            }),
        }
    }

    async fn oc_connection(
        &self,
        p: &Principal,
        connected: bool,
        error: Option<&Error>,
    ) -> Result<()> {
        let p = p.clone();
        let detail = error.map(oc::diagnostic).unwrap_or(Value::Null);
        let failure_code = error.map(|error| {
            safe_runtime_error_code(&error.code)
                .unwrap_or("NATIVE_CONNECTION_FAILURE")
                .to_owned()
        });
        self.run(move |db| {
            write_connection_state(db, &p, connected, &detail, failure_code.as_deref())
        })
        .await
    }
    async fn oc_snapshot(
        &self,
        p: &Principal,
        service: &Service,
        options: &Options,
        b: &Value,
        boot: &str,
        events: &oc::EventState,
    ) -> Result<()> {
        let root = model::text(b, "native_root_id")?;
        service
            .verify_binding_identity(
                root,
                options,
                model::text(b, "binding_id")?,
                model::positive(b, "generation")?,
            )
            .await?;
        let principal = p.clone();
        let bound_children = self
            .run(move |db| bound_child_sessions(db, &principal))
            .await?;
        let snapshot = tokio::time::timeout(
            Duration::from_secs(20),
            service.snapshot(root, &b["observation"]["native"], &bound_children),
        )
        .await
        .map_err(|_| Error::new("NATIVE_SNAPSHOT_TIMEOUT", "bounded readback did not finish"))??;
        let mut state = snapshot.state;
        state["boot_id"] = json!(boot);
        state["native_scope_key"] = json!(options.scope());
        state["event_stream"] = json!(events);
        let p = p.clone();
        let event = model::new_id();
        self.run(move|db|{
            let (id,generation,current) = runtime::scope(db,&p,true)?;
            if current["native_scope_key"] != state["native_scope_key"] || current["native_root_id"] != state["native_root_id"] {
                return Err(Error::new("NATIVE_SCOPE_MISMATCH","snapshot has another service namespace/root"));
            }
            runtime::observe(db,&p,&json!({"event_id":event,"state":state}))?;
            // Finding a root cannot settle an unknown create. Exact ownership/location
            // readback must succeed and the original open must already be settled.
            db.execute("UPDATE bindings SET state='ready' WHERE binding_id=?1 AND generation=?2 AND state='reconciling' AND COALESCE(json_extract(state_json,'$.recovery_required'),0)=0 AND EXISTS(SELECT 1 FROM operations WHERE binding_id=?1 AND binding_generation=?2 AND method='agent.open' AND state='settled') AND NOT EXISTS(SELECT 1 FROM operations WHERE binding_id=?1 AND binding_generation=?2 AND state IN ('sending','native_accepted','outcome_unknown') AND method IN ('agent.open','task.dispatch','agent.send'))",params![id,generation])?;
            Ok(())
        }).await
    }

    // A single pass keeps explicit references to shared read/retry state and
    // the optional owned route; none of these are independent authorities.
    #[allow(clippy::too_many_arguments)]
    async fn drive_opencode_bindings(
        &self,
        bindings: &[Value],
        principals: &BTreeMap<(String, i64), Principal>,
        service: &Service,
        shared_options: &Options,
        owned_options: Option<&Options>,
        boot: &str,
        event_state: &oc::EventState,
        execution_reader: &mut execution_reads::Reader,
        result_retries: &mut BTreeMap<(String, i64), (String, Instant)>,
        stopping: &watch::Receiver<bool>,
    ) {
        execution_reader
            .schedule(
                self,
                service,
                shared_options,
                bindings,
                principals,
                self.config.as_ref(),
                stopping.clone(),
            )
            .await;
        for binding in bindings {
            if *stopping.borrow() {
                break;
            }
            let key = (
                binding["binding_id"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned(),
                binding["generation"].as_i64().unwrap_or(0),
            );
            let Some(p) = principals.get(&key) else {
                continue;
            };
            let binding_options = if let Some(options) = owned_options {
                options.clone()
            } else {
                match Options::parse(&binding["route"]["native_options"]) {
                    Ok(options) => options,
                    Err(_) => continue,
                }
            };
            if binding_options.connection_file != shared_options.connection_file
                || binding_options.expected_version != shared_options.expected_version
            {
                let _ = self
                    .oc_connection(
                        p,
                        false,
                        Some(&Error::new(
                            "NATIVE_SERVICE_CONFLICT",
                            "one service namespace cannot select multiple connection records/versions",
                        )),
                    )
                    .await;
                continue;
            }
            let _ = self.oc_connection(p, true, None).await;
            let config = self.config.clone();
            let principal = p.clone();
            let pending = self
                .run(move |db| {
                    let (id, generation, _) = runtime::scope(db, &principal, true)?;
                    let mut stmt=db.prepare("SELECT operation_id FROM operations WHERE binding_id=?1 AND binding_generation=?2 AND state IN ('sending','native_accepted','outcome_unknown') AND method IN ('agent.open','task.dispatch','agent.send','agent.configure','agent.goal','agent.background') ORDER BY created_at_ms LIMIT 16")?;
                    let ids=stmt.query_map(params![id,generation],|r|r.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
                    ids.into_iter().map(|id|original_with_config(db,&principal,&id,&config)).collect::<Result<Vec<_>>>()
                })
                .await;
            if let Ok(pending) = pending {
                for command in pending {
                    if *stopping.borrow() {
                        break;
                    }
                    let result = service.reconcile(&command, &binding_options).await;
                    if matches!(result.outcome, EffectOutcome::Applied) {
                        let _ = self.record_oc_outcome(p, result).await;
                    }
                }
            }
            let principal = p.clone();
            let fresh = self
                .run(move |db| runtime::scope(db, &principal, true).map(|(_, _, binding)| binding))
                .await;
            if let Ok(fresh) = &fresh
                && fresh["native_root_id"].is_string()
                && let Err(error) = self
                    .oc_snapshot(p, service, &binding_options, fresh, boot, event_state)
                    .await
            {
                let _ = self.oc_connection(p, false, Some(&error)).await;
            }
            let config = self.config.clone();
            let principal = p.clone();
            let next = self
                .run(move |db| runtime::next_with_config(db, &principal, &config))
                .await;
            let next = match next {
                Ok(value) => value,
                Err(error) => {
                    let safe_code = safe_runtime_error_code(&error.code).map(str::to_owned);
                    if owned_options.is_some()
                        && let Some(error_code) = safe_code.clone()
                    {
                        let binding_id = key.0.clone();
                        let generation = key.1;
                        match self
                            .run(move |db| {
                                operations::record_owned_open_dispatch_failure(
                                    db,
                                    &binding_id,
                                    generation,
                                    "runtime_command_select",
                                    "selection_error",
                                    &error_code,
                                    "queued",
                                )
                            })
                            .await
                        {
                            Ok(_) => {}
                            Err(persist_error) => eprintln!(
                                "OpenCode owned dispatch diagnostic: {}",
                                safe_runtime_error_code(&persist_error.code)
                                    .unwrap_or("STORE_ERROR")
                            ),
                        }
                    }
                    eprintln!(
                        "OpenCode command selection: {}",
                        safe_code.as_deref().unwrap_or("RUNTIME_SELECTOR_ERROR")
                    );
                    // Do not turn selector infrastructure or authority errors
                    // into an idle readback pass. A later pass may retry only
                    // the same durable queued Operation.
                    continue;
                }
            };
            let Some(raw_command) = next.get("command") else {
                eprintln!("OpenCode command selection: RUNTIME_COMMAND_RESPONSE_INVALID");
                continue;
            };
            let command = if raw_command.is_null() {
                None
            } else {
                match serde_json::from_value::<RuntimeCommand>(raw_command.clone()) {
                    Ok(command) => Some(command),
                    Err(_) => {
                        // runtime::next commits the sending boundary before it
                        // returns a command. Keep that Operation unresolved;
                        // never misreport it as not dispatched or retry it.
                        eprintln!("OpenCode command decode: RUNTIME_COMMAND_INVALID");
                        continue;
                    }
                }
            };
            let command = if let Some(command) = command {
                command
            } else {
                // Explicit queued work wins. Recover at most one read per binding
                // per five seconds, rotating past unresolved reads.
                if result_retries
                    .get(&key)
                    .is_some_and(|(_, at)| at.elapsed() < Duration::from_secs(5))
                {
                    continue;
                }
                let after = result_retries
                    .get(&key)
                    .map(|(id, _)| id.clone())
                    .unwrap_or_default();
                let config = self.config.clone();
                let principal = p.clone();
                let read = self
                    .run(move |db| result_reads::next_read(db, &principal, &after, &config))
                    .await;
                let Ok(Some(command)) = read else {
                    continue;
                };
                result_retries.insert(key.clone(), (command.operation_id.clone(), Instant::now()));
                command
            };
            if command.method == "agent.result" {
                self.oc_result_outcome(p, service, &binding_options, &command)
                    .await;
                continue;
            }
            let result = if command.method == "agent.refresh" {
                let read = match fresh {
                    Ok(fresh) => {
                        self.oc_snapshot(p, service, &binding_options, &fresh, boot, event_state)
                            .await
                    }
                    Err(error) => Err(error),
                };
                RuntimeOutcome {
                    operation_id: command.operation_id.clone(),
                    outcome: if read.is_ok() {
                        EffectOutcome::Applied
                    } else {
                        EffectOutcome::Unknown
                    },
                    native_root_id: command.native_root_id.clone(),
                    native_scope_key: Some(binding_options.scope()),
                    turn_id: None,
                    native_input_id: None,
                    details: match read {
                        Ok(()) => {
                            json!({"completion_condition":"native_snapshot_recorded","family_complete":false})
                        }
                        Err(error) => oc::diagnostic(&error),
                    },
                }
            } else if command.method == "agent.reconcile" {
                self.oc_reconcile_outcome(p, service, &binding_options, &command)
                    .await
            } else {
                service.execute(&command, &binding_options).await
            };
            if let Err(error) = self.record_oc_outcome(p, result).await {
                eprintln!("OpenCode receipt: {}", error.code);
            }
        }
    }

    async fn drive_owned_opencode(
        &self,
        binding_id: &str,
        generation: i64,
        mut stopping: watch::Receiver<bool>,
    ) {
        let binding_id = binding_id.to_owned();
        let boot = model::new_id();
        let mut changed = self.changed.subscribe();
        let mut execution_reader = execution_reads::Reader::default();
        let mut result_retries: BTreeMap<(String, i64), (String, Instant)> = BTreeMap::new();

        // The Store is the only process-start authority. Retrying this call can
        // only read the exact retained intent after the durable unknown boundary;
        // it cannot mint a replacement process for an unresolved effect.
        let handle = loop {
            if *stopping.borrow() {
                return;
            }
            match self
                .ensure_owned_opencode_service(&binding_id, generation, stopping.clone())
                .await
            {
                Ok(handle) => break handle,
                Err(error) => {
                    eprintln!("OpenCode owned-service binding: {}", error.code);
                    let id = binding_id.clone();
                    let still_active = self
                        .run(move |db| active_owned_binding(db, &id, generation))
                        .await;
                    if !matches!(still_active, Ok(Some(_))) {
                        return;
                    }
                    tokio::select! {
                        _ = stopping.changed() => {},
                        _ = changed.changed() => {},
                        _ = tokio::time::sleep(Duration::from_secs(5)) => {},
                    }
                }
            }
        };
        let service = handle.service().clone();
        let options = handle.options().clone();

        let initial_id = binding_id.clone();
        let initial = self
            .run(move |db| active_owned_binding(db, &initial_id, generation))
            .await;
        let binding = match initial {
            Ok(Some(binding)) => binding,
            Ok(None) => {
                execution_reader.close().await;
                self.close_owned_opencode_service(handle).await;
                return;
            }
            Err(error) => {
                eprintln!("OpenCode owned-service binding: {}", error.code);
                execution_reader.close().await;
                self.close_owned_opencode_service(handle).await;
                return;
            }
        };
        let attach_binding = binding.clone();
        let attach_boot = boot.clone();
        let principal = self
            .run(move |db| attach(db, &attach_binding, &attach_boot, "owned_fresh_service"))
            .await;
        let principal = match principal {
            Ok(principal) => principal,
            Err(error) => {
                eprintln!("OpenCode owned-service attachment: {}", error.code);
                execution_reader.close().await;
                self.close_owned_opencode_service(handle).await;
                return;
            }
        };
        let key = (binding_id.clone(), generation);
        let principals = BTreeMap::from([(key.clone(), principal.clone())]);
        let event_state = oc::EventState {
            connected: false,
            revision: 0,
            gaps: 0,
            last_gap: None,
        };

        while !*stopping.borrow() {
            let id = binding_id.clone();
            let _binding = match self
                .run(move |db| active_owned_binding(db, &id, generation))
                .await
            {
                Ok(Some(binding)) => binding,
                Ok(None) => break,
                Err(error) => {
                    eprintln!("OpenCode owned-service scope: {}", error.code);
                    tokio::select! {
                        _ = stopping.changed() => {},
                        _ = changed.changed() => {},
                        _ = tokio::time::sleep(Duration::from_secs(5)) => {},
                    }
                    continue;
                }
            };

            let scope_principal = principal.clone();
            let live_scope = self
                .run(move |db| runtime::scope(db, &scope_principal, true).map(|(_, _, b)| b))
                .await;
            let binding = match live_scope {
                Ok(binding) => binding,
                Err(error) => {
                    if matches!(
                        error.code.as_str(),
                        "BINDING_CLOSED" | "UNAUTHORIZED" | "STALE_LINK"
                    ) {
                        break;
                    }
                    eprintln!("OpenCode owned-service authority: {}", error.code);
                    tokio::select! {
                        _ = stopping.changed() => {},
                        _ = changed.changed() => {},
                        _ = tokio::time::sleep(Duration::from_secs(5)) => {},
                    }
                    continue;
                }
            };

            match service.verify().await {
                Ok(()) => {
                    let bindings = [binding];
                    self.drive_opencode_bindings(
                        &bindings,
                        &principals,
                        &service,
                        &options,
                        Some(&options),
                        &boot,
                        &event_state,
                        &mut execution_reader,
                        &mut result_retries,
                        &stopping,
                    )
                    .await;
                }
                Err(error) => {
                    let _ = self.oc_connection(&principal, false, Some(&error)).await;
                }
            }
            tokio::select! {
                _ = stopping.changed() => {},
                _ = changed.changed() => {},
                _ = tokio::time::sleep(Duration::from_secs(5)) => {},
            }
        }

        execution_reader.close().await;
        let _ = self.oc_connection(&principal, false, None).await;
        self.close_owned_opencode_service(handle).await;
    }

    async fn drive_opencode(&self, service_id: &str, mut stopping: watch::Receiver<bool>) {
        let boot = model::new_id();
        let mut principals = BTreeMap::new();
        let mut execution_reader = execution_reads::Reader::default();
        let mut result_retries: BTreeMap<(String, i64), (String, Instant)> = BTreeMap::new();
        let mut connection = None;
        let mut events: Option<oc::EventReader> = None;
        let mut changed = self.changed.subscribe();
        while !*stopping.borrow() {
            let service_id = service_id.to_owned();
            let list = match self.run(move |db| bindings(db, Some(&service_id))).await {
                Ok(v) => v,
                Err(_) => break,
            };
            if list.is_empty() {
                break;
            }
            let options = match Options::parse(&list[0]["route"]["native_options"]) {
                Ok(o) => o,
                Err(_) => break,
            };
            for b in &list {
                let key = (
                    b["binding_id"].as_str().unwrap_or_default().to_owned(),
                    b["generation"].as_i64().unwrap_or(0),
                );
                if let std::collections::btree_map::Entry::Vacant(entry) = principals.entry(key) {
                    let binding = b.clone();
                    let boot = boot.clone();
                    match self
                        .run(move |db| attach(db, &binding, &boot, "external_shared_service"))
                        .await
                    {
                        Ok(p) => {
                            entry.insert(p);
                        }
                        Err(e) => eprintln!("OpenCode attachment: {}", e.code),
                    }
                }
            }
            if connection.is_none() {
                match Service::connect(&options).await {
                    Ok(client) => {
                        events = Some(client.events(stopping.clone()));
                        connection = Some(client);
                    }
                    Err(e) => {
                        for p in principals.values() {
                            let _ = self.oc_connection(p, false, Some(&e)).await;
                        }
                    }
                }
            }
            if let (Some(service), Some(event_reader)) = (&connection, &events) {
                let event_state = event_reader.state.borrow().clone();
                match service.verify().await {
                    Err(e) => {
                        for p in principals.values() {
                            let _ = self.oc_connection(p, false, Some(&e)).await;
                        }
                        events = None;
                        connection = None;
                    }
                    Ok(()) => {
                        self.drive_opencode_bindings(
                            &list,
                            &principals,
                            service,
                            &options,
                            None,
                            &boot,
                            &event_state,
                            &mut execution_reader,
                            &mut result_retries,
                            &stopping,
                        )
                        .await;
                    }
                }
            }
            // Bounded readback is supplemental to notifications; neither wake nor
            // timeout is native completion or authorization for a replacement run.
            // Coalesce event storms rather than one readback for each token.
            tokio::time::sleep(Duration::from_millis(200)).await;
            tokio::select! {
                _=stopping.changed()=>{},
                _=changed.changed()=>{},
                _=tokio::time::sleep(Duration::from_secs(5))=>{},
                _=async { match events.as_mut() { Some(reader)=>{let _=reader.state.changed().await;},None=>std::future::pending::<()>().await } }=>{},
            }
        }
        execution_reader.close().await;
        for p in principals.values() {
            let _ = self.oc_connection(p, false, None).await;
        }
    }
}

fn safe_runtime_error_code(code: &str) -> Option<&str> {
    (!code.is_empty()
        && code.len() <= 64
        && code.as_bytes().first().is_some_and(u8::is_ascii_uppercase)
        && code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_'))
    .then_some(code)
}

fn worker_failure_code(result: &std::result::Result<(), tokio::task::JoinError>) -> &'static str {
    match result {
        Ok(()) => "NATIVE_WORKER_EXITED",
        Err(error) if error.is_panic() => "NATIVE_WORKER_PANICKED",
        Err(error) if error.is_cancelled() => "NATIVE_WORKER_CANCELLED",
        Err(_) => "NATIVE_WORKER_FAILED",
    }
}

async fn finish_opencode_supervisor(
    worker_stop: watch::Sender<bool>,
    workers: BTreeMap<String, JoinHandle<()>>,
    owned_workers: BTreeMap<(String, i64), JoinHandle<()>>,
    result: Result<()>,
) -> Result<()> {
    let _ = worker_stop.send(true);
    for worker in workers.into_values() {
        let _ = worker.await;
    }
    for worker in owned_workers.into_values() {
        let _ = worker.await;
    }
    result
}

#[cfg(test)]
mod tests;

#[cfg(test)]
#[path = "opencode/reconcile_failure_tests.rs"]
mod reconcile_failure_tests;

#[cfg(test)]
#[path = "opencode/worker_failure_tests.rs"]
mod worker_failure_tests;
