//! Built-in HTTP modules use the existing operation/observation boundary. They
//! own clients only; neither restart nor shutdown owns an OpenCode process.
mod execution_reads;
mod result_reads;
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
    collections::BTreeMap,
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
        if service.is_none_or(|s| b["route"]["native_options"]["service_id"] == s) {
            result.push(b);
        }
    }
    Ok(result)
}
fn attach(db: &mut Connection, binding: &Value, boot: &str) -> Result<Principal> {
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
    tx.execute("UPDATE bindings SET state=CASE WHEN state='ready' THEN 'reconciling' ELSE state END,state_json=json_set(state_json,'$.bridge_boot_id',?3,'$.module_link_id',?4,'$.connection','connecting','$.native_owner','external_shared_service') WHERE binding_id=?1 AND generation=?2",params![id,generation,boot,p.link_id])?;
    tx.commit()?;
    Ok(p)
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
impl Store {
    pub async fn supervise_opencode(self, mut stopping: watch::Receiver<bool>) {
        let mut workers: BTreeMap<String, JoinHandle<()>> = BTreeMap::new();
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
            match self.run(|db| bindings(db, None)).await {
                Ok(bindings) => {
                    for b in bindings {
                        let Ok(options) = Options::parse(&b["route"]["native_options"]) else {
                            continue;
                        };
                        if let std::collections::btree_map::Entry::Vacant(entry) =
                            workers.entry(options.service_id.clone())
                        {
                            let store = self.clone();
                            let stop = stopping.clone();
                            entry.insert(tokio::spawn(async move {
                                store.drive_opencode(&options.service_id, stop).await;
                            }));
                        }
                    }
                }
                Err(e) => eprintln!("OpenCode supervisor: {}", e.code),
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
    async fn record_oc_outcome(&self, p: &Principal, outcome: RuntimeOutcome) -> Result<()> {
        let p = p.clone();
        let value = json!(outcome);
        self.run(move |db| runtime::outcome(db, &p, &value)).await?;
        self.changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
        Ok(())
    }
    async fn oc_connection(
        &self,
        p: &Principal,
        connected: bool,
        error: Option<&Error>,
    ) -> Result<()> {
        let p = p.clone();
        let detail = error.map(oc::diagnostic).unwrap_or(Value::Null);
        self.run(move|db|{
            let (id,generation,_)=runtime::scope(db,&p,true)?;
            db.execute("UPDATE bindings SET state=CASE WHEN NOT ?3 AND state='ready' THEN 'reconciling' ELSE state END,state_json=json_set(state_json,'$.connection',?4,'$.native_transport_error',json(?5)) WHERE binding_id=?1 AND generation=?2",
                params![id,generation,connected,if connected{"connected"}else{"native_unavailable"},model::canonical(&detail)?])?;Ok(())
        }).await
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
            .verify_binding(
                root,
                options,
                model::text(b, "binding_id")?,
                model::positive(b, "generation")?,
            )
            .await?;
        let snapshot = tokio::time::timeout(
            Duration::from_secs(20),
            service.snapshot(root, &b["observation"]["native"]),
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
            // Finding a root cannot settle an unknown create. Ownership/settings
            // readback must succeed and the original open must already be settled.
            db.execute("UPDATE bindings SET state='ready' WHERE binding_id=?1 AND generation=?2 AND state='reconciling' AND EXISTS(SELECT 1 FROM operations WHERE binding_id=?1 AND binding_generation=?2 AND method='agent.open' AND state='settled')",params![id,generation])?;
            Ok(())
        }).await
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
                    match self.run(move |db| attach(db, &binding, &boot)).await {
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
                        execution_reader
                            .schedule(
                                self,
                                service,
                                &options,
                                &list,
                                &principals,
                                stopping.clone(),
                            )
                            .await;
                        for b in list {
                            if *stopping.borrow() {
                                break;
                            }
                            let Some(p) = principals.get(&(
                                b["binding_id"].as_str().unwrap_or_default().to_owned(),
                                b["generation"].as_i64().unwrap_or(0),
                            )) else {
                                continue;
                            };
                            let binding_options =
                                match Options::parse(&b["route"]["native_options"]) {
                                    Ok(o) => o,
                                    Err(_) => continue,
                                };
                            if binding_options.connection_file != options.connection_file
                                || binding_options.expected_version != options.expected_version
                            {
                                let _=self.oc_connection(p,false,Some(&Error::new("NATIVE_SERVICE_CONFLICT","one service namespace cannot select multiple connection records/versions"))).await;
                                continue;
                            }
                            let _ = self.oc_connection(p, true, None).await;
                            // Restart recovery reads retained operation identities. It
                            // never recreates a session or resubmits an original input.
                            let principal = p.clone();
                            let pending=self.run(move|db|{
                            let (id,generation,_)=runtime::scope(db,&principal,true)?;
                            let mut stmt=db.prepare("SELECT operation_id FROM operations WHERE binding_id=?1 AND binding_generation=?2 AND state IN ('sending','native_accepted','outcome_unknown') AND method IN ('agent.open','task.dispatch','agent.send') ORDER BY created_at_ms LIMIT 16")?;
                            let ids=stmt.query_map(params![id,generation],|r|r.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
                            ids.into_iter().map(|id|original(db,&principal,&id)).collect::<Result<Vec<_>>>()
                        }).await;
                            if let Ok(pending) = pending {
                                for command in pending {
                                    if *stopping.borrow() {
                                        break;
                                    }
                                    let result =
                                        service.reconcile(&command, &binding_options).await;
                                    if matches!(result.outcome, EffectOutcome::Applied) {
                                        let _ = self.record_oc_outcome(p, result).await;
                                    }
                                }
                            }
                            let principal = p.clone();
                            let fresh = self
                                .run(move |db| {
                                    runtime::scope(db, &principal, true).map(|(_, _, b)| b)
                                })
                                .await;
                            if let Ok(fresh) = &fresh
                                && fresh["native_root_id"].is_string()
                                && let Err(e) = self
                                    .oc_snapshot(
                                        p,
                                        service,
                                        &binding_options,
                                        fresh,
                                        &boot,
                                        &event_state,
                                    )
                                    .await
                            {
                                let _ = self.oc_connection(p, false, Some(&e)).await;
                            }
                            let principal = p.clone();
                            let next = self.run(move |db| runtime::next(db, &principal)).await;
                            let command = next.ok().and_then(|v| {
                                serde_json::from_value::<RuntimeCommand>(v["command"].clone()).ok()
                            });
                            let command = if let Some(command) = command {
                                command
                            } else {
                                // Explicit queued work wins. Recover at most one read per
                                // binding per five seconds, rotating past unresolved reads.
                                let key = (
                                    model::text(&b, "binding_id").unwrap_or_default().to_owned(),
                                    b["generation"].as_i64().unwrap_or(0),
                                );
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
                                let principal = p.clone();
                                let read = self
                                    .run(move |db| result_reads::next_read(db, &principal, &after))
                                    .await;
                                let Ok(Some(command)) = read else {
                                    continue;
                                };
                                result_retries
                                    .insert(key, (command.operation_id.clone(), Instant::now()));
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
                                        self.oc_snapshot(
                                            p,
                                            service,
                                            &binding_options,
                                            &fresh,
                                            &boot,
                                            &event_state,
                                        )
                                        .await
                                    }
                                    Err(e) => Err(e),
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
                                        Err(e) => oc::diagnostic(&e),
                                    },
                                }
                            } else if command.method == "agent.reconcile" {
                                let principal = p.clone();
                                let target = command.input["operation_id"]
                                    .as_str()
                                    .unwrap_or_default()
                                    .to_owned();
                                let target =
                                    self.run(move |db| original(db, &principal, &target)).await;
                                let resolved = if let Ok(target) = target {
                                    if target.method == "agent.result" {
                                        self.oc_result_outcome(
                                            p,
                                            service,
                                            &binding_options,
                                            &target,
                                        )
                                        .await
                                    } else {
                                        let r = service.reconcile(&target, &binding_options).await;
                                        let resolved = matches!(r.outcome, EffectOutcome::Applied);
                                        self.record_oc_outcome(p, r).await.is_ok() && resolved
                                    }
                                } else {
                                    false
                                };
                                RuntimeOutcome {
                                    operation_id: command.operation_id.clone(),
                                    outcome: EffectOutcome::Applied,
                                    native_root_id: command.native_root_id.clone(),
                                    native_scope_key: Some(binding_options.scope()),
                                    turn_id: None,
                                    native_input_id: None,
                                    details: json!({"completion_condition":"readback_attempted","resolved":resolved,"replayed_native_input":false}),
                                }
                            } else {
                                service.execute(&command, &binding_options).await
                            };
                            if let Err(e) = self.record_oc_outcome(p, result).await {
                                eprintln!("OpenCode receipt: {}", e.code);
                            }
                        }
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

#[cfg(test)]
mod tests;
