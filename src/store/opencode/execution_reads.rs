//! Native input disposition is separate from the immutable admission receipt.
use super::{Store, operations, original_with_config, runtime, tasks};
use crate::{
    error::{Error, Result},
    model::{self, Principal},
    runtime::{
        EffectOutcome, RuntimeCommand, RuntimeOutcome,
        opencode_v2::{self as oc, ExecutionRead, Options, Service},
    },
};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};
use tokio::{sync::watch, task::JoinHandle};

type BindingKey = (String, i64);

/// One cancellable GET task per service; it cannot stall queued native replies.
/// Rotate both bindings and operations instead of starving later unknown inputs.
#[derive(Default)]
pub(super) struct Reader {
    task: Option<JoinHandle<()>>,
    last_binding: Option<BindingKey>,
    retries: BTreeMap<BindingKey, (String, Instant)>,
}
impl Reader {
    // The reader borrows the current pass's service, scope and shutdown state.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn schedule(
        &mut self,
        store: &Store,
        service: &Service,
        connection: &Options,
        bindings: &[Value],
        principals: &BTreeMap<BindingKey, Principal>,
        config: &crate::config::Config,
        stopping: watch::Receiver<bool>,
    ) {
        if self.task.as_ref().is_some_and(|task| task.is_finished())
            && let Some(task) = self.task.take()
        {
            let _ = task.await;
        }
        if self.task.is_some() || *stopping.borrow() {
            return;
        }
        self.retries.retain(|key, _| {
            bindings.iter().any(|b| {
                b["binding_id"].as_str() == Some(key.0.as_str())
                    && b["generation"].as_i64() == Some(key.1)
            })
        });
        let mut ordered: Vec<_> = bindings
            .iter()
            .filter_map(|b| {
                Some((
                    (
                        b["binding_id"].as_str()?.to_owned(),
                        b["generation"].as_i64()?,
                    ),
                    b,
                ))
            })
            .collect();
        ordered.sort_by(|a, b| a.0.cmp(&b.0));
        let start = self
            .last_binding
            .as_ref()
            .and_then(|last| ordered.iter().position(|(key, _)| key > last))
            .unwrap_or(0);
        ordered.rotate_left(start);
        for (key, b) in ordered {
            let Some(p) = principals.get(&key) else {
                continue;
            };
            let options = if b["route"]["owned_service"].is_object() {
                let binding_id = key.0.clone();
                let generation = key.1;
                let config = (*config).clone();
                match store
                    .run(move |db| {
                        super::super::launcher_owned_service::effective_options_for_binding(
                            db,
                            &config,
                            &binding_id,
                            generation,
                        )
                    })
                    .await
                {
                    Ok(Some(options)) => options,
                    _ => continue,
                }
            } else {
                match Options::parse(&b["route"]["native_options"]) {
                    Ok(options) => options,
                    Err(_) => continue,
                }
            };
            if options.connection_file != connection.connection_file
                || options.expected_version != connection.expected_version
                || !b["native_root_id"].is_string()
                || self
                    .retries
                    .get(&key)
                    .is_some_and(|(_, at)| at.elapsed() < Duration::from_secs(5))
            {
                continue;
            }
            let after = self
                .retries
                .get(&key)
                .map(|(id, _)| id.clone())
                .unwrap_or_default();
            let principal = p.clone();
            let read_config = config.clone();
            let next = store
                .run(move |db| next_read(db, &principal, &after, &read_config))
                .await;
            let Ok(Some((command, saved))) = next else {
                continue;
            };
            self.retries
                .insert(key.clone(), (command.operation_id.clone(), Instant::now()));
            self.last_binding = Some(key);
            let store = store.clone();
            let service = service.clone();
            let p = p.clone();
            let config = (*config).clone();
            let mut stopping = stopping.clone();
            self.task = Some(tokio::spawn(async move {
                if *stopping.borrow() {
                    return;
                }
                tokio::select! {
                    _ = stopping.changed() => {},
                    _ = store.oc_execution_read(p, service, options, command, saved, config) => {},
                }
            }));
            break;
        }
    }
    pub(super) async fn close(mut self) {
        if let Some(task) = self.task.take() {
            // Only the host-owned GET future; never the externally owned agent.
            task.abort();
            let _ = task.await;
        }
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

fn is_terminal(proof: &Value) -> bool {
    matches!(
        proof["disposition"].as_str(),
        Some("completed" | "failed" | "cancelled")
    )
}
fn next_read(
    db: &Connection,
    p: &Principal,
    after: &str,
    config: &crate::config::Config,
) -> Result<Option<(RuntimeCommand, Option<Value>)>> {
    let (id, generation, _) = runtime::scope(db, p, true)?;
    let query = |after: &str| -> Result<Option<String>> {
        Ok(db.query_row("SELECT operation_id FROM operations WHERE binding_id=?1 AND binding_generation=?2
          AND method IN ('task.dispatch','agent.send','agent.goal') AND (method!='agent.goal' OR json_extract(original_request_json,'$.action')='continue') AND state IN ('sending','native_accepted','outcome_unknown','settled')
          AND COALESCE(json_extract(native_refs_json,'$.input_execution.disposition'),'') NOT IN ('completed','failed','cancelled')
          AND operation_id>?3 ORDER BY operation_id LIMIT 1", params![id,generation,after], |r| r.get(0)).optional()?)
    };
    let next = match query(after)? {
        Some(id) => Some(id),
        None if !after.is_empty() => query("")?,
        None => None,
    };
    next.map(|id| {
        let command = original_with_config(db, p, &id, config)?;
        let op = operations::get_operation(db, &id)?;
        Ok((command, op["native_refs"].get("execution_scan").cloned()))
    })
    .transpose()
}

/// The single SQLite owner rechecks the exact command/link after network I/O.
fn current(
    db: &Connection,
    p: &Principal,
    command: &RuntimeCommand,
    config: &crate::config::Config,
) -> Result<Value> {
    if json!(original_with_config(db, p, &command.operation_id, config)?) != json!(command) {
        return Err(Error::new(
            "STALE_EXECUTION_READ",
            "native command scope changed during log read",
        ));
    }
    let (_, _, binding) = runtime::scope(db, p, true)?;
    let options = Options::parse(&command.route["native_options"])?;
    if binding["route"]["runtime"] != oc::RUNTIME || binding["native_scope_key"] != options.scope()
    {
        return Err(Error::new(
            "NATIVE_SCOPE_MISMATCH",
            "native execution scope changed",
        ));
    }
    let op = operations::get_operation(db, &command.operation_id)?;
    if !matches!(
        op["state"].as_str(),
        Some("sending" | "native_accepted" | "outcome_unknown" | "settled")
    ) {
        return Err(Error::new(
            "STALE_EXECUTION_READ",
            "input operation is no longer admitted",
        ));
    }
    Ok(op)
}

fn record(
    db: &mut Connection,
    p: &Principal,
    command: &RuntimeCommand,
    saved: Option<&Value>,
    read: ExecutionRead,
    config: &crate::config::Config,
) -> Result<bool> {
    let op = current(db, p, command, config)?;
    if op["native_refs"].get("execution_scan") != saved {
        return Err(Error::new(
            "STALE_EXECUTION_READ",
            "log checkpoint changed during the read",
        ));
    }
    if is_terminal(&op["native_refs"]["input_execution"]) {
        return Ok(false);
    }
    let options = Options::parse(&command.route["native_options"])?;
    let proof = if read.synced {
        read.scan.proof(command).map(|mut proof| {
            proof["native_scope_key"] = json!(options.scope());
            proof["native_service_version"] = json!(options.expected_version);
            proof
        })
    } else {
        None
    };
    // A terminal can precede the prompt's HTTP reply. First retain authentic
    // inbox admission through the normal contract, never as a made-up turn ACK.
    let admitted = proof.is_some() && op["state"] != "settled";
    if admitted {
        runtime::outcome(
            db,
            p,
            &json!(RuntimeOutcome {
                operation_id: command.operation_id.clone(),
                outcome: EffectOutcome::Applied,
                native_scope_key: Some(options.scope()),
                native_root_id: command.native_root_id.clone(),
                turn_id: None,
                native_input_id: Some(oc::input_id(&command.operation_id)),
                details: json!({"completion_condition":"native_input_admitted","delivery":"queue",
                "evidence":"durable_inbox_log",
                "assistant_result_correlation":"not_exposed",
                "assistant_result_correlation_reason":"assistant_message_has_no_input_parent_in_public_projection",
                "execution_complete":false}),
            }),
        )?;
    }
    // A crash between these transactions leaves an admitted, unresolved producer
    // which the next read selects again. No duplicate native write is needed.
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let op = current(&tx, p, command, config)?;
    let mut refs = op["native_refs"].as_object().cloned().unwrap_or_default();
    refs.insert("execution_scan".into(), json!(read.scan));
    let status = json!({"synced":read.synced,"gap":read.gap.or(if proof.is_none() && read.synced {
        Some("NATIVE_INPUT_NOT_OBSERVED") } else { None })});
    refs.insert("execution_read".into(), status);
    let now = model::now_ms()?;
    if let Some(proof) = proof {
        if op["native_refs"]["input_id"] != proof["native_input_id"] {
            return Err(Error::new(
                "NATIVE_INPUT_MISMATCH",
                "admission receipt has another input ID",
            ));
        }
        if op["native_refs"]["input_execution"] != proof {
            let stream = format!(
                "opencode-execution:{}:{}",
                command.binding_id, command.generation
            );
            let encoded = model::canonical(&proof)?;
            let event_key = format!(
                "{}:{}",
                command.operation_id,
                model::digest(encoded.as_bytes())
            );
            tx.execute("INSERT OR IGNORE INTO observations(source_stream_id,source_event_key,binding_id,binding_generation,operation_id,kind,payload_json,recorded_at_ms)
                VALUES(?1,?2,?3,?4,?5,'opencode.input_execution',?6,?7)",
                params![stream,event_key,command.binding_id,command.generation,command.operation_id,encoded,now])?;
            let observation: i64 = tx.query_row("SELECT observation_id FROM observations WHERE source_stream_id=?1 AND source_event_key=?2",
                params![stream,event_key], |r| r.get(0))?;
            if command.method == "task.dispatch" {
                let attempt_id = model::text(&op, "attempt_id")?;
                let attempt = tasks::get_attempt(&tx, attempt_id)?;
                if attempt["released_at_ms"].is_null() {
                    let mut producers: Vec<Value> =
                        serde_json::from_value(attempt["producers"].clone())?;
                    let matches: Vec<usize> = producers
                        .iter()
                        .enumerate()
                        .filter_map(|(i, p)| {
                            (p["assignment_id"] == command.operation_id).then_some(i)
                        })
                        .collect();
                    if matches.len() != 1 {
                        return Err(Error::conflict(
                            "exact input producer is missing or duplicated",
                        ));
                    }
                    let producer = &mut producers[matches[0]];
                    if producer["native_session_id"] != proof["native_session_id"]
                        || producer["native_input_id"] != proof["native_input_id"]
                        || (producer["native_run_id"].is_string()
                            && producer["native_run_id"] != proof["native_run_id"])
                    {
                        return Err(Error::new(
                            "NATIVE_INPUT_MISMATCH",
                            "producer identity differs from the native log",
                        ));
                    }
                    if !is_terminal(producer) {
                        if proof["native_run_id"].is_string() {
                            producer["native_run_id"] = proof["native_run_id"].clone();
                            producer["native_run_id_kind"] = proof["native_run_id_kind"].clone();
                        }
                        producer["execution_observation_id"] = json!(observation);
                        producer["execution_disposition"] = proof["disposition"].clone();
                        if is_terminal(&proof) {
                            producer["disposition"] = proof["disposition"].clone();
                            producer["terminal_evidence"] = json!({"observation_id":observation,
                                "event":proof["terminal"]["event"],
                                "stage":proof["terminal"]["stage"],
                                "error_code":proof["terminal"]["error_code"],
                                "correlation":proof["correlation"]});
                        }
                        tx.execute("UPDATE attempts SET producers_json=?2,updated_at_ms=?3 WHERE attempt_id=?1 AND released_at_ms IS NULL",
                            params![attempt_id,model::canonical(&json!(producers))?,now])?;
                    }
                }
            }
        }
        refs.insert("input_execution".into(), proof);
    }
    let changed = op["native_refs"] != json!(refs);
    if changed {
        tx.execute(
            "UPDATE operations SET native_refs_json=?2,updated_at_ms=?3 WHERE operation_id=?1",
            params![command.operation_id, model::canonical(&json!(refs))?, now],
        )?;
    }
    super::super::capacity::sync_operation(&tx, &command.operation_id, now)?;
    if let Some(attempt_id) = op["attempt_id"].as_str() {
        super::super::capacity::sync_attempt(&tx, attempt_id, now)?;
    }
    tx.commit()?;
    Ok(changed || admitted)
}

impl Store {
    async fn oc_execution_read(
        &self,
        p: Principal,
        service: Service,
        options: Options,
        command: RuntimeCommand,
        saved: Option<Value>,
        config: crate::config::Config,
    ) {
        let read = tokio::time::timeout(
            Duration::from_secs(20),
            service.read_execution(&command, &options, saved.as_ref()),
        )
        .await
        .unwrap_or_else(|_| {
            Err(Error::new(
                "NATIVE_LOG_READ_TIMEOUT",
                "bounded execution read timed out",
            ))
        });
        let result = match read {
            Ok(read) => {
                let p = p.clone();
                let command = command.clone();
                let config = config.clone();
                self.run(move |db| record(db, &p, &command, saved.as_ref(), read, &config))
                    .await
            }
            Err(e) => Err(e),
        };
        match result {
            Ok(true) => self.changed.send_modify(|v| *v = v.wrapping_add(1)),
            Ok(false) => {}
            Err(e) => {
                // The diagnostic belongs to this input, not the whole service.
                // Leave prior evidence and checked progress intact on failure.
                let config = config.clone();
                let _ = self.run(move |db| {
                    let op = current(db, &p, &command, &config)?;
                    let status = json!({"synced":false,"gap":e.code});
                    if op["native_refs"]["execution_read"] != status {
                        db.execute("UPDATE operations SET native_refs_json=json_set(COALESCE(native_refs_json,'{}'),'$.execution_read',json(?2)),updated_at_ms=?3 WHERE operation_id=?1",
                            params![command.operation_id,model::canonical(&status)?,model::now_ms()?])?;
                    }
                    Ok(())
                }).await;
            }
        }
    }
}
