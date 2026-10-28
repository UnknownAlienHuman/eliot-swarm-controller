//! Exact-subject reader actors. Presence here never grants native control.
use super::{
    AgentKey, PluginHost, RPC_DEADLINE, RPC_GRACE, STOP_GRACE, StoreHandle,
    build_process_environment, redact_native_error, signal_progress,
};
use crate::common::now_ms;
use eliot_core::{Agent, Command, CommandState, Error, Result, StderrSource};
use eliot_plugin::{ObservationPage, ObservationQuery};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::{
    sync::{Mutex, mpsc, oneshot, watch},
    time::{Instant, MissedTickBehavior, interval, timeout_at},
};

pub(super) struct ObserverEntry {
    agent: Agent,
    sender: mpsc::Sender<ObserverMessage>,
    stop: watch::Sender<bool>,
    done: watch::Receiver<Option<bool>>,
    gate: Mutex<()>,
    detaching: AtomicBool,
}

enum ObserverMessage {
    Snapshot {
        query: ObservationQuery,
        until: Instant,
        reply: oneshot::Sender<Result<Value>>,
    },
    Detach {
        payload: Box<(Agent, Command)>,
        reply: oneshot::Sender<Result<Value>>,
    },
}

impl PluginHost {
    pub(crate) fn observes_generation(&self, id: &str, generation: i64) -> bool {
        self.state
            .observers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&AgentKey {
                id: id.to_owned(),
                generation,
            })
            .is_some_and(|entry| !entry.detaching.load(Ordering::SeqCst) && !*entry.stop.borrow())
    }

    pub(crate) fn observer_count(&self) -> usize {
        self.state
            .observers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    pub(crate) async fn attach_observer(
        &self,
        agent: Agent,
        command: Command,
        source_state: Option<Value>,
    ) -> Result<Value> {
        let until = Instant::now() + RPC_DEADLINE;
        timeout_at(
            until,
            self.check_observer_command(&agent, &command, "attach"),
        )
        .await
        .map_err(|_| Error::new("busy", "observer admission deadline elapsed"))??;
        let key = AgentKey {
            id: agent.id.clone(),
            generation: agent.generation,
        };
        let (sender, receiver) = mpsc::channel(16);
        let (stop, cancellation) = watch::channel(false);
        let (done, done_rx) = watch::channel(None);
        let entry = Arc::new(ObserverEntry {
            agent,
            sender,
            stop,
            done: done_rx,
            gate: Mutex::new(()),
            detaching: AtomicBool::new(false),
        });
        {
            // Same admission lock as discovery shutdown; registration precedes
            // all process/native I/O and survives a disconnected RPC caller.
            let _admission = self
                .state
                .readers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if self.state.discovery_closed.load(Ordering::SeqCst) {
                return Err(Error::new("unavailable", "reader admission is closed"));
            }
            let mut observers = self
                .state
                .observers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if observers.contains_key(&key) {
                return Err(Error::new(
                    "busy",
                    "an observer already owns this generation",
                ));
            }
            observers.insert(key.clone(), Arc::clone(&entry));
        }
        let (ready, result) = oneshot::channel();
        let host = self.clone();
        tokio::spawn(async move {
            host.run_observer(
                entry,
                command,
                source_state,
                receiver,
                cancellation,
                ready,
                done,
                until,
            )
            .await;
            host.state
                .observers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&key);
            signal_progress(&host.state);
        });
        timeout_at(until, result)
            .await
            .map_err(|_| Error::new("unknown", "observer startup deadline elapsed"))?
            .map_err(|_| Error::new("unknown", "observer ended before attach readback"))?
    }

    pub(crate) async fn snapshot_observer(&self, agent: &Agent, query: Value) -> Result<Value> {
        let until = Instant::now() + RPC_DEADLINE;
        let query: ObservationQuery = serde_json::from_value(query)
            .map_err(|_| Error::new("invalid", "invalid observation query"))?;
        query.validate()?;
        if !matches!(query.source.as_str(), "native" | "rollout") {
            return Err(Error::new(
                "invalid",
                "observation source must be native or rollout",
            ));
        }
        let entry = self.observer_entry(&agent.id, agent.generation)?;
        let gate = timeout_at(until, entry.gate.lock())
            .await
            .map_err(|_| Error::new("busy", "observer admission deadline elapsed"))?;
        if entry.detaching.load(Ordering::SeqCst) || *entry.stop.borrow() {
            return Err(Error::new("bad_state", "observer reader is closing"));
        }
        timeout_at(
            until,
            check_binding(&self.state.store, agent, Some(&query.source)),
        )
        .await
        .map_err(|_| Error::new("busy", "observer source-check deadline elapsed"))??;
        let (reply, response) = oneshot::channel();
        timeout_at(
            until,
            entry.sender.send(ObserverMessage::Snapshot {
                query,
                until,
                reply,
            }),
        )
        .await
        .map_err(|_| Error::new("busy", "observer request queue deadline elapsed"))?
        .map_err(|_| Error::new("unavailable", "observer reader ended"))?;
        drop(gate);
        timeout_at(until, response)
            .await
            .map_err(|_| Error::new("unknown", "observer snapshot deadline elapsed"))?
            .map_err(|_| Error::new("unavailable", "observer reader ended"))?
    }

    pub(crate) async fn detach_observer(&self, agent: &Agent, command: Command) -> Result<Value> {
        let until = Instant::now() + RPC_DEADLINE + STOP_GRACE * 3 + RPC_GRACE;
        timeout_at(
            until,
            self.check_observer_command(agent, &command, "detach"),
        )
        .await
        .map_err(|_| Error::new("unknown", "observer detach fence-check deadline elapsed"))??;
        let previous = command
            .expected_generation
            .ok_or_else(|| Error::new("stale_generation", "detach has no retained generation"))?;
        let entry = match self.observer_entry(&agent.id, previous) {
            Ok(entry) => entry,
            Err(_) => {
                // The new command's committed fence revokes observation even
                // after restart. Absence of a current-host handle is not process
                // exit evidence; retain old OS unknown rows and never adopt PIDs.
                return Ok(detach_receipt(agent, &command.id, true));
            }
        };
        if entry.agent.native_scope != agent.native_scope
            || entry.agent.native_ref != agent.native_ref
        {
            return Err(Error::new(
                "source_integrity",
                "detach targets another native subject",
            ));
        }
        let gate = timeout_at(until, entry.gate.lock())
            .await
            .map_err(|_| Error::new("unknown", "observer detach gate deadline elapsed"))?;
        entry.detaching.store(true, Ordering::SeqCst);
        let command_id = command.id.clone();
        let (reply, response) = oneshot::channel();
        let sent = timeout_at(
            until,
            entry.sender.send(ObserverMessage::Detach {
                payload: Box::new((agent.clone(), command)),
                reply,
            }),
        )
        .await;
        drop(gate);
        if !matches!(sent, Ok(Ok(()))) {
            let mut done = entry.done.clone();
            let _ = timeout_at(until, done.wait_for(|value| value.is_some())).await;
            return if *done.borrow() == Some(true) {
                Ok(detached(agent, &command_id))
            } else {
                Err(Error::new(
                    "unknown",
                    "observer reader closure is unconfirmed",
                ))
            };
        }
        match timeout_at(until, response).await {
            Ok(Ok(result)) => result,
            _ if *entry.done.borrow() == Some(true) => Ok(detached(agent, &command_id)),
            _ => Err(Error::new(
                "unknown",
                "observer reader closure is unconfirmed",
            )),
        }
    }

    pub(super) async fn cancel_observers(&self) {
        let entries: Vec<_> = self
            .state
            .observers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect();
        for entry in &entries {
            entry.stop.send_replace(true);
        }
        let until = Instant::now() + RPC_DEADLINE + STOP_GRACE * 3 + RPC_GRACE;
        for entry in entries {
            let mut done = entry.done.clone();
            let _ = timeout_at(until, done.wait_for(|value| value.is_some())).await;
        }
    }

    fn observer_entry(&self, id: &str, generation: i64) -> Result<Arc<ObserverEntry>> {
        self.state
            .observers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&AgentKey {
                id: id.to_owned(),
                generation,
            })
            .cloned()
            .ok_or_else(|| {
                Error::new(
                    "unavailable",
                    "no owned observer reader for this generation",
                )
            })
    }

    async fn check_observer_command(
        &self,
        agent: &Agent,
        command: &Command,
        verb: &str,
    ) -> Result<()> {
        let store = self.state.store.lock().await;
        let retained = store
            .get_command(&command.id)?
            .ok_or_else(|| Error::new("not_found", "observer command was not admitted"))?;
        if retained.verb != verb
            || retained.target.as_deref() != Some(agent.id.as_str())
            || retained.generation != Some(agent.generation)
            || retained.state != CommandState::Dispatched
            || command.verb != retained.verb
            || command.generation != retained.generation
            || command.expected_generation != retained.expected_generation
            || command.target != retained.target
            || command.args != retained.args
        {
            return Err(Error::new(
                "stale_generation",
                "observer command is not the committed transition",
            ));
        }
        check_agent(&store, agent)
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_observer(
        &self,
        entry: Arc<ObserverEntry>,
        command: Command,
        source_state: Option<Value>,
        mut messages: mpsc::Receiver<ObserverMessage>,
        mut cancellation: watch::Receiver<bool>,
        ready: oneshot::Sender<Result<Value>>,
        done: watch::Sender<Option<bool>>,
        until: Instant,
    ) {
        let mut transport = None;
        let mut spawn_attempted = false;
        let startup = async {
            let _gate = entry.gate.lock().await;
            if *entry.stop.borrow() || entry.detaching.load(Ordering::SeqCst) {
                return Err(Error::new(
                    "cancelled",
                    "observer attach was cancelled before spawn",
                ));
            }
            self.check_observer_command(&entry.agent, &command, "attach")
                .await?;
            let states = check_binding(&self.state.store, &entry.agent, None).await?;
            let environment =
                Value::Object(build_process_environment(&entry.agent.profile_snapshot)?);
            self.state
                .monitoring
                .prepare_observer_generation(&entry.agent)
                .await?;
            if *entry.stop.borrow()
                || entry.detaching.load(Ordering::SeqCst)
                || Instant::now() >= until
            {
                return Err(Error::new(
                    "cancelled",
                    "observer attach was cancelled before spawn",
                ));
            }
            spawn_attempted = true;
            transport = Some(
                self.open_reader(
                    &AgentKey {
                        id: entry.agent.id.clone(),
                        generation: entry.agent.generation,
                    },
                    &entry.agent.harness,
                    &entry.agent.profile_snapshot,
                    &environment,
                    Some(&entry.agent),
                )
                .await?,
            );
            drop(_gate);
            let reader = transport.as_mut().expect("reader was just registered");
            reader
                .observe_os(&self.state.monitoring, &entry.agent)
                .await?;
            let manifest = reader.initialize(&entry.agent.harness, until).await?;
            if !manifest.features.observe {
                return Err(Error::new(
                    "unsupported",
                    "plugin does not advertise observation attach",
                ));
            }
            let plan = source_plan(&states)?;
            let observed = manifest
                .sources
                .get("observed")
                .ok_or_else(|| Error::new("unsupported", "plugin has no observed source plan"))?;
            if serde_json::to_value(observed).ok().as_ref() != Some(&plan) {
                return Err(Error::new(
                    "source_integrity",
                    "plugin observed plan differs from committed native_read ownership",
                ));
            }
            check_binding(&self.state.store, &entry.agent, None).await?;
            if entry.detaching.load(Ordering::SeqCst) {
                return Err(Error::new(
                    "cancelled",
                    "observer attach was fenced before native I/O",
                ));
            }
            let value = reader.client.call("agent.attach", json!({
                "agent_id": entry.agent.id, "generation": entry.agent.generation,
                "command_id": command.id, "mode": "observe",
                "native_scope": entry.agent.native_scope, "native_ref": entry.agent.native_ref,
                "profile": entry.agent.profile_snapshot, "target_environment": environment,
                "source_plan": plan, "source_state": source_state,
            }), until).await?;
            validate_subject(&value, &entry.agent)?;
            if value["state"] != "attached"
                || value["observation_state"] != "unknown"
                || value["command_id"].as_str() != Some(command.id.as_str())
                || !value["metadata"].is_object()
                || !value["coverage"].is_object()
            {
                return Err(Error::new(
                    "source_integrity",
                    "plugin did not return bound observer readiness",
                ));
            }
            safe_value(value, &reader.secrets)
        };
        let result = tokio::select! {
            biased;
            _ = cancellation.changed() => Err(Error::new("cancelled", "observer attach was cancelled")),
            result = timeout_at(until, startup) => result.unwrap_or_else(|_| Err(Error::new("unknown", "observer startup deadline elapsed"))),
        };
        let started = result.is_ok();
        let _ = ready.send(result.map_err(|error| {
            transport
                .as_ref()
                .map_or(error.clone(), |reader| reader.diagnose(error))
        }));
        let mut detach = None;
        let mut reason = "observer_start_failed";
        if started && let Some(reader) = transport.as_mut() {
            let mut ticks = interval(std::time::Duration::from_secs(2));
            ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
            ticks.tick().await;
            loop {
                tokio::select! {
                    biased;
                    _ = cancellation.changed() => { reason = "observer_cancelled"; break; }
                    status = reader.child.wait() => {
                        let _ = status;
                        reason = "observer_reader_exited";
                        break;
                    }
                    message = messages.recv() => match message {
                        Some(ObserverMessage::Snapshot { query, until, reply }) => {
                            if reply.is_closed() { continue; }
                            let result = if entry.detaching.load(Ordering::SeqCst) {
                                Err(Error::new("bad_state", "observer detach fenced new reads"))
                            } else { read_snapshot(self, reader, &entry.agent, query, until).await };
                            let _ = reply.send(result);
                        }
                        Some(ObserverMessage::Detach { payload, reply }) => {
                            let (agent, command) = *payload;
                            let result = self.check_observer_command(&agent, &command, "detach").await;
                            let result = match result {
                                Ok(()) => reader.client.call("agent.detach", json!({
                                    "agent_id": entry.agent.id, "generation": entry.agent.generation,
                                    "command_id": command.id, "native_scope": entry.agent.native_scope,
                                    "native_ref": entry.agent.native_ref,
                                }), Instant::now() + RPC_DEADLINE).await.and_then(|value| {
                                    validate_subject(&value, &entry.agent)?;
                                    if value["state"] != "detached" || value["observation_state"] != "unknown"
                                        || value["command_id"].as_str() != Some(command.id.as_str())
                                    {
                                        return Err(Error::new("unknown", "plugin did not confirm observer detach"));
                                    }
                                    safe_value(value, &reader.secrets)
                                }),
                                Err(error) => Err(error),
                            };
                            detach = Some((agent, command.id, reply, result));
                            reason = "observer_detached";
                            break;
                        }
                        None => { reason = "observer_owner_closed"; break; }
                    },
                    _ = ticks.tick(), if !entry.detaching.load(Ordering::SeqCst) => {
                        let query = ObservationQuery {
                            source: "native".to_owned(), view: "subject".to_owned(),
                            turn_id: None, cursor: None, limit: 1,
                        };
                        if read_snapshot(self, reader, &entry.agent, query, Instant::now() + RPC_DEADLINE).await.is_err() {
                            reason = "observer_readback_unavailable";
                            break;
                        }
                    }
                }
            }
        }
        entry.detaching.store(true, Ordering::SeqCst);
        let (closed, stderr) = match transport {
            Some(reader) => {
                let closure = reader.close(self, Some(&entry.agent)).await;
                (closure.closed, Some(closure.stderr))
            }
            None => (!spawn_attempted, None),
        };
        // A committed detach fence can race the next poll. Exact reader cleanup
        // then belongs to Router's detach settlement, not the loss callback that
        // would otherwise mark that same dispatched detach command unknown.
        let fenced = if closed {
            self.state
                .store
                .lock()
                .await
                .observer_active(&entry.agent.id, entry.agent.generation)
                .is_ok_and(|active| !active)
        } else {
            false
        };
        let planned = closed
            && (fenced
                || detach
                    .as_ref()
                    .is_some_and(|(_, _, _, result)| result.is_ok()));
        if !planned {
            self.observer_unavailable(&entry.agent, reason, closed)
                .await;
        }
        done.send_replace(Some(closed));
        if let Some((agent, command_id, reply, result)) = detach {
            let result = match result {
                Ok(_) if closed => Ok(detached(&agent, &command_id)),
                Err(error) => Err(match stderr {
                    Some(stderr) => error.with_stderr(StderrSource::Plugin, stderr),
                    None => error,
                }),
                _ => Err(Error::new(
                    "unknown",
                    "observer reader cleanup is unconfirmed",
                )),
            };
            let _ = reply.send(result);
        }
    }

    async fn observer_unavailable(&self, agent: &Agent, reason: &str, closed: bool) {
        let mut store = self.state.store.lock().await;
        if let Ok(events) =
            store.mark_observer_unavailable(&agent.id, agent.generation, reason, closed, now_ms())
        {
            self.state.bus.publish(&events);
        }
        signal_progress(&self.state);
    }
}

fn check_agent(store: &eliot_store::Store, expected: &Agent) -> Result<()> {
    let current = store
        .get_agent(&expected.id)?
        .ok_or_else(|| Error::new("not_found", "observed Agent was not found"))?;
    if current.generation != expected.generation
        || current.native_scope != expected.native_scope
        || current.native_ref != expected.native_ref
        || !current.observe_only
        || current.own_identity
        || current.harness != expected.harness
        || current.origin != eliot_core::AgentOrigin::External
        || expected.generation <= 0
        || current.profile_snapshot != expected.profile_snapshot
        || expected.native_scope.as_deref().is_none_or(str::is_empty)
        || expected.native_ref.as_deref().is_none_or(str::is_empty)
    {
        return Err(Error::new(
            "stale_generation",
            "observer native subject/generation is no longer bound",
        ));
    }
    Ok(())
}

async fn check_binding(
    store: &StoreHandle,
    agent: &Agent,
    source: Option<&str>,
) -> Result<Vec<Value>> {
    let store = store.lock().await;
    check_agent(&store, agent)?;
    if !store.observer_active(&agent.id, agent.generation)? {
        return Err(Error::new(
            "stale_generation",
            "observer reads are fenced by detach or source closure",
        ));
    }
    let states: Vec<_> = store
        .list_source_states(&agent.id, agent.generation)?
        .into_iter()
        .filter(|state| state["source"] == "observe")
        .collect();
    if source.is_some_and(|source| !matches!(source, "native" | "rollout" | "observe"))
        || !states.iter().any(|state| state["state"] == "open")
    {
        return Err(Error::new(
            "source_integrity",
            "observer source is not committed and open",
        ));
    }
    source_plan(&states)?;
    Ok(states)
}

fn source_plan(states: &[Value]) -> Result<Value> {
    let mut plan = serde_json::Map::new();
    for state in states {
        if state["state"] == "open"
            && let Some(owners) = state["owners"].as_array()
        {
            for owner in owners {
                let family = owner["family"].as_str().ok_or_else(|| {
                    Error::new("source_integrity", "observer owner has no family")
                })?;
                if owner["source"] != "observe" || owner["mechanism"] != "native_read" {
                    return Err(Error::new(
                        "source_integrity",
                        "observer family is not owned by native_read",
                    ));
                }
                plan.insert(family.to_owned(), json!("native_read"));
            }
        }
    }
    if plan.is_empty() {
        return Err(Error::new(
            "source_integrity",
            "observer source owns no read families",
        ));
    }
    Ok(Value::Object(plan))
}

pub(super) async fn callback_error(
    store: &StoreHandle,
    observation: Option<&Agent>,
    method: &str,
    params: &Value,
    until: Instant,
) -> Option<Error> {
    let agent = observation?;
    if matches!(method, "process.put" | "events.put" | "source.handoff") {
        return Some(Error::new(
            "not_allowed",
            "observer history is private and cannot publish lifecycle, hand off sources, or adopt processes",
        ));
    }
    if params["agent_id"].as_str() != Some(agent.id.as_str())
        || params["generation"].as_i64() != Some(agent.generation)
    {
        return Some(Error::new(
            "stale_generation",
            "observer callback identity does not match its reader",
        ));
    }
    let Some(source @ "observe") = params["source"].as_str() else {
        return Some(Error::new(
            "not_allowed",
            "observer callback source is outside its binding",
        ));
    };
    if method == "source.state" {
        // A closed source can still expose its last committed acknowledgement;
        // this is Store readback, never permission for another native read/write.
        let store = match timeout_at(until, store.lock()).await {
            Ok(store) => store,
            Err(_) => return Some(Error::new("busy", "observer source-state deadline elapsed")),
        };
        return check_agent(&store, agent).err();
    }
    match timeout_at(until, check_binding(store, agent, Some(source))).await {
        Ok(result) => result.err(),
        Err(_) => Some(Error::new("busy", "observer source-check deadline elapsed")),
    }
}

async fn read_snapshot(
    host: &PluginHost,
    reader: &mut super::reader::ReaderTransport,
    agent: &Agent,
    query: ObservationQuery,
    until: Instant,
) -> Result<Value> {
    let result = timeout_at(
        until,
        read_snapshot_until(host, reader, agent, query, until),
    )
    .await
    .map_err(|_| Error::new("unavailable", "observer snapshot deadline elapsed"))?;
    result.map_err(|error| reader.diagnose(error))
}

async fn read_snapshot_until(
    host: &PluginHost,
    reader: &mut super::reader::ReaderTransport,
    agent: &Agent,
    query: ObservationQuery,
    until: Instant,
) -> Result<Value> {
    query.validate()?;
    check_binding(&host.state.store, agent, Some(&query.source)).await?;
    let value = reader
        .client
        .call(
            "agent.snapshot",
            json!({
                "agent_id": agent.id, "generation": agent.generation,
                "native_scope": agent.native_scope, "native_ref": agent.native_ref,
                "observation": query,
            }),
            until,
        )
        .await
        .map_err(|error| redact_native_error(error, &reader.secrets))?;
    validate_subject(&value, agent)?;
    let page: ObservationPage = serde_json::from_value(value.clone()).map_err(|_| {
        Error::new(
            "source_integrity",
            "plugin returned an invalid observation page",
        )
    })?;
    page.validate()?;
    if page.source != query.source
        || page.view != query.view
        || page.entries.len() > query.limit
        || page
            .next_cursor
            .as_deref()
            .is_some_and(|cursor| Some(cursor) == query.cursor.as_deref())
    {
        return Err(Error::new(
            "source_integrity",
            "observation page does not match its query",
        ));
    }
    // A takeover/detach committed while the read was pending fences its result.
    check_binding(&host.state.store, agent, Some(&query.source)).await?;
    safe_value(value, &reader.secrets)
}

fn validate_subject(value: &Value, agent: &Agent) -> Result<()> {
    if value["agent_id"].as_str() != Some(agent.id.as_str())
        || value["generation"].as_i64() != Some(agent.generation)
        || value["native_scope"].as_str() != agent.native_scope.as_deref()
        || value["native_ref"].as_str() != agent.native_ref.as_deref()
    {
        return Err(Error::new(
            "source_integrity",
            "reader response names another native subject/generation",
        ));
    }
    Ok(())
}

fn safe_value(mut value: Value, secrets: &[String]) -> Result<Value> {
    if serde_json::to_vec(&value)
        .map_err(|_| Error::new("internal", "cannot encode observer result"))?
        .len()
        > eliot_plugin::MAX_OBSERVATION_PAGE_BYTES
    {
        return Err(Error::new(
            "frame_too_large",
            "observer result exceeds 900 KiB",
        ));
    }
    if crate::redaction::redact_value(&mut value, secrets) {
        return Err(Error::new(
            "source_integrity",
            "observer response contains reserved credentials",
        ));
    }
    Ok(value)
}

fn detached(agent: &Agent, command_id: &str) -> Value {
    detach_receipt(agent, command_id, false)
}

fn detach_receipt(agent: &Agent, command_id: &str, logical_only: bool) -> Value {
    json!({"state": "detached", "agent_id": agent.id, "generation": agent.generation,
        "native_scope": agent.native_scope, "native_ref": agent.native_ref,
        "reader_closed": (!logical_only).then_some(true),
        "command_id": command_id, "observation_state": "unknown",
        "metadata": {"logical_only": logical_only,
            "cleanup_capability": crate::workspaces::custody_capability(),
            "orphan_possible": !cfg!(windows)}})
}
