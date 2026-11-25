//! Authenticated source-request intake.
//!
//! Owns serve_plugin_requests: credential-checked events.put,
//! source.* and process.put serving with Store transactions,
//! redaction and event publication. Shared ordering and generation
//! ownership stay with super; actor lifecycle lives in actor.

use super::{
    AgentKey, Bus, EventSource, HostState, PluginConnection, RPC_GRACE, StoreHandle, deadline,
    fence_runtime_generation, late_batch_error, record_source_gap, record_store_batch_rejection,
    redact_native_error, redact_source_gap, safe_cause_code, signal_progress, source_state_binding,
};
use super::{native_children, observation, source_handoff};

use crate::command_guard::{CommandGuardPolicy, CommandGuardRequest, REQUEST_FIELD};
use crate::common::{now_ms, secure_eq};
use crate::git_monitor::GitProbe;
use crate::monitor::Monitoring;

use eliot_core::limits::{
    MAX_TOOL_BATCHES_PER_GROUP, MAX_TOOL_EVENTS_PER_BATCH, MAX_TOOL_GROUP_BYTES,
};
use eliot_core::{Error, PermissionRefusal, Result, SecretPolicy, SourceBatch, SourceGap};
use eliot_plugin::protocol::{
    HookEvent, HookResponse, SessionUsageSampleAck, SessionUsageSampleRequest,
};
use eliot_plugin::sync::LockExt;
use eliot_store::{BatchCommit, PreparedSourceBatch, ReadyToolBatch, family_for_kind};
use serde_json::{Value, json};

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio::sync::{Mutex as AsyncMutex, oneshot};
use tokio::time::{Instant, timeout_at};
use ulid::Ulid;

struct BatchIntake<'a> {
    host: &'a std::sync::Weak<HostState>,
    monitoring: &'a Monitoring,
    policy: &'a CommandGuardPolicy,
    credentials: &'a [String],
}

#[derive(Default)]
pub(super) struct ReadyToolBatches {
    queue: Mutex<Vec<QueuedToolBatch>>,
    streams: Mutex<HashMap<AgentKey, Weak<AsyncMutex<()>>>>,
}

struct QueuedToolBatch {
    ticket: Arc<()>,
    batch: ReadyToolBatch,
    queued: Instant,
    deadline: Instant,
    reply: oneshot::Sender<ToolBatchOutcome>,
}

enum ToolBatchOutcome {
    Direct(PreparedSourceBatch),
    Committed {
        result: Result<BatchCommit>,
        wait: Duration,
        write_timing: Option<SourceWriteTiming>,
        batches: usize,
        group_bytes: usize,
    },
    RestartCutoff(String),
    Unsettled(Error),
}

#[derive(Clone, Copy)]
struct SourceWriteTiming {
    elapsed: Duration,
    held: Duration,
}

impl ReadyToolBatches {
    /// Keep one FIFO admission lock shared by any overlapping connections for
    /// this controlled stream. A connection holds it through commit or gap.
    fn stream_order(&self, key: &AgentKey) -> Arc<AsyncMutex<()>> {
        let mut streams = self.streams.lock_recover();
        if let Some(order) = streams.get(key).and_then(Weak::upgrade) {
            return order;
        }
        streams.retain(|_, order| order.strong_count() > 0);
        let order = Arc::new(AsyncMutex::new(()));
        streams.insert(key.clone(), Arc::downgrade(&order));
        order
    }

    /// A caller that acquires the existing Store lock serves the bounded
    /// ready queue. Other callers join through their own result channels.
    /// A full queue uses the original single-batch path.
    async fn try_submit(
        &self,
        host: &HostState,
        store: &StoreHandle,
        bus: &Bus,
        batch: ReadyToolBatch,
        until: Instant,
    ) -> ToolBatchOutcome {
        let (reply, mut received) = oneshot::channel();
        let ticket = Arc::new(());
        {
            let mut queue = self.queue.lock_recover();
            queue.retain(|job| !job.reply.is_closed());
            let queued_bytes: usize = queue.iter().map(|job| job.batch.encoded_bytes()).sum();
            if queue.len() >= MAX_TOOL_BATCHES_PER_GROUP
                || batch.encoded_bytes() > MAX_TOOL_GROUP_BYTES - queued_bytes
            {
                return ToolBatchOutcome::Direct(batch.into_batch());
            }
            queue.push(QueuedToolBatch {
                ticket: Arc::clone(&ticket),
                batch,
                queued: Instant::now(),
                deadline: until,
                reply,
            });
        }
        match timeout_at(until, async {
            tokio::select! {
                biased;
                result = &mut received => result,
                mut writer = store.lock() => {
                    let queued = std::mem::take(&mut *self.queue.lock_recover());
                    let acquired = Instant::now();
                    let mut batches = Vec::with_capacity(queued.len());
                    let mut replies = Vec::with_capacity(queued.len());
                    let cutoff = host.planned_restart_cutoff.lock_recover().clone();
                    for job in queued {
                        if let Some(cutoff) = &cutoff {
                            let _ = job.reply.send(ToolBatchOutcome::RestartCutoff(cutoff.clone()));
                        } else if job.deadline <= acquired {
                            let _ = job.reply.send(ToolBatchOutcome::Unsettled(Error::new(
                                "busy",
                                "Store lock deadline elapsed for events.put",
                            )));
                        } else if !job.reply.is_closed() {
                            batches.push(job.batch);
                            replies.push((job.queued, job.reply));
                        }
                    }
                    if batches.is_empty() {
                        drop(writer);
                        return received.await;
                    }
                    let count = batches.len();
                    let group_bytes = batches.iter().map(ReadyToolBatch::encoded_bytes).sum();
                    let now = now_ms();
                    let started = Instant::now();
                    let results = if count == 1 {
                        vec![writer.put_batch(batches.pop().expect("one ready batch").into_batch(), now)]
                    } else {
                        writer.put_tool_batch_group(batches, now)
                    };
                    let elapsed = started.elapsed();
                    for commit in results.iter().filter_map(|result| result.as_ref().ok()) {
                        bus.publish(&commit.events);
                    }
                    let timing = SourceWriteTiming { elapsed, held: acquired.elapsed() };
                    drop(writer);
                    for ((queued_at, reply), result) in replies.into_iter().zip(results) {
                        let _ = reply.send(ToolBatchOutcome::Committed {
                            result,
                            wait: acquired - queued_at,
                            write_timing: (count == 1).then_some(timing),
                            batches: count,
                            group_bytes,
                        });
                    }
                    if count > 1 && crate::logging::is_debug() {
                        crate::logging::record_debug(
                            &SecretPolicy::default(),
                            "store.source_batch_group",
                            &[],
                            &[
                                ("store_elapsed_us", timing.elapsed.as_micros() as u64),
                                ("lock_hold_us", timing.held.as_micros() as u64),
                                ("commit_batches", count as u64),
                                ("commit_batch_bytes", group_bytes as u64),
                            ],
                        );
                    }
                    received.await
                }
            }
        })
        .await
        {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => ToolBatchOutcome::Unsettled(Error::new(
                "unknown",
                "tool group acknowledgement was not delivered",
            )),
            Err(_) => {
                let mut queue = self.queue.lock_recover();
                if let Some(index) = queue.iter().position(|job| Arc::ptr_eq(&job.ticket, &ticket)) {
                    queue.remove(index);
                    ToolBatchOutcome::Unsettled(Error::new(
                        "busy",
                        "Store lock deadline elapsed for events.put",
                    ))
                } else {
                    ToolBatchOutcome::Unsettled(Error::new(
                        "unknown",
                        "tool group acknowledgement deadline elapsed after dequeue",
                    ))
                }
            }
        }
    }
}

pub(super) async fn serve_plugin_requests(mut connection: PluginConnection, source: EventSource) {
    let EventSource {
        store,
        bus,
        monitoring,
        source_credentials,
        state,
        key,
        agent_id,
        generation,
        expected_credential,
        observation,
        command_guard_policy,
    } = source;
    let binding = source_handoff::SourceBinding {
        agent_id: &agent_id,
        generation,
        observation: observation.as_ref(),
    };
    let client = connection.client();
    let stream_order = state
        .upgrade()
        .map(|host| host.tool_batches.stream_order(&key));
    while let Some(request) = connection.next_request().await {
        let request_id = request.id.clone();
        let method = request.method.clone();
        let reply_deadline = deadline();
        // A Store lock timeout must leave time to transmit the proven rejection.
        let request_deadline = match method.as_str() {
            "events.put" => reply_deadline - RPC_GRACE,
            "usage.sample" => Instant::now() + std::time::Duration::from_millis(150),
            _ => reply_deadline,
        };
        let mut rejected_batch = None;
        let mut stream_guard = None;
        let result = if !matches!(
            method.as_str(),
            "events.put"
                | "usage.sample"
                | "source.gap"
                | "source.handoff"
                | "source.health"
                | "source.state"
                | "process.put"
                | "child"
                | "hook.event"
        ) {
            Err(Error::new(
                "unsupported",
                "plugin host only accepts events.put, usage.sample, source.gap, source.handoff, source.health, source.state, process.put, child, and hook.event",
            ))
        } else {
            let mut params = request.params;
            let child_wire = if method == "child" {
                Some(params.clone())
            } else {
                None
            };
            let credential = params
                .as_object_mut()
                .and_then(|object| object.remove("credential"))
                .and_then(|value| value.as_str().map(ToOwned::to_owned));
            if !credential
                .as_deref()
                .is_some_and(|credential| secure_eq(credential, &expected_credential))
            {
                Err(Error::new("not_allowed", "plugin credential is invalid"))
            } else if let Some(error) = observation::callback_error(
                &store,
                observation.as_ref(),
                &method,
                &params,
                request_deadline,
            )
            .await
            {
                Err(error)
            } else {
                match method.as_str() {
                    "process.put" => {
                        let current = state
                            .upgrade()
                            .is_some_and(|state| state.running.lock_recover().contains_key(&key));
                        if !current
                            || params.get("agent_id").and_then(Value::as_str)
                                != Some(agent_id.as_str())
                            || params.get("generation").and_then(Value::as_i64) != Some(generation)
                        {
                            Err(Error::new(
                                "stale_generation",
                                "process.put does not match the current plugin generation",
                            ))
                        } else {
                            match (
                                params
                                    .get("pid")
                                    .and_then(Value::as_u64)
                                    .and_then(|pid| u32::try_from(pid).ok())
                                    .filter(|pid| *pid > 0),
                                params.get("role").and_then(Value::as_str),
                            ) {
                                (Some(pid), Some("harness")) => {
                                    match timeout_at(
                                        request_deadline,
                                        monitoring
                                            .process_put(&agent_id, generation, pid, "harness"),
                                    )
                                    .await
                                    {
                                        Ok(result) => result,
                                        Err(_) => Err(Error::new(
                                            "unknown",
                                            "process.put acknowledgement deadline elapsed",
                                        )),
                                    }
                                }
                                _ => Err(Error::new(
                                    "invalid",
                                    "process.put requires a nonzero PID and the harness role",
                                )),
                            }
                        }
                    }
                    "usage.sample" => {
                        let current = state
                            .upgrade()
                            .is_some_and(|state| state.running.lock_recover().contains_key(&key));
                        if observation.is_some()
                            || !current
                            || params.get("agent_id").and_then(Value::as_str)
                                != Some(agent_id.as_str())
                            || params.get("generation").and_then(Value::as_i64) != Some(generation)
                        {
                            Err(Error::new(
                                "stale_generation",
                                "usage.sample does not match the current plugin generation",
                            ))
                        } else {
                            put_usage_sample(
                                &store,
                                &monitoring,
                                params,
                                &agent_id,
                                generation,
                                &source_credentials,
                                request_deadline,
                            )
                            .await
                        }
                    }
                    "events.put" => {
                        if let Some(order) = &stream_order
                            && params["source"].as_str() == Some("stream")
                            && params["agent_id"].as_str() == Some(&agent_id)
                            && params["generation"].as_i64() == Some(generation)
                        {
                            match timeout_at(request_deadline, order.lock()).await {
                                Ok(guard) => stream_guard = Some(guard),
                                Err(_) => {
                                    // Coverage is unresolved while the earlier batch owns the
                                    // stream. Retirement settles the missing suffix in order.
                                    let result = Err(late_batch_error(
                                        &state,
                                        &agent_id,
                                        generation,
                                        "stream_order",
                                    ));
                                    let _ =
                                        client.respond(request_id, result, reply_deadline).await;
                                    let _ = client.close_input(Instant::now() + RPC_GRACE).await;
                                    break;
                                }
                            }
                        }
                        let result = put_events(
                            &store,
                            &bus,
                            params,
                            binding,
                            BatchIntake {
                                host: &state,
                                monitoring: &monitoring,
                                credentials: &source_credentials,
                                policy: &command_guard_policy,
                            },
                            request_deadline,
                            &mut rejected_batch,
                        )
                        .await;
                        match result {
                            Err(error) if error.code == "unknown" => Err(late_batch_error(
                                &state,
                                &agent_id,
                                generation,
                                "store_commit",
                            )),
                            result => result,
                        }
                    }
                    "source.gap" => {
                        let is_producer = params.get("agent_id").and_then(Value::as_str)
                            == Some(agent_id.as_str())
                            && params.get("generation").and_then(Value::as_i64) == Some(generation);
                        if is_producer {
                            record_source_gap(
                                &store,
                                &bus,
                                params,
                                binding,
                                &source_credentials,
                                request_deadline,
                            )
                            .await
                        } else if observation.is_some() {
                            Err(Error::new(
                                "not_allowed",
                                "source.gap binding does not match this Agent generation",
                            ))
                        } else {
                            native_children::record_child_gap(
                                &store,
                                &bus,
                                params,
                                &agent_id,
                                generation,
                                &source_credentials,
                                request_deadline,
                            )
                            .await
                        }
                    }
                    "source.handoff" => {
                        source_handoff::record_source_handoff(
                            &store,
                            &bus,
                            params,
                            binding,
                            &source_credentials,
                            request_deadline,
                        )
                        .await
                    }
                    "source.health" => {
                        source_handoff::record_source_health(
                            &store,
                            &bus,
                            params,
                            binding,
                            &source_credentials,
                            request_deadline,
                        )
                        .await
                    }
                    "source.state" => {
                        let is_producer = params.get("agent_id").and_then(Value::as_str)
                            == Some(agent_id.as_str())
                            && params.get("generation").and_then(Value::as_i64) == Some(generation);
                        if is_producer {
                            match source_state_binding(&params, &agent_id, generation) {
                                Ok(source_name) => {
                                    match timeout_at(request_deadline, store.lock()).await {
                                        Ok(store) => {
                                            match store.read_source_state(
                                                &agent_id,
                                                generation,
                                                &source_name,
                                            ) {
                                                Ok(mut source_state) => {
                                                    crate::redaction::redact_value(
                                                        &mut source_state,
                                                        &source_credentials,
                                                    );
                                                    Ok(source_state)
                                                }
                                                Err(error) => Err(redact_native_error(
                                                    error,
                                                    &source_credentials,
                                                )),
                                            }
                                        }
                                        Err(_) => Err(Error::new(
                                            "busy",
                                            "Store lock deadline elapsed for source.state",
                                        )),
                                    }
                                }
                                Err(error) => Err(error),
                            }
                        } else if observation.is_some() {
                            Err(Error::new(
                                "not_allowed",
                                "source.state is outside this plugin's Agent binding",
                            ))
                        } else {
                            native_children::read_child_state(
                                &store,
                                &params,
                                &agent_id,
                                generation,
                                &source_credentials,
                                request_deadline,
                            )
                            .await
                        }
                    }
                    "child" => {
                        let wire = child_wire.unwrap_or(Value::Null);
                        drop(params);
                        if observation.is_some() {
                            Err(Error::new(
                                "not_allowed",
                                "observer callbacks cannot register native children",
                            ))
                        } else {
                            native_children::register_child(
                                &store,
                                &bus,
                                wire,
                                &agent_id,
                                generation,
                                &source_credentials,
                                request_deadline,
                            )
                            .await
                        }
                    }
                    "hook.event" => {
                        if observation.is_some() {
                            Err(Error::new(
                                "not_allowed",
                                "observer callbacks cannot claim turn context",
                            ))
                        } else {
                            match serde_json::from_value::<HookEvent>(params) {
                                Err(_) => {
                                    Err(Error::new("invalid", "hook.event params are malformed"))
                                }
                                Ok(event)
                                    if !matches!(
                                        event.hook.as_str(),
                                        "before_tool" | "after_tool" | "before_model"
                                    ) || event.turn_id.trim().is_empty()
                                        || event.consumer_id.trim().is_empty() =>
                                {
                                    Err(Error::new(
                                        "invalid",
                                        "hook.event requires before_tool or before_model with an exact turn and consumer",
                                    ))
                                }
                                Ok(event) => match timeout_at(request_deadline, store.lock()).await
                                {
                                    Ok(mut store) => match store.claim_turn_safe_point_context(
                                        &agent_id,
                                        generation,
                                        &event.turn_id,
                                        &event.consumer_id,
                                        (&event.hook, &request_id),
                                        now_ms(),
                                    ) {
                                        Ok(change) => {
                                            bus.publish(&change.events);
                                            serde_json::to_value(HookResponse {
                                                context: change.value,
                                            })
                                            .map_err(|_| {
                                                Error::new(
                                                    "internal",
                                                    "hook context response could not be encoded",
                                                )
                                            })
                                        }
                                        Err(error) => Err(error),
                                    },
                                    Err(_) => Err(Error::new(
                                        "busy",
                                        "hook context claim Store deadline elapsed",
                                    )),
                                },
                            }
                        }
                    }
                    _ => Err(Error::new(
                        "unsupported",
                        "plugin request method is not implemented",
                    )),
                }
            }
        };
        let result = if let Some(gap) = rejected_batch
            && !persist_rejected_batch_gap(&store, &bus, binding, gap, reply_deadline).await
        {
            Err(late_batch_error(
                &state,
                &agent_id,
                generation,
                "rejected_gap",
            ))
        } else {
            result
        };
        drop(stream_guard);
        let result = result.map_err(|error| redact_native_error(error, &source_credentials));
        let unsettled = matches!(
            method.as_str(),
            "events.put" | "source.gap" | "source.handoff" | "source.health"
        ) && result.as_ref().is_err_and(|error| error.code == "unknown");
        let replied = client.respond(request_id, result, reply_deadline).await;
        // On a broken reply path, the runtime actor owns source retirement and
        // generation loss. A reader that already reached EOF may still have
        // terminal facts queued, so drain them before the actor settles loss.
        if unsettled || (replied.is_err() && !client.reader_closed()) {
            let _ = client.close_input(Instant::now() + RPC_GRACE).await;
            break;
        }
    }
    if let Some(state) = state.upgrade() {
        if observation.is_none() {
            fence_runtime_generation(&state, &key);
        }
        signal_progress(&state);
    }
}

async fn completion_capture(
    state: &std::sync::Weak<HostState>,
    store: &StoreHandle,
    batch: &SourceBatch,
    until: Instant,
) -> (Option<eliot_store::CompletionWorktreeCapture>, Option<bool>) {
    if batch.source != "stream"
        || !batch
            .events
            .iter()
            .any(|event| event.kind == "turn.completed")
    {
        return (None, None);
    }
    let Some(host) = state.upgrade() else {
        return (None, None);
    };
    if host.completion_interests.lock_recover().is_empty() {
        return (None, None);
    }
    // Both lookups and Git capture leave the native lifecycle commit its own budget.
    let capture_until = until - RPC_GRACE;
    let (agent, registered, fields) = {
        let Ok(store) = timeout_at(capture_until, store.lock()).await else {
            return (None, None);
        };
        let Some(agent) = store
            .get_agent(&batch.agent_id)
            .ok()
            .flatten()
            .filter(|agent| agent.generation == batch.generation)
        else {
            return (None, None);
        };
        let fields = completion_fields_for(&host, &agent);
        if !fields.needed() {
            return (None, None);
        }
        let registered = if agent.project.is_empty() {
            Some(false)
        } else {
            store
                .get_project_registration("owner", &agent.project)
                .ok()
                .map(|registration| registration.is_some())
        };
        (agent, registered, fields)
    };
    let Some(turn_id) = agent.active_turn.as_deref() else {
        return (None, None);
    };
    if !batch.events.iter().any(|event| {
        event.kind == "turn.completed"
            && event.data.get("turn_id").and_then(Value::as_str) == Some(turn_id)
    }) {
        return (None, None);
    }

    let project_check = if agent.parent.is_some() || agent.policy_opt_out_reason.is_some() {
        None
    } else {
        match registered {
            Some(false) => Some(false),
            Some(true) => crate::config::load_project_registration_config(&host.data_root)
                .ok()
                .and_then(|projects| {
                    projects
                        .into_iter()
                        .find(|project| project.id == agent.project)
                })
                .map(|project| {
                    project
                        .policy
                        .as_ref()
                        .and_then(|policy| policy.check.as_ref())
                        .is_some()
                }),
            None => None,
        }
    };
    let capture = match (fields.worktree, project_check) {
        (true, Some(true)) => match (
            GitProbe::new(),
            agent.workspace.as_deref(),
            agent.base_oid.as_deref(),
        ) {
            (Ok(probe), Some(workspace), Some(base_oid)) => {
                if crate::logging::is_debug() {
                    crate::logging::record_debug(
                        &SecretPolicy::new(host.secrets.lock_recover().iter()),
                        "project.worktree.capture",
                        &[("agent", &agent.id), ("turn_id", turn_id)],
                        &[],
                    );
                }
                match timeout_at(
                    capture_until,
                    probe.capture_worktree(workspace, base_oid, now_ms()),
                )
                .await
                {
                    Ok(Ok(capture)) => Some(capture),
                    _ => None,
                }
            }
            _ => None,
        },
        (true, Some(false)) => host.monitoring.completion_worktree_capture(&agent),
        _ => None,
    };
    let capture = capture.map(|capture| eliot_store::CompletionWorktreeCapture {
        base: capture.base,
        fingerprint: capture.fingerprint,
        files: capture.files,
        lines: capture.lines,
    });
    (capture, project_check)
}

fn completion_fields_for(
    host: &HostState,
    agent: &eliot_core::Agent,
) -> crate::automation::CompletionFields {
    let interests = host.completion_interests.lock_recover();
    let project = format!("project:{}", agent.project);
    let mut fields = crate::automation::CompletionFields::default();
    let Some(subject) = serde_json::to_value(eliot_core::EventSubject::from(agent)).ok() else {
        return fields;
    };
    for principal in ["owner", project.as_str()] {
        if let Some(rules) = interests.get(principal) {
            for rule in rules.iter().filter(|rule| rule.matches_agent(&subject)) {
                fields.worktree |= rule.fields.worktree;
                fields.project_check |= rule.fields.project_check;
            }
        }
    }
    fields
}
async fn put_usage_sample(
    store: &StoreHandle,
    monitoring: &Monitoring,
    params: Value,
    bound_agent: &str,
    bound_generation: i64,
    credentials: &[String],
    until: Instant,
) -> Result<Value> {
    let mut request: SessionUsageSampleRequest = serde_json::from_value(params)
        .map_err(|_| Error::new("invalid", "usage.sample params are malformed"))?;
    request.validate()?;
    if request.agent_id != bound_agent || request.generation != bound_generation {
        return Err(Error::new(
            "stale_generation",
            "usage.sample binding does not match the current plugin generation",
        ));
    }
    let mut usage = serde_json::to_value(&request.usage)
        .map_err(|_| Error::new("invalid", "usage.sample payload could not be encoded"))?;
    SecretPolicy::new(credentials.iter()).redact_values(&mut usage);
    request.usage = serde_json::from_value(usage)
        .map_err(|_| Error::new("invalid", "usage.sample payload is invalid after redaction"))?;
    request.validate()?;

    let latest = match timeout_at(until, store.lock()).await {
        Ok(mut store) => store.put_session_usage_sample(
            bound_agent,
            bound_generation,
            request.turn_id.as_deref(),
            request.usage,
            now_ms(),
        ),
        Err(_) => Err(Error::new(
            "busy",
            "Store lock deadline elapsed for usage.sample",
        )),
    }?;
    monitoring.record_session_usage(bound_agent, bound_generation, latest);
    serde_json::to_value(SessionUsageSampleAck { accepted: true }).map_err(|_| {
        Error::new(
            "internal",
            "usage.sample acknowledgement could not be encoded",
        )
    })
}

async fn put_events(
    store: &StoreHandle,
    bus: &Bus,
    params: Value,
    binding: source_handoff::SourceBinding<'_>,
    intake: BatchIntake<'_>,
    until: Instant,
    rejected: &mut Option<SourceGap>,
) -> Result<Value> {
    let invalid_batch = || Error::new("invalid", "events.put params are malformed");
    let credentials = intake.credentials;
    let monitoring = intake.monitoring;
    let guard_policy = intake.policy;
    let gap = batch_gap(&params).ok_or_else(invalid_batch)?;
    let is_child = gap.agent_id != binding.agent_id || gap.generation != binding.generation;
    let subject = binding.expected_subject()?;
    if is_child && subject.is_some() {
        return Err(Error::new(
            "stale_generation",
            "events.put generation does not match this Agent generation",
        ));
    }
    let mut permission_refusal = None;
    let batch = serde_json::from_value(params)
        .map_err(|_| invalid_batch())
        .and_then(|mut batch| {
            permission_refusal = apply_command_guard(&mut batch, guard_policy, credentials)?;
            PreparedSourceBatch::new(batch, credentials)
        });
    let batch = match batch {
        Ok(batch) => batch,
        Err(error) => {
            *rejected = rejection_gap(gap, &error.code, credentials);
            return Err(error);
        }
    };
    let ready = batch.as_batch();
    permission_refusal = permission_refusal.filter(|refusal| {
        ready.events.iter().any(|event| {
            event.kind == "request.opened"
                && event.data["request_id"] == refusal.request_id
                && event.data["turn_id"] == refusal.turn_id
        })
    });
    let (completion_worktree, project_check) = if !is_child && subject.is_none() {
        completion_capture(intake.host, store, ready, until).await
    } else {
        (None, None)
    };
    let batch_id = ready.batch_id.clone();
    let event_kinds = ready
        .events
        .iter()
        .take(4)
        .map(|event| event.kind.chars().take(48).collect::<String>())
        .collect::<Vec<_>>()
        .join(",");
    let contains_session_usage = ready
        .events
        .iter()
        .any(|event| event.kind == "session.usage");
    let native_events = ready.events.len() as u64;
    if let Some(cutoff) = planned_restart_cutoff(intake.host) {
        record_planned_restart_cutoff(credentials, &gap, &batch_id, &event_kinds, &cutoff);
        return Err(planned_restart_cutoff_error(&cutoff));
    }
    let ready_group = if !is_child
        && subject.is_none()
        && ready.source == "stream"
        && (1..=MAX_TOOL_EVENTS_PER_BATCH).contains(&ready.events.len())
        && ready
            .events
            .iter()
            .all(|event| matches!(event.kind.as_str(), "tool.started" | "tool.completed"))
        && let Some(host) = intake.host.upgrade()
    {
        match ReadyToolBatch::new(batch) {
            Ok(batch) => {
                host.tool_batches
                    .try_submit(&host, store, bus, batch, until)
                    .await
            }
            Err(error) => {
                *rejected = rejection_gap(gap, &error.code, credentials);
                return Err(error);
            }
        }
    } else {
        ToolBatchOutcome::Direct(batch)
    };
    let result = match ready_group {
        ToolBatchOutcome::Committed {
            result,
            wait,
            write_timing,
            batches,
            group_bytes,
        } => {
            if crate::logging::is_debug()
                && let Ok(commit) = &result
            {
                let mut fields = vec![
                    ("lock_wait_us", wait.as_micros() as u64),
                    ("events", commit.events.len() as u64),
                    ("native_events", native_events),
                    ("replayed", u64::from(commit.replayed)),
                    ("commit_batches", batches as u64),
                    ("commit_batch_bytes", group_bytes as u64),
                ];
                if let Some(timing) = write_timing {
                    fields.extend([
                        ("lock_hold_us", timing.held.as_micros() as u64),
                        ("store_elapsed_us", timing.elapsed.as_micros() as u64),
                    ]);
                }
                crate::logging::record_debug(
                    &SecretPolicy::new(credentials),
                    "store.source_batch",
                    &[("agent", &gap.agent_id), ("source", &gap.source)],
                    &fields,
                );
            }
            result
        }
        ToolBatchOutcome::RestartCutoff(cutoff) => {
            record_planned_restart_cutoff(credentials, &gap, &batch_id, &event_kinds, &cutoff);
            return Err(planned_restart_cutoff_error(&cutoff));
        }
        ToolBatchOutcome::Unsettled(error) => Err(error),
        ToolBatchOutcome::Direct(batch) => {
            let queued = Instant::now();
            match timeout_at(until, store.lock()).await {
                Ok(mut store) => {
                    let acquired = Instant::now();
                    if let Some(cutoff) = planned_restart_cutoff(intake.host) {
                        drop(store);
                        record_planned_restart_cutoff(
                            credentials,
                            &gap,
                            &batch_id,
                            &event_kinds,
                            &cutoff,
                        );
                        return Err(planned_restart_cutoff_error(&cutoff));
                    }
                    let now = now_ms();
                    let started = Instant::now();
                    let result = if is_child {
                        store.put_native_child_batch(
                            binding.agent_id,
                            binding.generation,
                            batch,
                            now,
                        )
                    } else if let Some(subject) = subject {
                        store.put_observed_batch(batch, subject, now)
                    } else {
                        store.put_batch_with_completion_capture(
                            batch,
                            completion_worktree,
                            project_check,
                            now,
                        )
                    };
                    let store_elapsed = started.elapsed();
                    if let Ok(commit) = &result {
                        bus.publish(&commit.events);
                    }
                    let latest_usage = if result.is_ok() && contains_session_usage {
                        store
                            .get_session_usage(&gap.agent_id, gap.generation)
                            .unwrap_or(None)
                    } else {
                        None
                    };
                    let held = acquired.elapsed();
                    drop(store);
                    if let Some(usage) = latest_usage {
                        monitoring.record_session_usage(&gap.agent_id, gap.generation, usage);
                    }
                    if crate::logging::is_debug()
                        && let Ok(commit) = &result
                    {
                        crate::logging::record_debug(
                            &SecretPolicy::new(credentials),
                            "store.source_batch",
                            &[("agent", &gap.agent_id), ("source", &gap.source)],
                            &[
                                ("lock_wait_us", (acquired - queued).as_micros() as u64),
                                ("lock_hold_us", held.as_micros() as u64),
                                ("events", commit.events.len() as u64),
                                ("native_events", native_events),
                                ("replayed", u64::from(commit.replayed)),
                                ("store_elapsed_us", store_elapsed.as_micros() as u64),
                            ],
                        );
                    }
                    result
                }
                Err(_) => {
                    if let Some(cutoff) = planned_restart_cutoff(intake.host) {
                        record_planned_restart_cutoff(
                            credentials,
                            &gap,
                            &batch_id,
                            &event_kinds,
                            &cutoff,
                        );
                        return Err(planned_restart_cutoff_error(&cutoff));
                    }
                    Err(Error::new(
                        "busy",
                        "Store lock deadline elapsed for events.put",
                    ))
                }
            }
        }
    };
    match result {
        Ok(commit) => {
            let mut ack = commit.ack;
            ack.permission_refusal = permission_refusal;
            Ok(json!(ack))
        }
        Err(error) => {
            let cause = safe_cause_code(&error.code);
            record_store_batch_rejection(
                credentials,
                &AgentKey {
                    id: gap.agent_id.clone(),
                    generation: gap.generation,
                },
                &gap.source,
                &batch_id,
                &event_kinds,
                &cause,
                &error,
            );
            if !matches!(
                error.code.as_str(),
                "not_allowed" | "stale_generation" | "not_found" | "unknown"
            ) {
                *rejected = rejection_gap(gap, &cause, credentials);
            }
            match error.code.as_str() {
                "unknown" | "busy" | "not_allowed" | "stale_generation" => Err(error),
                _ => Err(Error::new(
                    "rejected",
                    format!("events.put rejected: {cause}"),
                )),
            }
        }
    }
}

fn planned_restart_cutoff(host: &std::sync::Weak<HostState>) -> Option<String> {
    host.upgrade()
        .and_then(|state| state.planned_restart_cutoff.lock_recover().clone())
}

fn planned_restart_cutoff_error(cutoff: &str) -> Error {
    Error::new(
        "unavailable",
        format!("planned restart native input cutoff {cutoff}; no new batch write was attempted"),
    )
}

fn record_planned_restart_cutoff(
    credentials: &[String],
    gap: &SourceGap,
    batch_id: &str,
    event_kinds: &str,
    cutoff: &str,
) {
    let policy = SecretPolicy::new(credentials.iter());
    let generation = gap.generation.to_string();
    crate::logging::record(
        &policy,
        "events_put_planned_restart_cutoff",
        &[
            ("agent_id", &gap.agent_id),
            ("generation", &generation),
            ("source", &gap.source),
            ("batch_id", batch_id),
            ("event_kind", event_kinds),
            ("coverage_ref", cutoff),
            ("outcome", "unacknowledged"),
        ],
        None,
    );
}

fn apply_command_guard(
    batch: &mut SourceBatch,
    policy: &CommandGuardPolicy,
    credentials: &[String],
) -> Result<Option<PermissionRefusal>> {
    for event in &mut batch.events {
        if event.kind == "request.opened"
            && let Some(details) = event.data.get_mut("details").and_then(Value::as_object_mut)
        {
            details.remove("guard_refusal");
        }
    }
    if !batch
        .events
        .iter()
        .any(|event| event.data.get(REQUEST_FIELD).is_some())
    {
        return Ok(None);
    }
    if batch.source != "stream" {
        return Err(Error::new(
            "not_allowed",
            "command guard context is valid only on the live stream source",
        ));
    }
    let mut refusal = None;
    for event in &mut batch.events {
        let Some(data) = event.data.as_object_mut() else {
            continue;
        };
        let Some(raw_guard) = data.remove(REQUEST_FIELD) else {
            continue;
        };
        if policy.is_empty() {
            continue;
        }
        if event.kind != "request.opened" {
            return Err(Error::new(
                "not_allowed",
                "command guard context requires request.opened",
            ));
        }
        let guard: CommandGuardRequest = serde_json::from_value(raw_guard)
            .map_err(|_| Error::new("invalid", "command guard context is malformed"))?;
        if guard.commands.is_empty()
            || guard.commands.len() > crate::command_guard::MAX_REQUEST_COMMANDS
            || guard.commands.iter().any(|command| {
                command.trim().is_empty()
                    || command.len() > crate::command_guard::MAX_COMMAND_BYTES
                    || command
                        .chars()
                        .any(|ch| ch.is_control() && !matches!(ch, '\t' | '\n' | '\r'))
            })
        {
            return Err(Error::new(
                "invalid",
                "command guard command list is invalid",
            ));
        }
        let Some(request_id) = data
            .get("request_id")
            .and_then(Value::as_str)
            .map(str::to_owned)
        else {
            return Err(Error::new(
                "invalid",
                "guarded request has no exact request id",
            ));
        };
        let Some(turn_id) = data
            .get("turn_id")
            .and_then(Value::as_str)
            .map(str::to_owned)
        else {
            return Err(Error::new(
                "invalid",
                "guarded request has no exact turn id",
            ));
        };
        if request_id.trim().is_empty() || turn_id.trim().is_empty() {
            return Err(Error::new("invalid", "guarded request identity is empty"));
        }
        let Some(reason) = policy.refusal(&guard.commands) else {
            continue;
        };
        if refusal.is_some() {
            return Err(Error::new(
                "unsupported",
                "one native source batch cannot decide multiple guarded requests",
            ));
        }
        let reason = SecretPolicy::new(credentials.iter()).redact_text(reason);
        let details = data
            .get_mut("details")
            .and_then(Value::as_object_mut)
            .ok_or_else(|| Error::new("invalid", "guarded approval details must be an object"))?;
        details.insert("guard_refusal".to_owned(), Value::String(reason.clone()));
        refusal = Some(PermissionRefusal {
            agent_id: batch.agent_id.clone(),
            generation: batch.generation,
            request_id,
            turn_id,
            message: reason,
        });
    }
    Ok(refusal)
}

// Recover only the source binding and recognized kinds from a malformed batch.
// The Store validates the binding and family ownership before committing the gap.
fn batch_gap(params: &Value) -> Option<SourceGap> {
    let mut families: Vec<_> = params["events"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|event| family_for_kind(event["kind"].as_str()?).ok())
        .map(str::to_owned)
        .collect();
    families.sort_unstable();
    families.dedup();
    Some(SourceGap {
        gap_id: String::new(),
        agent_id: params["agent_id"].as_str()?.to_owned(),
        generation: params["generation"].as_i64()?,
        source: params["source"].as_str()?.to_owned(),
        families,
        reason: String::new(),
        from: None,
        to: params["cursor"].as_str().map(str::to_owned),
    })
}

fn rejection_gap(mut gap: SourceGap, cause: &str, credentials: &[String]) -> Option<SourceGap> {
    if matches!(
        gap.source.as_str(),
        "daemon" | "os" | "git" | "workspace_process"
    ) {
        return None;
    }
    let policy = SecretPolicy::new(credentials);
    if gap
        .to
        .as_deref()
        .is_some_and(|cursor| policy.contains_native_secret(cursor))
    {
        gap.to = None;
    }
    gap.gap_id = Ulid::new().to_string();
    gap.reason = format!("events.put rejected: {cause}");
    redact_source_gap(&mut gap, credentials).ok()?;
    Some(gap)
}

async fn persist_rejected_batch_gap(
    store: &StoreHandle,
    bus: &Bus,
    binding: source_handoff::SourceBinding<'_>,
    gap: SourceGap,
    until: Instant,
) -> bool {
    let Ok(subject) = binding.expected_subject() else {
        return false;
    };
    // The gap is a host write. Retry it once after a proven no-write so
    // contention that outlasts the batch and first gap attempt can still settle.
    for attempt in 0..2 {
        let Ok(mut store) = timeout_at(until, store.lock()).await else {
            return false;
        };
        match store.record_rejected_batch_gap(
            gap.clone(),
            (binding.agent_id, binding.generation),
            subject,
            now_ms(),
        ) {
            Ok(change) => {
                bus.publish(&change.events);
                return true;
            }
            Err(error) if error.code == "busy" && attempt == 0 => {}
            Err(_) => return false,
        }
    }
    false
}
