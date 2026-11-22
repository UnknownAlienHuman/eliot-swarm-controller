//! Authenticated source-request intake.
//!
//! Owns serve_plugin_requests: credential-checked events.put,
//! source.* and process.put serving with Store transactions,
//! redaction and event publication. Shared ordering and generation
//! ownership stay with super; actor lifecycle lives in actor.

use super::{
    AgentKey, Bus, EventSource, PluginConnection, RPC_GRACE, StoreHandle, deadline,
    fence_runtime_generation, late_batch_error, record_source_gap, record_store_batch_rejection,
    redact_native_error, redact_source_batch, redact_source_gap, safe_cause_code, signal_progress,
    source_state_binding,
};
use super::{native_children, observation, source_handoff};

use crate::command_guard::{CommandGuardPolicy, CommandGuardRequest, REQUEST_FIELD};
use crate::common::{now_ms, secure_eq};

use eliot_core::{Error, PermissionRefusal, Result, SecretPolicy, SourceBatch, SourceGap};
use eliot_plugin::protocol::{HookEvent, HookResponse};
use eliot_plugin::sync::LockExt;
use eliot_store::family_for_kind;
use serde_json::{Value, json};

use tokio::time::{Instant, timeout_at};
use ulid::Ulid;

struct GuardEvaluation<'a> {
    policy: &'a CommandGuardPolicy,
    credentials: &'a [String],
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
    while let Some(request) = connection.next_request().await {
        let request_id = request.id.clone();
        let method = request.method.clone();
        let reply_deadline = deadline();
        // A Store lock timeout must leave time to transmit the proven rejection.
        let request_deadline = if method == "events.put" {
            reply_deadline - RPC_GRACE
        } else {
            reply_deadline
        };
        let mut rejected_batch = None;
        let result = if !matches!(
            method.as_str(),
            "events.put"
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
                "plugin host only accepts events.put, source.gap, source.handoff, source.health, source.state, process.put, child, and hook.event",
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
                    "events.put" => {
                        let result = put_events(
                            &store,
                            &bus,
                            params,
                            binding,
                            GuardEvaluation {
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
                                        "before_tool" | "before_model"
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
                                        &event.hook,
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
        let result = result.map_err(|error| redact_native_error(error, &source_credentials));
        let unsettled = matches!(
            method.as_str(),
            "events.put" | "source.gap" | "source.handoff" | "source.health"
        ) && result.as_ref().is_err_and(|error| error.code == "unknown");
        let replied = client.respond(request_id, result, reply_deadline).await;
        if let Some(gap) = rejected_batch {
            persist_rejected_batch_gap(&store, &bus, binding, gap).await;
        }
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

async fn put_events(
    store: &StoreHandle,
    bus: &Bus,
    params: Value,
    binding: source_handoff::SourceBinding<'_>,
    guard: GuardEvaluation<'_>,
    until: Instant,
    rejected: &mut Option<SourceGap>,
) -> Result<Value> {
    let invalid_batch = || Error::new("invalid", "events.put params are malformed");
    let credentials = guard.credentials;
    let guard_policy = guard.policy;
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
            redact_source_batch(&mut batch, credentials)?;
            Ok(batch)
        });
    let batch: SourceBatch = match batch {
        Ok(batch) => batch,
        Err(error) => {
            *rejected = rejection_gap(gap, &error.code, credentials);
            return Err(error);
        }
    };
    let batch_id = batch.batch_id.clone();
    let event_kinds = batch
        .events
        .iter()
        .take(4)
        .map(|event| event.kind.chars().take(48).collect::<String>())
        .collect::<Vec<_>>()
        .join(",");
    let result = match timeout_at(until, store.lock()).await {
        Ok(mut store) => {
            let now = now_ms();
            let result = if is_child {
                store.put_native_child_batch(binding.agent_id, binding.generation, batch, now)
            } else if let Some(subject) = subject {
                store.put_observed_batch(batch, subject, now)
            } else {
                store.put_batch(batch, now)
            };
            if let Ok(commit) = &result {
                bus.publish(&commit.events);
            }
            result
        }
        Err(_) => Err(Error::new(
            "busy",
            "Store lock deadline elapsed for events.put",
        )),
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

fn apply_command_guard(
    batch: &mut SourceBatch,
    policy: &CommandGuardPolicy,
    credentials: &[String],
) -> Result<Option<PermissionRefusal>> {
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
        if event.kind == "request.opened"
            && let Some(details) = event.data.get_mut("details").and_then(Value::as_object_mut)
        {
            details.remove("guard_refusal");
        }
        let Some(data) = event.data.as_object_mut() else {
            continue;
        };
        let Some(raw_guard) = data.remove(REQUEST_FIELD) else {
            continue;
        };
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
                    || command.chars().any(char::is_control)
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
        .is_some_and(|cursor| policy.contains_text(cursor))
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
) {
    let Ok(subject) = binding.expected_subject() else {
        return;
    };
    let until = deadline();
    // The gap is a host write. Retry it once after a proven no-write so
    // contention that outlasts the batch and first gap attempt can still settle.
    for attempt in 0..2 {
        let Ok(mut store) = timeout_at(until, store.lock()).await else {
            return;
        };
        match store.record_rejected_batch_gap(
            gap.clone(),
            (binding.agent_id, binding.generation),
            subject,
            now_ms(),
        ) {
            Ok(change) => {
                bus.publish(&change.events);
                return;
            }
            Err(error) if error.code == "busy" && attempt == 0 => {}
            Err(_) => return,
        }
    }
}
