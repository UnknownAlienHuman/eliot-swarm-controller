//! Authenticated source-request intake.
//!
//! Owns serve_plugin_requests: credential-checked events.put,
//! source.* and process.put serving with Store transactions,
//! redaction and event publication. Shared ordering and generation
//! ownership stay with super; actor lifecycle lives in actor.

use super::{
    AgentKey, EventSource, PluginConnection, deadline, fence_runtime_generation, late_batch_error,
    record_source_gap, record_store_batch_rejection, redact_native_error, redact_source_batch,
    safe_cause_code, signal_progress, source_state_binding,
};
use super::{native_children, observation, source_handoff};

use crate::common::{now_ms, secure_eq};

use eliot_core::{Error, SecretPolicy, SourceBatch};
use eliot_plugin::protocol::{HookEvent, HookResponse};
use eliot_plugin::sync::LockExt;
use serde_json::Value;

use tokio::time::timeout_at;

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
        let request_deadline = deadline();
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
                if method == "events.put" {
                    Err(late_batch_error(
                        &state,
                        &agent_id,
                        generation,
                        "plugin_credential_invalid",
                    ))
                } else {
                    Err(Error::new("not_allowed", "plugin credential is invalid"))
                }
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
                    "events.put" => match serde_json::from_value::<SourceBatch>(params) {
                        Ok(mut batch)
                            if batch.agent_id == agent_id && batch.generation == generation =>
                        {
                            match redact_source_batch(&mut batch, &source_credentials) {
                                Err(error) => Err(error),
                                Ok(()) => match binding.expected_subject() {
                                    Err(error) => Err(error),
                                    Ok(subject) => match timeout_at(request_deadline, store.lock())
                                        .await
                                    {
                                        Ok(mut store) => {
                                            let batch_policy =
                                                SecretPolicy::new(source_credentials.iter());
                                            let batch_source = batch_policy
                                                .redact_text(&batch.source)
                                                .chars()
                                                .take(256)
                                                .collect::<String>();
                                            let batch_id = batch_policy
                                                .redact_text(&batch.batch_id)
                                                .chars()
                                                .take(256)
                                                .collect::<String>();
                                            let event_kind = batch
                                                .events
                                                .iter()
                                                .take(4)
                                                .map(|event| {
                                                    batch_policy
                                                        .redact_text(&event.kind)
                                                        .chars()
                                                        .take(48)
                                                        .collect::<String>()
                                                })
                                                .collect::<Vec<_>>()
                                                .join(",");
                                            let rejection_key = AgentKey {
                                                id: agent_id.clone(),
                                                generation,
                                            };
                                            let result = match subject {
                                                Some(subject) => store.put_observed_batch(
                                                    batch,
                                                    subject,
                                                    now_ms(),
                                                ),
                                                None => store.put_batch(batch, now_ms()),
                                            };
                                            match result {
                                                Ok(commit) => {
                                                    bus.publish(&commit.events);
                                                    drop(store);
                                                    serde_json::to_value(commit.ack).map_err(|_| {
                                                        late_batch_error(
                                                            &state,
                                                            &agent_id,
                                                            generation,
                                                            "ack_encoding_failed",
                                                        )
                                                    })
                                                }
                                                Err(error) => {
                                                    let cause = if store.is_write_faulted() {
                                                        "store_write_faulted".to_owned()
                                                    } else {
                                                        format!(
                                                            "store_{}",
                                                            safe_cause_code(&error.code)
                                                        )
                                                    };
                                                    let unknown = late_batch_error(
                                                        &state, &agent_id, generation, &cause,
                                                    );
                                                    record_store_batch_rejection(
                                                        &source_credentials,
                                                        &rejection_key,
                                                        &batch_source,
                                                        &batch_id,
                                                        &event_kind,
                                                        &cause,
                                                        &error,
                                                    );
                                                    Err(unknown)
                                                }
                                            }
                                        }
                                        Err(_) => Err(late_batch_error(
                                            &state,
                                            &agent_id,
                                            generation,
                                            "store_lock_deadline",
                                        )),
                                    },
                                },
                            }
                        }
                        Ok(batch) => {
                            if observation.is_some() {
                                Err(Error::new(
                                    "stale_generation",
                                    "events.put generation does not match this Agent generation",
                                ))
                            } else {
                                native_children::put_child_batch(
                                    &store,
                                    &bus,
                                    &state,
                                    batch,
                                    (&agent_id, generation),
                                    &source_credentials,
                                    request_deadline,
                                )
                                .await
                            }
                        }
                        Err(_) => Err(Error::new("invalid", "events.put params are malformed")),
                    },
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
        let _ = client.respond(request_id, result, request_deadline).await;
    }
    if let Some(state) = state.upgrade() {
        if observation.is_none() {
            fence_runtime_generation(&state, &key);
        }
        signal_progress(&state);
    }
}
