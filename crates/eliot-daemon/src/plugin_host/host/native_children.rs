//! Native-child observation on an authenticated producer connection.
//!
//! Producer identity comes from the private connection, never the wire. A child
//! receives no credential and no control verbs.

use super::{Bus, StoreHandle};
use crate::common::now_ms;
use eliot_core::{ChildRegistration, Error, Result, SecretPolicy, SourceBatch, SourceGap};
use eliot_plugin::protocol::ChildParams;
use serde_json::Value;
use std::sync::Weak;
use tokio::time::{Instant, timeout_at};

pub(super) async fn register_child(
    store: &StoreHandle,
    bus: &Bus,
    params: Value,
    producer: &str,
    producer_generation: i64,
    secrets: &[String],
    until: Instant,
) -> Result<Value> {
    let ChildParams { registration, .. } = serde_json::from_value(params)
        .map_err(|_| Error::new("invalid", "child params are malformed"))?;
    registration.validate()?;
    check_registration_secrets(&registration, secrets)?;
    let mut store = timeout_at(until, store.lock())
        .await
        .map_err(|_| Error::new("busy", "Store lock deadline elapsed for child"))?;
    let result = store.register_native_child(producer, producer_generation, registration, now_ms());
    let faulted = store.is_write_faulted();
    match result {
        Ok(change) => {
            bus.publish(&change.events);
            let ack = change.value;
            drop(store);
            serde_json::to_value(ack).map_err(|_| {
                Error::new(
                    "unknown",
                    "child acknowledgement is unavailable; settle with source.state",
                )
            })
        }
        Err(_) if faulted => Err(Error::new(
            "unknown",
            "child outcome is unknown; settle with source.state",
        )),
        Err(error) => Err(super::redact_native_error(error, secrets)),
    }
}

pub(super) async fn put_child_batch(
    store: &StoreHandle,
    bus: &Bus,
    state: &Weak<super::HostState>,
    mut batch: SourceBatch,
    producer: &str,
    producer_generation: i64,
    secrets: &[String],
    until: Instant,
) -> Result<Value> {
    super::redact_source_batch(&mut batch, secrets)?;
    let child_id = batch.agent_id.clone();
    let child_generation = batch.generation;
    let mut store = timeout_at(until, store.lock()).await.map_err(|_| {
        super::late_batch_error(state, &child_id, child_generation, "store_lock_deadline")
    })?;
    let result = store.put_native_child_batch(producer, producer_generation, batch, now_ms());
    match result {
        Ok(commit) => {
            bus.publish(&commit.events);
            drop(store);
            serde_json::to_value(commit.ack).map_err(|_| {
                super::late_batch_error(state, &child_id, child_generation, "ack_encoding_failed")
            })
        }
        Err(error) => {
            let cause = if store.is_write_faulted() {
                "store_write_faulted".to_owned()
            } else {
                format!("store_{}", super::safe_cause_code(&error.code))
            };
            Err(super::late_batch_error(
                state,
                &child_id,
                child_generation,
                &cause,
            ))
        }
    }
}

pub(super) async fn record_child_gap(
    store: &StoreHandle,
    bus: &Bus,
    params: Value,
    producer: &str,
    producer_generation: i64,
    secrets: &[String],
    until: Instant,
) -> Result<Value> {
    let mut gap: SourceGap = serde_json::from_value(params)
        .map_err(|_| Error::new("invalid", "source.gap params are malformed"))?;
    super::redact_source_gap(&mut gap, secrets)?;
    let mut store = timeout_at(until, store.lock())
        .await
        .map_err(|_| Error::new("busy", "Store lock deadline elapsed for source.gap"))?;
    let result = store.record_native_child_source_gap(producer, producer_generation, gap, now_ms());
    let faulted = store.is_write_faulted();
    match result {
        Ok(change) => {
            bus.publish(&change.events);
            let ack = change.value;
            drop(store);
            serde_json::to_value(ack).map_err(|_| {
                Error::new(
                    "internal",
                    "source.gap acknowledgement could not be encoded",
                )
            })
        }
        Err(_) if faulted => Err(Error::new(
            "unknown",
            "source.gap outcome is unknown; settle with source.state",
        )),
        Err(error) => Err(super::redact_native_error(error, secrets)),
    }
}

pub(super) fn child_state_binding(params: &Value) -> Result<(String, i64, String)> {
    let object = params
        .as_object()
        .ok_or_else(|| Error::new("invalid", "source.state params must be an object"))?;
    if object
        .keys()
        .any(|key| !["agent_id", "generation", "source"].contains(&key.as_str()))
    {
        return Err(Error::new(
            "invalid",
            "source.state contains an unsupported field",
        ));
    }
    let agent = object
        .get("agent_id")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::new("invalid", "source.state requires agent_id"))?;
    let generation = object
        .get("generation")
        .and_then(Value::as_i64)
        .ok_or_else(|| Error::new("invalid", "source.state requires generation"))?;
    let source = object
        .get("source")
        .and_then(Value::as_str)
        .filter(|source| !source.trim().is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| Error::new("invalid", "source.state requires source"))?;
    Ok((agent.to_owned(), generation, source))
}

pub(super) async fn read_child_state(
    store: &StoreHandle,
    params: &Value,
    producer: &str,
    producer_generation: i64,
    secrets: &[String],
    until: Instant,
) -> Result<Value> {
    let (child, child_generation, source) = child_state_binding(params)?;
    let store = timeout_at(until, store.lock())
        .await
        .map_err(|_| Error::new("busy", "Store lock deadline elapsed for source.state"))?;
    match store.read_native_child_source_state(
        producer,
        producer_generation,
        &child,
        child_generation,
        &source,
    ) {
        Ok(mut state) => {
            crate::redaction::redact_value(&mut state, secrets);
            Ok(state)
        }
        Err(error) => Err(super::redact_native_error(error, secrets)),
    }
}

pub(super) async fn is_codex_producer(store: &StoreHandle, producer: &str) -> Result<bool> {
    let store = timeout_at(super::deadline(), store.lock())
        .await
        .map_err(|_| Error::new("unknown", "native child producer read timed out"))?;
    let agent = store
        .get_agent(producer)?
        .ok_or_else(|| Error::new("not_found", "native child producer was not found"))?;
    Ok(agent.harness == "codex")
}

pub(super) async fn mark_children_unavailable(
    store: &StoreHandle,
    bus: &Bus,
    producer: &str,
    producer_generation: i64,
    reason: &str,
    reader_closed: bool,
) -> Result<()> {
    let mut store = timeout_at(super::deadline(), store.lock())
        .await
        .map_err(|_| Error::new("unknown", "native child availability persistence timed out"))?;
    let events = store.mark_native_children_unavailable(
        producer,
        producer_generation,
        reason,
        reader_closed,
        now_ms(),
    )?;
    bus.publish(&events);
    Ok(())
}

fn check_registration_secrets(registration: &ChildRegistration, secrets: &[String]) -> Result<()> {
    let policy = SecretPolicy::new(secrets);
    if [
        registration.registration_id.as_str(),
        registration.parent_id.as_str(),
        registration.parent_native_ref.as_str(),
        registration.native_scope.as_str(),
        registration.native_ref.as_str(),
    ]
    .into_iter()
    .any(|value| policy.contains_text(value))
    {
        return Err(Error::new(
            "secret_in_payload",
            "native source payload contains a known secret in immutable metadata",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registration() -> ChildRegistration {
        ChildRegistration {
            registration_id: "spawn:child".to_owned(),
            parent_id: "a1".to_owned(),
            parent_generation: 1,
            parent_native_ref: "parent-thread".to_owned(),
            native_scope: "codex:test".to_owned(),
            native_ref: "child-thread".to_owned(),
        }
    }

    #[test]
    fn child_state_binding_accepts_only_the_supported_shape() {
        let bound = child_state_binding(&serde_json::json!({
            "agent_id": "a2",
            "generation": 1,
            "source": "stream",
        }))
        .unwrap();
        assert_eq!(bound.0, "a2");
        assert_eq!(bound.1, 1);
        assert_eq!(bound.2, "stream");
        for params in [
            serde_json::json!({"agent_id": "a1", "generation": 1}),
            serde_json::json!({"agent_id": "a1", "generation": 1, "source": "stream", "extra": true}),
            serde_json::json!({"agent_id": "a1", "generation": 1, "source": "  "}),
        ] {
            assert_eq!(child_state_binding(&params).unwrap_err().code, "invalid");
        }
    }

    #[test]
    fn registration_secrets_reject_known_values_without_mutation() {
        let clean = registration();
        check_registration_secrets(&clean, &["other".to_owned()]).unwrap();
        for field in [
            "registration_id",
            "parent_id",
            "parent_native_ref",
            "native_scope",
            "native_ref",
        ] {
            let mut value = serde_json::to_value(registration()).unwrap();
            value[field] = serde_json::json!("prefix-secret-value");
            let attempt: ChildRegistration = serde_json::from_value(value).unwrap();
            let original = attempt.clone();
            let error =
                check_registration_secrets(&attempt, &["secret-value".to_owned()]).unwrap_err();
            assert_eq!(error.code, "secret_in_payload");
            assert_eq!(attempt, original);
        }
    }
}
