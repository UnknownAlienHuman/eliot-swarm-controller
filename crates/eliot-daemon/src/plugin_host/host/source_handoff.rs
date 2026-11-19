use super::{Bus, StoreHandle};
use eliot_core::{
    Agent, Error as CoreError, Result as CoreResult, SecretPolicy, SourceHandoff, SourceHandoffAck,
    SourceHealthUpdate,
};
use serde_json::Value;
use tokio::time::{Instant, timeout_at};

#[derive(Clone, Copy)]
pub(super) struct SourceBinding<'a> {
    pub agent_id: &'a str,
    pub generation: i64,
    pub observation: Option<&'a Agent>,
}

impl<'a> SourceBinding<'a> {
    pub(super) fn expected_subject(self) -> CoreResult<Option<(&'a str, &'a str)>> {
        let Some(agent) = self.observation else {
            return Ok(None);
        };
        if agent.id != self.agent_id
            || agent.generation != self.generation
            || !agent.observe_only
            || agent.own_identity
            || agent.origin != eliot_core::AgentOrigin::External
        {
            return Err(CoreError::new(
                "not_allowed",
                "observer source context is not bound",
            ));
        }
        match (agent.native_scope.as_deref(), agent.native_ref.as_deref()) {
            (Some(scope), Some(reference)) if !scope.is_empty() && !reference.is_empty() => {
                Ok(Some((scope, reference)))
            }
            _ => Err(CoreError::new(
                "not_allowed",
                "observer source context has no native subject",
            )),
        }
    }

    fn validate(self, agent: &str, generation: i64) -> CoreResult<()> {
        if agent != self.agent_id {
            return Err(CoreError::new(
                "not_allowed",
                "source callback Agent does not match this plugin connection",
            ));
        }
        if generation != self.generation {
            return Err(CoreError::new(
                "stale_generation",
                "source callback generation does not match this plugin connection",
            ));
        }
        Ok(())
    }
}

pub(super) async fn record_source_handoff(
    store: &StoreHandle,
    bus: &Bus,
    params: Value,
    binding: SourceBinding<'_>,
    secrets: &[String],
    until: Instant,
) -> CoreResult<Value> {
    let mut handoff = decode_handoff(params)?;
    binding.validate(&handoff.agent_id, handoff.generation)?;
    sanitize_handoff(&mut handoff, secrets)?;
    let subject = binding.expected_subject()?;

    // Store rechecks source, family, generation, predecessor ownership, and the
    // successor fence transactionally. Never hold its lock across an await.
    let (result, write_faulted) = {
        let mut store = timeout_at(until, store.lock()).await.map_err(|_| {
            CoreError::new("busy", "Store lock deadline elapsed for source.handoff")
        })?;
        let result = match subject {
            Some(subject) => {
                store.record_observed_source_handoff(handoff, subject, super::now_ms())
            }
            None => store.record_source_handoff(handoff, super::now_ms()),
        };
        (result, store.is_write_faulted())
    };

    match result {
        Ok(change) => {
            // The Store commit precedes publication and acknowledgement.
            bus.publish(&change.events);
            let ack: SourceHandoffAck = change.value;
            serde_json::to_value(ack).map_err(|_| {
                CoreError::new("unknown", "source.handoff acknowledgement is unavailable")
            })
        }
        Err(_) if write_faulted => Err(CoreError::new(
            "unknown",
            "source.handoff outcome is unknown",
        )),
        Err(error) => Err(super::redact_native_error(error, secrets)),
    }
}

pub(super) async fn record_source_health(
    store: &StoreHandle,
    bus: &Bus,
    params: Value,
    binding: SourceBinding<'_>,
    secrets: &[String],
    until: Instant,
) -> CoreResult<Value> {
    let subject = binding.expected_subject()?;
    let update: SourceHealthUpdate = serde_json::from_value(params)
        .map_err(|_| CoreError::new("invalid", "source.health params are malformed"))?;
    binding.validate(&update.agent_id, update.generation)?;
    update.validate()?;
    let policy = SecretPolicy::new(secrets);
    if [&update.agent_id, &update.source, &update.health]
        .into_iter()
        .any(|value| policy.contains_text(value))
    {
        return Err(CoreError::new(
            "secret_in_payload",
            "source.health contains a known secret in immutable metadata",
        ));
    }
    // Store rechecks source, generation, Agent authority, and the open-row
    // fence transactionally. Never hold its lock across an await.
    let (result, faulted) = {
        let mut store = timeout_at(until, store.lock())
            .await
            .map_err(|_| CoreError::new("busy", "Store lock deadline elapsed for source.health"))?;
        let result = match subject {
            Some(subject) => store.set_native_source_health(update, subject, super::now_ms()),
            None => store.set_source_health(update, super::now_ms()),
        };
        (result, store.is_write_faulted())
    };
    match result {
        Ok(change) => {
            bus.publish(&change.events);
            serde_json::to_value(change.value).map_err(|_| {
                CoreError::new("unknown", "source.health acknowledgement is unavailable")
            })
        }
        Err(_) if faulted => Err(CoreError::new(
            "unknown",
            "source.health outcome is unknown",
        )),
        Err(error) => Err(super::redact_native_error(error, secrets)),
    }
}

fn decode_handoff(params: Value) -> CoreResult<SourceHandoff> {
    serde_json::from_value(params)
        .map_err(|_| CoreError::new("invalid", "source.handoff params are malformed"))
}

fn sanitize_handoff(handoff: &mut SourceHandoff, secrets: &[String]) -> CoreResult<()> {
    handoff.prepare(&SecretPolicy::new(secrets))
}
