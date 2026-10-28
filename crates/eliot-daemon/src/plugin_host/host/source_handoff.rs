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
                CoreError::new(
                    "unknown",
                    "source.handoff acknowledgement is unavailable; settle with source.state",
                )
            })
        }
        Err(_) if write_faulted => Err(CoreError::new(
            "unknown",
            "source.handoff outcome is unknown; settle with source.state",
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
    let subject = binding.expected_subject()?.ok_or_else(|| {
        CoreError::new(
            "unsupported",
            "controlled source.health has no current caller",
        )
    })?;
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
    let (result, faulted) = {
        let mut store = timeout_at(until, store.lock())
            .await
            .map_err(|_| CoreError::new("busy", "Store lock deadline elapsed for source.health"))?;
        let result = store.set_native_source_health(update, subject, super::now_ms());
        (result, store.is_write_faulted())
    };
    match result {
        Ok(change) => {
            bus.publish(&change.events);
            serde_json::to_value(change.value).map_err(|_| {
                CoreError::new(
                    "unknown",
                    "source.health acknowledgement is unavailable; settle with source.state",
                )
            })
        }
        Err(_) if faulted => Err(CoreError::new(
            "unknown",
            "source.health outcome is unknown; settle with source.state",
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

#[cfg(test)]
mod tests {
    use super::*;
    use eliot_core::SourceHandoffGap;
    use serde_json::json;

    fn handoff() -> SourceHandoff {
        SourceHandoff {
            agent_id: "a1".to_owned(),
            generation: 1,
            source: "fake-stream".to_owned(),
            families: vec!["session".to_owned()],
            successor: "fake-next".to_owned(),
            mechanism: "native_stream".to_owned(),
            cursor: None,
            gap: Some(SourceHandoffGap {
                gap_id: "gap-1".to_owned(),
                reason: "reader resumed".to_owned(),
                from: Some("cursor-1".to_owned()),
                to: Some("cursor-2".to_owned()),
            }),
        }
    }

    #[test]
    fn secret_in_immutable_gap_metadata_fails_before_reason_redaction() {
        let mut handoff = handoff();
        handoff.gap.as_mut().unwrap().gap_id = "secret-gap-id".to_owned();
        handoff.gap.as_mut().unwrap().reason = "secret-gap-id in reason".to_owned();
        let original = handoff.clone();

        let error = sanitize_handoff(&mut handoff, &["secret-gap-id".to_owned()]).unwrap_err();

        assert_eq!(error.code, "secret_in_payload");
        assert_eq!(handoff, original);
    }

    #[test]
    fn only_gap_reason_is_redacted() {
        let mut handoff = handoff();
        handoff.gap.as_mut().unwrap().reason = "reader saw test-secret".to_owned();
        let mut expected = handoff.clone();
        expected.gap.as_mut().unwrap().reason = "reader saw [REDACTED]".to_owned();

        sanitize_handoff(&mut handoff, &["test-secret".to_owned()]).unwrap();

        assert_eq!(handoff, expected);
    }

    #[test]
    fn top_level_and_nested_gap_unknown_fields_are_rejected() {
        let mut top_level = serde_json::to_value(handoff()).unwrap();
        top_level["unexpected"] = json!(true);

        let mut nested_gap = serde_json::to_value(handoff()).unwrap();
        nested_gap["gap"]["unexpected"] = json!(true);

        for params in [top_level, nested_gap] {
            let error = decode_handoff(params).unwrap_err();
            assert_eq!(error.code, "invalid");
            assert_eq!(error.message, "source.handoff params are malformed");
        }
    }
}
