use super::{Bus, StoreHandle};
use eliot_core::{
    Error as CoreError, Result as CoreResult, SecretPolicy, SourceHandoff, SourceHandoffAck,
};
use serde_json::Value;
use tokio::time::{Instant, timeout_at};

pub(super) async fn record_source_handoff(
    store: &StoreHandle,
    bus: &Bus,
    params: Value,
    expected_agent: &str,
    generation: i64,
    secrets: &[String],
    until: Instant,
) -> CoreResult<Value> {
    let mut handoff = decode_handoff(params)?;
    if handoff.agent_id != expected_agent {
        return Err(CoreError::new(
            "not_allowed",
            "source.handoff Agent binding does not match this plugin connection",
        ));
    }
    if handoff.generation != generation {
        return Err(CoreError::new(
            "stale_generation",
            "source.handoff generation does not match this plugin connection",
        ));
    }
    sanitize_handoff(&mut handoff, secrets)?;

    // Store rechecks source, family, generation, predecessor ownership, and the
    // successor fence transactionally. Never hold its lock across an await.
    let (result, write_faulted) = {
        let mut store = timeout_at(until, store.lock()).await.map_err(|_| {
            CoreError::new("busy", "Store lock deadline elapsed for source.handoff")
        })?;
        let result = store.record_source_handoff(handoff, super::now_ms());
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
