//! Native-family handoff wire data and pure acknowledgement/readback checks.
//! Store remains authoritative for the transaction and successor publication.

use crate::{Error, Result, SecretPolicy, api::SourceOwner, canonical_json_sha256, parse_agent_id};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value};

/// An explicit uncovered interval committed with the ownership transfer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "Self", deny_unknown_fields)]
pub struct SourceHandoffGap {
    pub gap_id: String,
    pub reason: String,
    pub from: Option<String>,
    pub to: Option<String>,
}

/// `source` names the expected predecessor. Families are a semantic set; no
/// other arrays or opaque identifiers are normalized by this contract.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "Self", deny_unknown_fields)]
pub struct SourceHandoff {
    pub agent_id: String,
    pub generation: i64,
    pub source: String,
    pub families: Vec<String>,
    pub successor: String,
    pub mechanism: String,
    pub cursor: Option<String>,
    pub gap: Option<SourceHandoffGap>,
}

/// The committed transfer, not a receipt for native input or successor facts.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "Self", deny_unknown_fields)]
pub struct SourceHandoffAck {
    pub agent_id: String,
    pub generation: i64,
    pub source: String,
    pub families: Vec<String>,
    pub successor: String,
    pub mechanism: String,
    pub cursor: Option<String>,
    pub gap_id: Option<String>,
    pub seq: i64,
}

// Remote Self derives keep one field definition while allowing this small
// object-map boundary. Ordinary named-struct derives also accept arrays.
impl Serialize for SourceHandoffGap {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        Self::serialize(self, serializer)
    }
}

impl<'de> Deserialize<'de> for SourceHandoffGap {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let map = Map::<String, Value>::deserialize(deserializer)?;
        Self::deserialize(Value::Object(map)).map_err(serde::de::Error::custom)
    }
}

impl Serialize for SourceHandoff {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        Self::serialize(self, serializer)
    }
}

impl<'de> Deserialize<'de> for SourceHandoff {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let map = Map::<String, Value>::deserialize(deserializer)?;
        Self::deserialize(Value::Object(map)).map_err(serde::de::Error::custom)
    }
}

impl Serialize for SourceHandoffAck {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        Self::serialize(self, serializer)
    }
}

impl<'de> Deserialize<'de> for SourceHandoffAck {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let map = Map::<String, Value>::deserialize(deserializer)?;
        Self::deserialize(Value::Object(map)).map_err(serde::de::Error::custom)
    }
}

impl SourceHandoff {
    /// Prepare once with the complete scoped policy before submission, and retain
    /// this same value for acknowledgement/readback. The Host repeats this check
    /// with its policy; an unknown additional Host secret remains an unproven
    /// digest rather than permission to guess a different request.
    pub fn prepare(&mut self, policy: &SecretPolicy) -> Result<()> {
        let immutable_secret = [
            self.agent_id.as_str(),
            self.source.as_str(),
            self.successor.as_str(),
            self.mechanism.as_str(),
        ]
        .into_iter()
        .any(|value| policy.contains_text(value))
            || self
                .families
                .iter()
                .any(|family| policy.contains_text(family))
            || self
                .cursor
                .as_deref()
                .is_some_and(|cursor| policy.contains_text(cursor))
            || self.gap.as_ref().is_some_and(|gap| {
                policy.contains_text(&gap.gap_id)
                    || [&gap.from, &gap.to]
                        .into_iter()
                        .flatten()
                        .any(|cursor| policy.contains_text(cursor))
            });
        // Check every immutable field before redacting even the mutable reason.
        if immutable_secret {
            return Err(Error::new(
                "secret_in_payload",
                "source.handoff contains a known secret in immutable metadata",
            ));
        }
        if let Some(gap) = self.gap.as_mut() {
            gap.reason = policy.redact_text(&gap.reason);
        }
        Ok(())
    }

    pub fn validate(&self) -> Result<()> {
        parse_agent_id(&self.agent_id)?;
        if self.generation < 0
            || self.source.trim().is_empty()
            || self.successor.trim().is_empty()
            || self.mechanism.trim().is_empty()
            || self.source == self.successor
            || self.cursor.is_some() == self.gap.is_some()
            || self
                .cursor
                .as_ref()
                .is_some_and(|cursor| cursor.trim().is_empty())
            || self
                .gap
                .as_ref()
                .is_some_and(|gap| gap.gap_id.trim().is_empty() || gap.reason.trim().is_empty())
        {
            return Err(Error::new("invalid", "invalid native source handoff"));
        }
        validate_families(&self.families)
    }

    pub fn validate_ack(&self, ack: &SourceHandoffAck) -> Result<()> {
        if self.validate().is_err()
            || ack.agent_id != self.agent_id
            || ack.generation != self.generation
            || ack.source != self.source
            || ack.successor != self.successor
            || ack.mechanism != self.mechanism
            || validate_families(&ack.families).is_err()
            || sorted_families(&ack.families) != sorted_families(&self.families)
            || ack.cursor != self.cursor
            || ack.gap_id.as_deref() != self.gap.as_ref().map(|gap| gap.gap_id.as_str())
            || ack.seq <= 0
        {
            return Err(unproven());
        }
        Ok(())
    }

    /// Verify the predecessor's private source.state projection. The retained
    /// digest binds fields (including gap reason) absent from the ACK. Current
    /// owners and boundary must still match; the last ACK alone is insufficient.
    pub fn ack_from_state(&self, value: &Value) -> Result<SourceHandoffAck> {
        self.validate().map_err(|_| unproven())?;
        let map = value.as_object().ok_or_else(unproven)?;
        let state: HandoffState =
            serde_json::from_value(Value::Object(map.clone())).map_err(|_| unproven())?;
        if state.agent_id != self.agent_id
            || state.generation != self.generation
            || state.source != self.source
        {
            return Err(unproven());
        }
        let mut normalized = self.clone();
        normalized.families.sort_unstable();
        let payload = serde_json::to_value(normalized).map_err(|_| unproven())?;
        let digest = canonical_json_sha256(&payload).map_err(|_| unproven())?;
        if state.last_handoff_digest != digest {
            return Err(unproven());
        }
        self.validate_ack(&state.last_handoff)?;
        for family in &self.families {
            let mut owners = state.owners.iter().filter(|owner| owner.family == *family);
            let owner = owners.next().ok_or_else(unproven)?;
            if owners.next().is_some()
                || owner.source != self.successor
                || owner.mechanism != self.mechanism
            {
                return Err(unproven());
            }
        }
        match (&self.cursor, &self.gap) {
            (Some(cursor), None) if state.cursor.as_ref() == Some(cursor) => {}
            (None, Some(gap))
                if state.gap_id.as_ref() == Some(&gap.gap_id)
                    && state.gap_from == gap.from
                    && state.gap_to == gap.to
                    && state
                        .gap_seq
                        .is_some_and(|seq| seq > 0 && seq < state.last_handoff.seq) => {}
            _ => return Err(unproven()),
        }
        Ok(state.last_handoff)
    }
}

fn validate_families(families: &[String]) -> Result<()> {
    if families.is_empty()
        || sorted_families(families)
            .windows(2)
            .any(|pair| pair[0] == pair[1])
    {
        return Err(Error::new(
            "invalid",
            "handoff families must be nonempty and unique",
        ));
    }
    if families.iter().any(|family| {
        !matches!(
            family.as_str(),
            "session" | "turn" | "message" | "tool" | "request" | "child" | "goal" | "usage"
        )
    }) {
        return Err(Error::new(
            "not_allowed",
            "handoff cannot transfer daemon-owned families",
        ));
    }
    if families
        .iter()
        .any(|family| matches!(family.as_str(), "child" | "goal" | "usage"))
    {
        return Err(Error::new(
            "unsupported",
            "handoff family is not implemented",
        ));
    }
    Ok(())
}

fn sorted_families(families: &[String]) -> Vec<&str> {
    let mut families: Vec<_> = families.iter().map(String::as_str).collect();
    families.sort_unstable();
    families
}

#[derive(Deserialize)]
struct HandoffState {
    agent_id: String,
    generation: i64,
    source: String,
    #[serde(deserialize_with = "owner_objects")]
    owners: Vec<SourceOwner>,
    #[serde(deserialize_with = "nullable")]
    cursor: Option<String>,
    #[serde(deserialize_with = "nullable")]
    gap_id: Option<String>,
    #[serde(deserialize_with = "nullable")]
    gap_from: Option<String>,
    #[serde(deserialize_with = "nullable")]
    gap_to: Option<String>,
    #[serde(deserialize_with = "nullable")]
    gap_seq: Option<i64>,
    last_handoff_digest: [u8; 32],
    last_handoff: SourceHandoffAck,
}

fn owner_objects<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Vec<SourceOwner>, D::Error> {
    let maps = Vec::<Map<String, Value>>::deserialize(deserializer)?;
    maps.into_iter()
        .map(|map| object_value(map).map_err(serde::de::Error::custom))
        .collect()
}

fn object_value<T: DeserializeOwned>(map: Map<String, Value>) -> serde_json::Result<T> {
    serde_json::from_value(Value::Object(map))
}

fn nullable<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(
    deserializer: D,
) -> std::result::Result<Option<T>, D::Error> {
    Option::<T>::deserialize(deserializer)
}

fn unproven() -> Error {
    Error::new(
        "unknown",
        "source handoff acknowledgement or current ownership is unproven",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn request(gap: bool) -> SourceHandoff {
        SourceHandoff {
            agent_id: "a1".to_owned(),
            generation: 1,
            source: "stream".to_owned(),
            families: vec!["turn".to_owned(), "message".to_owned()],
            successor: "transcript".to_owned(),
            mechanism: "fake_recovery".to_owned(),
            cursor: (!gap).then(|| "edge:1".to_owned()),
            gap: gap.then(|| SourceHandoffGap {
                gap_id: "gap:1".to_owned(),
                reason: "native stream has no replay cursor".to_owned(),
                from: None,
                to: Some("edge:1".to_owned()),
            }),
        }
    }

    fn ack(request: &SourceHandoff) -> SourceHandoffAck {
        SourceHandoffAck {
            agent_id: request.agent_id.clone(),
            generation: request.generation,
            source: request.source.clone(),
            families: vec!["message".to_owned(), "turn".to_owned()],
            successor: request.successor.clone(),
            mechanism: request.mechanism.clone(),
            cursor: request.cursor.clone(),
            gap_id: request.gap.as_ref().map(|gap| gap.gap_id.clone()),
            seq: 7,
        }
    }

    fn state(request: &SourceHandoff) -> Value {
        let mut payload = serde_json::to_value(request).unwrap();
        payload["families"] = json!(["message", "turn"]);
        json!({
            "agent_id":"a1", "generation":1, "source":"stream", "state":"open", "health":"stale",
            "last_store_seq":1,
            "owners":[
                {"family":"session","source":"stream","mechanism":"native_stream"},
                {"family":"message","source":"transcript","mechanism":"fake_recovery"},
                {"family":"turn","source":"transcript","mechanism":"fake_recovery"}
            ],
            "cursor":request.cursor,
            "gap_id":request.gap.as_ref().map(|gap| &gap.gap_id),
            "gap_from":request.gap.as_ref().and_then(|gap| gap.from.as_ref()),
            "gap_to":request.gap.as_ref().and_then(|gap| gap.to.as_ref()),
            "gap_seq":request.gap.as_ref().map(|_| 6),
            "last_handoff_digest":canonical_json_sha256(&payload).unwrap(),
            "last_handoff":ack(request)
        })
    }

    fn assert_unknown<T: std::fmt::Debug>(result: Result<T>) {
        let error = result.expect_err("unproven handoff");
        assert_eq!(error.code, "unknown");
        assert_eq!(
            error.message,
            "source handoff acknowledgement or current ownership is unproven"
        );
        assert!(error.diagnostics.is_empty());
    }

    #[test]
    fn prepared_reason_matches_stored_full_digest_but_raw_reason_stays_unproven() {
        let policy = SecretPolicy::new(["dummy-private-credential"]);
        let mut raw = request(true);
        raw.gap.as_mut().unwrap().reason = "lost dummy-private-credential transport".to_owned();
        let mut prepared = raw.clone();
        prepared.prepare(&policy).unwrap();
        assert_eq!(
            prepared.gap.as_ref().unwrap().reason,
            "lost [REDACTED] transport"
        );
        prepared.validate().unwrap();
        let submitted = serde_json::to_value(&prepared).unwrap();
        let stored_request: SourceHandoff = serde_json::from_value(submitted.clone()).unwrap();
        let stored = state(&stored_request);
        assert_eq!(prepared.ack_from_state(&stored).unwrap(), ack(&prepared));
        assert_unknown(raw.ack_from_state(&stored));
        assert_eq!(serde_json::to_value(&prepared).unwrap(), submitted);
    }

    #[test]
    fn immutable_secret_rejection_leaves_every_request_field_unchanged() {
        let policy = SecretPolicy::new(["dummy-private-credential"]);
        for pointer in [
            "/agent_id",
            "/source",
            "/successor",
            "/mechanism",
            "/families/0",
            "/cursor",
            "/gap/gap_id",
            "/gap/from",
            "/gap/to",
        ] {
            let mut original = request(true);
            original.gap.as_mut().unwrap().reason =
                "redact dummy-private-credential only after checks".to_owned();
            let mut wire = serde_json::to_value(original).unwrap();
            *wire.pointer_mut(pointer).unwrap() = json!("prefix-dummy-private-credential-suffix");
            let mut request: SourceHandoff = serde_json::from_value(wire).unwrap();
            let before = request.clone();
            let error = request.prepare(&policy).unwrap_err();
            assert_eq!(error.code, "secret_in_payload");
            assert_eq!(
                error.message,
                "source.handoff contains a known secret in immutable metadata"
            );
            assert!(error.diagnostics.is_empty());
            assert_eq!(request, before);
        }
    }

    #[test]
    fn repeated_preparation_preserves_markers_and_the_frozen_request_digest() {
        let policy = SecretPolicy::new(["dummy-private-credential", "REDACTED"]);
        let mut request = request(true);
        request.gap.as_mut().unwrap().reason =
            "dummy-private-credential REDACTED [REDACTED]".to_owned();
        request.prepare(&policy).unwrap();
        assert_eq!(
            request.gap.as_ref().unwrap().reason,
            "[REDACTED] [REDACTED] [REDACTED]"
        );
        let before = request.clone();
        let stored = state(&request);
        request.prepare(&policy).unwrap();
        assert_eq!(request, before);
        assert_eq!(request.ack_from_state(&stored).unwrap(), ack(&request));
    }

    #[test]
    fn an_additional_host_only_secret_does_not_enable_readback_repair() {
        let caller_policy = SecretPolicy::new(["dummy-caller-credential"]);
        let host_policy =
            SecretPolicy::new(["dummy-caller-credential", "dummy-host-only-credential"]);
        let mut caller_request = request(true);
        caller_request.gap.as_mut().unwrap().reason =
            "dummy-caller-credential dummy-host-only-credential".to_owned();
        caller_request.prepare(&caller_policy).unwrap();
        let mut stored_request = caller_request.clone();
        stored_request.prepare(&host_policy).unwrap();
        let stored = state(&stored_request);
        assert_unknown(caller_request.ack_from_state(&stored));
        assert_eq!(
            stored_request.ack_from_state(&stored).unwrap(),
            ack(&stored_request)
        );
    }

    #[test]
    fn handoff_wire_is_object_only_and_preserves_nullable_boundaries() {
        let request = request(true);
        let wire = serde_json::to_value(&request).unwrap();
        assert_eq!(wire["cursor"], Value::Null);
        assert_eq!(wire["gap"]["from"], Value::Null);
        assert_eq!(
            serde_json::from_value::<SourceHandoff>(wire.clone()).unwrap(),
            request
        );
        let mut extra = wire.clone();
        extra["handoff_id"] = json!("not-supported");
        assert!(serde_json::from_value::<SourceHandoff>(extra).is_err());
        let mut extra = wire;
        extra["gap"]["extra"] = json!(true);
        assert!(serde_json::from_value::<SourceHandoff>(extra).is_err());
        assert!(serde_json::from_value::<SourceHandoff>(json!(["a1",1,"stream",["turn"],"transcript","fake_recovery",null,{"gap_id":"g","reason":"gap","from":null,"to":null}])).is_err());
        let mut nested = serde_json::to_value(&request).unwrap();
        nested["gap"] = json!(["g", "gap", null, null]);
        assert!(serde_json::from_value::<SourceHandoff>(nested).is_err());
        assert!(
            serde_json::from_value::<SourceHandoffAck>(json!([
                "a1",
                1,
                "stream",
                ["turn"],
                "transcript",
                "fake_recovery",
                null,
                "g",
                7
            ]))
            .is_err()
        );
    }

    #[test]
    fn request_rejects_ambiguous_boundaries_and_non_native_or_duplicate_families() {
        let original = request(false);
        for families in [vec![], vec!["turn", "turn"]] {
            let mut request = original.clone();
            request.families = families.into_iter().map(str::to_owned).collect();
            assert_eq!(request.validate().unwrap_err().code, "invalid");
        }
        let mut request = original.clone();
        request.cursor = None;
        assert!(request.validate().is_err());
        request.gap = super::tests::request(true).gap;
        request.cursor = original.cursor.clone();
        assert!(request.validate().is_err());
        request = original.clone();
        request.successor = request.source.clone();
        assert!(request.validate().is_err());
        request = original;
        request.generation = 0;
        request.validate().unwrap();
        request.agent_id = "a01".to_owned();
        assert!(request.validate().is_err());
    }

    #[test]
    fn family_error_classes_survive_wire_decode_and_ack_failures_stay_unknown() {
        for (family, code) in [
            ("child", "unsupported"),
            ("goal", "unsupported"),
            ("usage", "unsupported"),
            ("process", "not_allowed"),
            ("git", "not_allowed"),
            ("file", "not_allowed"),
            ("workspace_process", "not_allowed"),
            ("agent", "not_allowed"),
            ("command", "not_allowed"),
        ] {
            let mut request = request(false);
            request.families = vec![family.to_owned()];
            let wire = serde_json::to_value(&request).unwrap();
            let decoded: SourceHandoff = serde_json::from_value(wire).unwrap();
            assert_eq!(decoded.validate().unwrap_err().code, code);
            assert_unknown(decoded.validate_ack(&ack(&decoded)));
            assert_unknown(decoded.ack_from_state(&Value::Null));
        }
    }

    #[test]
    fn ack_compares_family_sets_without_mutation_and_binds_every_field() {
        let request = request(false);
        let original = ack(&request);
        request.validate_ack(&original).unwrap();
        let mut reordered = original.clone();
        reordered.families.reverse();
        request.validate_ack(&reordered).unwrap();
        for field in [
            "agent_id",
            "generation",
            "source",
            "families",
            "successor",
            "mechanism",
            "cursor",
            "gap_id",
            "seq",
        ] {
            let mut wire = serde_json::to_value(&original).unwrap();
            wire[field] = match field {
                "generation" => json!(2),
                "seq" => json!(0),
                "families" => json!(["turn", "turn"]),
                _ => json!("different"),
            };
            let changed: SourceHandoffAck = serde_json::from_value(wire).unwrap();
            assert_unknown(request.validate_ack(&changed));
        }
        assert_eq!(request.families, ["turn", "message"]);
        assert_eq!(original.families, ["message", "turn"]);
    }

    #[test]
    fn readback_requires_retained_digest_current_owners_and_exact_boundary() {
        for gap in [false, true] {
            let request = request(gap);
            let state = state(&request);
            let before = state.clone();
            assert_eq!(request.ack_from_state(&state).unwrap(), ack(&request));
            assert_eq!(state, before);
            for field in [
                "agent_id",
                "generation",
                "source",
                "last_handoff_digest",
                "last_handoff",
            ] {
                let mut changed = state.clone();
                changed[field] = match field {
                    "generation" => json!(2),
                    "last_handoff_digest" => json!(vec![0_u8; 32]),
                    "last_handoff" => Value::Null,
                    _ => json!("different"),
                };
                assert_unknown(request.ack_from_state(&changed));
            }
        }
    }

    #[test]
    fn readback_rejects_mixed_missing_duplicate_or_wrong_mechanism_owners() {
        let request = request(true);
        for change in ["mixed", "missing", "duplicate", "mechanism"] {
            let mut state = state(&request);
            match change {
                "mixed" => state["owners"][1]["source"] = json!("stream"),
                "missing" => {
                    state["owners"].as_array_mut().unwrap().remove(1);
                }
                "duplicate" => {
                    let duplicate = state["owners"][1].clone();
                    state["owners"].as_array_mut().unwrap().push(duplicate);
                }
                _ => state["owners"][1]["mechanism"] = json!("other"),
            }
            assert_unknown(request.ack_from_state(&state));
        }
    }

    #[test]
    fn retained_ack_does_not_prove_an_advanced_cursor_or_gap() {
        let cursor_request = request(false);
        let mut advanced = state(&cursor_request);
        advanced["cursor"] = json!("edge:2");
        assert_unknown(cursor_request.ack_from_state(&advanced));
        let gap_request = request(true);
        for field in ["gap_id", "gap_from", "gap_to", "gap_seq"] {
            let mut advanced = state(&gap_request);
            advanced[field] = if field == "gap_seq" {
                json!(0)
            } else {
                json!("advanced")
            };
            assert_unknown(gap_request.ack_from_state(&advanced));
        }
        for seq in [7, 8] {
            let mut advanced = state(&gap_request);
            advanced["gap_seq"] = json!(seq);
            assert_unknown(gap_request.ack_from_state(&advanced));
        }
        let mut different_reason = gap_request.clone();
        different_reason.gap.as_mut().unwrap().reason = "different gap reason".to_owned();
        assert_unknown(different_reason.ack_from_state(&state(&gap_request)));
    }

    #[test]
    fn malformed_state_and_nested_ack_owner_shapes_stay_unknown() {
        let request = request(true);
        assert_unknown(request.ack_from_state(&Value::Null));
        assert_unknown(request.ack_from_state(&json!([])));
        for field in ["owners", "last_handoff", "last_handoff_digest", "gap_from"] {
            let mut malformed = state(&request);
            malformed.as_object_mut().unwrap().remove(field);
            assert_unknown(request.ack_from_state(&malformed));
        }
        let mut malformed = state(&request);
        malformed["owners"] = json!([["message", "transcript", "fake_recovery"]]);
        assert_unknown(request.ack_from_state(&malformed));
        let mut malformed = state(&request);
        malformed["last_handoff"] = json!([
            "a1",
            1,
            "stream",
            ["message", "turn"],
            "transcript",
            "fake_recovery",
            null,
            "gap:1",
            7
        ]);
        assert_unknown(request.ack_from_state(&malformed));
    }
}
