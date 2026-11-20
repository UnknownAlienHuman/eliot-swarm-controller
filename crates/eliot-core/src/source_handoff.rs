//! Native-family handoff wire data and pure acknowledgement/readback checks.
//! Store remains authoritative for the transaction and successor publication.

use crate::{Error, Result, SecretPolicy, parse_agent_id};
use serde::{Deserialize, Serialize};
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

fn unproven() -> Error {
    Error::new(
        "unknown",
        "source handoff acknowledgement or current ownership is unproven",
    )
}
