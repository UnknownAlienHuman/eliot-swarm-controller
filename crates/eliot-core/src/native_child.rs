//! Private `child` registration. The authenticated producer is supplied by the
//! host, never by this payload; a native child receives no control credential.

use crate::{Agent, AgentControl, AgentOrigin, AgentState, Error, Result, parse_agent_id};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::BTreeMap;

pub const NATIVE_CHILD_FAMILIES: [&str; 3] = ["session", "turn", "message"];

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "Self", deny_unknown_fields)]
pub struct ChildRegistration {
    pub registration_id: String,
    pub parent_id: String,
    pub parent_generation: i64,
    pub parent_native_ref: String,
    pub native_scope: String,
    pub native_ref: String,
}

/// The immutable registration result, not a child completion or control grant.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(remote = "Self", deny_unknown_fields)]
pub struct ChildRegistrationAck {
    pub registration_id: String,
    pub agent: Agent,
    pub source_plan: BTreeMap<String, String>,
}

impl Serialize for ChildRegistration {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        Self::serialize(self, serializer)
    }
}

impl<'de> Deserialize<'de> for ChildRegistration {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let map = Map::<String, Value>::deserialize(deserializer)?;
        Self::deserialize(Value::Object(map)).map_err(serde::de::Error::custom)
    }
}

impl Serialize for ChildRegistrationAck {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        Self::serialize(self, serializer)
    }
}

impl<'de> Deserialize<'de> for ChildRegistrationAck {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let map = Map::<String, Value>::deserialize(deserializer)?;
        Self::deserialize(Value::Object(map)).map_err(serde::de::Error::custom)
    }
}

impl ChildRegistration {
    pub fn validate(&self) -> Result<()> {
        parse_agent_id(&self.parent_id)?;
        if self.parent_generation <= 0
            || [
                self.registration_id.as_str(),
                self.parent_native_ref.as_str(),
                self.native_scope.as_str(),
                self.native_ref.as_str(),
            ]
            .iter()
            .any(|value| value.trim().is_empty() || value.chars().any(char::is_control))
            || self.parent_native_ref == self.native_ref
        {
            return Err(Error::new("invalid", "invalid native child registration"));
        }
        Ok(())
    }

    pub fn validate_ack(&self, ack: &ChildRegistrationAck) -> Result<()> {
        self.validate()?;
        let agent = &ack.agent;
        parse_agent_id(&agent.id)?;
        parse_agent_id(&agent.root)?;
        if ack.registration_id != self.registration_id
            || agent.id == self.parent_id
            || agent.parent.as_deref() != Some(self.parent_id.as_str())
            || agent.generation != 1
            || agent.depth <= 0
            || agent.origin != AgentOrigin::Native
            || !agent.observe_only
            || agent.own_identity
            || agent.control != AgentControl::Slf
            || agent.state != AgentState::Unknown
            || agent.native_scope.as_deref() != Some(self.native_scope.as_str())
            || agent.native_ref.as_deref() != Some(self.native_ref.as_str())
            || ack.source_plan.len() != NATIVE_CHILD_FAMILIES.len()
            || NATIVE_CHILD_FAMILIES
                .iter()
                .any(|family| ack.source_plan.get(*family).map(String::as_str) != Some("stream"))
        {
            return Err(Error::new(
                "source_integrity",
                "child acknowledgement does not match its observation-only registration",
            ));
        }
        Ok(())
    }
}
