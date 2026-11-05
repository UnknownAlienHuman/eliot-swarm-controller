//! Private `child` registration. The authenticated producer is supplied by the
//! host, never by this payload; a native child receives no control credential.

use crate::{Agent, AgentControl, AgentOrigin, AgentState, Error, Result, parse_agent_id};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::BTreeMap;

pub const CODEX_NATIVE_CHILD_FAMILIES: [&str; 3] = ["session", "turn", "message"];
pub const MUSE_NATIVE_CHILD_FAMILIES: [&str; 1] = ["turn"];
pub const NATIVE_CHILD_FAMILIES: [&str; 3] = CODEX_NATIVE_CHILD_FAMILIES;

/// Observation stream families currently implemented for each supported native-child harness.
pub fn native_child_families_for_harness(harness: &str) -> Option<&'static [&'static str]> {
    match harness {
        "codex" => Some(&CODEX_NATIVE_CHILD_FAMILIES),
        "muse" => Some(&MUSE_NATIVE_CHILD_FAMILIES),
        _ => None,
    }
}

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

    /// Validate the registration acknowledgement against caller-specified
    /// expected harness and coverage. No silent default harness fallback is allowed.
    pub fn validate_ack(
        &self,
        ack: &ChildRegistrationAck,
        expected_harness: &str,
        expected_families: &[&str],
    ) -> Result<()> {
        self.validate()?;
        let allowed = native_child_families_for_harness(expected_harness).ok_or_else(|| {
            Error::new(
                "source_integrity",
                "child acknowledgement named an unsupported harness",
            )
        })?;
        if expected_families != allowed {
            return Err(Error::new(
                "source_integrity",
                "expected coverage does not match the implemented harness coverage",
            ));
        }
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
            || agent.harness != expected_harness
            || agent.native_scope.as_deref() != Some(self.native_scope.as_str())
            || agent.native_ref.as_deref() != Some(self.native_ref.as_str())
            || ack.source_plan.len() != expected_families.len()
            || expected_families
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

#[cfg(test)]
mod tests {
    use super::*;

    fn test_agent(id: &str, harness: &str, reg: &ChildRegistration) -> Agent {
        let mut agent = Agent::new(
            id,
            "proj".to_owned(),
            harness.to_owned(),
            "prof".to_owned(),
            1,
        );
        agent.generation = 1;
        agent.origin = AgentOrigin::Native;
        agent.observe_only = true;
        agent.parent = Some(reg.parent_id.clone());
        agent.root = reg.parent_id.clone();
        agent.depth = 1;
        agent.control = AgentControl::Slf;
        agent.state = AgentState::Unknown;
        agent.native_scope = Some(reg.native_scope.clone());
        agent.native_ref = Some(reg.native_ref.clone());
        agent
    }

    #[test]
    fn validate_ack_checks_expected_harness_and_coverage_for_codex() {
        let reg = ChildRegistration {
            registration_id: "codex:spawn:1".to_owned(),
            parent_id: "a1".to_owned(),
            parent_generation: 1,
            parent_native_ref: "p-ref".to_owned(),
            native_scope: "codex:scope".to_owned(),
            native_ref: "c-ref".to_owned(),
        };
        let ack = ChildRegistrationAck {
            registration_id: reg.registration_id.clone(),
            agent: test_agent("a2", "codex", &reg),
            source_plan: BTreeMap::from([
                ("session".to_owned(), "stream".to_owned()),
                ("turn".to_owned(), "stream".to_owned()),
                ("message".to_owned(), "stream".to_owned()),
            ]),
        };
        reg.validate_ack(&ack, "codex", &CODEX_NATIVE_CHILD_FAMILIES)
            .unwrap();

        // Wrong harness requested:
        assert_eq!(
            reg.validate_ack(&ack, "muse", &MUSE_NATIVE_CHILD_FAMILIES)
                .unwrap_err()
                .code,
            "source_integrity"
        );

        // Producer widening attempt:
        assert_eq!(
            reg.validate_ack(&ack, "codex", &["session", "turn", "message", "extra"])
                .unwrap_err()
                .code,
            "source_integrity"
        );
    }

    #[test]
    fn validate_ack_checks_expected_harness_and_coverage_for_muse() {
        let reg = ChildRegistration {
            registration_id: "muse:spawn:step-1".to_owned(),
            parent_id: "a1".to_owned(),
            parent_generation: 1,
            parent_native_ref: "sess-1".to_owned(),
            native_scope: "muse:scope".to_owned(),
            native_ref: "muse:child:step-1".to_owned(),
        };
        let ack = ChildRegistrationAck {
            registration_id: reg.registration_id.clone(),
            agent: test_agent("a2", "muse", &reg),
            source_plan: BTreeMap::from([("turn".to_owned(), "stream".to_owned())]),
        };
        reg.validate_ack(&ack, "muse", &MUSE_NATIVE_CHILD_FAMILIES)
            .unwrap();

        // Codex families rejected for Muse:
        assert_eq!(
            reg.validate_ack(&ack, "codex", &CODEX_NATIVE_CHILD_FAMILIES)
                .unwrap_err()
                .code,
            "source_integrity"
        );

        // Widening coverage for Muse rejected:
        assert_eq!(
            reg.validate_ack(&ack, "muse", &["turn", "session"])
                .unwrap_err()
                .code,
            "source_integrity"
        );

        // Unknown harness rejected:
        assert_eq!(
            reg.validate_ack(&ack, "unknown", &["turn"])
                .unwrap_err()
                .code,
            "source_integrity"
        );
    }
}
