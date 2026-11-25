//! Exact, bounded startup evidence exchanged by the update operator.

use crate::{Error, FailureOrigin, Result};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const UPDATE_HEALTH_MAX_SUBJECTS: usize = 128;

#[derive(JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UpdateProbeKind {
    Resume,
    Launch,
}

#[derive(JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateStartSubject {
    pub agent: String,
    pub generation: i64,
    pub harness: String,
    pub kind: UpdateProbeKind,
    pub binding: String,
}

#[derive(JsonSchema, Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum UpdateHealthRequest {
    Inventory,
    Boot,
    AdapterProbe { subject: UpdateStartSubject },
}

impl UpdateHealthRequest {
    pub fn validate(&self) -> Result<()> {
        if let Self::AdapterProbe { subject } = self {
            crate::parse_agent_id(&subject.agent)?;
            if subject.generation < 1
                || !super::is_supported_harness(&subject.harness)
                || subject.binding.len() != 64
                || !subject
                    .binding
                    .bytes()
                    .all(|ch| matches!(ch, b'0'..=b'9' | b'a'..=b'f'))
            {
                return Err(Error::new("invalid", "invalid update readiness subject"));
            }
        }
        Ok(())
    }
}

#[derive(JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UpdateStartState {
    Ready,
    NotDispatched,
    Refused,
    Unknown,
}

#[derive(JsonSchema, Clone, Debug, Serialize, Deserialize)]
pub struct UpdateStartEvidence {
    pub subject: UpdateStartSubject,
    pub state: UpdateStartState,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub origin: Option<FailureOrigin>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub command_id: Option<String>,
}

impl UpdateStartEvidence {
    pub fn deterministic_swarm_refusal(&self) -> bool {
        self.state == UpdateStartState::Refused
            && self.origin == Some(FailureOrigin::Swarm)
            && matches!(
                self.code.as_deref(),
                Some("profile_error" | "invalid_settings")
            )
    }
}

#[derive(JsonSchema, Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum UpdateHealthResponse {
    Inventory {
        live: Vec<UpdateStartSubject>,
        samples: Vec<UpdateStartSubject>,
        truncated: bool,
    },
    Boot {
        finished: bool,
        complete: bool,
        requested: usize,
        attempts: Vec<UpdateStartEvidence>,
    },
    AdapterProbe {
        evidence: UpdateStartEvidence,
    },
}
