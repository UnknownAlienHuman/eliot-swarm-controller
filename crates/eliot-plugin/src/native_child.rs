use eliot_core::{ChildRegistration, ChildRegistrationAck, Error, Result};
use tokio::time::Instant;

use crate::{PeerClient, protocol::ChildParams, session::EVENT_DEADLINE};

/// A validated, observe-only native child identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChildBinding {
    pub registration: ChildRegistration,
    pub agent_id: String,
    pub agent_generation: i64,
    pub native_ref: String,
}

pub async fn register_child(
    host: &PeerClient,
    credential: &str,
    registration: ChildRegistration,
    harness: &str,
    families: &[&str],
) -> Result<ChildRegistrationAck> {
    registration.validate()?;
    let params = serde_json::to_value(ChildParams {
        credential: credential.to_owned(),
        registration: registration.clone(),
    })
    .map_err(|_| Error::new("internal", "child params could not be encoded"))?;
    let value = host
        .call("child", params, Instant::now() + EVENT_DEADLINE)
        .await?;
    let ack: ChildRegistrationAck = serde_json::from_value(value)
        .map_err(|_| Error::new("source_integrity", "child ACK was not well formed"))?;
    registration.validate_ack(&ack, harness, families)?;
    Ok(ack)
}

/// Only these replies prove the registration did not take effect. When the
/// acknowledgement is unresolved, retain the original registration identity.
pub fn is_admission_rejection(error: &Error) -> bool {
    matches!(
        error.code.as_str(),
        "invalid" | "unsupported" | "stale_generation" | "not_allowed" | "id_conflict"
    )
}

pub fn binding_from_ack(
    registration: ChildRegistration,
    ack: &ChildRegistrationAck,
) -> ChildBinding {
    ChildBinding {
        registration,
        agent_id: ack.agent.id.clone(),
        agent_generation: ack.agent.generation,
        native_ref: ack.agent.native_ref.clone().unwrap_or_default(),
    }
}

#[derive(Clone, Copy)]
pub enum ChildFact {
    Declared,
    Started,
    Completed {
        status: &'static str,
        native: &'static str,
    },
    UnrecognizedTerminal,
}

pub enum ChildAction {
    Ignore,
    Register,
    Start,
    Complete {
        status: &'static str,
        native: &'static str,
    },
    ConflictingTerminal,
    UnknownTerminal,
}

#[derive(Clone, Debug, Default)]
pub struct ChildLifecycle {
    pub active_turn: Option<String>,
    pub terminal: Option<String>,
    pub unknown: bool,
}

impl ChildLifecycle {
    pub fn started(&mut self, turn_id: &str) {
        self.active_turn = Some(turn_id.to_owned());
        self.terminal = None;
    }

    pub fn completed(&mut self, status: Option<&str>) {
        self.active_turn = None;
        self.terminal = status.map(str::to_owned);
    }

    /// Observation ended while the child was still open. Preserve any
    /// committed terminal and let callers record a gap for the lost source.
    pub fn observation_lost(&mut self) -> bool {
        let open = !self.unknown && self.terminal.is_none();
        self.unknown |= open;
        open
    }

    pub fn plan(&self, registered: bool, fact: ChildFact) -> ChildAction {
        if self.unknown {
            return ChildAction::Ignore;
        }
        if let Some(terminal) = self.terminal.as_deref() {
            return match fact {
                ChildFact::Completed { status, .. } if status != terminal => {
                    ChildAction::ConflictingTerminal
                }
                _ => ChildAction::Ignore,
            };
        }
        if !registered {
            return ChildAction::Register;
        }
        match fact {
            ChildFact::Started if self.active_turn.is_none() => ChildAction::Start,
            ChildFact::Completed { status, native } => ChildAction::Complete { status, native },
            ChildFact::UnrecognizedTerminal => ChildAction::UnknownTerminal,
            ChildFact::Declared | ChildFact::Started => ChildAction::Ignore,
        }
    }
}
