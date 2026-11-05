//! Codex native-child registration (producer half).
//!
//! The existing producer credential authenticates this call; the child receives
//! no credential and no control verbs. Wire shape follows the settled backend
//! contract: `protocol::ChildParams { credential, registration }` via RPC method
//! `child`, returning `ChildRegistrationAck` directly. Until that type lands in
//! `eliot-plugin`, the parameter envelope is built locally from the same fields;
//! the ACK is validated with `ChildRegistration::validate_ack` before any fact
//! is published under the child identity.

use eliot_core::{ChildRegistration, ChildRegistrationAck, Error, Result};
use eliot_plugin::PeerClient;
use tokio::time::Instant;

/// Bounded registration attempt: a lost or conflicting ACK is explicit
/// unknown, never a replay of native effects.
const CHILD_DEADLINE: std::time::Duration = std::time::Duration::from_secs(30);

/// The single-parent binding the producer needs before publishing child facts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ChildBinding {
    pub registration: ChildRegistration,
    pub agent_id: String,
    pub agent_generation: i64,
    pub native_ref: String,
}

/// Registration identity for one native child thread.
///
/// The spawn item is stable for one native spawn call, so an idempotent retry
/// returns the same registration id instead of a second child.
pub(crate) fn registration_id(spawn_item_id: &str, child_thread: &str) -> String {
    format!("codex:spawn:{spawn_item_id}:{child_thread}")
}

/// Build a registration for a confirmed child of `parent_native_ref`.
///
/// `parent_id` / `parent_generation` are the immediate parent Agent, never the
/// root when the parent itself is a child.
pub(crate) fn build_registration(
    spawn_item_id: &str,
    parent_id: &str,
    parent_generation: i64,
    parent_native_ref: &str,
    native_scope: &str,
    child_thread: &str,
) -> ChildRegistration {
    ChildRegistration {
        registration_id: registration_id(spawn_item_id, child_thread),
        parent_id: parent_id.to_owned(),
        parent_generation,
        parent_native_ref: parent_native_ref.to_owned(),
        native_scope: native_scope.to_owned(),
        native_ref: child_thread.to_owned(),
    }
}

/// Register one native child through the host. Success commits nothing by
/// itself; the caller publishes child facts only after this ACK validates.
///
/// The host authenticates the producer from the private connection. No child
/// credential is sent and no child control verb is exposed.
pub(crate) async fn register_child(
    host: &PeerClient,
    credential: &str,
    registration: ChildRegistration,
) -> Result<ChildRegistrationAck> {
    registration.validate()?;
    let params = serde_json::json!({
        "credential": credential,
        "registration": registration,
    });
    let value = host
        .call("child", params, Instant::now() + CHILD_DEADLINE)
        .await?;
    let ack: ChildRegistrationAck = serde_json::from_value(value)
        .map_err(|_| Error::new("source_integrity", "child ACK was not well formed"))?;
    registration.validate_ack(&ack)?;
    Ok(ack)
}

/// Whether an RPC error proves the registration was never admitted. Only
/// deterministic rejections are retryable; anything else stays unknown.
pub(crate) fn is_admission_rejection(error: &Error) -> bool {
    matches!(
        error.code.as_str(),
        "invalid" | "unsupported" | "stale_generation" | "not_allowed" | "id_conflict"
    )
}

/// Build the child binding from a validated ACK.
pub(crate) fn binding_from_ack(
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

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use eliot_core::{Agent, AgentControl, AgentOrigin, AgentState};
    use serde_json::{Value, json};
    use std::collections::BTreeMap;

    fn registration() -> ChildRegistration {
        build_registration(
            "call-1",
            "a1",
            1,
            "parent-thread",
            "codex:test-scope",
            "child-thread",
        )
    }

    pub(crate) fn test_ack(reg: &ChildRegistration) -> ChildRegistrationAck {
        let mut agent = Agent::new(
            "a2",
            "proj".to_owned(),
            "codex".to_owned(),
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
        ChildRegistrationAck {
            registration_id: reg.registration_id.clone(),
            agent,
            source_plan: BTreeMap::from([
                ("message".to_owned(), "stream".to_owned()),
                ("session".to_owned(), "stream".to_owned()),
                ("turn".to_owned(), "stream".to_owned()),
            ]),
        }
    }

    #[test]
    fn ack_validation_accepts_only_the_exact_child_plan() {
        let reg = registration();
        reg.validate_ack(&test_ack(&reg)).expect("exact plan");
        let mut wrong = test_ack(&reg);
        wrong
            .source_plan
            .insert("tool".to_owned(), "stream".to_owned());
        assert!(reg.validate_ack(&wrong).is_err());
        let mut wrong = test_ack(&reg);
        wrong.agent.native_ref = Some("other".to_owned());
        assert!(reg.validate_ack(&wrong).is_err());
        let mut wrong = test_ack(&reg);
        wrong.agent.control = AgentControl::Tree;
        assert!(reg.validate_ack(&wrong).is_err());
    }

    #[test]
    fn child_params_envelope_carries_credential_and_registration_only() {
        let reg = registration();
        let params = json!({"credential": "producer-cred", "registration": reg});
        assert_eq!(params["credential"], "producer-cred");
        assert_eq!(params["registration"]["native_ref"], "child-thread");
        let decoded: ChildRegistration =
            serde_json::from_value(params["registration"].clone()).expect("decode");
        assert_eq!(decoded, reg);
        let value: Value = serde_json::to_value(test_ack(&reg)).expect("ack encode");
        let round: ChildRegistrationAck = serde_json::from_value(value).expect("ack decode");
        reg.validate_ack(&round).expect("round trip");
    }

    #[test]
    fn only_deterministic_rejections_are_retryable() {
        for code in [
            "invalid",
            "unsupported",
            "stale_generation",
            "not_allowed",
            "id_conflict",
        ] {
            assert!(is_admission_rejection(&Error::new(code, "x")), "{code}");
        }
        for code in ["unknown", "source_integrity", "busy", "transport_closed"] {
            assert!(!is_admission_rejection(&Error::new(code, "x")), "{code}");
        }
    }

    #[test]
    fn registration_id_is_deterministic_for_idempotent_retry() {
        assert_eq!(
            registration_id("call-1", "child"),
            "codex:spawn:call-1:child"
        );
        assert_eq!(
            registration_id("call-1", "child"),
            registration_id("call-1", "child")
        );
    }

    #[test]
    fn binding_carries_validated_child_identity() {
        let reg = registration();
        let ack = test_ack(&reg);
        let binding = binding_from_ack(reg.clone(), &ack);
        assert_eq!(binding.agent_id, ack.agent.id);
        assert_eq!(binding.agent_generation, 1);
        assert_eq!(binding.native_ref, "child-thread");
        assert_eq!(binding.registration.registration_id, reg.registration_id);
    }
}
