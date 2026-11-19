//! Codex native-child registration (producer half).
//!
//! The existing producer credential authenticates this call; the child receives
//! no credential and no control verbs. Wire shape follows the settled backend
//! contract: `protocol::ChildParams { credential, registration }` via RPC method
//! `child`, returning `ChildRegistrationAck` directly. Until that type lands in
//! `eliot-plugin`, the parameter envelope is built locally from the same fields;
//! the ACK is validated with `ChildRegistration::validate_ack` before any fact
//! is published under the child identity.

use eliot_core::{
    CODEX_NATIVE_CHILD_FAMILIES, ChildRegistration, ChildRegistrationAck, Error, Result,
};
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
    registration.validate_ack(&ack, "codex", &CODEX_NATIVE_CHILD_FAMILIES)?;
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
