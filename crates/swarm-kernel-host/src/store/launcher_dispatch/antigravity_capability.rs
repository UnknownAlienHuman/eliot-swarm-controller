use crate::{
    error::{Error, Result},
    model,
};
use serde_json::{Value, json};
use swarm_contracts::module_contract::{
    runtime_command_schema, task_dispatch_admission_schema, task_dispatch_context_schema,
    task_prompt_schema,
};

use super::LaunchParent;

pub(super) const ANTIGRAVITY_ARTIFACT_ID: &str = "eliot-antigravity.rust-headless.1";
const ANTIGRAVITY_ARTIFACT_VERSION: &str = "6";
const ANTIGRAVITY_CAPABILITY_KIND: &str = "antigravity_current_dispatch";

pub(super) fn unavailable() -> Error {
    Error::new(
        "LAUNCH_DISPATCH_CAPABILITY_UNAVAILABLE",
        "no current trusted native dispatch capability is available for this route",
    )
}

fn stale_session(predicate: &'static str) -> Error {
    Error::new(
        "STALE_ANTIGRAVITY_SESSION",
        format!("current Antigravity session failed predicate: {predicate}"),
    )
}

fn bounded_identity(value: &str, maximum: usize) -> bool {
    !value.trim().is_empty() && value.len() <= maximum && !value.chars().any(char::is_control)
}

fn required_text<'a>(
    value: &'a Value,
    field: &str,
    predicate: &'static str,
    maximum: usize,
) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|text| bounded_identity(text, maximum))
        .ok_or_else(|| stale_session(predicate))
}

pub(super) fn validate_retained_descriptor(
    identity: &super::super::module_handshake::RetainedModuleIdentity,
) -> Result<()> {
    let has_capability = |name: &str| {
        identity
            .capabilities
            .iter()
            .any(|capability| capability.as_str() == name)
    };
    if identity.module_id.as_str() != "antigravity"
        || identity.artifact.artifact_id.as_str() != ANTIGRAVITY_ARTIFACT_ID
        || identity.artifact.version.as_str() != ANTIGRAVITY_ARTIFACT_VERSION
        || identity.protocol.major != 1
        || identity.protocol.minor != 0
        || !has_capability("agent.refresh")
        || !has_capability("task.dispatch")
        || !identity.command_schemas.contains(&runtime_command_schema())
        || !identity.command_schemas.contains(&task_prompt_schema())
        || !identity
            .command_schemas
            .contains(&task_dispatch_context_schema())
        || !identity
            .event_schemas
            .contains(&task_dispatch_admission_schema())
    {
        return Err(unavailable());
    }
    Ok(())
}

/// Validate the current Store-owned binding and the exact successful Open
/// evidence. Dispatch no longer depends on a second `agent.refresh` Operation
/// or a proof packet that repeats the same assignment, lease, boot and session
/// facts. The adapter still performs its own command-time binding/root checks.
pub(super) fn validate_current_session(binding: &Value) -> Result<()> {
    if binding["state"] != "ready" || !binding["released_at_ms"].is_null() {
        return Err(stale_session("binding.ready_and_unreleased"));
    }
    let root = required_text(
        binding,
        "native_root_id",
        "binding.native_root_id_present",
        512,
    )?;
    let _scope = required_text(
        binding,
        "native_scope_key",
        "binding.native_scope_key_present",
        32 * 1024,
    )?;
    let observation = binding
        .get("observation")
        .filter(|value| value.is_object())
        .ok_or_else(|| stale_session("binding.observation_present"))?;
    let boot = required_text(
        observation,
        "bridge_boot_id",
        "observation.bridge_boot_id_present",
        256,
    )?;
    if observation["connection"] != "connected" || observation["recovery_required"] == true {
        return Err(stale_session("observation.connected_without_recovery"));
    }
    let opening = observation
        .get("opening_evidence")
        .filter(|value| value.is_object())
        .ok_or_else(|| stale_session("observation.opening_evidence_present"))?;
    if opening["completion_condition"] != "native_session_initialized"
        || opening["bridge_boot_id"] != boot
        || opening["native_conversation_id"] != root
    {
        return Err(stale_session("opening.identity_matches_current_binding"));
    }
    let describe = opening
        .get("describe")
        .filter(|value| value.is_object())
        .ok_or_else(|| stale_session("opening.describe_present"))?;

    match binding["route"]["native_options"].get("modelId") {
        Some(Value::String(requested)) if bounded_identity(requested, 256) => {
            if describe["model_requested"] != requested.as_str()
                || describe["model_observed"] != requested.as_str()
                || describe["model_verification"] != "verified"
            {
                return Err(stale_session("opening.explicit_model_verified"));
            }
        }
        None => {
            let observed_valid = describe["model_observed"]
                .as_str()
                .is_none_or(|observed| bounded_identity(observed, 256));
            if !describe["model_requested"].is_null()
                || !observed_valid
                || describe["model_verification"] != "native_default"
            {
                return Err(stale_session("opening.native_default_model"));
            }
        }
        _ => return Err(stale_session("route.model_id_valid_or_absent")),
    }
    Ok(())
}

pub(super) fn build_packet(
    parent: &LaunchParent,
    attempt: &Value,
    binding: &Value,
) -> Result<Value> {
    validate_current_session(binding)?;
    let route_alias = super::launch_manifest_route_alias(&parent.manifest)?;
    let identity_digest = super::digest_value(&json!({
        "kind":ANTIGRAVITY_CAPABILITY_KIND,
        "module_id":"antigravity",
        "artifact_id":ANTIGRAVITY_ARTIFACT_ID,
        "artifact_version":ANTIGRAVITY_ARTIFACT_VERSION,
        "protocol":{"major":1,"minor":0},
        "action":"task.dispatch"
    }))?;
    Ok(json!({
        "schema_version":1,
        "launch_operation_id":parent.operation_id,
        "plan_digest":parent.manifest["plan_digest"],
        "task":{
            "task_id":model::text(&parent.manifest["task"],"task_id")?,
            "revision":model::positive(&parent.manifest["task"],"observed_revision")?,
            "attempt_id":model::text(&parent.manifest["task"],"attempt_id")?,
            "snapshot_digest":super::digest_value(&attempt["task_snapshot"])?
        },
        "selection":{
            "route":route_alias,
            "provider":Value::Null,
            "model":binding["observation"]["opening_evidence"]["describe"]["model_observed"],
            "variant":Value::Null
        },
        "purpose":parent.manifest["runtime"]["purpose"],
        "capability":{
            "kind":ANTIGRAVITY_CAPABILITY_KIND,
            "identity_digest":identity_digest
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    const BINDING_ID: &str = "binding-antigravity";
    const GENERATION: i64 = 7;
    const BOOT_ID: &str = "boot-antigravity-7";
    const ROOT_ID: &str = "conversation-antigravity-7";
    const SCOPE_KEY: &str = "antigravity:scope";
    const MODEL_ID: &str = "provider/model-v5";

    fn binding() -> Value {
        json!({
            "binding_id":BINDING_ID,
            "generation":GENERATION,
            "state":"ready",
            "released_at_ms":null,
            "module_artifact_id":ANTIGRAVITY_ARTIFACT_ID,
            "native_root_id":ROOT_ID,
            "native_scope_key":SCOPE_KEY,
            "route":{
                "alias":"antigravity-test",
                "runtime":"antigravity",
                "module_artifact_id":ANTIGRAVITY_ARTIFACT_ID,
                "native_options":{"modelId":MODEL_ID}
            },
            "observation":{
                "connection":"connected",
                "recovery_required":false,
                "bridge_boot_id":BOOT_ID,
                "opening_evidence":{
                    "completion_condition":"native_session_initialized",
                    "bridge_boot_id":BOOT_ID,
                    "native_conversation_id":ROOT_ID,
                    "describe":{
                        "session_id":ROOT_ID,
                        "model_requested":MODEL_ID,
                        "model_observed":MODEL_ID,
                        "model_verification":"verified"
                    }
                }
            }
        })
    }

    fn parent() -> LaunchParent {
        let manifest = json!({
            "plan_digest":format!("sha256:{}", "a".repeat(64)),
            "runtime":{"route":{"alias":"antigravity-test"},"purpose":"implementation"},
            "task":{"task_id":"task","observed_revision":1,"attempt_id":"attempt"},
            "binding":{"binding_id":BINDING_ID,"generation":GENERATION,"native_root_id":ROOT_ID}
        });
        LaunchParent {
            operation_id: "launch".into(),
            state: "queued".into(),
            task_id: Some("task".into()),
            attempt_id: Some("attempt".into()),
            binding_id: Some(BINDING_ID.into()),
            binding_generation: Some(GENERATION),
            effective_request_json: "{}".into(),
            effective: json!({}),
            manifest,
        }
    }

    fn attempt() -> Value {
        json!({"task_snapshot":{"revision":1,"spec":{} }})
    }

    #[test]
    fn packet_uses_current_store_session_without_refresh_or_proof_digests() {
        let current = binding();
        validate_current_session(&current).expect("current exact Open evidence");
        let packet = build_packet(&parent(), &attempt(), &current).expect("dispatch packet");
        assert_eq!(packet["selection"]["model"], MODEL_ID);
        assert_eq!(packet["capability"]["kind"], ANTIGRAVITY_CAPABILITY_KIND);
        assert!(packet["capability"]["identity_digest"].is_string());
        assert_eq!(packet["capability"].as_object().unwrap().len(), 2);
        assert!(packet.to_string().find("session_proof").is_none());
        assert!(packet.to_string().find("lease_authority_digest").is_none());
        assert!(packet.to_string().find("assignment_digest").is_none());
    }

    #[test]
    fn native_default_model_keeps_unknown_effective_model() {
        let mut current = binding();
        current["route"]["native_options"] = json!({});
        current["observation"]["opening_evidence"]["describe"] = json!({
            "session_id":ROOT_ID,
            "model_requested":null,
            "model_observed":null,
            "model_verification":"native_default"
        });
        validate_current_session(&current).expect("native default session");
        let packet = build_packet(&parent(), &attempt(), &current).expect("dispatch packet");
        assert!(packet["selection"]["model"].is_null());
    }

    #[test]
    fn stale_boot_recovery_connection_root_and_model_are_rejected() {
        let mut stale_boot = binding();
        stale_boot["observation"]["opening_evidence"]["bridge_boot_id"] = json!("other");
        assert!(validate_current_session(&stale_boot).is_err());

        let mut recovery = binding();
        recovery["observation"]["recovery_required"] = json!(true);
        assert!(validate_current_session(&recovery).is_err());

        let mut disconnected = binding();
        disconnected["observation"]["connection"] = json!("disconnected");
        assert!(validate_current_session(&disconnected).is_err());

        let mut wrong_root = binding();
        wrong_root["observation"]["opening_evidence"]["native_conversation_id"] =
            json!("another-root");
        assert!(validate_current_session(&wrong_root).is_err());

        let mut wrong_model = binding();
        wrong_model["observation"]["opening_evidence"]["describe"]["model_observed"] =
            json!("another-model");
        assert!(validate_current_session(&wrong_model).is_err());
    }
}
