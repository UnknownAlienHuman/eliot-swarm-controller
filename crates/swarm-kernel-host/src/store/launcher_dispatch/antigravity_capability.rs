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
const ANTIGRAVITY_CAPABILITY_KIND: &str = "antigravity_store_binding_v1";

pub(super) fn is_current_capability(value: &Value) -> bool {
    value["kind"] == ANTIGRAVITY_CAPABILITY_KIND
}

pub(super) fn unavailable() -> Error {
    Error::new(
        "LAUNCH_DISPATCH_CAPABILITY_UNAVAILABLE",
        "no current trusted native dispatch capability is available for this route",
    )
}

fn stale_state() -> Error {
    Error::new(
        "STALE_ANTIGRAVITY_DISPATCH_STATE",
        "the current Antigravity binding state is not safe for dispatch",
    )
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
        || identity.artifact.version.as_str() != "6"
        || identity.protocol.major != 1
        || identity.protocol.minor != 0
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

/// Resolve the current Antigravity dispatch identity from Store-owned
/// binding/open/observation facts. This is not a second proof packet and
/// requires no `agent.refresh` Operation.
pub(super) fn current_capability(binding: &Value) -> Result<Value> {
    let binding_id = model::text(binding, "binding_id").map_err(|_| stale_state())?;
    let generation = model::positive(binding, "generation").map_err(|_| stale_state())?;
    let root_id = model::text(binding, "native_root_id").map_err(|_| stale_state())?;
    let scope_key = model::text(binding, "native_scope_key").map_err(|_| stale_state())?;
    let observation = binding
        .get("observation")
        .filter(|value| value.is_object())
        .ok_or_else(stale_state)?;
    let opening = observation
        .get("opening_evidence")
        .filter(|value| value.is_object())
        .ok_or_else(stale_state)?;
    let describe = opening
        .get("describe")
        .filter(|value| value.is_object())
        .ok_or_else(stale_state)?;
    let native = observation
        .get("native")
        .filter(|value| value.is_object())
        .ok_or_else(stale_state)?;
    let init = native
        .get("init")
        .filter(|value| value.is_object())
        .ok_or_else(stale_state)?;
    let boot_id = model::text(observation, "bridge_boot_id").map_err(|_| stale_state())?;
    let requested_model = binding["route"]["native_options"]
        .get("modelId")
        .and_then(Value::as_str);
    let observed_model = describe.get("model_observed").and_then(Value::as_str);
    let model_verification = describe
        .get("model_verification")
        .and_then(Value::as_str)
        .ok_or_else(stale_state)?;

    let model_matches = match requested_model {
        Some(requested) => {
            !requested.is_empty()
                && requested.len() <= 256
                && describe["model_requested"].as_str() == Some(requested)
                && observed_model == Some(requested)
                && model_verification == "verified"
                && init["model"].as_str() == Some(requested)
                && native["requested_model_id"].as_str() == Some(requested)
                && native["model_readback_state"] == "verified"
        }
        None => {
            describe["model_requested"].is_null()
                && observed_model.is_none_or(|model| !model.is_empty() && model.len() <= 256)
                && model_verification == "native_default"
                && init["model"].as_str() == observed_model
                && native["requested_model_id"].is_null()
                && native["model_readback_state"] == "native_default"
        }
    };

    if binding["state"] != "ready"
        || !binding["released_at_ms"].is_null()
        || observation["connection"] != "connected"
        || observation["recovery_required"] == true
        || model::text(opening, "bridge_boot_id").map_err(|_| stale_state())? != boot_id
        || model::text(opening, "native_conversation_id").map_err(|_| stale_state())? != root_id
        || model::text(native, "boot_id").map_err(|_| stale_state())? != boot_id
        || model::text(native, "native_root_id").map_err(|_| stale_state())? != root_id
        || model::text(native, "native_scope_key").map_err(|_| stale_state())? != scope_key
        || native["phase"] != "ready"
        || model::text(init, "conversation_id").map_err(|_| stale_state())? != root_id
        || native["native_events_seen"]
            .as_u64()
            .is_none_or(|events| events == 0)
        || native["gaps"] != 0
        || !native["stream_error_code"].is_null()
        || !native["exit_code"].is_null()
        || native["turn_pending"] != false
        || !model_matches
    {
        return Err(stale_state());
    }

    Ok(json!({
        "kind":ANTIGRAVITY_CAPABILITY_KIND,
        "binding_id":binding_id,
        "binding_generation":generation,
        "worker_boot_id":boot_id,
        "native_root_id":root_id,
        "native_scope_key_sha256":model::digest(scope_key.as_bytes()),
        "model_requested":requested_model,
        "model_observed":observed_model,
        "model_verification":model_verification,
    }))
}

pub(super) fn build_packet(
    parent: &LaunchParent,
    attempt: &Value,
    binding: &Value,
    capability_value: &Value,
) -> Result<Value> {
    let current = current_capability(binding)?;
    if model::canonical(&current)? != model::canonical(capability_value)? {
        return Err(stale_state());
    }
    let route_alias = super::launch_manifest_route_alias(&parent.manifest)?;
    let identity_digest = super::digest_value(&current)?;
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
            "model":current["model_observed"],
            "variant":Value::Null
        },
        "purpose":parent.manifest["runtime"]["purpose"],
        "capability":{
            "kind":ANTIGRAVITY_CAPABILITY_KIND,
            "identity_digest":identity_digest,
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
    const SCOPE_KEY: &str = "native-scope-antigravity-7";
    const MODEL_ID: &str = "provider/model-v5";

    fn binding(model: Option<&str>) -> Value {
        let (requested, observed, verification) = match model {
            Some(model) => (json!(model), json!(model), json!("verified")),
            None => (Value::Null, Value::Null, json!("native_default")),
        };
        let mut native_options = serde_json::Map::new();
        if let Some(model) = model {
            native_options.insert("modelId".to_owned(), json!(model));
        }
        json!({
            "binding_id":BINDING_ID,
            "generation":GENERATION,
            "state":"ready",
            "released_at_ms":null,
            "native_root_id":ROOT_ID,
            "native_scope_key":SCOPE_KEY,
            "route":{
                "alias":"antigravity-test",
                "runtime":"antigravity",
                "module_artifact_id":ANTIGRAVITY_ARTIFACT_ID,
                "native_options":native_options,
            },
            "observation":{
                "connection":"connected",
                "recovery_required":false,
                "bridge_boot_id":BOOT_ID,
                "opening_evidence":{
                    "bridge_boot_id":BOOT_ID,
                    "native_conversation_id":ROOT_ID,
                    "describe":{
                        "model_requested":requested,
                        "model_observed":observed,
                        "model_verification":verification,
                    }
                },
                "native":{
                    "phase":"ready",
                    "init":{"conversation_id":ROOT_ID,"model":observed},
                    "native_events_seen":1,
                    "gaps":0,
                    "stream_error_code":null,
                    "exit_code":null,
                    "turn_pending":false,
                    "boot_id":BOOT_ID,
                    "native_root_id":ROOT_ID,
                    "native_scope_key":SCOPE_KEY,
                    "requested_model_id":requested,
                    "model_readback_state":verification,
                }
            }
        })
    }

    fn parent() -> LaunchParent {
        LaunchParent {
            operation_id: "launch-antigravity".to_owned(),
            state: "queued".to_owned(),
            task_id: Some("task-antigravity".to_owned()),
            attempt_id: Some("attempt-antigravity".to_owned()),
            binding_id: Some(BINDING_ID.to_owned()),
            binding_generation: Some(GENERATION),
            effective_request_json: "{}".to_owned(),
            effective: json!({}),
            manifest: json!({
                "plan_digest":model::digest(b"plan"),
                "task":{
                    "task_id":"task-antigravity",
                    "observed_revision":1,
                    "attempt_id":"attempt-antigravity",
                },
                "runtime":{"route":{"alias":"antigravity-test"},"purpose":"implementation"},
            }),
        }
    }

    fn attempt() -> Value {
        json!({
            "task_id":"task-antigravity",
            "task_revision":1,
            "attempt_id":"attempt-antigravity",
            "binding_id":BINDING_ID,
            "binding_generation":GENERATION,
            "task_snapshot":{"revision":1},
        })
    }

    #[test]
    fn explicit_model_uses_store_binding_without_refresh_proof() {
        let binding = binding(Some(MODEL_ID));
        let capability = current_capability(&binding).unwrap();
        assert_eq!(capability["kind"], ANTIGRAVITY_CAPABILITY_KIND);
        assert_eq!(capability["worker_boot_id"], BOOT_ID);
        assert_eq!(capability["model_observed"], MODEL_ID);
        let packet = build_packet(&parent(), &attempt(), &binding, &capability).unwrap();
        assert_eq!(packet["selection"]["model"], MODEL_ID);
        assert_eq!(packet["capability"]["kind"], ANTIGRAVITY_CAPABILITY_KIND);
        assert!(packet["capability"]["identity_digest"].as_str().is_some());
        assert_eq!(packet["capability"].as_object().unwrap().len(), 2);
    }

    #[test]
    fn native_default_keeps_model_unknown_without_refresh_proof() {
        let binding = binding(None);
        let capability = current_capability(&binding).unwrap();
        assert!(capability["model_requested"].is_null());
        assert!(capability["model_observed"].is_null());
        assert_eq!(capability["model_verification"], "native_default");
        let packet = build_packet(&parent(), &attempt(), &binding, &capability).unwrap();
        assert!(packet["selection"]["model"].is_null());
    }

    #[test]
    fn changed_boot_model_or_native_health_fails_closed() {
        for mutation in [
            "boot",
            "model",
            "phase",
            "gap",
            "turn",
            "recovery",
            "connection",
        ] {
            let mut binding = binding(Some(MODEL_ID));
            match mutation {
                "boot" => binding["observation"]["bridge_boot_id"] = json!("other-boot"),
                "model" => binding["observation"]["native"]["init"]["model"] = json!("other-model"),
                "phase" => binding["observation"]["native"]["phase"] = json!("stream_ended"),
                "gap" => binding["observation"]["native"]["gaps"] = json!(1),
                "turn" => binding["observation"]["native"]["turn_pending"] = json!(true),
                "recovery" => binding["observation"]["recovery_required"] = json!(true),
                "connection" => binding["observation"]["connection"] = json!("disconnected"),
                _ => unreachable!(),
            }
            assert_eq!(
                current_capability(&binding).unwrap_err().code,
                "STALE_ANTIGRAVITY_DISPATCH_STATE",
                "{mutation}",
            );
        }
    }
}
