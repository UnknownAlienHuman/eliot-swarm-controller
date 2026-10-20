use crate::{
    error::{Error, Result},
    model,
};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};
use swarm_contracts::{
    module_contract::{
        runtime_command_schema, task_dispatch_admission_schema, task_dispatch_context_schema,
        task_prompt_schema,
    },
    runtime::{
        ANTIGRAVITY_DISPATCH_SESSION_PROOF_KIND, AntigravityDispatchSessionProof, EffectOutcome,
        RuntimeOutcome,
    },
};

use super::LaunchParent;

pub(super) const ANTIGRAVITY_ARTIFACT_ID: &str = "eliot-antigravity.rust-headless.1";
pub(super) const ANTIGRAVITY_PROOF_KIND: &str = ANTIGRAVITY_DISPATCH_SESSION_PROOF_KIND;

pub(super) fn is_session_proof(value: &Value) -> bool {
    matches!(
        value["kind"].as_str(),
        Some(ANTIGRAVITY_PROOF_KIND)
            | Some(swarm_contracts::runtime::ANTIGRAVITY_NATIVE_DEFAULT_SESSION_PROOF_KIND)
    )
}

pub(super) fn unavailable() -> Error {
    Error::new(
        "LAUNCH_DISPATCH_CAPABILITY_UNAVAILABLE",
        "no current trusted native dispatch capability is available for this route",
    )
}

fn stale_proof() -> Error {
    Error::new(
        "STALE_ANTIGRAVITY_DISPATCH_PROOF",
        "the current Antigravity session proof does not match the retained binding",
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

/// Read the latest refresh result for the exact live binding generation. A
/// newer queued, unresolved, rejected, or malformed refresh suppresses older
/// evidence; callers never fall back to an older successful snapshot.
pub(super) fn require_current_session_proof(db: &Connection, binding: &Value) -> Result<Value> {
    let binding_id = model::text(binding, "binding_id")?;
    let generation = model::positive(binding, "generation")?;
    let latest: Option<(String, String, Option<i64>, Option<String>)> = db
        .query_row(
            "SELECT operation_id,state,settled_at_ms,result_json \
             FROM operations WHERE binding_id=?1 AND binding_generation=?2 \
               AND method='agent.refresh' \
             ORDER BY rowid DESC LIMIT 1",
            params![binding_id, generation],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let Some((operation_id, state, settled_at_ms, result_json)) = latest else {
        return Err(stale_proof());
    };
    if state != "settled" || settled_at_ms.is_none_or(|time| time <= 0) {
        return Err(stale_proof());
    }
    let result_json = result_json.ok_or_else(stale_proof)?;
    let result: RuntimeOutcome = serde_json::from_str(&result_json).map_err(|_| stale_proof())?;
    let proof: AntigravityDispatchSessionProof =
        serde_json::from_value(result.details["antigravity_dispatch_session_proof"].clone())
            .map_err(|_| stale_proof())?;
    proof.validate().map_err(|_| stale_proof())?;
    validate_proof_matches_binding(&proof, &result, binding, &operation_id)?;
    Ok(serde_json::to_value(proof)?)
}

fn validate_proof_matches_binding(
    proof: &AntigravityDispatchSessionProof,
    result: &RuntimeOutcome,
    binding: &Value,
    operation_id: &str,
) -> Result<()> {
    let binding_id = binding["binding_id"].as_str().ok_or_else(stale_proof)?;
    let generation = binding["generation"].as_i64().ok_or_else(stale_proof)?;
    let root_id = binding["native_root_id"].as_str().ok_or_else(stale_proof)?;
    let scope_key = binding["native_scope_key"]
        .as_str()
        .ok_or_else(stale_proof)?;
    let observation = &binding["observation"];
    let opening = &observation["opening_evidence"];
    let opening_description = &opening["describe"];
    let snapshot = &result.details["snapshot"];
    let init = &snapshot["init"];

    if binding["state"] != "ready"
        || !binding["released_at_ms"].is_null()
        || observation["connection"] != "connected"
        || observation["recovery_required"] == true
        || proof.refresh_operation_id != operation_id
        || proof.binding_id != binding_id
        || proof.binding_generation != generation
        || Some(proof.worker_boot_id.as_str()) != observation["bridge_boot_id"].as_str()
        || Some(proof.worker_boot_id.as_str()) != opening["bridge_boot_id"].as_str()
        || proof.native_root_id != root_id
        || proof.conversation_id != root_id
        || proof.native_scope_key_sha256 != model::digest(scope_key.as_bytes())
        || result.operation_id != operation_id
        || result.outcome != EffectOutcome::Applied
        || result.native_root_id.as_deref() != Some(root_id)
        || result.native_scope_key.as_deref() != Some(scope_key)
        || result.details["completion_condition"] != "current_process_native_observation"
        || result.details["target"].as_str() != Some(root_id)
        || opening["native_conversation_id"].as_str() != Some(root_id)
        || opening_description["model_requested"].as_str() != proof.requested_model_id.as_deref()
        || opening_description["model_observed"].as_str() != proof.observed_model_id.as_deref()
        || opening_description["model_verification"].as_str()
            != Some(proof.model_readback_state.as_str())
        || (proof.requested_model_id.is_none()
            && binding["route"]["native_options"].get("modelId").is_some())
        || snapshot["phase"].as_str() != Some(proof.stream_phase.as_str())
        || init["conversation_id"].as_str() != Some(proof.conversation_id.as_str())
        || init["model"].as_str() != proof.observed_model_id.as_deref()
        || snapshot["native_events_seen"].as_u64() != Some(proof.native_events_seen)
        || snapshot["result_ordinal"].as_u64() != Some(proof.result_ordinal)
        || snapshot["gaps"].as_u64() != Some(proof.gap_count)
        || snapshot["stream_error_code"].as_str() != proof.stream_error_code.as_deref()
        || snapshot["exit_code"].as_i64() != proof.stream_exit_code.map(i64::from)
    {
        return Err(stale_proof());
    }
    Ok(())
}

pub(super) fn build_packet(
    parent: &LaunchParent,
    attempt: &Value,
    binding: &Value,
    proof_value: &Value,
) -> Result<Value> {
    let proof: AntigravityDispatchSessionProof =
        serde_json::from_value(proof_value.clone()).map_err(|_| stale_proof())?;
    proof.validate().map_err(|_| stale_proof())?;
    validate_proof_binding_only(&proof, binding)?;

    let route_alias = super::launch_manifest_route_alias(&parent.manifest)?;
    let assignment = json!({
        "task_id":attempt["task_id"],
        "task_revision":attempt["task_revision"],
        "attempt_id":attempt["attempt_id"],
        "binding_id":attempt["binding_id"],
        "binding_generation":attempt["binding_generation"],
        "native_session_id":parent.manifest["binding"]["native_root_id"],
        "participant_id":parent.manifest["participant"]["client_id"],
        "grant_revision":parent.manifest["participant"]["grant_revision"],
    });
    let assignment_digest = super::digest_value(&assignment)?;
    let lease_authority_digest =
        super::digest_value(&parent.manifest["workspace"]["lease_authority"])?;
    let session_proof_digest = super::digest_value(proof_value)?;
    let identity_digest = super::digest_value(&json!({
        "launch_operation_id":parent.operation_id,
        "plan_digest":parent.manifest["plan_digest"],
        "assignment_digest":assignment_digest,
        "lease_authority_digest":lease_authority_digest,
        "session_proof_digest":session_proof_digest,
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
            "model":proof.observed_model_id,
            "variant":Value::Null
        },
        "purpose":parent.manifest["runtime"]["purpose"],
        "capability":{
            "kind":proof.kind,
            "identity_digest":identity_digest,
            "assignment_digest":assignment_digest,
            "lease_authority_digest":lease_authority_digest,
            "session_proof_digest":session_proof_digest,
            "session_proof":proof
        }
    }))
}

fn validate_proof_binding_only(
    proof: &AntigravityDispatchSessionProof,
    binding: &Value,
) -> Result<()> {
    let binding_id = binding["binding_id"].as_str().ok_or_else(stale_proof)?;
    let generation = binding["generation"].as_i64().ok_or_else(stale_proof)?;
    let root_id = binding["native_root_id"].as_str().ok_or_else(stale_proof)?;
    let scope_key = binding["native_scope_key"]
        .as_str()
        .ok_or_else(stale_proof)?;
    if proof.binding_id != binding_id
        || proof.binding_generation != generation
        || proof.native_root_id != root_id
        || proof.conversation_id != root_id
        || proof.native_scope_key_sha256 != model::digest(scope_key.as_bytes())
        || Some(proof.worker_boot_id.as_str()) != binding["observation"]["bridge_boot_id"].as_str()
        || (proof.requested_model_id.is_none()
            && binding["route"]["native_options"].get("modelId").is_some())
    {
        return Err(stale_proof());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    const BINDING_ID: &str = "binding-antigravity";
    const GENERATION: i64 = 7;
    const BOOT_ID: &str = "boot-antigravity-7";
    const ROOT_ID: &str = "conversation-antigravity-7";
    const SCOPE_KEY: &str = "native-scope-antigravity-7";
    const MODEL_ID: &str = "provider/model-v5";

    fn session_proof_fixture() -> AntigravityDispatchSessionProof {
        AntigravityDispatchSessionProof {
            schema_version: 1,
            kind: ANTIGRAVITY_PROOF_KIND.to_owned(),
            refresh_operation_id: "refresh-current".to_owned(),
            binding_id: BINDING_ID.to_owned(),
            binding_generation: GENERATION,
            worker_boot_id: BOOT_ID.to_owned(),
            native_root_id: ROOT_ID.to_owned(),
            native_scope_key_sha256: model::digest(SCOPE_KEY.as_bytes()),
            conversation_id: ROOT_ID.to_owned(),
            requested_model_id: Some(MODEL_ID.to_owned()),
            observed_model_id: Some(MODEL_ID.to_owned()),
            model_readback_state: "verified".to_owned(),
            stream_phase: "ready".to_owned(),
            native_events_seen: 1,
            result_ordinal: 0,
            gap_count: 0,
            turn_pending: false,
            stream_error_code: None,
            stream_exit_code: None,
        }
    }

    fn binding() -> Value {
        json!({
            "binding_id":BINDING_ID,
            "generation":GENERATION,
            "state":"ready",
            "released_at_ms":null,
            "native_root_id":ROOT_ID,
            "native_scope_key":SCOPE_KEY,
            "observation":{
                "connection":"connected",
                "recovery_required":false,
                "bridge_boot_id":BOOT_ID,
                "opening_evidence":{
                    "bridge_boot_id":BOOT_ID,
                    "native_conversation_id":ROOT_ID,
                    "describe":{
                        "model_requested":MODEL_ID,
                        "model_observed":MODEL_ID,
                        "model_verification":"verified"
                    }
                }
            }
        })
    }

    fn refresh_result(proof: &AntigravityDispatchSessionProof) -> Value {
        json!({
            "operation_id":proof.refresh_operation_id,
            "outcome":"applied",
            "native_scope_key":SCOPE_KEY,
            "native_root_id":ROOT_ID,
            "turn_id":null,
            "native_input_id":null,
            "details":{
                "completion_condition":"current_process_native_observation",
                "target":ROOT_ID,
                "snapshot":{
                    "phase":"ready",
                    "init":{"conversation_id":ROOT_ID,"model":MODEL_ID},
                    "native_events_seen":proof.native_events_seen,
                    "result_ordinal":proof.result_ordinal,
                    "gaps":proof.gap_count,
                    "stream_error_code":proof.stream_error_code,
                    "exit_code":proof.stream_exit_code
                },
                "antigravity_dispatch_session_proof":proof
            }
        })
    }

    fn db_with_refresh(state: &str, created_at_ms: i64, result: Option<&Value>) -> Connection {
        let db = Connection::open_in_memory().expect("in-memory Store fixture");
        db.execute_batch(
            "CREATE TABLE operations (
                operation_id TEXT NOT NULL,
                binding_id TEXT NOT NULL,
                binding_generation INTEGER NOT NULL,
                method TEXT NOT NULL,
                state TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                settled_at_ms INTEGER,
                result_json TEXT
            );",
        )
        .expect("minimal operations schema");
        db.execute(
            "INSERT INTO operations(operation_id,binding_id,binding_generation,method,state,created_at_ms,settled_at_ms,result_json) \
             VALUES('refresh-current',?1,?2,'agent.refresh',?3,?4,?5,?6)",
            params![
                BINDING_ID,
                GENERATION,
                state,
                created_at_ms,
                if state == "settled" { Some(created_at_ms + 1) } else { None },
                result.map(serde_json::to_string).transpose().expect("serialize refresh result"),
            ],
        )
        .expect("insert refresh operation");
        db
    }

    #[test]
    fn current_settled_refresh_matches_live_binding_scope_and_open_evidence() {
        let proof = session_proof_fixture();
        let result = refresh_result(&proof);
        let db = db_with_refresh("settled", 10, Some(&result));

        let retained =
            require_current_session_proof(&db, &binding()).expect("exact current refresh proof");

        assert_eq!(retained["refresh_operation_id"], "refresh-current");
        assert_eq!(
            retained["native_scope_key_sha256"],
            model::digest(SCOPE_KEY.as_bytes())
        );
    }

    #[test]
    fn newer_unsettled_refresh_suppresses_older_current_proof() {
        let proof = session_proof_fixture();
        let result = refresh_result(&proof);
        let db = db_with_refresh("settled", 10, Some(&result));
        db.execute(
            "INSERT INTO operations(operation_id,binding_id,binding_generation,method,state,created_at_ms) \
             VALUES('refresh-newer',?1,?2,'agent.refresh','queued',20)",
            params![BINDING_ID, GENERATION],
        )
        .expect("insert newer refresh");

        assert!(require_current_session_proof(&db, &binding()).is_err());
    }

    #[test]
    fn native_default_session_proof_preserves_unknown_model_and_exact_binding() {
        let mut proof = session_proof_fixture();
        proof.schema_version = 2;
        proof.kind = swarm_contracts::runtime::ANTIGRAVITY_NATIVE_DEFAULT_SESSION_PROOF_KIND.into();
        proof.requested_model_id = None;
        proof.observed_model_id = None;
        proof.model_readback_state = "native_default".into();
        assert!(proof.validate().is_ok());
        let mut current_binding = binding();
        current_binding["observation"]["opening_evidence"]["describe"] = json!({
            "model_requested":null,"model_observed":null,"model_verification":"native_default"
        });
        let mut result = refresh_result(&proof);
        result["details"]["snapshot"]["init"]["model"] = Value::Null;
        let db = db_with_refresh("settled", 10, Some(&result));
        let retained = require_current_session_proof(&db, &current_binding)
            .expect("same current native-default session is dispatchable");
        assert_eq!(retained["schema_version"], 2);
        assert!(retained["requested_model_id"].is_null());
        assert!(retained["observed_model_id"].is_null());
        assert_eq!(retained["native_root_id"], ROOT_ID);

        // Missing selection cannot relabel a route that actually requested a
        // model. All boot/identity and current-refresh fences still apply.
        current_binding["route"]["native_options"]["modelId"] = json!(MODEL_ID);
        assert!(require_current_session_proof(&db, &current_binding).is_err());
        current_binding["route"]["native_options"]
            .as_object_mut()
            .unwrap()
            .remove("modelId");
        current_binding["observation"]["bridge_boot_id"] = json!("another-boot");
        assert!(require_current_session_proof(&db, &current_binding).is_err());
        proof.turn_pending = true;
        assert!(proof.validate().is_err());
    }

    #[test]
    fn native_default_model_state_cannot_bypass_explicit_or_legacy_proof_contract() {
        let mut proof = session_proof_fixture();
        proof.requested_model_id = None;
        proof.model_readback_state = "native_default".into();
        assert!(
            proof.validate().is_err(),
            "v1 still requires explicit model verification"
        );
        proof.schema_version = 2;
        assert!(
            proof.validate().is_err(),
            "schema and kind must identify the same contract"
        );
        proof.kind = swarm_contracts::runtime::ANTIGRAVITY_NATIVE_DEFAULT_SESSION_PROOF_KIND.into();
        assert!(
            proof.validate().is_ok(),
            "an observed default may remain a fact"
        );
        proof.requested_model_id = Some(MODEL_ID.into());
        assert!(
            proof.validate().is_err(),
            "explicit requests cannot use the default state"
        );
    }

    #[test]
    fn stale_boot_generation_and_pending_turn_are_rejected() {
        let proof = session_proof_fixture();
        let result = refresh_result(&proof);
        let db = db_with_refresh("settled", 10, Some(&result));

        let mut stale_binding = binding();
        stale_binding["observation"]["bridge_boot_id"] = json!("another-boot");
        assert!(require_current_session_proof(&db, &stale_binding).is_err());

        let mut wrong_generation = session_proof_fixture();
        wrong_generation.binding_generation += 1;
        assert!(wrong_generation.validate().is_ok());
        let mut malformed_result = refresh_result(&wrong_generation);
        malformed_result["operation_id"] = json!(wrong_generation.refresh_operation_id);
        let stale_db = db_with_refresh("settled", 10, Some(&malformed_result));
        assert!(require_current_session_proof(&stale_db, &binding()).is_err());

        let mut pending_turn = session_proof_fixture();
        pending_turn.turn_pending = true;
        assert!(pending_turn.validate().is_err());
    }
}
