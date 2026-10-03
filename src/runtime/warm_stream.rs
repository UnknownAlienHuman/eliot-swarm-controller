//! Receipt validation for the Antigravity warm-stream adapter.
//!
//! A CLI result has no native turn ID. This contract binds the local adapter's
//! sequential result association to an exact operation, native conversation,
//! bridge boot, result ordinal, response digest, and already-recorded Store
//! observation. It deliberately does not manufacture a native run identifier.

use crate::{
    error::{Error, Result},
    runtime::{EffectOutcome, RuntimeOutcome},
};
use serde_json::{Value, json};

pub const RUNTIME: &str = "antigravity";
pub const ARTIFACT_ID: &str = "antigravity-cli-warm-bridge.2";
const COMPLETION_CONDITION: &str = "native_terminal_result_observed";

/// The binding, operation, and latest committed observation passed by the
/// Store after it has verified that `observation_id` is the materialized
/// observation for this binding generation.
pub struct OutcomeContext<'a> {
    pub binding: &'a Value,
    pub operation: &'a Value,
    pub outcome: &'a RuntimeOutcome,
    pub observation_id: Option<i64>,
    pub observation: &'a Value,
}

/// Only the new artifact opts into this contract. Existing bridge.1 bindings
/// retain their original behavior and cannot claim this stronger evidence.
pub fn is_route(route: &Value) -> bool {
    route["runtime"] == RUNTIME && route["module_artifact_id"] == ARTIFACT_ID
}

/// Validate a terminal warm-stream receipt and return a producer for a
/// `task.dispatch`. `agent.send` receipts are validated and stored on the
/// Operation, but do not create an Attempt producer. Unknown and Accepted
/// outcomes never create a producer.
pub fn validate_outcome(context: OutcomeContext<'_>) -> Result<Option<Value>> {
    let OutcomeContext {
        binding,
        operation,
        outcome,
        observation_id,
        observation,
    } = context;

    if !is_route(&binding["route"]) {
        return Err(Error::new(
            "UNSUPPORTED_RUNTIME",
            "warm-stream receipt requires the exact Antigravity bridge.2 route",
        ));
    }
    if !matches!(
        operation["method"].as_str(),
        Some("task.dispatch" | "agent.send")
    ) {
        return Err(Error::invalid(
            "warm-stream terminal receipts apply only to task.dispatch and agent.send",
        ));
    }
    if matches!(
        outcome.outcome,
        EffectOutcome::Unknown | EffectOutcome::Accepted
    ) {
        return Ok(None);
    }

    let receipt = &outcome.details["local_execution_ref"];
    if receipt.is_null() {
        if matches!(outcome.outcome, EffectOutcome::Applied) {
            return Err(identity_error(
                "Applied warm-stream outcomes require a terminal local execution receipt",
            ));
        }
        // A local preflight rejection has no native execution receipt and
        // therefore cannot discharge an Attempt producer.
        return Ok(None);
    }

    if operation["operation_id"].as_str() != Some(outcome.operation_id.as_str())
        || operation["binding_id"] != binding["binding_id"]
        || operation["binding_generation"] != binding["generation"]
    {
        return Err(identity_error(
            "warm-stream receipt names another Operation or binding generation",
        ));
    }
    if outcome.turn_id.is_some() || outcome.native_input_id.is_some() {
        return Err(identity_error(
            "warm-stream receipts must not invent native turn or inbox IDs",
        ));
    }

    let operation_id = required_text(&operation["operation_id"], "operation_id")?;
    let native_conversation_id = required_text(&binding["native_root_id"], "native_root_id")?;
    let native_scope_key = required_text(&binding["native_scope_key"], "native_scope_key")?;
    let active_boot_id = required_text(
        &binding["observation"]["bridge_boot_id"],
        "active bridge boot_id",
    )?;
    if outcome.native_root_id.as_deref() != Some(native_conversation_id)
        || outcome.native_scope_key.as_deref() != Some(native_scope_key)
    {
        return Err(identity_error(
            "warm-stream outcome does not name the binding's exact native identity",
        ));
    }

    let cited_observation_id = receipt["observation_id"]
        .as_i64()
        .filter(|id| *id > 0)
        .ok_or_else(|| identity_error("warm-stream receipt needs a positive observation_id"))?;
    if observation_id != Some(cited_observation_id)
        || binding["observation"]["native_observation_id"] != cited_observation_id
    {
        return Err(identity_error(
            "warm-stream receipt does not cite the binding's current recorded observation",
        ));
    }
    if observation["boot_id"] != active_boot_id
        || observation["native_root_id"] != native_conversation_id
        || observation["native_scope_key"] != native_scope_key
    {
        return Err(identity_error(
            "warm-stream observation belongs to another boot or native conversation",
        ));
    }

    if outcome.details["completion_condition"] != COMPLETION_CONDITION
        || receipt["input_operation_id"] != operation_id
        || receipt["native_conversation_id"] != native_conversation_id
        || receipt["bridge_boot_id"] != active_boot_id
        || receipt["result_ordinal"]
            .as_u64()
            .is_none_or(|ordinal| ordinal == 0)
        || !is_sha256(receipt["response_sha256"].as_str())
        || receipt["status"].as_str().is_none()
    {
        return Err(identity_error(
            "warm-stream outcome has an incomplete or mismatched local result fingerprint",
        ));
    }

    let observed_results = observation["local_execution_results"]
        .as_array()
        .ok_or_else(|| identity_error("warm-stream observation has no local result receipts"))?;
    validate_observed_ordinals(observed_results)?;
    let matches = observed_results
        .iter()
        .filter(|observed| same_fingerprint(receipt, observed))
        .count();
    if matches != 1 {
        return Err(identity_error(
            "warm-stream outcome is not backed by exactly one matching observed result",
        ));
    }

    let (disposition, expected_outcome) = match receipt["status"].as_str() {
        Some("SUCCESS") => ("completed", EffectOutcome::Applied),
        Some("ERROR") => ("failed", EffectOutcome::Rejected),
        Some("CANCELED" | "INTERRUPTED") => ("cancelled", EffectOutcome::Rejected),
        // WAITING/RUNNING are nonterminal; unknown statuses are not promoted
        // to terminal success or failure without a documented contract.
        _ => {
            return Err(identity_error(
                "warm-stream result status is not a supported terminal status",
            ));
        }
    };
    if !same_outcome(outcome.outcome, expected_outcome) {
        return Err(identity_error(
            "warm-stream terminal status conflicts with the reported outcome",
        ));
    }

    if operation["method"] != "task.dispatch" {
        return Ok(None);
    }

    Ok(Some(json!({
        "assignment_id":operation_id,
        "native_session_id":native_conversation_id,
        "disposition":disposition,
        "local_execution_ref": {
            "kind":"antigravity_warm_result",
            "input_operation_id":operation_id,
            "native_conversation_id":native_conversation_id,
            "bridge_boot_id":active_boot_id,
            "result_ordinal":receipt["result_ordinal"],
            "response_sha256":receipt["response_sha256"],
            "status":receipt["status"],
            "observation_id":cited_observation_id
        }
    })))
}

fn required_text<'a>(value: &'a Value, label: &str) -> Result<&'a str> {
    value
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| identity_error(format!("warm-stream receipt needs {label}")))
}

fn is_sha256(value: Option<&str>) -> bool {
    value.is_some_and(|value| {
        value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

fn same_fingerprint(receipt: &Value, observed: &Value) -> bool {
    [
        "input_operation_id",
        "native_conversation_id",
        "bridge_boot_id",
        "result_ordinal",
        "response_sha256",
        "status",
    ]
    .iter()
    .all(|key| receipt[*key] == observed[*key])
}

fn validate_observed_ordinals(receipts: &[Value]) -> Result<()> {
    let mut prior = 0_u64;
    for receipt in receipts {
        let ordinal = receipt["result_ordinal"]
            .as_u64()
            .filter(|ordinal| *ordinal > 0)
            .ok_or_else(|| identity_error("observed result ordinal must be positive"))?;
        if ordinal <= prior {
            return Err(identity_error(
                "observed warm-stream result ordinals must increase strictly",
            ));
        }
        prior = ordinal;
    }
    Ok(())
}

fn same_outcome(actual: EffectOutcome, expected: EffectOutcome) -> bool {
    matches!(
        (actual, expected),
        (EffectOutcome::Applied, EffectOutcome::Applied)
            | (EffectOutcome::Rejected, EffectOutcome::Rejected)
    )
}

fn identity_error(message: impl Into<String>) -> Error {
    Error::new("NATIVE_IDENTITY_MISMATCH", message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn fixture(status: &str, effect: EffectOutcome) -> (Value, Value, RuntimeOutcome, Value, i64) {
        let receipt = json!({
            "input_operation_id":"op-1",
            "native_conversation_id":"conversation-1",
            "bridge_boot_id":"boot-1",
            "result_ordinal":7,
            "response_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "status":status,
            "observation_id":41
        });
        let observed = json!({
            "boot_id":"boot-1",
            "native_root_id":"conversation-1",
            "native_scope_key":"antigravity:C:/project",
            "local_execution_results":[{
                "input_operation_id":"op-1",
                "native_conversation_id":"conversation-1",
                "bridge_boot_id":"boot-1",
                "result_ordinal":7,
                "response_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "status":status
            }]
        });
        let binding = json!({
            "binding_id":"binding-1",
            "generation":3,
            "native_root_id":"conversation-1",
            "native_scope_key":"antigravity:C:/project",
            "route":{"runtime":RUNTIME,"module_artifact_id":ARTIFACT_ID},
            "observation":{
                "bridge_boot_id":"boot-1",
                "native_observation_id":41
            }
        });
        let operation = json!({
            "operation_id":"op-1",
            "method":"task.dispatch",
            "binding_id":"binding-1",
            "binding_generation":3
        });
        let outcome = RuntimeOutcome {
            operation_id: "op-1".into(),
            outcome: effect,
            native_scope_key: Some("antigravity:C:/project".into()),
            native_root_id: Some("conversation-1".into()),
            turn_id: None,
            native_input_id: None,
            details: json!({
                "completion_condition":COMPLETION_CONDITION,
                "local_execution_ref":receipt
            }),
        };
        (binding, operation, outcome, observed, 41)
    }

    fn validate(
        binding: &Value,
        operation: &Value,
        outcome: &RuntimeOutcome,
        observed: &Value,
        observation_id: i64,
    ) -> Result<Option<Value>> {
        validate_outcome(OutcomeContext {
            binding,
            operation,
            outcome,
            observation_id: Some(observation_id),
            observation: observed,
        })
    }

    #[test]
    fn success_creates_local_execution_producer_without_native_turn_id() {
        let (binding, operation, outcome, observed, observation_id) =
            fixture("SUCCESS", EffectOutcome::Applied);
        let producer = validate(&binding, &operation, &outcome, &observed, observation_id)
            .unwrap()
            .unwrap();
        assert_eq!(producer["disposition"], "completed");
        assert_eq!(producer["native_session_id"], "conversation-1");
        assert_eq!(producer["local_execution_ref"]["result_ordinal"], 7);
        assert!(producer.get("native_run_id").is_none());
    }

    #[test]
    fn terminal_failures_are_rejected_and_nonterminal_status_cannot_be_applied() {
        for (status, disposition) in [
            ("ERROR", "failed"),
            ("CANCELED", "cancelled"),
            ("INTERRUPTED", "cancelled"),
        ] {
            let (binding, operation, outcome, observed, observation_id) =
                fixture(status, EffectOutcome::Rejected);
            let producer = validate(&binding, &operation, &outcome, &observed, observation_id)
                .unwrap()
                .unwrap();
            assert_eq!(producer["disposition"], disposition);
        }
        let (binding, operation, outcome, observed, observation_id) =
            fixture("WAITING", EffectOutcome::Applied);
        assert!(validate(&binding, &operation, &outcome, &observed, observation_id).is_err());
    }

    #[test]
    fn mismatched_observation_and_old_artifact_do_not_authorize_receipt() {
        let (mut binding, operation, outcome, observed, observation_id) =
            fixture("SUCCESS", EffectOutcome::Applied);
        binding["observation"]["native_observation_id"] = json!(42);
        assert!(validate(&binding, &operation, &outcome, &observed, observation_id).is_err());

        let (mut binding, operation, outcome, observed, observation_id) =
            fixture("SUCCESS", EffectOutcome::Applied);
        binding["route"]["module_artifact_id"] = json!("antigravity-cli-warm-bridge.1");
        assert!(!is_route(&binding["route"]));
        assert!(validate(&binding, &operation, &outcome, &observed, observation_id).is_err());
    }

    #[test]
    fn root_boot_fingerprint_and_synthetic_native_ids_are_guarded() {
        let (binding, operation, mut outcome, observed, observation_id) =
            fixture("SUCCESS", EffectOutcome::Applied);
        outcome.native_root_id = Some("other-conversation".into());
        assert!(validate(&binding, &operation, &outcome, &observed, observation_id).is_err());

        let (binding, operation, outcome, mut observed, observation_id) =
            fixture("SUCCESS", EffectOutcome::Applied);
        observed["boot_id"] = json!("old-boot");
        assert!(validate(&binding, &operation, &outcome, &observed, observation_id).is_err());

        let (binding, operation, outcome, mut observed, observation_id) =
            fixture("SUCCESS", EffectOutcome::Applied);
        observed["local_execution_results"][0]["response_sha256"] =
            json!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        assert!(validate(&binding, &operation, &outcome, &observed, observation_id).is_err());

        let (binding, operation, mut outcome, observed, observation_id) =
            fixture("SUCCESS", EffectOutcome::Applied);
        outcome.turn_id = Some("synthetic-turn".into());
        assert!(validate(&binding, &operation, &outcome, &observed, observation_id).is_err());
    }

    #[test]
    fn local_preflight_rejection_needs_no_native_producer() {
        let (binding, operation, mut outcome, observed, observation_id) =
            fixture("ERROR", EffectOutcome::Rejected);
        outcome.details = json!({"diagnostic_code":"NATIVE_SPAWN_FAILED"});
        assert_eq!(
            validate(&binding, &operation, &outcome, &observed, observation_id).unwrap(),
            None
        );
    }

    #[test]
    fn unknown_outcome_never_creates_a_producer() {
        let (binding, operation, mut outcome, observed, observation_id) =
            fixture("SUCCESS", EffectOutcome::Unknown);
        outcome.details = json!({"diagnostic_code":"STREAM_ENDED_BEFORE_RESULT_EVIDENCE"});
        assert_eq!(
            validate(&binding, &operation, &outcome, &observed, observation_id).unwrap(),
            None
        );
    }
}
