//! Receipt validation for Antigravity warm-stream adapters.
//!
//! The CLI exposes no native turn/input identity. A terminal receipt is therefore
//! bound directly to the authenticated module Operation, binding generation,
//! current adapter boot, native conversation, strictly positive result ordinal,
//! response digest and documented terminal status. Observation is independent
//! current-state data; it is not a prerequisite or a duplicate result ledger.

use crate::{
    error::{Error, Result},
    runtime::{EffectOutcome, RuntimeOutcome},
};
use serde_json::{Value, json};

pub const RUNTIME: &str = "antigravity";
pub const ARTIFACT_ID: &str = "antigravity-cli-warm-bridge.3";
const RETAINED_BRIDGE_ARTIFACT_ID: &str = "antigravity-cli-warm-bridge.2";
pub const RUST_HEADLESS_ARTIFACT_ID: &str = "eliot-antigravity.rust-headless.1";
const COMPLETION_CONDITION: &str = "native_terminal_result_observed";

pub struct OutcomeContext<'a> {
    pub binding: &'a Value,
    pub operation: &'a Value,
    pub outcome: &'a RuntimeOutcome,
    /// Supplied only after Store validates an exact admitted reconcile link and
    /// the binding's proven previous-owner departure transition.
    pub allowed_origin_boot_id: Option<&'a str>,
}

pub fn is_route(route: &Value) -> bool {
    route["runtime"] == RUNTIME
        && matches!(
            route["module_artifact_id"].as_str(),
            Some(ARTIFACT_ID) | Some(RETAINED_BRIDGE_ARTIFACT_ID) | Some(RUST_HEADLESS_ARTIFACT_ID)
        )
}

/// Validate one terminal receipt. `task.dispatch` returns an Attempt producer;
/// `agent.send` is validated but creates no new producer. Unknown/Accepted and
/// local preflight rejection remain outside terminal producer settlement.
pub fn validate_outcome(context: OutcomeContext<'_>) -> Result<Option<Value>> {
    let OutcomeContext {
        binding,
        operation,
        outcome,
        allowed_origin_boot_id,
    } = context;

    if !is_route(&binding["route"]) {
        return Err(Error::new(
            "UNSUPPORTED_RUNTIME",
            "warm-stream receipt requires a supported exact Antigravity artifact route",
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

    let origin_boot_id = required_text(&receipt["bridge_boot_id"], "origin bridge boot_id")?;
    let accepted_boot = origin_boot_id == active_boot_id
        || allowed_origin_boot_id.is_some_and(|allowed| allowed == origin_boot_id);
    if outcome.details["completion_condition"] != COMPLETION_CONDITION
        || receipt["input_operation_id"] != operation_id
        || receipt["native_conversation_id"] != native_conversation_id
        || !accepted_boot
        || receipt["result_ordinal"]
            .as_u64()
            .is_none_or(|ordinal| ordinal == 0)
        || !is_sha256(receipt["response_sha256"].as_str())
        || receipt["status"].as_str().is_none()
    {
        return Err(identity_error(
            "warm-stream outcome has an incomplete or mismatched terminal fingerprint",
        ));
    }

    let (disposition, expected_outcome) = match receipt["status"].as_str() {
        Some("SUCCESS") => ("completed", EffectOutcome::Applied),
        Some("ERROR") => ("failed", EffectOutcome::Rejected),
        Some("CANCELED" | "INTERRUPTED") => ("cancelled", EffectOutcome::Rejected),
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
            "bridge_boot_id":origin_boot_id,
            "result_ordinal":receipt["result_ordinal"],
            "response_sha256":receipt["response_sha256"],
            "status":receipt["status"]
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

    fn fixture(status: &str, effect: EffectOutcome) -> (Value, Value, RuntimeOutcome) {
        let binding = json!({
            "binding_id":"binding-1",
            "generation":3,
            "native_root_id":"conversation-1",
            "native_scope_key":"antigravity:C:/project",
            "route":{"runtime":RUNTIME,"module_artifact_id":RUST_HEADLESS_ARTIFACT_ID},
            "observation":{"bridge_boot_id":"boot-1"}
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
                "local_execution_ref":{
                    "input_operation_id":"op-1",
                    "native_conversation_id":"conversation-1",
                    "bridge_boot_id":"boot-1",
                    "result_ordinal":7,
                    "response_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "status":status
                }
            }),
        };
        (binding, operation, outcome)
    }

    #[test]
    fn terminal_dispatch_is_self_contained() {
        let (binding, operation, outcome) = fixture("SUCCESS", EffectOutcome::Applied);
        let producer = validate_outcome(OutcomeContext {
            binding: &binding,
            operation: &operation,
            outcome: &outcome,
            allowed_origin_boot_id: None,
        })
        .unwrap()
        .unwrap();
        assert_eq!(producer["disposition"], "completed");
        assert_eq!(producer["local_execution_ref"]["result_ordinal"], 7);
        assert!(
            producer["local_execution_ref"]
                .get("observation_id")
                .is_none()
        );
    }

    #[test]
    fn terminal_status_and_operation_identity_are_closed() {
        for (status, effect, disposition) in [
            ("ERROR", EffectOutcome::Rejected, "failed"),
            ("CANCELED", EffectOutcome::Rejected, "cancelled"),
            ("INTERRUPTED", EffectOutcome::Rejected, "cancelled"),
        ] {
            let (binding, operation, outcome) = fixture(status, effect);
            let producer = validate_outcome(OutcomeContext {
                binding: &binding,
                operation: &operation,
                outcome: &outcome,
                allowed_origin_boot_id: None,
            })
            .unwrap()
            .unwrap();
            assert_eq!(producer["disposition"], disposition);
        }

        let (binding, operation, outcome) = fixture("WAITING", EffectOutcome::Applied);
        assert!(
            validate_outcome(OutcomeContext {
                binding: &binding,
                operation: &operation,
                outcome: &outcome,
                allowed_origin_boot_id: None,
            })
            .is_err()
        );
    }

    #[test]
    fn current_boot_root_and_scope_are_required() {
        let (binding, operation, mut outcome) = fixture("SUCCESS", EffectOutcome::Applied);
        outcome.details["local_execution_ref"]["bridge_boot_id"] = json!("old-boot");
        assert!(
            validate_outcome(OutcomeContext {
                binding: &binding,
                operation: &operation,
                outcome: &outcome,
                allowed_origin_boot_id: None,
            })
            .is_err()
        );

        let (binding, operation, mut outcome) = fixture("SUCCESS", EffectOutcome::Applied);
        outcome.native_root_id = Some("other".into());
        assert!(
            validate_outcome(OutcomeContext {
                binding: &binding,
                operation: &operation,
                outcome: &outcome,
                allowed_origin_boot_id: None,
            })
            .is_err()
        );
    }

    #[test]
    fn prior_boot_requires_store_validated_reconcile_origin() {
        let (mut binding, operation, mut outcome) = fixture("SUCCESS", EffectOutcome::Applied);
        binding["observation"]["bridge_boot_id"] = json!("boot-2");
        binding["observation"]["previous_bridge_boot_id"] = json!("boot-1");
        binding["observation"]["recovery_required"] = json!(true);
        binding["state"] = json!("reconciling");
        outcome.details["reconcile_operation_id"] = json!("reconcile-1");
        let producer = validate_outcome(OutcomeContext {
            binding: &binding,
            operation: &operation,
            outcome: &outcome,
            allowed_origin_boot_id: Some("boot-1"),
        })
        .unwrap()
        .unwrap();
        assert_eq!(producer["local_execution_ref"]["bridge_boot_id"], "boot-1");

        assert!(
            validate_outcome(OutcomeContext {
                binding: &binding,
                operation: &operation,
                outcome: &outcome,
                allowed_origin_boot_id: None,
            })
            .is_err()
        );
    }

    #[test]
    fn legacy_observation_field_is_ignored_during_finite_transition() {
        let (binding, operation, mut outcome) = fixture("SUCCESS", EffectOutcome::Applied);
        outcome.details["local_execution_ref"]["observation_id"] = json!(41);
        assert!(
            validate_outcome(OutcomeContext {
                binding: &binding,
                operation: &operation,
                outcome: &outcome,
                allowed_origin_boot_id: None,
            })
            .unwrap()
            .is_some()
        );
    }

    #[test]
    fn send_is_validated_without_creating_an_attempt_producer() {
        let (binding, mut operation, outcome) = fixture("SUCCESS", EffectOutcome::Applied);
        operation["method"] = json!("agent.send");
        assert!(
            validate_outcome(OutcomeContext {
                binding: &binding,
                operation: &operation,
                outcome: &outcome,
                allowed_origin_boot_id: None,
            })
            .unwrap()
            .is_none()
        );
    }
}
