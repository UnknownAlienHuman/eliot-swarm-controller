//! Identity checks for the pinned shared Codex app-server module.
use crate::{
    error::{Error, Result},
    model,
    runtime::RuntimeOutcome,
};
use serde_json::{Value, json};

pub const RUNTIME: &str = "codex";
pub const ARTIFACT_ID: &str = "codex-sdk-18194bf-bridge.3";

pub fn is_controller_route(route: &Value) -> bool {
    route["runtime"] == RUNTIME && route["module_artifact_id"] == ARTIFACT_ID
}

/// The controller supplies these canonical bytes to the Python SDK bridge;
/// cross-language JSON serialization must not change the admitted prompt.
pub fn dispatch_instruction(task_snapshot: &Value, text: &str) -> Result<String> {
    if !task_snapshot.is_object() || text.trim().is_empty() {
        return Err(Error::invalid(
            "Codex dispatch requires its immutable Task and text",
        ));
    }
    Ok(format!(
        "Task specification: {}\n\n{text}",
        model::canonical(task_snapshot)?
    ))
}

/// A user item and a turn are distinct native identities. Both must be bound
/// to this exact dispatch by the module's native history readback.
pub fn dispatch_producer(
    binding: &Value,
    outcome: &RuntimeOutcome,
    task_snapshot: &Value,
    text: &str,
) -> Result<Value> {
    let instruction = dispatch_instruction(task_snapshot, text)?;
    validate_input_receipt(binding, outcome, &instruction, None)?;
    Ok(json!({
        "assignment_id":outcome.operation_id,
        "native_session_id":outcome.native_root_id,
        "native_run_id":outcome.turn_id,
        "native_input_id":outcome.native_input_id,
        "client_user_message_id":outcome.operation_id,
        "admission_kind":"native_turn_input",
        "disposition":"admitted"
    }))
}

pub fn validate_input_receipt(
    binding: &Value,
    outcome: &RuntimeOutcome,
    instruction: &str,
    expected_turn: Option<&str>,
) -> Result<()> {
    let turn = outcome.turn_id.as_deref().filter(|v| !v.is_empty());
    let input = outcome.native_input_id.as_deref().filter(|v| !v.is_empty());
    let root = outcome.native_root_id.as_deref().filter(|v| !v.is_empty());
    let scope = outcome
        .native_scope_key
        .as_deref()
        .filter(|v| !v.is_empty());
    if !is_controller_route(&binding["route"])
        || turn.is_none()
        || input.is_none()
        || root.is_none()
        || scope.is_none()
        || expected_turn.is_some_and(|expected| Some(expected) != turn)
        || binding["native_root_id"].as_str() != root
        || binding["native_scope_key"].as_str() != scope
        || outcome.details["completion_condition"] != "native_input_admitted"
        || outcome.details["native_input_readback"] != "verified"
        || outcome.details["client_user_message_id"] != outcome.operation_id
        || outcome.details["prompt_sha256"] != model::digest(instruction.as_bytes())
        || outcome.details["prompt_bytes"].as_u64() != Some(instruction.len() as u64)
    {
        return Err(Error::new(
            "NATIVE_IDENTITY_MISMATCH",
            "Codex dispatch requires exact thread, turn, user item and prompt readback",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::EffectOutcome;

    #[test]
    fn dispatch_binds_separate_native_identities_to_exact_prompt() {
        let snapshot = json!({"title":"Unicode 🐇", "metadata":{"ratio":1.0}});
        let text = "return a marker";
        let prompt = dispatch_instruction(&snapshot, text).unwrap();
        let binding = json!({"route":{"runtime":RUNTIME,"module_artifact_id":ARTIFACT_ID},
            "native_root_id":"thread", "native_scope_key":"shared-server"});
        let mut outcome = RuntimeOutcome {
            operation_id: "operation".into(),
            outcome: EffectOutcome::Applied,
            native_scope_key: Some("shared-server".into()),
            native_root_id: Some("thread".into()),
            turn_id: Some("turn".into()),
            native_input_id: Some("native-user-item".into()),
            details: json!({"completion_condition":"native_input_admitted",
                "native_input_readback":"verified", "client_user_message_id":"operation",
                "prompt_sha256":model::digest(prompt.as_bytes()), "prompt_bytes":prompt.len()}),
        };
        let producer = dispatch_producer(&binding, &outcome, &snapshot, text).unwrap();
        assert_eq!(producer["native_run_id"], "turn");
        assert_eq!(producer["native_input_id"], "native-user-item");
        assert_eq!(producer["disposition"], "admitted");
        assert!(validate_input_receipt(&binding, &outcome, &prompt, Some("turn")).is_ok());
        assert!(validate_input_receipt(&binding, &outcome, &prompt, Some("another-turn")).is_err());
        assert!(dispatch_producer(&binding, &outcome, &snapshot, "different text").is_err());
        assert!(dispatch_producer(&binding, &outcome, &json!({"title":"changed"}), text).is_err());
        outcome.details["client_user_message_id"] = json!("another-operation");
        assert!(dispatch_producer(&binding, &outcome, &snapshot, text).is_err());
        outcome.details["client_user_message_id"] = json!("operation");
        outcome.native_input_id = None;
        assert!(dispatch_producer(&binding, &outcome, &snapshot, text).is_err());
    }

    #[test]
    fn older_observer_artifact_cannot_report_controller_admission() {
        assert!(!is_controller_route(&json!({"runtime":RUNTIME,
            "module_artifact_id":"codex-sdk-18194bf-bridge.1"})));
        assert!(!is_controller_route(&json!({"runtime":"another-runtime",
            "module_artifact_id":ARTIFACT_ID})));
    }
}
