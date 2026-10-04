//! Narrow contract shared by sessionless, one-shot executors.
//!
//! Operation identity is the controller's batch identity. A vendor session ID,
//! when a provider reports one, remains per-run evidence and never becomes a
//! binding root or turn. Native completion also never accepts a Task.

use crate::{
    error::{Error, Result},
    model,
    runtime::{EffectOutcome, RuntimeOutcome},
};
use serde_json::{Value, json};

pub const EXECUTION_SHAPE: &str = "sessionless_batch";
pub const COMMAND_RUNTIME: &str = "command";
pub const COMMAND_ARTIFACT_ID: &str = "command-mod-0.1.0-glue.4";
pub const COMMAND_PREVIOUS_ARTIFACT_ID: &str = "command-mod-0.1.0-glue.3";
pub const COMMAND_LEGACY_ARTIFACT_ID: &str = "command-mod-0.1.0-glue.2";
pub const BATCH_OUTPUTS: [&str; 3] = ["result.json", "thread.md", "thread.json"];

pub fn is_command_route(route: &Value) -> bool {
    route["runtime"] == COMMAND_RUNTIME
        && matches!(
            route["module_artifact_id"].as_str(),
            Some(COMMAND_ARTIFACT_ID | COMMAND_PREVIOUS_ARTIFACT_ID)
        )
}

pub fn is_legacy_command_route(route: &Value) -> bool {
    route["runtime"] == COMMAND_RUNTIME && route["module_artifact_id"] == COMMAND_LEGACY_ARTIFACT_ID
}

pub fn is_sessionless_route(route: &Value) -> bool {
    (route["runtime"] == crate::runtime::zed::RUNTIME
        && route["module_artifact_id"] == crate::runtime::zed::ARTIFACT_ID)
        || is_command_route(route)
        || is_legacy_command_route(route)
}

pub fn supports(route: &Value, method: &str) -> bool {
    if !is_sessionless_route(route) {
        return false;
    }
    if is_legacy_command_route(route) {
        return false;
    }
    match method {
        "agent.open" | "task.dispatch" | "agent.refresh" | "agent.reconcile" => true,
        "agent.result" => route["runtime"] == crate::runtime::zed::RUNTIME,
        _ => false,
    }
}

pub fn validate_command(route: &Value, method: &str, input: &Value) -> Result<()> {
    if !supports(route, method) {
        return Err(Error::new(
            "UNSUPPORTED_RUNTIME",
            "sessionless batch runtimes do not provide this operation",
        ));
    }
    if method == "task.dispatch" {
        model::text(input, "text")?;
        if let Some(snapshot) = input.get("task_snapshot")
            && !snapshot.is_object()
        {
            return Err(Error::invalid("immutable task snapshot must be an object"));
        }
    }
    if method == "agent.result" {
        let selector = input
            .get("selector")
            .ok_or_else(|| Error::invalid("batch output selector is required"))?;
        model::fields(selector, &["kind", "operation_id", "native_output"])?;
        if model::text(selector, "kind")? != "batch_output"
            || !BATCH_OUTPUTS.contains(&model::text(selector, "native_output")?)
        {
            return Err(Error::invalid(
                "batch_output selector must name an allowlisted native output",
            ));
        }
        model::text(selector, "operation_id")?;
    }
    if method == "agent.reconcile" {
        model::text(input, "operation_id")?;
    }
    Ok(())
}

/// Compose the exact dispatch text with the immutable Task snapshot, matching
/// the shared native instruction contract used by the session adapter.
pub fn instruction(input: &Value) -> Result<String> {
    let text = model::text(input, "text")?;
    let snapshot = input
        .get("task_snapshot")
        .filter(|value| value.is_object())
        .ok_or_else(|| Error::invalid("immutable task snapshot is required"))?;
    Ok(format!(
        "{text}\n\nELIOT immutable task snapshot:\n{}",
        model::canonical(snapshot)?
    ))
}

pub fn prompt_facts(instruction: &str, task_snapshot: &Value) -> Result<Value> {
    if !task_snapshot.is_object() {
        return Err(Error::invalid("immutable task snapshot is required"));
    }
    Ok(json!({
        "prompt_sha256": model::digest(instruction.as_bytes()),
        "prompt_bytes": instruction.len(),
        "task_snapshot_sha256": model::digest(model::canonical(task_snapshot)?.as_bytes())
    }))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandReceiptFacts {
    pub batch_run_id: String,
    pub prompt_sha256: String,
    pub prompt_bytes: usize,
}

/// Core-owned identity for a Command batch. The Operation ID is stable across
/// module reconnects; prompt facts use the shared canonical instruction.
pub fn command_receipt_facts(operation_id: &str, instruction: &str) -> CommandReceiptFacts {
    let operation_digest = model::digest(operation_id.as_bytes());
    CommandReceiptFacts {
        batch_run_id: format!("command-batch:{}", &operation_digest[..32]),
        prompt_sha256: model::digest(instruction.as_bytes()),
        prompt_bytes: instruction.len(),
    }
}

impl CommandReceiptFacts {
    pub fn as_json(&self) -> Value {
        json!({
            "batch_run_id":self.batch_run_id,
            "prompt_sha256":self.prompt_sha256,
            "prompt_bytes":self.prompt_bytes
        })
    }
}

fn native_identity_is_null(outcome: &RuntimeOutcome) -> bool {
    outcome.native_root_id.is_none()
        && outcome.native_scope_key.is_none()
        && outcome.turn_id.is_none()
        && outcome.native_input_id.is_none()
}

fn successful_terminal(details: &Value) -> bool {
    details["execution_shape"] == EXECUTION_SHAPE
        && details["completion_condition"] == "native_result_observed"
        && details["exit_code"] == 0
        && terminal_status(details) == Some("success")
}

fn command_successful_terminal(details: &Value) -> bool {
    successful_terminal(details)
        && details["signal"].is_null()
        && details["spawn_error_observed"] == false
        && details["timed_out"] == false
}

fn terminal_status(details: &Value) -> Option<&str> {
    let subtype = details["result_subtype"].as_str();
    let status = details["native_result"]["status"].as_str();
    match (subtype, status) {
        (Some("success"), Some("completed")) | (Some("completed"), Some("success")) => {
            Some("success")
        }
        (Some(left), Some(right)) if left == right => Some(left),
        (Some(_), Some(_)) => None,
        (Some(value), None) | (None, Some(value)) => match value {
            "completed" => Some("success"),
            _ => Some(value),
        },
        (None, None) => None,
    }
}

fn failed_terminal(details: &Value) -> bool {
    if details["execution_shape"] != EXECUTION_SHAPE
        || details["completion_condition"] != "native_result_observed"
    {
        return false;
    }
    let status = terminal_status(details);
    let exit_code = details["exit_code"].as_i64();
    let expected_exit = match status {
        Some("error") => Some(1),
        Some("timeout") => Some(2),
        Some("interrupted") => Some(3),
        Some("max_turns") => Some(8),
        _ => None,
    };
    expected_exit.is_some() && exit_code == expected_exit
}

fn command_failed_terminal(details: &Value) -> bool {
    if details["execution_shape"] != EXECUTION_SHAPE
        || details["completion_condition"] != "native_result_observed"
        || !details["signal"].is_null()
        || details["spawn_error_observed"] != false
        || details["timed_out"] != false
    {
        return false;
    }
    matches!(
        (terminal_status(details), details["exit_code"].as_i64()),
        (Some("error"), Some(1 | 3 | 4 | 5 | 6 | 7 | 9 | 10 | 130)) | (Some("max_turns"), Some(8))
    )
}

/// Validate the shared core envelope before it can settle a sessionless
/// Operation. Runtime-specific terminal evidence remains in `details`.
pub fn validate_outcome(route: &Value, method: &str, outcome: &RuntimeOutcome) -> Result<()> {
    if !is_sessionless_route(route) {
        return Ok(());
    }
    if !native_identity_is_null(outcome) {
        return Err(Error::invalid(
            "sessionless batch outcomes cannot claim a native root, scope, turn or input ID",
        ));
    }
    if outcome.details["execution_shape"] != EXECUTION_SHAPE {
        return Err(Error::invalid(
            "sessionless batch outcome requires its execution shape",
        ));
    }
    match method {
        "agent.open" => {
            let valid = match outcome.outcome {
                EffectOutcome::Applied => {
                    outcome.details["completion_condition"] == "executor_preflight_completed"
                        && outcome.details["native_session_state"] == "not_started"
                }
                EffectOutcome::Rejected => {
                    outcome.details["completion_condition"] == "executor_preflight_rejected"
                        && outcome.details["native_session_state"] == "not_started"
                }
                EffectOutcome::Unknown => true,
                EffectOutcome::Accepted => false,
            };
            if !valid {
                return Err(Error::invalid(
                    "batch open is only a rootless executor preflight",
                ));
            }
        }
        "task.dispatch" => {
            if model::text(&outcome.details, "batch_run_id").is_err() {
                return Err(Error::invalid(
                    "sessionless batch dispatch must preserve its stable run identity",
                ));
            }
            match outcome.outcome {
                EffectOutcome::Applied
                    if !(if route["runtime"] == COMMAND_RUNTIME {
                        command_successful_terminal(&outcome.details)
                    } else {
                        successful_terminal(&outcome.details)
                    }) =>
                {
                    return Err(Error::invalid(
                        "applied batch dispatch requires a successful result and exit code 0",
                    ));
                }
                EffectOutcome::Rejected
                    if !(if route["runtime"] == COMMAND_RUNTIME {
                        command_failed_terminal(&outcome.details)
                    } else {
                        failed_terminal(&outcome.details)
                    }) && !(outcome.details["completion_condition"]
                        == "executor_launch_rejected"
                        && outcome.details["native_session_state"] == "not_started") =>
                {
                    return Err(Error::invalid(
                        "rejected batch dispatch requires an observed native failure result",
                    ));
                }
                EffectOutcome::Unknown => {}
                EffectOutcome::Accepted => {
                    return Err(Error::invalid(
                        "sessionless batch dispatch has no asynchronous admission boundary",
                    ));
                }
                _ => {}
            }
        }
        "agent.refresh" | "agent.reconcile" => {
            if matches!(outcome.outcome, EffectOutcome::Applied)
                && !matches!(
                    outcome.details["completion_condition"].as_str(),
                    Some("batch_snapshot_readback" | "batch_readback_recorded")
                )
            {
                return Err(Error::invalid(
                    "batch readback must report a saved snapshot or reconciliation receipt",
                ));
            }
        }
        "agent.result" => {
            let valid = if matches!(outcome.outcome, EffectOutcome::Applied) {
                outcome.details["completion_condition"] == "batch_output_artifacts_selected"
                    && outcome.details["artifact_refs"]
                        .as_array()
                        .is_some_and(|refs| !refs.is_empty())
            } else if matches!(outcome.outcome, EffectOutcome::Rejected) {
                matches!(
                    outcome.details["completion_condition"].as_str(),
                    Some(
                        "batch_output_missing"
                            | "batch_output_range_invalid"
                            | "batch_output_unavailable"
                    )
                ) && outcome.details["artifact_refs"] == json!([])
            } else {
                false
            };
            if route["runtime"] != crate::runtime::zed::RUNTIME || !valid {
                return Err(Error::invalid(
                    "batch result must report selected retained artifacts or an absent output",
                ));
            }
        }
        _ => {
            return Err(Error::new(
                "UNSUPPORTED_RUNTIME",
                "sessionless batch runtimes do not provide this operation",
            ));
        }
    }
    Ok(())
}

pub fn dispatch_producer(outcome: &RuntimeOutcome) -> Value {
    let disposition = if matches!(outcome.outcome, EffectOutcome::Applied) {
        "completed"
    } else {
        "failed"
    };
    json!({
        "assignment_id": outcome.operation_id,
        "execution_shape": EXECUTION_SHAPE,
        "dispatch_operation_id": outcome.operation_id,
        "batch_run_id": outcome.details.get("batch_run_id").cloned().unwrap_or(Value::Null),
        "per_run_native_session_id": outcome.details.get("native_session_id").cloned().unwrap_or(Value::Null),
        "disposition": disposition,
        "terminal_evidence": {
            "completion_condition": outcome.details["completion_condition"],
            "exit_code": outcome.details.get("exit_code").cloned().unwrap_or(Value::Null),
            "result_subtype": outcome.details.get("result_subtype").cloned().unwrap_or(Value::Null),
            "result_sha256": outcome.details.get("result_sha256").cloned().unwrap_or(Value::Null)
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::{EffectOutcome, RuntimeOutcome};

    fn zed_route() -> Value {
        json!({"runtime":"zed","module_artifact_id":crate::runtime::zed::ARTIFACT_ID})
    }

    fn outcome(outcome: EffectOutcome, details: Value) -> RuntimeOutcome {
        RuntimeOutcome {
            operation_id: "op-1".into(),
            outcome,
            native_scope_key: None,
            native_root_id: None,
            turn_id: None,
            native_input_id: None,
            details,
        }
    }

    #[test]
    fn recognizes_only_the_exact_batch_artifacts_and_supported_methods() {
        assert!(is_sessionless_route(&zed_route()));
        assert!(supports(&zed_route(), "agent.result"));
        assert!(!supports(&zed_route(), "agent.send"));
        assert!(!is_sessionless_route(&json!({
            "runtime":"zed",
            "module_artifact_id":"unrecognized"
        })));
        assert!(supports(
            &json!({"runtime":"command","module_artifact_id":COMMAND_ARTIFACT_ID}),
            "agent.reconcile"
        ));
        assert!(!supports(
            &json!({"runtime":"command","module_artifact_id":COMMAND_ARTIFACT_ID}),
            "agent.result"
        ));
        assert!(supports(
            &json!({"runtime":"command","module_artifact_id":COMMAND_PREVIOUS_ARTIFACT_ID}),
            "agent.reconcile"
        ));
        let legacy_command = json!({
            "runtime":COMMAND_RUNTIME,
            "module_artifact_id":COMMAND_LEGACY_ARTIFACT_ID
        });
        assert!(is_sessionless_route(&legacy_command));
        assert!(!supports(&legacy_command, "task.dispatch"));
        assert!(
            validate_command(&legacy_command, "task.dispatch", &json!({"text":"old"})).is_err()
        );
    }

    #[test]
    fn command_receipt_identity_uses_stable_operation_and_canonical_unicode_prompt() {
        let snapshot = json!({"zeta":"🐇","alpha":{"text":"naïve"}});
        let input = json!({"text":"réponds 🐇","task_snapshot":snapshot});
        let instruction = instruction(&input).unwrap();
        assert_eq!(
            instruction,
            "réponds 🐇\n\nELIOT immutable task snapshot:\n{\"alpha\":{\"text\":\"naïve\"},\"zeta\":\"🐇\"}"
        );
        let first = command_receipt_facts("operation-雪", &instruction);
        let second = command_receipt_facts("operation-雪", &instruction);
        assert_eq!(first, second);
        assert_eq!(first.prompt_sha256, model::digest(instruction.as_bytes()));
        assert_eq!(first.prompt_bytes, instruction.as_bytes().len());
        assert!(first.batch_run_id.starts_with("command-batch:"));
        assert_eq!(first.batch_run_id.len(), "command-batch:".len() + 32);
    }

    #[test]
    fn rejects_a_batch_output_selector_with_a_path_or_unlisted_name() {
        assert!(validate_command(
            &zed_route(),
            "agent.result",
            &json!({"selector":{"kind":"batch_output","operation_id":"op-1","native_output":"result.json"}})
        )
        .is_ok());
        assert!(validate_command(
            &zed_route(),
            "agent.result",
            &json!({"selector":{"kind":"batch_output","operation_id":"op-1","native_output":"../../secret"}})
        )
        .is_err());
        assert!(validate_command(
            &zed_route(),
            "agent.result",
            &json!({"selector":{"kind":"batch_output","operation_id":"op-1","native_output":"result.json","path":"C:/secret"}})
        )
        .is_err());
    }

    #[test]
    fn requires_rootless_preflight_and_correlated_successful_terminal() {
        let preflight = outcome(
            EffectOutcome::Applied,
            json!({
                "execution_shape":EXECUTION_SHAPE,
                "completion_condition":"executor_preflight_completed",
                "native_session_state":"not_started"
            }),
        );
        assert!(validate_outcome(&zed_route(), "agent.open", &preflight).is_ok());
        let mut fake_session = preflight;
        fake_session.native_root_id = Some("session-1".into());
        assert!(validate_outcome(&zed_route(), "agent.open", &fake_session).is_err());

        let success = outcome(
            EffectOutcome::Applied,
            json!({
                "execution_shape":EXECUTION_SHAPE,
                "completion_condition":"native_result_observed",
                "batch_run_id":"op-1-run",
                "exit_code":0,
                "native_result":{"status":"completed"}
            }),
        );
        assert!(validate_outcome(&zed_route(), "task.dispatch", &success).is_ok());
        let inconsistent = outcome(
            EffectOutcome::Applied,
            json!({
                "execution_shape":EXECUTION_SHAPE,
                "completion_condition":"native_result_observed",
                "exit_code":1,
                "result_subtype":"success"
            }),
        );
        assert!(validate_outcome(&zed_route(), "task.dispatch", &inconsistent).is_err());
    }

    #[test]
    fn rejected_dispatch_requires_a_matching_native_terminal_pair() {
        let valid = outcome(
            EffectOutcome::Rejected,
            json!({
                "execution_shape":EXECUTION_SHAPE,
                "completion_condition":"native_result_observed",
                "batch_run_id":"op-run",
                "exit_code":1,
                "result_subtype":"error"
            }),
        );
        assert!(validate_outcome(&zed_route(), "task.dispatch", &valid).is_ok());
        let mismatched = outcome(
            EffectOutcome::Rejected,
            json!({
                "execution_shape":EXECUTION_SHAPE,
                "completion_condition":"native_result_observed",
                "batch_run_id":"op-run",
                "exit_code":0,
                "result_subtype":"error"
            }),
        );
        assert!(validate_outcome(&zed_route(), "task.dispatch", &mismatched).is_err());
        let no_identity = outcome(
            EffectOutcome::Unknown,
            json!({
                "execution_shape":EXECUTION_SHAPE,
                "completion_condition":"native_result_observed",
                "exit_code":1
            }),
        );
        assert!(validate_outcome(&zed_route(), "task.dispatch", &no_identity).is_err());

        let command_route = json!({
            "runtime":COMMAND_RUNTIME,
            "module_artifact_id":COMMAND_ARTIFACT_ID
        });
        let max_turns = outcome(
            EffectOutcome::Rejected,
            json!({
                "execution_shape":EXECUTION_SHAPE,
                "completion_condition":"native_result_observed",
                "batch_run_id":"op-run",
                "exit_code":8,
                "result_subtype":"max_turns",
                "signal":null,
                "spawn_error_observed":false,
                "timed_out":false
            }),
        );
        assert!(validate_outcome(&command_route, "task.dispatch", &max_turns).is_ok());
        let rewritten_exit = outcome(
            EffectOutcome::Rejected,
            json!({
                "execution_shape":EXECUTION_SHAPE,
                "completion_condition":"native_result_observed",
                "batch_run_id":"op-run",
                "exit_code":1,
                "result_subtype":"max_turns"
            }),
        );
        assert!(validate_outcome(&command_route, "task.dispatch", &rewritten_exit).is_err());
        let conflicting_status = outcome(
            EffectOutcome::Applied,
            json!({
                "execution_shape":EXECUTION_SHAPE,
                "completion_condition":"native_result_observed",
                "batch_run_id":"op-run",
                "exit_code":0,
                "result_subtype":"success",
                "native_result":{"status":"error"}
            }),
        );
        assert!(validate_outcome(&command_route, "task.dispatch", &conflicting_status).is_err());
    }

    #[test]
    fn command_terminal_failures_require_documented_matching_exit_and_clean_process_facts() {
        let route = json!({
            "runtime":COMMAND_RUNTIME,
            "module_artifact_id":COMMAND_ARTIFACT_ID
        });
        for exit_code in [1, 3, 4, 5, 6, 7, 9, 10, 130] {
            let rejected = outcome(
                EffectOutcome::Rejected,
                json!({
                    "execution_shape":EXECUTION_SHAPE,
                    "completion_condition":"native_result_observed",
                    "batch_run_id":"stable-run",
                    "result_subtype":"error",
                    "exit_code":exit_code,
                    "signal":null,
                    "spawn_error_observed":false,
                    "timed_out":false
                }),
            );
            assert!(validate_outcome(&route, "task.dispatch", &rejected).is_ok());
        }

        for invalid in [
            json!({
                "execution_shape":EXECUTION_SHAPE,
                "completion_condition":"native_result_observed",
                "batch_run_id":"stable-run",
                "result_subtype":"error",
                "exit_code":8,
                "signal":null,
                "spawn_error_observed":false,
                "timed_out":false
            }),
            json!({
                "execution_shape":EXECUTION_SHAPE,
                "completion_condition":"native_result_observed",
                "batch_run_id":"stable-run",
                "result_subtype":"error",
                "exit_code":3,
                "signal":"SIGTERM",
                "spawn_error_observed":false,
                "timed_out":false
            }),
            json!({
                "execution_shape":EXECUTION_SHAPE,
                "completion_condition":"native_result_observed",
                "batch_run_id":"stable-run",
                "result_subtype":"error",
                "exit_code":3,
                "signal":null,
                "spawn_error_observed":true,
                "timed_out":false
            }),
            json!({
                "execution_shape":EXECUTION_SHAPE,
                "completion_condition":"native_result_observed",
                "batch_run_id":"stable-run",
                "result_subtype":"error",
                "exit_code":3,
                "signal":null,
                "spawn_error_observed":false,
                "timed_out":true
            }),
        ] {
            assert!(
                validate_outcome(
                    &route,
                    "task.dispatch",
                    &outcome(EffectOutcome::Rejected, invalid)
                )
                .is_err()
            );
        }
    }

    #[test]
    fn user_dispatch_needs_text_before_host_injects_the_immutable_snapshot() {
        let request = json!({"text":"do the task"});
        assert!(validate_command(&zed_route(), "task.dispatch", &request).is_ok());
        assert!(instruction(&request).is_err());
        assert!(
            validate_command(
                &zed_route(),
                "task.dispatch",
                &json!({"text":"do the task","task_snapshot":"mutable"})
            )
            .is_err()
        );
    }

    #[test]
    fn batch_producer_keeps_operation_and_vendor_session_in_distinct_fields() {
        let out = outcome(
            EffectOutcome::Applied,
            json!({
                "execution_shape":EXECUTION_SHAPE,
                "completion_condition":"native_result_observed",
                "batch_run_id":"zed-run",
                "native_session_id":"vendor-session",
                "exit_code":0,
                "result_sha256":"abc"
            }),
        );
        let producer = dispatch_producer(&out);
        assert_eq!(producer["dispatch_operation_id"], "op-1");
        assert_eq!(producer["batch_run_id"], "zed-run");
        assert_eq!(producer["per_run_native_session_id"], "vendor-session");
        assert!(producer.get("native_session_id").is_none());
        assert!(producer.get("native_run_id").is_none());
    }
}
