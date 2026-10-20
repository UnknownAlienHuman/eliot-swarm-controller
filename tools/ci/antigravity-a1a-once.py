from pathlib import Path
import subprocess


def remove_between(path: str, start: str, end: str) -> None:
    file = Path(path)
    text = file.read_text(encoding="utf-8")
    if text.count(start) != 1 or text.count(end) != 1:
        raise SystemExit(f"{path}: removal markers changed: {start!r} / {end!r}")
    begin = text.index(start)
    finish = text.index(end, begin)
    file.write_text(text[:begin] + text[finish:], encoding="utf-8", newline="")


controller_path = Path("modules/antigravity-rust/src/controller.rs")
controller = controller_path.read_text(encoding="utf-8")
replacements = [
    ("    local_execution_results: VecDeque<serde_json::Value>,\n", ""),
    ("            local_execution_results: VecDeque::new(),\n", ""),
    (
        "                \"artifact v5 requires the Store-produced TaskPrompt and dispatch admission schemas\",",
        "                \"artifact v6 requires the Store-produced TaskPrompt and dispatch admission schemas\",",
    ),
    (
        "        if let Some(receipt) = outcome.details.get(\"local_execution_ref\") {\n"
        "            if self.local_execution_results.len() == crate::stream::MAX_TURNS {\n"
        "                self.local_execution_results.pop_front();\n"
        "            }\n"
        "            self.local_execution_results.push_back(receipt.clone());\n"
        "        }\n",
        "",
    ),
    (
        "            object.insert(\n"
        "                \"local_execution_results\".to_owned(),\n"
        "                json!(self.local_execution_results),\n"
        "            );\n",
        "",
    ),
    (
        "        self.remove_local_execution_result(&outcome.operation_id);\n"
        "        self.pending_observation = None;\n",
        "",
    ),
    (
        "        self.remove_local_execution_result(operation_id);\n"
        "        self.pending_observation = None;\n",
        "",
    ),
]
for old, new in replacements:
    count = controller.count(old)
    if count != 1:
        raise SystemExit(f"controller replacement expected once, found {count}: {old!r}")
    controller = controller.replace(old, new)
controller_path.write_text(controller, encoding="utf-8", newline="")

remove_between(
    "modules/antigravity-rust/src/controller.rs",
    "    pub fn receipt_needs_observation(value: &serde_json::Value) -> bool {",
    "    /// Record a deterministic rejection for a manager-admitted but unsupported",
)
remove_between(
    "modules/antigravity-rust/src/controller.rs",
    "    pub fn bind_observation_id(\n",
    "    pub fn remember_acknowledged_outcome(&mut self, outcome: &RuntimeOutcome) {",
)
remove_between(
    "modules/antigravity-rust/src/controller.rs",
    "    fn replace_pending_outcome(&mut self, outcome: &RuntimeOutcome) -> Result<()> {",
    "    pub fn acknowledge_outcome(&mut self, operation_id: &str) {",
)
remove_between(
    "modules/antigravity-rust/src/controller.rs",
    "    fn remove_local_execution_result(&mut self, operation_id: &str) {",
    "    pub fn snapshot(&self) -> serde_json::Value {",
)

main_path = Path("modules/antigravity-rust/src/main.rs")
main = main_path.read_text(encoding="utf-8")
old_import = "    runtime::{RuntimeCommand, RuntimeOutcome},\n"
if main.count(old_import) != 1:
    raise SystemExit("main RuntimeOutcome import changed")
main = main.replace(old_import, "    runtime::RuntimeCommand,\n")
start = main.index("    for mut value in controller.pending_receipts() {")
response = main.index("        let response = host\n", start)
new_prefix = (
    "    for value in controller.pending_receipts() {\n"
    "        let operation_id = value\n"
    "            .get(\"operation_id\")\n"
    "            .and_then(Value::as_str)\n"
    "            .ok_or_else(|| {\n"
    "                Error::new(\"MODULE_OUTCOME_INVALID\", \"retained Operation ID is missing\")\n"
    "            })?\n"
    "            .to_owned();\n"
)
main = main[:start] + new_prefix + main[response:]
main_path.write_text(main, encoding="utf-8", newline="")
remove_between(
    "modules/antigravity-rust/src/main.rs",
    "async fn record_observation_for_receipt(\n",
    "fn observation_params(\n",
)

runtime_path = Path("crates/swarm-kernel-host/src/store/runtime.rs")
runtime = runtime_path.read_text(encoding="utf-8")
start_marker = "    // Warm CLI results have no native turn or inbox identity. Validate their\n"
end_marker = "    if o[\"method\"] == \"agent.result\"\n"
if runtime.count(start_marker) != 1:
    raise SystemExit("warm producer block start changed")
start = runtime.index(start_marker)
end = runtime.index(end_marker, start)
replacement = '''    // Warm CLI results have no native turn or inbox identity. Validate the
    // self-contained terminal fingerprint against this exact Operation,
    // binding generation and current authenticated adapter boot.
    let warm_producer = if crate::runtime::warm_stream::is_route(&b["route"])
        && matches!(o["method"].as_str(), Some("task.dispatch" | "agent.send"))
    {
        crate::runtime::warm_stream::validate_outcome(
            crate::runtime::warm_stream::OutcomeContext {
                binding: &b,
                operation: &o,
                outcome: &r,
            },
        )?
    } else {
        None
    };
'''
runtime = runtime[:start] + replacement + runtime[end:]
runtime_path.write_text(runtime, encoding="utf-8", newline="")

warm_stream = r'''//! Receipt validation for Antigravity warm-stream adapters.
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
            "bridge_boot_id":active_boot_id,
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
        })
        .unwrap()
        .unwrap();
        assert_eq!(producer["disposition"], "completed");
        assert_eq!(producer["local_execution_ref"]["result_ordinal"], 7);
        assert!(producer["local_execution_ref"].get("observation_id").is_none());
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
            })
            .unwrap()
            .unwrap();
            assert_eq!(producer["disposition"], disposition);
        }

        let (binding, operation, outcome) = fixture("WAITING", EffectOutcome::Applied);
        assert!(validate_outcome(OutcomeContext {
            binding: &binding,
            operation: &operation,
            outcome: &outcome,
        })
        .is_err());
    }

    #[test]
    fn current_boot_root_and_scope_are_required() {
        let (binding, operation, mut outcome) = fixture("SUCCESS", EffectOutcome::Applied);
        outcome.details["local_execution_ref"]["bridge_boot_id"] = json!("old-boot");
        assert!(validate_outcome(OutcomeContext {
            binding: &binding,
            operation: &operation,
            outcome: &outcome,
        })
        .is_err());

        let (binding, operation, mut outcome) = fixture("SUCCESS", EffectOutcome::Applied);
        outcome.native_root_id = Some("other".into());
        assert!(validate_outcome(OutcomeContext {
            binding: &binding,
            operation: &operation,
            outcome: &outcome,
        })
        .is_err());
    }

    #[test]
    fn legacy_observation_field_is_ignored_during_finite_transition() {
        let (binding, operation, mut outcome) = fixture("SUCCESS", EffectOutcome::Applied);
        outcome.details["local_execution_ref"]["observation_id"] = json!(41);
        assert!(validate_outcome(OutcomeContext {
            binding: &binding,
            operation: &operation,
            outcome: &outcome,
        })
        .unwrap()
        .is_some());
    }

    #[test]
    fn send_is_validated_without_creating_an_attempt_producer() {
        let (binding, mut operation, outcome) = fixture("SUCCESS", EffectOutcome::Applied);
        operation["method"] = json!("agent.send");
        assert!(validate_outcome(OutcomeContext {
            binding: &binding,
            operation: &operation,
            outcome: &outcome,
        })
        .unwrap()
        .is_none());
    }
}
'''
Path("crates/swarm-kernel-host/src/runtime/warm_stream.rs").write_text(
    warm_stream, encoding="utf-8", newline=""
)

production = [
    Path("modules/antigravity-rust/src/controller.rs").read_text(encoding="utf-8"),
    Path("modules/antigravity-rust/src/main.rs").read_text(encoding="utf-8"),
    Path("crates/swarm-kernel-host/src/runtime/warm_stream.rs").read_text(encoding="utf-8"),
    Path("crates/swarm-kernel-host/src/store/runtime.rs").read_text(encoding="utf-8"),
]
joined = "\n".join(production)
for forbidden in [
    "receipt_needs_observation",
    "record_observation_for_receipt",
    "local_execution_results",
    "bind_observation_id",
    "warm-stream receipt requires the exact materialized observation",
]:
    if forbidden in joined:
        raise SystemExit(f"forbidden observation coupling remains: {forbidden}")

changed = set(subprocess.check_output(["git", "diff", "--name-only"], text=True).splitlines())
expected = {
    "modules/antigravity-rust/src/controller.rs",
    "modules/antigravity-rust/src/main.rs",
    "crates/swarm-kernel-host/src/runtime/warm_stream.rs",
    "crates/swarm-kernel-host/src/store/runtime.rs",
}
if changed != expected:
    raise SystemExit(f"unexpected changed files: {sorted(changed)}")
