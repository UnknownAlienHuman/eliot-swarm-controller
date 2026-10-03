//! Native session preparation and first-input identity adoption for the
//! pinned Claude Agent SDK bridge. The prepared process has no native session
//! identity until its first Task input produces the SDK's `system/init` frame.

use crate::{
    error::{Error, Result},
    model,
    runtime::{EffectOutcome, RuntimeOutcome},
};
use rusqlite::{Transaction, params};
use serde_json::{Value, json};

pub const CLAUDE_RUNTIME: &str = "claude";
pub const CLAUDE_ARTIFACT_ID: &str = "claude-agent-sdk-0.3.287-bridge.3";

pub fn is_prepared_claude_route(route: &Value) -> bool {
    route["runtime"] == CLAUDE_RUNTIME && route["module_artifact_id"] == CLAUDE_ARTIFACT_ID
}

fn nonempty<'a>(value: Option<&'a str>, field: &str) -> Result<&'a str> {
    value
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Error::invalid(format!("{field} is required")))
}

fn bridge_boot_id(binding: &Value) -> Result<&str> {
    nonempty(
        binding["observation"]["bridge_boot_id"].as_str(),
        "active Claude bridge boot id",
    )
}

/// Validate the exact rootless open receipt. The boot id is learned from the
/// already-authenticated module hello; the SDK's initialize control result is
/// deliberately not promoted into a native session id.
pub fn validate_prepared_open(binding: &Value, outcome: &RuntimeOutcome) -> Result<()> {
    let boot = bridge_boot_id(binding)?;
    if !is_prepared_claude_route(&binding["route"])
        || !matches!(outcome.outcome, EffectOutcome::Applied)
        || outcome.native_root_id.is_some()
        || outcome.native_scope_key.is_some()
        || outcome.turn_id.is_some()
        || outcome.native_input_id.is_some()
        || outcome.details["completion_condition"] != "native_executor_prepared"
        || outcome.details["native_session_state"] != "prepared"
        || outcome.details["bridge_boot_id"] != boot
        || !outcome.details["describe"]["session_id"].is_null()
    {
        return Err(Error::new(
            "NATIVE_IDENTITY_MISMATCH",
            "Claude open must prove the exact rootless prepared executor on this bridge boot",
        ));
    }
    Ok(())
}

fn validate_input_identity(binding: &Value, outcome: &RuntimeOutcome, initial: bool) -> Result<()> {
    let boot = bridge_boot_id(binding)?;
    let open = &binding["observation"]["opening_evidence"];
    let root = nonempty(
        outcome.native_root_id.as_deref(),
        "observed Claude session id",
    )?;
    let scope = nonempty(outcome.native_scope_key.as_deref(), "Claude session scope")?;
    let input = nonempty(outcome.native_input_id.as_deref(), "native input id")?;
    let rootless = binding["native_root_id"].is_null() && binding["native_scope_key"].is_null();
    let identity_matches = if initial {
        rootless
    } else {
        binding["native_root_id"] == root && binding["native_scope_key"] == scope
    };
    if !is_prepared_claude_route(&binding["route"])
        || binding["state"] != "ready"
        || !identity_matches
        || binding["observation"]["recovery_required"] == true
        || !matches!(outcome.outcome, EffectOutcome::Applied)
        || outcome.turn_id.is_some()
        || open["completion_condition"] != "native_executor_prepared"
        || open["native_session_state"] != "prepared"
        || open["bridge_boot_id"] != boot
        || outcome.details["bridge_boot_id"] != boot
        || outcome.details["completion_condition"] != "native_input_admitted"
        || outcome.details["initial_task_dispatch"] != initial
        || outcome.details["evidence"] != "native_frame_echo"
        || outcome.details["execution_complete"] != false
        || outcome.details["user_message_uuid"] != input
        || outcome.details["native_frame_session_id"] != root
        || (initial && outcome.details["system_init_session_id"] != root)
        || outcome.details["native_scope_key"] != scope
    {
        return Err(Error::new(
            "NATIVE_IDENTITY_MISMATCH",
            "first Claude Task input must be admitted by the prepared executor on its original boot",
        ));
    }
    Ok(())
}

/// Atomically adopt the actual native root and scope on the first confirmed
/// Task input. The existing partial unique index enforces one live controller
/// owner per native root; a conflicting root aborts this transaction.
pub fn adopt_first_input_identity(
    tx: &Transaction<'_>,
    binding: &Value,
    outcome: &RuntimeOutcome,
) -> Result<()> {
    validate_input_identity(binding, outcome, true)?;
    let boot = bridge_boot_id(binding)?;
    let root = outcome.native_root_id.as_deref().expect("validated root");
    let scope = outcome
        .native_scope_key
        .as_deref()
        .expect("validated scope");
    let changed = tx.execute(
        "UPDATE bindings SET native_root_id=?3,native_scope_key=?4,state_json=json_set(state_json,'$.first_dispatch_adoption',json(?6)) WHERE binding_id=?1 AND generation=?2 AND state='ready' AND native_root_id IS NULL AND native_scope_key IS NULL AND json_extract(route_json,'$.runtime')=?7 AND json_extract(route_json,'$.module_artifact_id')=?8 AND json_extract(state_json,'$.bridge_boot_id')=?5 AND json_extract(state_json,'$.opening_evidence.completion_condition')='native_executor_prepared' AND json_extract(state_json,'$.opening_evidence.bridge_boot_id')=?5 AND COALESCE(json_extract(state_json,'$.recovery_required'),0)=0",
        params![
            binding["binding_id"].as_str().unwrap_or_default(),
            binding["generation"].as_i64().unwrap_or_default(),
            root,
            scope,
            boot,
            model::canonical(&json!({
                "operation_id": outcome.operation_id,
                "native_root_id": root,
                "native_scope_key": scope,
                "native_input_id": outcome.native_input_id,
                "bridge_boot_id": boot
            }))?,
            CLAUDE_RUNTIME,
            CLAUDE_ARTIFACT_ID
        ],
    )?;
    if changed != 1 {
        return Err(Error::conflict(
            "prepared Claude binding changed before first-session adoption",
        ));
    }
    Ok(())
}

/// Build the initial Task producer only after checking the exact frozen Task
/// snapshot/text prompt and the SDK's actual session and input receipts.
pub fn dispatch_producer(
    binding: &Value,
    outcome: &RuntimeOutcome,
    task_snapshot: &Value,
    text: &str,
) -> Result<Value> {
    let initial = binding["native_root_id"].is_null() && binding["native_scope_key"].is_null();
    validate_input_identity(binding, outcome, initial)?;
    if !task_snapshot.is_object() || text.trim().is_empty() || outcome.operation_id.is_empty() {
        return Err(Error::invalid(
            "first Claude dispatch requires its exact Task snapshot, text and Operation id",
        ));
    }
    let snapshot = model::canonical(task_snapshot)?;
    let instruction = format!("Task specification: {snapshot}\n\n{text}");
    let input = outcome
        .native_input_id
        .as_deref()
        .expect("validated native input");
    let root = outcome.native_root_id.as_deref().expect("validated root");
    if outcome.details["prompt_sha256"] != model::digest(instruction.as_bytes())
        || outcome.details["prompt_bytes"].as_u64() != Some(instruction.len() as u64)
        || outcome.details["task_snapshot_sha256"] != model::digest(snapshot.as_bytes())
    {
        return Err(Error::new(
            "NATIVE_IDENTITY_MISMATCH",
            "Claude input receipt does not bind the exact frozen Task prompt",
        ));
    }
    Ok(json!({
        "assignment_id": outcome.operation_id,
        "native_session_id": root,
        "native_run_id": Value::Null,
        "native_input_id": input,
        "client_user_message_id": input,
        "native_scope_key": outcome.native_scope_key,
        "bridge_boot_id": outcome.details["bridge_boot_id"],
        "admission_kind": "claude_native_input",
        "disposition": "admitted"
    }))
}

/// Validate an ordinary next-turn send on an already adopted prepared
/// Claude session. Unlike the initial Task dispatch, it has no Task snapshot;
/// the input receipt must bind the exact request text and explicitly carry a
/// null snapshot digest.
pub fn validate_send_receipt(binding: &Value, outcome: &RuntimeOutcome, text: &str) -> Result<()> {
    validate_input_identity(binding, outcome, false)?;
    if text.trim().is_empty()
        || outcome.details["prompt_sha256"] != model::digest(text.as_bytes())
        || outcome.details["prompt_bytes"].as_u64() != Some(text.len() as u64)
        || outcome
            .details
            .get("task_snapshot_sha256")
            .is_none_or(|digest| !digest.is_null())
    {
        return Err(Error::new(
            "NATIVE_IDENTITY_MISMATCH",
            "Claude next-turn receipt must bind the exact request text without a Task snapshot",
        ));
    }
    Ok(())
}

fn sdk_result_terminal_status(event: &Value) -> Option<&'static str> {
    let subtype = event["result_subtype"].as_str()?;
    let is_error = event["is_error"].as_bool()?;
    match subtype {
        "success" => Some(if is_error { "failed" } else { "completed" }),
        "error_during_execution"
        | "error_max_turns"
        | "error_max_budget_usd"
        | "error_max_structured_output_retries" => Some("failed"),
        _ => None,
    }
}

/// Apply one uniquely correlated SDK terminal result to a Claude producer.
/// A reused native turn ID is never manufactured; ambiguous/missing input or
/// result links leave the producer admitted and unresolved.
pub fn apply_input_execution(producer: &mut Value, state: &Value, observation_id: Option<i64>) {
    let Some(observation_id) = observation_id.filter(|id| *id > 0) else {
        return;
    };
    if producer["admission_kind"] != "claude_native_input"
        || producer["native_run_id"].as_str().is_some()
        || matches!(
            producer["disposition"].as_str(),
            Some("completed" | "failed" | "cancelled")
        )
    {
        return;
    }
    let (Some(input), Some(session), Some(scope), Some(boot)) = (
        producer["native_input_id"]
            .as_str()
            .filter(|value| !value.is_empty()),
        producer["native_session_id"]
            .as_str()
            .filter(|value| !value.is_empty()),
        producer["native_scope_key"]
            .as_str()
            .filter(|value| !value.is_empty()),
        producer["bridge_boot_id"]
            .as_str()
            .filter(|value| !value.is_empty()),
    ) else {
        return;
    };
    if state["native_root_id"] != session
        || state["native_scope_key"] != scope
        || state["boot_id"] != boot
    {
        return;
    }
    let Some(executions) = state["input_executions"].as_array() else {
        return;
    };
    let matches: Vec<&Value> = executions
        .iter()
        .filter(|event| {
            event["native_input_id"] == input
                && event["native_session_id"] == session
                && event["native_scope_key"] == scope
                && event["bridge_boot_id"] == boot
        })
        .collect();
    let [event] = matches.as_slice() else {
        return;
    };
    let terminal = event["terminal_status"].as_str();
    let result_frame = event["result_frame_uuid"]
        .as_str()
        .filter(|value| !value.is_empty());
    let output_sha256 = event["result_sha256"]
        .as_str()
        .filter(|value| !value.is_empty());
    let output_bytes = event["result_bytes"].as_u64();
    let result_index = event["result_index"].as_u64();
    let model = event["effective_model"]
        .as_str()
        .filter(|value| !value.is_empty());
    let result_subtype = event["result_subtype"]
        .as_str()
        .filter(|value| !value.is_empty());
    let expected_terminal = sdk_result_terminal_status(event);
    let user_ids = event["user_message_uuids"].as_array();
    let result_frame_occurrences = result_frame.map_or(0, |frame| {
        executions
            .iter()
            .filter(|candidate| candidate["result_frame_uuid"] == frame)
            .count()
    });
    let valid_output_digest = output_sha256.is_some_and(|digest| {
        digest.len() == 64
            && digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    });
    if event["correlation"] != "unique"
        || user_ids.is_none_or(|ids| ids.len() != 1 || ids[0] != input)
        || result_frame.is_none()
        || result_frame_occurrences != 1
        || result_index.is_none()
        || model.is_none()
        || result_subtype.is_none()
        || expected_terminal.is_none()
        || expected_terminal != terminal
        || !matches!(terminal, Some("completed" | "failed"))
        || (terminal == Some("completed")
            && (result_subtype != Some("success")
                || !valid_output_digest
                || output_bytes.is_none()))
    {
        return;
    }
    let terminal_disposition = json!(terminal);
    let terminal_evidence = json!({
        "source":"claude_sdk_result",
        "observation_id":observation_id,
        "result_frame_uuid":result_frame,
        "result_index":event["result_index"],
        "native_input_id":input,
        "native_session_id":session,
        "native_scope_key":scope,
        "bridge_boot_id":boot,
        "result_subtype":result_subtype,
        "effective_model":model,
        "result_sha256":output_sha256,
        "result_bytes":output_bytes,
        "stop_reason":event["stop_reason"]
    });
    producer["disposition"] = terminal_disposition;
    producer["terminal_evidence"] = terminal_evidence;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding() -> Value {
        json!({
            "state":"ready",
            "binding_id":"binding-1",
            "generation":1,
            "native_root_id":null,
            "native_scope_key":null,
            "route":{"runtime":CLAUDE_RUNTIME,"module_artifact_id":CLAUDE_ARTIFACT_ID},
            "observation":{
                "bridge_boot_id":"boot-1",
                "opening_evidence":{
                    "completion_condition":"native_executor_prepared",
                    "native_session_state":"prepared",
                    "bridge_boot_id":"boot-1"
                }
            }
        })
    }

    fn open_outcome(binding: &Value) -> RuntimeOutcome {
        RuntimeOutcome {
            operation_id: "open-1".into(),
            outcome: EffectOutcome::Applied,
            native_scope_key: None,
            native_root_id: None,
            turn_id: None,
            native_input_id: None,
            details: json!({
                "completion_condition":"native_executor_prepared",
                "native_session_state":"prepared",
                "bridge_boot_id":binding["observation"]["bridge_boot_id"],
                "describe":{"session_id":null}
            }),
        }
    }

    fn initial_dispatch(binding: &Value) -> (RuntimeOutcome, Value, String) {
        let snapshot = json!({"title":"frozen Task","metadata":{"version":1}});
        let text = "execute this Task".to_owned();
        let prompt = format!(
            "Task specification: {}\n\n{}",
            model::canonical(&snapshot).unwrap(),
            text
        );
        let input_id = "d34d0011-1111-4111-8111-111111111111";
        (
            RuntimeOutcome {
                operation_id: "dispatch-1".into(),
                outcome: EffectOutcome::Applied,
                native_scope_key: Some("claude:/fixture/config".into()),
                native_root_id: Some("actual-sdk-session".into()),
                turn_id: None,
                native_input_id: Some(input_id.into()),
                details: json!({
                    "completion_condition":"native_input_admitted",
                    "initial_task_dispatch":true,
                    "evidence":"native_frame_echo",
                    "execution_complete":false,
                    "user_message_uuid":input_id,
                    "bridge_boot_id":binding["observation"]["bridge_boot_id"],
                    "system_init_session_id":"actual-sdk-session",
                    "native_frame_session_id":"actual-sdk-session",
                    "native_scope_key":"claude:/fixture/config",
                    "prompt_sha256":model::digest(prompt.as_bytes()),
                    "prompt_bytes":prompt.len(),
                    "task_snapshot_sha256":model::digest(model::canonical(&snapshot).unwrap().as_bytes())
                }),
            },
            snapshot,
            text,
        )
    }

    #[test]
    fn prepared_open_is_rootless_and_pins_the_live_boot() {
        let binding = binding();
        let open = open_outcome(&binding);
        assert!(validate_prepared_open(&binding, &open).is_ok());
        let mut wrong_boot = open;
        wrong_boot.details["bridge_boot_id"] = json!("another-boot");
        assert!(validate_prepared_open(&binding, &wrong_boot).is_err());
    }

    #[test]
    fn initial_producer_requires_actual_session_echo_and_exact_task_prompt() {
        let binding = binding();
        let (outcome, snapshot, text) = initial_dispatch(&binding);
        let producer = dispatch_producer(&binding, &outcome, &snapshot, &text).unwrap();
        assert_eq!(producer["native_session_id"], "actual-sdk-session");
        assert_eq!(
            producer["native_input_id"],
            outcome.native_input_id.as_deref().unwrap()
        );
        assert!(producer["native_run_id"].is_null());

        let changed_text = dispatch_producer(&binding, &outcome, &snapshot, "different text");
        assert!(changed_text.is_err());
        let mut wrong_root = outcome;
        wrong_root.details["system_init_session_id"] = json!("requested-not-observed");
        assert!(dispatch_producer(&binding, &wrong_root, &snapshot, &text).is_err());
    }

    #[test]
    fn subsequent_dispatch_requires_same_native_root_prompt_and_unique_terminal_echo() {
        let mut binding = binding();
        binding["native_root_id"] = json!("actual-sdk-session");
        binding["native_scope_key"] = json!("claude:/fixture/config");
        let (mut outcome, snapshot, text) = initial_dispatch(&binding);
        outcome.operation_id = "dispatch-2".into();
        outcome.details["initial_task_dispatch"] = json!(false);
        outcome.details["system_init_session_id"] = Value::Null;
        let producer = dispatch_producer(&binding, &outcome, &snapshot, &text).unwrap();
        assert_eq!(producer["native_session_id"], binding["native_root_id"]);
        assert_eq!(
            producer["native_input_id"],
            outcome.native_input_id.as_deref().unwrap()
        );
        assert!(producer["native_run_id"].is_null());

        let mut wrong_prompt = outcome;
        wrong_prompt.details["prompt_sha256"] = json!("wrong-prompt");
        assert!(dispatch_producer(&binding, &wrong_prompt, &snapshot, &text).is_err());

        let (mut outcome, snapshot, text) = initial_dispatch(&binding);
        outcome.details["initial_task_dispatch"] = json!(false);
        outcome.details["system_init_session_id"] = Value::Null;
        outcome.native_root_id = Some("another-session".into());
        outcome.details["native_frame_session_id"] = json!("another-session");
        assert!(dispatch_producer(&binding, &outcome, &snapshot, &text).is_err());

        let (mut first_outcome, snapshot, text) = initial_dispatch(&binding);
        first_outcome.details["initial_task_dispatch"] = json!(false);
        first_outcome.details["system_init_session_id"] = Value::Null;
        let first_input = first_outcome.native_input_id.as_deref().unwrap();
        let (mut second_outcome, _, _) = initial_dispatch(&binding);
        second_outcome.operation_id = "dispatch-3".into();
        second_outcome.details["initial_task_dispatch"] = json!(false);
        second_outcome.details["system_init_session_id"] = Value::Null;
        second_outcome.native_input_id = Some("d34d0022-2222-4222-8222-222222222222".into());
        second_outcome.details["user_message_uuid"] = json!("d34d0022-2222-4222-8222-222222222222");
        let second_input = second_outcome.native_input_id.as_deref().unwrap();
        let mut first_producer =
            dispatch_producer(&binding, &first_outcome, &snapshot, &text).unwrap();
        let mut second_producer =
            dispatch_producer(&binding, &second_outcome, &snapshot, &text).unwrap();
        let mut state = json!({
            "native_root_id":"actual-sdk-session",
            "native_scope_key":"claude:/fixture/config",
            "boot_id":"boot-1",
            "input_executions":[{
                "native_input_id":first_input,
                "native_session_id":"actual-sdk-session",
                "native_scope_key":"claude:/fixture/config",
                "bridge_boot_id":"boot-1",
                "correlation":"unique",
                "user_message_uuids":[first_input],
                "result_frame_uuid":"native-result-frame-1",
                "result_index":0,
                "result_subtype":"success",
                "terminal_status":"completed",
                "is_error":false,
                "effective_model":"claude-sonnet-5",
                "result_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "result_bytes":17,
                "stop_reason":"end_turn"
            },{
                "native_input_id":second_input,
                "native_session_id":"actual-sdk-session",
                "native_scope_key":"claude:/fixture/config",
                "bridge_boot_id":"boot-1",
                "correlation":"unique",
                "user_message_uuids":[second_input],
                "result_frame_uuid":"native-result-frame-2",
                "result_index":1,
                "result_subtype":"success",
                "terminal_status":"completed",
                "is_error":false,
                "effective_model":"claude-opus-5",
                "result_sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "result_bytes":18,
                "stop_reason":"end_turn"
            }]
        });
        apply_input_execution(&mut first_producer, &state, Some(42));
        apply_input_execution(&mut second_producer, &state, Some(42));
        assert_eq!(first_producer["disposition"], "completed");
        assert_eq!(
            first_producer["terminal_evidence"]["result_frame_uuid"],
            "native-result-frame-1"
        );
        assert_eq!(
            first_producer["terminal_evidence"]["effective_model"],
            "claude-sonnet-5"
        );
        assert_eq!(
            first_producer["terminal_evidence"]["result_sha256"],
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        assert_eq!(second_producer["disposition"], "completed");
        assert_eq!(
            second_producer["terminal_evidence"]["result_frame_uuid"],
            "native-result-frame-2"
        );
        assert_eq!(
            second_producer["terminal_evidence"]["effective_model"],
            "claude-opus-5"
        );
        assert_eq!(
            second_producer["terminal_evidence"]["result_sha256"],
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        );

        let valid_state = state.clone();
        let mut ambiguous = dispatch_producer(&binding, &first_outcome, &snapshot, &text).unwrap();
        state["input_executions"][0]["correlation"] = json!("ambiguous_multi_input");
        apply_input_execution(&mut ambiguous, &state, Some(43));
        assert_eq!(ambiguous["disposition"], "admitted");
        state["input_executions"][0]["correlation"] = json!("unique");
        state["boot_id"] = json!("new-boot");
        apply_input_execution(&mut ambiguous, &state, Some(44));
        assert_eq!(ambiguous["disposition"], "admitted");

        state["boot_id"] = json!("boot-1");
        state["input_executions"][0]["native_session_id"] = json!("different-session");
        apply_input_execution(&mut ambiguous, &state, Some(45));
        assert_eq!(ambiguous["disposition"], "admitted");
        state["input_executions"][0]["native_session_id"] = json!("actual-sdk-session");
        let duplicate = state["input_executions"][0].clone();
        state["input_executions"]
            .as_array_mut()
            .unwrap()
            .push(duplicate);
        apply_input_execution(&mut ambiguous, &state, Some(46));
        assert_eq!(
            ambiguous["disposition"], "admitted",
            "duplicate terminal evidence is ambiguous"
        );

        let mut no_observation =
            dispatch_producer(&binding, &first_outcome, &snapshot, &text).unwrap();
        apply_input_execution(&mut no_observation, &valid_state, None);
        assert_eq!(no_observation["disposition"], "admitted");
        let mut zero_observation =
            dispatch_producer(&binding, &first_outcome, &snapshot, &text).unwrap();
        apply_input_execution(&mut zero_observation, &valid_state, Some(0));
        assert_eq!(zero_observation["disposition"], "admitted");

        let mut bad_digest_state = valid_state.clone();
        bad_digest_state["input_executions"][0]["result_sha256"] = json!("not-a-sha256");
        let mut bad_digest = dispatch_producer(&binding, &first_outcome, &snapshot, &text).unwrap();
        apply_input_execution(&mut bad_digest, &bad_digest_state, Some(47));
        assert_eq!(bad_digest["disposition"], "admitted");

        let mut missing_index_state = valid_state.clone();
        missing_index_state["input_executions"][0]["result_index"] = Value::Null;
        let mut missing_index =
            dispatch_producer(&binding, &first_outcome, &snapshot, &text).unwrap();
        apply_input_execution(&mut missing_index, &missing_index_state, Some(48));
        assert_eq!(missing_index["disposition"], "admitted");

        let mut colliding_frame_state = valid_state;
        colliding_frame_state["input_executions"][0]["result_frame_uuid"] =
            json!("reused-result-frame");
        colliding_frame_state["input_executions"][1]["result_frame_uuid"] =
            json!("reused-result-frame");
        let mut colliding_frame =
            dispatch_producer(&binding, &first_outcome, &snapshot, &text).unwrap();
        apply_input_execution(&mut colliding_frame, &colliding_frame_state, Some(49));
        assert_eq!(colliding_frame["disposition"], "admitted");
    }

    #[test]
    fn only_documented_terminal_result_subtypes_settle_input_execution() {
        let mut binding = binding();
        binding["native_root_id"] = json!("actual-sdk-session");
        binding["native_scope_key"] = json!("claude:/fixture/config");
        let (mut outcome, snapshot, text) = initial_dispatch(&binding);
        outcome.details["initial_task_dispatch"] = json!(false);
        outcome.details["system_init_session_id"] = Value::Null;
        let input = outcome.native_input_id.clone().unwrap();
        let fixture_state = |subtype: &str, terminal: &str, is_error: bool| {
            json!({
                "native_root_id":"actual-sdk-session",
                "native_scope_key":"claude:/fixture/config",
                "boot_id":"boot-1",
                "input_executions":[{
                    "native_input_id":input,
                    "native_session_id":"actual-sdk-session",
                    "native_scope_key":"claude:/fixture/config",
                    "bridge_boot_id":"boot-1",
                    "correlation":"unique",
                    "user_message_uuids":[input],
                    "result_frame_uuid":"result-frame",
                    "result_index":0,
                    "effective_model":"claude-sonnet-fixture",
                    "result_subtype":subtype,
                    "terminal_status":terminal,
                    "is_error":is_error,
                    "result_sha256":model::digest(b"reply"),
                    "result_bytes":5
                }]
            })
        };

        for (subtype, terminal, is_error, expected) in [
            ("success", "completed", false, "completed"),
            ("success", "failed", true, "failed"),
            ("error_during_execution", "failed", true, "failed"),
            ("error_max_turns", "failed", true, "failed"),
            ("error_max_budget_usd", "failed", true, "failed"),
            (
                "error_max_structured_output_retries",
                "failed",
                true,
                "failed",
            ),
        ] {
            let state = fixture_state(subtype, terminal, is_error);
            let mut producer = dispatch_producer(&binding, &outcome, &snapshot, &text).unwrap();
            apply_input_execution(&mut producer, &state, Some(50));
            assert_eq!(
                producer["disposition"], expected,
                "{subtype}/{terminal}/{is_error}"
            );
        }

        for (subtype, terminal, is_error) in [
            ("error_future_sdk_reason", "failed", true),
            ("error_max_turns", "completed", true),
            ("success", "completed", true),
            ("success", "failed", false),
        ] {
            let state = json!({
                "native_root_id":"actual-sdk-session",
                "native_scope_key":"claude:/fixture/config",
                "boot_id":"boot-1",
                "input_executions":[{
                    "native_input_id":input,
                    "native_session_id":"actual-sdk-session",
                    "native_scope_key":"claude:/fixture/config",
                    "bridge_boot_id":"boot-1",
                    "correlation":"unique",
                    "user_message_uuids":[input],
                    "result_frame_uuid":"result-frame",
                    "result_index":0,
                    "effective_model":"claude-sonnet-fixture",
                    "result_subtype":subtype,
                    "terminal_status":terminal,
                    "is_error":is_error,
                    "result_sha256":model::digest(b"reply"),
                    "result_bytes":5
                }]
            });
            let mut producer = dispatch_producer(&binding, &outcome, &snapshot, &text).unwrap();
            apply_input_execution(&mut producer, &state, Some(50));
            assert_eq!(
                producer["disposition"], "admitted",
                "{subtype}/{terminal}/{is_error}"
            );
        }

        let mut malformed_error_flag = fixture_state("success", "completed", false);
        malformed_error_flag["input_executions"][0]
            .as_object_mut()
            .unwrap()
            .remove("is_error");
        let mut producer = dispatch_producer(&binding, &outcome, &snapshot, &text).unwrap();
        apply_input_execution(&mut producer, &malformed_error_flag, Some(51));
        assert_eq!(
            producer["disposition"], "admitted",
            "missing SDK is_error flag stays unresolved"
        );
    }
}
