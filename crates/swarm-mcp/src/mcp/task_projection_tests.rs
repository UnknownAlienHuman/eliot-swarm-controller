use super::{CancelAction, cancel_action, iso8601_utc, pending_input_requests, project_operation};
use crate::mcp::TASK_POLL_INTERVAL_MS;
use rmcp::model::{
    ElicitRequest, ElicitRequestParams, ElicitationSchema, InputRequest, InputRequests,
    TaskPayload, TaskStatus,
};
use serde_json::{Value, json};
fn operation(state: &str) -> Value {
    json!({
        "operation_id": "op-1",
        "caller_id": "operator",
        "method": "agent.send",
        "state": state,
        "task_id": null,
        "attempt_id": null,
        "binding_id": "b1",
        "binding_generation": 1,
        "result": null,
        "created_at_ms": 1_700_000_000_123i64,
        "updated_at_ms": 1_700_000_060_000i64,
    })
}

#[test]
fn iso8601_utc_formats_epoch_milliseconds() {
    assert_eq!(iso8601_utc(0), "1970-01-01T00:00:00.000Z");
    assert_eq!(iso8601_utc(1_700_000_000_123), "2023-11-14T22:13:20.123Z");
    // Leap day and a far-future date exercise the civil conversion.
    assert_eq!(iso8601_utc(1_709_164_800_000), "2024-02-29T00:00:00.000Z");
    assert_eq!(iso8601_utc(4_102_444_800_000), "2100-01-01T00:00:00.000Z");
    assert_eq!(iso8601_utc(-1), "1969-12-31T23:59:59.999Z");
}

#[test]
fn projection_maps_every_operation_state() {
    // In-flight states stay working; the exact ELIOT state is named in
    // the status message, and outcome_unknown is never dressed up as a
    // failure or a success.
    for state in ["queued", "sending", "native_accepted", "outcome_unknown"] {
        let detailed = project_operation(&operation(state), InputRequests::new());
        assert_eq!(detailed.status(), TaskStatus::Working, "{state}");
        assert!(matches!(detailed.payload, TaskPayload::Working));
        assert_eq!(detailed.task.task_id, "op-1");
        assert_eq!(detailed.task.created_at, "2023-11-14T22:13:20.123Z");
        assert_eq!(detailed.task.last_updated_at, "2023-11-14T22:14:20.000Z");
        assert_eq!(detailed.task.ttl_ms, None);
        assert_eq!(detailed.task.poll_interval_ms, Some(TASK_POLL_INTERVAL_MS));
        assert_eq!(
            detailed.task.status_message.as_deref(),
            Some(format!("agent.send operation is {state}").as_str())
        );
    }

    // Settled: completed, carrying the Operation's recorded result as
    // the tool result a non-Tasks client would read.
    let mut settled = operation("settled");
    settled["result"] = json!({"operation_id": "op-1", "state": "settled", "answer": 42});
    let detailed = project_operation(&settled, InputRequests::new());
    assert_eq!(detailed.status(), TaskStatus::Completed);
    let TaskPayload::Completed { result } = detailed.payload else {
        panic!("settled must complete");
    };
    assert_eq!(result["structuredContent"]["answer"], json!(42));
    assert_eq!(result["isError"], json!(false));

    // Rejected: failed, with the exact ELIOT error preserved in data.
    let mut rejected = operation("rejected");
    rejected["result"] = json!({"code": "BINDING_NOT_READY", "message": "not ready"});
    let detailed = project_operation(&rejected, InputRequests::new());
    assert_eq!(detailed.status(), TaskStatus::Failed);
    let TaskPayload::Failed { error } = detailed.payload else {
        panic!("rejected must fail");
    };
    assert_eq!(error["message"], json!("not ready"));
    assert_eq!(
        error["data"],
        json!({"code": "BINDING_NOT_READY", "message": "not ready"})
    );

    // Cancelled.
    let detailed = project_operation(&operation("cancelled"), InputRequests::new());
    assert_eq!(detailed.status(), TaskStatus::Cancelled);
    assert!(matches!(detailed.payload, TaskPayload::Cancelled));
}

#[test]
fn projection_prefers_pending_input_over_working() {
    fn one_request() -> InputRequests {
        let mut requests = InputRequests::new();
        requests.insert(
            "req-1".to_string(),
            InputRequest::Elicitation(ElicitRequest::new(
                ElicitRequestParams::FormElicitationParams {
                    meta: None,
                    message: "waiting".to_string(),
                    requested_schema: ElicitationSchema::new(Default::default()),
                },
            )),
        );
        requests
    }
    let detailed = project_operation(&operation("native_accepted"), one_request());
    assert_eq!(detailed.status(), TaskStatus::InputRequired);
    let TaskPayload::InputRequired { input_requests } = detailed.payload else {
        panic!("pending input must require input");
    };
    assert_eq!(input_requests.len(), 1);
    // Terminal states never report input, even if items were supplied.
    let detailed = project_operation(&operation("cancelled"), one_request());
    assert_eq!(detailed.status(), TaskStatus::Cancelled);
}

fn attention_item(kind: &str, binding: &str, generation: i64, request_id: &str) -> Value {
    json!({
        "kind": kind,
        "scope_key": "scope",
        "binding_id": binding,
        "generation": generation,
        "address": {
            "binding_id": binding,
            "generation": generation,
            "session_id": "ses_1",
            "request_id": request_id,
            "request_kind": "permission",
            "fingerprint": "fp-1",
        },
        "source": {"kind": "binding_observation", "observed_at_ms": 1, "stale": false},
        "suggested_action": {"method": "agent.reply"},
        "manager_actionable": true,
    })
}

#[test]
fn pending_inputs_mirror_exactly_the_operations_attention_items() {
    let op = operation("native_accepted");
    let items = vec![
        attention_item("waiting_for_native_request", "b1", 1, "req-1"),
        attention_item("waiting_for_native_request", "b1", 1, "req-2"),
        // Another binding's request is not this Operation's.
        attention_item("waiting_for_native_request", "b2", 1, "req-foreign"),
        // Another generation of the same binding is not this Operation's.
        attention_item("waiting_for_native_request", "b1", 2, "req-old-generation"),
        // Other attention kinds are not input requests.
        attention_item("input_queued_not_consumed", "b1", 1, "req-queued"),
        attention_item("waiting_for_child_result", "b1", 1, "req-child"),
    ];
    let requests = pending_input_requests(&op, &items);
    let keys: Vec<&String> = requests.keys().collect();
    assert_eq!(keys, [&"req-1".to_string(), &"req-2".to_string()]);
    // The wire shape is an elicitation carrying the exact native
    // address and the reply path; no form schema is invented.
    let wire = serde_json::to_value(&requests["req-1"]).unwrap();
    assert_eq!(wire["method"], json!("elicitation/create"));
    let message = wire["params"]["message"].as_str().unwrap();
    assert!(
        message.contains("permission request req-1 in session ses_1"),
        "{message}"
    );
    assert!(message.contains("fingerprint fp-1"), "{message}");
    assert!(message.contains("agent_reply"), "{message}");
    assert!(
        wire["params"]["requestedSchema"]["properties"]
            .as_object()
            .unwrap()
            .is_empty()
    );
    // An unbound Operation has no pending native input.
    let mut unbound = operation("queued");
    unbound["binding_id"] = Value::Null;
    unbound["binding_generation"] = Value::Null;
    assert!(pending_input_requests(&unbound, &items).is_empty());
}

#[test]
fn cancel_action_mirrors_operation_cancel() {
    for state in ["settled", "rejected", "cancelled"] {
        assert_eq!(cancel_action(state), CancelAction::Ack, "{state}");
    }
    assert_eq!(cancel_action("queued"), CancelAction::Submit);
    for state in [
        "sending",
        "native_accepted",
        "outcome_unknown",
        "anything-else",
    ] {
        assert_eq!(cancel_action(state), CancelAction::Refuse, "{state}");
    }
}
