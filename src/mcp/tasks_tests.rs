//! S7 (R20 §17.1): MCP Tasks projection of Operations.
//!
//! Unit tests cover the pure projection (Operation record →
//! `DetailedTask`), the pending-input filter and the cancel decision;
//! integration tests run the real stack — StoreOwner, IPC listener and
//! facade — behind a real RMCP client over a duplex transport, with
//! and without the tasks extension declared.

use super::*;
use crate::{
    config::{Config, Route},
    ipc,
    platform::{DataRoot, bootstrap_credential},
    store::StoreOwner,
};
use rmcp::model::TaskStatus;
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::watch;

// ---------------------------------------------------------------------
// Pure projection
// ---------------------------------------------------------------------

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

#[test]
fn server_advertises_the_tasks_extension() {
    let facade = McpFacade::new(
        PathBuf::from("/nonexistent"),
        Credential {
            client_id: "test".into(),
            token: "test".into(),
        },
        Arc::new(Config::default().ipc),
    );
    assert!(facade.get_info().capabilities.supports_tasks());
}

// ---------------------------------------------------------------------
// Full stack: StoreOwner + IPC listener + facade over a duplex
// transport, driven by a raw JSON-RPC client (the crate builds rmcp
// server-only, and the wire shape is what this slice contracts on).
// ---------------------------------------------------------------------

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream};

struct Stack {
    owner: StoreOwner,
    operator: crate::model::Principal,
    credential: Credential,
    dir: PathBuf,
    config: Arc<Config>,
    stop: watch::Sender<bool>,
    accept: tokio::task::JoinHandle<()>,
}

async fn start_stack() -> Stack {
    let dir = std::env::temp_dir().join(format!("eliot-mcp-tasks-test-{}", model::new_id()));
    std::fs::create_dir_all(&dir).unwrap();
    let root = DataRoot::acquire(&dir).unwrap();
    let credential = bootstrap_credential(&root.path).unwrap();
    let mut cfg = Config::default();
    cfg.storage.data_dir = dir.clone();
    cfg.routes.push(Route {
        alias: "fixture".into(),
        runtime: "opencode_v2".into(),
        module_artifact_id: "eliot-opencode-v2.http.1".into(),
        enabled: true,
        native_options: json!({}),
    });
    let config = Arc::new(cfg);
    let owner = StoreOwner::start(root, config.clone(), credential.clone())
        .await
        .unwrap();
    let operator = owner.store.authenticate(credential.clone()).await.unwrap();
    // Accept IPC connections like the host loop does.
    let (stop, stopping) = watch::channel(false);
    let mut listener = ipc::Listener::bind(&dir).unwrap();
    let store = owner.store.clone();
    let ipc_config = Arc::new(config.ipc.clone());
    let accept = tokio::spawn(async move {
        let mut stopping = stopping;
        loop {
            tokio::select! {
                _ = stopping.changed() => break,
                accepted = listener.accept() => match accepted {
                    Ok(stream) => {
                        let store = store.clone();
                        let config = ipc_config.clone();
                        let stopping = stopping.clone();
                        tokio::spawn(async move {
                            let _ = ipc::serve(stream, store, config, stopping).await;
                        });
                    }
                    Err(_) => break,
                },
            }
        }
    });
    Stack {
        owner,
        operator,
        credential,
        dir,
        config,
        stop,
        accept,
    }
}

impl Stack {
    fn facade(&self) -> McpFacade {
        McpFacade::new(
            self.dir.clone(),
            self.credential.clone(),
            Arc::new(self.config.ipc.clone()),
        )
    }

    async fn store_call(&self, method: &str, params: Value) -> Result<Value> {
        self.owner
            .store
            .call(self.operator.clone(), method.to_string(), params)
            .await
    }

    async fn write(&self, method: &str, mut params: Value) -> Value {
        params["client_request_id"] = json!(model::new_id());
        self.store_call(method, params).await.unwrap()
    }

    async fn operation_count(&self, method: &str) -> usize {
        let list = self
            .store_call("operation.list", json!({"limit": 200}))
            .await
            .unwrap();
        list["items"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|item| item["method"] == method)
            .count()
    }

    async fn close(self) {
        let _ = self.stop.send(true);
        self.accept.abort();
        self.owner.close().await.unwrap();
    }
}

/// A minimal newline-delimited JSON-RPC client speaking to the served
/// facade, declaring (or not) the tasks extension at initialize.
struct McpClient {
    reader: BufReader<tokio::io::ReadHalf<DuplexStream>>,
    writer: tokio::io::WriteHalf<DuplexStream>,
    next_id: i64,
    server: tokio::task::JoinHandle<()>,
}

impl McpClient {
    async fn connect(stack: &Stack, tasks: bool) -> Self {
        let (server_io, client_io) = tokio::io::duplex(256 * 1024);
        let (server_read, server_write) = tokio::io::split(server_io);
        let facade = stack.facade();
        let server = tokio::spawn(async move {
            // Keep the RunningService alive for the life of the
            // connection, exactly like production's waiting() call;
            // dropping it closes the transport.
            if let Ok(running) = facade.serve((server_read, server_write)).await {
                let _ = running.waiting().await;
            }
        });
        let (read, writer) = tokio::io::split(client_io);
        let mut client = Self {
            reader: BufReader::new(read),
            writer,
            next_id: 0,
            server,
        };
        let capabilities = if tasks {
            json!({"extensions": {"io.modelcontextprotocol/tasks": {}}})
        } else {
            json!({})
        };
        let initialized = client
            .request(
                "initialize",
                json!({
                    "protocolVersion": "2025-11-25",
                    "capabilities": capabilities,
                    "clientInfo": {"name": "mcp-tasks-test", "version": "0"},
                }),
            )
            .await
            .expect("initialize must succeed");
        // The server advertises the tasks extension either way; the
        // client's declaration is what gates task handles.
        assert_eq!(
            initialized["capabilities"]["extensions"]["io.modelcontextprotocol/tasks"],
            json!({})
        );
        client.notify("notifications/initialized").await;
        client
    }

    async fn write_line(&mut self, value: &Value) {
        let mut line = serde_json::to_string(value).unwrap();
        line.push('\n');
        tokio::time::timeout(
            Duration::from_secs(15),
            self.writer.write_all(line.as_bytes()),
        )
        .await
        .unwrap()
        .unwrap();
    }

    async fn notify(&mut self, method: &str) {
        self.write_line(&json!({"jsonrpc": "2.0", "method": method}))
            .await;
    }

    /// Ok(result) / Err(error object) of the JSON-RPC response.
    async fn request(&mut self, method: &str, params: Value) -> std::result::Result<Value, Value> {
        self.next_id += 1;
        let id = self.next_id;
        self.write_line(&json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        }))
        .await;
        let mut line = String::new();
        loop {
            line.clear();
            let read =
                tokio::time::timeout(Duration::from_secs(15), self.reader.read_line(&mut line))
                    .await
                    .unwrap()
                    .unwrap();
            assert!(read > 0, "server closed the transport");
            let message: Value = serde_json::from_str(line.trim()).unwrap();
            if message["id"] != json!(id) {
                // A notification or an unrelated message; this facade
                // sends neither during these flows.
                continue;
            }
            if let Some(error) = message.get("error") {
                return Err(error.clone());
            }
            return Ok(message["result"].clone());
        }
    }

    async fn call_tool(
        &mut self,
        name: &str,
        arguments: Value,
    ) -> std::result::Result<Value, Value> {
        self.request("tools/call", json!({"name": name, "arguments": arguments}))
            .await
    }

    async fn close(self) {
        self.server.abort();
    }
}

#[tokio::test]
async fn tasks_client_sees_seed_poll_cancel_and_a_single_execution() {
    let stack = start_stack().await;
    let mut client = McpClient::connect(&stack, true).await;

    // The mutation returns a task seed whose taskId is the Operation.
    let seeded = client
        .call_tool("agent_open", json!({"lane_id": "one", "route": "fixture"}))
        .await
        .unwrap();
    assert_eq!(seeded["resultType"], json!("task"));
    assert_eq!(seeded["status"], json!("working"));
    let task_id = seeded["taskId"].as_str().unwrap().to_string();
    // The projection executed nothing twice: exactly one agent.open
    // Operation exists, and the seed read it back.
    assert_eq!(stack.operation_count("agent.open").await, 1);
    let operation = stack
        .store_call("operation.get", json!({"operation_id": task_id}))
        .await
        .unwrap();
    assert_eq!(operation["state"], json!("queued"));

    // Polling reflects the Operation's state and executes nothing.
    let polled = client
        .request("tasks/get", json!({"taskId": task_id}))
        .await
        .unwrap();
    assert_eq!(polled["taskId"], json!(task_id));
    assert_eq!(polled["status"], json!("working"));
    assert_eq!(stack.operation_count("agent.open").await, 1);

    // tasks/cancel submits the existing addressed cancellation.
    let ack = client
        .request("tasks/cancel", json!({"taskId": task_id}))
        .await
        .unwrap();
    assert_eq!(ack["resultType"], json!("complete"));
    let operation = stack
        .store_call("operation.get", json!({"operation_id": task_id}))
        .await
        .unwrap();
    assert_eq!(operation["state"], json!("cancelled"));
    assert_eq!(stack.operation_count("operation.cancel").await, 1);

    let polled = client
        .request("tasks/get", json!({"taskId": task_id}))
        .await
        .unwrap();
    assert_eq!(polled["status"], json!("cancelled"));

    client.close().await;
    stack.close().await;
}

#[tokio::test]
async fn retried_mutation_with_caller_id_resolves_to_the_same_task() {
    let stack = start_stack().await;
    let mut client = McpClient::connect(&stack, true).await;
    let request_id = model::new_id();
    let arguments = json!({
        "lane_id": "one",
        "route": "fixture",
        "client_request_id": request_id,
    });
    let first = client
        .call_tool("agent_open", arguments.clone())
        .await
        .unwrap();
    let second = client.call_tool("agent_open", arguments).await.unwrap();
    assert_eq!(first["resultType"], json!("task"));
    assert_eq!(second["resultType"], json!("task"));
    // The Phase A identity rule, one level up: same caller request ID →
    // same Operation → same taskId; the retry created no second copy.
    assert_eq!(first["taskId"], second["taskId"]);
    assert_eq!(stack.operation_count("agent.open").await, 1);
    client.close().await;
    stack.close().await;
}

#[tokio::test]
async fn client_without_tasks_gets_the_pre_tasks_response() {
    let stack = start_stack().await;
    let mut client = McpClient::connect(&stack, false).await;
    let result = client
        .call_tool("agent_open", json!({"lane_id": "one", "route": "fixture"}))
        .await
        .unwrap();
    // The pre-Tasks contract, unchanged: the durable Operation handle
    // in the structured result, no task handle anywhere.
    assert!(result.get("taskId").is_none(), "{result}");
    let structured = &result["structuredContent"];
    assert_eq!(structured["state"], json!("queued"));
    let operation_id = structured["operation_id"].as_str().unwrap().to_string();
    assert_eq!(stack.operation_count("agent.open").await, 1);
    // And tasks/get is gated off for this client by the router.
    let gated = client
        .request("tasks/get", json!({"taskId": operation_id}))
        .await;
    assert!(gated.is_err(), "tasks/get must be refused: {gated:?}");
    client.close().await;
    stack.close().await;
}

#[tokio::test]
async fn tasks_get_projects_settled_failed_and_unknown_operations() {
    let stack = start_stack().await;
    let mut client = McpClient::connect(&stack, true).await;

    // Settled synchronously: host.mode lands as a completed task whose
    // result is the Operation's recorded result.
    let mode = stack
        .write("host.mode", json!({"new_work": "disabled"}))
        .await;
    let polled = client
        .request(
            "tasks/get",
            json!({"taskId": mode["operation_id"].as_str().unwrap()}),
        )
        .await
        .unwrap();
    assert_eq!(polled["status"], json!("completed"));
    assert_eq!(
        polled["result"]["structuredContent"]["new_work"],
        json!("disabled")
    );
    assert_eq!(
        polled["result"]["structuredContent"]["operation_id"],
        mode["operation_id"]
    );

    // Admission-rejected: the rejected operation.cancel row (its target
    // does not exist) projects as failed, ELIOT error intact in data.
    let rejected = stack
        .store_call(
            "operation.cancel",
            json!({
                "operation_id": "missing",
                "reason": "test",
                "client_request_id": model::new_id(),
            }),
        )
        .await;
    assert!(rejected.is_err());
    let list = stack
        .store_call("operation.list", json!({"state": "rejected", "limit": 200}))
        .await
        .unwrap();
    let rejected = list["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["method"] == "operation.cancel")
        .unwrap()
        .clone();
    let polled = client
        .request(
            "tasks/get",
            json!({"taskId": rejected["operation_id"].as_str().unwrap()}),
        )
        .await
        .unwrap();
    assert_eq!(polled["status"], json!("failed"));
    assert_eq!(polled["error"]["data"], rejected["result"]);

    // Unknown taskId: the store's NOT_FOUND, preserved in error data.
    let unknown = client
        .request("tasks/get", json!({"taskId": "no-such-operation"}))
        .await;
    let error = unknown.expect_err("unknown task must error");
    assert_eq!(error["data"]["code"], json!("NOT_FOUND"));

    client.close().await;
    stack.close().await;
}

#[tokio::test]
async fn tasks_get_surfaces_the_operations_pending_native_input() {
    let stack = start_stack().await;
    // Fixture rows straight into the live database (the same approach
    // the capacity tests use): a ready binding whose native observation
    // holds one current permission request and one retained request,
    // plus a second binding with its own request, and one in-flight
    // Operation on the first binding.
    let now = model::now_ms().unwrap();
    let route = json!({
        "alias": "oc", "runtime": "opencode_v2",
        "module_artifact_id": "eliot-opencode-v2.http.1", "enabled": true,
        "native_options": {"service_id": "svc-a"},
    });
    let db = rusqlite::Connection::open(stack.dir.join("swarm.db")).unwrap();
    db.busy_timeout(Duration::from_secs(5)).unwrap();
    for (binding, root, requests) in [
        (
            "b1",
            "ses_root",
            json!([
                {"session_id": "ses_root", "request_id": "per_1", "kind": "permission", "fingerprint": "fp-1", "observed_now": true},
                {"session_id": "ses_root", "request_id": "per_stale", "kind": "permission", "fingerprint": "fp-2", "observed_now": false},
            ]),
        ),
        (
            "b2",
            "ses_other",
            json!([
                {"session_id": "ses_other", "request_id": "per_foreign", "kind": "permission", "fingerprint": "fp-3", "observed_now": true},
            ]),
        ),
    ] {
        let state = json!({
            "execution": "observed",
            "family_completeness": "partial",
            "connection": "connected",
            "observed_at_ms": now,
            "native": {
                "native_root_id": root,
                "native_scope_key": "opencode-v2:svc-a",
                "pending_requests": requests,
                "observed_children": [],
                "turns": [],
            },
        });
        db.execute(
            "INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,module_artifact_id,state,native_scope_key,native_root_id,route_json,state_json,created_at_ms) VALUES(?1,1,?2,'inst-1','eliot-opencode-v2.http.1','ready','opencode-v2:svc-a',?3,?4,?5,?6)",
            rusqlite::params![
                binding,
                format!("lane-{binding}"),
                root,
                model::canonical(&route).unwrap(),
                model::canonical(&state).unwrap(),
                now,
            ],
        )
        .unwrap();
    }
    db.execute(
        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,binding_id,binding_generation,state,due_at_ms,created_at_ms,updated_at_ms) VALUES('op-fixture','operator','req-fixture','agent.send','{}','{}','b1',1,'native_accepted',?1,?1,?1)",
        rusqlite::params![now],
    )
    .unwrap();
    drop(db);

    let facade = stack.facade();
    let detailed = facade.project_task("op-fixture").await.unwrap();
    // Only the Operation's own current request surfaces: not the
    // retained one the newest enumeration dropped, not the other
    // binding's.
    assert_eq!(detailed.status(), TaskStatus::InputRequired);
    let TaskPayload::InputRequired { input_requests } = detailed.payload else {
        panic!("pending native input must require input");
    };
    let keys: Vec<&String> = input_requests.keys().collect();
    assert_eq!(keys, [&"per_1".to_string()]);
    let wire = serde_json::to_value(&input_requests["per_1"]).unwrap();
    assert!(
        wire["params"]["message"]
            .as_str()
            .unwrap()
            .contains("permission request per_1 in session ses_root")
    );
    stack.close().await;
}
