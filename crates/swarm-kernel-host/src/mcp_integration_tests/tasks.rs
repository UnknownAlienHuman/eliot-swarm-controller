//! Real root Store/IPC integration through the extracted public MCP handler.

use crate::{
    config::{Config, McpToolProfile, Route},
    error::Result,
    ipc,
    model::{self, Credential},
    platform::{DataRoot, bootstrap_credential},
    store::StoreOwner,
};
use rmcp::ServiceExt;
use serde_json::{Value, json};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::watch;

use super::public_facade;

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
        workspace_option: None,
        owned_service: None,
        alias: "fixture".into(),
        runtime: "opencode_v2".into(),
        module_artifact_id: "eliot-opencode-v2.http.1".into(),
        enabled: true,
        native_options: json!({}),
        admission_policy: None,
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
    fn facade(&self) -> swarm_mcp::ProfiledFacade {
        public_facade(
            self.dir.clone(),
            self.credential.clone(),
            self.config.ipc.clone(),
            McpToolProfile::Full,
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
    let operation_id = first["taskId"].as_str().unwrap();
    let stored = stack
        .store_call("operation.get", json!({"operation_id": operation_id}))
        .await
        .unwrap();
    assert_eq!(stored["operation_id"], json!(operation_id));
    assert_eq!(stored["method"], json!("agent.open"));

    // The Phase A identity rule binds both retried responses to the durable
    // Operation, not merely to each other or to a fabricated stable handle.
    assert_eq!(second["taskId"], stored["operation_id"]);
    assert_eq!(stack.operation_count("agent.open").await, 1);

    let polled = client
        .request("tasks/get", json!({"taskId": stored["operation_id"]}))
        .await
        .unwrap();
    assert_eq!(polled["taskId"], stored["operation_id"]);
    assert_eq!(polled["status"], json!("working"));
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

    // host.mode settles synchronously, but its result is intentionally outside
    // operation.get's closed receipt vocabulary. Tasks/get follows that safe
    // public readback instead of exposing the private mode-change result.
    let mode = stack
        .write("host.mode", json!({"new_work": "disabled"}))
        .await;
    let operation = stack
        .store_call(
            "operation.get",
            json!({"operation_id": mode["operation_id"]}),
        )
        .await
        .unwrap();
    assert_eq!(operation["result_status"], json!("not_projected"));
    assert!(operation.get("result").is_none());
    let polled = client
        .request(
            "tasks/get",
            json!({"taskId": mode["operation_id"].as_str().unwrap()}),
        )
        .await
        .unwrap();
    assert_eq!(polled["status"], json!("completed"));
    assert_eq!(polled["taskId"], mode["operation_id"]);
    assert_eq!(
        polled["result"]["structuredContent"],
        json!({"result": null})
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
