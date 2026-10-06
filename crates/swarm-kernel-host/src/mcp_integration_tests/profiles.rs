//! Real Store/IPC authorization tests through the extracted MCP facade.

use crate::{
    config::{Config, McpToolProfile},
    ipc,
    platform::{DataRoot, bootstrap_credential},
    store::StoreOwner,
};
use rmcp::ServiceExt;
use serde_json::{Value, json};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream},
    sync::watch,
};

use super::public_facade;

struct ProfileClient {
    reader: BufReader<tokio::io::ReadHalf<DuplexStream>>,
    writer: tokio::io::WriteHalf<DuplexStream>,
    next_id: i64,
    server: tokio::task::JoinHandle<()>,
}

impl ProfileClient {
    async fn connect(facade: swarm_mcp::ProfiledFacade, tasks: bool) -> Self {
        let (server_io, client_io) = tokio::io::duplex(256 * 1024);
        let (server_read, server_write) = tokio::io::split(server_io);
        let server = tokio::spawn(async move {
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
        client
            .request(
                "initialize",
                json!({
                    "protocolVersion": "2025-11-25",
                    "capabilities": capabilities,
                    "clientInfo": {"name": "mcp-profile-test", "version": "0"},
                }),
            )
            .await
            .expect("initialize must succeed");
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
                continue;
            }
            if let Some(error) = message.get("error") {
                return Err(error.clone());
            }
            return Ok(message["result"].clone());
        }
    }

    async fn close(self) {
        self.server.abort();
    }
}

#[tokio::test]
async fn observer_hides_mutation_and_rejects_manual_tool_and_task_cancel_before_ipc() {
    let host = start_manager_host().await;
    let operator_credential = bootstrap_credential(&host.dir).unwrap();
    let facade = public_facade(
        host.dir.clone(),
        operator_credential,
        Config::default().ipc,
        McpToolProfile::Observer,
    );
    let mut client = ProfileClient::connect(facade, true).await;

    let listing = client.request("tools/list", json!({})).await.unwrap();
    let tools = listing["tools"].as_array().unwrap();
    assert!(
        tools
            .iter()
            .any(|tool| tool["name"] == json!("swarm_tools_search"))
    );
    assert!(
        tools
            .iter()
            .any(|tool| tool["name"] == json!("operation_get"))
    );
    assert!(
        tools
            .iter()
            .all(|tool| tool["annotations"]["readOnlyHint"] == true)
    );
    assert!(
        !tools
            .iter()
            .any(|tool| tool["name"] == json!("message_cancel"))
    );

    let manually_addressed = client
        .request(
            "tools/call",
            json!({
                "name": "message_cancel",
                "arguments": {"delivery_id": "d-1", "payload_digest": "sha256:claimed"},
            }),
        )
        .await
        .expect_err("hidden mutation must be rejected before attempting IPC");
    assert_eq!(manually_addressed["code"], json!(-32601));

    let task_cancel = client
        .request("tasks/cancel", json!({"taskId": "op-1"}))
        .await
        .expect_err("observer cannot cancel through the Tasks protocol");
    assert_eq!(task_cancel["code"], json!(-32601));
    client.close().await;
    host.close().await;
}

#[tokio::test]
async fn restricted_mutations_require_caller_ids_before_ipc() {
    let host = start_manager_host().await;
    let mut manager = ProfileClient::connect(
        public_facade(
            host.dir.clone(),
            host.manager_credential.clone(),
            Config::default().ipc,
            McpToolProfile::Manager,
        ),
        true,
    )
    .await;
    let listing = manager.request("tools/list", json!({})).await.unwrap();
    let manager_tools = listing["tools"].as_array().unwrap();
    for tool in manager_tools
        .iter()
        .filter(|tool| tool["annotations"]["readOnlyHint"] != json!(true))
    {
        assert!(
            tool["inputSchema"]["required"]
                .as_array()
                .unwrap()
                .contains(&json!("client_request_id")),
            "{}",
            tool["name"]
        );
    }

    let missing_tool_id = manager
        .request(
            "tools/call",
            json!({
                "name": "message_send",
                "arguments": {"recipient": "operator", "text": "must fail before IPC"},
            }),
        )
        .await
        .expect_err("restricted mutation without a caller ID must fail at MCP boundary");
    assert_eq!(missing_tool_id["code"], json!(-32602));

    let missing_task_id = manager
        .request("tasks/cancel", json!({"taskId": "op-1"}))
        .await
        .expect_err("restricted tasks/cancel without caller ID must fail before IPC");
    assert_eq!(missing_task_id["code"], json!(-32602));
    manager.close().await;

    let mut full = ProfileClient::connect(
        public_facade(
            host.dir.clone(),
            host.manager_credential.clone(),
            Config::default().ipc,
            McpToolProfile::Full,
        ),
        false,
    )
    .await;
    let listing = full.request("tools/list", json!({})).await.unwrap();
    for tool in listing["tools"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|tool| tool["annotations"]["readOnlyHint"] != json!(true))
    {
        assert!(
            !tool["inputSchema"]["required"]
                .as_array()
                .is_some_and(|required| required.contains(&json!("client_request_id"))),
            "{}",
            tool["name"]
        );
    }
    full.close().await;
    host.close().await;
}

struct ManagerHost {
    owner: StoreOwner,
    manager_credential: crate::model::Credential,
    dir: PathBuf,
    stop: watch::Sender<bool>,
    accept: tokio::task::JoinHandle<()>,
}

async fn start_manager_host() -> ManagerHost {
    let dir = std::env::temp_dir().join(format!(
        "eliot-mcp-profile-app-auth-{}",
        crate::model::new_id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let root = DataRoot::acquire(&dir).unwrap();
    let operator_credential = bootstrap_credential(&root.path).unwrap();
    let mut config = Config::default();
    config.storage.data_dir = dir.clone();
    let config = Arc::new(config);
    let owner = StoreOwner::start(root, config.clone(), operator_credential.clone())
        .await
        .unwrap();
    let operator = owner.store.authenticate(operator_credential).await.unwrap();
    let manager_token = format!("{}{}", crate::model::new_id(), crate::model::new_id());
    let manager_credential = crate::model::Credential {
        client_id: "profile-manager".into(),
        token: manager_token.clone(),
    };
    owner
        .store
        .call(
            operator,
            "client.register".into(),
            json!({
                "client_request_id": crate::model::new_id(),
                "client_id": manager_credential.client_id.clone(),
                "role": "manager",
                "token_hash": crate::model::digest(manager_token.as_bytes()),
            }),
        )
        .await
        .unwrap();

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
    ManagerHost {
        owner,
        manager_credential,
        dir,
        stop,
        accept,
    }
}

impl ManagerHost {
    async fn close(self) {
        let _ = self.stop.send(true);
        self.accept.abort();
        let _ = self.accept.await;
        self.owner.close().await.unwrap();
        std::fs::remove_dir_all(self.dir).unwrap();
    }
}

#[tokio::test]
async fn gm_profile_does_not_elevate_a_manager_credential() {
    let host = start_manager_host().await;
    let mut client = ProfileClient::connect(
        public_facade(
            host.dir.clone(),
            host.manager_credential.clone(),
            Config::default().ipc,
            McpToolProfile::Gm,
        ),
        false,
    )
    .await;

    let operator_credential = bootstrap_credential(&host.dir).unwrap();
    let operator = host
        .owner
        .store
        .authenticate(operator_credential.clone())
        .await
        .unwrap();
    let task = host
        .owner
        .store
        .call(
            operator.clone(),
            "task.create".into(),
            json!({
                "client_request_id": crate::model::new_id(),
                "project_id": "profile-test",
                "spec": {
                    "objective": "exercise stale request forwarding",
                    "phase": "test",
                    "requirements": [{"id": "r1", "statement": "one requirement"}],
                },
            }),
        )
        .await
        .unwrap();
    let stale = client
        .request(
            "tools/call",
            json!({
                "name": "task_claim",
                "arguments": {
                    "task_id": task["task_id"],
                    "expected_revision": 2,
                    "client_request_id": crate::model::new_id(),
                },
            }),
        )
        .await
        .unwrap();
    assert_eq!(stale["isError"], json!(true));
    let stale_error: Value =
        serde_json::from_str(stale["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(stale_error["error"]["code"], json!("STALE_REVISION"));

    let sent = client
        .request(
            "tools/call",
            json!({
                "name": "message_send",
                "arguments": {
                    "recipient": "operator",
                    "text": "profile test delivery",
                    "client_request_id": crate::model::new_id(),
                },
            }),
        )
        .await
        .unwrap();
    let structured = &sent["structuredContent"];
    let delivery_id = structured["delivery_id"].as_str().unwrap();
    let mismatched_digest = format!("{}-wrong", structured["payload_digest"].as_str().unwrap());
    let cancelled = client
        .request(
            "tools/call",
            json!({
                "name": "message_cancel",
                "arguments": {
                    "delivery_id": delivery_id,
                    "payload_digest": mismatched_digest,
                    "client_request_id": crate::model::new_id(),
                },
            }),
        )
        .await
        .unwrap();
    assert_eq!(cancelled["isError"], json!(true));
    let digest_error: Value =
        serde_json::from_str(cancelled["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(digest_error["error"]["code"], json!("DIGEST_MISMATCH"));

    let result = client
        .request(
            "tools/call",
            json!({
                "name": "host_mode",
                "arguments": {
                    "new_work": "disabled",
                    "client_request_id": crate::model::new_id(),
                },
            }),
        )
        .await
        .unwrap();
    assert_eq!(result["isError"], json!(true));
    let error: Value =
        serde_json::from_str(result["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(error["error"]["code"], json!("FORBIDDEN"));

    let owner_principal = host
        .owner
        .store
        .authenticate(operator_credential)
        .await
        .unwrap();
    let status = host
        .owner
        .store
        .call(owner_principal, "host.status".into(), json!({}))
        .await
        .unwrap();
    assert_eq!(status["execution_mode"]["new_work"], json!("enabled"));

    client.close().await;
    host.close().await;
}
