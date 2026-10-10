//! Real Store/IPC authorization tests through the extracted MCP facade.

use crate::{
    config::{Config, McpToolProfile},
    ipc,
    platform::{DataRoot, bootstrap_credential},
    store::StoreOwner,
};
use rmcp::ServiceExt;
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    io,
    path::PathBuf,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, DuplexStream, ReadBuf},
    sync::watch,
};

use super::{public_facade, public_facade_with_expected_client_id};

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

#[test]
fn distinct_expected_client_id_rejects_profile_binding() {
    let credential = crate::model::Credential {
        client_id: "credential-under-test".into(),
        token: "unused-by-profile-construction".into(),
    };
    let expected_client_id = "profile-bound-to-another-client";
    assert_ne!(credential.client_id, expected_client_id);

    let error = match public_facade_with_expected_client_id(
        std::env::temp_dir().join("eliot-mcp-profile-mismatch-test"),
        credential,
        Config::default().ipc,
        McpToolProfile::Observer,
        expected_client_id.to_owned(),
    ) {
        Err(error) => error,
        Ok(_) => panic!("a profile bound to another client must fail construction"),
    };
    assert_eq!(error, "PROFILE_MISMATCH");
}

fn observer_registry_tool_names() -> BTreeSet<String> {
    let surface =
        swarm_mcp::launch_profile_surface(McpToolProfile::Observer, "role-core", &[], &[])
            .expect("the Observer core is a valid registry surface");
    surface["core_methods"]
        .as_array()
        .expect("the registry surface has core methods")
        .iter()
        .filter_map(Value::as_str)
        .filter(|method| {
            swarm_contracts::mcp_catalog::exposes_method(McpToolProfile::Observer, method)
                && (*method == "swarm.tools.search"
                    || swarm_mcp::application_method_read_only(method) == Some(true))
        })
        .map(swarm_contracts::mcp_catalog::tool_name)
        .collect()
}

struct IpcRequestRecorder {
    inner: ipc::Stream,
    pending: Vec<u8>,
    methods: Arc<Mutex<Vec<String>>>,
}

impl IpcRequestRecorder {
    fn record_bytes(&mut self, bytes: &[u8]) {
        self.pending.extend_from_slice(bytes);
        while let Some(newline) = self.pending.iter().position(|byte| *byte == b'\n') {
            let line = self.pending.drain(..=newline).collect::<Vec<_>>();
            if let Ok(request) = serde_json::from_slice::<Value>(&line[..newline])
                && let Some(method) = request.get("method").and_then(Value::as_str)
            {
                self.methods.lock().unwrap().push(method.to_owned());
            }
        }
    }
}

impl AsyncRead for IpcRequestRecorder {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let filled_before = output.filled().len();
        let result = Pin::new(&mut this.inner).poll_read(context, output);
        if let Poll::Ready(Ok(())) = &result {
            this.record_bytes(&output.filled()[filled_before..]);
        }
        result
    }
}

impl AsyncWrite for IpcRequestRecorder {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(context, bytes)
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(context)
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(context)
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

    let expected_names = observer_registry_tool_names();
    let mut cursor: Option<String> = None;
    let mut seen_cursors = BTreeSet::new();
    let mut all_tools = Vec::new();
    loop {
        let mut params = json!({});
        if let Some(cursor) = &cursor {
            params["cursor"] = json!(cursor);
        }
        let page = client.request("tools/list", params).await.unwrap();
        all_tools.extend(
            page["tools"]
                .as_array()
                .expect("each Observer page contains a tool array")
                .iter()
                .cloned(),
        );
        cursor = page
            .get("nextCursor")
            .and_then(Value::as_str)
            .map(str::to_owned);
        if let Some(next_cursor) = &cursor {
            assert!(
                seen_cursors.insert(next_cursor.clone()),
                "Observer tools/list repeated a pagination cursor"
            );
        } else {
            break;
        }
    }
    let observed_names: BTreeSet<String> = all_tools
        .iter()
        .map(|tool| {
            tool["name"]
                .as_str()
                .expect("every listed Observer tool has a name")
                .to_owned()
        })
        .collect();
    assert_eq!(
        observed_names.len(),
        all_tools.len(),
        "duplicate tool across pages"
    );
    assert_eq!(observed_names, expected_names);
    for tool in &all_tools {
        assert_eq!(
            tool["annotations"]["readOnlyHint"],
            json!(true),
            "{} must be advertised read-only",
            tool["name"]
        );
    }

    let before_manual_call = host.request_count();
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
    assert_eq!(
        host.request_methods_since(before_manual_call),
        Vec::<String>::new()
    );
    assert_eq!(host.authorization_reads_since(before_manual_call), 0);
    assert!(
        host.application_methods_since(before_manual_call)
            .is_empty()
    );

    let before_task_cancel = host.request_count();
    let task_cancel = client
        .request("tasks/cancel", json!({"taskId": "op-1"}))
        .await
        .expect_err("observer cannot cancel through the Tasks protocol");
    assert_eq!(task_cancel["code"], json!(-32601));
    assert_eq!(
        host.request_methods_since(before_task_cancel),
        Vec::<String>::new()
    );
    assert_eq!(host.authorization_reads_since(before_task_cancel), 0);
    assert!(
        host.application_methods_since(before_task_cancel)
            .is_empty()
    );
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

    let before_missing_tool_id = host.request_count();
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
    assert_eq!(host.authorization_reads_since(before_missing_tool_id), 1);
    assert!(
        host.application_methods_since(before_missing_tool_id)
            .is_empty()
    );

    let before_missing_task_id = host.request_count();
    let missing_task_id = manager
        .request("tasks/cancel", json!({"taskId": "op-1"}))
        .await
        .expect_err("restricted tasks/cancel without caller ID must fail before IPC");
    assert_eq!(missing_task_id["code"], json!(-32602));
    assert_eq!(host.authorization_reads_since(before_missing_task_id), 1);
    assert!(
        host.application_methods_since(before_missing_task_id)
            .is_empty()
    );
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
    request_methods: Arc<Mutex<Vec<String>>>,
}

impl ManagerHost {
    fn request_count(&self) -> usize {
        self.request_methods.lock().unwrap().len()
    }

    fn request_methods_since(&self, previous_count: usize) -> Vec<String> {
        self.request_methods
            .lock()
            .unwrap()
            .iter()
            .skip(previous_count)
            .cloned()
            .collect()
    }

    fn authorization_reads_since(&self, previous_count: usize) -> usize {
        self.request_methods_since(previous_count)
            .iter()
            .filter(|method| method.as_str() == "mcp.authorization")
            .count()
    }

    fn application_methods_since(&self, previous_count: usize) -> Vec<String> {
        self.request_methods_since(previous_count)
            .into_iter()
            .filter(|method| !matches!(method.as_str(), "client.hello" | "mcp.authorization"))
            .collect()
    }
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
    let request_methods = Arc::new(Mutex::new(Vec::new()));
    let server_request_methods = request_methods.clone();
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
                        let request_methods = server_request_methods.clone();
                        let stream: ipc::Stream = Box::new(IpcRequestRecorder {
                            inner: stream,
                            pending: Vec::new(),
                            methods: request_methods,
                        });
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
        request_methods,
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

    // host.mode is a GM-only manual tool and is not loaded on the default
    // profile surface. The live Store gate independently denies this
    // manager credential even when called directly.
    let manager_principal = host
        .owner
        .store
        .authenticate(host.manager_credential.clone())
        .await
        .unwrap();
    let direct_error = host
        .owner
        .store
        .call(
            manager_principal,
            "host.mode".into(),
            json!({
                "client_request_id": crate::model::new_id(),
                "new_work": "disabled",
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(direct_error.code, "FORBIDDEN");

    let method_not_found = client
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
        .unwrap_err();
    assert_eq!(method_not_found["code"], json!(-32601));
    assert_eq!(method_not_found["message"], json!("host.mode"));

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
