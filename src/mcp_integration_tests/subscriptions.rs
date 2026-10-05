//! Real Store/IPC subscription tests served by the extracted public facade.

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
use swarm_mcp::config::{McpConfig, McpProfileConfig, Storage};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream},
    sync::{mpsc, watch},
};

use super::public_facade;

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
    let dir = std::env::temp_dir().join(format!("eliot-mcp-subs-test-{}", model::new_id()));
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
    });
    let config = Arc::new(cfg);
    let owner = StoreOwner::start(root, config.clone(), credential.clone())
        .await
        .unwrap();
    let operator = owner.store.authenticate(credential.clone()).await.unwrap();
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

    async fn create_task(&self) -> Value {
        self.write(
            "task.create",
            json!({"project_id": "p", "spec": {"objective": "t", "phase": "p", "requirements": [{"id": "r1", "statement": "s"}]}}),
        )
        .await
    }

    async fn delta(&self, after: i64) -> Value {
        self.store_call("report.delta", json!({"after": after, "limit": 200}))
            .await
            .unwrap()
    }

    async fn close(self) {
        let _ = self.stop.send(true);
        self.accept.abort();
        self.owner.close().await.unwrap();
    }
}

struct SubClient {
    writer: tokio::io::WriteHalf<DuplexStream>,
    responses: mpsc::UnboundedReceiver<Value>,
    notifications: mpsc::UnboundedReceiver<Value>,
    next_id: i64,
    server: tokio::task::JoinHandle<()>,
}

impl SubClient {
    async fn connect(facade: swarm_mcp::ProfiledFacade) -> Self {
        let (server_io, client_io) = tokio::io::duplex(256 * 1024);
        let (server_read, server_write) = tokio::io::split(server_io);
        let server = tokio::spawn(async move {
            if let Ok(running) = facade.serve((server_read, server_write)).await {
                let _ = running.waiting().await;
            }
        });
        let (read, writer) = tokio::io::split(client_io);
        let (responses_tx, responses) = mpsc::unbounded_channel();
        let (notifications_tx, notifications) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let mut lines = BufReader::new(read).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let Ok(message) = serde_json::from_str::<Value>(&line) else {
                    continue;
                };
                if message.get("id").is_some() {
                    let _ = responses_tx.send(message);
                } else {
                    let _ = notifications_tx.send(message);
                }
            }
        });
        let mut client = Self {
            writer,
            responses,
            notifications,
            next_id: 0,
            server,
        };
        let initialized = client
            .request(
                "initialize",
                json!({
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": {"name": "mcp-subscriptions-test", "version": "0"},
                }),
            )
            .await
            .expect("initialize must succeed");
        // The subscriptions extension is advertised next to tasks;
        // the client need not declare anything to use it.
        assert_eq!(
            initialized["capabilities"]["extensions"]["eliot/subscriptions"],
            json!({})
        );
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
        loop {
            let message = tokio::time::timeout(Duration::from_secs(15), self.responses.recv())
                .await
                .unwrap()
                .expect("server closed the transport");
            if message["id"] != json!(id) {
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

    /// The next notification, waiting up to `dur`.
    async fn next_notification(&mut self, dur: Duration) -> Option<Value> {
        tokio::time::timeout(dur, self.notifications.recv())
            .await
            .ok()
            .flatten()
    }

    /// Read notifications until one satisfies `pred` (others are
    /// kept in `seen` for later assertions), or time out.
    async fn until_notification(
        &mut self,
        dur: Duration,
        seen: &mut Vec<Value>,
        pred: impl Fn(&Value) -> bool,
    ) -> Option<Value> {
        let deadline = tokio::time::Instant::now() + dur;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return None;
            }
            let message = self.next_notification(remaining).await?;
            if pred(&message) {
                return Some(message);
            }
            seen.push(message);
        }
    }

    async fn close(self) {
        self.server.abort();
    }
}

#[tokio::test]
async fn subscribe_delivers_committed_transitions_with_frame_identity() {
    let stack = start_stack().await;
    let facade = stack.facade();
    let mut client = SubClient::connect(facade).await;

    let ack = client
        .request(
            "eliot/subscribe",
            json!({"categories": ["reports", "operations"], "after": 0}),
        )
        .await
        .unwrap();
    let subscription_id = ack["subscription_id"].as_str().unwrap().to_string();
    assert!(
        ack["queue_capacity"]
            .as_u64()
            .is_some_and(|depth| depth > 0 && depth <= 64)
    );
    assert_eq!(ack["cursor"], json!(0));

    // A mutation through the tool surface commits one stream entry.
    let created = client
        .call_tool(
            "task_create",
            json!({"project_id": "p", "spec": {"objective": "t", "phase": "p", "requirements": [{"id": "r1", "statement": "s"}]}}),
        )
        .await
        .unwrap();
    let operation_id = created["structuredContent"]["operation_id"]
        .as_str()
        .unwrap()
        .to_string();

    let mut seen = Vec::new();
    let notification = client
        .until_notification(Duration::from_secs(10), &mut seen, |message| {
            message["method"] == json!("notifications/eliot/committed")
                && message["params"]["item"]["kind"] == json!("task.create")
        })
        .await
        .expect("the committed task.create must be notified");
    let params = &notification["params"];
    assert_eq!(params["subscription_id"], json!(subscription_id));
    assert_eq!(params["categories"], json!(["reports", "operations"]));
    assert_eq!(params["item"]["operation_id"], json!(operation_id));
    assert_eq!(params["cursor"], params["item"]["cursor"]);
    // Frame identity: the S2 frame of the page the entry came from.
    assert_eq!(
        params["frame"]["source_kind"],
        json!("observation_timeline")
    );
    assert!(params["frame"]["projection_revision"].is_string());
    assert_eq!(params["frame"]["range"]["after"], json!(0));
    assert_eq!(params["frame"]["coverage_complete"], json!(true));

    // Unsubscribe stops delivery; committed work continues regardless.
    let ack = client
        .request(
            "eliot/unsubscribe",
            json!({"subscription_id": subscription_id}),
        )
        .await
        .unwrap();
    assert_eq!(ack["unsubscribed"], json!(true));
    stack.create_task().await;
    assert!(
        client
            .next_notification(Duration::from_millis(900))
            .await
            .is_none(),
        "no notification may arrive after unsubscribe"
    );
    // Unknown custom methods keep the router's default answer.
    let unknown = client.request("eliot/bogus", json!({})).await;
    assert_eq!(unknown.unwrap_err()["code"], json!(-32601));

    client.close().await;
    stack.close().await;
}

#[tokio::test]
async fn subscriptions_are_isolated_and_never_touch_operations() {
    let stack = start_stack().await;
    let facade = stack.facade();
    let mut client = SubClient::connect(facade).await;
    let head = stack.delta(0).await["projection"]["range"]["next_cursor"]
        .as_i64()
        .unwrap_or(0);

    let reports = client
        .request(
            "eliot/subscribe",
            json!({"categories": ["reports"], "after": head}),
        )
        .await
        .unwrap()["subscription_id"]
        .as_str()
        .unwrap()
        .to_string();
    let operations = client
        .request(
            "eliot/subscribe",
            json!({"categories": ["operations"], "after": head}),
        )
        .await
        .unwrap()["subscription_id"]
        .as_str()
        .unwrap()
        .to_string();

    stack.create_task().await;
    // Both subscriptions observe the same committed fact, each
    // under its own ID and category.
    let mut seen = Vec::new();
    for (want_id, want_category) in [(&reports, "reports"), (&operations, "operations")] {
        let notification = client
            .until_notification(Duration::from_secs(10), &mut seen, |message| {
                message["params"]["subscription_id"] == json!(want_id)
                    && message["params"]["item"]["kind"] == json!("task.create")
            })
            .await
            .expect("each subscription observes the committed fact");
        assert_eq!(notification["params"]["categories"], json!([want_category]));
    }

    // Dropping one subscription leaves the other -- and the
    // Operations themselves -- exactly as they were.
    client
        .request("eliot/unsubscribe", json!({"subscription_id": reports}))
        .await
        .unwrap();
    let second = stack.create_task().await;
    let mut seen = Vec::new();
    let notification = client
        .until_notification(Duration::from_secs(10), &mut seen, |message| {
            message["params"]["subscription_id"] == json!(operations)
                && message["params"]["item"]["kind"] == json!("task.create")
        })
        .await
        .expect("the surviving subscription keeps delivering");
    assert_eq!(
        notification["params"]["item"]["operation_id"],
        second["operation_id"]
    );
    assert!(
        seen.iter()
            .all(|message| message["params"]["subscription_id"] != json!(reports)),
        "the dropped subscription stays silent: {seen:?}"
    );
    let operation = stack
        .store_call(
            "operation.get",
            json!({"operation_id": second["operation_id"]}),
        )
        .await
        .unwrap();
    assert_eq!(operation["state"], json!("settled"));

    client.close().await;
    stack.close().await;
}

#[tokio::test]
async fn rejected_admission_notifies_one_safe_failure_fact_without_message_data() {
    let stack = start_stack().await;
    let facade = stack.facade();
    let mut client = SubClient::connect(facade).await;
    let ack = client
        .request(
            "eliot/subscribe",
            json!({"categories": ["reports", "operations"], "after": 0}),
        )
        .await
        .unwrap();
    let subscription_id = ack["subscription_id"].as_str().unwrap().to_owned();

    // One valid commit proves the subscription is live.
    stack.create_task().await;
    let mut seen = Vec::new();
    client
        .until_notification(Duration::from_secs(10), &mut seen, |message| {
            message["params"]["item"]["kind"] == json!("task.create")
        })
        .await
        .expect("the valid commit must be notified");

    // A rejected message admission commits its durable Operation receipt
    // and one closed failure fact. Replaying the same request returns the
    // retained error without adding another fact.
    let request = json!({
        "client_request_id": "rejected-subscription-fixture",
        "recipient": "private-receiver-sentinel",
        "text": "private-body-sentinel",
    });
    for _ in 0..2 {
        let rejected = stack
            .store_call("message.send", request.clone())
            .await
            .expect_err("unregistered recipient must reject the request");
        assert_eq!(rejected.code, "NOT_FOUND");
    }
    let list = stack
        .store_call("operation.list", json!({"state": "rejected", "limit": 200}))
        .await
        .unwrap();
    let rejected_operation = list["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["method"] == "message.send")
        .expect("the rejected message Operation row exists");
    let operation_id = rejected_operation["operation_id"]
        .as_str()
        .expect("rejected message Operation identity")
        .to_owned();
    assert_eq!(rejected_operation["state"], "rejected");

    let mut seen = Vec::new();
    let notification = client
        .until_notification(Duration::from_secs(10), &mut seen, |message| {
            message["params"]["item"]["operation_id"] == json!(operation_id)
                && message["params"]["item"]["kind"] == json!("operation.rejected")
        })
        .await
        .expect("one committed rejection fact must be notified");
    assert_eq!(
        notification["method"],
        json!("notifications/eliot/committed")
    );
    let params = &notification["params"];
    assert_eq!(params["subscription_id"], json!(subscription_id));
    assert_eq!(params["categories"], json!(["reports", "operations"]));
    let item = &params["item"];
    assert_eq!(item["operation_id"], json!(operation_id));
    assert_eq!(item["kind"], json!("operation.rejected"));
    let expected_occurrence = format!("operation:{operation_id}:operation_rejected");
    let expected_payload = json!({
        "schema_version":1,
        "phase":"operation_rejected",
        "status":"rejected",
        "occurrence_id":expected_occurrence,
        "error_code":"OPERATION_REJECTED",
    });
    assert_eq!(item["payload"], expected_payload);
    assert_eq!(item["payload"].as_object().unwrap().len(), 5);
    assert_eq!(item.as_object().unwrap().len(), 5);
    let notification_json = serde_json::to_string(&notification).unwrap();
    assert!(!notification_json.contains("private-body-sentinel"));
    assert!(!notification_json.contains("private-receiver-sentinel"));

    let report = stack
        .store_call("report.delta", json!({"after":0,"limit":200}))
        .await
        .unwrap();
    let facts = report["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|fact| fact["operation_id"] == operation_id)
        .collect::<Vec<_>>();
    assert_eq!(facts.len(), 1, "replay retained exactly one safe fact");
    assert_eq!(facts[0]["kind"], "operation.rejected");
    let stored_payload = facts[0]["payload"].clone();
    assert_eq!(stored_payload["occurrence_id"], expected_occurrence);
    assert_eq!(stored_payload, expected_payload);
    let stored_payload_json = serde_json::to_string(&stored_payload).unwrap();
    assert!(!stored_payload_json.contains("private-body-sentinel"));
    assert!(!stored_payload_json.contains("private-receiver-sentinel"));
    assert!(
        client
            .next_notification(Duration::from_millis(900))
            .await
            .is_none(),
        "an exact rejection replay does not emit a duplicate notification"
    );

    client.close().await;
    stack.close().await;
}

#[tokio::test]
async fn reconnect_resubscribes_from_cursor_without_replay_continuity() {
    let stack = start_stack().await;
    let mut client = SubClient::connect(stack.facade()).await;
    let ack = client
        .request(
            "eliot/subscribe",
            json!({"categories": ["reports"], "after": 0}),
        )
        .await
        .unwrap();
    let old_id = ack["subscription_id"].as_str().unwrap().to_string();

    let first = stack.create_task().await;
    let mut seen = Vec::new();
    let notification = client
        .until_notification(Duration::from_secs(10), &mut seen, |message| {
            message["params"]["item"]["operation_id"] == first["operation_id"]
        })
        .await
        .expect("first task notified before disconnect");
    let cursor = notification["params"]["cursor"].as_i64().unwrap();
    client.close().await;

    // Facts committed while no session exists.
    let second = stack.create_task().await;
    let third = stack.create_task().await;

    // A new session: the old subscription ID is unknown, exactly as
    // the acknowledgement's resync note states.
    let mut client = SubClient::connect(stack.facade()).await;
    let gone = client
        .request("eliot/unsubscribe", json!({"subscription_id": old_id}))
        .await;
    assert_eq!(gone.unwrap_err()["data"]["code"], json!("NOT_FOUND"));

    // Re-establish from the last honestly reported cursor: the
    // missed facts arrive from the committed stream -- and nothing
    // is executed a second time to produce them.
    let ack = client
        .request(
            "eliot/subscribe",
            json!({"categories": ["reports"], "after": cursor}),
        )
        .await
        .unwrap();
    assert_eq!(ack["cursor"], json!(cursor));
    assert_eq!(ack["resync"]["after"], json!(cursor));
    let mut seen = Vec::new();
    for expected in [&second, &third] {
        let notification = client
            .until_notification(Duration::from_secs(10), &mut seen, |message| {
                message["params"]["item"]["operation_id"] == expected["operation_id"]
            })
            .await
            .expect("missed facts are delivered from the cursor");
        assert!(notification["params"]["cursor"].as_i64().unwrap() > cursor);
    }
    let list = stack
        .store_call("operation.list", json!({"limit": 200}))
        .await
        .unwrap();
    assert_eq!(
        list["items"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|entry| entry["method"] == "task.create")
            .count(),
        3,
        "resync read committed facts; it executed nothing again"
    );

    client.close().await;
    stack.close().await;
}
