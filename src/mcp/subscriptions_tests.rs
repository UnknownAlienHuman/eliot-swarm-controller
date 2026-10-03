//! S8 (R20 §17.3): bounded MCP subscriptions over committed facts.
//!
//! Pure tests cover category matching (including the gap-reference
//! and foreign-recipient edges), the subscribe/unsubscribe param
//! discipline and the notification shapes. Hub tests drive the real
//! poller against a real store with the queue receiver held by the
//! test, which is what makes the overflow -> lagged path
//! deterministic: the consumer simply never drains until the queue
//! has filled. Full-stack tests run StoreOwner + IPC + the facade
//! over a duplex transport with a raw JSON-RPC client, like S7's.

use super::subscriptions::*;
use super::*;
use crate::{
    config::{Config, Route},
    ipc,
    platform::{DataRoot, bootstrap_credential},
    store::StoreOwner,
};
use rmcp::model::CustomNotification;
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::{mpsc, watch};

// ---------------------------------------------------------------------
// Pure: categories, params, notification shapes
// ---------------------------------------------------------------------

fn item(kind: &str, operation_id: Option<&str>, payload: Value) -> Value {
    json!({
        "cursor": 7,
        "kind": kind,
        "payload": payload,
        "recorded_at_ms": 1_700_000_000_000_i64,
        "operation_id": operation_id,
    })
}

#[test]
fn categories_match_only_committed_stream_facts() {
    let all = [Category::Reports, Category::Mailbox, Category::Operations];
    // An Operation admission: a report entry and an operation
    // transition, not a mailbox delivery.
    let admission = item("task.create", Some("op-1"), json!({"task_id": "t-1"}));
    assert_eq!(
        matched_categories(&all, &admission, "operator"),
        [Category::Reports, Category::Operations]
    );
    // A mailbox delivery to the facade's own client: all three --
    // it is a committed report entry, a delivery, and the send
    // Operation's admission.
    let mail = item(
        "message.send",
        Some("op-2"),
        json!({"recipient": "operator", "delivery_id": "d-1"}),
    );
    assert_eq!(
        matched_categories(&all, &mail, "operator"),
        [Category::Reports, Category::Mailbox, Category::Operations]
    );
    // The same delivery addressed to another client is not this
    // facade's mailbox (message.read is recipient-scoped).
    assert_eq!(
        matched_categories(&all, &mail, "someone-else"),
        [Category::Reports, Category::Operations]
    );
    // A native observation with no Operation and no recipient:
    // reports only.
    let native = item("runtime.state", None, json!({"note": "observed"}));
    assert_eq!(
        matched_categories(&all, &native, "operator"),
        [Category::Reports]
    );
    // A gap reference (oversized payload detached by the store):
    // the payload -- and with it the recipient -- is not inline, so
    // mailbox cannot claim it; identity categories still match.
    let gap = json!({
        "cursor": 9,
        "kind": "message.send",
        "recorded_at_ms": 1_700_000_000_000_i64,
        "operation_id": "op-3",
        "gap": {"reason": "item_exceeds_single_item_bytes"},
    });
    assert_eq!(
        matched_categories(&all, &gap, "operator"),
        [Category::Reports, Category::Operations]
    );
    // Category subsets are respected exactly.
    assert_eq!(
        matched_categories(&[Category::Mailbox], &mail, "operator"),
        [Category::Mailbox]
    );
    assert!(matched_categories(&[Category::Operations], &native, "operator").is_empty());
}

#[test]
fn subscribe_params_are_strict() {
    let (categories, after) = parse_subscribe(&json!({
        "categories": ["reports", "operations", "reports"],
        "after": 12,
    }))
    .unwrap();
    assert_eq!(categories, [Category::Reports, Category::Operations]);
    assert_eq!(after, Some(12));
    let (_, after) = parse_subscribe(&json!({"categories": ["mailbox"]})).unwrap();
    assert_eq!(after, None);
    for bad in [
        json!({}),
        json!({"categories": []}),
        json!({"categories": ["reports", "live-stream"]}),
        json!({"categories": [42]}),
        json!({"categories": ["reports"], "after": -1}),
        json!({"categories": ["reports"], "after": "12"}),
    ] {
        let error = parse_subscribe(&bad).unwrap_err();
        assert_eq!(error.code, "INVALID_PARAMS", "{bad}");
    }
    assert_eq!(
        parse_unsubscribe(&json!({"subscription_id": "s-1"})).unwrap(),
        "s-1"
    );
    for bad in [
        json!({}),
        json!({"subscription_id": ""}),
        json!({"subscription_id": 3}),
    ] {
        assert_eq!(parse_unsubscribe(&bad).unwrap_err().code, "INVALID_PARAMS");
    }
}

#[test]
fn notification_shapes_carry_frame_identity_and_resync() {
    let frame = json!({
        "source_kind": "observation_timeline",
        "projection_revision": "sha256:abc",
        "range": {"after": 3, "next_cursor": 7},
        "coverage_complete": true,
        "gap_reason": null,
    });
    let entry = item("task.create", Some("op-1"), json!({"task_id": "t-1"}));
    let notification = committed_notification(
        "sub-1",
        &[Category::Reports, Category::Operations],
        &entry,
        &frame,
    );
    assert_eq!(notification.method, COMMITTED_NOTIFICATION);
    let params = notification.params.expect("params");
    assert_eq!(params["subscription_id"], json!("sub-1"));
    assert_eq!(params["categories"], json!(["reports", "operations"]));
    assert_eq!(params["cursor"], json!(7));
    assert_eq!(params["item"], entry);
    // The frame is carried verbatim -- the subscriber detects gaps
    // from the page's own range/revision, not from trust.
    assert_eq!(params["frame"], frame);

    let gap = LaggedGap {
        dropped_items: 6,
        from_cursor: 4,
        through_cursor: 10,
        head_reached: true,
    };
    let lagged = lagged_notification("sub-1", &gap);
    assert_eq!(lagged.method, LAGGED_NOTIFICATION);
    let params = lagged.params.expect("params");
    assert_eq!(params["dropped_items"], json!(6));
    assert_eq!(params["from_cursor"], json!(4));
    assert_eq!(params["through_cursor"], json!(10));
    assert_eq!(params["resync"]["after"], json!(4));
    assert_eq!(
        params["resync"]["reads"],
        json!(["report.delta", "message.read", "operation.get"])
    );
}

// ---------------------------------------------------------------------
// Stack: StoreOwner + IPC listener, facade factories with a tuned
// subscription hub.
// ---------------------------------------------------------------------

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
    fn facade(&self, queue_depth: usize, poll_interval: Duration) -> McpFacade {
        McpFacade {
            root: self.dir.clone(),
            credential: self.credential.clone(),
            ipc_config: Arc::new(self.config.ipc.clone()),
            client: Mutex::new(None),
            pump_client: Arc::new(Mutex::new(None)),
            subscriptions: Arc::new(SubscriptionHub::new(queue_depth, poll_interval)),
        }
    }

    fn pump_source(&self) -> Arc<PumpSource> {
        Arc::new(PumpSource::new(
            self.dir.clone(),
            self.credential.clone(),
            Arc::new(self.config.ipc.clone()),
            Arc::new(Mutex::new(None)),
        ))
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

// ---------------------------------------------------------------------
// Hub: overflow -> exactly one lagged, and resync recovers the facts
// ---------------------------------------------------------------------

async fn recv_timeout(
    rx: &mut mpsc::Receiver<CustomNotification>,
    dur: Duration,
) -> Option<CustomNotification> {
    tokio::time::timeout(dur, rx.recv()).await.ok().flatten()
}

#[tokio::test]
async fn overflow_emits_exactly_one_lagged_and_resync_recovers_the_gap() {
    let stack = start_stack().await;
    // Ground truth head, then all ten facts committed BEFORE the
    // subscription opens: the poller's first page then holds the
    // whole backlog atomically, whatever the scheduling.
    let head = stack.delta(0).await["projection"]["range"]["next_cursor"]
        .as_i64()
        .unwrap_or(0);
    for _ in 0..10 {
        stack.create_task().await;
    }
    let committed_stream = stack.delta(head).await;
    let cursors: Vec<i64> = committed_stream["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| entry["cursor"].as_i64().unwrap())
        .collect();
    assert_eq!(cursors.len(), 10, "{committed_stream}");
    // The consumer never reads: the bounded queue (4) fills and the
    // poller fast-forwards the rest as one lagged episode.
    let hub = Arc::new(SubscriptionHub::new(4, Duration::from_millis(10)));
    let (id, mut rx, ack) = hub
        .open(stack.pump_source(), vec![Category::Reports], Some(head))
        .unwrap();
    assert_eq!(ack["queue_capacity"], json!(4));
    assert_eq!(ack["cursor"], json!(head));
    assert_eq!(ack["resync"]["reads"][0], json!("report.delta"));
    // Let the poller run its episode to the head (10 ms ticks; this
    // is generous), still without draining the queue.
    tokio::time::sleep(Duration::from_millis(600)).await;

    // Exactly the queue's depth of committed notifications, in
    // cursor order, each carrying its page frame.
    for (index, expected) in cursors[..4].iter().enumerate() {
        let notification = recv_timeout(&mut rx, Duration::from_secs(5))
            .await
            .expect("queued committed notification");
        assert_eq!(notification.method, COMMITTED_NOTIFICATION);
        let params = notification.params.expect("params");
        assert_eq!(params["subscription_id"], json!(id));
        assert_eq!(params["cursor"], json!(expected));
        assert_eq!(params["item"]["kind"], json!("task.create"));
        assert!(params["frame"]["projection_revision"].is_string());
        // The frame is the page this item was read from: its range
        // starts at the head (first item) or at an earlier delivered
        // cursor, always strictly before the item, and its
        // next_cursor reaches at least the item.
        let page_after = params["frame"]["range"]["after"].as_i64().unwrap();
        let page_next = params["frame"]["range"]["next_cursor"].as_i64().unwrap();
        assert!(page_after < *expected, "{params}");
        assert!(page_next >= *expected, "{params}");
        if index == 0 {
            assert_eq!(page_after, head);
        } else {
            assert!(
                page_after == head || cursors[..index].contains(&page_after),
                "{params}"
            );
        }
    }
    // Then exactly one lagged marker for the whole skipped range.
    let lagged = recv_timeout(&mut rx, Duration::from_secs(5))
        .await
        .expect("one lagged marker");
    assert_eq!(lagged.method, LAGGED_NOTIFICATION);
    let params = lagged.params.expect("params");
    assert_eq!(params["dropped_items"], json!(6), "{params}");
    assert_eq!(params["from_cursor"], json!(cursors[3]), "{params}");
    assert_eq!(params["through_cursor"], json!(cursors[9]), "{params}");
    // No second marker, no late duplicates.
    assert!(
        recv_timeout(&mut rx, Duration::from_millis(400))
            .await
            .is_none()
    );

    // Resync through the exact read from the marker's cursor
    // recovers precisely the dropped facts.
    let resync = stack.delta(params["from_cursor"].as_i64().unwrap()).await;
    let recovered: Vec<i64> = resync["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| entry["cursor"].as_i64().unwrap())
        .collect();
    assert_eq!(recovered, cursors[4..]);

    // Unsubscribing ends delivery; the store is untouched by it.
    let ack = hub.unsubscribe(&id).unwrap();
    assert_eq!(ack["unsubscribed"], json!(true));
    stack.create_task().await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        recv_timeout(&mut rx, Duration::from_millis(300))
            .await
            .is_none()
    );
    assert_eq!(
        hub.unsubscribe(&id).unwrap_err().code,
        "NOT_FOUND",
        "a second unsubscribe finds nothing"
    );
    stack.close().await;
}

#[tokio::test]
async fn hub_bounds_subscriptions_and_sessions() {
    let stack = start_stack().await;
    let hub = Arc::new(SubscriptionHub::new(2, Duration::from_millis(10)));
    let mut ids = Vec::new();
    for _ in 0..MAX_SUBSCRIPTIONS {
        let (id, _rx, _ack) = hub
            .open(stack.pump_source(), vec![Category::Reports], Some(0))
            .unwrap();
        ids.push(id);
    }
    let error = hub
        .open(stack.pump_source(), vec![Category::Reports], Some(0))
        .expect_err("the 17th subscription must be refused");
    assert_eq!(error.code, "SUBSCRIPTION_LIMIT");
    for id in &ids {
        hub.unsubscribe(id).unwrap();
    }
    // After the session's subscriptions are gone, every old ID is
    // unknown -- the reconnect contract, at hub level.
    assert_eq!(hub.unsubscribe(&ids[0]).unwrap_err().code, "NOT_FOUND");
    stack.close().await;
}

// ---------------------------------------------------------------------
// Full stack over a duplex transport, raw JSON-RPC client
// ---------------------------------------------------------------------

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream};

/// A newline-delimited JSON-RPC client that separates responses
/// from server notifications, so tests can assert on both streams.
struct SubClient {
    writer: tokio::io::WriteHalf<DuplexStream>,
    responses: mpsc::UnboundedReceiver<Value>,
    notifications: mpsc::UnboundedReceiver<Value>,
    next_id: i64,
    server: tokio::task::JoinHandle<()>,
}

impl SubClient {
    async fn connect(facade: McpFacade) -> Self {
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
            initialized["capabilities"]["extensions"][EXTENSION_ID],
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
    let facade = stack.facade(MAX_QUEUE_DEPTH, POLL_INTERVAL);
    let mut client = SubClient::connect(facade).await;

    let ack = client
        .request(
            SUBSCRIBE_METHOD,
            json!({"categories": ["reports", "operations"], "after": 0}),
        )
        .await
        .unwrap();
    let subscription_id = ack["subscription_id"].as_str().unwrap().to_string();
    assert_eq!(ack["queue_capacity"], json!(MAX_QUEUE_DEPTH));
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
            message["method"] == json!(COMMITTED_NOTIFICATION)
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
            UNSUBSCRIBE_METHOD,
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
    let facade = stack.facade(MAX_QUEUE_DEPTH, POLL_INTERVAL);
    let mut client = SubClient::connect(facade).await;
    let head = stack.delta(0).await["projection"]["range"]["next_cursor"]
        .as_i64()
        .unwrap_or(0);

    let reports = client
        .request(
            SUBSCRIBE_METHOD,
            json!({"categories": ["reports"], "after": head}),
        )
        .await
        .unwrap()["subscription_id"]
        .as_str()
        .unwrap()
        .to_string();
    let operations = client
        .request(
            SUBSCRIBE_METHOD,
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
        .request(UNSUBSCRIBE_METHOD, json!({"subscription_id": reports}))
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
async fn rejected_admission_commits_no_fact_and_notifies_nothing() {
    let stack = start_stack().await;
    let facade = stack.facade(MAX_QUEUE_DEPTH, POLL_INTERVAL);
    let mut client = SubClient::connect(facade).await;
    let ack = client
        .request(
            SUBSCRIBE_METHOD,
            json!({"categories": ["reports"], "after": 0}),
        )
        .await
        .unwrap();
    let _ = ack["subscription_id"].as_str().unwrap();

    // One valid commit proves the subscription is live.
    stack.create_task().await;
    let mut seen = Vec::new();
    client
        .until_notification(Duration::from_secs(10), &mut seen, |message| {
            message["params"]["item"]["kind"] == json!("task.create")
        })
        .await
        .expect("the valid commit must be notified");

    // A rejected admission: the Operation row exists (state
    // rejected) but mutate() commits no stream entry for it -- so
    // there is no committed fact to notify, and the subscription
    // must stay silent even though something "happened".
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
    assert!(
        list["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry["method"] == "operation.cancel"),
        "the rejected operation row exists"
    );
    assert!(
        client
            .next_notification(Duration::from_millis(900))
            .await
            .is_none(),
        "an uncommitted (rejected) event produces no notification"
    );

    client.close().await;
    stack.close().await;
}

#[tokio::test]
async fn reconnect_resubscribes_from_cursor_without_replay_continuity() {
    let stack = start_stack().await;
    let mut client = SubClient::connect(stack.facade(MAX_QUEUE_DEPTH, POLL_INTERVAL)).await;
    let ack = client
        .request(
            SUBSCRIBE_METHOD,
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
    let mut client = SubClient::connect(stack.facade(MAX_QUEUE_DEPTH, POLL_INTERVAL)).await;
    let gone = client
        .request(UNSUBSCRIBE_METHOD, json!({"subscription_id": old_id}))
        .await;
    assert_eq!(gone.unwrap_err()["data"]["code"], json!("NOT_FOUND"));

    // Re-establish from the last honestly reported cursor: the
    // missed facts arrive from the committed stream -- and nothing
    // is executed a second time to produce them.
    let ack = client
        .request(
            SUBSCRIBE_METHOD,
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
