use crate::{
    IpcConfig, ipc_endpoint,
    multiplex::{Session, Stream},
};
use serde_json::{Value, json};
#[cfg(any(windows, test))]
use std::time::Duration;
use std::{path::Path, sync::Arc};
use swarm_contracts::{
    Credential,
    error::{Error, Result},
};

async fn open_stream(root: &Path) -> Result<Stream> {
    let endpoint = ipc_endpoint(root)?;
    #[cfg(unix)]
    {
        Ok(Box::new(tokio::net::UnixStream::connect(&endpoint).await?))
    }
    #[cfg(windows)]
    {
        use tokio::net::windows::named_pipe::ClientOptions;
        // Only establishment is retried, before authentication/application IO.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            match ClientOptions::new().open(&endpoint) {
                Ok(stream) => return Ok(Box::new(stream)),
                Err(error)
                    if error.raw_os_error() == Some(231)
                        && tokio::time::Instant::now() < deadline =>
                {
                    tokio::time::sleep(Duration::from_millis(25)).await
                }
                Err(error) => return Err(error.into()),
            }
        }
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = endpoint;
        Err(Error::new(
            "HOST_UNAVAILABLE",
            "local IPC is supported only on Windows and Unix",
        ))
    }
}

#[cfg(test)]
mod multiplex_tests {
    use super::*;
    use futures_util::{SinkExt, StreamExt};
    use tokio::{io::DuplexStream, sync::oneshot};
    use tokio_util::codec::{FramedRead, FramedWrite, LinesCodec};

    fn fixture(capacity: usize, pipe: usize) -> (Client, DuplexStream) {
        let (owned, peer) = tokio::io::duplex(pipe);
        let config = IpcConfig {
            max_frame_bytes: 4096,
            max_inflight_per_connection: capacity,
            write_timeout_seconds: 1,
            ..IpcConfig::default()
        };
        (
            Client {
                session: Session::attach(Box::new(owned), &config).unwrap(),
            },
            peer,
        )
    }
    fn response(request: &Value, result: Value) -> String {
        json!({"jsonrpc":"2.0","id":request["id"],"result":result}).to_string()
    }

    #[tokio::test]
    async fn ipc_mux_out_of_order_replies_match_exact_concurrent_ids() {
        let (client, peer) = fixture(4, 16 * 1024);
        let clone = client.clone();
        let task = tokio::spawn(async move {
            let (r, w) = tokio::io::split(peer);
            let mut r = FramedRead::new(r, LinesCodec::new());
            let mut w = FramedWrite::new(w, LinesCodec::new());
            let one: Value = serde_json::from_str(&r.next().await.unwrap().unwrap()).unwrap();
            let two: Value = serde_json::from_str(&r.next().await.unwrap().unwrap()).unwrap();
            assert_ne!(one["id"], two["id"]);
            assert_eq!(one["id"].as_str().unwrap().len(), 36);
            w.send(response(&two, two["params"].clone())).await.unwrap();
            w.send(response(&one, one["params"].clone())).await.unwrap();
            assert!(r.next().await.is_none());
        });
        let (one, two) = tokio::join!(
            client.request_shared("module.next", json!({"tag":"next"})),
            clone.request_shared("module.outcome", json!({"tag":"outcome"}))
        );
        assert_eq!(one.unwrap()["tag"], "next");
        assert_eq!(two.unwrap()["tag"], "outcome");
        client.close().await;
        task.await.unwrap();
    }
    #[tokio::test]
    async fn ipc_mux_capacity_and_queued_cancel_send_no_frame_and_do_not_poison() {
        let (client, peer) = fixture(1, 16 * 1024);
        let mut first = Box::pin(client.request_shared("must.not.send", json!({})));
        assert!(futures_util::poll!(first.as_mut()).is_pending());
        let error = client
            .request_shared("also.not.sent", json!({}))
            .await
            .unwrap_err();
        assert_eq!(error.code, "IPC_CAPACITY");
        drop(first);
        assert!(client.is_connected());
        // The bounded writer still owns the cancelled queue slot until its
        // next poll skips it; this is not an implicit RPC retry.
        tokio::task::yield_now().await;
        let task = tokio::spawn(async move {
            let (r, w) = tokio::io::split(peer);
            let mut r = FramedRead::new(r, LinesCodec::new());
            let mut w = FramedWrite::new(w, LinesCodec::new());
            let request: Value = serde_json::from_str(&r.next().await.unwrap().unwrap()).unwrap();
            assert_eq!(request["method"], "only.sent");
            w.send(response(&request, json!({"ok":true})))
                .await
                .unwrap();
            assert!(r.next().await.is_none());
        });
        assert_eq!(
            client.request_shared("only.sent", json!({})).await.unwrap()["ok"],
            true
        );
        client.close().await;
        task.await.unwrap();
    }
    #[tokio::test]
    async fn ipc_mux_application_refusal_keeps_same_transport_usable() {
        let (client, peer) = fixture(2, 16 * 1024);
        let task = tokio::spawn(async move {
            let (r, w) = tokio::io::split(peer);
            let mut r = FramedRead::new(r, LinesCodec::new());
            let mut w = FramedWrite::new(w, LinesCodec::new());
            let request: Value = serde_json::from_str(&r.next().await.unwrap().unwrap()).unwrap();
            w.send(json!({"jsonrpc":"2.0","id":request["id"],"error":{"code":-32000,"message":"scope denied","data":{"code":"FORBIDDEN"}}}).to_string()).await.unwrap();
            let next: Value = serde_json::from_str(&r.next().await.unwrap().unwrap()).unwrap();
            assert_ne!(request["id"], next["id"]);
            w.send(response(&next, json!({"ok":true}))).await.unwrap();
            assert!(r.next().await.is_none());
        });
        assert_eq!(
            client
                .request_shared("denied", json!({}))
                .await
                .unwrap_err()
                .code,
            "FORBIDDEN"
        );
        assert!(client.is_connected());
        assert_eq!(
            client.request_shared("allowed", json!({})).await.unwrap()["ok"],
            true
        );
        client.close().await;
        task.await.unwrap();
    }
    #[tokio::test]
    async fn ipc_mux_cancel_after_write_poison_drains_without_resend_or_new_hello() {
        let (client, peer) = fixture(2, 16 * 1024);
        let (seen, received) = oneshot::channel();
        let (late, after_cancel) = oneshot::channel();
        let peer = tokio::spawn(async move {
            let (r, w) = tokio::io::split(peer);
            let mut r = FramedRead::new(r, LinesCodec::new());
            let mut w = FramedWrite::new(w, LinesCodec::new());
            let request: Value = serde_json::from_str(&r.next().await.unwrap().unwrap()).unwrap();
            seen.send(()).unwrap();
            after_cancel.await.unwrap();
            let _ = w.send(response(&request, json!({"late":true}))).await;
            assert!(r.next().await.is_none());
        });
        let clone = client.clone();
        let call = tokio::spawn(async move {
            clone
                .request_shared("mutation", json!({"logical_id":"retain"}))
                .await
        });
        received.await.unwrap();
        call.abort();
        assert!(call.await.unwrap_err().is_cancelled());
        assert!(!client.is_connected());
        assert_eq!(
            client
                .request_shared("never.replay", json!({}))
                .await
                .unwrap_err()
                .code,
            "DISCONNECTED"
        );
        late.send(()).unwrap();
        client.close().await;
        peer.await.unwrap();
    }
    #[tokio::test]
    async fn ipc_mux_retired_reply_cannot_complete_another_pending_request() {
        let (client, peer) = fixture(2, 16 * 1024);
        let peer = tokio::spawn(async move {
            let (r, w) = tokio::io::split(peer);
            let mut r = FramedRead::new(r, LinesCodec::new());
            let mut w = FramedWrite::new(w, LinesCodec::new());
            let old: Value = serde_json::from_str(&r.next().await.unwrap().unwrap()).unwrap();
            w.send(response(&old, json!({"old":true}))).await.unwrap();
            let next: Value = serde_json::from_str(&r.next().await.unwrap().unwrap()).unwrap();
            assert_ne!(old["id"], next["id"]);
            w.send(response(&old, json!({"forged_next":true})))
                .await
                .unwrap();
            assert!(r.next().await.is_none());
        });
        assert_eq!(
            client.request_shared("first", json!({})).await.unwrap()["old"],
            true
        );
        assert_eq!(
            client
                .request_shared("second", json!({}))
                .await
                .unwrap_err()
                .code,
            "OUTCOME_UNKNOWN"
        );
        assert!(!client.is_connected());
        client.close().await;
        peer.await.unwrap();
    }
    #[tokio::test]
    async fn ipc_mux_partial_eof_fails_all_unresolved_calls_unknown() {
        use tokio::io::AsyncWriteExt;
        let (client, peer) = fixture(2, 16 * 1024);
        let peer = tokio::spawn(async move {
            let (r, mut w) = tokio::io::split(peer);
            let mut r = FramedRead::new(r, LinesCodec::new());
            assert!(r.next().await.is_some());
            assert!(r.next().await.is_some());
            w.write_all(b"{\"jsonrpc\":").await.unwrap();
        });
        let (a, b) = tokio::join!(
            client.request_shared("first", json!({})),
            client.request_shared("second", json!({}))
        );
        assert_eq!(a.unwrap_err().code, "OUTCOME_UNKNOWN");
        assert_eq!(b.unwrap_err().code, "OUTCOME_UNKNOWN");
        client.close().await;
        peer.await.unwrap();
    }
    #[tokio::test]
    async fn ipc_mux_writer_backpressure_is_bounded_unknown_not_a_retry() {
        let (client, _nonreading_peer) = fixture(2, 16);
        let (a, b) = tokio::time::timeout(Duration::from_secs(3), async {
            tokio::join!(
                client.request_shared("large", json!({"body":"x".repeat(1024)})),
                client.request_shared("queued", json!({}))
            )
        })
        .await
        .unwrap();
        assert_eq!(a.unwrap_err().code, "OUTCOME_UNKNOWN");
        assert_eq!(b.unwrap_err().code, "OUTCOME_UNKNOWN");
        assert!(!client.is_connected());
        client.close().await;
    }
}

/// Authenticated bounded IPC. Clones share the SAME socket/pipe and reader,
/// writer, pending map and failure state; cloning never authenticates/reconnects.
#[derive(Clone)]
pub struct Client {
    session: Arc<Session>,
}
impl Client {
    pub async fn connect(root: &Path, credential: &Credential, config: &IpcConfig) -> Result<Self> {
        let stream=open_stream(root).await.map_err(|error|if error.code=="IO_ERROR" {Error::new("HOST_UNAVAILABLE","controller IPC is unavailable; no application request was sent; start or reconnect the host")}else{error})?;
        let client = Self {
            session: Session::attach(stream, config)?,
        };
        // No public handle exists until this one physical authentication has
        // completed. No application frame can overtake client.hello.
        client.session.exchange("client.hello",json!(credential)).await.map_err(|error| {
            if matches!(error.code.as_str(),"OUTCOME_UNKNOWN"|"PROTOCOL_ERROR"|"INVALID_PARAMS") {Error::new("HOST_HANDSHAKE_FAILED","controller authentication handshake did not complete; no application request was sent; reconnect before dispatch")}else{error}
        })?;
        Ok(client)
    }
    /// Source-compatible sequential call surface for existing CLI/adapters.
    pub async fn request(&mut self, method: &str, params: Value) -> Result<Value> {
        self.request_shared(method, params).await
    }
    /// Correlated concurrent exchange on this same authenticated link. Capacity
    /// refusal is pre-submit; cancellation after possible write poisons all
    /// unresolved calls as OUTCOME_UNKNOWN. No application request is resent.
    pub async fn request_shared(&self, method: &str, params: Value) -> Result<Value> {
        if method == "client.hello" {
            return Err(Error::invalid("client.hello is reserved for the transport"));
        }
        self.session.exchange(method, params).await
    }
    /// Transport usability only, never native/service/authorization liveness.
    pub fn is_connected(&self) -> bool {
        self.session.connected()
    }
    /// Close and join the fixed IO tasks while the runtime remains alive.
    /// All clones close together; unresolved RPCs retain uncertain delivery.
    pub async fn close(&self) {
        self.session.close().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{SinkExt, StreamExt};
    use tokio::io::DuplexStream;
    use tokio_util::codec::{FramedRead, FramedWrite, LinesCodec};
    const TEST_FRAME_LIMIT: usize = 4096;

    fn client_on_duplex(stream: DuplexStream) -> Client {
        let config = IpcConfig {
            max_frame_bytes: TEST_FRAME_LIMIT,
            write_timeout_seconds: 1,
            ..IpcConfig::default()
        };
        Client {
            session: Session::attach(Box::new(stream), &config).unwrap(),
        }
    }

    fn fake_peer_client(
        reply: impl FnOnce(&Value) -> String + Send + 'static,
    ) -> (Client, tokio::task::JoinHandle<(Value, Vec<Value>)>) {
        let (client_stream, peer_stream) = tokio::io::duplex(16 * 1024);
        let client = client_on_duplex(client_stream);
        let (read, write) = tokio::io::split(peer_stream);
        let peer = tokio::spawn(async move {
            let mut reader =
                FramedRead::new(read, LinesCodec::new_with_max_length(TEST_FRAME_LIMIT));
            let mut writer =
                FramedWrite::new(write, LinesCodec::new_with_max_length(TEST_FRAME_LIMIT));
            let line = reader
                .next()
                .await
                .expect("Client sends one request")
                .expect("request frame is valid");
            let request: Value = serde_json::from_str(&line).expect("request JSON");
            writer
                .send(reply(&request))
                .await
                .expect("write fake reply");
            let mut following = Vec::new();
            while let Some(line) = reader.next().await {
                let Ok(line) = line else { break };
                following.push(serde_json::from_str(&line).expect("following request JSON"));
            }
            (request, following)
        });
        (client, peer)
    }

    async fn assert_poisoned_client(
        reply: impl FnOnce(&Value) -> String + Send + 'static,
    ) -> Value {
        let (mut client, peer) = fake_peer_client(reply);
        let error = client
            .request(
                "task.create",
                json!({"client_request_id":"ipc-logical-request","project_id":"fixture","spec":{}}),
            )
            .await
            .expect_err("invalid post-send reply must not be treated as settled");
        assert_eq!(error.code, "OUTCOME_UNKNOWN");
        let error = client
            .request("task.create", json!({"client_request_id":"must-not-send"}))
            .await
            .expect_err("uncertain link must refuse another request");
        assert_eq!(error.code, "DISCONNECTED");
        drop(client);
        let (first, following) = peer.await.expect("fake peer joins");
        assert_eq!(first["method"], "task.create");
        assert!(following.is_empty(), "poisoned Client wrote another frame");
        first
    }

    #[tokio::test]
    async fn malformed_and_mismatched_post_send_replies_poison_the_client() {
        assert_poisoned_client(|_| "not-json".to_owned()).await;
        assert_poisoned_client(|request| {
            serde_json::to_string(&json!({
                "jsonrpc":"2.0",
                "id":format!("{}-other", request["id"].as_str().unwrap()),
                "result":{"accepted":true},
            }))
            .unwrap()
        })
        .await;
    }

    #[tokio::test]
    async fn nonnumeric_rpc_error_code_is_unknown_and_poisoning() {
        let first = assert_poisoned_client(|request| {
            serde_json::to_string(&json!({
                "jsonrpc":"2.0",
                "id":request["id"],
                "error":{"code":"-32000","message":"malformed peer error","data":{"code":"FORBIDDEN"}},
            }))
            .unwrap()
        })
        .await;
        assert_eq!(first["id"].as_str().unwrap().len(), 36);
    }
    #[cfg(feature = "qualification-faults")]
    #[tokio::test]
    async fn armed_fault_preserves_page_then_withholds_one_admit_without_replay() {
        use crate::qualification_faults;

        assert!(qualification_faults::arm_single_reply_drop_for_admit());
        let (mut page_client, page_peer) = fake_peer_client(|request| {
            serde_json::to_string(&json!({
                "jsonrpc":"2.0", "id":request["id"],
                "result":{"schema_version":1},
            }))
            .unwrap()
        });
        let page = page_client
            .request("automation.scheduler.page", json!({"scope":"page"}))
            .await
            .expect("an armed admit fault must preserve the page reply");
        assert_eq!(page["schema_version"], 1);
        drop(page_client);
        let (page_request, page_following) = page_peer.await.unwrap();
        assert_eq!(page_request["method"], "automation.scheduler.page");
        assert!(page_following.is_empty());

        let parameters = json!({"page_id":"exact-page", "cut":"exact-cut"});
        let (mut admit_client, admit_peer) = fake_peer_client(|request| {
            serde_json::to_string(&json!({
                "jsonrpc":"2.0", "id":request["id"],
                "result":{"accepted":true},
            }))
            .unwrap()
        });
        let error = admit_client
            .request("automation.scheduler.admit", parameters.clone())
            .await
            .expect_err("the actual matched admit reply must be withheld");
        assert_eq!(
            error.code,
            qualification_faults::QUALIFICATION_REPLY_DROPPED
        );
        assert!(error.message.contains("withheld_frame_bytes="));
        let withheld_ids = qualification_faults::parse_withheld_jsonrpc_ids(&error.message)
            .expect("the diagnostic records the matched JSON-RPC request/reply IDs");
        let refused = admit_client
            .request("automation.scheduler.admit", parameters.clone())
            .await
            .expect_err("a poisoned connection must send no retry frame");
        assert_eq!(refused.code, "DISCONNECTED");
        drop(admit_client);
        let (request, following) = admit_peer.await.unwrap();
        assert_eq!(request["method"], "automation.scheduler.admit");
        assert_eq!(request["params"], parameters);
        assert_eq!(request["id"], withheld_ids.0);
        assert_eq!(withheld_ids.0, withheld_ids.1);
        assert!(
            following.is_empty(),
            "the peer must observe one unaltered admit frame only"
        );
        assert!(
            !qualification_faults::arm_single_reply_drop_for_admit(),
            "a spent process fault must never rearm"
        );
    }
}
