use crate::{IpcConfig, ipc_endpoint};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{path::Path, time::Duration};
use swarm_contracts::{
    Credential,
    error::{Error, Result},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    time,
};
use tokio_util::codec::{FramedRead, FramedWrite, LinesCodec};

trait IoStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> IoStream for T {}
type Stream = Box<dyn IoStream>;

async fn open_stream(root: &Path) -> Result<Stream> {
    let endpoint = ipc_endpoint(root)?;
    #[cfg(unix)]
    {
        Ok(Box::new(tokio::net::UnixStream::connect(&endpoint).await?))
    }
    #[cfg(windows)]
    {
        use tokio::net::windows::named_pipe::ClientOptions;
        // Retry only establishment while no application request has been sent.
        let deadline = time::Instant::now() + Duration::from_secs(5);
        loop {
            match ClientOptions::new().open(&endpoint) {
                Ok(stream) => return Ok(Box::new(stream)),
                Err(error)
                    if error.raw_os_error() == Some(231) && time::Instant::now() < deadline =>
                {
                    time::sleep(Duration::from_millis(25)).await;
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

fn encode(value: &Value, limit: usize) -> Result<String> {
    let line = serde_json::to_string(value)?;
    if line.len() > limit {
        return Err(Error::new(
            "FRAME_TOO_LARGE",
            "use smaller pages or artifact ranges",
        ));
    }
    Ok(line)
}

/// Sequential local client. A failed transport is never reused or auto-replayed.
pub struct Client {
    reader: FramedRead<tokio::io::ReadHalf<Stream>, LinesCodec>,
    writer: FramedWrite<tokio::io::WriteHalf<Stream>, LinesCodec>,
    limit: usize,
    write_timeout: Duration,
    usable: bool,
}

impl Client {
    pub async fn connect(root: &Path, credential: &Credential, config: &IpcConfig) -> Result<Self> {
        let stream = open_stream(root).await.map_err(|error| {
            if error.code == "IO_ERROR" {
                Error::new(
                    "HOST_UNAVAILABLE",
                    "controller IPC is unavailable; no application request was sent; start or reconnect the host",
                )
            } else {
                error
            }
        })?;
        let (read, write) = tokio::io::split(stream);
        let mut client = Self {
            reader: FramedRead::new(
                read,
                LinesCodec::new_with_max_length(config.max_frame_bytes),
            ),
            writer: FramedWrite::new(
                write,
                LinesCodec::new_with_max_length(config.max_frame_bytes),
            ),
            limit: config.max_frame_bytes,
            write_timeout: Duration::from_secs(config.write_timeout_seconds),
            usable: true,
        };
        client
            .exchange("client.hello", json!(credential))
            .await
            .map_err(|error| {
                if matches!(
                    error.code.as_str(),
                    "OUTCOME_UNKNOWN" | "PROTOCOL_ERROR" | "INVALID_PARAMS"
                ) {
                    Error::new(
                        "HOST_HANDSHAKE_FAILED",
                        "controller authentication handshake did not complete; no application request was sent; reconnect before dispatch",
                    )
                } else {
                    error
                }
            })?;
        Ok(client)
    }

    pub async fn request(&mut self, method: &str, params: Value) -> Result<Value> {
        if method == "client.hello" {
            return Err(Error::invalid("client.hello is reserved for the transport"));
        }
        self.exchange(method, params).await
    }

    async fn exchange(&mut self, method: &str, params: Value) -> Result<Value> {
        if !self.usable {
            return Err(Error::new(
                "DISCONNECTED",
                "client link failed; reconnect before another request",
            ));
        }
        let id = uuid::Uuid::new_v4().to_string();
        let line = encode(
            &json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}),
            self.limit,
        )?;
        self.usable = false;
        time::timeout(self.write_timeout, self.writer.send(line))
            .await
            .map_err(|_| unknown_write())?
            .map_err(|_| unknown_write())?;
        let frame = time::timeout(Duration::from_secs(60), self.reader.next())
            .await
            .map_err(|_| {
                Error::new(
                    "OUTCOME_UNKNOWN",
                    "reply missing; inspect the operation before retrying",
                )
            })?
            .ok_or_else(|| Error::new("OUTCOME_UNKNOWN", "server closed before reply"))?
            .map_err(|_| unknown_response())?;
        let response: Value = serde_json::from_str(&frame).map_err(|_| unknown_response())?;
        if response["id"] != id || response["jsonrpc"] != "2.0" {
            return Err(unknown_response());
        }
        // Qualification-only seam. A complete, matched response has just been
        // received and the request was written exactly once and unaltered, so
        // every Store decision it produced is already durable.
        #[cfg(feature = "qualification-faults")]
        if crate::qualification_faults::take_single_reply_drop_if_armed(method) {
            // Poison the connection exactly as an uncertain reply does:
            // usable stays false, so the caller must reconnect before any
            // further request and cannot replay this frame.
            let reply_id = response["id"].as_str().unwrap_or_default();
            return Err(Error::new(
                crate::qualification_faults::QUALIFICATION_REPLY_DROPPED,
                format!(
                    "qualified reply withheld at the transport seam; the request was sent once and is not reusable; withheld_frame_bytes={}; jsonrpc_request_id={}; jsonrpc_reply_id={}",
                    frame.len(),
                    id,
                    reply_id
                ),
            ));
        }
        if let Some(error) = response.get("error") {
            if response.get("result").is_some()
                || !error.is_object()
                || error["code"].as_i64().is_none()
                || error["message"].as_str().is_none()
                || error["data"]["code"].as_str().is_none_or(str::is_empty)
            {
                return Err(unknown_response());
            }
            self.usable = true;
            return Err(Error::new(
                error["data"]["code"].as_str().unwrap_or("RPC_ERROR"),
                error["message"].as_str().unwrap_or("RPC error"),
            ));
        }
        let result = response
            .get("result")
            .cloned()
            .ok_or_else(unknown_response)?;
        self.usable = true;
        Ok(result)
    }
}

fn unknown_response() -> Error {
    Error::new(
        "OUTCOME_UNKNOWN",
        "reply was malformed or could not be matched; preserve the logical request ID and inspect the original operation before retrying",
    )
}

fn unknown_write() -> Error {
    Error::new(
        "OUTCOME_UNKNOWN",
        "request write did not complete; delivery is uncertain; preserve the logical request ID and inspect the original operation before retrying",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::DuplexStream;
    const TEST_FRAME_LIMIT: usize = 4096;

    fn client_on_duplex(stream: DuplexStream) -> Client {
        let stream: Stream = Box::new(stream);
        let (read, write) = tokio::io::split(stream);
        Client {
            reader: FramedRead::new(read, LinesCodec::new_with_max_length(TEST_FRAME_LIMIT)),
            writer: FramedWrite::new(write, LinesCodec::new_with_max_length(TEST_FRAME_LIMIT)),
            limit: TEST_FRAME_LIMIT,
            write_timeout: Duration::from_secs(1),
            usable: true,
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
