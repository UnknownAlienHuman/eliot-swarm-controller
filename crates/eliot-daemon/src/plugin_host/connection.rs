use eliot_core::{Error, Result};
use eliot_plugin::{MAX_FRAME_BYTES, RpcError, RpcRequest, RpcResponse};
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{Mutex, mpsc, oneshot, watch},
    time::{Instant, timeout_at},
};
use tokio_util::codec::{FramedRead, FramedWrite, LinesCodec};

const PIPE_QUEUE_CAPACITY: usize = 16;
const MAX_PENDING_CALLS: usize = 32;

type Pending = HashMap<String, oneshot::Sender<Result<RpcResponse>>>;

#[derive(Clone)]
pub(crate) struct PluginClient {
    inner: Arc<ClientInner>,
}

struct ClientInner {
    writer: mpsc::Sender<WriterMessage>,
    pending: Mutex<Pending>,
    next_id: AtomicU64,
    closed: Mutex<Option<Error>>,
    closed_signal: watch::Sender<()>,
}

struct Outbound {
    frame: String,
    deadline: Instant,
    written: oneshot::Sender<Result<()>>,
}

enum WriterMessage {
    Frame(Outbound),
    Close {
        deadline: Instant,
        closed: oneshot::Sender<Result<()>>,
    },
}

pub(crate) struct PluginConnection {
    client: PluginClient,
    requests: mpsc::Receiver<RpcRequest>,
}

impl PluginConnection {
    pub(crate) fn new<R, W>(reader: R, writer: W) -> Self
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let (writer_tx, writer_rx) = mpsc::channel(PIPE_QUEUE_CAPACITY);
        let (request_tx, request_rx) = mpsc::channel(PIPE_QUEUE_CAPACITY);
        let (closed_signal, _) = watch::channel(());
        let inner = Arc::new(ClientInner {
            writer: writer_tx,
            pending: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(0),
            closed: Mutex::new(None),
            closed_signal,
        });
        let client = PluginClient {
            inner: Arc::clone(&inner),
        };

        tokio::spawn(writer_loop(writer, writer_rx, Arc::clone(&inner)));
        tokio::spawn(reader_loop(reader, request_tx, client.clone()));

        Self {
            client,
            requests: request_rx,
        }
    }

    pub(crate) fn client(&self) -> PluginClient {
        self.client.clone()
    }

    pub(crate) async fn next_request(&mut self) -> Option<RpcRequest> {
        self.requests.recv().await
    }
}

impl PluginClient {
    /// Send one host request and await its matching plugin response while the
    /// independent reader continues routing plugin requests and other responses.
    pub(crate) async fn call(
        &self,
        method: &str,
        params: Value,
        deadline: Instant,
    ) -> Result<Value> {
        if method.trim().is_empty() || !params.is_object() {
            return Err(Error::new(
                "invalid",
                "plugin RPC requires a method and object params",
            ));
        }
        if let Some(error) = self.inner.closed.lock().await.clone() {
            return Err(error);
        }

        let sequence = self
            .inner
            .next_id
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map_err(|_| Error::new("internal", "plugin RPC id space exhausted"))?;
        let id = Value::String(format!("host-{sequence}"));
        let key = id_key(&id)?;
        let request = RpcRequest {
            jsonrpc: "2.0".to_owned(),
            id: id.clone(),
            method: method.to_owned(),
            params,
        };
        let frame = encode_frame(&request)?;

        let (response_tx, response_rx) = oneshot::channel();
        {
            let mut pending = self.inner.pending.lock().await;
            if pending.len() >= MAX_PENDING_CALLS {
                return Err(Error::new(
                    "busy",
                    "plugin has reached its bounded in-flight request limit",
                ));
            }
            pending.insert(key.clone(), response_tx);
        }

        if let Err(error) = self.send_frame(frame, deadline).await {
            self.inner.pending.lock().await.remove(&key);
            return Err(error);
        }

        let response = match timeout_at(deadline, response_rx).await {
            Ok(Ok(Ok(response))) => response,
            Ok(Ok(Err(error))) => {
                self.inner.pending.lock().await.remove(&key);
                return Err(unknown_transport_error(error));
            }
            Ok(Err(_)) => {
                self.inner.pending.lock().await.remove(&key);
                return Err(Error::new(
                    "unknown",
                    "plugin connection ended while a native outcome was pending",
                ));
            }
            Err(_) => {
                self.inner.pending.lock().await.remove(&key);
                return Err(Error::new(
                    "unknown",
                    format!("plugin method {method} timed out after dispatch"),
                ));
            }
        };

        if let Some(error) = response.error {
            let code = error
                .data
                .as_ref()
                .and_then(|data| data.get("code"))
                .and_then(Value::as_str)
                .filter(|code| {
                    !code.is_empty()
                        && code.len() <= 64
                        && code
                            .bytes()
                            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
                });
            let Some(code) = code else {
                return Err(Error::new(
                    "unknown",
                    "plugin outcome unknown: plugin_error_reply_missing_code",
                ));
            };
            return Err(Error::new(code, error.message));
        }

        response.result.ok_or_else(|| {
            Error::new(
                "unknown",
                "plugin outcome unknown: plugin_response_missing_result",
            )
        })
    }

    pub(crate) async fn respond(
        &self,
        id: Value,
        result: std::result::Result<Value, Error>,
        deadline: Instant,
    ) -> Result<()> {
        let response = match result {
            Ok(result) => RpcResponse {
                jsonrpc: "2.0".to_owned(),
                id,
                result: Some(result),
                error: None,
            },
            Err(error) => RpcResponse {
                jsonrpc: "2.0".to_owned(),
                id,
                result: None,
                error: Some(RpcError {
                    code: -32000,
                    message: error.message,
                    data: Some(json!({"code": error.code})),
                }),
            },
        };
        self.send_frame(encode_frame(&response)?, deadline).await
    }

    /// Flush queued protocol frames, then close only the plugin's stdin write
    /// half. The independent stdout reader remains alive to drain EOF.
    pub(crate) async fn close_input(&self, deadline: Instant) -> Result<()> {
        let (closed_tx, closed_rx) = oneshot::channel();
        let message = WriterMessage::Close {
            deadline,
            closed: closed_tx,
        };
        match timeout_at(deadline, self.inner.writer.send(message)).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => {
                return Err(Error::new(
                    "unavailable",
                    "plugin input writer is already closed",
                ));
            }
            Err(_) => {
                return Err(Error::new(
                    "unknown",
                    "plugin input close was not queued before its deadline",
                ));
            }
        }
        match timeout_at(deadline, closed_rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(Error::new(
                "unknown",
                "plugin input writer exited before close completed",
            )),
            Err(_) => Err(Error::new(
                "unknown",
                "plugin input close exceeded its deadline",
            )),
        }
    }

    async fn send_frame(&self, frame: String, deadline: Instant) -> Result<()> {
        let (written_tx, written_rx) = oneshot::channel();
        let outbound = WriterMessage::Frame(Outbound {
            frame,
            deadline,
            written: written_tx,
        });

        match timeout_at(deadline, self.inner.writer.send(outbound)).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => {
                return Err(Error::new(
                    "unavailable",
                    "plugin connection closed before the frame was written",
                ));
            }
            Err(_) => {
                return Err(Error::new(
                    "busy",
                    "plugin protocol writer did not accept the frame before its deadline",
                ));
            }
        }

        let result = match timeout_at(deadline, written_rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(Error::new(
                "unknown",
                "plugin protocol writer stopped during a frame",
            )),
            Err(_) => Err(Error::new(
                "unknown",
                "plugin protocol frame write exceeded its deadline",
            )),
        };
        // Once queued, the writer may have sent any prefix or the whole frame.
        // A framing/connection failure cannot prove that native I/O did not occur.
        result.map_err(unknown_transport_error)
    }
}

async fn writer_loop<W>(
    writer: W,
    mut outgoing: mpsc::Receiver<WriterMessage>,
    inner: Arc<ClientInner>,
) where
    W: AsyncWrite + Unpin,
{
    let mut writer = FramedWrite::new(
        writer,
        LinesCodec::new_with_max_length(MAX_FRAME_BYTES.saturating_sub(1)),
    );
    let mut closed_signal = inner.closed_signal.subscribe();
    if inner.closed.lock().await.is_some() {
        return;
    }
    loop {
        let message = tokio::select! {
            _ = closed_signal.changed() => return,
            message = outgoing.recv() => match message {
                Some(message) => message,
                None => break,
            },
        };
        let outbound = match message {
            WriterMessage::Frame(outbound) => outbound,
            WriterMessage::Close { deadline, closed } => {
                let result = match timeout_at(deadline, SinkExt::<String>::close(&mut writer)).await
                {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(_)) => Err(Error::new("unknown", "plugin stdin close failed")),
                    Err(_) => Err(Error::new("unknown", "plugin stdin close timed out")),
                };
                let close_error = result
                    .as_ref()
                    .err()
                    .cloned()
                    .unwrap_or_else(|| Error::new("unavailable", "plugin stdin was closed"));
                let _ = closed.send(result);
                fail_connection(&inner, close_error).await;
                return;
            }
        };
        let result = match timeout_at(outbound.deadline, writer.send(outbound.frame)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(Error::new(
                "unknown",
                "plugin protocol write failed; the native outcome may be unknown",
            )),
            Err(_) => Err(Error::new(
                "unknown",
                "plugin protocol write timed out; the native outcome may be unknown",
            )),
        };
        let failed = result.is_err();
        let failure = result.as_ref().err().cloned();
        let _ = outbound.written.send(result);
        if failed {
            fail_connection(
                &inner,
                failure.unwrap_or_else(|| Error::new("unknown", "plugin writer stopped")),
            )
            .await;
            return;
        }
    }
    fail_connection(
        &inner,
        Error::new("unknown", "plugin protocol writer was closed"),
    )
    .await;
}

async fn reader_loop<R>(reader: R, requests: mpsc::Sender<RpcRequest>, client: PluginClient)
where
    R: AsyncRead + Unpin,
{
    let mut reader = FramedRead::new(
        reader,
        LinesCodec::new_with_max_length(MAX_FRAME_BYTES.saturating_sub(1)),
    );
    let failure = loop {
        match reader.next().await {
            Some(Ok(line)) if line.len().saturating_add(1) <= MAX_FRAME_BYTES => {
                match decode_frame(&line) {
                    Ok(Incoming::Response(response)) => {
                        route_response(response, &client.inner).await
                    }
                    Ok(Incoming::Request(request)) => {
                        let id = request.id.clone();
                        let is_event_commit = request.method == "events.put";
                        let response_error = match requests.try_send(request) {
                            Ok(()) => None,
                            Err(mpsc::error::TrySendError::Full(_)) if is_event_commit => {
                                Some(Error::new(
                                    "unknown",
                                    "events.put outcome unknown: plugin_request_queue_overflow",
                                ))
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) if is_event_commit => {
                                Some(Error::new(
                                    "unknown",
                                    "events.put outcome unknown: plugin_request_queue_closed",
                                ))
                            }
                            Err(mpsc::error::TrySendError::Full(_)) => {
                                Some(Error::new("busy", "plugin request queue is full"))
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => {
                                Some(Error::new("unavailable", "plugin request queue is closed"))
                            }
                        };
                        if let Some(error) = response_error {
                            let _ = client
                                .respond(id, Err(error), Instant::now() + Duration::from_secs(5))
                                .await;
                        }
                    }
                    Err(error) => {
                        break Error::new(
                            "unavailable",
                            format!("plugin sent an invalid protocol frame: {}", error.message),
                        );
                    }
                }
            }
            Some(Ok(_)) => {
                break Error::new("frame_too_large", "plugin frame exceeds 1 MiB");
            }
            Some(Err(_)) => {
                break Error::new("unavailable", "plugin sent an invalid or oversized frame");
            }
            None => {
                break Error::new(
                    "unknown",
                    "plugin output closed while a native outcome may be pending",
                );
            }
        }
    };
    fail_connection(&client.inner, failure).await;
}

async fn route_response(response: RpcResponse, inner: &ClientInner) {
    let Ok(key) = id_key(&response.id) else {
        return;
    };
    if let Some(sender) = inner.pending.lock().await.remove(&key) {
        let _ = sender.send(Ok(response));
    }
}

async fn fail_pending(inner: &ClientInner, error: Error) {
    let error = unknown_transport_error(error);
    let pending = std::mem::take(&mut *inner.pending.lock().await);
    for (_, sender) in pending {
        let _ = sender.send(Err(error.clone()));
    }
}

fn unknown_transport_error(error: Error) -> Error {
    if error.code == "unknown"
        && error
            .message
            .strip_prefix("plugin outcome unknown: ")
            .is_some_and(|cause| {
                cause.starts_with("plugin_transport_")
                    && cause.len() <= 64
                    && cause
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
            })
    {
        return error;
    }
    // Only a bounded cause code crosses this boundary. Never copy a raw frame,
    // I/O diagnostic, or plugin error body into a persisted unknown cause.
    let cause: String = error
        .code
        .chars()
        .filter(|character| character.is_ascii_alphanumeric() || *character == '_')
        .take(40)
        .collect();
    let cause = if cause.is_empty() { "error" } else { &cause };
    Error::new(
        "unknown",
        format!("plugin outcome unknown: plugin_transport_{cause}"),
    )
}

async fn fail_connection(inner: &ClientInner, error: Error) {
    let first = {
        let mut closed = inner.closed.lock().await;
        if closed.is_some() {
            false
        } else {
            *closed = Some(error.clone());
            true
        }
    };
    if first {
        inner.closed_signal.send_replace(());
        fail_pending(inner, error).await;
    }
}

enum Incoming {
    Request(RpcRequest),
    Response(RpcResponse),
}

fn decode_frame(line: &str) -> Result<Incoming> {
    let value: Value = serde_json::from_str(line)
        .map_err(|_| Error::new("invalid", "plugin sent invalid JSON"))?;
    let Some(object) = value.as_object() else {
        return Err(Error::new(
            "invalid",
            "plugin JSON-RPC frame must be an object",
        ));
    };
    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return Err(Error::new("invalid", "plugin JSON-RPC version must be 2.0"));
    }

    if object.contains_key("method") {
        let request: RpcRequest = serde_json::from_value(value)
            .map_err(|_| Error::new("invalid", "plugin request is malformed"))?;
        if !valid_id(&request.id) || !request.params.is_object() {
            return Err(Error::new(
                "invalid",
                "plugin request requires an id and object params",
            ));
        }
        Ok(Incoming::Request(request))
    } else {
        let response: RpcResponse = serde_json::from_value(value)
            .map_err(|_| Error::new("invalid", "plugin response is malformed"))?;
        if !valid_id(&response.id) || response.error.is_some() == response.result.is_some() {
            return Err(Error::new(
                "invalid",
                "plugin response requires exactly one result or error",
            ));
        }
        Ok(Incoming::Response(response))
    }
}

fn encode_frame<T: Serialize>(message: &T) -> Result<String> {
    let frame = serde_json::to_string(message)
        .map_err(|_| Error::new("internal", "plugin JSON-RPC frame could not be encoded"))?;
    if frame.len().saturating_add(1) > MAX_FRAME_BYTES {
        return Err(Error::new(
            "frame_too_large",
            "plugin JSON-RPC frame exceeds 1 MiB",
        ));
    }
    Ok(frame)
}

fn id_key(id: &Value) -> Result<String> {
    if !valid_id(id) {
        return Err(Error::new("invalid", "JSON-RPC id must be a scalar"));
    }
    serde_json::to_string(id).map_err(|_| Error::new("invalid", "JSON-RPC id could not be encoded"))
}

fn valid_id(id: &Value) -> bool {
    id.is_null() || id.is_string() || id.is_number()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, duplex, split};

    enum PeerFault {
        InvalidJson,
        InvalidResponse,
        OversizedFrame,
        Eof,
        ErrorReply(Option<Value>),
    }

    async fn error_after_written_request(fault: PeerFault) -> Error {
        let (host, peer) = duplex(64);
        let (reader, writer) = split(host);
        let connection = PluginConnection::new(reader, writer);
        let client = connection.client();
        let until = Instant::now() + Duration::from_secs(5);
        let responder =
            tokio::spawn(async move {
                let mut peer = BufReader::new(peer);
                let mut line = String::new();
                let read = timeout_at(until, peer.read_line(&mut line))
                    .await
                    .expect("request deadline")
                    .expect("read request");
                assert!(read > 0 && line.ends_with('\n'));
                let request: RpcRequest =
                    serde_json::from_str(&line).expect("complete written request");
                let bytes = match fault {
                PeerFault::InvalidJson => b"not a protocol frame\n".to_vec(),
                PeerFault::InvalidResponse => format!("{}\n", json!({
                    "jsonrpc": "2.0", "id": request.id,
                })).into_bytes(),
                PeerFault::OversizedFrame => {
                    let mut bytes = vec![b'x'; MAX_FRAME_BYTES];
                    bytes.push(b'\n');
                    bytes
                }
                PeerFault::Eof => return,
                PeerFault::ErrorReply(data) => format!("{}\n", json!({
                    "jsonrpc": "2.0", "id": request.id,
                    "error": {"code": -32000, "message": "untrusted plugin body", "data": data},
                })).into_bytes(),
            };
                // An oversized peer write may meet reader closure at the frame cap.
                let _ = timeout_at(until, peer.get_mut().write_all(&bytes))
                    .await
                    .expect("response deadline");
            });
        let error = client
            .call("agent.command", json!({"verb": "send"}), until)
            .await
            .expect_err("fault must not prove a definite outcome");
        responder.await.expect("peer task");
        error
    }

    #[tokio::test]
    async fn transport_faults_after_written_request_are_unknown() {
        for fault in [
            PeerFault::InvalidJson,
            PeerFault::InvalidResponse,
            PeerFault::OversizedFrame,
            PeerFault::Eof,
        ] {
            let error = error_after_written_request(fault).await;
            assert_eq!(error.code, "unknown");
            assert!(
                error
                    .message
                    .starts_with("plugin outcome unknown: plugin_transport_")
            );
            assert!(!error.message.contains("not a protocol frame"));
        }
    }

    #[tokio::test]
    async fn error_reply_without_explicit_code_is_unknown() {
        for data in [
            None,
            Some(json!({})),
            Some(json!({"code": ""})),
            Some(json!({"code": "bad\ncode"})),
        ] {
            let error = error_after_written_request(PeerFault::ErrorReply(data)).await;
            assert_eq!(error.code, "unknown");
            assert!(error.message.contains("plugin_error_reply_missing_code"));
            assert!(!error.message.contains("untrusted plugin body"));
        }
    }

    #[tokio::test]
    async fn explicit_no_io_error_code_is_preserved() {
        let error = error_after_written_request(PeerFault::ErrorReply(Some(json!({
            "code": "native_not_dispatched",
        }))))
        .await;
        assert_eq!(error.code, "native_not_dispatched");
    }
}
