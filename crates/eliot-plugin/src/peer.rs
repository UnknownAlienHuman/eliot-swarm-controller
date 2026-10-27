use crate::protocol::{MAX_EVENT_BYTES, MAX_FRAME_BYTES, RpcError, RpcRequest, RpcResponse};
use eliot_core::{BatchAck, Error, Result, SourceBatch};
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{Stdin, Stdout, stdin, stdout},
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot},
    time::{Instant, timeout_at},
};
use tokio_util::codec::{FramedRead, FramedWrite, LinesCodec};

const WRITE_QUEUE_CAPACITY: usize = 16;
const MAX_PENDING_CALLS: usize = 16;
const INVALID_FRAME_RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);

type PendingCalls = Arc<StdMutex<HashMap<String, oneshot::Sender<Result<Value>>>>>;

struct OutboundFrame {
    encoded: String,
    written: oneshot::Sender<Result<()>>,
}

/// Eliot's plugin-side JSON-RPC 2.0 NDJSON peer over stdin/stdout.
///
/// The peer owns the only stdin reader. Callers keep polling `next_request` while
/// outbound calls are pending so responses are routed without a second reader.
pub struct Peer {
    reader: FramedRead<Stdin, LinesCodec>,
    client: PeerClient,
    _writer_task: tokio::task::JoinHandle<()>,
}

/// Cloneable handle for plugin-originated requests and host responses.
#[derive(Clone)]
pub struct PeerClient {
    writes: mpsc::Sender<OutboundFrame>,
    pending: PendingCalls,
    pending_slots: Arc<Semaphore>,
    next_id: Arc<AtomicU64>,
}

struct PreparedCall {
    id: String,
    encoded: String,
}

struct PendingGuard {
    pending: PendingCalls,
    id: String,
}

/// Serializes `events.put` commits for one source and latches uncertain outcomes.
pub struct CommitSink {
    client: PeerClient,
    credential: String,
    unknown: bool,
}

enum Incoming {
    Request(RpcRequest),
    Response(RpcResponse),
}

impl Peer {
    /// Connect the Eliot plugin peer to the process's stdin and stdout.
    pub fn stdio() -> Self {
        let (writes, write_rx) = mpsc::channel(WRITE_QUEUE_CAPACITY);
        let pending = Arc::new(StdMutex::new(HashMap::new()));
        let writer = FramedWrite::new(stdout(), LinesCodec::new());
        let writer_task = tokio::spawn(writer_loop(writer, write_rx, Arc::clone(&pending)));

        let client = PeerClient {
            writes,
            pending,
            pending_slots: Arc::new(Semaphore::new(MAX_PENDING_CALLS)),
            next_id: Arc::new(AtomicU64::new(0)),
        };
        let reader = FramedRead::new(
            stdin(),
            LinesCodec::new_with_max_length(MAX_FRAME_BYTES.saturating_sub(1)),
        );

        Self {
            reader,
            client,
            _writer_task: writer_task,
        }
    }

    /// Clone the handle used to send plugin requests and host responses.
    pub fn client(&self) -> PeerClient {
        self.client.clone()
    }

    /// Read until one host request arrives, routing response frames to pending calls.
    pub async fn next_request(&mut self) -> Option<RpcRequest> {
        loop {
            match self.reader.next().await {
                Some(Ok(line)) if line.len().saturating_add(1) <= MAX_FRAME_BYTES => {
                    match decode_frame(&line) {
                        Ok(Incoming::Request(request)) => return Some(request),
                        Ok(Incoming::Response(response)) => {
                            self.client.route_response(response);
                        }
                        Err(error) => {
                            let deadline = Instant::now() + INVALID_FRAME_RESPONSE_TIMEOUT;
                            if self
                                .client
                                .respond(Value::Null, Err(preserve_rpc_error(error)), deadline)
                                .await
                                .is_err()
                            {
                                self.client.fail_pending(Error::new(
                                    "transport_write",
                                    "plugin protocol write failed",
                                ));
                                return None;
                            }
                        }
                    }
                }
                Some(Ok(_)) | Some(Err(_)) => {
                    self.client.fail_pending(Error::new(
                        "invalid_frame",
                        "invalid or oversized NDJSON frame",
                    ));
                    return None;
                }
                None => {
                    self.client
                        .fail_pending(Error::new("transport_closed", "plugin input closed"));
                    return None;
                }
            }
        }
    }
}

impl PeerClient {
    /// Send a plugin-originated request and await its matching host response.
    pub async fn call(&self, method: &str, params: Value, deadline: Instant) -> Result<Value> {
        let permit = self.acquire_pending(deadline).await?;
        let prepared = self.prepare_call(method, params)?;
        self.call_prepared(prepared, deadline, permit).await
    }

    /// Respond to a host request before its absolute deadline.
    pub async fn respond(&self, id: Value, result: Result<Value>, deadline: Instant) -> Result<()> {
        let response = match result {
            Ok(value) => RpcResponse::success(id, value),
            Err(error) => RpcResponse::failure(id, response_error(error)),
        };
        self.write_encoded(encode_frame(&response)?, deadline).await
    }

    fn prepare_call(&self, method: &str, params: Value) -> Result<PreparedCall> {
        let sequence = self
            .next_id
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .map_err(|_| Error::new("internal", "plugin request id space exhausted"))?;
        let id = format!("plugin-call-{sequence}");
        let request = RpcRequest {
            jsonrpc: "2.0".to_owned(),
            id: Value::String(id.clone()),
            method: method.to_owned(),
            params,
        };
        Ok(PreparedCall {
            id,
            encoded: encode_frame(&request)?,
        })
    }

    async fn acquire_pending(&self, deadline: Instant) -> Result<OwnedSemaphorePermit> {
        timeout_at(deadline, Arc::clone(&self.pending_slots).acquire_owned())
            .await
            .map_err(|_| timeout_error("plugin request capacity deadline elapsed"))?
            .map_err(|_| Error::new("transport_closed", "plugin peer is closed"))
    }

    async fn call_prepared(
        &self,
        prepared: PreparedCall,
        deadline: Instant,
        _permit: OwnedSemaphorePermit,
    ) -> Result<Value> {
        let (response_tx, response_rx) = oneshot::channel();
        {
            let mut pending = self
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            pending.insert(prepared.id.clone(), response_tx);
        }
        let _pending_guard = PendingGuard {
            pending: Arc::clone(&self.pending),
            id: prepared.id,
        };

        self.write_encoded(prepared.encoded, deadline).await?;
        timeout_at(deadline, response_rx)
            .await
            .map_err(|_| timeout_error("plugin response deadline elapsed"))?
            .map_err(|_| Error::new("transport_closed", "plugin response channel closed"))?
    }

    async fn write_encoded(&self, encoded: String, deadline: Instant) -> Result<()> {
        let (written_tx, written_rx) = oneshot::channel();
        timeout_at(
            deadline,
            self.writes.send(OutboundFrame {
                encoded,
                written: written_tx,
            }),
        )
        .await
        .map_err(|_| timeout_error("plugin write queue deadline elapsed"))?
        .map_err(|_| Error::new("transport_closed", "plugin writer is closed"))?;

        timeout_at(deadline, written_rx)
            .await
            .map_err(|_| timeout_error("plugin write deadline elapsed"))?
            .map_err(|_| Error::new("transport_closed", "plugin writer stopped"))?
    }

    fn route_response(&self, response: RpcResponse) {
        let Some(id) = response.id.as_str() else {
            return;
        };
        let sender = self
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(id);
        let Some(sender) = sender else {
            return;
        };
        let result = match (response.result, response.error) {
            (Some(value), None) => Ok(value),
            (None, Some(error)) => Err(call_error(error)),
            _ => Err(Error::new(
                "invalid_response",
                "JSON-RPC response must contain exactly one result or error",
            )),
        };
        let _ = sender.send(result);
    }

    fn fail_pending(&self, error: Error) {
        let pending = {
            let mut pending = self
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            std::mem::take(&mut *pending)
        };
        for (_, sender) in pending {
            let _ = sender.send(Err(error.clone()));
        }
    }
}

impl CommitSink {
    /// Create a commit sink bound to one host credential.
    pub fn new(client: PeerClient, credential: impl Into<String>) -> Self {
        Self {
            client,
            credential: credential.into(),
            unknown: false,
        }
    }

    /// Commit one complete event batch and validate its acknowledgement.
    pub async fn commit(&mut self, batch: SourceBatch, deadline: Instant) -> Result<BatchAck> {
        if self.unknown {
            return Err(unknown_batch_error());
        }
        if self.credential.len() > crate::protocol::MAX_CREDENTIAL_BYTES {
            return Err(Error::new(
                "invalid",
                "event credential exceeds its size limit",
            ));
        }
        for event in &batch.events {
            let encoded_data = serde_json::to_vec(&event.data)
                .map_err(|_| Error::new("internal", "event data could not be encoded"))?;
            if encoded_data.len() > MAX_EVENT_BYTES {
                return Err(Error::new(
                    "frame_too_large",
                    "event data exceeds the 64 KiB persisted-event limit",
                ));
            }
        }

        let expected_count = i64::try_from(batch.events.len())
            .map_err(|_| Error::new("frame_too_large", "event batch is too large"))?;
        self.unknown = true;
        let permit = match self.client.acquire_pending(deadline).await {
            Ok(permit) => permit,
            Err(_) => return Err(unknown_batch_error()),
        };
        let mut params = match serde_json::to_value(batch) {
            Ok(params) => params,
            Err(_) => {
                self.unknown = false;
                return Err(Error::new("internal", "event batch could not be encoded"));
            }
        };
        let Some(params) = params.as_object_mut() else {
            self.unknown = false;
            return Err(Error::new(
                "internal",
                "event batch encoding was not an object",
            ));
        };
        params.insert(
            "credential".to_owned(),
            Value::String(self.credential.clone()),
        );

        // A local frame-size rejection clears the latch because no frame was queued.
        let prepared = match self
            .client
            .prepare_call("events.put", Value::Object(std::mem::take(params)))
        {
            Ok(prepared) => prepared,
            Err(error) => {
                self.unknown = false;
                return Err(error);
            }
        };
        let result = self.client.call_prepared(prepared, deadline, permit).await;
        let value = match result {
            Ok(value) => value,
            Err(_) => return Err(unknown_batch_error()),
        };
        let ack: BatchAck = match serde_json::from_value(value) {
            Ok(ack) => ack,
            Err(_) => return Err(unknown_batch_error()),
        };
        let actual_count = ack
            .last_seq
            .checked_sub(ack.first_seq)
            .and_then(|count| count.checked_add(1));
        if ack.first_seq <= 0 || actual_count != Some(expected_count) {
            return Err(unknown_batch_error());
        }

        self.unknown = false;
        Ok(ack)
    }

    /// Whether a commit was sent without a valid acknowledgement.
    pub fn is_unknown(&self) -> bool {
        self.unknown
    }
}

pub(crate) fn preserve_rpc_error(error: RpcError) -> Error {
    let semantic = error
        .data
        .as_ref()
        .and_then(|data| data.get("code"))
        .and_then(Value::as_str)
        .unwrap_or("rpc_error");
    Error::new(format!("rpc/{}/{semantic}", error.code), error.message)
}

async fn writer_loop(
    mut writer: FramedWrite<Stdout, LinesCodec>,
    mut writes: mpsc::Receiver<OutboundFrame>,
    pending: PendingCalls,
) {
    while let Some(outbound) = writes.recv().await {
        let result = writer
            .send(outbound.encoded)
            .await
            .map_err(|_| Error::new("transport_write", "plugin protocol write failed"));
        let failure = result.as_ref().err().cloned();
        let _ = outbound.written.send(result);
        if let Some(error) = failure {
            let pending_calls = {
                let mut pending = pending
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                std::mem::take(&mut *pending)
            };
            for (_, sender) in pending_calls {
                let _ = sender.send(Err(error.clone()));
            }
            return;
        }
    }
    let _ = SinkExt::<String>::close(&mut writer).await;
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        self.pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.id);
    }
}

fn decode_frame(line: &str) -> std::result::Result<Incoming, RpcError> {
    let value: Value = serde_json::from_str(line)
        .map_err(|_| protocol_error("invalid", -32700, "invalid JSON-RPC frame"))?;
    let Some(object) = value.as_object() else {
        return Err(protocol_error(
            "invalid",
            -32600,
            "JSON-RPC frame must be an object",
        ));
    };
    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return Err(protocol_error(
            "invalid",
            -32600,
            "JSON-RPC version must be 2.0",
        ));
    }

    if object.contains_key("method") {
        let request: RpcRequest = serde_json::from_value(value)
            .map_err(|_| protocol_error("invalid", -32600, "invalid JSON-RPC request"))?;
        if !valid_id(&request.id) || !request.params.is_object() {
            return Err(protocol_error(
                "invalid",
                -32602,
                "JSON-RPC request requires an id and object params",
            ));
        }
        return Ok(Incoming::Request(request));
    }

    let response: RpcResponse = serde_json::from_value(value)
        .map_err(|_| protocol_error("invalid", -32600, "invalid JSON-RPC response"))?;
    if !valid_id(&response.id) || response.error.is_some() == response.result.is_some() {
        return Err(protocol_error(
            "invalid",
            -32600,
            "JSON-RPC response must contain exactly one result or error",
        ));
    }
    Ok(Incoming::Response(response))
}

fn encode_frame<T: Serialize>(message: &T) -> Result<String> {
    let encoded = serde_json::to_string(message)
        .map_err(|_| Error::new("internal", "JSON-RPC frame could not be encoded"))?;
    if encoded.len().saturating_add(1) > MAX_FRAME_BYTES {
        return Err(Error::new(
            "frame_too_large",
            "encoded JSON-RPC frame exceeds 1 MiB",
        ));
    }
    Ok(encoded)
}

fn response_error(error: Error) -> RpcError {
    if let Some(rest) = error.code.strip_prefix("rpc/")
        && let Some((wire_code, semantic)) = rest.split_once('/')
        && let Ok(wire_code) = wire_code.parse::<i64>()
    {
        return RpcError {
            code: wire_code,
            message: error.message,
            data: Some(json!({"code": semantic})),
        };
    }
    RpcError {
        code: -32000,
        message: error.message,
        data: Some(json!({"code": error.code})),
    }
}

fn call_error(error: RpcError) -> Error {
    let code = error
        .data
        .as_ref()
        .and_then(|data| data.get("code"))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(|| format!("rpc_{}", error.code));
    Error::new(code, error.message)
}

fn protocol_error(code: &str, rpc_code: i64, message: &str) -> RpcError {
    RpcError {
        code: rpc_code,
        message: message.to_owned(),
        data: Some(json!({"code": code})),
    }
}

fn timeout_error(message: &str) -> Error {
    Error::new("timeout", message)
}

fn unknown_batch_error() -> Error {
    Error::new(
        "unknown",
        "event batch outcome is unknown; do not replay it",
    )
}

fn valid_id(id: &Value) -> bool {
    id.is_null() || id.is_string() || id.is_number()
}
