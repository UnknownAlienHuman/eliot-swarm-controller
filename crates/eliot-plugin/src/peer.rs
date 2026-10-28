use crate::protocol::{MAX_EVENT_BYTES, MAX_FRAME_BYTES, RpcError, RpcRequest, RpcResponse};
use eliot_core::{
    BatchAck, Error, MAX_STDERR_DIAGNOSTIC_BYTES, Result, SourceBatch, bound_stderr_diagnostics,
};
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
        let mut response = match result {
            Ok(value) => RpcResponse::success(id, value),
            Err(error) => RpcResponse::failure(id, response_error(error)),
        };
        bound_response_diagnostics(&mut response);
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
    let code = format!("rpc/{}/{semantic}", error.code);
    decode_error_diagnostics(Error::new(code, error.message), error.data.as_ref())
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
    let (wire_code, semantic) = if let Some(rest) = error.code.strip_prefix("rpc/")
        && let Some((wire_code, semantic)) = rest.split_once('/')
        && let Ok(wire_code) = wire_code.parse::<i64>()
    {
        (wire_code, semantic.to_owned())
    } else {
        (-32000, error.code.clone())
    };
    let mut data = json!({"code": semantic});
    // Core serializes optional data using its shared encoded budget. A failed
    // optional conversion must never replace the primary code or message.
    if let Ok(encoded) = serde_json::to_value(&error)
        && let Some(diagnostics) = encoded.get("diagnostics")
    {
        data["diagnostics"] = diagnostics.clone();
    }
    RpcError {
        code: wire_code,
        message: error.message,
        data: Some(data),
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
    decode_error_diagnostics(Error::new(code, error.message), error.data.as_ref())
}

fn decode_error_diagnostics(primary: Error, data: Option<&Value>) -> Error {
    let Some(diagnostics) = data.and_then(|data| data.get("diagnostics")) else {
        return primary;
    };
    // Core's deserializer validates sources/counts/shapes and omits malformed
    // optional data. Mandatory fields here are our already-decoded originals.
    serde_json::from_value(json!({
        "code": primary.code,
        "message": primary.message,
        "diagnostics": diagnostics,
    }))
    .unwrap_or(primary)
}

fn bound_response_diagnostics(response: &mut RpcResponse) {
    let Some(error) = response.error.as_mut() else {
        return;
    };
    let primary = call_error(error.clone());
    let mut diagnostics = primary.diagnostics;
    let Some(data) = error.data.as_mut().and_then(Value::as_object_mut) else {
        return;
    };
    data.remove("diagnostics");
    if diagnostics.is_empty() {
        return;
    }
    let base = serde_json::to_vec(&*response).map_or(usize::MAX, |bytes| bytes.len());
    let budget = MAX_FRAME_BYTES
        .saturating_sub(1)
        .saturating_sub(base.saturating_add(",\"diagnostics\":".len()))
        .min(MAX_STDERR_DIAGNOSTIC_BYTES);
    bound_stderr_diagnostics(&mut diagnostics, budget);
    if !diagnostics.is_empty()
        && let Ok(encoded) = serde_json::to_value(diagnostics)
        && let Some(error) = response.error.as_mut()
        && let Some(data) = error.data.as_mut().and_then(Value::as_object_mut)
    {
        data.insert("diagnostics".to_owned(), encoded);
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use eliot_core::{SecretPolicy, StderrSnapshot, StderrSource, classify_dispatched_error};

    fn snapshot(text: &str) -> StderrSnapshot {
        StderrSnapshot {
            text: text.to_owned(),
            truncated: false,
            eof: true,
            read_error: false,
        }
    }

    #[test]
    fn error_rpc_roundtrip_preserves_both_stderr_sources() {
        let original = Error::new("unknown", "potential native effect")
            .with_stderr(StderrSource::Plugin, snapshot("dummy plugin diagnostic"))
            .with_stderr(StderrSource::Harness, snapshot("dummy harness diagnostic"));
        let wire = response_error(original.clone());
        assert_eq!(call_error(wire.clone()), original);
        let preserved = preserve_rpc_error(wire.clone());
        let roundtrip = response_error(preserved);
        assert_eq!(roundtrip.code, wire.code);
        assert_eq!(roundtrip.message, wire.message);
        assert_eq!(roundtrip.data, wire.data);
    }

    #[test]
    fn malformed_optional_diagnostics_never_replace_the_primary_error() {
        for diagnostics in [
            json!(true),
            json!([{"source": "unknown", "snapshot": {}}]),
            json!([{"source": "plugin", "snapshot": {"text": 123}}]),
        ] {
            let error = call_error(RpcError {
                code: -32000,
                message: "original failure".to_owned(),
                data: Some(json!({"code": "unknown", "diagnostics": diagnostics})),
            });
            assert_eq!(error.code, "unknown");
            assert_eq!(error.message, "original failure");
            assert!(error.diagnostics.is_empty());
        }
    }

    #[test]
    fn control_bytes_and_short_secret_expansion_preserve_settlement() {
        let policy = SecretPolicy::new(vec!["a".to_owned()]);
        let safe = policy.redact_text(&"a\0".repeat(16 * 1024));
        let original = Error::new("unknown", "original failure");
        let with_diagnostics = original
            .clone()
            .with_stderr(StderrSource::Plugin, snapshot(&safe))
            .with_stderr(StderrSource::Harness, snapshot(&safe));
        let mut response = RpcResponse::failure(json!("test"), response_error(with_diagnostics));
        bound_response_diagnostics(&mut response);
        let frame = encode_frame(&response).expect("bounded error response");
        let decoded = call_error(response.error.expect("error branch"));
        assert_eq!(decoded.code, original.code);
        assert_eq!(decoded.message, original.message);
        assert_eq!(
            classify_dispatched_error(&decoded.code),
            classify_dispatched_error(&original.code)
        );
        assert_eq!(decoded.diagnostics.len(), 2);
        assert!(
            decoded
                .diagnostics
                .iter()
                .all(|value| value.snapshot.truncated)
        );
        assert!(
            decoded
                .diagnostics
                .iter()
                .all(|value| !value.snapshot.text.contains('a'))
        );
        assert!(
            serde_json::to_vec(&decoded.diagnostics).unwrap().len() <= MAX_STDERR_DIAGNOSTIC_BYTES
        );
        assert!(serde_json::to_vec(&decoded).unwrap().len() <= 64 * 1024);
        assert!(frame.len() < MAX_FRAME_BYTES);
    }

    #[test]
    fn response_budget_accounts_for_the_mandatory_rpc_envelope() {
        let primary = Error::new("unknown", "original failure");
        let mut response = RpcResponse::failure(json!(""), response_error(primary.clone()));
        let base = serde_json::to_vec(&response).unwrap().len();
        response.id = json!("i".repeat(MAX_FRAME_BYTES - base - 513));
        response.error = Some(response_error(
            primary
                .clone()
                .with_stderr(StderrSource::Plugin, snapshot(&"\0".repeat(16 * 1024)))
                .with_stderr(StderrSource::Harness, snapshot(&"\0".repeat(16 * 1024))),
        ));
        bound_response_diagnostics(&mut response);
        let frame = encode_frame(&response).expect("optional tails cannot overflow the frame");
        let decoded = call_error(response.error.unwrap());
        assert_eq!(decoded.code, primary.code);
        assert_eq!(decoded.message, primary.message);
        assert_eq!(decoded.diagnostics.len(), 2);
        assert!(
            decoded
                .diagnostics
                .iter()
                .all(|value| value.snapshot.truncated)
        );
        assert!(frame.len() < MAX_FRAME_BYTES);
    }
}
