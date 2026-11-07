use crate::protocol::{
    DecodeErrorKind, EncodeErrorKind, MAX_EVENT_BYTES, MAX_FRAME_BYTES,
    PrivateIncoming as Incoming, RpcError, RpcRequest, RpcResponse, decode_private_frame,
    encode_private_frame,
};
use crate::sync::LockExt;
use eliot_core::{
    BatchAck, Error, MAX_STDERR_DIAGNOSTIC_BYTES, Result, SourceBatch, bound_stderr_diagnostics,
};
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use serde_json::{Value, json};
use std::future::Future;
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, Stdin, stdin, stdout},
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot},
    time::{Instant, timeout_at},
};
use tokio_util::codec::{FramedRead, FramedWrite, LinesCodec};

const WRITE_QUEUE_CAPACITY: usize = 16;
const MAX_PENDING_CALLS: usize = 16;
const INVALID_FRAME_RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);

/// Outcome of a low-priority snapshot that yields to the next host request.
pub enum SnapshotDispatch<T> {
    Ready(T),
    Preempted(Option<RpcRequest>),
}

/// Poll one snapshot concurrently with the host request queue. A control
/// arriving first cancels the readback future so the adapter can serve it.
pub async fn preemptible_snapshot<F>(
    requests: &mut mpsc::Receiver<RpcRequest>,
    snapshot: F,
) -> SnapshotDispatch<F::Output>
where
    F: Future,
{
    tokio::select! {
        biased;
        request = requests.recv() => SnapshotDispatch::Preempted(request),
        result = snapshot => SnapshotDispatch::Ready(result),
    }
}

type PendingCalls = Arc<StdMutex<HashMap<String, oneshot::Sender<Result<Value>>>>>;

struct OutboundFrame {
    encoded: String,
    written: oneshot::Sender<Result<()>>,
}

/// Eliot's plugin-side JSON-RPC 2.0 NDJSON peer over stdin/stdout or arbitrary I/O.
///
/// The peer owns the only stream reader. Callers keep polling `next_request` while
/// outbound calls are pending so responses are routed without a second reader.
pub struct Peer<R = Stdin> {
    reader: FramedRead<R, LinesCodec>,
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
    // One unacknowledged batch per (agent_id, generation, source).
    unknown_batches: HashMap<(String, i64, String), SourceBatch>,
}

impl<R> Peer<R>
where
    R: AsyncRead + Unpin,
{
    /// Connect the Eliot plugin peer to arbitrary reader and writer streams.
    pub fn from_io<W>(reader: R, writer: W) -> Self
    where
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let (writes, write_rx) = mpsc::channel(WRITE_QUEUE_CAPACITY);
        let pending = Arc::new(StdMutex::new(HashMap::new()));
        let writer = FramedWrite::new(writer, LinesCodec::new());
        let writer_task = tokio::spawn(writer_loop(writer, write_rx, Arc::clone(&pending)));

        let client = PeerClient {
            writes,
            pending,
            pending_slots: Arc::new(Semaphore::new(MAX_PENDING_CALLS)),
            next_id: Arc::new(AtomicU64::new(0)),
        };
        let reader = FramedRead::new(
            reader,
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

impl Peer<Stdin> {
    /// Connect the Eliot plugin peer to the process's stdin and stdout.
    pub fn stdio() -> Self {
        Self::from_io(stdin(), stdout())
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
            let mut pending = self.pending.lock_recover();
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
        let sender = self.pending.lock_recover().remove(id);
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
            let mut pending = self.pending.lock_recover();
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
            unknown_batches: HashMap::new(),
        }
    }

    /// Commit one complete event batch and validate its acknowledgement.
    pub async fn commit(&mut self, batch: SourceBatch, deadline: Instant) -> Result<BatchAck> {
        let key = Self::scope_key(&batch);
        if self.unknown_batches.contains_key(&key) {
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
        if batch.events.is_empty() {
            return Err(Error::new(
                "invalid",
                "native event batch must contain at least one event",
            ));
        }

        let expected_count = i64::try_from(batch.events.len())
            .map_err(|_| Error::new("frame_too_large", "event batch is too large"))?;
        let expected_cursor = batch.cursor.clone();
        self.unknown_batches.insert(key.clone(), batch.clone());
        let permit = match self.client.acquire_pending(deadline).await {
            Ok(permit) => permit,
            Err(_) => return Err(unknown_batch_error()),
        };
        let mut params = match serde_json::to_value(batch) {
            Ok(params) => params,
            Err(_) => {
                self.unknown_batches.remove(&key);
                return Err(Error::new("internal", "event batch could not be encoded"));
            }
        };
        let Some(params) = params.as_object_mut() else {
            self.unknown_batches.remove(&key);
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
                self.unknown_batches.remove(&key);
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
        if ack.first_seq <= 0
            || actual_count != Some(expected_count)
            || ack.cursor != expected_cursor
        {
            return Err(unknown_batch_error());
        }

        self.unknown_batches.remove(&key);
        Ok(ack)
    }

    /// Settle an uncertain commit only when private source-state readback proves
    /// this exact frozen batch and its complete acknowledgement.
    pub fn settle_from_state(&mut self, batch: &SourceBatch, state: &Value) -> Result<BatchAck> {
        let key = Self::scope_key(batch);
        if self.unknown_batches.get(&key) != Some(batch) {
            return Err(unknown_batch_error());
        }

        let digest = batch.semantic_digest().map_err(|_| unknown_batch_error())?;
        let first_seq = state
            .get("last_batch_first_seq")
            .and_then(Value::as_i64)
            .ok_or_else(unknown_batch_error)?;
        let last_seq = state
            .get("last_batch_last_seq")
            .and_then(Value::as_i64)
            .ok_or_else(unknown_batch_error)?;
        let count = last_seq
            .checked_sub(first_seq)
            .and_then(|value| value.checked_add(1));
        let expected_count = i64::try_from(batch.events.len()).ok();
        let expected_digest = serde_json::to_value(digest).map_err(|_| unknown_batch_error())?;

        if batch.events.is_empty()
            || first_seq <= 0
            || last_seq < first_seq
            || count != expected_count
            || state.get("agent_id").and_then(Value::as_str) != Some(batch.agent_id.as_str())
            || state.get("generation").and_then(Value::as_i64) != Some(batch.generation)
            || state.get("source").and_then(Value::as_str) != Some(batch.source.as_str())
            || state.get("last_batch_id").and_then(Value::as_str) != Some(batch.batch_id.as_str())
            || state.get("last_batch_digest") != Some(&expected_digest)
            || state.get("cursor") != Some(&serde_json::json!(batch.cursor))
        {
            return Err(unknown_batch_error());
        }

        let ack = BatchAck {
            first_seq,
            last_seq,
            cursor: batch.cursor.clone(),
        };
        self.unknown_batches.remove(&key);
        Ok(ack)
    }

    /// Whether this exact (agent, generation, source) has a commit sent
    /// without a valid acknowledgement. Other sources never share this latch.
    pub fn is_unknown(&self, agent_id: &str, generation: i64, source: &str) -> bool {
        self.unknown_batches
            .contains_key(&(agent_id.to_owned(), generation, source.to_owned()))
    }

    fn scope_key(batch: &SourceBatch) -> (String, i64, String) {
        (
            batch.agent_id.clone(),
            batch.generation,
            batch.source.clone(),
        )
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

async fn writer_loop<W>(
    mut writer: FramedWrite<W, LinesCodec>,
    mut writes: mpsc::Receiver<OutboundFrame>,
    pending: PendingCalls,
) where
    W: AsyncWrite + Unpin + Send + 'static,
{
    while let Some(outbound) = writes.recv().await {
        let result = writer
            .send(outbound.encoded)
            .await
            .map_err(|_| Error::new("transport_write", "plugin protocol write failed"));
        let failure = result.as_ref().err().cloned();
        let _ = outbound.written.send(result);
        if let Some(error) = failure {
            let pending_calls = {
                let mut pending = pending.lock_recover();
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
        self.pending.lock_recover().remove(&self.id);
    }
}

fn decode_frame(line: &str) -> std::result::Result<Incoming, RpcError> {
    decode_private_frame(line).map_err(|kind| {
        let (code, message) = match kind {
            DecodeErrorKind::InvalidJson => (-32700, "invalid JSON-RPC frame"),
            DecodeErrorKind::NotObject => (-32600, "JSON-RPC frame must be an object"),
            DecodeErrorKind::InvalidVersion => (-32600, "JSON-RPC version must be 2.0"),
            DecodeErrorKind::InvalidRequest => (-32600, "invalid JSON-RPC request"),
            DecodeErrorKind::InvalidRequestParams => {
                (-32602, "JSON-RPC request requires an id and object params")
            }
            DecodeErrorKind::InvalidResponse => (-32600, "invalid JSON-RPC response"),
            DecodeErrorKind::InvalidResponseShape => (
                -32600,
                "JSON-RPC response must contain exactly one result or error",
            ),
        };
        protocol_error("invalid", code, message)
    })
}

fn encode_frame<T: Serialize>(message: &T) -> Result<String> {
    encode_private_frame(message).map_err(|kind| match kind {
        EncodeErrorKind::Unencodable => {
            Error::new("internal", "JSON-RPC frame could not be encoded")
        }
        EncodeErrorKind::TooLarge => {
            Error::new("frame_too_large", "encoded JSON-RPC frame exceeds 1 MiB")
        }
    })
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

#[cfg(test)]
mod tests {
    use super::*;
    use eliot_core::{SecretPolicy, StderrSnapshot, StderrSource, classify_dispatched_error};

    #[test]
    fn private_decoder_preserves_peer_error_codes_and_messages() {
        for (line, code, message) in [
            ("{", -32700, "invalid JSON-RPC frame"),
            ("null", -32600, "JSON-RPC frame must be an object"),
            ("{}", -32600, "JSON-RPC version must be 2.0"),
            (
                r#"{"jsonrpc":"2.0","id":1,"method":null}"#,
                -32600,
                "invalid JSON-RPC request",
            ),
            (
                r#"{"jsonrpc":"2.0","id":1,"method":"call","params":null}"#,
                -32602,
                "JSON-RPC request requires an id and object params",
            ),
            (
                r#"{"jsonrpc":"2.0","id":1,"error":true}"#,
                -32600,
                "invalid JSON-RPC response",
            ),
            (
                r#"{"jsonrpc":"2.0","id":1,"result":null}"#,
                -32600,
                "JSON-RPC response must contain exactly one result or error",
            ),
        ] {
            let error = decode_frame(line).err().expect("invalid private frame");
            assert_eq!(error.code, code);
            assert_eq!(error.message, message);
            assert_eq!(error.data, Some(json!({"code": "invalid"})));
        }
    }

    fn snapshot(text: &str) -> StderrSnapshot {
        StderrSnapshot {
            text: text.to_owned(),
            truncated: false,
            eof: true,
            read_error: false,
        }
    }

    fn test_client() -> PeerClient {
        let (writes, _receiver) = mpsc::channel(1);
        PeerClient {
            writes,
            pending: Arc::new(StdMutex::new(HashMap::new())),
            pending_slots: Arc::new(Semaphore::new(MAX_PENDING_CALLS)),
            next_id: Arc::new(AtomicU64::new(0)),
        }
    }

    fn batch_readback(batch: &SourceBatch) -> Value {
        json!({
            "agent_id": batch.agent_id,
            "generation": batch.generation,
            "source": batch.source,
            "state": "open",
            "cursor": batch.cursor,
            "last_batch_id": batch.batch_id,
            "last_batch_digest": batch.semantic_digest().unwrap(),
            "last_batch_first_seq": 7,
            "last_batch_last_seq": 7,
        })
    }

    #[test]
    fn uncertain_batch_settlement_requires_exact_scope_digest_cursor_and_range() {
        let batch = SourceBatch {
            batch_id: "batch-1".to_owned(),
            agent_id: "a1".to_owned(),
            generation: 3,
            source: "transcript".to_owned(),
            events: vec![eliot_core::NativeEvent {
                kind: "message.completed".to_owned(),
                native_id: "native-1".to_owned(),
                data: json!({"text":"complete"}),
                ts: Some(123),
            }],
            cursor: Some("opaque-edge".to_owned()),
        };
        let mut sink = CommitSink::new(test_client(), "credential");
        sink.unknown_batches
            .insert(CommitSink::scope_key(&batch), batch.clone());

        let ack = sink
            .settle_from_state(&batch, &batch_readback(&batch))
            .expect("exact retained batch proof");
        assert_eq!(ack.first_seq, 7);
        assert_eq!(ack.last_seq, 7);
        assert_eq!(ack.cursor, batch.cursor);
        assert!(!sink.is_unknown(&batch.agent_id, batch.generation, &batch.source));

        let mut different_payload = batch.clone();
        different_payload.events[0].data["text"] = json!("changed");
        sink.unknown_batches
            .insert(CommitSink::scope_key(&batch), batch.clone());
        assert!(
            sink.settle_from_state(&different_payload, &batch_readback(&batch))
                .is_err()
        );
        assert!(sink.is_unknown(&batch.agent_id, batch.generation, &batch.source));

        for (field, value) in [
            ("agent_id", json!("a2")),
            ("generation", json!(4)),
            ("source", json!("other")),
            ("last_batch_id", json!("other-batch")),
            (
                "last_batch_digest",
                serde_json::to_value([0u8; 32]).unwrap(),
            ),
            ("cursor", json!("other-edge")),
            ("last_batch_first_seq", json!(0)),
            ("last_batch_last_seq", json!(8)),
        ] {
            let mut proof = batch_readback(&batch);
            proof[field] = value;
            sink.unknown_batches
                .insert(CommitSink::scope_key(&batch), batch.clone());
            assert!(
                sink.settle_from_state(&batch, &proof).is_err(),
                "field={field}"
            );
            assert!(
                sink.is_unknown(&batch.agent_id, batch.generation, &batch.source),
                "field={field}"
            );
        }
    }

    #[tokio::test]
    async fn unknown_batch_is_isolated_through_actual_peer_rpc() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, duplex, split};

        fn mk_batch(id: &str, agent: &str, source: &str, edge: &str, native: &str) -> SourceBatch {
            SourceBatch {
                batch_id: id.to_owned(),
                agent_id: agent.to_owned(),
                generation: 3,
                source: source.to_owned(),
                events: vec![eliot_core::NativeEvent {
                    kind: "message.completed".to_owned(),
                    native_id: native.to_owned(),
                    data: json!({"text": id}),
                    ts: Some(1),
                }],
                cursor: Some(edge.to_owned()),
            }
        }

        let batch_a = mk_batch("batch-a", "a1", "transcript", "edge-a", "native-a");
        let batch_b = mk_batch("batch-b", "a2", "transcript", "edge-b", "native-b");
        let batch_parent = mk_batch(
            "batch-parent",
            "a1",
            "stream",
            "edge-parent",
            "native-parent",
        );

        let (host_io, plugin_io) = duplex(64 * 1024);
        let (plugin_read, plugin_write) = split(plugin_io);
        let mut peer = Peer::from_io(plugin_read, plugin_write);
        let client = peer.client();
        let peer_task = tokio::spawn(async move { while peer.next_request().await.is_some() {} });

        let (host_read, mut host_write) = split(host_io);
        let mut host_reader = BufReader::new(host_read);
        let mut recorded: Vec<(String, Value)> = Vec::new();

        // Fake host: 1) read A, never reply (ACK lost). 2) read B, ack 7. 3) read parent, ack 8.
        let fake_host = async {
            let mut line = String::new();
            line.clear();
            host_reader
                .read_line(&mut line)
                .await
                .expect("A request read");
            let req_a: RpcRequest = serde_json::from_str(&line).expect("decode A");
            assert_eq!(req_a.method, "events.put");
            assert_eq!(req_a.params["agent_id"], "a1");
            assert_eq!(req_a.params["source"], "transcript");
            recorded.push((req_a.method.clone(), req_a.params.clone()));
            // ACK for A is dropped on purpose: no response written.

            line.clear();
            host_reader
                .read_line(&mut line)
                .await
                .expect("B request read");
            let req_b: RpcRequest = serde_json::from_str(&line).expect("decode B");
            assert_eq!(req_b.method, "events.put");
            assert_eq!(req_b.params["agent_id"], "a2");
            recorded.push((req_b.method.clone(), req_b.params.clone()));
            let ack_b = RpcResponse::success(
                req_b.id.clone(),
                json!({"first_seq":7,"last_seq":7,"cursor":"edge-b"}),
            );
            let frame = encode_frame(&ack_b).expect("ack B frame");
            host_write
                .write_all(format!("{frame}\n").as_bytes())
                .await
                .expect("send ack B");

            line.clear();
            host_reader
                .read_line(&mut line)
                .await
                .expect("parent request read");
            let req_p: RpcRequest = serde_json::from_str(&line).expect("decode parent");
            assert_eq!(req_p.method, "events.put");
            assert_eq!(req_p.params["agent_id"], "a1");
            assert_eq!(req_p.params["source"], "stream");
            recorded.push((req_p.method.clone(), req_p.params.clone()));
            let ack_p = RpcResponse::success(
                req_p.id.clone(),
                json!({"first_seq":8,"last_seq":8,"cursor":"edge-parent"}),
            );
            let frame = encode_frame(&ack_p).expect("ack parent frame");
            host_write
                .write_all(format!("{frame}\n").as_bytes())
                .await
                .expect("send ack parent");

            drop(host_write);
            drop(host_reader);
            recorded
        };

        let plugin_calls = async {
            let mut sink = CommitSink::new(client, "credential");
            // Fresh deadline per commit: the first deadline expires while A's
            // ACK is lost, so B/parent need their own windows.
            let deadline_a = Instant::now() + Duration::from_millis(300);

            // A: processed by host but ACK lost -> unknown, exact batch retained.
            let err_a = sink
                .commit(batch_a.clone(), deadline_a)
                .await
                .expect_err("A ACK lost");
            assert_eq!(err_a.code, "unknown");
            assert!(sink.is_unknown(&batch_a.agent_id, batch_a.generation, &batch_a.source));

            // SAME sink: sibling B and parent different source commit through healthy RPC.
            let deadline_b = Instant::now() + Duration::from_secs(5);
            let ack_b = sink
                .commit(batch_b.clone(), deadline_b)
                .await
                .expect("B commit succeeds");
            assert_eq!(ack_b.first_seq, 7);
            assert_eq!(ack_b.cursor.as_deref(), Some("edge-b"));
            let deadline_p = Instant::now() + Duration::from_secs(5);
            let ack_p = sink
                .commit(batch_parent.clone(), deadline_p)
                .await
                .expect("parent commit succeeds");
            assert_eq!(ack_p.first_seq, 8);
            assert_eq!(ack_p.cursor.as_deref(), Some("edge-parent"));

            // A remains blocked; same-scope retry never hits wire.
            assert!(sink.is_unknown(&batch_a.agent_id, batch_a.generation, &batch_a.source));
            let err_retry = sink
                .commit(batch_a.clone(), Instant::now() + Duration::from_secs(5))
                .await
                .expect_err("A same-scope blocked");
            assert_eq!(err_retry.code, "unknown");

            // Mismatched digest / wrong generation / wrong source cannot settle A.
            let mut wrong_digest = batch_readback(&batch_a);
            wrong_digest["last_batch_digest"] = serde_json::to_value([9u8; 32]).unwrap();
            assert!(sink.settle_from_state(&batch_a, &wrong_digest).is_err());
            let mut wrong_gen = batch_readback(&batch_a);
            wrong_gen["generation"] = json!(99);
            assert!(sink.settle_from_state(&batch_a, &wrong_gen).is_err());
            let mut wrong_source = batch_readback(&batch_a);
            wrong_source["source"] = json!("other");
            assert!(sink.settle_from_state(&batch_a, &wrong_source).is_err());
            let mut wrong_agent = batch_readback(&batch_a);
            wrong_agent["agent_id"] = json!("a2");
            assert!(sink.settle_from_state(&batch_a, &wrong_agent).is_err());
            assert!(sink.is_unknown(&batch_a.agent_id, batch_a.generation, &batch_a.source));

            // Exact readback settles only A.
            let ack_a = sink
                .settle_from_state(&batch_a, &batch_readback(&batch_a))
                .expect("exact readback");
            assert_eq!(ack_a.first_seq, 7);
            assert_eq!(ack_a.cursor, batch_a.cursor);
            assert!(!sink.is_unknown(&batch_a.agent_id, batch_a.generation, &batch_a.source));
        };

        let (recorded, ()) = tokio::join!(fake_host, plugin_calls);
        peer_task.await.expect("peer task clean shutdown");

        // Actual wire evidence: exactly three events.put calls (A, B, parent). The
        // blocked A retry never reached the wire.
        assert_eq!(recorded.len(), 3);
        assert_eq!(recorded[0].0, "events.put");
        assert_eq!(recorded[0].1["agent_id"], "a1");
        assert_eq!(recorded[0].1["source"], "transcript");
        assert_eq!(recorded[1].0, "events.put");
        assert_eq!(recorded[1].1["agent_id"], "a2");
        assert_eq!(recorded[2].0, "events.put");
        assert_eq!(recorded[2].1["agent_id"], "a1");
        assert_eq!(recorded[2].1["source"], "stream");
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

    #[tokio::test]
    async fn connected_peer_records_request_and_handles_busy_retry_to_canonical_ack() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, duplex, split};

        let (host_io, plugin_io) = duplex(64 * 1024);
        let (plugin_read, plugin_write) = split(plugin_io);
        let mut peer = Peer::from_io(plugin_read, plugin_write);
        let client = peer.client();

        let peer_task = tokio::spawn(async move { while peer.next_request().await.is_some() {} });

        let mut recorded = Vec::new();
        let (host_read, mut host_write) = split(host_io);
        let mut host_reader = BufReader::new(host_read);

        let fake_host = async {
            let mut line = String::new();

            // 1. First call: fake host records request and replies with busy error.
            line.clear();
            host_reader
                .read_line(&mut line)
                .await
                .expect("first request read");
            let req1: RpcRequest = serde_json::from_str(&line).expect("decode req1");
            recorded.push((req1.method.clone(), req1.params.clone()));
            let busy_reply = RpcResponse::failure(
                req1.id.clone(),
                RpcError {
                    code: -32020,
                    message: "plugin request queue is full".to_owned(),
                    data: Some(json!({"code": "busy"})),
                },
            );
            let frame = encode_frame(&busy_reply).expect("encode busy response");
            host_write
                .write_all(format!("{frame}\n").as_bytes())
                .await
                .expect("send busy response");

            // 2. Second call: fake host records retry and replies with canonical ACK.
            line.clear();
            host_reader
                .read_line(&mut line)
                .await
                .expect("second request read");
            let req2: RpcRequest = serde_json::from_str(&line).expect("decode req2");
            recorded.push((req2.method.clone(), req2.params.clone()));
            let ack_reply = RpcResponse::success(
                req2.id.clone(),
                json!({
                    "first_seq": 1,
                    "last_seq": 1,
                    "cursor": "cursor-1",
                }),
            );
            let frame = encode_frame(&ack_reply).expect("encode ack response");
            host_write
                .write_all(format!("{frame}\n").as_bytes())
                .await
                .expect("send ack response");

            drop(host_write);
            drop(host_reader);
            recorded
        };

        let plugin_calls = async {
            let deadline = Instant::now() + Duration::from_secs(5);
            let first_call = client
                .call(
                    "events.put",
                    json!({"credential": "fixture-cred", "events": [{"kind": "test"}]}),
                    deadline,
                )
                .await;
            let err = first_call.expect_err("first call must fail with busy");
            assert_eq!(err.code, "busy");

            let second_call = client
                .call(
                    "events.put",
                    json!({"credential": "fixture-cred", "events": [{"kind": "test"}]}),
                    deadline,
                )
                .await;
            let ack_val = second_call.expect("second call must succeed");
            let ack: BatchAck = serde_json::from_value(ack_val).expect("decode batch ack");
            assert_eq!(ack.first_seq, 1);
            assert_eq!(ack.last_seq, 1);
            assert_eq!(ack.cursor.as_deref(), Some("cursor-1"));
        };

        let (recorded, ()) = tokio::join!(fake_host, plugin_calls);
        peer_task.await.expect("peer task clean shutdown");

        assert_eq!(recorded.len(), 2);
        assert_eq!(recorded[0].0, "events.put");
        assert_eq!(recorded[0].1["credential"], "fixture-cred");
        assert_eq!(recorded[1].0, "events.put");
        assert_eq!(recorded[1].1["credential"], "fixture-cred");
    }
}
