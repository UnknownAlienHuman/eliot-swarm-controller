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
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch},
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
    retired: watch::Sender<bool>,
}

struct PreparedCall {
    id: String,
    encoded: String,
}

struct PendingGuard {
    pending: PendingCalls,
    id: String,
}

/// Sends event batches to the host and validates the returned acknowledgement.
pub struct CommitSink {
    client: PeerClient,
    credential: String,
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
            retired: watch::Sender::new(false),
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
        let mut retired = self.client.retired.subscribe();
        loop {
            let frame = tokio::select! {
                biased;
                _ = retired.wait_for(|retired| *retired) => return None,
                frame = self.reader.next() => frame,
            };
            match frame {
                Some(Ok(line)) if line.len().saturating_add(1) <= MAX_FRAME_BYTES => {
                    match decode_frame(&line) {
                        Ok(Incoming::Request(request)) => return Some(request),
                        Ok(Incoming::Response(response)) => {
                            if !self.client.route_response(response) {
                                return None;
                            }
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
        if *self.retired.borrow() {
            return Err(Error::new("transport_closed", "plugin peer is closed"));
        }
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

    /// Route one host response to its pending call. Returns whether the peer
    /// stays usable: a non-string id can never match a pending call (outbound
    /// ids are always `plugin-call-{seq}` strings), so the peer is
    /// non-conformant and every pending call fails at once with `unknown`
    /// instead of stranding until its deadline. Each failed call was already
    /// sent, so every effect stays uncertain.
    fn route_response(&self, response: RpcResponse) -> bool {
        let Some(id) = response.id.as_str() else {
            self.fail_pending(Error::new("unknown", "plugin response id is not a string"));
            return false;
        };
        let sender = self.pending.lock_recover().remove(id);
        let Some(sender) = sender else {
            return true;
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
        true
    }

    pub(crate) fn retire(&self) {
        self.retired.send_replace(true);
        self.pending_slots.close();
        self.fail_pending(Error::new("transport_closed", "plugin peer is closed"));
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
        }
    }

    /// Commit one complete event batch and validate its acknowledgement.
    pub async fn commit(&mut self, batch: SourceBatch, deadline: Instant) -> Result<BatchAck> {
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
        let mut params = serde_json::to_value(batch)
            .map_err(|_| Error::new("invalid", "event batch could not be encoded"))?;
        params["credential"] = Value::String(self.credential.clone());
        let prepared = self.client.prepare_call("events.put", params)?;
        let permit = self
            .client
            .acquire_pending(deadline)
            .await
            .map_err(|error| {
                if error.code == "timeout" {
                    Error::new("busy", "event batch could not enter the request queue")
                } else {
                    error
                }
            })?;
        let value = match self.client.call_prepared(prepared, deadline, permit).await {
            Ok(value) => value,
            Err(error) if is_definite_batch_rejection(&error.code) => return Err(error),
            Err(_) => return Err(self.lost_ack()),
        };
        let ack: BatchAck = serde_json::from_value(value).map_err(|_| self.lost_ack())?;
        let actual_count = ack
            .last_seq
            .checked_sub(ack.first_seq)
            .and_then(|count| count.checked_add(1));
        if ack.first_seq <= 0
            || actual_count != Some(expected_count)
            || ack.cursor != expected_cursor
        {
            return Err(self.lost_ack());
        }
        Ok(ack)
    }

    fn lost_ack(&self) -> Error {
        self.client.retire();
        Error::new("unknown", "event batch acknowledgement unavailable")
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
    // The typed failure location and origin ride along so the daemon can
    // report why and where without guessing from free text.
    if let Ok(encoded) = serde_json::to_value(&error) {
        for key in [
            "where",
            "origin",
            "origin_evidence",
            "exit_code",
            "ntstatus",
            "diagnostics",
        ] {
            if let Some(field) = encoded.get(key) {
                data[key] = field.clone();
            }
        }
    }
    RpcError {
        code: wire_code,
        message: *error.message,
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
    let Some(data) = data else {
        return primary;
    };
    // Core's deserializer validates sources/counts/shapes and omits malformed
    // optional data. Mandatory fields here are our already-decoded originals.
    let mut value = json!({
        "code": primary.code,
        "message": primary.message,
    });
    for key in [
        "where",
        "origin",
        "origin_evidence",
        "exit_code",
        "ntstatus",
        "diagnostics",
    ] {
        if let Some(field) = data.get(key) {
            value[key] = field.clone();
        }
    }
    serde_json::from_value(value).unwrap_or(primary)
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

/// `events.put` errors that prove the batch was never committed.
pub fn is_definite_batch_rejection(code: &str) -> bool {
    matches!(
        code,
        "rejected"
            | "busy"
            | "frame_too_large"
            | "secret_in_payload"
            | "invalid"
            | "not_allowed"
            | "stale_generation"
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
            retired: watch::Sender::new(false),
        }
    }

    #[test]
    fn malformed_response_id_fails_pending_calls_with_unknown() {
        let client = test_client();
        let (tx, mut rx) = oneshot::channel();
        client
            .pending
            .lock_recover()
            .insert("plugin-call-7".to_owned(), tx);
        let response = RpcResponse {
            jsonrpc: "2.0".to_owned(),
            id: json!(42),
            result: Some(json!({"ok": true})),
            error: None,
        };
        // A non-string id can never match the string outbound id: the peer
        // is non-conformant, so the call fails at once with `unknown`
        // instead of stranding until its deadline, and the peer stops.
        assert!(!client.route_response(response));
        let error = rx
            .try_recv()
            .expect("malformed id wakes the pending call")
            .expect_err("pending call fails");
        assert_eq!(error.code, "unknown");
        assert!(client.pending.lock_recover().is_empty());
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
            assert_eq!(error.message.as_ref(), "original failure");
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

    #[test]
    fn definite_batch_rejection_codes_are_documented() {
        for code in [
            "secret_in_payload",
            "invalid",
            "not_allowed",
            "stale_generation",
            "rejected",
            "busy",
            "frame_too_large",
        ] {
            assert!(is_definite_batch_rejection(code), "code={code}");
        }
        for code in [
            "unknown",
            "timeout",
            "transport_closed",
            "transport_write",
            "invalid_response",
            "source_integrity",
            "internal",
        ] {
            assert!(!is_definite_batch_rejection(code), "code={code}");
        }
    }

    #[tokio::test]
    async fn definite_rejection_keeps_peer_available() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, duplex, split};

        let batch = SourceBatch {
            batch_id: "batch-secret".to_owned(),
            agent_id: "a1".to_owned(),
            generation: 3,
            source: "stream".to_owned(),
            events: vec![eliot_core::NativeEvent {
                kind: "session.ready".to_owned(),
                native_id: "native-1".to_owned(),
                data: json!({"native_scope": "claude-config:/secret-path"}),
                ts: Some(1),
            }],
            cursor: None,
        };

        let (host_io, plugin_io) = duplex(64 * 1024);
        let (plugin_read, plugin_write) = split(plugin_io);
        let mut peer = Peer::from_io(plugin_read, plugin_write);
        let client = peer.client();
        let peer_task = tokio::spawn(async move { while peer.next_request().await.is_some() {} });

        let (host_read, mut host_write) = split(host_io);
        let mut host_reader = BufReader::new(host_read);
        let fake_host = async {
            let mut line = String::new();
            host_reader
                .read_line(&mut line)
                .await
                .expect("events.put request read");
            let req: RpcRequest = serde_json::from_str(&line).expect("decode request");
            assert_eq!(req.method, "events.put");
            let reply = RpcResponse::failure(
                req.id.clone(),
                RpcError {
                    code: -32000,
                    message: "native source payload contains a known secret in immutable metadata"
                        .to_owned(),
                    data: Some(json!({"code": "secret_in_payload"})),
                },
            );
            let frame = encode_frame(&reply).expect("encode failure response");
            host_write
                .write_all(format!("{frame}\n").as_bytes())
                .await
                .expect("send failure response");
            drop(host_write);
            drop(host_reader);
        };

        let plugin_calls = async {
            let mut sink = CommitSink::new(client.clone(), "credential");
            let err = sink
                .commit(batch.clone(), Instant::now() + Duration::from_secs(5))
                .await
                .expect_err("definite rejection surfaces");
            assert_eq!(err.code, "secret_in_payload");
            assert!(!*client.retired.borrow());
        };

        tokio::join!(fake_host, plugin_calls);
        peer_task.await.expect("peer task clean shutdown");
    }

    #[tokio::test]
    async fn unknown_write_outcome_retires_peer() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, duplex, split};

        let batch = SourceBatch {
            batch_id: "batch-store".to_owned(),
            agent_id: "a9".to_owned(),
            generation: 3,
            source: "stream".to_owned(),
            events: vec![eliot_core::NativeEvent {
                kind: "turn.started".to_owned(),
                native_id: "native-9".to_owned(),
                data: json!({"turn_id": "t-1"}),
                ts: Some(1),
            }],
            cursor: None,
        };

        let (host_io, plugin_io) = duplex(64 * 1024);
        let (plugin_read, plugin_write) = split(plugin_io);
        let mut peer = Peer::from_io(plugin_read, plugin_write);
        let client = peer.client();
        let peer_task = tokio::spawn(async move { while peer.next_request().await.is_some() {} });

        let (host_read, mut host_write) = split(host_io);
        let mut host_reader = BufReader::new(host_read);
        let fake_host = async {
            let mut line = String::new();
            host_reader
                .read_line(&mut line)
                .await
                .expect("events.put request read");
            let req: RpcRequest = serde_json::from_str(&line).expect("decode request");
            assert_eq!(req.method, "events.put");
            let reply = RpcResponse::failure(
                req.id.clone(),
                RpcError {
                    code: -32000,
                    message: "events.put outcome unknown: store_commit".to_owned(),
                    data: Some(json!({"code": "unknown"})),
                },
            );
            let frame = encode_frame(&reply).expect("encode failure response");
            host_write
                .write_all(format!("{frame}\n").as_bytes())
                .await
                .expect("send failure response");
            drop(host_write);
            drop(host_reader);
        };

        let plugin_calls = async {
            let mut sink = CommitSink::new(client.clone(), "credential");
            let err = sink
                .commit(batch.clone(), Instant::now() + Duration::from_secs(5))
                .await
                .expect_err("post-transaction error stays unknown");
            assert_eq!(err.code, "unknown");
            assert!(*client.retired.borrow());
        };

        tokio::join!(fake_host, plugin_calls);
        peer_task.await.expect("peer task clean shutdown");
    }
}
